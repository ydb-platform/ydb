#include "partition.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/pqtablet/common/logging.h>
#include "partition_util.h"
#include <util/string/escape.h>

#define LOG_PREFIX_INT TStringBuilder() << "[" << TabletId << "]" << GetLogPrefix()

namespace NKikimr::NPQ {

bool TPartition::ExecRequestForCompaction(TWriteMsg& p, TProcessParametersBase& parameters, TEvKeyValue::TEvRequest* request, const TInstant blobCreationUnixTime)
{
    const auto& ctx = ActorContext();
    ui64& curOffset = parameters.CurOffset;
    ui64 poffset = p.Offset ? *p.Offset : curOffset;

    YDB_LOG_TRACE_COMP(Service, "Topic partition process write",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"topicName", TopicName()},
        {"partition", Partition},
        {"sourceId", EscapeC(p.Msg.SourceId)},
        {"disableDeduplication", p.Msg.DisableDeduplication},
        {"seqNo", p.Msg.SeqNo},
        {"initialSeqNo", p.InitialSeqNo});

    AFL_ENSURE(poffset >= curOffset);

    bool needCompactHead = poffset > curOffset;
    if (needCompactHead) { //got gap
        // не нужно проверять p.Msg.PartNo. мы можем начать с блоба в середине сообщения
        curOffset = poffset;
    }

    if (p.Msg.PartNo == 0) { //create new PartitionedBlob
        if (CompactionBlobEncoder.PartitionedBlob.HasFormedBlobs()) {
            //clear currently-writed blobs
            auto oldCmdWrite = request->Record.GetCmdWrite();
            request->Record.ClearCmdWrite();
            for (ssize_t i = 0; i < oldCmdWrite.size(); ++i) {
                auto key = TKey::FromString(oldCmdWrite.Get(i).GetKey());
                if (key.GetType() != TKeyPrefix::TypeTmpData) {
                    request->Record.AddCmdWrite()->CopyFrom(oldCmdWrite.Get(i));
                }
            }
        }
        CompactionBlobEncoder.NewPartitionedBlob(Partition,
                                                 curOffset,
                                                 p.Msg.SourceId,
                                                 p.Msg.SeqNo,
                                                 p.Msg.TotalParts,
                                                 p.Msg.TotalSize,
                                                 parameters.HeadCleared,
                                                 needCompactHead,
                                                 MaxBlobSize);
    }

    YDB_LOG_DEBUG_COMP(Service, "Topic partition part blob processing sourceId seqNo partNo",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"topicName", TopicName()},
        {"partition", Partition},
        {"sourceId", EscapeC(p.Msg.SourceId)},
        {"seqNo", p.Msg.SeqNo},
        {"partNo", p.Msg.PartNo});

    TString s;
    if (!CompactionBlobEncoder.PartitionedBlob.IsNextPart(p.Msg.SourceId, p.Msg.SeqNo, p.Msg.PartNo, &s)) {
        //this must not be happen - client sends gaps, fail this client till the end
        //now no changes will leak
        YDB_LOG_DEBUG_COMP(Service, "Can't append message part",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"s", s});
        ctx.Send(TabletActorId, new TEvents::TEvPoison());
        return false;
    }

    TMaybe<TPartData> partData;
    if (p.Msg.TotalParts > 1) { //this is multi-part message
        partData = TPartData(p.Msg.PartNo, p.Msg.TotalParts, p.Msg.TotalSize);
    }
    WriteTimestamp = ctx.Now();
    WriteTimestampEstimate = p.Msg.WriteTimestamp > 0 ? TInstant::MilliSeconds(p.Msg.WriteTimestamp) : WriteTimestamp;
    TClientBlob blob(std::move(p.Msg.SourceId), p.Msg.SeqNo, std::move(p.Msg.Data), partData, WriteTimestampEstimate,
                        TInstant::MilliSeconds(p.Msg.CreateTimestamp == 0 ? curOffset : p.Msg.CreateTimestamp),
                        p.Msg.UncompressedSize, std::move(p.Msg.PartitionKey), std::move(p.Msg.ExplicitHashKey),
                        p.Msg.MessageCount, p.Msg.IsBatch); //remove curOffset when LB will report CTime

    bool lastBlobPart = blob.IsLastPart();

    //will return compacted tmp blob
    auto newWrite = CompactionBlobEncoder.PartitionedBlob.Add(std::move(blob));

    if (newWrite && !newWrite->Value.empty()) {
        AddCmdWrite(newWrite, request, blobCreationUnixTime, ctx, false);

        YDB_LOG_DEBUG_COMP(Service, "Topic partition part blob sourceId seqNo partNo result is size",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"topicName", TopicName()},
            {"partition", Partition},
            {"sourceId", EscapeC(p.Msg.SourceId)},
            {"seqNo", p.Msg.SeqNo},
            {"partNo", p.Msg.PartNo},
            {"key", newWrite->Key},
            {"valueSize", newWrite->Value.size()});

        parameters.HeadCleared = true;
    }

    if (lastBlobPart) {
        FirstCompactionPart = std::make_pair(curOffset, p.Msg.PartNo);

        AFL_ENSURE(CompactionBlobEncoder.PartitionedBlob.IsComplete());
        ui32 curWrites = RenameTmpCmdWrites(request);
        AFL_ENSURE(curWrites <= CompactionBlobEncoder.PartitionedBlob.GetFormedBlobs().size());
        RenameFormedBlobs(CompactionBlobEncoder.PartitionedBlob.GetFormedBlobs(),
                          parameters,
                          curWrites,
                          request,
                          CompactionBlobEncoder,
                          ctx);

        ui32 countOfLastParts = 0;
        for (auto& x : CompactionBlobEncoder.PartitionedBlob.GetClientBlobs()) {
            if (CompactionBlobEncoder.NewHead.GetBatches().empty() || CompactionBlobEncoder.NewHead.GetLastBatch().Packed) {
                CompactionBlobEncoder.NewHead.AddBatch(TBatch(curOffset, x.GetPartNo()));
                CompactionBlobEncoder.NewHead.PackedSize += GetMaxHeaderSize(); //upper bound for packed size
            }

            if (x.IsLastPart()) {
                ++countOfLastParts;
            }

            AFL_ENSURE(!CompactionBlobEncoder.NewHead.GetLastBatch().Packed);
            CompactionBlobEncoder.NewHead.AddBlob(x);
            CompactionBlobEncoder.NewHead.PackedSize += x.GetSerializedSize();
            if (CompactionBlobEncoder.NewHead.GetLastBatch().GetUnpackedSize() >= BATCH_UNPACK_SIZE_BORDER) {
                CompactionBlobEncoder.PackLastBatch();
            }
        }

        AFL_ENSURE(countOfLastParts == 1);

        YDB_LOG_DEBUG_COMP(Service, "Topic partition part blob complete sourceId seqNo partNo FormedBlobsCount",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"topicName", TopicName()},
            {"partition", Partition},
            {"sourceId", EscapeC(p.Msg.SourceId)},
            {"seqNo", p.Msg.SeqNo},
            {"partNo", p.Msg.PartNo},
            {"compactionBlobEncoderPartitionedBlobFormedBlobsSize", CompactionBlobEncoder.PartitionedBlob.GetFormedBlobs().size()},
            {"newHead", CompactionBlobEncoder.NewHead});

        curOffset += p.Msg.MessageCount;
        CompactionBlobEncoder.ClearPartitionedBlob(Partition, MaxBlobSize);
    }

    return true;
}

size_t TPartition::GetBodyKeysCountLimit() const
{
    return AppData()->PQConfig.GetCompactionConfig().GetBlobsCount();
}

ui64 TPartition::GetCumulativeSizeLimit() const
{
    return Min(AppData()->PQConfig.GetCompactionConfig().GetBlobsSize(), 2 * MaxBlobSize);
}

ui64 TPartition::GetCompactedBlobSizeLowerBound() const
{
    return Config.GetPartitionConfig().GetLowWatermark();
}

void TPartition::DumpKeysForBlobsCompaction() const
{
    YDB_LOG_DEBUG_COMP(Service, "==== keys for blobs compaction ====",
        {"logPrefix", NPQ_LOG_PREFIX});
    for (size_t i = 0; i < BlobEncoder.DataKeysBody.size(); ++i) {
        const auto& k = BlobEncoder.DataKeysBody[i];
        YDB_LOG_DEBUG_COMP(Service, "Dump NPQLOGPREFIX, #_((k.Size >= GetCompactedBlobSizeLowerBound()) ? 'R' '*'), #_k.Key, #_k.Size",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"kSizeCompactedBlobSizeLowerBoundR", ((k.Size >= GetCompactedBlobSizeLowerBound()) ? 'R' : '*')},
            {"key", k.Key},
            {"kSize", k.Size});
    }
    YDB_LOG_DEBUG_COMP(Service, "===================================",
        {"logPrefix", NPQ_LOG_PREFIX});
}

void TPartition::TryRunCompaction(bool force)
{
    if (StopCompaction) {
        YDB_LOG_DEBUG_COMP(Service, "Blobs compaction is stopped",
            {"logPrefix", NPQ_LOG_PREFIX});
        return;
    }

    if (CompactionInProgress) {
        YDB_LOG_DEBUG_COMP(Service, "Blobs compaction in progress",
            {"logPrefix", NPQ_LOG_PREFIX});
        return;
    }

    if (BlobEncoder.DataKeysBody.empty()) {
        YDB_LOG_DEBUG_COMP(Service, "No data for blobs compaction",
            {"logPrefix", NPQ_LOG_PREFIX});
        return;
    }

    //DumpKeysForBlobsCompaction();

    const ui64 blobsKeyCountLimit = GetBodyKeysCountLimit();
    const ui64 compactedBlobSizeLowerBound = GetCompactedBlobSizeLowerBound();

    if ((BlobEncoder.DataKeysBody.size() < blobsKeyCountLimit) && (BlobEncoder.GetSize() < GetCumulativeSizeLimit()) && !force) {
        YDB_LOG_DEBUG_COMP(Service, "No data for blobs compaction",
            {"logPrefix", NPQ_LOG_PREFIX});
        return;
    }

    size_t blobsCount = 0, blobsSize = 0;
    for (; blobsCount < BlobEncoder.DataKeysBody.size(); ++blobsCount) {
        const auto& k = BlobEncoder.DataKeysBody[blobsCount];
        if (k.Size < compactedBlobSizeLowerBound) {
            // неполный блоб. можно дописать
            blobsSize += k.Size;
            if (blobsSize > 2 * MaxBlobSize) {
                // KV не может отдать много
                blobsSize -= k.Size;
                break;
            }
            YDB_LOG_DEBUG_COMP(Service, "Blob key for append",
                {"logPrefix", NPQ_LOG_PREFIX},
                {"key", k.Key},
                {"kSize", k.Size});
        } else {
            YDB_LOG_DEBUG_COMP(Service, "Blob key for rename",
                {"logPrefix", NPQ_LOG_PREFIX},
                {"key", k.Key});
        }
    }

    YDB_LOG_DEBUG_COMP(Service, "Keys were taken away. Let's read bytes",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"blobsCount", blobsCount},
        {"blobsSize", blobsSize});

    CompactionInProgress = true;

    Send(SelfId(), new TEvPQ::TEvRunCompaction(blobsCount));
}

void TPartition::Handle(TEvPQ::TEvForceCompaction::TPtr&)
{
    TryRunCompaction(true);
}

void TPartition::Handle(TEvPQ::TEvRunCompaction::TPtr& ev)
{
    const ui64 blobsCount = ev->Get()->BlobsCount;

    YDB_LOG_DEBUG_COMP(Service, "Begin compaction for blobs",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"blobsCount", blobsCount});

    TVector<TRequestedBlob> blobs;
    TBlobKeyTokens tokens;

    KeysForCompaction.clear();
    for (size_t i = 0; i < blobsCount; ++i) {
        const auto& k = BlobEncoder.DataKeysBody[i];
        if (k.Size >= GetCompactedBlobSizeLowerBound()) {
            KeysForCompaction.emplace_back(k, Max<ui64>());
            continue;
        }

        YDB_LOG_DEBUG_COMP(Service, "Request blob key",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"key", k.Key});

        KeysForCompaction.emplace_back(k, blobs.size());

        blobs.push_back(TRequestedBlob(
            k.Key.GetOffset(),
            k.Key.GetPartNo(),
            k.Key.GetCount(),
            k.Key.GetInternalPartsCount(),
            k.Size,
            TString(),
            k.Key,
            k.Timestamp.Seconds()
        ));
        tokens.Append(k.BlobKeyToken);
    }

    CompactionBlobsCount = blobs.size();

    auto request = MakeHolder<TEvPQ::TEvBlobRequest>(ERequestCookie::ReadBlobsForCompaction,
                                                     Partition,
                                                     std::move(blobs));
    Send(BlobCache, request.Release());

    YDB_LOG_DEBUG_COMP(Service, "Request blobs for compaction",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"compactionBlobsCount", CompactionBlobsCount});
}

bool TPartition::CompactRequestedBlob(const TRequestedBlob& requestedBlob,
                                      TProcessParametersBase& parameters,
                                      bool needToCompactHead,
                                      TEvKeyValue::TEvRequest* compactionRequest,
                                      TInstant& blobCreationUnixTime,
                                      bool wasThePreviousBlobBig,
                                      bool& newHeadIsInitialized)
{
    YDB_LOG_DEBUG_COMP(Service, "Dump NPQLOGPREFIX, #_requestedBlob.Key, #_parameters.CurOffset",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"requestedBlobKey", requestedBlob.Key},
        {"parametersCurOffset", parameters.CurOffset});

    ui64 offset = requestedBlob.Key.GetOffset();

    auto batches = requestedBlob.GetBatches();
    AFL_ENSURE(batches != nullptr);
    for (const auto& batch : *batches) {
        for (const auto& blob : batch.Blobs) {
            YDB_LOG_DEBUG_COMP(Service, "Try append part /",
                {"logPrefix", NPQ_LOG_PREFIX},
                {"offset", offset},
                {"partNo", blob.GetPartNo()},
                {"totalParts", blob.GetTotalParts()});

            ui16 partNo = blob.GetPartNo();
            const auto offsetPartNo = std::make_pair(offset, partNo);
            if (blob.IsLastPart()) {
                offset += blob.MessageCount;
            }

            if (FirstCompactionPart && (offsetPartNo <= *FirstCompactionPart)) {
                YDB_LOG_DEBUG_COMP(Service, "Part skipped",
                    {"logPrefix", NPQ_LOG_PREFIX},
                    {"offsetPartNoFirst", offsetPartNo.first},
                    {"offsetPartNoSecond", offsetPartNo.second},
                    {"firstCompactionPartFirst", FirstCompactionPart->first},
                    {"firstCompactionPartSecond", FirstCompactionPart->second});
                continue;
            }

            if (!newHeadIsInitialized) {
                CompactionBlobEncoder.NewHead.Offset = offsetPartNo.first;
                CompactionBlobEncoder.NewHead.PartNo = offsetPartNo.second;
                newHeadIsInitialized = true;
            }

            if (wasThePreviousBlobBig && blob.PartData && (partNo != 0)) {
                // надо продолжить писать большое сообщение
                CompactionBlobEncoder.NewHead.PartNo = partNo;
                CompactionBlobEncoder.NewPartitionedBlob(Partition,
                                                         parameters.CurOffset,
                                                         blob.SourceId,
                                                         blob.SeqNo,
                                                         blob.GetTotalParts(),
                                                         blob.GetTotalSize(),
                                                         parameters.HeadCleared,
                                                         needToCompactHead,
                                                         MaxBlobSize,
                                                         partNo);
            } else if (!CompactionBlobEncoder.PartitionedBlob.IsInited()) {
                CompactionBlobEncoder.NewPartitionedBlob(Partition,
                                                         parameters.CurOffset,
                                                         blob.SourceId,
                                                         blob.SeqNo,
                                                         blob.GetTotalParts(),
                                                         blob.GetTotalSize(),
                                                         parameters.HeadCleared,
                                                         needToCompactHead,
                                                         MaxBlobSize,
                                                         partNo);
            }
            wasThePreviousBlobBig = false;

            TWriteMsg msg{Max<ui64>(), offsetPartNo.first, TEvPQ::TEvWrite::TMsg{
                .SourceId = blob.SourceId,
                .SeqNo = blob.SeqNo,
                .PartNo = (ui16)(blob.PartData ? blob.PartData->PartNo : 0),
                .TotalParts = (ui16)(blob.PartData ? blob.PartData->TotalParts : 1),
                .TotalSize = (ui32)(blob.PartData ? blob.PartData->TotalSize : blob.UncompressedSize),
                .CreateTimestamp = blob.CreateTimestamp.MilliSeconds(),
                .ReceiveTimestamp = blob.CreateTimestamp.MilliSeconds(),

                // Disable deduplication, because otherwise we get an error,
                // due to the messages written through Kafka protocol with idempotent producer
                // have seqnos starting from 0 for new producer epochs (i.e. they have duplicate seqnos).
                // Disabling deduplication here is safe because all deduplication checks have been done already,
                // when the messages were written to the supportive partition.
                .DisableDeduplication = true,

                .WriteTimestamp = blob.WriteTimestamp.MilliSeconds(),
                .Data = blob.Data,
                .UncompressedSize = blob.UncompressedSize,
                .PartitionKey = blob.PartitionKey,
                .ExplicitHashKey = blob.ExplicitHashKey,
                .External = false,
                .IgnoreQuotaDeadline = true,
                .HeartbeatVersion = std::nullopt,
                .MessageCount = blob.MessageCount,
                .IsBatch = blob.IsBatch,
            }, std::nullopt};
            msg.Internal = true;

            blobCreationUnixTime = std::max(blobCreationUnixTime, blob.WriteTimestamp);
            if (!ExecRequestForCompaction(msg, parameters, compactionRequest, blobCreationUnixTime)) {
                YDB_LOG_DEBUG_COMP(Service, "Part not appended",
                    {"logPrefix", NPQ_LOG_PREFIX},
                    {"offsetPartNoFirst", offsetPartNo.first},
                    {"offsetPartNoSecond", offsetPartNo.second});
                return false;
            }

            YDB_LOG_DEBUG_COMP(Service, "Part appended",
                {"logPrefix", NPQ_LOG_PREFIX},
                {"offsetPartNoFirst", offsetPartNo.first},
                {"offsetPartNoSecond", offsetPartNo.second});
        }
    }

    return true;
}

void TPartition::RenameCompactedBlob(TDataKey& k,
                                     const size_t size,
                                     const bool needToCompactHead,
                                     bool& newHeadIsInitialized,
                                     TProcessParametersBase& parameters,
                                     TEvKeyValue::TEvRequest* compactionRequest)
{
    const auto& ctx = ActorContext();

    if (!newHeadIsInitialized) {
        CompactionBlobEncoder.NewHead.Offset = k.Key.GetOffset();
        CompactionBlobEncoder.NewHead.PartNo = k.Key.GetPartNo();
        newHeadIsInitialized = true;
    }

    if (!CompactionBlobEncoder.PartitionedBlob.IsInited()) {
        CompactionBlobEncoder.NewPartitionedBlob(Partition,
                                                 parameters.CurOffset,
                                                 "",                      // SourceId
                                                 0,                       // SeqNo
                                                 0,                       // TotalParts
                                                 0,                       // TotalSize
                                                 parameters.HeadCleared,  // headCleared
                                                 needToCompactHead,       // needCompactHead
                                                 MaxBlobSize);
    }
    auto write = CompactionBlobEncoder.PartitionedBlob.Add(k.Key, size, k.Timestamp, false);
    if (write && !write->Value.empty()) {
        // надо записать содержимое головы перед первым большим блобом
        AddCmdWrite(write, compactionRequest, k.Timestamp, ctx, false);
        CompactionBlobEncoder.CompactedKeys.emplace_back(write->Key, write->Value.size());
    }

    if (const auto& formedBlobs = CompactionBlobEncoder.PartitionedBlob.GetFormedBlobs(); !formedBlobs.empty()) {
        ui32 curWrites = RenameTmpCmdWrites(compactionRequest);
        RenameFormedBlobs(formedBlobs,
                          parameters,
                          curWrites,
                          compactionRequest,
                          CompactionBlobEncoder,
                          ctx);
    }

    k.BlobKeyToken->NeedDelete = false;

    parameters.CurOffset += k.Key.GetCount();

    FirstCompactionPart = Nothing();
}

bool TPartition::InitNewHeadForCompaction()
{
    CompactionBlobEncoder.NewHead.Clear();
    CompactionBlobEncoder.NewHead.PackedSize = 0;

    if (FirstCompactionPart) {
        // сначала надо пропустить обработанные части сообщений
        return false;
    }

    const auto& first = KeysForCompaction[0].first;
    CompactionBlobEncoder.NewHead.Offset = first.Key.GetOffset();
    CompactionBlobEncoder.NewHead.PartNo = first.Key.GetPartNo();

    return true;
}

void TPartition::BlobsForCompactionWereRead(const TVector<NPQ::TRequestedBlob>& blobs)
{
    const auto& ctx = ActorContext();

    YDB_LOG_DEBUG_COMP(Service, "Continue blobs compaction",
        {"logPrefix", NPQ_LOG_PREFIX});
    YDB_LOG_INFO_COMP(NKikimrServices::PQ_KV_OPS, "Begin blobs compaction",
        {"LOGPREFIXINT", LOG_PREFIX_INT});

    AFL_ENSURE(CompactionInProgress);
    AFL_ENSURE(blobs.size() == CompactionBlobsCount);

    CompactionBlobEncoder.ClearPartitionedBlob(Partition, MaxBlobSize);

    // Empty partition may will be filling from offset great than zero from mirror actor if source partition old and was clean by retantion time
    if (!CompactionBlobEncoder.Head.GetCount() &&
        !CompactionBlobEncoder.NewHead.GetCount() &&
        CompactionBlobEncoder.IsEmpty()) {
        // если это первое сообщение, то надо поправить StartOffset
        CompactionBlobEncoder.StartOffset = BlobEncoder.StartOffset;
    }

    bool newHeadIsInitialized = InitNewHeadForCompaction();

    TProcessParametersBase parameters;
    parameters.CurOffset = CompactionBlobEncoder.EndOffset;
    parameters.HeadCleared = (CompactionBlobEncoder.Head.PackedSize == 0);

    auto compactionRequest = MakeHolder<TEvKeyValue::TEvRequest>();
    compactionRequest->Record.SetCookie(ERequestCookie::WriteBlobsForCompaction);

    AFL_ENSURE(CompactionBlobEncoder.NewHead.GetBatches().empty());

    TInstant blobCreationUnixTime = TInstant::Zero();
    for (size_t i = 0; i < KeysForCompaction.size(); ++i) {
        auto& [k, pos] = KeysForCompaction[i];
        bool needToCompactHead = (parameters.CurOffset < k.Key.GetOffset());

        YDB_LOG_DEBUG_COMP(Service, "Key[ /",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"i", i},
            {"keysForCompactionSize", KeysForCompaction.size()},
            {"key", k.Key});

        if (pos == Max<size_t>()) {
            // большой блоб надо переименовать
            YDB_LOG_INFO_COMP(NKikimrServices::PQ_KV_OPS, "Rename key",
                {"LOGPREFIXINT", LOG_PREFIX_INT},
                {"key", k.Key});

            if (!WasTheLastBlobBig) {
                needToCompactHead = true;
            }
            YDB_LOG_DEBUG_COMP(Service, "Need to compact head",
                {"logPrefix", NPQ_LOG_PREFIX},
                {"needToCompactHead", needToCompactHead});

            parameters.CurOffset = k.Key.GetOffset();

            RenameCompactedBlob(k, k.Size,
                                needToCompactHead,
                                newHeadIsInitialized,
                                parameters,
                                compactionRequest.Get());

            CompactionBlobEncoder.ClearPartitionedBlob(Partition, MaxBlobSize);

            CompactionBlobEncoder.NewHead.Clear();
            CompactionBlobEncoder.NewHead.Offset = parameters.CurOffset;

            parameters.HeadCleared = true;
            WasTheLastBlobBig = true;
            FirstCompactionPart = Nothing();
        } else {
            // маленький блоб надо дописать
            YDB_LOG_INFO_COMP(NKikimrServices::PQ_KV_OPS, "Append blob for key",
                {"LOGPREFIXINT", LOG_PREFIX_INT},
                {"key", k.Key});
            YDB_LOG_DEBUG_COMP(Service, "Need to compact head",
                {"logPrefix", NPQ_LOG_PREFIX},
                {"needToCompactHead", needToCompactHead});

            const TRequestedBlob& requestedBlob = blobs[pos];
            if (!CompactRequestedBlob(requestedBlob, parameters, needToCompactHead, compactionRequest.Get(), blobCreationUnixTime, WasTheLastBlobBig, newHeadIsInitialized)) {
                YDB_LOG_DEBUG_COMP(Service, "Can't append blob for key",
                    {"logPrefix", NPQ_LOG_PREFIX},
                    {"key", k.Key});
                Y_FAIL("Something went wrong");
                return;
            }

            WasTheLastBlobBig = false;
        }
    }

    if (!CompactionBlobEncoder.IsLastBatchPacked()) {
        CompactionBlobEncoder.PackLastBatch();
    }

    CompactionBlobEncoder.HeadCleared = parameters.HeadCleared;

    EndProcessWritesForCompaction(compactionRequest.Get(), blobCreationUnixTime, ctx);

    if (CompactionBlobEncoder.PartitionedBlob.IsInited() && !CompactionBlobEncoder.PartitionedBlob.GetClientBlobs().empty()) {
        // В PartitionedBlob может лежать начало сообщения. Надо оставить последний ключ,
        // чтобы на следующей итерации добавить его к голове
        KeysForCompaction.pop_back();
    } else {
        FirstCompactionPart = Nothing();
    }

    YDB_LOG_INFO_COMP(NKikimrServices::PQ_KV_OPS, "Send request to KV",
        {"LOGPREFIXINT", LOG_PREFIX_INT});
    // for debugging purposes
    //DumpKeyValueRequest(compactionRequest->Record);

    ctx.Send(BlobCache, compactionRequest.Release(), 0, 0);
}

void TPartition::BlobsForCompactionWereWrite()
{
    const auto& ctx = ActorContext();

    YDB_LOG_INFO_COMP(NKikimrServices::PQ_KV_OPS, "Blobs compaction is completed",
        {"LOGPREFIXINT", LOG_PREFIX_INT});

    AFL_ENSURE(CompactionInProgress);
    AFL_ENSURE(BlobEncoder.DataKeysBody.size() >= KeysForCompaction.size());

    for (size_t i = 0; i < KeysForCompaction.size(); ++i) {
        BlobEncoder.pop_front();

        if (BlobEncoder.DataKeysBody.empty()) {
            if (BlobEncoder.HeadKeys.empty()) {
                BlobEncoder.StartOffset = BlobEncoder.EndOffset;
            } else {
                BlobEncoder.StartOffset = BlobEncoder.HeadKeys.front().Key.GetOffset();
            }
        } else {
            BlobEncoder.StartOffset = BlobEncoder.DataKeysBody.front().Key.GetOffset();
        }
    }

    CompactionBlobEncoder.SyncHeadKeys();
    CompactionBlobEncoder.SyncNewHeadKey();

    AFL_ENSURE(CompactionBlobEncoder.EndOffset == CompactionBlobEncoder.Head.GetNextOffset())
        ("EndOffset", CompactionBlobEncoder.EndOffset)
        ("NextOffset", CompactionBlobEncoder.Head.GetNextOffset());

    if (!CompactionBlobEncoder.CompactedKeys.empty() || CompactionBlobEncoder.Head.PackedSize == 0) { //has compactedkeys or head is already empty
        CompactionBlobEncoder.SyncHeadFromNewHead();
    }

    CompactionBlobEncoder.SyncDataKeysBody(ctx.Now(),
                                           [this](const TString& key){ return MakeBlobKeyToken(key); },
                                           CompactionBlobEncoder.StartOffset,
                                           GapOffsets,
                                           GapSize);
    CompactionBlobEncoder.SyncHead(CompactionBlobEncoder.StartOffset, CompactionBlobEncoder.EndOffset);
    CompactionBlobEncoder.ResetNewHead(CompactionBlobEncoder.EndOffset);
    CompactionBlobEncoder.CheckHeadConsistency(CompactLevelBorder, TotalLevels, TotalMaxCount);

    CompactionInProgress = false;
    KeysForCompaction.clear();
    CompactionBlobsCount = 0;

    TryProcessGetWriteInfoRequest(ctx);

    ProcessTxsAndUserActs(ctx); // Now you can delete unnecessary keys.
    TryRunCompaction();
}

void TPartition::TryProcessGetWriteInfoRequest(const TActorContext& ctx)
{
    if (PendingGetWriteInfoRequest) {
        ProcessPendingEvent(std::move(PendingGetWriteInfoRequest), ctx);
        PendingGetWriteInfoRequest = nullptr;
    }
}

void TPartition::EndProcessWritesForCompaction(TEvKeyValue::TEvRequest* request, const TInstant blobCreationUnixTime, const TActorContext& ctx)
{
    if (CompactionBlobEncoder.HeadCleared) {
        AFL_ENSURE(!CompactionBlobEncoder.CompactedKeys.empty() || CompactionBlobEncoder.Head.PackedSize == 0)
            ("CompactedKeys.size", CompactionBlobEncoder.CompactedKeys.size())
            ("Head.Offset", CompactionBlobEncoder.Head.Offset)
            ("Head.PartNo", CompactionBlobEncoder.Head.PartNo)
            ("Head.PackedSize", CompactionBlobEncoder.Head.PackedSize);
        for (ui32 i = 0; i < TotalLevels; ++i) {
            CompactionBlobEncoder.DataKeysHead[i].Clear();
        }
    }

    if (CompactionBlobEncoder.NewHead.PackedSize == 0) { //nothing added to head - just compaction or tmp part blobs writed
        CompactionBlobEncoder.HaveData =
            request->Record.CmdWriteSize() > 0
            || request->Record.CmdRenameSize() > 0
            || request->Record.CmdDeleteRangeSize() > 0;
        return;
    }

    std::pair<TKey, ui32> res = GetNewCompactionWriteKey(CompactionBlobEncoder.HeadCleared);
    const auto& key = res.first;

    AFL_ENSURE(!key.HasSuffix() || key.IsHead()); // body or head

    YDB_LOG_DEBUG_COMP(Service, "Add new write blob: topic partition compactOffset HeadOffset endOffset curOffset size WTime",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"topicName", TopicName()},
        {"partition", Partition},
        {"offset", key.GetOffset()},
        {"count", key.GetCount()},
        {"compactionBlobEncoderHeadOffset", CompactionBlobEncoder.Head.Offset},
        {"compactionBlobEncoderEndOffset", CompactionBlobEncoder.EndOffset},
        {"compactionBlobEncoderNewHeadNextOffset", CompactionBlobEncoder.NewHead.GetNextOffset()},
        {"key", key},
        {"second", res.second},
        {"ctxNowMilliSeconds", ctx.Now().MilliSeconds()});
    AddNewCompactionWriteBlob(res, request, blobCreationUnixTime, ctx);

    CompactionBlobEncoder.HaveData = true;
}

std::pair<TKey, ui32> TPartition::GetNewCompactionWriteKeyImpl(const bool headCleared, const bool needCompaction, const ui32 headSize)
{
    TKey key = CompactionBlobEncoder.KeyForWrite(TKeyPrefix::TypeData, Partition, needCompaction);

    if (CompactionBlobEncoder.NewHead.PackedSize > 0) {
        AFL_ENSURE(CompactionBlobEncoder.DataKeysHead.size() == TotalLevels);
        CompactionBlobEncoder.DataKeysHead[TotalLevels - 1].AddKey(key, CompactionBlobEncoder.NewHead.PackedSize);
    }
    AFL_ENSURE(headSize + CompactionBlobEncoder.NewHead.PackedSize <= 3 * MaxSizeCheck);

    std::pair<TKey, ui32> res;

    if (needCompaction) { //compact all
        for (ui32 i = 0; i < TotalLevels; ++i) {
            CompactionBlobEncoder.DataKeysHead[i].Clear();
        }
        if (!headCleared) { //compacted blob must contain both head and NewHead
            key = TKey::ForBody(TKeyPrefix::TypeData,
                                Partition,
                                CompactionBlobEncoder.Head.Offset,
                                CompactionBlobEncoder.Head.PartNo,
                                CompactionBlobEncoder.NewHead.GetCount() + CompactionBlobEncoder.Head.GetCount(),
                                CompactionBlobEncoder.Head.GetInternalPartsCount() +  CompactionBlobEncoder.NewHead.GetInternalPartsCount());
            if (HasAppData() && AppData()->FeatureFlags.GetEnableTopicWriteOffsetDeltaInKeys()) {
                ui64 offsetDelta = 0;
                if (!CompactionBlobEncoder.Head.GetBatches().empty()) {
                    offsetDelta += CompactionBlobEncoder.Head.GetOffsetDelta();
                }
                if (!CompactionBlobEncoder.NewHead.GetBatches().empty()) {
                    offsetDelta += CompactionBlobEncoder.NewHead.GetOffsetDelta();
                }
                if (offsetDelta > 0) {
                    key.SetOffsetDelta(offsetDelta);
                }
            }
        } //otherwise KV blob is not from head (!key.HasSuffix()) and contains only new data from NewHead
        res = std::make_pair(key, headSize + CompactionBlobEncoder.NewHead.PackedSize);
    } else {
        res = CompactionBlobEncoder.Compact(key, headCleared);
        AFL_ENSURE(res.first.IsHead()) //may compact some KV blobs from head, but new KV blob is from head too
            ("res.first", res.first.ToString());
        AFL_ENSURE(res.second >= CompactionBlobEncoder.NewHead.PackedSize) //at least new data must be writed
            ("res.second", res.second)
            ("NewHead.PackedSize", CompactionBlobEncoder.NewHead.PackedSize);
    }
    AFL_ENSURE(res.second <= MaxBlobSize)
        ("headCleared", headCleared)
        ("needCompaction", needCompaction)
        ("headSize", headSize)
        ("Head.PackedSize", CompactionBlobEncoder.Head.PackedSize)
        ("NewHead.PackedSize", CompactionBlobEncoder.NewHead.PackedSize)
        ("key", res.first.ToString())
        ("size", res.second)
        ("MaxBlobSize", MaxBlobSize)
        ("MaxSizeCheck", MaxSizeCheck);

    return res;
}

std::pair<TKey, ui32> TPartition::GetNewCompactionWriteKey(const bool headCleared)
{
    bool needCompaction = false;
    ui32 headSize = headCleared ? 0 : CompactionBlobEncoder.Head.PackedSize;
    if (headSize + CompactionBlobEncoder.NewHead.PackedSize > 0 &&
        headSize + CompactionBlobEncoder.NewHead.PackedSize >= Min<ui32>(MaxBlobSize, Config.GetPartitionConfig().GetLowWatermark())) {
        needCompaction = true;
    }

    if (CompactionBlobEncoder.PartitionedBlob.IsInited()) { //has active partitioned blob - compaction is forbiden, head and newHead will be compacted when this partitioned blob is finished
        needCompaction = false;
    }

    AFL_ENSURE(CompactionBlobEncoder.NewHead.PackedSize > 0 || needCompaction); //smthing must be here

    return GetNewCompactionWriteKeyImpl(headCleared, needCompaction, headSize);
}

void TPartition::AddNewCompactionWriteBlob(std::pair<TKey, ui32>& res, TEvKeyValue::TEvRequest* request, const TInstant blobCreationUnixTime, const TActorContext& ctx)
{
    const auto& key = res.first;

    TInstant compactionWriteTimestamp;
    TString valueD = CompactionBlobEncoder.SerializeForKey(key, res.second, CompactionBlobEncoder.EndOffset, compactionWriteTimestamp);
    auto write = request->Record.AddCmdWrite();
    write->SetKey(key.Data(), key.Size());
    write->SetValue(valueD);
    write->SetCreationUnixTime(blobCreationUnixTime.Seconds());  // note: The time is rounded to second precision.
    // Y_ASSERT(blobCreationUnixTime.Seconds() > 0); TODO: move fake time in tests forward and enable checking

    bool isInline = key.IsHead() && valueD.size() < MAX_INLINE_SIZE;

    if (isInline) {
        write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
    } else {
        auto channel = GetChannel(NextChannel(key.IsHead(), valueD.size()));
        write->SetStorageChannel(channel);
        write->SetTactic(AppData(ctx)->PQConfig.GetTactic());
    }

    if (!key.IsHead()) {
        if (!CompactionBlobEncoder.DataKeysBody.empty() && CompactionBlobEncoder.CompactedKeys.empty()) {
            AFL_ENSURE(CompactionBlobEncoder.DataKeysBody.back().Key.GetOffset() + CompactionBlobEncoder.DataKeysBody.back().Key.GetCount() <= key.GetOffset())
                ("LAST KEY", CompactionBlobEncoder.DataKeysBody.back().Key.ToString())
                ("HeadOffset", CompactionBlobEncoder.Head.Offset)
                ("NEW KEY", key.ToString());
        }

        CompactionBlobEncoder.CompactedKeys.push_back(res);

        // CompactionBlobEncoder.ResetNewHead ???
        CompactionBlobEncoder.NewHead.Clear();
        CompactionBlobEncoder.NewHead.Offset = res.first.GetOffset() + res.first.GetCount();
        CompactionBlobEncoder.NewHead.PartNo = 0;
    } else {
        AFL_ENSURE(CompactionBlobEncoder.NewHeadKey.Size == 0);
        CompactionBlobEncoder.NewHeadKey = {key, res.second, compactionWriteTimestamp, 0, MakeBlobKeyToken(key.ToString())};
    }
}

bool TPartition::ThereIsUncompactedData() const
{
    if (CompactionInProgress) {
        return true;
    }

    return
        (BlobEncoder.DataKeysBody.size() >= GetBodyKeysCountLimit()) ||
        (BlobEncoder.BodySize >= GetCumulativeSizeLimit());
}

TInstant TPartition::GetFirstUncompactedBlobTimestamp() const
{
    const auto& ctx = ActorContext();

    if (BlobEncoder.DataKeysBody.empty()) {
        return ctx.Now();
    }
    if (BlobEncoder.DataKeysBody.size() <= GetBodyKeysCountLimit()) {
        return BlobEncoder.DataKeysBody.front().Timestamp;
    }

    return BlobEncoder.DataKeysBody[GetBodyKeysCountLimit()].Timestamp;
}


void TPartition::CheckTimestampsOrderInZones(TStringBuf validateReason) const {
    TInstant prev = TInstant::Zero();
    size_t pos = 0;
    auto check = [&](const auto& seq, TStringBuf zoneName) {
        size_t in_zone_pos = 0;
        for (const TDataKey& k : seq) {
            const auto curr = k.Timestamp;
            const bool disorder = (curr < prev);
            PQ_ENSURE(!disorder)
                ("prev_tmestamp", prev.MicroSeconds())
                ("curr_timestamp", curr.MicroSeconds())
                ("zone", zoneName)
                ("offset", k.Key.GetOffset())
                ("part_no", k.Key.GetPartNo())
                ("pos", pos)
                ("in_zone_pos", in_zone_pos)
                ("validate_reason", validateReason);
           prev = curr;
            ++pos;
            ++in_zone_pos;
        }
    };
    check(CompactionBlobEncoder.DataKeysBody, "compacted_body");
    check(CompactionBlobEncoder.HeadKeys, "compacted_head");
    check(BlobEncoder.DataKeysBody, "fastwrite_body");
}

void TPartition::InitFirstCompactionPart()
{
    if (CompactionBlobEncoder.HeadKeys.empty()) {
        return;
    }
    CompactionBlobEncoder.Head.MutableLastBatch().Unpack();
    const auto& batch = CompactionBlobEncoder.Head.GetLastBatch();
    FirstCompactionPart = std::make_pair(batch.GetOffset(), batch.Blobs.back().GetPartNo());
    CompactionBlobEncoder.Head.MutableLastBatch().Pack();
}

}
