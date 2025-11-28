#include "partition.h"
#include <ydb/core/persqueue/pqtablet/common/logging.h>
#include "partition_util.h"
#include <util/string/escape.h>

#define LOG_PREFIX_INT TStringBuilder() << "[" << TabletId << "]" << GetLogPrefix()
#define PQBC_LOG_I(stream) LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::PQ_KV_OPS, LOG_PREFIX_INT << stream)

namespace NKikimr::NPQ {

bool TPartition::ExecRequestForCompaction(TWriteMsg& p, TProcessParametersBase& parameters, TEvKeyValue::TEvRequest* request, const TInstant blobCreationUnixTime)
{
    const auto& ctx = ActorContext();
    ui64& curOffset = parameters.CurOffset;
    ui64 poffset = p.Offset ? *p.Offset : curOffset;

    LOG_T("Topic '" << TopicName() << "' partition " << Partition
            << " process write for '" << EscapeC(p.Msg.SourceId) << "'"
            << " DisableDeduplication=" << p.Msg.DisableDeduplication
            << " SeqNo=" << p.Msg.SeqNo
            << " InitialSeqNo=" << p.InitialSeqNo
    );

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

    LOG_D("Topic '" << TopicName() << "' partition " << Partition
            << " part blob processing sourceId '" << EscapeC(p.Msg.SourceId)
            << "' seqNo " << p.Msg.SeqNo << " partNo " << p.Msg.PartNo
    );

    TString s;
    if (!CompactionBlobEncoder.PartitionedBlob.IsNextPart(p.Msg.SourceId, p.Msg.SeqNo, p.Msg.PartNo, &s)) {
        //this must not be happen - client sends gaps, fail this client till the end
        //now no changes will leak
        LOG_D("Can't append message part (" << s << ")");
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
                        p.Msg.UncompressedSize, std::move(p.Msg.PartitionKey), std::move(p.Msg.ExplicitHashKey)); //remove curOffset when LB will report CTime

    bool lastBlobPart = blob.IsLastPart();

    //will return compacted tmp blob
    auto newWrite = CompactionBlobEncoder.PartitionedBlob.Add(std::move(blob));

    if (newWrite && !newWrite->Value.empty()) {
        AddCmdWrite(newWrite, request, blobCreationUnixTime, ctx, false);

        LOG_D("Topic '" << TopicName() <<
                "' partition " << Partition <<
                " part blob sourceId '" << EscapeC(p.Msg.SourceId) <<
                "' seqNo " << p.Msg.SeqNo << " partNo " << p.Msg.PartNo <<
                " result is " << newWrite->Key.ToString() <<
                " size " << newWrite->Value.size()
        );

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

        LOG_D("Topic '" << TopicName() << "' partition " << Partition
                << " part blob complete sourceId '" << EscapeC(p.Msg.SourceId) << "' seqNo " << p.Msg.SeqNo
                << " partNo " << p.Msg.PartNo << " FormedBlobsCount " << CompactionBlobEncoder.PartitionedBlob.GetFormedBlobs().size()
                << " NewHead: " << CompactionBlobEncoder.NewHead
        );

        ++curOffset;
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
    LOG_D("==== keys for blobs compaction ====");
    for (size_t i = 0; i < BlobEncoder.DataKeysBody.size(); ++i) {
        const auto& k = BlobEncoder.DataKeysBody[i];
        LOG_D(((k.Size >= GetCompactedBlobSizeLowerBound()) ? 'R' : '*') << " " << k.Key.ToString() << " " << k.Size);
    }
    LOG_D("===================================");
}

void TPartition::TryRunCompaction(bool force)
{
    if (StopCompaction) {
        LOG_D("Blobs compaction is stopped");
        return;
    }

    if (CompactionInProgress) {
        LOG_D("Blobs compaction in progress");
        return;
    }

    if (BlobEncoder.DataKeysBody.empty()) {
        LOG_D("No data for blobs compaction");
        return;
    }

    //DumpKeysForBlobsCompaction();

    const ui64 blobsKeyCountLimit = GetBodyKeysCountLimit();
    const ui64 compactedBlobSizeLowerBound = GetCompactedBlobSizeLowerBound();

    if ((BlobEncoder.DataKeysBody.size() < blobsKeyCountLimit) && (BlobEncoder.GetSize() < GetCumulativeSizeLimit()) && !force) {
        LOG_D("No data for blobs compaction");
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
            LOG_D("Blob key for append " << k.Key.ToString() << " (" << k.Size << ")");
        } else {
            LOG_D("Blob key for rename " << k.Key.ToString());
        }
    }

    LOG_D(blobsCount << " keys were taken away. Let's read " << blobsSize << " bytes");

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

    LOG_D("Begin compaction for " << blobsCount << " blobs");

    TVector<TRequestedBlob> blobs;
    TBlobKeyTokens tokens;

    KeysForCompaction.clear();
    for (size_t i = 0; i < blobsCount; ++i) {
        const auto& k = BlobEncoder.DataKeysBody[i];
        if (k.Size >= GetCompactedBlobSizeLowerBound()) {
            KeysForCompaction.emplace_back(k, Max<ui64>());
            continue;
        }

        LOG_D("Request blob key " << k.Key.ToString());

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

    LOG_D("Request " << CompactionBlobsCount << " blobs for compaction");
}

bool TPartition::CompactRequestedBlob(const TRequestedBlob& requestedBlob,
                                      TProcessParametersBase& parameters,
                                      bool needToCompactHead,
                                      TEvKeyValue::TEvRequest* compactionRequest,
                                      TInstant& blobCreationUnixTime,
                                      bool wasThePreviousBlobBig,
                                      bool& newHeadIsInitialized)
{
    LOG_D("requestedBlob.Key=" << requestedBlob.Key.ToString() << ", parameters.CurOffset=" << parameters.CurOffset);

    ui64 offset = requestedBlob.Key.GetOffset();

    for (TBlobIterator it(requestedBlob.Key, requestedBlob.Value); it.IsValid(); it.Next()) {
        TBatch batch = it.GetBatch();
        batch.Unpack();

        for (const auto& blob : batch.Blobs) {
            LOG_D("Try append part " << offset << "." << blob.GetPartNo() << "/" << blob.GetTotalParts());

            ui16 partNo = blob.GetPartNo();
            const auto offsetPartNo = std::make_pair(offset, partNo);
            if (blob.IsLastPart()) {
                ++offset;
            }

            if (FirstCompactionPart && (offsetPartNo <= *FirstCompactionPart)) {
                LOG_D("Part " << offsetPartNo.first << ":" << offsetPartNo.second << " skipped" <<
                      " (" << FirstCompactionPart->first << ":" << FirstCompactionPart->second << ")");
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
            }, std::nullopt};
            msg.Internal = true;

            blobCreationUnixTime = std::max(blobCreationUnixTime, blob.WriteTimestamp);
            if (!ExecRequestForCompaction(msg, parameters, compactionRequest, blobCreationUnixTime)) {
                LOG_D("Part " << offsetPartNo.first << ":" << offsetPartNo.second << " not appended");
                return false;
            }

            LOG_D("Part " << offsetPartNo.first << ":" << offsetPartNo.second << " appended");
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

    LOG_D("Continue blobs compaction");
    PQBC_LOG_I("Begin blobs compaction");

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

        LOG_D("key[" << i << "/" << KeysForCompaction.size() << "] " << k.Key.ToString());

        if (pos == Max<size_t>()) {
            // большой блоб надо переименовать
            PQBC_LOG_I("Rename key " << k.Key.ToString());

            if (!WasTheLastBlobBig) {
                needToCompactHead = true;
            }
            LOG_D("Need to compact head " << needToCompactHead);

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
            PQBC_LOG_I("Append blob for key " << k.Key.ToString());
            LOG_D("Need to compact head " << needToCompactHead);

            const TRequestedBlob& requestedBlob = blobs[pos];
            if (!CompactRequestedBlob(requestedBlob, parameters, needToCompactHead, compactionRequest.Get(), blobCreationUnixTime, WasTheLastBlobBig, newHeadIsInitialized)) {
                LOG_D("Can't append blob for key " << k.Key.ToString());
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

    PQBC_LOG_I("Send request to KV");
    // for debugging purposes
    //DumpKeyValueRequest(compactionRequest->Record);

    ctx.Send(BlobCache, compactionRequest.Release(), 0, 0);
}

void TPartition::BlobsForCompactionWereWrite()
{
    const auto& ctx = ActorContext();

    PQBC_LOG_I("Blobs compaction is completed");

    AFL_ENSURE(CompactionInProgress);
    AFL_ENSURE(BlobEncoder.DataKeysBody.size() >= KeysForCompaction.size());

    for (size_t i = 0; i < KeysForCompaction.size(); ++i) {
        BlobEncoder.BodySize -= BlobEncoder.DataKeysBody.front().Size;
        BlobEncoder.DataKeysBody.pop_front();

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

    LOG_D("Add new write blob: topic '" << TopicName() << "' partition " << Partition
            << " compactOffset " << key.GetOffset() << "," << key.GetCount()
            << " HeadOffset " << CompactionBlobEncoder.Head.Offset << " endOffset " << CompactionBlobEncoder.EndOffset << " curOffset "
            << CompactionBlobEncoder.NewHead.GetNextOffset() << " " << key.ToString()
            << " size " << res.second << " WTime " << ctx.Now().MilliSeconds()
    );
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
