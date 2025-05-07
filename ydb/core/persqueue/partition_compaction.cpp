#include "partition.h"
#include "partition_log.h"
#include "partition_util.h"
#include <util/string/escape.h>

namespace NKikimr::NPQ {

bool TPartition::ExecRequestForCompaction(TWriteMsg& p, TProcessParametersBase& parameters, TEvKeyValue::TEvRequest* request)
{
    const auto& ctx = ActorContext();

    ui64& curOffset = parameters.CurOffset;

    ui64 poffset = p.Offset ? *p.Offset : curOffset;

    PQ_LOG_T("Topic '" << TopicName() << "' partition " << Partition
            << " process write for '" << EscapeC(p.Msg.SourceId) << "'"
            << " DisableDeduplication=" << p.Msg.DisableDeduplication
            << " SeqNo=" << p.Msg.SeqNo
            << " InitialSeqNo=" << p.InitialSeqNo
    );

    Y_ABORT_UNLESS(poffset >= curOffset);

    bool needCompactHead = poffset > curOffset;
    if (needCompactHead) { //got gap
        Y_ABORT_UNLESS(p.Msg.PartNo == 0);
        curOffset = poffset;
    }

    if (p.Msg.PartNo == 0) { //create new PartitionedBlob
        if (CompactionZone.PartitionedBlob.HasFormedBlobs()) {
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
        CompactionZone.NewPartitionedBlob(Partition,
                                          curOffset,
                                          p.Msg.SourceId,
                                          p.Msg.SeqNo,
                                          p.Msg.TotalParts,
                                          p.Msg.TotalSize,
                                          parameters.HeadCleared,
                                          needCompactHead,
                                          MaxBlobSize);
    }

    PQ_LOG_D("Topic '" << TopicName() << "' partition " << Partition
            << " part blob processing sourceId '" << EscapeC(p.Msg.SourceId)
            << "' seqNo " << p.Msg.SeqNo << " partNo " << p.Msg.PartNo
    );

    TString s;
    if (!CompactionZone.PartitionedBlob.IsNextPart(p.Msg.SourceId, p.Msg.SeqNo, p.Msg.PartNo, &s)) {
        //this must not be happen - client sends gaps, fail this client till the end
        //now no changes will leak
        ctx.Send(Tablet, new TEvents::TEvPoisonPill());
        return false;
    }

    // Empty partition may will be filling from offset great than zero from mirror actor if source partition old and was clean by retantion time
    if (!CompactionZone.Head.GetCount() && !CompactionZone.NewHead.GetCount() && CompactionZone.DataKeysBody.empty() && CompactionZone.HeadKeys.empty() && p.Offset) {
        CompactionZone.StartOffset = *p.Offset;
    }

    TMaybe<TPartData> partData;
    if (p.Msg.TotalParts > 1) { //this is multi-part message
        partData = TPartData(p.Msg.PartNo, p.Msg.TotalParts, p.Msg.TotalSize);
    }
    WriteTimestamp = ctx.Now();
    WriteTimestampEstimate = p.Msg.WriteTimestamp > 0 ? TInstant::MilliSeconds(p.Msg.WriteTimestamp) : WriteTimestamp;
    TClientBlob blob(p.Msg.SourceId, p.Msg.SeqNo, std::move(p.Msg.Data), std::move(partData), WriteTimestampEstimate,
                        TInstant::MilliSeconds(p.Msg.CreateTimestamp == 0 ? curOffset : p.Msg.CreateTimestamp),
                        p.Msg.UncompressedSize, p.Msg.PartitionKey, p.Msg.ExplicitHashKey); //remove curOffset when LB will report CTime

    bool lastBlobPart = blob.IsLastPart();

    //will return compacted tmp blob
    auto newWrite = CompactionZone.PartitionedBlob.Add(std::move(blob));

    if (newWrite && !newWrite->Value.empty()) {
        AddCmdWrite(newWrite, request, ctx);

        PQ_LOG_D("Topic '" << TopicName() <<
                "' partition " << Partition <<
                " part blob sourceId '" << EscapeC(p.Msg.SourceId) <<
                "' seqNo " << p.Msg.SeqNo << " partNo " << p.Msg.PartNo <<
                " result is " << newWrite->Key.ToString() <<
                " size " << newWrite->Value.size()
        );
    }

    if (lastBlobPart) {
        Y_ABORT_UNLESS(CompactionZone.PartitionedBlob.IsComplete());
        ui32 curWrites = RenameTmpCmdWrites(request);
        Y_ABORT_UNLESS(curWrites <= CompactionZone.PartitionedBlob.GetFormedBlobs().size());
        RenameFormedBlobs(CompactionZone.PartitionedBlob.GetFormedBlobs(),
                          parameters,
                          curWrites,
                          request,
                          CompactionZone,
                          ctx);

        ui32 countOfLastParts = 0;
        for (auto& x : CompactionZone.PartitionedBlob.GetClientBlobs()) {
            if (CompactionZone.NewHead.GetBatches().empty() || CompactionZone.NewHead.GetLastBatch().Packed) {
                CompactionZone.NewHead.AddBatch(TBatch(curOffset, x.GetPartNo()));
                CompactionZone.NewHead.PackedSize += GetMaxHeaderSize(); //upper bound for packed size
            }

            if (x.IsLastPart()) {
                ++countOfLastParts;
            }

            Y_ABORT_UNLESS(!CompactionZone.NewHead.GetLastBatch().Packed);
            CompactionZone.NewHead.AddBlob(x);
            CompactionZone.NewHead.PackedSize += x.GetBlobSize();
            if (CompactionZone.NewHead.GetLastBatch().GetUnpackedSize() >= BATCH_UNPACK_SIZE_BORDER) {
                CompactionZone.PackLastBatch();
            }
        }

        Y_ABORT_UNLESS(countOfLastParts == 1);

        PQ_LOG_D("Topic '" << TopicName() << "' partition " << Partition
                << " part blob complete sourceId '" << EscapeC(p.Msg.SourceId) << "' seqNo " << p.Msg.SeqNo
                << " partNo " << p.Msg.PartNo << " FormedBlobsCount " << CompactionZone.PartitionedBlob.GetFormedBlobs().size()
                << " NewHead: " << CompactionZone.NewHead
        );

        ++curOffset;
        CompactionZone.ClearPartitionedBlob(Partition, MaxBlobSize);
    }

    return true;
}

// вынести в настройки
const size_t BodyKeysCountLimit = 100;

ui64 TPartition::GetCumulativeSizeLimit() const
{
    return 3 * MaxBlobSize;
}

void TPartition::TryRunCompaction()
{
    if (CompactionInProgress) {
        PQ_LOG_D("compaction in progress");
        return;
    }

    if (BlobEncoder.DataKeysBody.empty()) {
        PQ_LOG_D("no data for compaction");
        return;
    }

    ui64 cumulativeSize = BlobEncoder.DataKeysBody.back().CumulativeSize;
    cumulativeSize -= BlobEncoder.DataKeysBody.front().CumulativeSize;

    if (!CompactionZone.HeadKeys.empty()) {
        cumulativeSize += CompactionZone.HeadKeys.back().CumulativeSize;
        cumulativeSize -= CompactionZone.HeadKeys.front().CumulativeSize;
    }

    if ((cumulativeSize < GetCumulativeSizeLimit()) &&
        (BlobEncoder.DataKeysBody.size() < BodyKeysCountLimit)) {
        PQ_LOG_D("need more data for compaction. " <<
                 "CumulativeSize=" << BlobEncoder.DataKeysBody.back().CumulativeSize <<
                 ", Count=" << BlobEncoder.DataKeysBody.size());
        return;
    }

    CompactionInProgress = true;

    Send(SelfId(), new TEvPQ::TEvRunCompaction(MaxBlobSize, cumulativeSize));
}

void TPartition::Handle(TEvPQ::TEvRunCompaction::TPtr& ev)
{
    const ui64 cumulativeSize = ev->Get()->CumulativeSize;

    PQ_LOG_D("begin compaction for " << cumulativeSize << " bytes");

    ui32 size = 0;
    ui32 count = 0;
    TBlobKeyTokens tokens;
    const auto& front = BlobEncoder.DataKeysBody.front();

    auto blobs = BlobEncoder.GetBlobsFromBody(front.Key.GetOffset(), front.Key.GetPartNo(),
                                           BodyKeysCountLimit,
                                           Min(cumulativeSize, GetCumulativeSizeLimit()),
                                           count,
                                           size,
                                           0, // lastOffset
                                           &tokens);
    CompactionBlobsCount = blobs.size();
    auto request = MakeHolder<TEvPQ::TEvBlobRequest>(ERequestCookie::ReadBlobsForCompaction,
                                                     Partition,
                                                     std::move(blobs));
    Send(BlobCache, request.Release());

    PQ_LOG_D("request " << CompactionBlobsCount << " blobs for compaction");
}

void TPartition::BlobsForCompactionWereRead(const TVector<NPQ::TRequestedBlob>& blobs)
{
    const auto& ctx = ActorContext();

    PQ_LOG_D("continue compaction");

    Y_ABORT_UNLESS(CompactionInProgress);
    Y_ABORT_UNLESS(blobs.size() == CompactionBlobsCount);

    TProcessParametersBase parameters;
    parameters.CurOffset = CompactionZone.PartitionedBlob.IsInited() ? CompactionZone.PartitionedBlob.GetOffset() : CompactionZone.EndOffset;
    parameters.HeadCleared = (CompactionZone.Head.PackedSize == 0);

    CompactionZone.NewHead.Offset = CompactionZone.EndOffset;
    CompactionZone.NewHead.PartNo = 0; // ???
    CompactionZone.NewHead.PackedSize = 0;

    auto compactionRequest = MakeHolder<TEvKeyValue::TEvRequest>();
    compactionRequest->Record.SetCookie(ERequestCookie::WriteBlobsForCompaction);

    Y_ABORT_UNLESS(CompactionZone.NewHead.GetBatches().empty());

    for (const auto& requestedBlob : blobs) {
        //DBGTRACE_LOG("Key=" << requestedBlob.Key.ToString() <<
        //             ", Size=" << requestedBlob.Value.size() << " (" << requestedBlob.Size << ")" <<
        //             ", Offset=" << requestedBlob.Offset <<
        //             ", PartNo=" << requestedBlob.PartNo <<
        //             ", Count=" << requestedBlob.Count <<
        //             ", InternalPartsCount=" << requestedBlob.InternalPartsCount);

        for (TBlobIterator it(requestedBlob.Key, requestedBlob.Value); it.IsValid(); it.Next()) {
            TBatch batch = it.GetBatch();
            batch.Unpack();

            //DBGTRACE_LOG(batch.GetOffset() <<
            //             ", " << batch.GetPartNo() <<
            //             ", " << batch.GetCount() <<
            //             ", " << batch.GetInternalPartsCount() <<
            //             ", " << batch.Blobs.size());

            for (const auto& blob : batch.Blobs) {
                //DBGTRACE_LOG(blob.SourceId <<
                //             ", " << blob.SeqNo <<
                //             ", " << blob.Data.size() <<
                //             ", " << blob.CreateTimestamp <<
                //             ", " << blob.WriteTimestamp <<
                //             ", " << blob.UncompressedSize);

                TWriteMsg msg{Max<ui64>(), Nothing(), TEvPQ::TEvWrite::TMsg{
                    .SourceId = blob.SourceId,
                    .SeqNo = blob.SeqNo,
                    .PartNo = (ui16)(blob.PartData ? blob.PartData->PartNo : 0),
                    .TotalParts = (ui16)(blob.PartData ? blob.PartData->TotalParts : 1),
                    .TotalSize = (ui32)(blob.PartData ? blob.PartData->TotalSize : blob.UncompressedSize),
                    .CreateTimestamp = blob.CreateTimestamp.MilliSeconds(),
                    .ReceiveTimestamp = blob.CreateTimestamp.MilliSeconds(),
                    .DisableDeduplication = false,
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

                ExecRequestForCompaction(msg, parameters, compactionRequest.Get());
            }
        }
    }

    if (!CompactionZone.IsLastBatchPacked()) {
        CompactionZone.PackLastBatch();
    }

    EndProcessWritesForCompaction(compactionRequest.Get(), ctx);

    DumpKeyValueRequest(compactionRequest->Record);
    ctx.Send(BlobCache, compactionRequest.Release(), 0, 0);
}

void TPartition::BlobsForCompactionWereWrite()
{
    const auto& ctx = ActorContext();

    PQ_LOG_D("compaction completed");

    Y_ABORT_UNLESS(CompactionInProgress);
    Y_ABORT_UNLESS(BlobEncoder.DataKeysBody.size() >= CompactionBlobsCount);

    for (size_t i = 0; i < CompactionBlobsCount; ++i) {
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

    CompactionZone.SyncHeadKeys();
    CompactionZone.SyncNewHeadKey();

    Y_ABORT_UNLESS(CompactionZone.EndOffset == CompactionZone.Head.GetNextOffset());

    if (!CompactionZone.CompactedKeys.empty() || CompactionZone.Head.PackedSize == 0) { //has compactedkeys or head is already empty
        CompactionZone.SyncHeadFromNewHead();
    }

    CompactionZone.SyncDataKeysBody(ctx.Now(),
                                    [this](const TString& key){ return MakeBlobKeyToken(key); },
                                    CompactionZone.StartOffset,
                                    GapOffsets,
                                    GapSize);
    CompactionZone.SyncHead(CompactionZone.StartOffset, CompactionZone.EndOffset);
    CompactionZone.ResetNewHead(CompactionZone.EndOffset);
    CompactionZone.CheckHeadConsistency(CompactLevelBorder, TotalLevels, TotalMaxCount);

    CompactionInProgress = false;
    CompactionBlobsCount = 0;

    ProcessTxsAndUserActs(ctx); // надо удалить старые ключи
    TryRunCompaction();
}

void TPartition::EndProcessWritesForCompaction(TEvKeyValue::TEvRequest* request, const TActorContext& ctx)
{
    if (HeadCleared) {
        Y_ABORT_UNLESS(!CompactionZone.CompactedKeys.empty() || CompactionZone.Head.PackedSize == 0);
        for (ui32 i = 0; i < TotalLevels; ++i) {
            CompactionZone.DataKeysHead[i].Clear();
        }
    }

    if (CompactionZone.NewHead.PackedSize == 0) { //nothing added to head - just compaction or tmp part blobs writed
        HaveData =
            request->Record.CmdWriteSize() > 0
            || request->Record.CmdRenameSize() > 0
            || request->Record.CmdDeleteRangeSize() > 0;
        return;
    }

    std::pair<TKey, ui32> res = GetNewCompactionWriteKey(HeadCleared);
    const auto& key = res.first;

    Y_ABORT_UNLESS(!key.HasSuffix() || key.IsHead()); // body or head

    PQ_LOG_D("Add new write blob: topic '" << TopicName() << "' partition " << Partition
            << " compactOffset " << key.GetOffset() << "," << key.GetCount()
            << " HeadOffset " << CompactionZone.Head.Offset << " endOffset " << CompactionZone.EndOffset << " curOffset "
            << CompactionZone.NewHead.GetNextOffset() << " " << key.ToString()
            << " size " << res.second << " WTime " << ctx.Now().MilliSeconds()
    );
    AddNewCompactionWriteBlob(res, request, ctx);

    HaveData = true;
}

std::pair<TKey, ui32> TPartition::GetNewCompactionWriteKeyImpl(bool headCleared, bool needCompaction, ui32 headSize)
{
    TKey key = CompactionZone.KeyForWrite(TKeyPrefix::TypeData, Partition, needCompaction);

    if (CompactionZone.NewHead.PackedSize > 0) {
        Y_ABORT_UNLESS(CompactionZone.DataKeysHead.size() == TotalLevels);
        CompactionZone.DataKeysHead[TotalLevels - 1].AddKey(key, CompactionZone.NewHead.PackedSize);
    }
    Y_ABORT_UNLESS(headSize + CompactionZone.NewHead.PackedSize <= 3 * MaxSizeCheck);

    std::pair<TKey, ui32> res;

    if (needCompaction) { //compact all
        for (ui32 i = 0; i < TotalLevels; ++i) {
            CompactionZone.DataKeysHead[i].Clear();
        }
        if (!headCleared) { //compacted blob must contain both head and NewHead
            key = TKey::ForBody(TKeyPrefix::TypeData,
                                Partition,
                                CompactionZone.Head.Offset,
                                CompactionZone.Head.PartNo,
                                CompactionZone.NewHead.GetCount() + CompactionZone.Head.GetCount(),
                                CompactionZone.Head.GetInternalPartsCount() +  CompactionZone.NewHead.GetInternalPartsCount());
        } //otherwise KV blob is not from head (!key.HasSuffix()) and contains only new data from NewHead
        res = std::make_pair(key, headSize + CompactionZone.NewHead.PackedSize);
    } else {
        res = CompactionZone.Compact(key, headCleared);
        Y_ABORT_UNLESS(res.first.IsHead());//may compact some KV blobs from head, but new KV blob is from head too
        Y_ABORT_UNLESS(res.second >= CompactionZone.NewHead.PackedSize); //at least new data must be writed
    }
    Y_ABORT_UNLESS(res.second <= MaxBlobSize);

    return res;
}

std::pair<TKey, ui32> TPartition::GetNewCompactionWriteKey(bool headCleared)
{
    bool needCompaction = false;
    ui32 headSize = headCleared ? 0 : CompactionZone.Head.PackedSize;
    if (headSize + CompactionZone.NewHead.PackedSize > 0 &&
        headSize + CompactionZone.NewHead.PackedSize >= Min<ui32>(MaxBlobSize, Config.GetPartitionConfig().GetLowWatermark())) {
        needCompaction = true;
    }

    if (CompactionZone.PartitionedBlob.IsInited()) { //has active partitioned blob - compaction is forbiden, head and newHead will be compacted when this partitioned blob is finished
        needCompaction = false;
    }

    Y_ABORT_UNLESS(CompactionZone.NewHead.PackedSize > 0 || needCompaction); //smthing must be here

    return GetNewCompactionWriteKeyImpl(headCleared, needCompaction, headSize);
}

void TPartition::AddNewCompactionWriteBlob(std::pair<TKey, ui32>& res, TEvKeyValue::TEvRequest* request, const TActorContext& ctx)
{
    const auto& key = res.first;

    TString valueD = CompactionZone.SerializeForKey(key, res.second, CompactionZone.EndOffset, PendingWriteTimestamp);

    auto write = request->Record.AddCmdWrite();
    write->SetKey(key.Data(), key.Size());
    write->SetValue(valueD);

    bool isInline = key.IsHead() && valueD.size() < MAX_INLINE_SIZE;

    if (isInline) {
        write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
    } else {
        auto channel = GetChannel(NextChannel(key.IsHead(), valueD.size()));
        write->SetStorageChannel(channel);
        write->SetTactic(AppData(ctx)->PQConfig.GetTactic());
    }

    ////Need to clear all compacted blobs
    //const TKey& k = CompactionZone.CompactedKeys.empty() ? key : CompactionZone.CompactedKeys.front().first;
    //ClearOldHead(k.GetOffset(), k.GetPartNo()); // schedule to delete the keys from the head

    if (!key.IsHead()) {
        if (!CompactionZone.DataKeysBody.empty() && CompactionZone.CompactedKeys.empty()) {
            Y_ABORT_UNLESS(CompactionZone.DataKeysBody.back().Key.GetOffset() + CompactionZone.DataKeysBody.back().Key.GetCount() <= key.GetOffset(),
                           "LAST KEY %s, HeadOffset %lu, NEWKEY %s",
                           CompactionZone.DataKeysBody.back().Key.ToString().c_str(),
                           CompactionZone.Head.Offset,
                           key.ToString().c_str());
        }

        CompactionZone.CompactedKeys.push_back(res);

        // CompactionZone.ResetNewHead ???
        CompactionZone.NewHead.Clear();
        CompactionZone.NewHead.Offset = res.first.GetOffset() + res.first.GetCount();
        CompactionZone.NewHead.PartNo = 0;
    } else {
        Y_ABORT_UNLESS(CompactionZone.NewHeadKey.Size == 0);
        CompactionZone.NewHeadKey = {key, res.second, CurrentTimestamp, 0, MakeBlobKeyToken(key.ToString())};
    }
}

}
