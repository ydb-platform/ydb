#include "partition.h"
#include "partition_log.h"
#include "partition_util.h"
#include <util/string/escape.h>
//#include <ydb/library/dbgtrace/debug_trace.h>

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

    PQ_LOG_D("Topic '" << TopicName() << "' partition " << Partition
            << " part blob processing sourceId '" << EscapeC(p.Msg.SourceId)
            << "' seqNo " << p.Msg.SeqNo << " partNo " << p.Msg.PartNo
    );

    TString s;
    if (!CompactionBlobEncoder.PartitionedBlob.IsNextPart(p.Msg.SourceId, p.Msg.SeqNo, p.Msg.PartNo, &s)) {
        //this must not be happen - client sends gaps, fail this client till the end
        //now no changes will leak
        ctx.Send(Tablet, new TEvents::TEvPoisonPill());
        return false;
    }

    // Empty partition may will be filling from offset great than zero from mirror actor if source partition old and was clean by retantion time
    if (!CompactionBlobEncoder.Head.GetCount() && !CompactionBlobEncoder.NewHead.GetCount() && CompactionBlobEncoder.DataKeysBody.empty() && CompactionBlobEncoder.HeadKeys.empty() && p.Offset) {
        CompactionBlobEncoder.StartOffset = *p.Offset;
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
    auto newWrite = CompactionBlobEncoder.PartitionedBlob.Add(std::move(blob));

    if (newWrite && !newWrite->Value.empty()) {
        //DBGTRACE_LOG("add new write " << newWrite->Key.ToString());
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
        Y_ABORT_UNLESS(CompactionBlobEncoder.PartitionedBlob.IsComplete());
        ui32 curWrites = RenameTmpCmdWrites(request);
        Y_ABORT_UNLESS(curWrites <= CompactionBlobEncoder.PartitionedBlob.GetFormedBlobs().size());
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

            Y_ABORT_UNLESS(!CompactionBlobEncoder.NewHead.GetLastBatch().Packed);
            CompactionBlobEncoder.NewHead.AddBlob(x);
            CompactionBlobEncoder.NewHead.PackedSize += x.GetBlobSize();
            if (CompactionBlobEncoder.NewHead.GetLastBatch().GetUnpackedSize() >= BATCH_UNPACK_SIZE_BORDER) {
                CompactionBlobEncoder.PackLastBatch();
            }
        }

        Y_ABORT_UNLESS(countOfLastParts == 1);

        PQ_LOG_D("Topic '" << TopicName() << "' partition " << Partition
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
    // Settings will be made later.
    return 300;
}

ui64 TPartition::GetCumulativeSizeLimit() const
{
    // Settings will be made later.
    return MaxBlobSize;
    //return 6_MB;
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

#if 1
    const ui64 cumulativeSize = BlobEncoder.BodySize;
    ////for (size_t i = 0; i < CompactionBlobEncoder.HeadKeys.size(); ++i) {
    ////    const auto& k = CompactionBlobEncoder.HeadKeys[i];

    ////    cumulativeSize += k.Size;
    ////}
    ////PQ_LOG_D("cumulativeSize=" << cumulativeSize);
    ////Y_ABORT_UNLESS(cumulativeSize <= MaxBlobSize,
    ////               "cumulativeSize=%" PRIu64,
    ////               cumulativeSize);
    //ui64 requestSize = 0;
    //for (size_t i = 0; (i < BlobEncoder.DataKeysBody.size()) && (cumulativeSize < GetCumulativeSizeLimit()); ++i) {
    //    const auto& k = BlobEncoder.DataKeysBody[i];

    //    cumulativeSize += k.Size;
    //    if (cumulativeSize > GetCumulativeSizeLimit()) {
    //        cumulativeSize -= k.Size;
    //        break;
    //    }
    //    requestSize += k.Size;
    //}
    //PQ_LOG_D("cumulativeSize=" << cumulativeSize << ", requestSize=" << requestSize);
#else
    ui64 cumulativeSize = BlobEncoder.DataKeysBody.back().CumulativeSize;
    cumulativeSize -= BlobEncoder.DataKeysBody.front().CumulativeSize;

    if (!CompactionBlobEncoder.HeadKeys.empty()) {
        cumulativeSize += CompactionBlobEncoder.HeadKeys.back().CumulativeSize;
        cumulativeSize -= CompactionBlobEncoder.HeadKeys.front().CumulativeSize;
    }
#endif

    if ((cumulativeSize < GetCumulativeSizeLimit()) &&
        (BlobEncoder.DataKeysBody.size() < GetBodyKeysCountLimit())) {
        PQ_LOG_D("need more data for compaction. " <<
                 //"cumulativeSize=" << cumulativeSize << ", requestSize=" << requestSize <<
                 "cumulativeSize=" << cumulativeSize <<
                 ", count=" << BlobEncoder.DataKeysBody.size());
        return;
    }

    PQ_LOG_D("need run compaction for " << cumulativeSize << " bytes in " << BlobEncoder.DataKeysBody.size() << " blobs");

    CompactionInProgress = true;

    //Send(SelfId(), new TEvPQ::TEvRunCompaction(MaxBlobSize, requestSize));
    Send(SelfId(), new TEvPQ::TEvRunCompaction(MaxBlobSize, cumulativeSize));
}

void TPartition::Handle(TEvPQ::TEvRunCompaction::TPtr& ev)
{
    const ui64 cumulativeSize = ev->Get()->CumulativeSize;

    PQ_LOG_D("begin compaction for " << cumulativeSize << " bytes in " << BlobEncoder.DataKeysBody.size() << " blobs");

    //DumpZones(__FILE__, __LINE__);

    ui32 size = 0;
    ui32 count = 0;
    TBlobKeyTokens tokens;
    const auto& front = BlobEncoder.DataKeysBody.front();

    auto blobs = BlobEncoder.GetBlobsFromBody(front.Key.GetOffset(), front.Key.GetPartNo(),
                                              Max<ui32>(),
                                              cumulativeSize,
                                              count,
                                              size,
                                              0, // lastOffset
                                              &tokens);
    PQ_LOG_D("count=" << count << ", size=" << size);
    for (const auto& b : blobs) {
        PQ_LOG_D("request key " << b.Key.ToString() << ", size " << b.Size);
    }
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
    parameters.CurOffset = CompactionBlobEncoder.PartitionedBlob.IsInited() ? CompactionBlobEncoder.PartitionedBlob.GetOffset() : CompactionBlobEncoder.EndOffset;
    parameters.HeadCleared = (CompactionBlobEncoder.Head.PackedSize == 0);

    CompactionBlobEncoder.NewHead.Offset = CompactionBlobEncoder.EndOffset;
    CompactionBlobEncoder.NewHead.PartNo = 0; // ???
    CompactionBlobEncoder.NewHead.PackedSize = 0;

    //DBGTRACE_LOG("CurOffset=" << parameters.CurOffset << ", NewHead.Offset=" << CompactionBlobEncoder.NewHead.Offset);

    auto compactionRequest = MakeHolder<TEvKeyValue::TEvRequest>();
    compactionRequest->Record.SetCookie(ERequestCookie::WriteBlobsForCompaction);

    Y_ABORT_UNLESS(CompactionBlobEncoder.NewHead.GetBatches().empty());

    //DumpZones(__FILE__, __LINE__);

    for (const auto& requestedBlob : blobs) {
        for (TBlobIterator it(requestedBlob.Key, requestedBlob.Value); it.IsValid(); it.Next()) {
            TBatch batch = it.GetBatch();
            batch.Unpack();

            for (const auto& blob : batch.Blobs) {
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
                //DumpZones(__FILE__, __LINE__);
            }
        }
    }

    //DumpZones(__FILE__, __LINE__);

    //DumpKeyValueRequest(compactionRequest->Record);

    if (!CompactionBlobEncoder.IsLastBatchPacked()) {
        CompactionBlobEncoder.PackLastBatch();
    }

    CompactionBlobEncoder.HeadCleared = parameters.HeadCleared;

    //DumpZones(__FILE__, __LINE__);

    EndProcessWritesForCompaction(compactionRequest.Get(), ctx);

    //DumpZones(__FILE__, __LINE__);
    //DumpKeyValueRequest(compactionRequest->Record);

    ctx.Send(BlobCache, compactionRequest.Release(), 0, 0);
}

void TPartition::BlobsForCompactionWereWrite()
{
    const auto& ctx = ActorContext();

    PQ_LOG_D("compaction completed");

    Y_ABORT_UNLESS(CompactionInProgress);
    Y_ABORT_UNLESS(BlobEncoder.DataKeysBody.size() >= CompactionBlobsCount);

    //DumpZones(__FILE__, __LINE__);

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

    //DumpZones(__FILE__, __LINE__);
    CompactionBlobEncoder.SyncHeadKeys();
    //DumpZones(__FILE__, __LINE__);
    CompactionBlobEncoder.SyncNewHeadKey();
    //DumpZones(__FILE__, __LINE__);

    Y_ABORT_UNLESS(CompactionBlobEncoder.EndOffset == CompactionBlobEncoder.Head.GetNextOffset());

    if (!CompactionBlobEncoder.CompactedKeys.empty() || CompactionBlobEncoder.Head.PackedSize == 0) { //has compactedkeys or head is already empty
        //DBGTRACE_LOG("need sync head from new head");
        //DumpZones(__FILE__, __LINE__);
        CompactionBlobEncoder.SyncHeadFromNewHead();
        //DumpZones(__FILE__, __LINE__);
    }

    CompactionBlobEncoder.SyncDataKeysBody(ctx.Now(),
                                           [this](const TString& key){ return MakeBlobKeyToken(key); },
                                           CompactionBlobEncoder.StartOffset,
                                           GapOffsets,
                                           GapSize);
    //DumpZones(__FILE__, __LINE__);
    CompactionBlobEncoder.SyncHead(CompactionBlobEncoder.StartOffset, CompactionBlobEncoder.EndOffset);
    //DumpZones(__FILE__, __LINE__);
    CompactionBlobEncoder.ResetNewHead(CompactionBlobEncoder.EndOffset);
    //DumpZones(__FILE__, __LINE__);
    CompactionBlobEncoder.CheckHeadConsistency(CompactLevelBorder, TotalLevels, TotalMaxCount);
    //DumpZones(__FILE__, __LINE__);

    CompactionInProgress = false;
    CompactionBlobsCount = 0;

    //DumpZones(__FILE__, __LINE__);

    ProcessTxsAndUserActs(ctx); // Now you can delete unnecessary keys.
    TryRunCompaction();
}

void TPartition::EndProcessWritesForCompaction(TEvKeyValue::TEvRequest* request, const TActorContext& ctx)
{
    if (CompactionBlobEncoder.HeadCleared) {
        Y_ABORT_UNLESS(!CompactionBlobEncoder.CompactedKeys.empty() || CompactionBlobEncoder.Head.PackedSize == 0,
                       "CompactedKeys.size=%" PRISZT
                       ", Head.Offset=%" PRIu64 ", Head.PartNo=%" PRIu16 ", Head.PackedSize=%" PRIu32,
                       CompactionBlobEncoder.CompactedKeys.size(),
                       CompactionBlobEncoder.Head.Offset, CompactionBlobEncoder.Head.PartNo, CompactionBlobEncoder.Head.PackedSize);
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

    Y_ABORT_UNLESS(!key.HasSuffix() || key.IsHead()); // body or head

    PQ_LOG_D("Add new write blob: topic '" << TopicName() << "' partition " << Partition
            << " compactOffset " << key.GetOffset() << "," << key.GetCount()
            << " HeadOffset " << CompactionBlobEncoder.Head.Offset << " endOffset " << CompactionBlobEncoder.EndOffset << " curOffset "
            << CompactionBlobEncoder.NewHead.GetNextOffset() << " " << key.ToString()
            << " size " << res.second << " WTime " << ctx.Now().MilliSeconds()
    );
    AddNewCompactionWriteBlob(res, request, ctx);

    CompactionBlobEncoder.HaveData = true;
}

std::pair<TKey, ui32> TPartition::GetNewCompactionWriteKeyImpl(const bool headCleared, const bool needCompaction, const ui32 headSize)
{
    TKey key = CompactionBlobEncoder.KeyForWrite(TKeyPrefix::TypeData, Partition, needCompaction);
    //DBGTRACE_LOG("key=" << key.ToString() << ", headCleared=" << headCleared << ", needCompaction=" << needCompaction << ", headSize=" << headSize);

    if (CompactionBlobEncoder.NewHead.PackedSize > 0) {
        Y_ABORT_UNLESS(CompactionBlobEncoder.DataKeysHead.size() == TotalLevels);
        CompactionBlobEncoder.DataKeysHead[TotalLevels - 1].AddKey(key, CompactionBlobEncoder.NewHead.PackedSize);
    }
    Y_ABORT_UNLESS(headSize + CompactionBlobEncoder.NewHead.PackedSize <= 3 * MaxSizeCheck);

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
            //DBGTRACE_LOG("key=" << key.ToString());
        } //otherwise KV blob is not from head (!key.HasSuffix()) and contains only new data from NewHead
        res = std::make_pair(key, headSize + CompactionBlobEncoder.NewHead.PackedSize);
    } else {
        res = CompactionBlobEncoder.Compact(key, headCleared);
        Y_ABORT_UNLESS(res.first.IsHead(),
                       "res.first=%s",
                       res.first.ToString().data());//may compact some KV blobs from head, but new KV blob is from head too
        Y_ABORT_UNLESS(res.second >= CompactionBlobEncoder.NewHead.PackedSize,
                       "res.second=%" PRIu32 ", NewHead.PackedSize=%" PRIu32,
                       res.second, CompactionBlobEncoder.NewHead.PackedSize); //at least new data must be writed
    }
    //if (!(res.second <= MaxBlobSize)) {
    //    DBGTRACE_LOG("headCleared=" << headCleared << ", needCompaction=" << needCompaction << ", headSize=" << headSize);
    //    DBGTRACE_LOG("key=" << key.ToString());
    //    DBGTRACE_LOG("res.first=" << res.first.ToString() << ", res.second=" << res.second);
    //    DBGTRACE_LOG("Head.PackedSize=" << CompactionBlobEncoder.Head.PackedSize << ", NewHead.PackedSize=" << CompactionBlobEncoder.NewHead.PackedSize);
    //    //DumpZones(__FILE__, __LINE__);
    //}
    Y_ABORT_UNLESS_S(res.second <= MaxBlobSize,
                     "headCleared=" << headCleared << ", needCompaction=" << needCompaction << ", headSize=" << headSize <<
                     ", Head.PackedSize=" << CompactionBlobEncoder.Head.PackedSize <<
                     ", NewHead.PackedSize=" << CompactionBlobEncoder.NewHead.PackedSize <<
                     ", key=" << res.first.ToString() << " (" << res.second << ")" <<
                     ", MaxBlobSize=" << MaxBlobSize << ", MaxSizeCheck=" << MaxSizeCheck);

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

    Y_ABORT_UNLESS(CompactionBlobEncoder.NewHead.PackedSize > 0 || needCompaction); //smthing must be here

    return GetNewCompactionWriteKeyImpl(headCleared, needCompaction, headSize);
}

void TPartition::AddNewCompactionWriteBlob(std::pair<TKey, ui32>& res, TEvKeyValue::TEvRequest* request, const TActorContext& ctx)
{
    const auto& key = res.first;

    TString valueD = CompactionBlobEncoder.SerializeForKey(key, res.second, CompactionBlobEncoder.EndOffset, PendingWriteTimestamp);

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

    if (!key.IsHead()) {
        if (!CompactionBlobEncoder.DataKeysBody.empty() && CompactionBlobEncoder.CompactedKeys.empty()) {
            Y_ABORT_UNLESS(CompactionBlobEncoder.DataKeysBody.back().Key.GetOffset() + CompactionBlobEncoder.DataKeysBody.back().Key.GetCount() <= key.GetOffset(),
                           "LAST KEY %s, HeadOffset %lu, NEWKEY %s",
                           CompactionBlobEncoder.DataKeysBody.back().Key.ToString().c_str(),
                           CompactionBlobEncoder.Head.Offset,
                           key.ToString().c_str());
        }

        //DBGTRACE_LOG("add key " << res.first.ToString());
        CompactionBlobEncoder.CompactedKeys.push_back(res);

        // CompactionBlobEncoder.ResetNewHead ???
        CompactionBlobEncoder.NewHead.Clear();
        CompactionBlobEncoder.NewHead.Offset = res.first.GetOffset() + res.first.GetCount();
        CompactionBlobEncoder.NewHead.PartNo = 0;
    } else {
        Y_ABORT_UNLESS(CompactionBlobEncoder.NewHeadKey.Size == 0);
        CompactionBlobEncoder.NewHeadKey = {key, res.second, CurrentTimestamp, 0, MakeBlobKeyToken(key.ToString())};
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

void TPartition::UpdateCompactionCounters()
{
    if (!InitDone) {
        return;
    }

    const auto& ctx = ActorContext();

    if (ThereIsUncompactedData()) {
        auto now = ctx.Now();
        auto begin = GetFirstUncompactedBlobTimestamp();

        CompactionTimeLag.Set((now - begin).MilliSeconds());

    } else {
        CompactionTimeLag.Set(0);
    }

    CompactionUnprocessedBytes.Set(BlobEncoder.BodySize);
    CompactionUnprocessedCount.Set(BlobEncoder.DataKeysBody.size());
}

TInstant TPartition::GetFirstUncompactedBlobTimestamp() const
{
    const auto& ctx = ActorContext();

    if (BlobEncoder.DataKeysBody.empty()) {
        return ctx.Now();
    }
    if (BlobEncoder.DataKeysBody.size() < GetBodyKeysCountLimit()) {
        return BlobEncoder.DataKeysBody.front().Timestamp;
    }

    return BlobEncoder.DataKeysBody[GetBodyKeysCountLimit()].Timestamp;
}

}
