#include "partition.h"
#include "partition_log.h"
#include "write_meta.h"
#include "partition_util.h"

namespace NKikimr::NPQ {


std::unique_ptr<TEvPQ::TEvRead> MakeEvRead(ui64 nextRequestCookie, ui64 startOffset, ui64 lastOffset, TMaybe<ui64> nextPartNo = Nothing()) {
    auto evRead = std::make_unique<TEvPQ::TEvRead>(
        nextRequestCookie,
        startOffset,
        lastOffset,
        nextPartNo.GetOrElse(0),
        lastOffset,
        TString{},
        CLIENTID_COMPACTION_CONSUMER,
        0,
        std::numeric_limits<ui32>::max(),
        0,
        0,
        "unknown",
        false,
        TActorId{}
    );
    evRead->IsInternal = true;
    return evRead;
}

TPartitionCompaction::TPartitionCompaction(ui64 firstUncompactedOffset, ui64 startCookie, ui64 endCookie, TPartition* partitionActor)
    : FirstUncompactedOffset(firstUncompactedOffset)
    , StartCookie(startCookie)
    , EndCookie(endCookie)
    , CurrentCookie(startCookie)
    , PartitionActor(partitionActor)
{
    Cerr << "===Compacter created\n";
}

void TPartitionCompaction::TryCompactionIfPossible() {
    if (!PartitionActor->Config.GetEnableCompactification()) {
        Cerr << "=== Compacter created, but compaction is disabled\n";
        Step = EStep::PENDING;
        return;
    }
    Cerr << "====== Try compaction, have request inflight: " << HaveRequestInflight << "\n";
    if (HaveRequestInflight)
        return;
    switch (Step) {
    case EStep::PENDING:
        Cerr << "=== Try compaction, pending, go to reading\n";
        ReadState = TReadState(FirstUncompactedOffset, PartitionActor);
        Step = EStep::READING;
        [[fallthrough]];
    case EStep::READING: {
        auto step = ReadState->ContinueIfPossible(GetNextCookie());
        Step = step;

        if (step == EStep::READING) {
            Cerr << "=== Try compaction, reading, keep reading\n";
            HaveRequestInflight = true;
            break;
        } else if (step == EStep::COMPACTING) {
            Cerr << "=== Try compaction, reading, go to compacting\n";
            Step = EStep::COMPACTING;
            CompactState = TCompactState(std::move(ReadState->GetData()), FirstUncompactedOffset, ReadState->GetLastOffset(), PartitionActor);
            ReadState = std::nullopt;
        } else {
            Cerr << "=== Try compaction, reading, switch to pending?\n";
            break;
        }
        [[fallthrough]];
    }
    case EStep::COMPACTING: {
        auto step = CompactState->ContinueIfPossible(GetNextCookie());
        Step = step;
        if (step == EStep::COMPACTING) {
            Cerr << "=== Try compaction, compacting, keep compacting\n";
            HaveRequestInflight = true;
            break;
        } else {
            CompactState = std::nullopt;
        }
    }
    }
}

void TPartitionCompaction::ProcessResponse(TEvPQ::TEvProxyResponse::TPtr& ev) {
    Cerr << "====== Compacter - process response with cookie: " << ev->Get()->Cookie << ", current cookie:" << CurrentCookie << Endl;
    if (ev->Get()->Cookie != CurrentCookie) {
        Cerr << "=== Got response with wrong cookie\n";
        return;
    }
    HaveRequestInflight = false;
    switch (Step) {
        case EStep::READING: {
            Y_ABORT_UNLESS(ReadState);
            ReadState->ProcessResponse(ev);
            break;
        }
        case EStep::COMPACTING: {
            Y_ABORT_UNLESS(CompactState);
            CompactState->ProcessResponse(ev);
            break;
        }
        case EStep::PENDING:
            Cerr << "=== Got response on pending, ignore\n";
            break;
        default:
            Y_ABORT();
    }
    TryCompactionIfPossible();
}

void TPartitionCompaction::ProcessResponse(TEvKeyValue::TEvResponse::TPtr& ev) {
    if (CompactState) {
        CompactState->ProcessKVResponse(ev);
    }
    HaveRequestInflight = false;

}
TPartitionCompaction::TReadState::TReadState(ui64 firstOffset, TPartition* partitionActor)
    : OffsetToRead(firstOffset)
    , PartitionActor(partitionActor)
{
    ui64 firstHeadOffset = 0;
    for (const auto& key: PartitionActor->CompactionBlobEncoder.HeadKeys) { // To do - no need to list all?
        Cerr << "===Have Head Key with offset: " << key.Key.GetOffset() << Endl;
        if (firstHeadOffset == 0 || key.Key.GetOffset() < firstHeadOffset) {
            firstHeadOffset = key.Key.GetOffset();
        }
    }

    if(PartitionActor->CompactionBlobEncoder.DataKeysBody.empty()) {
        LastOffset = firstOffset;
    } else if (firstHeadOffset) {
        LastOffset = firstHeadOffset;
    }
    Cerr << "===ReadState created, last offset = " << LastOffset <<", partition last offset: " << PartitionActor->BlobEncoder.EndOffset << "Data keys size: "
        << PartitionActor->BlobEncoder.DataKeysBody.size() << ", head: " << PartitionActor->BlobEncoder.DataKeysHead.size() << Endl;
}

bool TPartitionCompaction::TReadState::ProcessResponse(TEvPQ::TEvProxyResponse::TPtr& ev) {
    Cerr << "===ReadState process reponse\n";
    if (ev->Get()->Response->GetStatus() == NMsgBusProxy::MSTATUS_OK &&
        ev->Get()->Response->GetErrorCode() == NPersQueue::NErrorCode::OK &&
        ev->Get()->Response->GetPartitionResponse().HasCmdReadResult() &&
        ev->Get()->Response->GetPartitionResponse().GetCmdReadResult().ResultSize() > 0)
    {
        // empty?
    } else {
        Cerr << "====Read state: bad response\n";
        return false;
        // Will retry the request next;
    }
    const auto& readResult = ev->Get()->Response->GetPartitionResponse().GetCmdReadResult();
    for (ui32 i = 0; i < readResult.ResultSize(); ++i) {
        auto& res = readResult.GetResult(i);
        bool haveTruncatedMessage = LastMessage.Defined() && LastMessage->GetTotalParts() > LastMessage->GetPartNo() + 1;
        bool isNewMsg = !res.HasPartNo() || res.GetPartNo() == 0;
        if (haveTruncatedMessage && isNewMsg) {
            // Probably previous message was probably deleted (do we really expect this to happen though?)
            // Drop it anyway.
            LastMessage = Nothing();
        }
        bool isLastPart = !res.HasTotalParts()
                          || res.GetTotalParts() == res.GetPartNo() + 1;
        if (isNewMsg) {
            if (!isLastPart) {
                LastMessage.ConstructInPlace().CopyFrom(res);
            }
            // otherwise it's a single part message, will parse it in place
        } else { //glue to last res
            Y_ABORT_UNLESS(LastMessage.Defined());
            //auto rr = partResp->MutableResult(partResp->ResultSize() - 1);
            if (LastMessage->GetSeqNo() != res.GetSeqNo()
                || LastMessage->GetPartNo() + 1 != res.GetPartNo()
            ) {
                PQ_LOG_CRIT("Partition compaction: Handle TEvRead last read pos (seqno/parno): " << LastMessage->GetSeqNo()
                             << "," << LastMessage->GetPartNo() << " readed now " << res.GetSeqNo()
                             << ", " << res.GetPartNo());
            }
            Y_ABORT_UNLESS(LastMessage->GetSeqNo() == res.GetSeqNo());
            (*LastMessage->MutableData()) += res.GetData();
            LastMessage->SetPartitionKey(res.GetPartitionKey());
            LastMessage->SetPartNo(res.GetPartNo());
            LastMessage->SetUncompressedSize(LastMessage->GetUncompressedSize() + res.GetUncompressedSize());
            if (res.GetPartNo() + 1 == res.GetTotalParts()) {
                Y_ABORT_UNLESS((ui32)LastMessage->GetTotalSize() == (ui32)LastMessage->GetData().size());
            }
        }
        if (isLastPart) {
            const auto& message = LastMessage.Defined() ? LastMessage.GetRef() : res;
            OffsetToRead = message.GetOffset() + 1;
            NextPartNo = 0;

            auto proto(GetDeserializedData(message.GetData()));
            if (proto.GetChunkType() != NKikimrPQClient::TDataChunk::REGULAR) {
                LastMessage = Nothing();
                continue; //no such chunks must be on prod - ?
            }
            auto offset = message.GetOffset();
            TString key;
            if (message.HasPartitionKey()) {
                key = message.GetPartitionKey();
            }
            if (key.empty()) {
                for (const auto& kv : proto.GetMessageMeta()) {
                    if (kv.key() == "__key") {
                        key = kv.value();
                        break;
                    }
                }
            }
            TopicData[key] = offset;

            Cerr << "===ReadState got key '" << key << "' at offset " << offset << ", current map size: " << TopicData.size() << Endl;
            LastMessage = Nothing();
        } else {
            Y_ABORT_UNLESS(LastMessage.Defined());
            OffsetToRead = LastMessage->GetOffset();
            NextPartNo = LastMessage->GetPartNo() + 1;
        }
    }
    return true;
}

constexpr const ui64 MAX_REQUEST_DATA_SIZE = 20_MB;

TPartitionCompaction::EStep TPartitionCompaction::TReadState::ContinueIfPossible(ui64 nextRequestCookie) {
    if (TopicData.size() >= MAX_DATA_KEYS)
        return EStep::COMPACTING;

    if (OffsetToRead >= LastOffset) {
        return TopicData.size() ? EStep::COMPACTING : EStep::PENDING;
    }
    TStringBuilder msg;
    msg << "=== === ReadState send EvRead with cookie " << nextRequestCookie << ", offset = " << OffsetToRead << ", LastOffset: " << LastOffset << Endl;
    Cerr << msg;
    auto evRead = MakeEvRead(nextRequestCookie, OffsetToRead, LastOffset);
    PartitionActor->Send(PartitionActor->SelfId(), evRead.release());
    return EStep::READING;
}

THashMap<TString, ui64>&& TPartitionCompaction::TReadState::GetData() {
    return std::move(TopicData);
}

ui64 TPartitionCompaction::TReadState::GetLastOffset() {
    if(LastMessage) return LastOffset - 1;
    else return LastOffset;
}

TPartitionCompaction::TCompactState::TCompactState(
        THashMap<TString, ui64>&& data, ui64 firstUncompactedOffset, ui64 maxOffset, TPartition* partitionActor
)
    : MaxOffset(maxOffset)
    , TopicData(std::move(data))
    , PartitionActor(partitionActor)
    , DataKeysBody(PartitionActor->CompactionBlobEncoder.DataKeysBody)
{
    if (!PartitionActor->CompactionBlobEncoder.HeadKeys.empty()) {
        FirstHeadOffset = PartitionActor->CompactionBlobEncoder.HeadKeys.front().Key.GetOffset();
        FirstHeadPartNo = PartitionActor->CompactionBlobEncoder.HeadKeys.front().Key.GetPartNo();
    } else if (!PartitionActor->BlobEncoder.DataKeysBody.empty()) {
        FirstHeadOffset = PartitionActor->BlobEncoder.DataKeysBody.front().Key.GetOffset();
        FirstHeadPartNo = PartitionActor->BlobEncoder.DataKeysBody.front().Key.GetPartNo();
    } else {
        FirstHeadOffset = PartitionActor->BlobEncoder.EndOffset;
        FirstHeadPartNo = 0;
    }
    if (DataKeysBody.empty()) {
        Failure = true; //Probably, also an internal error ?
    }
    if (TopicData.empty()) {
        PQ_LOG_CRIT("Partition compaction state created with empty topic data for topic: " << PartitionActor->TopicName() << ":" << PartitionActor->Partition.OriginalPartitionId);
        Failure = true;
    }
    for (const auto& [_, offset] : TopicData) {
        PQ_LOG_CRIT("Partition compaction state - got offset = " << offset << " less then uncompacted offset = " << firstUncompactedOffset << "for topic: " << PartitionActor->TopicName() << ":" << PartitionActor->Partition.OriginalPartitionId);
        if (offset < firstUncompactedOffset) {
            Failure = true;
        }
        OffsetsToKeep.insert(offset);
    }
    TStringBuilder msg;
    msg << "===Compact state - start from offset: " << firstUncompactedOffset << ", max offset: " << maxOffset << Endl;
    Cerr << msg;
    for (const auto& offset : OffsetsToKeep) {
        Cerr << "===Compact state - got offset to keep: " << offset << Endl;
    }

    OffsetsToKeep.insert(FirstHeadOffset);
    OffsetsToKeep.insert(std::numeric_limits<ui64>::max());
    OffsetsIter = OffsetsToKeep.begin();

    KeysIter = DataKeysBody.begin();
}

TPartitionCompaction::EStep TPartitionCompaction::TCompactState::ContinueIfPossible(ui64 nextRequestCookie) {
    if (Failure) {
        return EStep::PENDING;
    }
    Cerr << "=== Compact state: continue if possible\n";
    while (KeysIter != DataKeysBody.end()) {
        const auto& currKey = KeysIter->Key;
        auto maxBlobOffset = currKey.GetOffset() + currKey.GetCount();
        if (maxBlobOffset >= MaxOffset)
            break;
        if (currKey.GetCount() == 0) {
            if (!DropOffset.Defined() && currKey.GetOffset() == DropOffset) {
                // A blob fully containing part of big message to drop; Delete it
                AddDeleteRange(currKey);
            }
            else {
                //Keep key consisting only of large message part;
                Cerr << "===Compact state - keep key " << KeysIter->Key.ToString() << Endl;
            }
            KeysIter++;
            continue;
        }
        if (currKey.GetOffset() > *OffsetsIter) {
            OffsetsIter++;
            continue;
        }
        if (maxBlobOffset < *OffsetsIter && maxBlobOffset < MaxOffset) {
            //No offsets to keep within with blob, remove it.
            if (!Request) {
                Request = MakeHolder<TEvKeyValue::TEvRequest>();
            }
            AddDeleteRange(currKey);
            KeysIter++;
            continue;
        }
        if (RequestDataSize > MAX_REQUEST_DATA_SIZE - 8_MB) {
            break;
        }
        //Need to read and process this blob.
        Cerr << "===Compact state - send evRead from offset " << currKey.GetOffset() << " part no " << currKey.GetPartNo() << Endl;
        auto evRead = MakeEvRead(nextRequestCookie, currKey.GetOffset(), currKey.GetOffset() + currKey.GetCount() + 1,
                currKey.GetPartNo());

        PartitionActor->Send(PartitionActor->SelfId(), evRead.release());
        return EStep::COMPACTING;
    }
    // Probably processed everything
    if (Request) {
        RunKvRequest();
        return EStep::COMPACTING;
    }
    // Nothing to do
    PartitionActor->BlobEncoder.FirstUncompactedOffset = MaxOffset;
    return EStep::PENDING;
}

//TPartitionCompaction::EStep
bool TPartitionCompaction::TCompactState::ProcessResponse(TEvPQ::TEvProxyResponse::TPtr& ev) {
    if (ev->Get()->Response->GetStatus() == NMsgBusProxy::MSTATUS_OK &&
        ev->Get()->Response->GetErrorCode() == NPersQueue::NErrorCode::OK &&
        ev->Get()->Response->GetPartitionResponse().HasCmdReadResult() &&
        ev->Get()->Response->GetPartitionResponse().GetCmdReadResult().ResultSize() > 0)
    {
        // empty
    } else {
        Cerr << "===Compact state: bad response\n";
        return false;
        // Will retry the request next;
    }
    Cerr << "===Compact state process reponse\n";
    auto& readResult = *ev->Get()->Response->MutablePartitionResponse()->MutableCmdReadResult();


    ui64 lastExpectedOffset = (KeysIter + 1 == DataKeysBody.end() ? FirstHeadOffset : (KeysIter + 1)->Key.GetOffset());
    ui64 lastExpectedPartNo = (KeysIter + 1 == DataKeysBody.end() ? FirstHeadPartNo : (KeysIter + 1)->Key.GetPartNo());

    TBatch batch{readResult.GetResult(0).GetOffset(), static_cast<ui16>(readResult.GetResult(0).GetPartNo())};
    for (ui32 i = 0; i < readResult.ResultSize(); ++i) {
        auto& res = *readResult.MutableResult(i);
        if (res.GetOffset() == lastExpectedOffset && res.GetPartNo() == lastExpectedPartNo) {
            break;
        }
        while (*OffsetsIter < res.GetOffset()) {
            ++OffsetsIter;
        }
        if (res.GetOffset() == *OffsetsIter || res.GetOffset() >= MaxOffset) {
            TStringBuilder msg;
            msg << "====Compact state: KEEP part: " << res.GetOffset() << ":" << res.GetPartNo() << Endl;
            Cerr << msg;
        } else {
            TStringBuilder msg;
            msg << "====Compact state: Delete part: " << res.GetOffset() << ":" << res.GetPartNo() << Endl;
            Cerr << msg;
            res.SetData("");
            res.SetUncompressedSize(0);
            DropOffset = res.GetOffset();
        }
        RequestDataSize += res.GetData().size();
        TClientBlob blob{res.GetSourceId(), res.GetSeqNo(), std::move(res.GetData()),
                         TPartData{static_cast<ui16>(res.GetPartNo()), static_cast<ui16>(res.GetTotalParts()), res.GetTotalSize()},
                         TInstant::MilliSeconds(res.GetWriteTimestampMS()), TInstant::MilliSeconds(res.GetCreateTimestampMS()),
                         res.GetUncompressedSize(), res.GetPartitionKey(), res.GetExplicitHash()};
        batch.AddBlob(std::move(blob));
    }
    if (!Request) {
        Request = MakeHolder<TEvKeyValue::TEvRequest>();
    }
    TString data;
    batch.SerializeTo(data);
    PartitionActor->AddCmdWrite(TPartitionedBlob::TFormedBlobInfo{KeysIter->Key, data}, Request.Get(), PartitionActor->ActorContext(), false);

    KeysIter++; //Blob processed, go on.
    return true;
}

void TPartitionCompaction::TCompactState::AddDeleteRange(const TKey& key) {
    DroppedKeys.push_back(key);
    TStringBuilder msg;
    msg << "====Compact state: Delete key: " << key.ToString() << ". (" <<key.GetOffset() << ":" << key.GetPartNo() << ":" << key.GetCount() << ")" << Endl;
    Cerr << msg;
    if (!Request) {
        Request = MakeHolder<TEvKeyValue::TEvRequest>();
    }
    auto* cmd = Request->Record.AddCmdDeleteRange();
    auto* range = cmd->MutableRange();

    range->SetFrom(key.data(), key.size());
    range->SetIncludeFrom(true);
    range->SetTo(key.data(), key.size());
    range->SetIncludeTo(true);
}

void TPartitionCompaction::TCompactState::RunKvRequest() {
    Cerr << "====Compact state: RunKvRequest\n";
    Y_ABORT_UNLESS(Request);
    Request->Record.SetCookie(TPartition::ERequestCookie::CompactificationWrite);
    PartitionActor->Send(PartitionActor->BlobCache, Request.Release(), 0, 0, PartitionActor->PersistRequestSpan.GetTraceId());
    RequestDataSize = 0;

    // ToDo: Do we need to add some meta here?
}


TPartitionCompaction::EStep TPartitionCompaction::TCompactState::ProcessKVResponse(TEvKeyValue::TEvResponse::TPtr& ev) {
    Cerr << "====Compact state: Process KVResponse\n";
    if (DroppedKeys) {
        std::deque<TDataKey> newDataKeys;
        ui64 i = 0;
        for (const auto& dataKey : PartitionActor->CompactionBlobEncoder.DataKeysBody) {
            while (i < DroppedKeys.size() && DroppedKeys[i].GetOffset() < dataKey.Key.GetOffset()) {
                i++;
            }
            if (i < DroppedKeys.size() && dataKey.Key == DroppedKeys[i]) {
                i++;
                continue;
            }
            newDataKeys.push_back(dataKey);
        }
        TStringBuilder msg;
        msg << "====Compact state: Dropped " << DroppedKeys.size() << ", compaction encoder had: " << PartitionActor->CompactionBlobEncoder.DataKeysBody.size()
            << " keys, now have: " << newDataKeys.size() << Endl;
        Cerr << msg;
        msg.clear();
        msg << "Old start offset: " << PartitionActor->CompactionBlobEncoder.StartOffset;
        PartitionActor->CompactionBlobEncoder.DataKeysBody = std::move(newDataKeys);
        if (PartitionActor->CompactionBlobEncoder.DataKeysBody.size() != 0) {
            PartitionActor->CompactionBlobEncoder.StartOffset = PartitionActor->CompactionBlobEncoder.DataKeysBody.front().Key.GetPartNo() == 0
                                                                ? PartitionActor->CompactionBlobEncoder.DataKeysBody.front().Key.GetOffset()
                                                                : PartitionActor->CompactionBlobEncoder.DataKeysBody.front().Key.GetOffset() + 1;
        } else if (PartitionActor->CompactionBlobEncoder.HeadKeys.size() != 0) {
            PartitionActor->CompactionBlobEncoder.StartOffset = PartitionActor->CompactionBlobEncoder.HeadKeys.front().Key.GetOffset();
        } else if (PartitionActor->BlobEncoder.DataKeysBody.size() != 0) {
            PartitionActor->CompactionBlobEncoder.StartOffset = PartitionActor->BlobEncoder.DataKeysBody.front().Key.GetOffset();
        } else {
            PartitionActor->CompactionBlobEncoder.StartOffset = PartitionActor->CompactionBlobEncoder.EndOffset;
        }
        msg << "New start offset: " << PartitionActor->CompactionBlobEncoder.StartOffset << Endl;
        Cerr << msg;
    }

    auto& response = ev->Get()->Record;
    if (response.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
        PQ_LOG_CRIT("Partition compaction state: Got not OK KV response");
        return EStep::PENDING;
    }
    if (response.DeleteRangeResultSize()) {
        for (ui32 i = 0; i < response.DeleteRangeResultSize(); ++i) {
            if (response.GetDeleteRangeResult(i).GetStatus() != NKikimrProto::OK) {
                PQ_LOG_CRIT("Partition compaction state: Got not OK DeleteRange response");
                return EStep::PENDING;
            }
        }
    }

    if (response.WriteResultSize()) {
        for (ui32 i = 0; i < response.WriteResultSize(); ++i) {
            if (response.GetWriteResult(i).GetStatus() != NKikimrProto::OK) {
                PQ_LOG_CRIT("Partition compaction state: Got not OK Write response");
                return EStep::PENDING;
            }
        }
    }
    return EStep::COMPACTING;
}
} // namespace NKikimr::NPQ