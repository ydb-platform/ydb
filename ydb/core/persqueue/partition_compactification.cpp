#include "partition.h"
#include "partition_log.h"
#include "write_meta.h"
#include "partition_util.h"

namespace NKikimr::NPQ {
std::unique_ptr<TEvPQ::TEvRead> MakeEvRead(ui64 nextRequestCookie, ui64 startOffset, ui64 lastOffset, TMaybe<ui64> nextPartNo = Nothing()) {
    Y_ABORT_UNLESS(nextRequestCookie != TPartition::ERequestCookie::ReadBlobsForCompaction);
    Y_ABORT_UNLESS(nextRequestCookie != TPartition::ERequestCookie::CompactificationWrite);
    auto evRead = std::make_unique<TEvPQ::TEvRead>(
        nextRequestCookie,
        startOffset,
        lastOffset,
        nextPartNo.GetOrElse(0),
        std::numeric_limits<ui32>::max(),
        TString{},
        CLIENTID_COMPACTION_CONSUMER,
        3000,
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
bool CheckResponse(TEvPQ::TEvProxyResponse::TPtr& ev) {
    if (ev->Get()->Response->GetStatus() == NMsgBusProxy::MSTATUS_OK &&
        ev->Get()->Response->GetErrorCode() == NPersQueue::NErrorCode::OK &&
        ev->Get()->Response->GetPartitionResponse().HasCmdReadResult() &&
        ev->Get()->Response->GetPartitionResponse().GetCmdReadResult().ResultSize() > 0)
    {
        return true;
    } else {
        return false;
        // Will retry the request next;
    }
}
bool TPartitionCompaction::TReadState::ProcessResponse(TEvPQ::TEvProxyResponse::TPtr& ev) {
    Cerr << "===ReadState process reponse\n";
    if (CheckResponse(ev))
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
            // Probably previous message was deleted (do we really expect this to happen though?)
            // Drop it anyway.
            LastMessage = Nothing();
        }
        bool isLastPart = !res.HasTotalParts()
                          || res.GetTotalParts() == res.GetPartNo() + 1;

        Cerr << "====Got part no " << res.GetPartNo() << ", is last: " << isLastPart << ", key: " << res.GetPartitionKey() << Endl;
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
            if (res.HasTotalParts() && res.GetPartNo() + 1 == res.GetTotalParts()) {
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
    return OffsetToRead - 1;
}

TPartitionCompaction::TCompactState::TCompactState(
        THashMap<TString, ui64>&& data, ui64 firstUncompactedOffset, ui64 maxOffset, TPartition* partitionActor
)
    : MaxOffset(maxOffset)
    , TopicData(std::move(data))
    , PartitionActor(partitionActor)
    , DataKeysBody(PartitionActor->CompactionBlobEncoder.GetDataKeysBody())
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
        if (offset < firstUncompactedOffset) {
            PQ_LOG_CRIT("Partition compaction state - got offset = " << offset << " less then uncompacted offset = " << firstUncompactedOffset << "for topic: " << PartitionActor->TopicName() << ":" << PartitionActor->Partition.OriginalPartitionId);
            Failure = true;
        }
    }
    MaxOffset = std::min(MaxOffset, FirstHeadOffset);
    TStringBuilder msg;
    msg << "===Compact state - start from offset: " << firstUncompactedOffset << ", max offset: " << maxOffset << Endl;
    Cerr << msg;

    KeysIter = DataKeysBody.begin();
}

TPartitionCompaction::EStep TPartitionCompaction::TCompactState::ContinueIfPossible(ui64 nextRequestCookie) {
    if (Failure) {
        return EStep::PENDING;
    }
    if (OffsetToCommit) {
        SendCommit(*OffsetToCommit, nextRequestCookie);
        return EStep::COMPACTING;
    }

    Cerr << "=== Compact state: continue if possible\n";
    bool doFinalize = false;
    while (KeysIter != DataKeysBody.end()) {
        const auto& currKey = KeysIter->Key;
        auto maxBlobOffset = currKey.GetOffset() + currKey.GetCount();
        if (maxBlobOffset >= MaxOffset) {
            doFinalize = true;
            break;
        }
        if (RequestDataSize > MAX_REQUEST_DATA_SIZE - 8_MB) {
            break;
        }
        //Need to read and process this blob.
        Cerr << "===Compact state - send evRead from offset " << currKey.GetOffset() << " part no " << currKey.GetPartNo() << ", count: " << currKey.GetCount() + 1 << Endl;
        Cerr << "===Current key: " << currKey.ToString() << Endl;
        auto evRead = MakeEvRead(nextRequestCookie, currKey.GetOffset(), maxBlobOffset, currKey.GetPartNo());

        PartitionActor->Send(PartitionActor->SelfId(), evRead.release());
        return EStep::COMPACTING;
    }
    // Probably processed everything
    if (doFinalize) {
        SaveLastBatch();
    }
    if (Request) {
        RunKvRequest();
        return EStep::COMPACTING;
    }
    // Nothing to do
    return EStep::PENDING;
}

void TPartitionCompaction::TCompactState::AddCmdWrite(const TKey& key, TBatch& batch) {
    Cerr << "===CompactState: add cmd write for key " << key.ToString() << "\n";
    if (!Request) {
        Request = MakeHolder<TEvKeyValue::TEvRequest>();
    }
    TString data;
    batch.Pack();
    batch.SerializeTo(data);
    TClientBlob::CheckBlob(key, data);
    ChangedKeys.emplace_back(key, data.size(), TInstant::Zero(), 0, nullptr);
    PartitionActor->AddCmdWrite(TPartitionedBlob::TFormedBlobInfo{key, data}, Request.Get(), PartitionActor->ActorContext(), false);
}

void ClearBlob(TClientBlob& blob) {
    blob.Data = TString{};
    blob.UncompressedSize = 0;
    if (blob.PartData) {
        blob.PartData->TotalSize = 0;
    }
}

void TPartitionCompaction::TCompactState::SaveLastBatch() {
    if (!LastBatch)
        return;

    TStringBuilder msg;
    msg << "===CompactState: save last batch, current size: " << LastBatch->Blobs.size() << "LastMsgBlobs: " << LastMessageBlobs.size();
    msg << "\n. Current internal parts count: " << LastBatch->GetInternalPartsCount() << Endl;
    Cerr << msg;
    for (auto& blob : LastMessageBlobs) {
        msg.clear();
        msg << "===CompactState: add blob to LastBatch, seqNo: " << blob.SeqNo << ", part: " << blob.GetPartNo() << " out of " << blob.GetTotalParts() << Endl;

        LastBatch->AddBlob(std::move(blob));
        msg << "Current internal parts count: " << LastBatch->GetInternalPartsCount() << Endl;
        Cerr << msg;
    }
    AddCmdWrite(LastBatchKey, LastBatch.GetRef());
    LastBatch = Nothing();
    LastMessageBlobs.clear();
    LastBatchKey = TKey();
}

//TPartitionCompaction::EStep
bool TPartitionCompaction::TCompactState::ProcessResponse(TEvPQ::TEvProxyResponse::TPtr& ev) {
    Cerr << "===CompactState process reponse\n";
    if (CheckResponse(ev))
    {
        // empty?
    } else {
        Cerr << "====Read state: bad response\n";
        return false;
        // Will retry the request next;
    }

    if (ev->Get()->Cookie == CommitCookie) {
        Cerr << "===CompactState: Got commit response with cookie = " << CommitCookie << Endl;
        OffsetToCommit = Nothing();
        CommitCookie = 0;
        return true;
    }
    const auto& readResult = ev->Get()->Response->MutablePartitionResponse()->GetCmdReadResult();


    ui64 lastExpectedOffset = (KeysIter + 1 == DataKeysBody.end() ? FirstHeadOffset : (KeysIter + 1)->Key.GetOffset());
    ui64 lastExpectedPartNo = (KeysIter + 1 == DataKeysBody.end() ? FirstHeadPartNo : (KeysIter + 1)->Key.GetPartNo());

    //Shows if current blob has truncated message in the end; If it hass, we cannot deside either to save or drop the last message
    //So we keep both it's batch and separetely it's parts
    bool isTruncatedBlob = false;
    for (const auto&  res : readResult.GetResult()) {
        if (lastExpectedOffset < MaxOffset && res.GetOffset() == lastExpectedOffset && res.HasTotalParts() && res.GetTotalParts() > lastExpectedPartNo + 1) {
            isTruncatedBlob = true;
            break;
        }
    }
    bool isMiddlePartOfMessage = (lastExpectedOffset == readResult.GetResult(0).GetOffset()
                                 && readResult.GetResult(0).GetPartNo() > 0);



    Cerr << "===CompactState process reponse starting from " << readResult.GetResult(0).GetOffset() << ":" << readResult.GetResult(0).GetPartNo() << " to " << lastExpectedOffset << ":" << lastExpectedPartNo << ", total response size: " << readResult.ResultSize() << Endl;

    TMaybe<TBatch> currentBatch;
    TVector<TClientBlob> currentMessageBlobs;
    for (ui32 i = 0; i < readResult.ResultSize(); ++i) {
        auto& res = readResult.GetResult(i);
        if (!currentBatch) {
            Cerr << "===Compact state: create currentBatch\n";
            currentBatch.ConstructInPlace(res.GetOffset(), res.GetPartNo());
        }

        TClientBlob blob{res.GetSourceId(), res.GetSeqNo(), std::move(res.GetData()),
                         Nothing(),
                         TInstant::MilliSeconds(res.GetWriteTimestampMS()), TInstant::MilliSeconds(res.GetCreateTimestampMS()),
                         res.GetUncompressedSize(), res.GetPartitionKey(), res.GetExplicitHash()};

        if (res.HasTotalParts()) {
            blob.PartData = TPartData{static_cast<ui16>(res.GetPartNo()), static_cast<ui16>(res.GetTotalParts()), res.GetTotalSize()};
        }
        Cerr << "===CompactState process blob - " << res.GetOffset() << ":" << res.GetPartNo() << " from SeqNo: " << res.GetSeqNo() << "(total parts: " << res.GetTotalParts() << Endl;


        if (FirstUnprocessedOffset && res.GetOffset() < FirstUnprocessedOffset || res.GetOffset() >= MaxOffset) {
            RequestDataSize += blob.Data.size();
            Cerr << "===CompactState - add blob, seqNo: " << blob.SeqNo << " to currentBatch part no " << blob.GetPartNo() << " out of total " << blob.GetTotalParts() << Endl;
            currentBatch->AddBlob(std::move(blob));
            continue;
        }
        currentMessageBlobs.push_back(std::move(blob));

        bool haveTruncatedMessage = LastMessage.Defined() && LastMessage->GetTotalParts() > LastMessage->GetPartNo() + 1;
        bool isNewMsg = !res.HasPartNo() || res.GetPartNo() == 0;
        if (haveTruncatedMessage && isNewMsg) {
            // Probably previous message was deleted (do we really expect this to happen though?)
            // Drop it anyway.
            LastMessage = Nothing();
        }
        bool isLastPart = !res.HasTotalParts()
                          || res.GetTotalParts() == res.GetPartNo() + 1;

        if (res.GetOffset() == lastExpectedOffset && res.GetPartNo() == lastExpectedPartNo) {
            break;
        }

        if (isNewMsg) {
            Cerr << "===CompactState got first part of message on offset " << res.GetOffset() << "\n";
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
            if (!res.HasTotalParts() || res.GetPartNo() + 1 == res.GetTotalParts()) {
                Y_ABORT_UNLESS((ui32)LastMessage->GetTotalSize() == (ui32)LastMessage->GetData().size());
            }
        }
        if (isMiddlePartOfMessage) {
            Y_ABORT_UNLESS(LastMessage.Defined() && isTruncatedBlob);
            ClearBlob(blob);
            Cerr << "===CompactState - add blob (from middle), seqNo: " << blob.SeqNo << " to currentBatch part no " << blob.GetPartNo() << " out of total " << blob.GetTotalParts() << Endl;

            currentBatch->AddBlob(std::move(blob));
            continue;
        }
        if (isLastPart) {
            FirstUnprocessedOffset = res.GetOffset() + 1;
            LastProcessedOffset = res.GetOffset();
            Y_ABORT_UNLESS(!isMiddlePartOfMessage);
            const auto& message = LastMessage.Defined() ? LastMessage.GetRef() : res;

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
            auto iter = TopicData.find(key);
            bool keepMessage = (iter.IsEnd() || iter->second == offset);
            Cerr << "===CompactState - composed message with key '" << key << "' at offset " << offset << ", need keep = " << keepMessage << Endl;

            if (LastBatch) {
                if (!keepMessage) {
                    for (auto blob: LastMessageBlobs) {
                        RequestDataSize -= blob.Data.size();
                        Cerr << "===CompactState: Zero blob for offset" << blob.GetPartNo() << Endl;
                        ClearBlob(blob);
                    }
                }
                SaveLastBatch();
            }

            if (!keepMessage) {
                for (auto& [key, batch] : CurrentMessageBatches) {
                    AddCmdWrite(TKey::FromString(key), batch);
                }
                for (auto& blob: currentMessageBlobs) {
                    Cerr << "===Zero message from seqNo: " << blob.SeqNo << ", part: " << blob.GetPartNo() << Endl;
                    ClearBlob(blob);
                }
            }
            for (auto& blob: currentMessageBlobs) {
                RequestDataSize += blob.Data.size();
                Cerr << "===CompactState - add blob to currentBatch, seqNo: " << blob.SeqNo << " from current message to current Batch part no " << blob.GetPartNo() << " out of total " << blob.GetTotalParts() << Endl;

                currentBatch->AddBlob(std::move(blob));
            }
            currentMessageBlobs.clear();
            CurrentMessageBatches.clear();
            LastMessage = Nothing();
            Cerr << "===Compact state: clear last meesage blobs\n";
        }
    }

    if (isMiddlePartOfMessage) {
        Cerr << "===CompactState - drop currentBatch to CurrentMessageBatches\n";
        CurrentMessageBatches.emplace(KeysIter->Key.ToString(), std::move(currentBatch.GetRef()));
        currentBatch = Nothing();
        KeysIter++;
        return true;
    }

    if (isTruncatedBlob) {
        TStringBuilder msg;
        msg << "===Compact state: drop currentMessageBlobs (" << currentMessageBlobs.size() << ") to LastMsgBlobs, current batch (" << currentBatch->Blobs.size() << ") to LastBatch\n";
        Cerr << msg;
        LastMessageBlobs = std::move(currentMessageBlobs);
        LastBatchKey = KeysIter->Key;
        LastBatch = std::move(currentBatch);
        if (RequestDataSize > MAX_REQUEST_DATA_SIZE - MAX_BLOB_SIZE) {
            // Current message is not complete yet. But we cannot read another blob as KV WriteRequest may become too large.
            // Drop parts of last message as they are and keep Keys Iterator on same position - we will continue
            // from the same blob on next iteration.
            // Also keep current offset - so we know we ignore all parts of already processed messages on next iteration.
            SaveLastBatch();
            return true;
        }
    } else {
        Cerr << "===Compact state: add CmdWrite with key: " << KeysIter->Key.ToString() << " from currentBatch\n";
        AddCmdWrite(KeysIter->Key, currentBatch.GetRef());
        currentBatch = Nothing();
    }
    KeysIter++; //Blob processed, go on.
    return true;
}

void TPartitionCompaction::TCompactState::AddDeleteRange(const TKey& key) {
    // Currently unused;
    //DroppedKeys.push_back(key);
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
}


TPartitionCompaction::EStep TPartitionCompaction::TCompactState::ProcessKVResponse(TEvKeyValue::TEvResponse::TPtr& ev) {
    Cerr << "====Compact state: Process KVResponse\n";
    /* Not used currently;
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
    */

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
    OffsetToCommit = LastProcessedOffset;
    //PartitionActor->CompactionBlobEncoder.RecalcBlobSizes(ChangedKeys);
    ChangedKeys.clear();
    return EStep::COMPACTING;
}

void TPartitionCompaction::TCompactState::SendCommit(ui64 offset, ui64 cookie) {
    Cerr << "===CompactState: send commit with offset = " << offset << "and cookie = " << cookie << Endl;
    CommitCookie = cookie;
    auto ev = MakeHolder<TEvPQ::TEvSetClientInfo>(CommitCookie, CLIENTID_COMPACTION_CONSUMER, offset, TString{}, 0, 0, 0, TActorId{});
    ev->IsInternal = true;
    PartitionActor->Send(PartitionActor->SelfId(), ev.Release()
);
}
} // namespace NKikimr::NPQ