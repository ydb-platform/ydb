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

TPartitionCompaction::TPartitionCompaction(ui64 firstUncompactedOffset, ui64 partRequestCookie, TPartition* partitionActor)
    : FirstUncompactedOffset(firstUncompactedOffset)
    , PartRequestCookie(partRequestCookie)
    , PartitionActor(partitionActor) {
    Cerr << "===Compacter created with cookie: " << partRequestCookie << Endl;
}

void TPartitionCompaction::TryCompactionIfPossible() {
    Y_ENSURE(PartitionActor->Config.GetEnableCompactification());

    if (PartitionActor->CompacterPartitionRequestInflight || PartitionActor->CompacterKvRequestInflight)
        return;
    switch (Step) {
    case EStep::PENDING:
        Cerr << "=== Try compaction, pending, go to reading\n";
        ReadState = TReadState(FirstUncompactedOffset, PartitionActor);
        Step = EStep::READING;
        [[fallthrough]];
    case EStep::READING: {
        auto step = ReadState->ContinueIfPossible(PartRequestCookie);
        Step = step;

        if (step == EStep::READING) {
            Cerr << "=== Try compaction, reading, keep reading\n";
            break;
        } else if (step == EStep::COMPACTING) {
            Cerr << "=== Try compaction, reading, go to compacting\n";
            Step = EStep::COMPACTING;
            CompactState.ConstructInPlace(std::move(ReadState->GetData()), FirstUncompactedOffset, ReadState->GetLastOffset(), PartitionActor);
            ReadState.Clear();
        } else {
            Cerr << "=== Try compaction, reading, switch to pending?\n";
            break;
        }
        [[fallthrough]];
    }
    case EStep::COMPACTING: {
        auto step = CompactState->ContinueIfPossible(PartRequestCookie);
        Step = step;
        if (step == EStep::COMPACTING) {
            Cerr << "=== Try compaction, compacting, keep compacting\n";
            break;
        } else {
            CompactState.Clear();
        }
    }
    }
}

void TPartitionCompaction::ProcessResponse(TEvPQ::TEvProxyResponse::TPtr& ev) {
    Cerr << "====== Compacter - process response with cookie: " << ev->Get()->Cookie << ", current cookie:" << PartRequestCookie << Endl;
    Y_ABORT_UNLESS(PartitionActor->CompacterPartitionRequestInflight);
    PartitionActor->CompacterPartitionRequestInflight = false;
    if (ev->Get()->Cookie != PartRequestCookie) {
        Cerr << "=== Got response with wrong cookie\n";
        return;
    }
    bool processResponseRestult = true;
    switch (Step) {
        case EStep::READING: {
            Y_ABORT_UNLESS(ReadState);
            processResponseRestult = ReadState->ProcessResponse(ev);
            break;
        }
        case EStep::COMPACTING: {
            Y_ABORT_UNLESS(CompactState);
            processResponseRestult = CompactState->ProcessResponse(ev);
            break;
        }
        case EStep::PENDING:
            Cerr << "=== Got response on pending, ignore\n";
            break;
        default:
            Y_ABORT();
    }
    if (!processResponseRestult) {
        PartitionActor->Send(PartitionActor->Tablet, new TEvents::TEvPoisonPill());
    }
    TryCompactionIfPossible();
}

void TPartitionCompaction::ProcessResponse(TEvKeyValue::TEvResponse::TPtr& ev) {
    //Partition must reset this flag;
    Y_ABORT_UNLESS(!PartitionActor->CompacterKvRequestInflight);
    if (CompactState) {
        if (!CompactState->ProcessKVResponse(ev)) {
            PartitionActor->Send(PartitionActor->Tablet, new TEvents::TEvPoisonPill());
        }
    }
}
TPartitionCompaction::TReadState::TReadState(ui64 firstOffset, TPartition* partitionActor)
    : OffsetToRead(firstOffset)
    , PartitionActor(partitionActor)
{
    ui64 firstHeadOffset = PartitionActor->EndOffset;
    for (const auto& key : PartitionActor->HeadKeys) {
        //ToDo: use first key only.
        Cerr << "===Have Head Key with offset: " << key.Key.GetOffset() << Endl;
        if (firstHeadOffset == 0 || key.Key.GetOffset() < firstHeadOffset) {
            firstHeadOffset = key.Key.GetOffset();
        }
    }

    if(partitionActor->DataKeysBody.empty()) {
        LastOffset = firstOffset;
    } else if (firstHeadOffset) {
        LastOffset = firstHeadOffset;
    }
    Cerr << "===ReadState created, last offset = " << LastOffset <<", partition last offset: " << PartitionActor->EndOffset << ". Data keys size: "
        << partitionActor->DataKeysBody.size() << ", head: " << PartitionActor->DataKeysHead.size() << Endl;
}
ui64 CheckResponse(TEvPQ::TEvProxyResponse::TPtr& ev) {
    ui64 ret = 0;
    if (ev->Get()->Response->GetStatus() == NMsgBusProxy::MSTATUS_OK &&
        ev->Get()->Response->GetErrorCode() == NPersQueue::NErrorCode::OK) {
            ++ret; // Status is OK
    }
    else {
        return ret;
    }
    if (ev->Get()->Response->GetPartitionResponse().HasCmdReadResult() &&
        ev->Get()->Response->GetPartitionResponse().GetCmdReadResult().ResultSize() > 0)
    {
        ++ret; // Has partition response;
    }
    return ret;
}
bool TPartitionCompaction::TReadState::ProcessResponse(TEvPQ::TEvProxyResponse::TPtr& ev) {
    Cerr << "===ReadState process reponse\n";
    if (CheckResponse(ev) >= 2) // Expect to have OK status and partition response;
    {
        // empty?
    } else {
        Cerr << "====ReadState: bad response\n";
        return false;
        // Will retry the request next;
    }
    const auto& readResult = ev->Get()->Response->GetPartitionResponse().GetCmdReadResult();
    for (ui32 i = 0; i < readResult.ResultSize(); ++i) {
        auto& res = readResult.GetResult(i);
        Cerr << "====ReadState: Got part " << res.GetOffset() << ":" << res.GetPartNo() << ", size: " << res.GetData().size() << Endl;
        if (SkipOffset && res.GetOffset() == SkipOffset) {
            Cerr << "====ReadState: skip due to SkipOffset " << res.GetOffset() << ":" << res.GetPartNo() << Endl;
            continue;
        }
        if (res.GetData().size() == 0) {
            Cerr << "====ReadState: skip due to empty, reset SkipOffset " << res.GetOffset() << ":" << res.GetPartNo() << Endl;
            SkipOffset = res.GetOffset();
            continue;
        }
        bool haveTruncatedMessage = LastMessage.Defined() && LastMessage->GetTotalParts() > LastMessage->GetPartNo() + 1;
        bool isNewMsg = !res.HasPartNo() || res.GetPartNo() == 0;
        if (haveTruncatedMessage && isNewMsg) {
            // Probably previous message was deleted (do we really expect this to happen though?)
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
            for (const auto& kv : proto.GetMessageMeta()) {
                if (kv.key() == "__key") {
                    key = kv.value();
                    break;
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
    auto evRead = MakeEvRead(nextRequestCookie, OffsetToRead, LastOffset, NextPartNo);
    PartitionActor->Send(PartitionActor->SelfId(), evRead.release());
    PartitionActor->CompacterPartitionRequestInflight = true;
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
    , CommittedOffset(firstUncompactedOffset)
    , DataKeysBody(partitionActor->DataKeysBody)
{
    if (!PartitionActor->HeadKeys.empty()) {
        FirstHeadOffset = PartitionActor->HeadKeys.front().Key.GetOffset();
        FirstHeadPartNo = PartitionActor->HeadKeys.front().Key.GetPartNo();
    } else {
        FirstHeadOffset = PartitionActor->EndOffset;
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
    msg << "===CompactState CREATED. Start from offset: " << firstUncompactedOffset << ", max offset: " << maxOffset << Endl;
    Cerr << msg;

    KeysIter = DataKeysBody.begin();
}

TPartitionCompaction::EStep TPartitionCompaction::TCompactState::ContinueIfPossible(ui64 nextRequestCookie) {
    if (Failure) {
        Cerr << "=== CompactState: failure\n";
        return EStep::PENDING;
    }
    Y_ABORT_UNLESS(!PartitionActor->CompacterPartitionRequestInflight && !PartitionActor->CompacterKvRequestInflight);
    Cerr << "=== CompactState: continue if possible, current key: " << KeysIter->Key.ToString() << Endl;

    bool doFinalize = false;
    while (KeysIter != DataKeysBody.end()) {
        const auto& currKey = KeysIter->Key;
        auto maxBlobOffset = currKey.GetOffset() + currKey.GetCount();
        if (maxBlobOffset >= MaxOffset) {
            doFinalize = true;
            break;
        }
        if (BlobsToWriteInRequest >= 3) {
            break;
        }
        //Need to read and process this blob.
        TStringBuilder msg;
        msg << "===CompactState - send evRead from offset " << currKey.GetOffset() << " part no " << currKey.GetPartNo() << ", end offset: " << maxBlobOffset + 1 << ", cookie: " << nextRequestCookie << Endl;
        Cerr << msg;
        auto evRead = MakeEvRead(nextRequestCookie, currKey.GetOffset(), maxBlobOffset + 1, currKey.GetPartNo());
        PartitionActor->Send(PartitionActor->SelfId(), evRead.release());
        PartitionActor->CompacterPartitionRequestInflight = true;
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
    if (OffsetToCommit) {
        SendCommit(nextRequestCookie);
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
    for (const auto& blob : batch.Blobs) {
        Cerr << "===CompactState: add cmd write, have blob " << blob.SeqNo << " part " << blob.GetPartNo() << ", size: " << blob.Data.size() << Endl;;
    }
    TString data;
    batch.Pack();
    batch.SerializeTo(data);
    TClientBlob::CheckBlob(key, data);
    UpdatedKeys.emplace(key, data.size());
    PartitionActor->AddCmdWrite(TPartitionedBlob::TFormedBlobInfo{key, data}, Request.Get(), PartitionActor->ActorContext(), false);
    BlobsToWriteInRequest++;
}

void ClearBlob(TClientBlob& blob) {
    blob.Data = TString{};
    blob.UncompressedSize = 0;
    Cerr << "===Clear message from seqNo: " << blob.SeqNo << ", part: " << blob.GetPartNo() << Endl;

    if (blob.PartData) {
        blob.PartData->TotalSize = 0;
    }
}

void TPartitionCompaction::TCompactState::SaveLastBatch() {
    if (!LastBatch)
        return;

    TStringBuilder msg;
    msg << "===CompactState: save last batch, current size: " << LastBatch->Blobs.size() << "LastMsgBlobs: " << CurrMsgPartsFromLastBatch.size();
    msg << "\n. Current internal parts count: " << LastBatch->GetInternalPartsCount() << Endl;
    Cerr << msg;
    for (auto& blob : CurrMsgPartsFromLastBatch) {
        msg.clear();
        msg << "===CompactState: add blob to LastBatch, seqNo: " << blob.SeqNo << ", part: " << blob.GetPartNo() << " out of " << blob.GetTotalParts() << Endl;

        LastBatch->AddBlob(std::move(blob));
        msg << "Current internal parts count: " << LastBatch->GetInternalPartsCount() << Endl;
        Cerr << msg;
    }
    AddCmdWrite(LastBatchKey, LastBatch.GetRef());
    LastBatch = Nothing();
    CurrMsgPartsFromLastBatch.clear();
    LastBatchKey = TKey();
}

//TPartitionCompaction::EStep
bool TPartitionCompaction::TCompactState::ProcessResponse(TEvPQ::TEvProxyResponse::TPtr& ev) {
    Cerr << "===CompactState process reponse\n";
    auto status = CheckResponse(ev);
    if (!status) {
        Cerr << "====CompactState: bad response\n";
        return false;
        // Will retry the request next;
    }
    if (ev->Get()->Cookie == CommitCookie) {
        Cerr << "===CompactState: Got commit response with cookie = " << CommitCookie << Endl;
        OffsetToCommit = Nothing();
        CommitCookie = 0;
        return true;
    }
    if (status < 2) {
        return false; //Expect to have partiton response unless this was a commit-ack;
    }
    const auto& readResult = ev->Get()->Response->MutablePartitionResponse()->GetCmdReadResult();

    ui64 lastExpectedOffset = (KeysIter + 1 == DataKeysBody.end())
                               ? FirstHeadOffset
                               : (KeysIter + 1)->Key.GetOffset();
    ui64 lastExpectedPartNo = (KeysIter + 1 == DataKeysBody.end())
                              ? FirstHeadPartNo
                              : (KeysIter + 1)->Key.GetPartNo();

    //Shows if current blob has truncated message in the end; If it hass, we cannot deside either to save or drop the last message
    //So we keep both it's batch and separetely it's parts
    bool isTruncatedBlob = lastExpectedPartNo > 0 && lastExpectedOffset < MaxOffset;
    bool isMiddlePartOfMessage = (lastExpectedOffset == readResult.GetResult(0).GetOffset()
                                 && readResult.GetResult(0).GetPartNo() > 0);

    Cerr << "===CompactState process reponse starting from " << readResult.GetResult(0).GetOffset() << ":" << readResult.GetResult(0).GetPartNo() << " to " << lastExpectedOffset << ":" << lastExpectedPartNo << ", total response size: " << readResult.ResultSize() << Endl;

    ui32 partsCount = 0;
    TMaybe<TBatch> currentBatch;
    TVector<TClientBlob> currentMessageBlobs;
    bool hasNonZeroParts = false;

    for (ui32 i = 0; i < readResult.ResultSize(); ++i) {
        auto& res = readResult.GetResult(i);
        if (res.GetOffset() == lastExpectedOffset && res.GetPartNo() == lastExpectedPartNo) {
            Cerr << "===CompactState got last part in blob on " << res.GetOffset() << ":" << res.GetPartNo() << Endl;
            break;
        }
        ++partsCount;
        if (!currentBatch) {
            currentBatch.ConstructInPlace(res.GetOffset(), res.GetPartNo());
        }

        TClientBlob blob{res.GetSourceId(), res.GetSeqNo(), std::move(res.GetData()),
                         Nothing(),
                         TInstant::MilliSeconds(res.GetWriteTimestampMS()), TInstant::MilliSeconds(res.GetCreateTimestampMS()),
                         res.GetUncompressedSize(), res.GetPartitionKey(), res.GetExplicitHash()};

        if (res.HasTotalParts()) {
            blob.PartData = TPartData{static_cast<ui16>(res.GetPartNo()), static_cast<ui16>(res.GetTotalParts()), res.GetTotalSize()};
        }
        TStringBuilder msg;
        msg << "===CompactState process part - " << res.GetOffset() << ":" << res.GetPartNo() << " from SeqNo: " << res.GetSeqNo() << "(total parts: " << res.GetTotalParts() <<", size: " << res.GetData().size() << "" << Endl;
        Cerr << msg;
        if (SkipOffset && res.GetOffset() == SkipOffset) { // skip parts of zeroed message
            currentBatch->AddBlob(std::move(blob));
            continue;
        }
        if (res.GetData().empty()) {
            SkipOffset = res.GetOffset();
            currentBatch->AddBlob(std::move(blob));
            continue;
        }
        hasNonZeroParts = hasNonZeroParts || res.GetData().size() > 0;

        if ((SavedLastProcessedOffset && res.GetOffset() <= SavedLastProcessedOffset)
            // These are parts of last message that we don't wan't to process
            || (CurrentMessage.Defined() && res.GetOffset() == CurrentMessage->GetOffset() && res.GetPartNo() <= CurrentMessage->GetPartNo())
             // We reached max offset and don't want to process more messages, but still need to add them to batch
             || res.GetOffset() >= MaxOffset
        ) {
            // This is either first parts of blob we processed before or parts of last message that we don't wan't to process,
            // so just add these to batch instantly.
            Cerr << "===CompactState - add blob, seqNo: " << blob.SeqNo << " to currentBatch part no " << blob.GetPartNo() << " out of total " << blob.GetTotalParts() << Endl;
            currentBatch->AddBlob(std::move(blob));
            continue;
        }

        bool haveTruncatedMessage = CurrentMessage.Defined() && CurrentMessage->GetTotalParts() > CurrentMessage->GetPartNo() + 1;
        bool isNewMsg = !res.HasPartNo() || res.GetPartNo() == 0;
        if (haveTruncatedMessage && isNewMsg) {
            // Probably previous message was deleted (do we really expect this to happen though?)
            // Drop it anyway.
            TStringBuilder msg; msg << "=== Got message from offset: " << res.GetOffset() << ":" << res.GetPartNo() << " when had truncated message at offset: " << CurrentMessage->GetOffset()
                << " part no: " << CurrentMessage->GetPartNo() << Endl; Cerr << msg;
            Y_ABORT();
            CurrentMessage = Nothing();
        }
        Y_ABORT_UNLESS(res.GetData().size() != 0);
        bool isLastPart = !res.HasTotalParts()
                          || res.GetTotalParts() == res.GetPartNo() + 1;


        Y_ABORT_UNLESS(res.GetData().size() != 0);
        if (isNewMsg) {
            Cerr << "===CompactState got first part of message on offset " << res.GetOffset() << ", part size: " << res.GetData().size() << " out of total " << res.GetTotalParts() << " parts\n";
            if (!isLastPart) {
                CurrentMessage.ConstructInPlace().CopyFrom(res);
            }
            // otherwise it's a single part message, will parse it in place
        } else { //glue to last res
            Cerr << "===CompactState internal part " << res.GetPartNo() << " of message on offset " << res.GetOffset() << ", part size: " << res.GetData().size() << Endl;
            Y_ABORT_UNLESS(CurrentMessage.Defined());
            if (CurrentMessage->GetSeqNo() != res.GetSeqNo()
                || CurrentMessage->GetPartNo() + 1 != res.GetPartNo()
            ) {
                PQ_LOG_CRIT("Partition compaction: Handle TEvRead last read pos (seqno/parno): " << CurrentMessage->GetSeqNo()
                             << "," << CurrentMessage->GetPartNo() << " readed now " << res.GetSeqNo()
                             << ", " << res.GetPartNo());
            }
            Y_ABORT_UNLESS(CurrentMessage->GetSeqNo() == res.GetSeqNo());
            (*CurrentMessage->MutableData()) += res.GetData();
            CurrentMessage->SetPartitionKey(res.GetPartitionKey());
            CurrentMessage->SetPartNo(res.GetPartNo());
            CurrentMessage->SetUncompressedSize(CurrentMessage->GetUncompressedSize() + res.GetUncompressedSize());

        }
        currentMessageBlobs.push_back(std::move(blob));

        if (isLastPart) {
            LastProcessedOffset = res.GetOffset();
            const auto& message = CurrentMessage.Defined() ? CurrentMessage.GetRef() : res;
            Y_ABORT_UNLESS(!message.HasTotalSize() || (ui32)message.GetTotalSize() == message.GetData().size());
            auto proto(GetDeserializedData(message.GetData()));
            if (proto.GetChunkType() != NKikimrPQClient::TDataChunk::REGULAR) {
                CurrentMessage = Nothing();
                Y_ABORT();
                continue; //no such chunks must be on prod - ?
            }
            auto offset = message.GetOffset();
            TString key;
            for (const auto& kv : proto.GetMessageMeta()) {
                if (kv.key() == "__key") {
                    key = kv.value();
                    break;
                }
            }
            auto iter = TopicData.find(key);
            bool keepMessage = (iter.IsEnd() || iter->second == offset);
            TStringBuilder msg; msg << "===CompactState - composed message with key '" << key << "' at offset " << offset << ", need keep = " << keepMessage << ", have last batch: "
                << LastBatch.Defined() << ", hanging parts: " << CurrMsgPartsFromLastBatch.size() << Endl; Cerr << msg;

            if (LastBatch) {
                if (!keepMessage) {
                    for (auto& blob: CurrMsgPartsFromLastBatch) {
                        ClearBlob(blob);
                    }
                }
                SaveLastBatch();
            }

            if (!keepMessage) {
                for (auto& key : CurrMsgMiggleBlobKeys) {
                    AddDeleteRange(key);
                }
                for (auto& blob: currentMessageBlobs) {
                    ClearBlob(blob);
                }
            }
            CurrMsgMiggleBlobKeys.clear();
            for (auto& blob: currentMessageBlobs) {
                Cerr << "===CompactState - add blob to currentBatch, seqNo: " << blob.SeqNo << " from current message to current Batch part no " << blob.GetPartNo() << " out of total " << blob.GetTotalParts() << Endl;
                currentBatch->AddBlob(std::move(blob));
            }
            currentMessageBlobs.clear();
            CurrentMessage = Nothing();
            Cerr << "===CompactState: clear last meesage blobs\n";
        }
    }
    Y_ENSURE(KeysIter->Key.GetInternalPartsCount() + KeysIter->Key.GetCount() == partsCount);
    if (!hasNonZeroParts) {
        EmptyBlobs.emplace(isTruncatedBlob ? lastExpectedOffset : lastExpectedOffset - 1, KeysIter->Key);
    }

    if (isMiddlePartOfMessage) {
        Cerr << "===CompactState - save currentKey " << KeysIter->Key.ToString() << " to CurrMsgMiggleBlobKeys\n ";
        CurrMsgMiggleBlobKeys.emplace_back(KeysIter->Key);
        KeysIter++;
        return true;
    }
    if (isTruncatedBlob) {
        TStringBuilder msg;
        msg << "===CompactState: drop currentMessageBlobs (" << currentMessageBlobs.size() << ") to LastMsgBlobs, current batch (" << currentBatch->Blobs.size() << ") to LastBatch\n";
        Cerr << msg;
        CurrMsgPartsFromLastBatch = std::move(currentMessageBlobs);
        LastBatchKey = KeysIter->Key;
        LastBatch = std::move(currentBatch);
        if (BlobsToWriteInRequest >= 3) {
            // Current message is not complete yet. But we cannot read another blob as KV WriteRequest may become too large.
            // Drop parts of last message as they are and keep Keys Iterator on same position - we will continue
            // from the same blob on next iteration.
            // Also keep current offset - so we know we ignore all parts of already processed messages on next iteration.
            SaveLastBatch();
            Cerr << "<Test>: Save last batch when have 3 blobs\n";
            return true;
        }
    } else {
        Cerr << "===CompactState: add CmdWrite with key: " << KeysIter->Key.ToString() << " from currentBatch\n";
        AddCmdWrite(KeysIter->Key, currentBatch.GetRef());
        currentBatch = Nothing();
    }
    KeysIter++; //Blob processed, go on.
    Cerr << "===Compact state: blob processed, got to next key: " << (KeysIter == DataKeysBody.end() ? TString{"None"} : KeysIter->Key.ToString());
    return true;
}

void TPartitionCompaction::TCompactState::AddDeleteRange(const TKey& key) {
    // Currently unused;
    //DroppedKeys.push_back(key);
    TStringBuilder msg;
    msg << "====CompactState: Delete key: " << key.ToString() << ". (" <<key.GetOffset() << ":" << key.GetPartNo() << ":" << key.GetCount() << ")" << Endl;
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
    DeletedKeys.emplace(key);
}

void TPartitionCompaction::TCompactState::RunKvRequest() {
    CurrentMessage.Clear();
    Cerr << "====CompactState: RunKvRequest\n";
    Y_ABORT_UNLESS(Request);
    Y_ABORT_UNLESS(!PartitionActor->CompacterKvRequestInflight);
    TVector<ui64> deleted;
    for (const auto&[offset, key] : EmptyBlobs) {
        if (SavedLastProcessedOffset >= offset) {
            AddDeleteRange(key);
            deleted.push_back(offset);
        }
    }
    for (auto offset : deleted) {
        EmptyBlobs.erase(offset);
    }
    Request->Record.SetCookie(static_cast<ui64>(ERequestCookie::CompactificationWrite));
    PartitionActor->SendCompacterWriteRequest(std::move(Request));
    BlobsToWriteInRequest = 0;
}


bool TPartitionCompaction::TCompactState::ProcessKVResponse(TEvKeyValue::TEvResponse::TPtr& ev) {
    Y_ABORT_UNLESS(!PartitionActor->CompacterKvRequestInflight);
    Cerr << "====CompactState: Process KVResponse\n";    auto& response = ev->Get()->Record;
    if (response.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
        PQ_LOG_CRIT("Partition compaction state: Got not OK KV response");
        return false;
    }
    if (response.DeleteRangeResultSize()) {
        for (ui32 i = 0; i < response.DeleteRangeResultSize(); ++i) {
            if (response.GetDeleteRangeResult(i).GetStatus() != NKikimrProto::OK) {
                PQ_LOG_CRIT("Partition compaction state: Got not OK DeleteRange response");
                return false;
            }
        }
    }

    if (response.WriteResultSize()) {
        for (ui32 i = 0; i < response.WriteResultSize(); ++i) {
            if (response.GetWriteResult(i).GetStatus() != NKikimrProto::OK) {
                PQ_LOG_CRIT("Partition compaction state: Got not OK Write response");
                return false;
            }
        }
    }
    if (LastProcessedOffset > CommittedOffset) {
        OffsetToCommit = LastProcessedOffset;
    }
    SavedLastProcessedOffset = LastProcessedOffset;
    UpdateDataKeysBody();
    return true;
}

void TPartitionCompaction::TCompactState::SendCommit(ui64 cookie) {
    if (OffsetToCommit.GetOrElse(0) <= CommittedOffset) {
        OffsetToCommit = Nothing();
        return;
    }
    Cerr << "===CompactState: send commit with offset = " << *OffsetToCommit << "and cookie = " << cookie << Endl;
    CommitCookie = cookie;
    auto ev = MakeHolder<TEvPQ::TEvSetClientInfo>(CommitCookie, CLIENTID_COMPACTION_CONSUMER, *OffsetToCommit, TString{}, 0, 0, 0, TActorId{});
    ev->IsInternal = true;
    PartitionActor->CompacterPartitionRequestInflight = true;
    PartitionActor->Send(PartitionActor->SelfId(), ev.Release());
}

void TPartitionCompaction::TCompactState::UpdateDataKeysBody() {
    Cerr << "====CompactState: UpdateDataKeysBody\n";
    Y_ABORT_UNLESS(UpdatedKeys || DeletedKeys);
    auto iterChng = UpdatedKeys.begin();
    auto iterDel = DeletedKeys.begin();

    std::deque<TDataKey> oldDataKeys = std::move(PartitionActor->DataKeysBody);
    auto iterExst = oldDataKeys.begin();
    PartitionActor->DataKeysBody.clear();
    ui64 currCumulSize = 0;
    ui64 zeroedKeys = 0;
    ui64 sizeDiff = 0;

    auto addCurrentKey = [&]() {
        iterExst->CumulativeSize = currCumulSize;
        currCumulSize += iterExst->Size;
        PartitionActor->DataKeysBody.emplace_back(std::move(*iterExst));
    };

    while (iterExst != oldDataKeys.end()) {
        TStringBuilder msg; msg << "====Compare keys. Current: " << iterExst->Key.GetOffset() << ":" << iterExst->Key.GetPartNo() << ", udpdated: ";
        if (iterChng != UpdatedKeys.end()) { msg << iterChng->first.GetOffset() << ":" << iterChng->first.GetPartNo(); } else { msg << " none";}
        msg << ", del: "; if (iterDel != DeletedKeys.end()) {msg << iterDel->GetOffset() << ":" << iterDel->GetPartNo(); } else { msg << " none";} msg << Endl; Cerr << msg;
        if (iterChng != UpdatedKeys.end() && iterChng->first == iterExst->Key) {
            Cerr << "====CompState: update key " << iterExst->Key.ToString() << Endl;
            sizeDiff += iterExst->Size - iterChng->second;
            iterExst->Size = iterChng->second;
            addCurrentKey();
            iterChng++;
        } else if (iterDel != DeletedKeys.end() && iterExst->Key == *iterDel) {
            Cerr << "====CompState: drop key " << iterExst->Key.ToString() << Endl;
            ++zeroedKeys;
            sizeDiff += iterExst->Size;
            iterDel++;
        } else {
            Y_ABORT_UNLESS(iterChng == UpdatedKeys.end() || iterChng->first.GetOffset() > iterExst->Key.GetOffset()
                || iterChng->first.GetOffset() == iterExst->Key.GetOffset() && iterChng->first.GetPartNo() > iterExst->Key.GetPartNo());
            Y_ABORT_UNLESS(iterDel == DeletedKeys.end() || iterDel->GetOffset() > iterExst->Key.GetOffset()
                || iterDel->GetOffset() == iterExst->Key.GetOffset() && iterDel->GetPartNo() > iterExst->Key.GetPartNo());
            addCurrentKey();
        }
        iterExst++;
    }
    Cerr << "====CompactState: UpdateDataKeysBody - end\n";

    Y_ENSURE(PartitionActor->DataKeysBody.size() == oldDataKeys.size() - zeroedKeys);
    Y_ENSURE(currCumulSize == PartitionActor->BodySize - sizeDiff);
    PartitionActor->BodySize = currCumulSize;
    PartitionActor->StartOffset = Max(
                    PartitionActor->StartOffset,
                    PartitionActor->DataKeysBody.front().Key.GetOffset() + (ui32)(PartitionActor->DataKeysBody.front().Key.GetPartNo() > 0));
    UpdatedKeys.clear();
    DeletedKeys.clear();
}

} // namespace NKikimr::NPQ