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
    , PartitionActor(partitionActor)
{
}

void TPartitionCompaction::TryCompactionIfPossible() {
    Y_ENSURE(PartitionActor->Config.GetEnableCompactification());
    if (PartitionActor->CompacterPartitionRequestInflight || PartitionActor->CompacterKvRequestInflight)
        return;
    FirstUncompactedOffset = Max(PartitionActor->StartOffset, FirstUncompactedOffset);
    switch (Step) {
    case EStep::PENDING:
        ReadState = TReadState(FirstUncompactedOffset, PartitionActor);
        Step = EStep::READING;
        [[fallthrough]];
    case EStep::READING: {
        auto step = ReadState->ContinueIfPossible(PartRequestCookie);
        Step = step;

        if (step == EStep::READING) {
            break;
        } else if (step == EStep::COMPACTING) {
            Step = EStep::COMPACTING;
            CompactState.ConstructInPlace(std::move(ReadState->GetData()), FirstUncompactedOffset, ReadState->GetLastOffset(), PartitionActor);
            ReadState.Clear();
        } else {
            break;
        }
        [[fallthrough]];
    }
    case EStep::COMPACTING: {
        auto step = CompactState->ContinueIfPossible(PartRequestCookie);
        Step = step;
        if (step == EStep::COMPACTING) {
            break;
        } else {
            CompactState.Clear();
        }
    }
    }
}

void TPartitionCompaction::ProcessResponse(TEvPQ::TEvError::TPtr& ev) {
    PQ_LOG_ERROR("Compaction for topic '" << PartitionActor->TopicConverter->GetClientsideName() << ", partition: "
                                      << PartitionActor->Partition << " proxy ERROR response: " << ev->Get()->Error);
    PartitionActor->Send(PartitionActor->Tablet, new TEvents::TEvPoisonPill());
    Step = EStep::PENDING;
    return;
}

void TPartitionCompaction::ProcessResponse(TEvPQ::TEvProxyResponse::TPtr& ev) {
    PQ_LOG_D("Compaction for topic '" << PartitionActor->TopicConverter->GetClientsideName() << ", partition: "
              << PartitionActor->Partition << " proxy response cookie: " << ev->Get()->Cookie);
    if (ev->Get()->Cookie != PartRequestCookie) {
        return;
    }
    bool processResponseResult = true;
    switch (Step) {
        case EStep::READING: {
            Y_ABORT_UNLESS(ReadState);
            processResponseResult = ReadState->ProcessResponse(ev);
            break;
        }
        case EStep::COMPACTING: {
            Y_ABORT_UNLESS(CompactState);
            processResponseResult = CompactState->ProcessResponse(ev);
            if (CompactState->SavedLastProcessedOffset > FirstUncompactedOffset) {
                FirstUncompactedOffset = CompactState->SavedLastProcessedOffset;
            }
            break;
        }
        case EStep::PENDING:
            break;
        default:
            Y_ABORT();
    }
    if (!processResponseResult) {
        PartitionActor->Send(PartitionActor->Tablet, new TEvents::TEvPoisonPill());
    }
    TryCompactionIfPossible();
}

void TPartitionCompaction::ProcessResponse(TEvKeyValue::TEvResponse::TPtr& ev) {
    //Partition must reset this flag;
    Y_ABORT_UNLESS(!PartitionActor->CompacterKvRequestInflight);
    PQ_LOG_D("Compaction for topic '" << PartitionActor->TopicConverter->GetClientsideName() << ", partition: "
              << PartitionActor->Partition << " Process KV response");
    if (CompactState) {
        if (!CompactState->ProcessKVResponse(ev)) {
            PQ_LOG_ERROR("Compaction for topic '" << PartitionActor->TopicConverter->GetClientsideName() << ", partition: "
                                              << PartitionActor->Partition << " Process KV response: BAD Status");

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
        if (firstHeadOffset == 0 || key.Key.GetOffset() < firstHeadOffset) {
            firstHeadOffset = key.Key.GetOffset();
        }
    }

    if(partitionActor->DataKeysBody.empty()) {
        LastOffset = firstOffset;
    } else if (firstHeadOffset) {
        LastOffset = firstHeadOffset;
    }
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
    if (CheckResponse(ev) >= 2) // Expect to have OK status and partition response;
    {
        // empty?
    } else {
        return false;
        // Will retry the request next;
    }
    const auto& readResult = ev->Get()->Response->GetPartitionResponse().GetCmdReadResult();
    for (ui32 i = 0; i < readResult.ResultSize(); ++i) {
        auto& res = readResult.GetResult(i);
        if (SkipOffset && res.GetOffset() == SkipOffset) {
            continue;
        }
        if (res.GetData().size() == 0) {
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
    auto evRead = MakeEvRead(nextRequestCookie, OffsetToRead, LastOffset, NextPartNo);
    PartitionActor->Send(PartitionActor->SelfId(), evRead.release());
    PQ_LOG_D("Compaction for topic '" << PartitionActor->TopicConverter->GetClientsideName() << ", partition: "
              << PartitionActor->Partition << " Send EvRead (Read state) from offset: " << OffsetToRead << ":" << NextPartNo);
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
    , LastProcessedOffset(partitionActor->StartOffset)
    , SavedLastProcessedOffset(partitionActor->StartOffset)
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

    KeysIter = DataKeysBody.begin();
}

TPartitionCompaction::EStep TPartitionCompaction::TCompactState::ContinueIfPossible(ui64 nextRequestCookie) {
    if (Failure) {
        return EStep::PENDING;
    }
    Y_ABORT_UNLESS(!PartitionActor->CompacterPartitionRequestInflight && !PartitionActor->CompacterKvRequestInflight);

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
        auto evRead = MakeEvRead(nextRequestCookie, currKey.GetOffset(), maxBlobOffset + 1, currKey.GetPartNo());
        PartitionActor->Send(PartitionActor->SelfId(), evRead.release());
        PQ_LOG_D("Compaction for topic '" << PartitionActor->TopicConverter->GetClientsideName() << ", partition: "
                  << PartitionActor->Partition << " Send EvRead (Compact state) from offset: " << currKey.GetOffset() << ":" << currKey.GetPartNo());
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
    if (!Request) {
        Request = MakeHolder<TEvKeyValue::TEvRequest>();
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

    if (blob.PartData) {
        blob.PartData->TotalSize = 0;
    }
}

void TPartitionCompaction::TCompactState::SaveLastBatch() {
    if (!LastBatch)
        return;

    for (auto& blob : CurrMsgPartsFromLastBatch) {
        LastBatch->AddBlob(std::move(blob));
    }
    AddCmdWrite(LastBatchKey, LastBatch.GetRef());
    LastBatch = Nothing();
    CurrMsgPartsFromLastBatch.clear();
    LastBatchKey = TKey();
}


bool TPartitionCompaction::TCompactState::ProcessResponse(TEvPQ::TEvProxyResponse::TPtr& ev) {
    auto status = CheckResponse(ev);
    if (!status) {
        return false;
        // Will retry the request next;
    }
    if (ev->Get()->Cookie == CommitCookie) {
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

    ui32 partsCount = 0;
    TMaybe<TBatch> currentBatch;
    TVector<TClientBlob> currentMessageBlobs;
    bool hasNonZeroParts = false;
    PQ_LOG_D("Compaction for topic '" << PartitionActor->TopicConverter->GetClientsideName() << ", partition: "
                                      << PartitionActor->Partition << " process read result in CompState starting from: "
                                      << readResult.GetResult(0).GetOffset() << ":" << readResult.GetResult(0).GetPartNo());
    for (ui32 i = 0; i < readResult.ResultSize(); ++i) {
        auto& res = readResult.GetResult(i);
        if (res.GetOffset() == lastExpectedOffset && res.GetPartNo() == lastExpectedPartNo) {
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
            currentBatch->AddBlob(std::move(blob));
            continue;
        }

        bool haveTruncatedMessage = CurrentMessage.Defined() && CurrentMessage->GetTotalParts() > CurrentMessage->GetPartNo() + 1;
        bool isNewMsg = !res.HasPartNo() || res.GetPartNo() == 0;
        if (haveTruncatedMessage && isNewMsg) {
            // Probably previous message was deleted (do we really expect this to happen though?)
            // Drop it anyway.
            Y_ABORT();
            CurrentMessage = Nothing();
        }
        Y_ABORT_UNLESS(res.GetData().size() != 0);
        bool isLastPart = !res.HasTotalParts()
                          || res.GetTotalParts() == res.GetPartNo() + 1;


        Y_ABORT_UNLESS(res.GetData().size() != 0);
        if (isNewMsg) {
            if (!isLastPart) {
                CurrentMessage.ConstructInPlace().CopyFrom(res);
            }
            // otherwise it's a single part message, will parse it in place
        } else { //glue to last res
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
                currentBatch->AddBlob(std::move(blob));
            }
            currentMessageBlobs.clear();
            CurrentMessage = Nothing();
        }
    }
    Y_ENSURE(KeysIter->Key.GetInternalPartsCount() + KeysIter->Key.GetCount() == partsCount);
    if (!hasNonZeroParts) {
        EmptyBlobs.emplace(isTruncatedBlob ? lastExpectedOffset : lastExpectedOffset - 1, KeysIter->Key);
    }

    if (isMiddlePartOfMessage) {
        CurrMsgMiggleBlobKeys.emplace_back(KeysIter->Key);
        KeysIter++;
        return true;
    }
    if (isTruncatedBlob && hasNonZeroParts) {
        CurrMsgPartsFromLastBatch = std::move(currentMessageBlobs);
        LastBatchKey = KeysIter->Key;
        LastBatch = std::move(currentBatch);
        if (BlobsToWriteInRequest >= 3) {
            // Current message is not complete yet. But we cannot read another blob as KV WriteRequest may become too large.
            // Drop parts of last message as they are and keep Keys Iterator on same position - we will continue
            // from the same blob on next iteration.
            // Also keep current offset - so we know we ignore all parts of already processed messages on next iteration.
            SaveLastBatch();
            return true;
        }
    } else {
        AddCmdWrite(KeysIter->Key, currentBatch.GetRef());
        currentBatch = Nothing();
    }
    KeysIter++; //Blob processed, go on.
    return true;
}

void TPartitionCompaction::TCompactState::AddDeleteRange(const TKey& key) {
    // Currently unused;
    //DroppedKeys.push_back(key);
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
    auto& response = ev->Get()->Record;
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
    if (LastProcessedOffset) {
        if (LastProcessedOffset > CommittedOffset) {
            OffsetToCommit = LastProcessedOffset;
        }
        SavedLastProcessedOffset = *LastProcessedOffset;
    }

    UpdateDataKeysBody();
    return true;
}

void TPartitionCompaction::TCompactState::SendCommit(ui64 cookie) {
    if (OffsetToCommit.GetOrElse(0) <= CommittedOffset) {
        OffsetToCommit = Nothing();
        return;
    }
    CommitCookie = cookie;
    auto ev = MakeHolder<TEvPQ::TEvSetClientInfo>(CommitCookie, CLIENTID_COMPACTION_CONSUMER, *OffsetToCommit, TString{}, 0, 0, 0, TActorId{});
    ev->IsInternal = true;
    PQ_LOG_D("Compaction for topic '" << PartitionActor->TopicConverter->GetClientsideName() << ", partition: "
              << PartitionActor->Partition << " commit offset: " << *OffsetToCommit);
    PartitionActor->CompacterPartitionRequestInflight = true;
    PartitionActor->Send(PartitionActor->SelfId(), ev.Release());
}

void TPartitionCompaction::TCompactState::UpdateDataKeysBody() {
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
        if (iterChng != UpdatedKeys.end() && iterChng->first == iterExst->Key) {
            sizeDiff += iterExst->Size - iterChng->second;
            iterExst->Size = iterChng->second;
            addCurrentKey();
            iterChng++;
        } else if (iterDel != DeletedKeys.end() && iterExst->Key == *iterDel) {
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