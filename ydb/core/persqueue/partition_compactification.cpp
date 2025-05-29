#include "partition.h"
#include "partition_log.h"
#include "write_meta.h"
#include "partition_util.h"

namespace NKikimr::NPQ {

TPartitionCompaction::TPartitionCompaction(ui64 firstUncompactedOffset, ui64 startCookie, ui64 endCookie, TPartition* partitionActor,
                                           ui64 readQuota)
    : QuotaTracker(std::make_shared<TQuotaTracker>(readQuota * 2, readQuota, TInstant::Now()))
    , FirstUncompactedOffset(firstUncompactedOffset)
    , StartCookie(startCookie)
    , EndCookie(endCookie)
    , PartitionActor(partitionActor)
{
    Cerr << "===Compacter created\n";
}

void TPartitionCompaction::TryCompactionIfPossible() {
    QuotaTracker->Update(Now());
    if (!PartitionActor->TabletConfig.GetEnableCompactification()) {
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
        auto step = ReadState->ContinueIfPossible(GetNextCookie()); //ToDo: track cookies;
        if (step == EStep::READING) {
            Cerr << "=== Try compaction, reading, keep reading\n";
            HaveRequestInflight = true;
            break;
        } else if (step == EStep::COMPACTING) {
            Cerr << "=== Try compaction, reading, go to compacting\n";
            Step = EStep::COMPACTING;
            CompactState = TCompactState(std::move(ReadState->GetData()), FirstUncompactedOffset, ReadState->GetLastOffset(), QuotaTracker, PartitionActor);
            ReadState = std::nullopt;
        } else {
            Step = step;
            Cerr << "=== Try compaction, reading, switch to pending?\n";
            break;
        }
        [[fallthrough]];
    }
    case EStep::COMPACTING: {
        if (!QuotaTracker->CanExaust(Now())) {
            break;
        }
        bool res = CompactState->ContinueIfPossible(0); // ToDo: track cookies;
        if (!res) {
            CompactState->RunPersistRequest();
            Step = EStep::PERSISTING;
            HaveRequestInflight = true;
            CompactState = std::nullopt;
            break;
        }
        break;
    }
    case EStep::PERSISTING: {
        // Just wait for request to finish and switch to Pending when ready
        break;
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
        case EStep::PENDING:
            Cerr << "=== Got response on pending, ignore\n";
            break;
        default:
            Y_ABORT();

    }
}
TPartitionCompaction::TReadState::TReadState(ui64 firstOffset, TPartition* partitionActor)
    : OffsetToRead(firstOffset)
    , PartitionActor(partitionActor)
{
    ui64 firstHeadOffset = 0;
    for (const auto& key: PartitionActor->BlobEncoder.HeadKeys) {
        Cerr << "===Have Head Key with offset: " << key.Key.GetOffset() << Endl;
        if (firstHeadOffset == 0 || key.Key.GetOffset() < firstHeadOffset) {
            firstHeadOffset = key.Key.GetOffset();
        }
    }

    if(PartitionActor->BlobEncoder.DataKeysBody.empty()) {
        LastOffset = firstOffset;
    } else if (firstHeadOffset) {
        LastOffset = firstHeadOffset - 1;
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
        Cerr << "Read state: bad response\n";
        return false;
        // Will retry the request next;
    }
    const auto& readResult = ev->Get()->Response->GetPartitionResponse().GetCmdReadResult();
    for (ui32 i = 0; i < readResult.ResultSize(); ++i) {
        bool haveTruncatedMessage = LastMessage.Defined() && LastMessage->GetTotalParts() > LastMessage->GetPartNo() + 1;
        bool isNewMsg = !readResult.GetResult(i).HasPartNo() || readResult.GetResult(i).GetPartNo() == 0;
        if (haveTruncatedMessage && isNewMsg) {
            // Probably previous message was probably deleted (do we really expect this to happen though?)
            // Drop it anyway.
            LastMessage = Nothing();
        }
        bool isLastPart = !readResult.GetResult(i).HasTotalParts()
                          || readResult.GetResult(i).GetTotalParts() == readResult.GetResult(i).GetPartNo() + 1;
        if (isNewMsg) {
            if (!isLastPart) {
                LastMessage.ConstructInPlace().CopyFrom(readResult.GetResult(i));
            }
            // otherwise it's a single part message, will parse it in place
        } else { //glue to last res
            Y_ENSURE(LastMessage.Defined());
            //auto rr = partResp->MutableResult(partResp->ResultSize() - 1);
            if (LastMessage->GetSeqNo() != readResult.GetResult(i).GetSeqNo()
                || LastMessage->GetPartNo() + 1 != readResult.GetResult(i).GetPartNo()
            ) {
                PQ_LOG_CRIT("Partition compaction: Handle TEvRead last read pos (seqno/parno): " << LastMessage->GetSeqNo()
                             << "," << LastMessage->GetPartNo() << " readed now " << readResult.GetResult(i).GetSeqNo()
                             << ", " << readResult.GetResult(i).GetPartNo());
            }
            Y_ENSURE(LastMessage->GetSeqNo() == readResult.GetResult(i).GetSeqNo());
            (*LastMessage->MutableData()) += readResult.GetResult(i).GetData();
            LastMessage->SetPartitionKey(readResult.GetResult(i).GetPartitionKey());
            LastMessage->SetPartNo(readResult.GetResult(i).GetPartNo());
            LastMessage->SetUncompressedSize(LastMessage->GetUncompressedSize() + readResult.GetResult(i).GetUncompressedSize());
            if (readResult.GetResult(i).GetPartNo() + 1 == readResult.GetResult(i).GetTotalParts()) {
                Y_ABORT_UNLESS((ui32)LastMessage->GetTotalSize() == (ui32)LastMessage->GetData().size());
            }
        }
        if (isLastPart) {
            const auto& message = LastMessage.Defined() ? LastMessage.GetRef() : readResult.GetResult(i);
            OffsetToRead = message.GetOffset() + 1;
            NextPartNo = 0;

            auto proto(GetDeserializedData(message.GetData()));
            if (proto.GetChunkType() != NKikimrPQClient::TDataChunk::REGULAR) {
                LastMessage = Nothing();
                continue; //TODO - no such chunks must be on prod
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
            if (key) {
                TopicData[key] = offset;
                Cerr << "===ReadState got key " << key << " at offset " << offset << ", current map size: " << TopicData.size() << Endl;
            }
            LastMessage = Nothing();
        } else {
            Y_ENSURE(LastMessage.Defined());
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
    Cerr << "=== === ReadState send EvRead with cookie " << nextRequestCookie << Endl;
    auto evRead = std::make_unique<TEvPQ::TEvRead>(
        nextRequestCookie,
        OffsetToRead,
        LastOffset,
        NextPartNo,
        LastOffset - OffsetToRead + 1,
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


bool TPartitionCompaction::TCompactState::ContinueIfPossible(ui64 nextRequestCookie) {
    Cerr << "=== Compact state: continue if possible\n";
    Y_UNUSED(nextRequestCookie);
    const auto& dataKeys = PartitionActor->BlobEncoder.CompactedKeys;
    for (const auto& [key, _] : dataKeys) {
        Cerr << "=== Compact state: key " << key.ToString() << "\n";
        if (key.GetOffset() <= NextOffset && key.GetOffset() + key.GetCount() >= NextOffset) {
            // This key contains current offset.
        }
    }

    Y_UNUSED(dataKeys);
    return true;
    //ToDo: !!
}

TPartitionCompaction::EStep TPartitionCompaction::TCompactState::ProcessResponse(TEvKeyValue::TEvResponse::TPtr& ev) {
    Y_UNUSED(ev);
    return EStep::PENDING;
    //ToDo: !!
}
void TPartitionCompaction::TCompactState::RunPersistRequest() {
    //ToDo: !!
}

} // namespace NKikimr::NPQ