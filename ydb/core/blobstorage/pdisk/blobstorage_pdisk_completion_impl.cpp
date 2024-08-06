#include "blobstorage_pdisk_completion_impl.h"
#include "blobstorage_pdisk_impl.h"
#include "blobstorage_pdisk_sectorrestorator.h"

constexpr size_t MAX_RESULTS_PER_BATCH = 50; // It took ~0.25ms in VDisk's handler to process such batch

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Completion actions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Log write completion action
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void TCompletionLogWrite::Exec(TActorSystem *actorSystem) {
    // bool isNewChunksCommited = false;
    if (CommitedLogChunks) {
        NWilson::TSpan span(TWilson::PDiskBasic, TraceId.Clone(), "PDisk.CommitLogChunks");
        auto* req = PDisk->ReqCreator.CreateFromArgs<TCommitLogChunks>(std::move(CommitedLogChunks), std::move(span));
        PDisk->InputRequest(req);
        //isNewChunksCommited = true;
    }
    for (auto it = Commits.begin(); it != Commits.end(); ++it) {
        TLogWrite *evLog = *it;
        Y_ABORT_UNLESS(evLog);
        if (evLog->Result->Status == NKikimrProto::OK) {
            TRequestBase *req = PDisk->ReqCreator.CreateFromArgs<TLogCommitDone>(*evLog);
            PDisk->InputRequest(req);
        }
    }

    auto sendResponse = [&] (TLogWrite *evLog) {
        Y_DEBUG_ABORT_UNLESS(evLog->Result);
        ui32 results = evLog->Result->Results.size();
        actorSystem->Send(evLog->Sender, evLog->Result.Release());
        PDisk->Mon.WriteLog.CountMultipleResponses(results);
    };

    THashMap<ui64, TLogWrite *> batchMap;
    NHPTimer::STime now = HPNow();
    for (auto it = LogWriteQueue.begin(); it != LogWriteQueue.end(); ++it) {
        TLogWrite &evLog = *(*it);
        TLogWrite *&batch = batchMap[evLog.Owner];
        LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDisk->PDiskId
                << " ReqId# " << evLog.ReqId.Id << " TEvLogResult Sender# " << evLog.Sender.LocalId()
                << " Lsn# " << evLog.Lsn << " Latency# " << evLog.LifeDurationMs(now)
                << " InputTime# " << HPMilliSeconds(evLog.InputTime - evLog.CreationTime)
                << " ScheduleTime# " << HPMilliSeconds(evLog.ScheduleTime - evLog.InputTime)
                << " DeviceTime# " << HPMilliSeconds(now - evLog.ScheduleTime)
                << " Size# " << evLog.Data.size());
        LWTRACK(PDiskLogWriteComplete, evLog.Orbit, PDisk->PDiskId, evLog.ReqId.Id, HPSecondsFloat(evLog.CreationTime),
                double(evLog.Cost) / 1000000.0,
                HPMilliSecondsFloat(now - evLog.CreationTime),
                HPMilliSecondsFloat(evLog.InputTime - evLog.CreationTime),
                HPMilliSecondsFloat(evLog.ScheduleTime - evLog.InputTime),
                HPMilliSecondsFloat(now - evLog.ScheduleTime),
                HPMilliSecondsFloat(GetTime - SubmitTime),
                batch ? batch->Result->Results.size() : 0);
        if (evLog.Result->Results) {
            evLog.Result->Results.front().Orbit = std::move(evLog.Orbit);
        }

        PDisk->Mon.LogResponseTime.Increment(evLog.LifeDurationMs(now));

        if (evLog.LogCallback) {
            (*evLog.LogCallback)(actorSystem, *evLog.Result);
        }
        if (evLog.Result->Status == NKikimrProto::OK) {
            if (batch) {
                if (batch->Sender == evLog.Sender && batch->Result->Results.size() < MAX_RESULTS_PER_BATCH) {
                    batch->Result->Results.push_back(std::move(evLog.Result->Results[0]));
                } else {
                    sendResponse(batch);
                    batch = &evLog;
                }
            } else {
                batch = &evLog;
            }
        } else {
            // Send all previous successes...
            if (batch) {
                sendResponse(batch);
            }
            batch = nullptr;
            // And only then - send the error.
            sendResponse(&evLog);
        }
    }

    for (auto &elem : batchMap) {
        if (elem.second) {
            sendResponse(elem.second);
        }
    }
    delete this;
}

void TCompletionLogWrite::Release(TActorSystem *actorSystem) {
    switch (Result) {
    case EIoResult::Ok:
    case EIoResult::Unknown:
        break;
    default:
        for (TLogWrite *logWrite : LogWriteQueue) {
            auto res = MakeHolder<TEvLogResult>(NKikimrProto::CORRUPTED, NKikimrBlobStorage::StatusIsValid,
                    ErrorReason);
            actorSystem->Send(logWrite->Sender, res.Release());
            PDisk->Mon.WriteLog.CountResponse();
        }
    }

    delete this;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Chunk read completion actions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TCompletionChunkReadPart::TCompletionChunkReadPart(TPDisk *pDisk, TIntrusivePtr<TChunkRead> &read, ui64 rawReadSize,
        ui64 payloadReadSize, ui64 commonBufferOffset, TCompletionChunkRead *cumulativeCompletion, bool isTheLastPart,
        NWilson::TSpan&& span)
    : TCompletionAction()
    , PDisk(pDisk)
    , Read(read)
    , RawReadSize(rawReadSize)
    , PayloadReadSize(payloadReadSize)
    , CommonBufferOffset(commonBufferOffset)
    , CumulativeCompletion(cumulativeCompletion)
    , Buffer(PDisk->BufferPool->Pop())
    , IsTheLastPart(isTheLastPart)
    , Span(std::move(span))
{
    if (!IsTheLastPart) {
        CumulativeCompletion->AddPart();
    }
}

TCompletionChunkReadPart::~TCompletionChunkReadPart() {
    if (CumulativeCompletion) {
        CumulativeCompletion->PartDeleted(PDisk->ActorSystem);
    }
    AtomicSub(PDisk->InFlightChunkRead, RawReadSize);
}

TBuffer *TCompletionChunkReadPart::GetBuffer() {
    return Buffer.Get();
}

void TCompletionChunkReadPart::Exec(TActorSystem *actorSystem) {
    auto execSpan = Span.CreateChild(TWilson::PDiskDetailed, "PDisk.CompletionChunkReadPart.Exec");
    Y_ABORT_UNLESS(actorSystem);
    Y_ABORT_UNLESS(CumulativeCompletion);
    if (TCompletionAction::Result != EIoResult::Ok) {
        Release(actorSystem);
        return;
    }

    const TDiskFormat &format = PDisk->Format;

    ui64 firstSector;
    ui64 lastSector;
    ui64 sectorOffset;
    bool isOk = ParseSectorOffset(PDisk->Format, actorSystem, PDisk->PDiskId,
            Read->Offset + CommonBufferOffset, PayloadReadSize, firstSector, lastSector, sectorOffset);
    Y_ABORT_UNLESS(isOk);

    TBufferWithGaps *commonBuffer = CumulativeCompletion->GetCommonBuffer();
    ui8 *destination = commonBuffer->RawDataPtr(CommonBufferOffset, PayloadReadSize);

    ui8* source = Buffer->Data();

    TPDiskStreamCypher cypher(PDisk->Cfg->EnableSectorEncryption);
    cypher.SetKey(format.ChunkKey);
    ui64 sectorIdx = firstSector;

    ui32 sectorPayloadSize;
    if (CommonBufferOffset == 0) { // First part
        sectorPayloadSize = Min(format.SectorPayloadSize() - sectorOffset, PayloadReadSize);
    } else { // Middle and last parts
        sectorPayloadSize = Min(format.SectorPayloadSize(), PayloadReadSize);
        sectorOffset = 0;
    }

    ui64 chunkNonce = CumulativeCompletion->GetChunkNonce();

    ui32 beginBadUserOffset = 0xffffffff;
    ui32 endBadUserOffset = 0xffffffff;
    ui32 userSectorSize = format.SectorPayloadSize();
    while (PayloadReadSize > 0) {
        ui32 beginUserOffset = sectorIdx * userSectorSize;

        TSectorRestorator restorator(false, 1, false,
            format, actorSystem, PDisk->PDiskActor, PDisk->PDiskId, &PDisk->Mon, PDisk->BufferPool.Get());
        ui64 lastNonce = Min((ui64)0, chunkNonce - 1);
        restorator.Restore(source, format.Offset(Read->ChunkIdx, sectorIdx), format.MagicDataChunk, lastNonce,
                Read->Owner);

        const ui32 sectorCount = 1;
        if (restorator.GoodSectorCount != sectorCount) {
            if (beginBadUserOffset == 0xffffffff) {
                beginBadUserOffset = beginUserOffset;
            }
            endBadUserOffset = beginUserOffset + userSectorSize;
        } else {
            if (beginBadUserOffset != 0xffffffff) {
                LOG_INFO_S(*actorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << (ui32)PDisk->PDiskId
                        << " ReqId# " << Read->ReqId
                        << " Can't read chunk chunkIdx# " << Read->ChunkIdx
                        << " for owner# " << Read->Owner
                        << " beginBadUserOffet# " << beginBadUserOffset << " endBadUserOffset# " << endBadUserOffset
                        << " due to multiple sectors with incorrect hashes. Marker# BPC001");
                commonBuffer->AddGap(beginBadUserOffset, endBadUserOffset);
                beginBadUserOffset = 0xffffffff;
                endBadUserOffset = 0xffffffff;
            }
        }

        Y_ABORT_UNLESS(sectorIdx >= firstSector);

        // Decrypt data
        if (beginBadUserOffset != 0xffffffff) {
            memset(destination, 0, sectorPayloadSize);
        } else {
            TDataSectorFooter *footer = (TDataSectorFooter*) (source + format.SectorSize - sizeof(TDataSectorFooter));
            if (footer->Nonce != chunkNonce + sectorIdx) {
                ui32 userOffset = sectorIdx * userSectorSize;
                LOG_INFO_S(*actorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << (ui32)PDisk->PDiskId
                        << " ReqId# " << Read->ReqId
                        << " Can't read chunk chunkIdx# " << Read->ChunkIdx
                        << " for owner# " << Read->Owner
                        << " nonce mismatch: expected# " << (ui64)(chunkNonce + sectorIdx)
                        << ", on-disk# " << (ui64)footer->Nonce
                        << " for userOffset# " << userOffset
                        << " ! Marker# BPC002");
                if (beginBadUserOffset == 0xffffffff) {
                    beginBadUserOffset = userOffset;
                }
                endBadUserOffset = beginUserOffset + userSectorSize;
                memset(destination, 0, sectorPayloadSize);
            } else {
                cypher.StartMessage(footer->Nonce);
                if (sectorOffset > 0 || intptr_t(destination) % 32) {
                    cypher.InplaceEncrypt(source, sectorOffset + sectorPayloadSize);
                    if (CommonBufferOffset == 0 || !IsTheLastPart) {
                        memcpy(destination, source + sectorOffset, sectorPayloadSize);
                    } else {
                        memcpy(destination, source, sectorPayloadSize);
                    }
                } else {
                    cypher.Encrypt(destination, source, sectorPayloadSize);
                }
                if (CanarySize > 0) {
                    ui32 canaryPosition = sectorOffset + sectorPayloadSize;
                    ui32 sizeToEncrypt = format.SectorSize - canaryPosition - ui32(sizeof(TDataSectorFooter));
                    cypher.InplaceEncrypt(source + canaryPosition, sizeToEncrypt);
                    PDisk->CheckLogCanary(source, Read->ChunkIdx, sectorIdx);
                }
            }
        }
        destination += sectorPayloadSize;
        source += format.SectorSize;
        PayloadReadSize -= sectorPayloadSize;
        sectorPayloadSize = Min(format.SectorPayloadSize(), PayloadReadSize);
        sectorOffset = 0;
        ++sectorIdx;
    }
    if (beginBadUserOffset != 0xffffffff) {
        LOG_INFO_S(*actorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << (ui32)PDisk->PDiskId
            << " ReqId# " << Read->ReqId
            << " Can't read chunk chunkIdx# " << Read->ChunkIdx
            << " for owner# " << Read->Owner
            << " beginBadUserOffet# " << beginBadUserOffset << " endBadUserOffset# " << endBadUserOffset
            << " due to multiple sectors with incorrect hashes/nonces. Marker# BPC003");
        commonBuffer->AddGap(beginBadUserOffset, endBadUserOffset);
        beginBadUserOffset = 0xffffffff;
        endBadUserOffset = 0xffffffff;
    }

    double deviceTimeMs = HPMilliSecondsFloat(GetTime - SubmitTime);
    LWTRACK(PDiskChunkReadPieceComplete, Read->Orbit, PDisk->PDiskId, RawReadSize, CommonBufferOffset, deviceTimeMs);
    CumulativeCompletion->PartReadComplete(actorSystem);
    CumulativeCompletion = nullptr;

    AtomicSub(PDisk->InFlightChunkRead, RawReadSize);
    RawReadSize = 0;
    execSpan.EndOk();
    Span.EndOk();
    delete this;
}

void TCompletionChunkReadPart::Release(TActorSystem *actorSystem) {
    if (CumulativeCompletion) {
        CumulativeCompletion->PartDeleted(actorSystem);
        CumulativeCompletion = nullptr;
    }
    AtomicSub(PDisk->InFlightChunkRead, RawReadSize);
    RawReadSize = 0;
    Span.EndError("release");
    delete this;
}

TCompletionChunkRead::~TCompletionChunkRead() {
    OnDestroy();
    Y_ABORT_UNLESS(CommonBuffer.Empty());
    Y_ABORT_UNLESS(DoubleFreeCanary == ReferenceCanary, "DoubleFreeCanary in TCompletionChunkRead is dead!");
    // Set DoubleFreeCanary to 0 and make sure compiler will not eliminate that action
    SecureWipeBuffer((ui8*)&DoubleFreeCanary, sizeof(DoubleFreeCanary));
}

void TCompletionChunkRead::Exec(TActorSystem *actorSystem) {
    auto execSpan = Span.CreateChild(TWilson::PDiskDetailed, "PDisk.CompletionChunkRead.Exec");
    THolder<TEvChunkReadResult> result = MakeHolder<TEvChunkReadResult>(NKikimrProto::OK,
        Read->ChunkIdx, Read->Offset, Read->Cookie, PDisk->GetStatusFlags(Read->Owner, Read->OwnerGroupType), "");
    result->Data = std::move(CommonBuffer);
    CommonBuffer.Clear();
    //Y_ABORT_UNLESS(result->Data.IsDetached());

    result->Data.Commit();

    Y_ABORT_UNLESS(Read);
    LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDisk->PDiskId << " ReqId# " << Read->ReqId.Id
            << " " << result->ToString() << " To# " << Read->Sender.LocalId());

    double responseTimeMs = HPMilliSecondsFloat(HPNow() - Read->CreationTime);
    PDisk->Mon.IncrementResponseTime(Read->PriorityClass, responseTimeMs, Read->Size);
    LWTRACK(PDiskChunkResponseTime, Read->Orbit, PDisk->PDiskId, Read->ReqId.Id, Read->PriorityClass, responseTimeMs,
            Read->Size);

    actorSystem->Send(Read->Sender, result.Release());
    Read->IsReplied = true;
    PDisk->Mon.GetReadCounter(Read->PriorityClass)->CountResponse();
    execSpan.EndOk();
    Span.EndOk();
    delete this;
}

void TCompletionChunkRead::ReplyError(TActorSystem *actorSystem, TString reason) {
    Y_ABORT_UNLESS(!Read->IsReplied);
    CommonBuffer.Clear();

    TStringStream error;
    error << "PDiskId# " << PDisk->PDiskId << " ReqId# " << Read->ReqId << " reason# " << reason;
    auto result = MakeHolder<TEvChunkReadResult>(NKikimrProto::CORRUPTED,
            Read->ChunkIdx, Read->Offset, Read->Cookie,
            PDisk->GetStatusFlags(Read->Owner, Read->OwnerGroupType), error.Str());

    result->Data.SetDebugInfoGenerator(PDisk->DebugInfoGenerator);

    LOG_WARN_S(*actorSystem, NKikimrServices::BS_PDISK, error.Str());
    actorSystem->Send(Read->Sender, result.Release());
    Read->IsReplied = true;
}

// Returns true if there is some pending requests to wait
bool TCompletionChunkRead::PartReadComplete(TActorSystem *actorSystem) {
    TAtomicBase partsPending = AtomicDecrement(PartsPending);
    if (partsPending == 0) {
        if (AtomicGet(Deletes) == 0) {
            Exec(actorSystem);
        } else {
            ReplyError(actorSystem, "One of ChunkReadPart failed due to unknown reason");
            delete this;
        }
        return true;
    } else {
        return false;
    }
}

void TCompletionEventSender::Exec(TActorSystem *actorSystem) {
    if (actorSystem) {
        if (Event) {
            LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_PDISK, "TCompletionEventSender " << Event->ToString());
        } else {
            LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_PDISK, "TCompletionEventSender no event");
        }
    }
    if (Event) {
        actorSystem->Send(Recipient, Event.Release());
    }
    if (Req) {
        PDisk->InputRequest(Req.Release());
    }
    if (Counter) {
        Counter->Inc();
    }
    delete this;
}

void TChunkTrimCompletion::Exec(TActorSystem *actorSystem) {
    double responseTimeMs = HPMilliSecondsFloat(HPNow() - StartTime);
    LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_PDISK,
            "PDiskId# " << PDisk->PDiskId << " ReqId# " << ReqId
            << " TChunkTrimCompletion timeMs# "
            << ui64(responseTimeMs) << " sizeBytes# " << SizeBytes);
    LWPROBE(PDiskTrimResponseTime, PDisk->PDiskId, ReqId.Id, responseTimeMs, SizeBytes);
    PDisk->Mon.Trim.CountResponse();
    NWilson::TSpan span(TWilson::PDiskBasic, std::move(TraceId), "PDisk.TryTrimChunk", NWilson::EFlags::AUTO_END, actorSystem);
    span.Attribute("size", static_cast<i64>(SizeBytes));
    TTryTrimChunk *tryTrim = PDisk->ReqCreator.CreateFromArgs<TTryTrimChunk>(SizeBytes, std::move(span));
    PDisk->InputRequest(tryTrim);
    delete this;
}

} // NPDisk
} // NKikimr

