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
        auto* req = PDisk->ReqCreator.CreateFromArgs<TCommitLogChunks>(std::move(CommitedLogChunks));
        PDisk->InputRequest(req);
        //isNewChunksCommited = true;
    }
    for (auto it = Commits.begin(); it != Commits.end(); ++it) {
        TLogWrite *evLog = *it;
        Y_VERIFY(evLog);
        if (evLog->Result->Status == NKikimrProto::OK) {
            TRequestBase *req = PDisk->ReqCreator.CreateFromArgs<TLogCommitDone>(*evLog);
            PDisk->InputRequest(req);
        }
    }

    auto sendResponse = [&] (TLogWrite *evLog) {
        Y_VERIFY_DEBUG(evLog->Result);
        ui32 results = evLog->Result->Results.size();
        actorSystem->Send(evLog->Sender, evLog->Result.Release());
        PDisk->Mon.WriteLog.CountMultipleResponses(results);
    };

    THashMap<ui64, TLogWrite *> batchMap;
    NHPTimer::STime now = HPNow();
    for (auto it = LogWriteQueue.begin(); it != LogWriteQueue.end(); ++it) {
        TLogWrite &evLog = *(*it);
        evLog.Replied = true;
        TLogWrite *&batch = batchMap[evLog.Owner];
        LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDisk->PCtx->PDiskId
                << " ReqId# " << evLog.ReqId.Id << " TEvLogResult Sender# " << evLog.Sender.LocalId()
                << " Lsn# " << evLog.Lsn << " Latency# " << evLog.LifeDurationMs(now)
                << " InputTime# " << HPMilliSeconds(evLog.InputTime - evLog.CreationTime)
                << " ScheduleTime# " << HPMilliSeconds(evLog.ScheduleTime - evLog.InputTime)
                << " DeviceTime# " << HPMilliSeconds(now - evLog.ScheduleTime)
                << " Size# " << evLog.Data.size());
        LWTRACK(PDiskLogWriteComplete, evLog.Orbit, PDisk->PCtx->PDiskId, evLog.ReqId.Id, HPSecondsFloat(evLog.CreationTime),
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
            evLog.Span.EndOk();
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
            evLog.Span.EndError(evLog.Result->ErrorReason);
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
    for (TLogWrite *logWrite : LogWriteQueue) {
        auto res = MakeHolder<TEvLogResult>(NKikimrProto::CORRUPTED,
            NKikimrBlobStorage::StatusIsValid, ErrorReason, PDisk->Keeper.GetLogChunkCount());
        logWrite->Replied = true;
        actorSystem->Send(logWrite->Sender, res.Release());
        PDisk->Mon.WriteLog.CountResponse();
    }

    delete this;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Chunk read completion actions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TCompletionChunkReadPart::TCompletionChunkReadPart(TPDisk *pDisk, TIntrusivePtr<TChunkRead> &read, ui64 rawReadSize,
        ui64 payloadReadSize, ui64 commonBufferOffset, TCompletionChunkRead *cumulativeCompletion, bool isTheLastPart)
    : TCompletionAction()
    , PDisk(pDisk)
    , Read(read)
    , RawReadSize(rawReadSize)
    , PayloadReadSize(payloadReadSize)
    , CommonBufferOffset(commonBufferOffset)
    , CumulativeCompletion(cumulativeCompletion)
    , ChunkNonce(CumulativeCompletion->GetChunkNonce())
    , Buffer(PDisk->BufferPool->Pop())
    , IsTheLastPart(isTheLastPart)
    , Span(read->Span.CreateChild(TWilson::PDiskDetailed, "PDisk.ChunkRead.CompletionPart"))
{
    TCompletionAction::CanBeExecutedInAdditionalCompletionThread = true;

    Span.Attribute("common_buffer_offset", (i64)CommonBufferOffset)
        .Attribute("raw_read_size", RawReadSize)
        .Attribute("is_last_piece", IsTheLastPart);

    TBufferWithGaps *commonBuffer = CumulativeCompletion->GetCommonBuffer();
    Destination = commonBuffer->RawDataPtr(CommonBufferOffset, PayloadReadSize);

    if (!IsTheLastPart) {
        CumulativeCompletion->AddPart();
    }
}

TCompletionChunkReadPart::~TCompletionChunkReadPart() {
    if (CumulativeCompletion) {
        CumulativeCompletion->PartDeleted(PDisk->PCtx->ActorSystem);
    }
    AtomicSub(PDisk->InFlightChunkRead, RawReadSize);
}

TBuffer *TCompletionChunkReadPart::GetBuffer() {
    return Buffer.Get();
}

void TCompletionChunkReadPart::UnencryptData(TActorSystem *actorSystem) {
    const TDiskFormat &format = PDisk->Format;

    ui64 firstSector;
    ui64 lastSector;
    ui64 sectorOffset;
    bool isOk = ParseSectorOffset(PDisk->Format, actorSystem, PDisk->PCtx->PDiskId,
            Read->Offset + CommonBufferOffset, PayloadReadSize, firstSector, lastSector, sectorOffset,
            PDisk->PCtx->PDiskLogPrefix);
    Y_VERIFY(isOk);

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

    ui32 beginBadUserOffset = 0xffffffff;
    ui32 endBadUserOffset = 0xffffffff;
    ui32 userSectorSize = format.SectorPayloadSize();
    while (PayloadReadSize > 0) {
        ui32 beginUserOffset = sectorIdx * userSectorSize;

        TSectorRestorator restorator(false, 1, false,
            format, PDisk->PCtx.get(), &PDisk->Mon, PDisk->BufferPool.Get());
        ui64 lastNonce = Min((ui64)0, ChunkNonce - 1);
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
                LOG_INFO_S(*actorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDisk->PCtx->PDiskId
                        << " ReqId# " << Read->ReqId
                        << " Can't read chunk chunkIdx# " << Read->ChunkIdx
                        << " for owner# " << Read->Owner
                        << " beginBadUserOffet# " << beginBadUserOffset << " endBadUserOffset# " << endBadUserOffset
                        << " due to multiple sectors with incorrect hashes. Marker# BPC001");
                CumulativeCompletion->AddGap(beginBadUserOffset, endBadUserOffset);
                beginBadUserOffset = 0xffffffff;
                endBadUserOffset = 0xffffffff;
            }
        }

        Y_VERIFY(sectorIdx >= firstSector);

        // Decrypt data
        if (beginBadUserOffset != 0xffffffff) {
            memset(Destination, 0, sectorPayloadSize);
        } else {
            TDataSectorFooter *footer = (TDataSectorFooter*) (source + format.SectorSize - sizeof(TDataSectorFooter));
            if (footer->Nonce != ChunkNonce + sectorIdx) {
                ui32 userOffset = sectorIdx * userSectorSize;
                LOG_INFO_S(*actorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDisk->PCtx->PDiskId
                        << " ReqId# " << Read->ReqId
                        << " Can't read chunk chunkIdx# " << Read->ChunkIdx
                        << " for owner# " << Read->Owner
                        << " nonce mismatch: expected# " << (ui64)(ChunkNonce + sectorIdx)
                        << ", on-disk# " << (ui64)footer->Nonce
                        << " for userOffset# " << userOffset
                        << " ! Marker# BPC002");
                if (beginBadUserOffset == 0xffffffff) {
                    beginBadUserOffset = userOffset;
                }
                endBadUserOffset = beginUserOffset + userSectorSize;
                memset(Destination, 0, sectorPayloadSize);
            } else {
                cypher.StartMessage(footer->Nonce);
                if (sectorOffset > 0 || intptr_t(Destination) % 32) {
                    cypher.InplaceEncrypt(source, sectorOffset + sectorPayloadSize);
                    if (CommonBufferOffset == 0 || !IsTheLastPart) {
                        memcpy(Destination, source + sectorOffset, sectorPayloadSize);
                    } else {
                        memcpy(Destination, source, sectorPayloadSize);
                    }
                } else {
                    cypher.Encrypt(Destination, source, sectorPayloadSize);
                }
                if (CanarySize > 0) {
                    ui32 canaryPosition = sectorOffset + sectorPayloadSize;
                    ui32 sizeToEncrypt = format.SectorSize - canaryPosition - ui32(sizeof(TDataSectorFooter));
                    cypher.InplaceEncrypt(source + canaryPosition, sizeToEncrypt);
                    PDisk->CheckLogCanary(source, Read->ChunkIdx, sectorIdx);
                }
            }
        }
        Destination += sectorPayloadSize;
        source += format.SectorSize;
        PayloadReadSize -= sectorPayloadSize;
        sectorPayloadSize = Min(format.SectorPayloadSize(), PayloadReadSize);
        sectorOffset = 0;
        ++sectorIdx;
    }
    if (beginBadUserOffset != 0xffffffff) {
        LOG_INFO_S(*actorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDisk->PCtx->PDiskId
            << " ReqId# " << Read->ReqId
            << " Can't read chunk chunkIdx# " << Read->ChunkIdx
            << " for owner# " << Read->Owner
            << " beginBadUserOffet# " << beginBadUserOffset << " endBadUserOffset# " << endBadUserOffset
            << " due to multiple sectors with incorrect hashes/nonces. Marker# BPC003");
        CumulativeCompletion->AddGap(beginBadUserOffset, endBadUserOffset);
        beginBadUserOffset = 0xffffffff;
        endBadUserOffset = 0xffffffff;
    }
}

void TCompletionChunkReadPart::Exec(TActorSystem *actorSystem) {
    Y_VERIFY(actorSystem);
    Y_VERIFY(CumulativeCompletion);
    if (TCompletionAction::Result != EIoResult::Ok) {
        Release(actorSystem);
        return;
    }

    if (Read->ChunkEncrypted) {
        Span.Event("PDisk.CompletionChunkReadPart.DecryptionStart");
        UnencryptData(actorSystem);
    }

    double deviceTimeMs = HPMilliSecondsFloat(GetTime - SubmitTime);
    LWTRACK(PDiskChunkReadPieceComplete, Orbit, PDisk->PCtx->PDiskId, RawReadSize, CommonBufferOffset, deviceTimeMs);
    Read->Orbit.Join(Orbit);
    CumulativeCompletion->PartReadComplete(actorSystem);
    CumulativeCompletion = nullptr;

    AtomicSub(PDisk->InFlightChunkRead, RawReadSize);
    RawReadSize = 0;
    Span.Event("PDisk.CompletionChunkReadPart.ExecStop", {{"RawReadSize", RawReadSize}, {"EncryptedChunk", Read->ChunkEncrypted}});
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

TCompletionChunkRead::TCompletionChunkRead(TPDisk *pDisk, TIntrusivePtr<TChunkRead> &read, std::function<void()> onDestroy,
            ui64 chunkNonce)
    : TCompletionAction()
    , PDisk(pDisk)
    , Read(read)
    // 1 in PartsPending stands for the last part, so if any non-last part completes it will not lead to call of Exec()
    , PartsPending(1)
    , Deletes(0)
    , OnDestroy(std::move(onDestroy))
    , ChunkNonce(chunkNonce)
    , Span(read->Span.CreateChild(TWilson::PDiskBasic, "PDisk.CompletionChunkRead"))
    , DoubleFreeCanary(ReferenceCanary)
{
    // in case of plain chunks we can have unaligned offset and unaligned size of the read
    // so we have to calculate headroom and tailroom as follow
    // ^ requested data
    // * not interesting data
    // | sector border (sector size equals to 4 in the example)
    //
    // | * * ^ ^ | ^ ^ ^ ^ | ^ ^ ^ * |
    // headroom = 2, size = 9, tailroom = 1, new size should be equal
    // - increase size by offset unalignment (2 in this case), newSize is 11 now
    // - align the size up to sector border, we get 12
    // - calculate tailroom as follow: 12 - 11 = 1
    size_t sectorSize = PDisk->Format.SectorSize;
    auto newSize = read->ChunkEncrypted
        ? read->Size
        : read->Size + read->Offset % sectorSize;
    size_t tailroom = AlignUp<size_t>(newSize, sectorSize) - newSize;
    CommonBuffer = TBufferWithGaps(read->Offset, newSize, tailroom);
}

TCompletionChunkRead::~TCompletionChunkRead() {
    OnDestroy();
    Y_VERIFY(CommonBuffer.Empty());
    Y_VERIFY(DoubleFreeCanary == ReferenceCanary, "DoubleFreeCanary in TCompletionChunkRead is dead!");
    // Set DoubleFreeCanary to 0 and make sure compiler will not eliminate that action
    SecureWipeBuffer((ui8*)&DoubleFreeCanary, sizeof(DoubleFreeCanary));
}

void TCompletionChunkRead::Exec(TActorSystem *actorSystem) {
    Read->Span.Event("PDisk.CompletionChunkRead.Exec");
    THolder<TEvChunkReadResult> result = MakeHolder<TEvChunkReadResult>(NKikimrProto::OK,
        Read->ChunkIdx, Read->Offset, Read->Cookie, PDisk->GetStatusFlags(Read->Owner, Read->OwnerGroupType), "");

    if (!Read->ChunkEncrypted) {
        size_t headroom = Read->Offset % PDisk->Format.SectorSize;
        auto newBuf = CommonBuffer.SubstrRaw(headroom, CommonBuffer.Size() - headroom);
        CommonBuffer.SetData(std::move(newBuf));
    }
    result->Data = std::move(CommonBuffer);
    CommonBuffer.Clear();
    //Y_VERIFY(result->Data.IsDetached());

    result->Data.Commit();

    Y_VERIFY(Read);
    LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_PDISK, "Reply from TCompletionChunkRead, PDiskId# " << PDisk->PCtx->PDiskId << " ReqId# " << Read->ReqId.Id
            << " " << result->ToString() << " To# " << Read->Sender.LocalId());

    double responseTimeMs = HPMilliSecondsFloat(HPNow() - Read->CreationTime);
    PDisk->Mon.IncrementResponseTime(Read->PriorityClass, responseTimeMs, Read->Size);
    LWTRACK(PDiskChunkResponseTime, Read->Orbit, PDisk->PCtx->PDiskId, Read->ReqId.Id, Read->PriorityClass, responseTimeMs,
            Read->Size);

    actorSystem->Send(Read->Sender, result.Release());
    Read->IsReplied = true;
    Read->Span.EndOk();

    PDisk->Mon.GetReadCounter(Read->PriorityClass)->CountResponse();
    delete this;
}

void TCompletionChunkRead::ReplyError(TActorSystem *actorSystem, TString reason) {
    Y_VERIFY(!Read->IsReplied);
    CommonBuffer.Clear();

    TStringStream error;
    error << "Reply Error from TCompletionChunkRead PDiskId# " << PDisk->PCtx->PDiskId << " ReqId# " << Read->ReqId << " reason# " << reason;
    auto result = MakeHolder<TEvChunkReadResult>(NKikimrProto::CORRUPTED,
            Read->ChunkIdx, Read->Offset, Read->Cookie,
            PDisk->GetStatusFlags(Read->Owner, Read->OwnerGroupType), error.Str());

    result->Data.SetDebugInfoGenerator(PDisk->DebugInfoGenerator);

    LOG_WARN_S(*actorSystem, NKikimrServices::BS_PDISK, error.Str());
    actorSystem->Send(Read->Sender, result.Release());
    Read->IsReplied = true;
    Read->Span.EndError(reason);
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
            "PDiskId# " << PDisk->PCtx->PDiskId << " ReqId# " << ReqId
            << " TChunkTrimCompletion timeMs# "
            << ui64(responseTimeMs) << " sizeBytes# " << SizeBytes);
    LWPROBE(PDiskTrimResponseTime, PDisk->PCtx->PDiskId, ReqId.Id, responseTimeMs, SizeBytes);
    PDisk->Mon.Trim.CountResponse();
    TTryTrimChunk *tryTrim = PDisk->ReqCreator.CreateFromArgs<TTryTrimChunk>(SizeBytes);
    PDisk->InputRequest(tryTrim);
    delete this;
}

void TChunkShredCompletion::Exec(TActorSystem *actorSystem) {
    LOG_TRACE_S(*actorSystem, NKikimrServices::BS_PDISK_SHRED,
            "PDiskId# " << PDisk->PCtx->PDiskId << " ReqId# " << ReqId
            << " TChunkShredCompletion Chunk# " << Chunk
            << " SectorIdx# " << SectorIdx
            << " SizeBytes# " << SizeBytes);
    PDisk->Mon.ChunkShred.CountResponse();
    TChunkShredResult *shredResult = PDisk->ReqCreator.CreateFromArgs<TChunkShredResult>(Chunk, SectorIdx, SizeBytes);
    PDisk->InputRequest(shredResult);
    delete this;
}

} // NPDisk
} // NKikimr
