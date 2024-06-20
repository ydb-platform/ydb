#include "defs.h"
#include "utils.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/base/vdisk_sync_common.h>

#include <ydb/core/util/stlog.h>


namespace NKikimr {
namespace NBalancing {
namespace {
    struct TPart {
        TLogoBlobID Key;
        NMatrix::TVectorType PartsMask;
        TVector<TRope> PartsData;
    };

    class TReader {
    private:
        const size_t BatchSize;
        TPDiskCtxPtr PDiskCtx;
        TQueue<TPartInfo> Parts;
        TReplQuoter::TPtr Quoter;
        const TBlobStorageGroupType GType;
        NMonGroup::TBalancingGroup& MonGroup;

        TVector<TPart> Result;
        ui32 Responses;
        ui32 ExpectedResponses;
    public:

        TReader(size_t batchSize, TPDiskCtxPtr pDiskCtx, TQueue<TPartInfo> parts, TReplQuoter::TPtr replPDiskReadQuoter, TBlobStorageGroupType gType, NMonGroup::TBalancingGroup& monGroup)
            : BatchSize(batchSize)
            , PDiskCtx(pDiskCtx)
            , Parts(std::move(parts))
            , Quoter(replPDiskReadQuoter)
            , GType(gType)
            , MonGroup(monGroup)
            , Result(Reserve(BatchSize))
            , Responses(0)
            , ExpectedResponses(0)
        {}

        void ScheduleJobQuant(const TActorId& selfId) {
            Result.resize(Min(Parts.size(), BatchSize));
            ExpectedResponses = 0;
            for (ui64 i = 0; i < BatchSize && !Parts.empty(); ++i) {
                auto item = Parts.front();
                Parts.pop();
                Result[i] = TPart{
                    .Key=item.Key,
                    .PartsMask=item.PartsMask,
                };
                std::visit(TOverloaded{
                    [&](const TRope& data) {
                        // part is already in memory, no need to read it from disk
                        Y_DEBUG_ABORT_UNLESS(item.PartsMask.CountBits() == 1);
                        Result[i].PartsData = {data};
                        ++Responses;
                    },
                    [&](const TDiskPart& diskPart) {
                        auto ev = std::make_unique<NPDisk::TEvChunkRead>(
                            PDiskCtx->Dsk->Owner,
                            PDiskCtx->Dsk->OwnerRound,
                            diskPart.ChunkIdx,
                            diskPart.Offset,
                            diskPart.Size,
                            NPriRead::HullLow,
                            reinterpret_cast<void*>(i)
                        );

                        TReplQuoter::QuoteMessage(
                            Quoter,
                            std::make_unique<IEventHandle>(PDiskCtx->PDiskId, selfId, ev.release()),
                            diskPart.Size
                        );
                        MonGroup.ReadFromHandoffBytes() += diskPart.Size;
                    }
                }, item.PartData);
                ++ExpectedResponses;
            }
        }

        std::pair<std::optional<TVector<TPart>>, ui32> TryGetResults() {
            if (ExpectedResponses == Responses) {
                ExpectedResponses = 0;
                Responses = 0;
                return {std::move(Result), Parts.size()};
            }
            return {std::nullopt, Parts.size()};
        }

        void Handle(NPDisk::TEvChunkReadResult::TPtr ev) {
            ++Responses;
            auto *msg = ev->Get();
            if (msg->Status != NKikimrProto::EReplyStatus::OK) {
                return;
            }
            ui64 i = reinterpret_cast<ui64>(msg->Cookie);
            const auto& key = Result[i].Key;
            auto data = TRope(msg->Data.ToString());
            auto localParts = Result[i].PartsMask;
            auto diskBlob = TDiskBlob(&data, localParts, GType, key);
            ui32 readSize = 0;

            for (ui8 partIdx = localParts.FirstPosition(); partIdx < localParts.GetSize(); partIdx = localParts.NextPosition(partIdx)) {
                TRope result;
                result = diskBlob.GetPart(partIdx, &result);
                readSize += result.size();
                Result[i].PartsData.emplace_back(std::move(result));
            }

            MonGroup.ReadFromHandoffResponseBytes() += readSize;
        }
    };

    class TSender : public TActorBootstrapped<TSender> {
        TActorId NotifyId;
        TQueueActorMapPtr QueueActorMapPtr;
        std::shared_ptr<TBalancingCtx> Ctx;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TReader Reader;
        TWaiter Mngr;

        struct TStats {
            ui32 PartsRead = 0;
            ui32 PartsSent = 0;
            ui32 PartsSentUnsuccsesfull = 0;
        };
        TStats Stats;

        void ScheduleJobQuant() {
            Mngr.Init();
            Reader.ScheduleJobQuant(SelfId());
            // if all parts are already in memory, we could process results right now
            TryProcessResults();
        }

        void TryProcessResults() {
            if (auto [batch, partsLeft] = Reader.TryGetResults(); batch.has_value()) {
                Stats.PartsRead += batch->size();
                ui64 epoch = Mngr.GetEpoch();
                ui32 partsSent = SendParts(std::move(*batch), epoch);
                Mngr.StartJob(TlsActivationContext->Now(), partsSent, partsLeft);
                Schedule(Mngr.SendTimeout, new NActors::TEvents::TEvCompleted(epoch));
                TryCompleteBatch(epoch);
            }
        }

        void TryCompleteBatch(NActors::TEvents::TEvCompleted::TPtr ev) {
            TryCompleteBatch(ev->Get()->Id);
        }

        void TryCompleteBatch(ui64 epoch) {
            if (auto ev = Mngr.IsJobDone(epoch, TlsActivationContext->Now()); ev != nullptr) {
                Send(NotifyId, ev);
                if (Mngr.IsPassAway()) {
                    PassAway();
                }
            }
        }

        void Handle(NPDisk::TEvChunkReadResult::TPtr ev) {
            Reader.Handle(ev);
            TryProcessResults();
        }

        ui32 SendParts(TVector<TPart> batch, ui64 epoch) {
            STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB11, VDISKP(Ctx->VCtx, "Sending parts"), (BatchSize, batch.size()));

            ui32 partsSent = 0;
            THashMap<TVDiskID, std::unique_ptr<TEvBlobStorage::TEvVMultiPut>> vDiskToEv;
            for (auto& part: batch) {
                auto localParts = part.PartsMask;
                for (ui8 partIdx = localParts.FirstPosition(), i = 0; partIdx < localParts.GetSize(); partIdx = localParts.NextPosition(partIdx), ++i) {
                    auto key = TLogoBlobID(part.Key, partIdx + 1);
                    auto& data = part.PartsData[i];
                    auto vDiskId = GetMainReplicaVDiskId(*GInfo, key);

                    if (Ctx->HugeBlobCtx->IsHugeBlob(GInfo->GetTopology().GType, part.Key)) {
                        auto& queue = (*QueueActorMapPtr)[TVDiskIdShort(vDiskId)];
                        auto ev = std::make_unique<TEvBlobStorage::TEvVPut>(
                            key, data, vDiskId,
                            true, &epoch,
                            TInstant::Max(), NKikimrBlobStorage::EPutHandleClass::AsyncBlob
                        );
                        TReplQuoter::QuoteMessage(
                            Ctx->VCtx->ReplNodeRequestQuoter,
                            std::make_unique<IEventHandle>(queue, SelfId(), ev.release()),
                            data.size()
                        );
                        Ctx->MonGroup.SentOnMainBytes() += data.size();
                    } else {
                        STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB12, VDISKP(Ctx->VCtx, "Add in multiput"), (LogoBlobId, key.ToString()),
                            (To, GInfo->GetTopology().GetOrderNumber(TVDiskIdShort(vDiskId))), (DataSize, data.size()));

                        auto& ev = vDiskToEv[vDiskId];
                        if (!ev) {
                            ev = std::make_unique<TEvBlobStorage::TEvVMultiPut>(vDiskId, TInstant::Max(), NKikimrBlobStorage::EPutHandleClass::AsyncBlob, true, &epoch);
                        }

                        ev->AddVPut(key, TRcBuf(data), nullptr, {}, NWilson::TTraceId());
                    }
                    partsSent++;
                }
            }

            for (auto& [vDiskId, ev]: vDiskToEv) {
                STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB12, VDISKP(Ctx->VCtx, "Send multiput"), (VDisk, vDiskId.ToString()));

                ui32 blobsSize = 0;
                for (const auto& item: ev->Record.GetItems()) {
                    blobsSize += item.GetBuffer().size();
                }

                auto& queue = (*QueueActorMapPtr)[TVDiskIdShort(vDiskId)];
                TReplQuoter::QuoteMessage(
                    Ctx->VCtx->ReplNodeRequestQuoter,
                    std::make_unique<IEventHandle>(queue, SelfId(), ev.release()),
                    blobsSize
                );
                Ctx->MonGroup.SentOnMainBytes() += blobsSize;
            }

            return partsSent;
        }

        void Handle(TEvBlobStorage::TEvVPutResult::TPtr ev) {
            ++Stats.PartsSent;
            if (ev->Get()->Record.GetStatus() != NKikimrProto::OK) {
                ++Stats.PartsSentUnsuccsesfull;
                STLOG(PRI_WARN, BS_VDISK_BALANCING, BSVB13, VDISKP(Ctx->VCtx, "Put failed"), (Msg, ev->Get()->ToString()));
            } else {
                ++Ctx->MonGroup.SentOnMain();
                Ctx->MonGroup.SentOnMainWithResponseBytes() += GInfo->GetTopology().GType.PartSize(LogoBlobIDFromLogoBlobID(ev->Get()->Record.GetBlobID()));
                STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB14, VDISKP(Ctx->VCtx, "Put done"), (Msg, ev->Get()->ToString()));
            }
            ui64 epoch = ev->Get()->Record.GetCookie();
            Mngr.PartJobDone(epoch);
            TryCompleteBatch(epoch);
        }

        void Handle(TEvBlobStorage::TEvVMultiPutResult::TPtr ev) {
            const auto& items = ev->Get()->Record.GetItems();
            Stats.PartsSent += items.size();
            for (const auto& item: items) {
                if (item.GetStatus() != NKikimrProto::OK) {
                    ++Stats.PartsSentUnsuccsesfull;
                    STLOG(PRI_WARN, BS_VDISK_BALANCING, BSVB15, VDISKP(Ctx->VCtx, "Put failed"), (Key, LogoBlobIDFromLogoBlobID(item.GetBlobID()).ToString()));
                    continue;
                }
                ++Ctx->MonGroup.SentOnMain();
                STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB16, VDISKP(Ctx->VCtx, "Put done"), (Key, LogoBlobIDFromLogoBlobID(item.GetBlobID()).ToString()));
            }
            ui64 epoch = ev->Get()->Record.GetCookie();
            TryCompleteBatch(epoch);
            Mngr.PartJobDone(epoch, items.size());
        }

        void PassAway() override {
            Send(NotifyId, new NActors::TEvents::TEvCompleted(SENDER_ID));
            TActorBootstrapped::PassAway();
        }

        void Handle(TEvVGenerationChange::TPtr ev) {
            GInfo = ev->Get()->NewInfo;
        }

        STRICT_STFUNC(StateFunc,
            cFunc(NActors::TEvents::TEvWakeup::EventType, ScheduleJobQuant)
            hFunc(NPDisk::TEvChunkReadResult, Handle)
            hFunc(TEvBlobStorage::TEvVPutResult, Handle)
            hFunc(TEvBlobStorage::TEvVMultiPutResult, Handle)
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway)
            hFunc(NActors::TEvents::TEvCompleted, TryCompleteBatch)

            hFunc(TEvVGenerationChange, Handle)
        );

    public:
        TSender(
            TActorId notifyId,
            TQueue<TPartInfo> parts,
            TQueueActorMapPtr queueActorMapPtr,
            std::shared_ptr<TBalancingCtx> ctx
        )
            : NotifyId(notifyId)
            , QueueActorMapPtr(queueActorMapPtr)
            , Ctx(ctx)
            , GInfo(ctx->GInfo)
            , Reader(32, Ctx->PDiskCtx, std::move(parts), ctx->VCtx->ReplPDiskReadQuoter, GInfo->GetTopology().GType, Ctx->MonGroup)
            , Mngr{.ServiceId=SENDER_ID}
        {}

        void Bootstrap() {
            Become(&TThis::StateFunc);
        }
    };
}

IActor* CreateSenderActor(
    TActorId notifyId,
    TQueue<TPartInfo> parts,
    TQueueActorMapPtr queueActorMapPtr,
    std::shared_ptr<TBalancingCtx> ctx
) {
    return new TSender(notifyId, parts, queueActorMapPtr, ctx);
}

} // NBalancing
} // NKikimr
