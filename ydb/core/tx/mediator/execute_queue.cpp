#include "mediator_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/tx/time_cast/time_cast.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NTxMediator {

    class TTxMediatorExecQueue : public TActor<TTxMediatorExecQueue> {
        struct TBucket {
            TActorId ActiveActor;
        };

        const TActorId Owner;
        const ui64 MediatorId;
        const ui64 HashRange;

        TTimeCastBuckets BucketSelector;
        TVector<TBucket> Buckets;

        TBucket& SelectBucket(TTabletId tablet) {
            const ui32 bucketIdx = BucketSelector.Select(tablet);
            Y_DEBUG_ABORT_UNLESS(bucketIdx < Buckets.size());
            return Buckets[bucketIdx];
        }

        template<typename TEv>
        void SendStepToBucket(TTabletId tablet, TStepId step, TVector<TTx> &tx, const TActorContext &ctx) {
            TBucket &bucket = SelectBucket(tablet);
            Sort(tx.begin(), tx.end(), TTx::TCmpOrderId());

            LOG_DEBUG(ctx, NKikimrServices::TX_MEDIATOR_PRIVATE, [&]() {
                TStringBuilder ss;
                ss << "Mediator exec queue [" << MediatorId << "], step# " << step << " for tablet [" << tablet << "]. TxIds:";
                for (const auto &x : tx)
                    ss << " txid# " << x.TxId;
                ss << " marker# M2";
                return (TString)ss;
            }());

            LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_EXEC_QUEUE, "Actor# " << ctx.SelfID.ToString()
                << " MediatorId# " << MediatorId << " SEND Ev to# " << bucket.ActiveActor.ToString()
                << " step# " << step << " forTablet# " << tablet << [&]() {
                        TStringBuilder ss;
                        for (const auto &x : tx)
                            ss << " txid# " << x.TxId;
                        ss << " marker# M3";
                        return (TString)ss;
                    }());
            ctx.Send(bucket.ActiveActor, new TEv(step, tablet, tx));
        }

        void Die(const TActorContext &ctx) override {
            for (const TBucket &bucket : Buckets)
                ctx.Send(bucket.ActiveActor, new TEvents::TEvPoisonPill());
            Buckets.clear();
            return TActor::Die(ctx);
        }

        void Handle(TEvTxMediator::TEvCommitStep::TPtr &ev, const TActorContext &ctx) {
            TEvTxMediator::TEvCommitStep *msg = ev->Get();
            TMediateStep *step = msg->MediateStep.Get();

            const ui32 totalCoordinators = step->Steps.size();

            LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_EXEC_QUEUE, "Actor# " << ctx.SelfID.ToString()
                << " MediatorId# " << MediatorId << " HANDLE TEvCommitStep " << step->ToString() << " marker# M1");

            for (ui32 i = 0; i != totalCoordinators; ++i) {
                TCoordinatorStep &coord = *step->Steps[i];
                Sort(coord.TabletsToTransaction.begin(), coord.TabletsToTransaction.end(), TCoordinatorStep::TabletToTransactionCmp());
            }

            ui64 activeTablet = Max<ui64>();
            ui64 lookupTablet = Max<ui64>();

            TVector<ui64> readPositions(totalCoordinators, 0);
            TVector<TTx> currentTx;

            do {
                for (ui64 ci = 0; ci != totalCoordinators; ++ci) {
                    ui64 &readPos = readPositions[ci];
                    TCoordinatorStep &coord = *step->Steps[ci];

                    const ui64 tttsize = coord.TabletsToTransaction.size();
                    while (readPos < tttsize) {
                        const std::pair<TTabletId, std::size_t> &x = coord.TabletsToTransaction[readPos];
                        if (x.first != activeTablet)
                            break;

                        currentTx.emplace_back(coord.Transactions[x.second]);
                        ++readPos;
                    }

                    if (readPos < tttsize) {
                        const std::pair<TTabletId, std::size_t> &x = coord.TabletsToTransaction[readPos];
                        if (x.first < lookupTablet)
                            lookupTablet = x.first;
                    }
                }

                if (activeTablet != Max<ui64>()) {
                    SendStepToBucket<TEvTxMediator::TEvCommitTabletStep>(activeTablet, step->To, currentTx, ctx);
                }

                activeTablet = lookupTablet;
                lookupTablet = Max<ui64>();
                currentTx.clear();
            } while (activeTablet != Max<ui64>());

            for (const TBucket &bucket : Buckets) {
                LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_EXEC_QUEUE, "Actor# " << ctx.SelfID.ToString()
                    << " MediatorId# " << MediatorId << " SEND TEvStepPlanComplete to# "
                    << bucket.ActiveActor.ToString() << " bucket.ActiveActor step# " << step->To);
                ctx.Send(bucket.ActiveActor, new TEvTxMediator::TEvStepPlanComplete(step->To));
            }
        }

        void Handle(TEvTxMediator::TEvRequestLostAcks::TPtr &ev, const TActorContext &ctx) {
            TEvTxMediator::TEvRequestLostAcks *msg = ev->Get();
            TCoordinatorStep *step = msg->CoordinatorStep.Get();
            const TActorId &ackTo = msg->AckTo;
            LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_EXEC_QUEUE, "Actor# " << ctx.SelfID.ToString()
                << " MediatorId# " << MediatorId << " HANDLE TEvRequestLostAcks " << step->ToString()
                << " AckTo# " << ackTo.ToString());

            Sort(step->TabletsToTransaction.begin(), step->TabletsToTransaction.end(), TCoordinatorStep::TabletToTransactionCmp());

            TVector<TTx> currentTx;
            TTabletId activeTablet = 0;

            for (TVector<std::pair<TTabletId, std::size_t>>::const_iterator it = step->TabletsToTransaction.begin(), end = step->TabletsToTransaction.end(); it != end; ++it) {
                if (activeTablet != it->first) {
                    if (activeTablet)
                        SendStepToBucket<TEvTxMediator::TEvOoOTabletStep>(activeTablet, step->Step, currentTx, ctx);
                    activeTablet = it->first;
                    currentTx.clear();
                }

                currentTx.emplace_back(step->Transactions[it->second]);
                currentTx.back().AckTo = ackTo;
            }

            if (activeTablet)
                SendStepToBucket<TEvTxMediator::TEvOoOTabletStep>(activeTablet, step->Step, currentTx, ctx);
        }

        void Handle(TEvMediatorTimecast::TEvWatch::TPtr &ev, const TActorContext &ctx) {
            const NKikimrTxMediatorTimecast::TEvWatch &record = ev->Get()->Record;
            // todo: check config coherence
            const TActorId &sender = ev->Sender;
            const TActorId &server = ev->Recipient;
            LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_EXEC_QUEUE, "Actor# " << ctx.SelfID.ToString()
                << " MediatorId# " << MediatorId << " HANDLE TEvWatch");

            for (ui32 bucketIdx : record.GetBucket()) {
                LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_EXEC_QUEUE, "Actor# " << ctx.SelfID.ToString()
                    << " MediatorId# " << MediatorId << " SEND TEvWatchBucket to# "
                    << Buckets[bucketIdx].ActiveActor.ToString() << " bucket.ActiveActor");
                ctx.Send(Buckets[bucketIdx].ActiveActor, new TEvTxMediator::TEvWatchBucket(sender, server));
            }
        }

        void Handle(TEvMediatorTimecast::TEvGranularWatch::TPtr &ev, const TActorContext &ctx) {
            const auto &record = ev->Get()->Record;
            const ui32 bucketIdx = record.GetBucket();

            LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_EXEC_QUEUE, "Actor# " << ctx.SelfID
                << " MediatorId# " << MediatorId << " HANDLE TEvGranularWatch from# " << ev->Sender
                << " bucket# " << bucketIdx);

            if (bucketIdx < Buckets.size()) {
                ev->Rewrite(ev->GetTypeRewrite(), Buckets[bucketIdx].ActiveActor);
                ctx.ExecutorThread.Send(ev.Release());
            }
        }

        void Handle(TEvMediatorTimecast::TEvGranularWatchModify::TPtr &ev, const TActorContext &ctx) {
            const auto &record = ev->Get()->Record;
            const ui32 bucketIdx = record.GetBucket();

            LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_EXEC_QUEUE, "Actor# " << ctx.SelfID
                << " MediatorId# " << MediatorId << " HANDLE TEvGranularWatchModify from# " << ev->Sender
                << " bucket# " << bucketIdx);

            if (bucketIdx < Buckets.size()) {
                ev->Rewrite(ev->GetTypeRewrite(), Buckets[bucketIdx].ActiveActor);
                ctx.ExecutorThread.Send(ev.Release());
            }
        }

        void Handle(TEvTxMediator::TEvServerDisconnected::TPtr &ev, const TActorContext &ctx) {
            const auto* msg = ev->Get();

            LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_EXEC_QUEUE, "Actor# " << ctx.SelfID
                << " MediatorId# " << MediatorId << " HANDLE TEvServerDisconnected server# " << msg->ServerId);

            // Broadcast to buckets
            for (const TBucket &bucket : Buckets) {
                ctx.Send(bucket.ActiveActor, new TEvTxMediator::TEvServerDisconnected(msg->ServerId));
            }
        }

        void Bootstrap(const TActorContext &ctx) {
            Buckets.resize(BucketSelector.Buckets());
            for (ui32 bucketIdx = 0; bucketIdx < Buckets.size(); ++bucketIdx)
                Buckets[bucketIdx].ActiveActor = ctx.ExecutorThread.RegisterActor(CreateTxMediatorTabletQueue(ctx.SelfID, MediatorId, 1, bucketIdx), TMailboxType::ReadAsFilled);
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::TX_MEDIATOR_EXECUTE_QUEUE_ACTOR;
        }

        TTxMediatorExecQueue(const TActorId &owner, ui64 mediator, ui64 hashRange, ui32 timecastBuckets)
            : TActor(&TThis::StateWork)
            , Owner(owner)
            , MediatorId(mediator)
            , HashRange(hashRange)
            , BucketSelector(timecastBuckets)
        {
            Y_UNUSED(HashRange);
        }

        TAutoPtr<IEventHandle> AfterRegister(const TActorId &self, const TActorId& parentId) override {
            Y_UNUSED(parentId);
            return new IEventHandle(self, self, new TEvents::TEvBootstrap());
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTxMediator::TEvCommitStep, Handle);
                HFunc(TEvTxMediator::TEvRequestLostAcks, Handle);
                HFunc(TEvMediatorTimecast::TEvWatch, Handle);
                HFunc(TEvMediatorTimecast::TEvGranularWatch, Handle);
                HFunc(TEvMediatorTimecast::TEvGranularWatchModify, Handle);
                HFunc(TEvTxMediator::TEvServerDisconnected, Handle);
                CFunc(TEvents::TSystem::PoisonPill, Die);
                CFunc(TEvents::TSystem::Bootstrap, Bootstrap);
            }
        }
    };
}

IActor* CreateTxMediatorExecQueue(const TActorId &owner, ui64 mediator, ui64 hashRange, ui32 timecastBuckets) {
    return new NTxMediator::TTxMediatorExecQueue(owner, mediator, hashRange, timecastBuckets);
}

}
