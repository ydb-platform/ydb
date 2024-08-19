#pragma once
#include "defs.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/scheme_types/scheme_types.h>
#include <ydb/core/tx/coordinator/public/events.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/tx/time_cast/time_cast.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/util/queue_oneone_inplace.h>
#include <ydb/core/protos/counters_mediator.pb.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/tx/tx.h>


#include <util/generic/map.h>

namespace NKikimr {
namespace NTxMediator {

    using TStepId = ui64;
    using TTxId = ui64;
    using TTabletId = ui64;

    struct TTx {
        // transaction body
        ui64 Moderator;
        TTxId TxId;
        TActorId AckTo;

        TTx(ui64 moderator, TTxId txid)
            : Moderator(moderator)
            , TxId(txid)
            , AckTo() // must be updated before commit
        {
            Y_ABORT_UNLESS(TxId != 0);
        }

        struct TCmpOrderId {
            bool operator()(const TTx &left, const TTx &right) const noexcept {
                return left.TxId < right.TxId;
            }
        };

        TString ToString() const {
            TStringStream str;
            str << "{TTx Moderator# " << Moderator;
            str << " txid# " << TxId;
            str << " AckTo# " << AckTo.ToString();
            str << "}";
            return str.Str();
        }
    };

    struct TCoordinatorStep {
        const TStepId Step;
        const TStepId PrevStep;

        TVector<TTx> Transactions;

        TVector<std::pair<TTabletId, std::size_t>> TabletsToTransaction; // tablet -> tx index in Transactions

        struct TabletToTransactionCmp {
            bool operator()(const std::pair<TTabletId, std::size_t> &left, const std::pair<TTabletId, std::size_t> &right) const {
                return left.first < right.first;
            }
        };

        TCoordinatorStep(const NKikimrTx::TEvCoordinatorStep &record);

        TString ToString() const {
            TStringStream str;
            str << "{TCoordinatorStep step# " << Step;
            str << " PrevStep# " << PrevStep;
            if (Transactions.size()) {
                str << "Transactions: {";
                for (size_t i = 0; i < Transactions.size(); ++i) {
                    str << Transactions[i].ToString();
                }
                str << "}";
            }
            if (TabletsToTransaction.size()) {
                str << "TabletsToTransaction: {";
                for (size_t i = 0; i < TabletsToTransaction.size(); ++i) {
                    str << "{tablet# " << TabletsToTransaction[i].first;
                    str << " txid# " << Transactions[TabletsToTransaction[i].second].TxId;
                    str << "}";
                }
                str << "}";
            }
            str << "}";
            return str.Str();
        }
    };

    struct TMediateStep {
        TStepId From;
        TStepId To;

        TVector<TAutoPtr<TCoordinatorStep>> Steps;

        TMediateStep(TStepId from, TStepId to)
            : From(from)
            , To(to)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{TMediateStep From " << From;
            str << " To# " << To;
            if (Steps.size()) {
                str << "Steps: {";
                for (size_t i = 0; i < Steps.size(); ++i) {
                    str << Steps[i]->ToString();
                }
                str << "}";
            }
            str << "}";
            return str.Str();
        }
    };
}

struct TEvTxMediator {
    using TTabletId = NTxMediator::TTabletId;
    using TStepId = NTxMediator::TStepId;

    enum EEv {
        EvCommitStep = EventSpaceBegin(TKikimrEvents::ES_TX_MEDIATOR),
        EvRequestLostAcks,
        EvMediatorConfiguration,

        EvCommitTabletStep = EvCommitStep + 1 * 512,
        EvStepPlanComplete,
        EvOoOTabletStep,
        EvWatchBucket,
        EvServerDisconnected,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_MEDIATOR), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_MEDIATOR)");

    struct TEvCommitStep : public TEventLocal<TEvCommitStep, EvCommitStep> {
        TAutoPtr<NTxMediator::TMediateStep> MediateStep;

        TEvCommitStep(TAutoPtr<NTxMediator::TMediateStep> &mds)
            : MediateStep(mds)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{TEvCommitStep MediateStep: " << MediateStep->ToString();
            str << "}";
            return str.Str();
        }
    };

    struct TEvRequestLostAcks : public TEventLocal<TEvRequestLostAcks, EvRequestLostAcks> {
        TAutoPtr<NTxMediator::TCoordinatorStep> CoordinatorStep;
        const TActorId AckTo;

        TEvRequestLostAcks(TAutoPtr<NTxMediator::TCoordinatorStep> &cds, const TActorId &ackTo)
            : CoordinatorStep(cds)
            , AckTo(ackTo)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{TEvRequestLostAcks CoordinatorStep: " << CoordinatorStep->ToString();
            str << " AckTo# " << AckTo.ToString();
            str << "}";
            return str.Str();
        }
    };

    // just reschedule command, actual transport is over command queue
    struct TEvCommitTabletStep : public TEventLocal<TEvCommitTabletStep, EvCommitTabletStep> {
        const TStepId Step;
        const TTabletId TabletId;
        TVector<NTxMediator::TTx> Transactions; // todo: inplace placing

        TEvCommitTabletStep(TStepId step, TTabletId tabletId, TVector<NTxMediator::TTx> &transactions)
            : Step(step)
            , TabletId(tabletId)
            , Transactions(transactions.begin(), transactions.end())
        {}

        TString ToString() const {
            TStringStream str;
            str << "{TEvCommitTabletStep step# " << Step;
            str << " TabletId# " << TabletId;
            str << " Transactions {";
            for (size_t i = 0; i < Transactions.size(); ++i) {
                str << Transactions[i].ToString();
            }
            str << "}}";
            return str.Str();
        }
    };

    struct TEvStepPlanComplete : public TEventLocal<TEvStepPlanComplete, EvStepPlanComplete> {
        const TStepId Step;

        TEvStepPlanComplete(TStepId step)
            : Step(step)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{TEvStepPlanComplete step# " << Step;
            str << "}";
            return str.Str();
        }
    };

    struct TEvOoOTabletStep : public TEventLocal<TEvOoOTabletStep, EvOoOTabletStep> {
        const TStepId Step;
        const TTabletId TabletId;
        TVector<NTxMediator::TTx> Transactions;

        TEvOoOTabletStep(TStepId step, TTabletId tabletId, TVector<NTxMediator::TTx> &transactions)
            : Step(step)
            , TabletId(tabletId)
            , Transactions(transactions.begin(), transactions.end())
        {}

        TString ToString() const {
            TStringStream str;
            str << "{TEvOoOTabletStep step# " << Step;
            str << " TabletId# " << TabletId;
            str << " Transactions {";
            for (size_t i = 0; i < Transactions.size(); ++i) {
                str << Transactions[i].ToString();
            }
            str << "}}";
            return str.Str();
        }
    };

    struct TEvWatchBucket : public TEventLocal<TEvWatchBucket, EvWatchBucket> {
        const TActorId Source;
        const TActorId ServerId;

        TEvWatchBucket(const TActorId &source, const TActorId &serverId)
            : Source(source)
            , ServerId(serverId)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{TEvWatchBucket Source# " << Source.ToString();
            str << "}";
            return str.Str();
        }
    };

    struct TEvServerDisconnected : public TEventLocal<TEvServerDisconnected, EvServerDisconnected> {
        const TActorId ServerId;

        TEvServerDisconnected(const TActorId &serverId)
            : ServerId(serverId)
        {}
    };
};

namespace NTxMediator {

typedef ui64 TCoordinatorId;

using NTabletFlatExecutor::ITransaction;
using NActors::TActorContext;

class TTxMediator : public TActor<TTxMediator>, public NTabletFlatExecutor::TTabletExecutedFlat {
    struct TConfig {
        ui64 CoordinatorsVersion;
        TCoordinators::TPtr CoordinatorSeletor;
        TTimeCastBuckets::TPtr Bukets;

        TConfig()
            : CoordinatorsVersion(0)
        {}

        TConfig(const TConfig& other) = default;
    };

    struct TCoordinatorInfo {
        typedef TDeque<TAutoPtr<TCoordinatorStep>> TQueueType; // todo: list/queue

        ui64 KnownPrevStep;
        TQueueType Queue;

        ui64 ActiveCoordinatorGeneration;
        TActorId AckTo;

        TCoordinatorInfo()
            : KnownPrevStep(0)
            , ActiveCoordinatorGeneration(0)
        {}
    };

    struct TVolatileState {
        TMap<ui64, TCoordinatorInfo> Domain;
        TMap<ui64, TCoordinatorInfo> Foreign;

        ui64 CompleteStep;
        ui64 LatestKnownStep;

        TVolatileState()
            : CompleteStep(0)
            , LatestKnownStep(0)
        {}
    };

    struct TTxInit;
    struct TTxConfigure;
    struct TTxSchema;
    struct TTxUpgrade;

    ITransaction* CreateTxInit();
    ITransaction* CreateTxConfigure(TActorId ackTo, ui64 version, const TVector<TCoordinatorId> &coordinators, ui32 timeCastBuckets);
    ITransaction* CreateTxSchema();
    ITransaction* CreateTxUpgrade();


    TConfig Config;
    TVolatileState VolatileState;

    TActorId ExecQueue;

    THashMap<TActorId, NKikimrTx::TEvCoordinatorSync> CoordinatorsSyncEnqueued;
    TVector<TAutoPtr<IEventHandle>> EnqueuedWatch;

    THashSet<TActorId> ConnectedServers;

    void Die(const TActorContext &ctx) override;
    void OnActivateExecutor(const TActorContext &ctx) override;
    void OnDetach(const TActorContext &ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) override;
    void DefaultSignalTabletActive(const TActorContext &ctx) override;

    void Handle(TEvSubDomain::TEvConfigure::TPtr &ev, const TActorContext &ctx);
    void HandleEnqueue(TEvTxCoordinator::TEvCoordinatorSync::TPtr &ev, const TActorContext &ctx);
    void HandleEnqueueWatch(TAutoPtr<IEventHandle> &ev, const TActorContext &ctx);
    void Handle(TEvTxCoordinator::TEvCoordinatorSync::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTxCoordinator::TEvCoordinatorStep::TPtr &ev, const TActorContext &ctx);
    void HandleForwardWatch(TAutoPtr<IEventHandle> &ev, const TActorContext &ctx);

    void Handle(TEvTabletPipe::TEvServerConnected::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr &ev, const TActorContext &ctx);

    void DoConfigure(const TEvSubDomain::TEvConfigure &ev, const TActorContext &ctx, const TActorId &ackTo = TActorId());

    static ui64 SubjectiveTime();
    void InitSelfState(const TActorContext &ctx);
    void ReplyEnqueuedSyncs(const TActorContext &ctx);
    void ProcessEnqueuedWatch(const TActorContext &ctx);


    void ReplySync(const TActorId &sender, const NKikimrTx::TEvCoordinatorSync &record, const TActorContext &ctx);
    void ReplyStep(const TActorId &sender, NKikimrTx::TEvCoordinatorStepResult::EStatus status, const NKikimrTx::TEvCoordinatorStep &request, const TActorContext &ctx);
    ui64 FindProgressCandidate();
    void Progress(ui64 to, const TActorContext &ctx);
    void CheckProgress(const TActorContext &ctx);
    void RequestLostAcks(const TActorId &sender, const NKikimrTx::TEvCoordinatorStep &request, const TActorContext &ctx);

    void ProcessDomainStep(const TActorId &sender, const NKikimrTx::TEvCoordinatorStep &request, ui64 coordinator, TCoordinatorInfo &info, const TActorContext &ctx);
    void ProcessForeignStep(const TActorId &sender, const NKikimrTx::TEvCoordinatorStep &request, ui64 coordinator, TCoordinatorInfo &info, const TActorContext &ctx);

public:
    struct Schema : NIceDb::Schema {
        static const ui32 CurrentVersion;

        struct State : Table<1> {
            enum EKeyType {
                DatabaseVersion,
            };

            struct StateKey : Column<0, NScheme::NTypeIds::Uint64> { using Type = EKeyType; }; // PK
            struct StateValue : Column<1, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<StateKey>;
            using TColumns = TableColumns<StateKey, StateValue>;
        };

        struct DomainConfiguration : Table<2> {
            struct Version : Column<1, NScheme::NTypeIds::Uint64> {};
            struct Coordinators : Column<2, NScheme::NTypeIds::String> { using Type = TVector<TCoordinatorId>; };
            struct TimeCastBuckets : Column<3, NScheme::NTypeIds::Uint32> { static constexpr ui32 Default = TDomainsInfo::TDomain::TimecastBucketsPerMediator; };

            using TKey = TableKey<Version>;
            using TColumns = TableColumns<Version, Coordinators, TimeCastBuckets>;
        };

        using TTables = SchemaTables<State, DomainConfiguration>;
    };

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_MEDIATOR_ACTOR;
    }

    TTxMediator(TTabletStorageInfo *info, const TActorId &tablet);

    // no incomming pipes is allowed in StateInit
    STFUNC_TABLET_INIT(StateInit,)

    STFUNC_TABLET_DEF(StateSync,
                     HFunc(TEvTxCoordinator::TEvCoordinatorSync, HandleEnqueue)
                     HFunc(TEvSubDomain::TEvConfigure, Handle)
                     FFunc(TEvMediatorTimecast::TEvWatch::EventType, HandleEnqueueWatch)
                     FFunc(TEvMediatorTimecast::TEvGranularWatch::EventType, HandleEnqueueWatch)
                     FFunc(TEvMediatorTimecast::TEvGranularWatchModify::EventType, HandleEnqueueWatch)
                     HFunc(TEvTabletPipe::TEvServerConnected, Handle)
                     HFunc(TEvTabletPipe::TEvServerDisconnected, Handle))

    STFUNC_TABLET_DEF(StateWork,
                     HFunc(TEvSubDomain::TEvConfigure, Handle)
                     HFunc(TEvTxCoordinator::TEvCoordinatorStep, Handle)
                     HFunc(TEvTxCoordinator::TEvCoordinatorSync, Handle)
                     FFunc(TEvMediatorTimecast::TEvWatch::EventType, HandleForwardWatch)
                     FFunc(TEvMediatorTimecast::TEvGranularWatch::EventType, HandleForwardWatch)
                     FFunc(TEvMediatorTimecast::TEvGranularWatchModify::EventType, HandleForwardWatch)
                     HFunc(NMon::TEvRemoteHttpInfo, RenderHtmlPage)
                     HFunc(TEvTabletPipe::TEvServerConnected, Handle)
                     HFunc(TEvTabletPipe::TEvServerDisconnected, Handle))

    STFUNC_TABLET_IGN(StateBroken,)
};
}

IActor* CreateTxMediatorTabletQueue(const TActorId &owner, ui64 mediator, ui64 hashRange, ui64 hashBucket);
IActor* CreateTxMediatorExecQueue(const TActorId &owner, ui64 mediator, ui64 hashRange, ui32 timecastBuckets);

}
