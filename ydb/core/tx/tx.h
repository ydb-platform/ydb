#pragma once
#include "defs.h"
#include <ydb/core/base/tabletid.h>
#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/tx.pb.h>
#include <ydb/core/protos/subdomains.pb.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/appdata.h>

#include <ydb/library/actors/core/event_pb.h>

namespace NKikimr {
struct TEvTxProxy {
    enum EEv {
        EvProposeTransaction = EventSpaceBegin(TKikimrEvents::ES_TX_PROXY),
        EvAcquireReadStep,
        EvSubscribeReadStep,
        EvUnsubscribeReadStep,
        EvSubscribeLastStep,
        EvUnsubscribeLastStep,
        EvRequirePlanSteps,

        EvProposeTransactionStatus = EvProposeTransaction + 1 * 512,
        EvAcquireReadStepResult,
        EvSubscribeReadStepResult,
        EvSubscribeReadStepUpdate,
        EvUpdatedLastStep,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_PROXY), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_PROXY)");

    struct TEvProposeTransaction : public TEventPB<TEvProposeTransaction, NKikimrTx::TEvProposeTransaction, EvProposeTransaction> {
        static constexpr ui32 FlagVolatile = NKikimrTx::TProxyTransaction::FLAG_VOLATILE;

        static constexpr ui32 AffectedRead = NKikimrTx::TProxyTransaction::FLAG_AFFECTED_READ;
        static constexpr ui32 AffectedWrite = NKikimrTx::TProxyTransaction::FLAG_AFFECTED_WRITE;

        TEvProposeTransaction() = default;

        TEvProposeTransaction(ui64 coordinator, ui64 txId, ui8 execLevel, ui64 minStep, ui64 maxStep);
    };

    struct TEvProposeTransactionStatus : public TEventPB<TEvProposeTransactionStatus, NKikimrTx::TEvProposeTransactionStatus, EvProposeTransactionStatus> {
        enum class EStatus {
            StatusUnknown,
            StatusDeclined,
            StatusOutdated,
            StatusAborted,
            StatusDeclinedNoSpace,
            StatusRestarting, // coordinator is restarting (tx dropped)

            StatusAccepted = 16, // accepted by coordinator
            StatusPlanned, // planned by coordinator
            StatusProcessed, // plan entry delivered to all tablets (for synth-exec-level)
            StatusConfirmed, // execution confirmed by moderator (for synth-exec-level)
        };

        TEvProposeTransactionStatus() = default;

        TEvProposeTransactionStatus(EStatus status, ui64 txid, ui64 stepId);

        EStatus GetStatus() const {
            Y_DEBUG_ABORT_UNLESS(Record.HasStatus());
            return static_cast<EStatus>(Record.GetStatus());
        }
    };

    struct TEvAcquireReadStep
        : public TEventPB<TEvAcquireReadStep, NKikimrTx::TEvAcquireReadStep, EvAcquireReadStep>
    {
        TEvAcquireReadStep() = default;

        explicit TEvAcquireReadStep(ui64 coordinator) {
            Record.SetCoordinatorID(coordinator);
        }
    };

    struct TEvAcquireReadStepResult
        : public TEventPB<TEvAcquireReadStepResult, NKikimrTx::TEvAcquireReadStepResult, EvAcquireReadStepResult>
    {
        TEvAcquireReadStepResult() = default;

        TEvAcquireReadStepResult(ui64 coordinator, ui64 step) {
            Record.SetCoordinatorID(coordinator);
            Record.SetStep(step);
        }
    };

    struct TEvSubscribeReadStep
        : public TEventPB<TEvSubscribeReadStep, NKikimrTx::TEvSubscribeReadStep, EvSubscribeReadStep>
    {
        TEvSubscribeReadStep() = default;

        TEvSubscribeReadStep(ui64 coordinator, ui64 seqNo) {
            Record.SetCoordinatorID(coordinator);
            Record.SetSeqNo(seqNo);
        }
    };

    struct TEvUnsubscribeReadStep
        : public TEventPB<TEvUnsubscribeReadStep, NKikimrTx::TEvUnsubscribeReadStep, EvUnsubscribeReadStep>
    {
        TEvUnsubscribeReadStep() = default;

        TEvUnsubscribeReadStep(ui64 coordinator, ui64 seqNo) {
            Record.SetCoordinatorID(coordinator);
            Record.SetSeqNo(seqNo);
        }
    };

    struct TEvSubscribeReadStepResult
        : public TEventPB<TEvSubscribeReadStepResult, NKikimrTx::TEvSubscribeReadStepResult, EvSubscribeReadStepResult>
    {
        TEvSubscribeReadStepResult() = default;

        TEvSubscribeReadStepResult(ui64 coordinator, ui64 seqNo, ui64 lastAcquireStep, ui64 nextAcquireStep) {
            Record.SetCoordinatorID(coordinator);
            Record.SetSeqNo(seqNo);
            Record.SetLastAcquireStep(lastAcquireStep);
            Record.SetNextAcquireStep(nextAcquireStep);
        }
    };

    struct TEvSubscribeReadStepUpdate
        : public TEventPB<TEvSubscribeReadStepUpdate, NKikimrTx::TEvSubscribeReadStepUpdate, EvSubscribeReadStepUpdate>
    {
        TEvSubscribeReadStepUpdate() = default;

        TEvSubscribeReadStepUpdate(ui64 coordinator, ui64 seqNo, ui64 nextAcquireStep) {
            Record.SetCoordinatorID(coordinator);
            Record.SetSeqNo(seqNo);
            Record.SetNextAcquireStep(nextAcquireStep);
        }
    };

    struct TEvSubscribeLastStep
        : public TEventPB<TEvSubscribeLastStep, NKikimrTx::TEvSubscribeLastStep, EvSubscribeLastStep>
    {
        TEvSubscribeLastStep() = default;

        TEvSubscribeLastStep(ui64 coordinator, ui64 seqNo) {
            Record.SetCoordinatorID(coordinator);
            Record.SetSeqNo(seqNo);
        }
    };

    struct TEvUnsubscribeLastStep
        : public TEventPB<TEvUnsubscribeLastStep, NKikimrTx::TEvUnsubscribeLastStep, EvUnsubscribeLastStep>
    {
        TEvUnsubscribeLastStep() = default;

        TEvUnsubscribeLastStep(ui64 coordinator, ui64 seqNo) {
            Record.SetCoordinatorID(coordinator);
            Record.SetSeqNo(seqNo);
        }
    };

    struct TEvUpdatedLastStep
        : public TEventPB<TEvUpdatedLastStep, NKikimrTx::TEvUpdatedLastStep, EvUpdatedLastStep>
    {
        TEvUpdatedLastStep() = default;

        TEvUpdatedLastStep(ui64 coordinator, ui64 seqNo, ui64 lastStep) {
            Record.SetCoordinatorID(coordinator);
            Record.SetSeqNo(seqNo);
            Record.SetLastStep(lastStep);
        }
    };

    struct TEvRequirePlanSteps
        : public TEventPB<TEvRequirePlanSteps, NKikimrTx::TEvRequirePlanSteps, EvRequirePlanSteps>
    {
        TEvRequirePlanSteps() = default;

        TEvRequirePlanSteps(ui64 coordinator, ui64 planStep) {
            Record.SetCoordinatorID(coordinator);
            Record.AddPlanSteps(planStep);
        }

        TEvRequirePlanSteps(ui64 coordinator, const std::set<ui64>& planSteps) {
            Record.SetCoordinatorID(coordinator);
            Record.MutablePlanSteps()->Reserve(planSteps.size());
            for (ui64 planStep : planSteps) {
                Record.AddPlanSteps(planStep);
            }
        }
    };
};

// basic

struct TExecLevelHierarchy {
    struct TEntry {
        ui32 ExecLevel;
        ui64 ReversedDomainMask;
    };

    TVector<TEntry> Entries;

    ui32 Select(ui64 mask) const {
        for (ui32 i = 0, e = Entries.size(); i != e; ++i) {
            const TEntry &x = Entries[i];

            if ((x.ReversedDomainMask & mask) == 0)
                return x.ExecLevel;
        }
        return 0;
    }
};

// test hierarchy
// one availability domain #0.
// one synthetic execution level (#0) with 2 controller shards (#0, #1).
// one domain execution level (#1) with 2 controller shards (#0, #1).
// one proxy #0.
// one mediator (0-#0)
// three dummy tx-tablets in domain (##0-2)
//      or 8 data shard in domain (##0-7)
// one scheme shard (#F0)

struct TTestTxConfig {
    static constexpr ui64 DomainUid = 0;
    static constexpr ui64 Coordinator = MakeTabletID(false, 0x0000000000800001);
    static constexpr ui64 Mediator = MakeTabletID(false, 0x0000000000810001);
    static constexpr ui64 TxAllocator = MakeTabletID(false, 0x0000000000820001);
    static constexpr ui64 Moderator = MakeTabletID(false, 0x0000000000830001);
    static constexpr ui64 TxTablet0 = 0x0000000000900000;
    static constexpr ui64 TxTablet1 = 0x0000000000900001;
    static constexpr ui64 TxTablet2 = 0x0000000000900002;
    static constexpr ui64 TxTablet3 = 0x0000000000900003;
    static constexpr ui64 TxTablet4 = 0x0000000000900004;
    static constexpr ui64 TxTablet5 = 0x0000000000900005;
    static constexpr ui64 TxTablet6 = 0x0000000000900006;
    static constexpr ui64 TxTablet7 = 0x0000000000900006;
    static constexpr ui64 FakeHiveTablets = MakeTabletID(true, 0x000000000090000a);
    static constexpr ui64 SchemeShard = MakeTabletID(false, 0x00000000008587a0);
    static constexpr ui64 Hive = MakeTabletID(false,  0x000000000000A001);
    static constexpr ui64 UseLessId = 0xFFFFFFFFFFFFFFF;
};

struct TEvSubDomain {
    enum EEv {
        EvConfigure = EventSpaceBegin(TKikimrEvents::ES_SUB_DOMAIN),
        EvConfigureStatus,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_SUB_DOMAIN), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_SUB_DOMAIN)");

    struct TEvConfigure : public TEventPB<TEvConfigure, NKikimrSubDomains::TProcessingParams, EvConfigure> {
        TEvConfigure() = default;

        TEvConfigure(const NKikimrSubDomains::TProcessingParams &processing);
        TEvConfigure(NKikimrSubDomains::TProcessingParams &&processing);
    };

    struct TEvConfigureStatus : public TEventPB<TEvConfigureStatus, NKikimrTx::TEvSubDomainConfigurationAck, EvConfigureStatus> {
        TEvConfigureStatus() = default;

        TEvConfigureStatus(NKikimrTx::TEvSubDomainConfigurationAck::EStatus status,
                           ui64 tabletId);
    };
};

TAutoPtr<TEvSubDomain::TEvConfigure> CreateDomainConfigurationFromStatic(const TAppData *appdata);

}

template<>
inline void Out<NKikimr::TEvTxProxy::TEvProposeTransactionStatus::EStatus>(IOutputStream& o,
        NKikimr::TEvTxProxy::TEvProposeTransactionStatus::EStatus x) {
    o << (ui32)x;
}


