#include "schemeshard__operation_part.h"

#include "schemeshard_impl.h"
#include "schemeshard_operation_factory.h"
#include "schemeshard_operation_planner.h"
#include "schemeshard_path.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/blob_depot/events.h>
#include <ydb/core/blockstore/core/blockstore.h>
#include <ydb/core/filestore/core/filestore.h>
#include <ydb/core/kesus/tablet/events.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/replication/controller/public_events.h>
#include <ydb/core/tx/sequenceshard/public/events.h>
#include <ydb/core/test_tablet/events.h>
#include <ydb/core/tx/tx_processing.h>

namespace NKikimr::NSchemeShard {

template <typename T>
struct TDebugEvent {
    static TString ToString(const typename T::TPtr& ev) {
        return ev->Get()->Record.ShortDebugString();
    }
};

template <>
struct TDebugEvent<TEvPrivate::TEvOperationPlan> {
    static TString ToString(const TEvPrivate::TEvOperationPlan::TPtr& ev) {
        return TStringBuilder() << "TEvOperationPlan {"
                                << " StepId: " << ev->Get()->StepId
                                << " TxId: " << ev->Get()->TxId
                                << " }";
    }
};

template <>
struct TDebugEvent<TEvPrivate::TEvCompletePublication> {
    static TString ToString(const TEvPrivate::TEvCompletePublication::TPtr& ev) {
        return ev->Get()->ToString();
    }
};

template <>
struct TDebugEvent<TEvPrivate::TEvCompleteBarrier> {
    static TString ToString(const TEvPrivate::TEvCompleteBarrier::TPtr& ev) {
        return ev->Get()->ToString();
    }
};

template <>
struct TDebugEvent<TEvPrivate::TEvCommitTenantUpdate> {
    static TString ToString(const TEvPrivate::TEvCommitTenantUpdate::TPtr&) {
        return "TEvCommitTenantUpdate { }";
    }
};

template <>
struct TDebugEvent<TEvPrivate::TEvUndoTenantUpdate> {
    static TString ToString(const TEvPrivate::TEvUndoTenantUpdate::TPtr&) {
        return "TEvUndoTenantUpdate { }";
    }
};


template <EventBasePtr TEvPtr>
TString ISubOperationState::DebugReply(const TEvPtr& ev) {
    using TEvType = typename EventTypeFromTEvPtr<TEvPtr>::type;
    return TDebugEvent<TEvType>::ToString(ev);
}


#define DefineDebugReply(NS, TEvType, ...) \
    template TString ISubOperationState::DebugReply(const ::NKikimr::NS::TEvType ## __HandlePtr& ev);

    SCHEMESHARD_INCOMING_EVENTS(DefineDebugReply)
#undef DefineDebugReply


static TString LogMessage(const TString& ev, TOperationContext& context, bool ignore) {
    return TStringBuilder() << (ignore ? "Ignore" : "Unexpected") << " message"
        << ": tablet# " << context.SS->SelfTabletId()
        << ", ev# " << ev;
}

#define DefaultHandleReply(NS, TEvType, ...) \
    bool ISubOperationState::HandleReply(::NKikimr::NS::TEvType ## __HandlePtr& ev, TOperationContext& context) { \
        const auto msg = LogMessage(DebugReply(ev), context, false); \
        LOG_CRIT_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "HandleReply " #NS << "::" << #TEvType << " " << msg); \
        Y_FAIL_S(msg); \
    } \
    \
    bool TSubOperationState::HandleReply(::NKikimr::NS::TEvType ## __HandlePtr& ev, TOperationContext& context) { \
        const bool ignore = MsgToIgnore.contains(NS::TEvType::EventType); \
        const auto msg = LogMessage(DebugReply(ev), context, ignore); \
        if (ignore) { \
            LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "HandleReply " #NS << "::" << #TEvType << " " << msg << " debug: " << DebugHint()); \
            return false; \
        } \
        LOG_CRIT_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "HandleReply " #NS << "::" << #TEvType << " " << msg << " debug: " << DebugHint()); \
        Y_FAIL_S(msg); \
    } \
    \
    bool TSubOperation::HandleReply(::NKikimr::NS::TEvType ## __HandlePtr& ev, TOperationContext& context) { \
        return Progress(context, &ISubOperationState::HandleReply, ev); \
    }

    SCHEMESHARD_INCOMING_EVENTS(DefaultHandleReply)
#undef DefaultHandleReply

void TSubOperationState::IgnoreMessages(TString debugHint, TSet<ui32> mgsIds) {
    LogHint = debugHint;
    MsgToIgnore.swap(mgsIds);
}

TVector<ISubOperation::TPtr> ConstructPartsFromPlan(TOperationId nextId, std::shared_ptr<const TSealedOperationPlan> plan,
        TOperationContext& context)
{
    TVector<ISubOperation::TPtr> result;
    for (const auto& blueprint : plan->GetParts()) {
        Y_ABORT_UNLESS(!std::holds_alternative<TMkDirPartBindings>(blueprint.Bindings));
        const TOperationId id(nextId.GetTxId(), nextId.GetSubTxId() + result.size());
        ISubOperation::TPtr part = AppData()->SchemeOperationFactory->MakePlannedPart(id, *plan, blueprint, context);
        part->BindToPlan(plan, blueprint);
        result.push_back(std::move(part));
    }
    return result;
}

// The cascade beneath a table lives in the planner (schemeshard_operation_planner_drop_table.cpp).
// This helper serves the operations that decompose by hand -- drop index, drop backup
// collection -- and appends the same parts the planner would.
ISubOperation::TPtr CascadeDropTableChildren(TVector<ISubOperation::TPtr>& result, const TOperationId& id, const TPath& table, TOperationContext& context) {
    auto planResult = PlanDropTableChildren(table, context);
    if (const auto* rejected = std::get_if<TRejectedOperation>(&planResult)) {
        return CreateReject(id, *rejected);
    }
    auto parts = ConstructPartsFromPlan(NextPartId(id, result), std::get<std::shared_ptr<const TSealedOperationPlan>>(planResult), context);
    std::move(parts.begin(), parts.end(), std::back_inserter(result));
    return nullptr;
}

static TPath ResolvePlanned(const TSealedOperationPlan& plan, const TPlannedPathView& view, TOperationContext& context) {
    if (view.PathId) {
        TPath byId = TPath::Init(*view.PathId, context.SS);
        if (byId.IsResolved()) {
            return byId;
        }
    }
    return TPath::Resolve(plan.Absolute(view.Path), context.SS);
}

TPath TSubOperationBase::PlannedPath(TPlanEffectId effect, TOperationContext& context) const {
    const auto& plan = GetPlan();
    return ResolvePlanned(plan, plan.ViewOfEffect(effect), context);
}

TPath TSubOperationBase::PlannedWritePath(TPhysicalWriteId write, TOperationContext& context) const {
    const auto& plan = GetPlan();
    return ResolvePlanned(plan, plan.ViewOfWrite(write), context);
}

}
