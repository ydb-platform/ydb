#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

template <typename T>
struct TDebugEvent {
    static TString ToString(const typename T::TPtr& ev) {
        return ev->Get()->Record.ShortDebugString();
    }
};

template <>
struct TDebugEvent<NBackgroundTasks::TEvAddTaskResult> {
    static TString ToString(const NBackgroundTasks::TEvAddTaskResult::TPtr& ev) {
        return ev->Get()->GetDebugString();
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


#define DefineDebugReply(TEvType, ...) \
    template TString ISubOperationState::DebugReply(const TEvType::TPtr& ev);

    SCHEMESHARD_INCOMING_EVENTS(DefineDebugReply)
#undef DefineDebugReply


static TString LogMessage(const TString& ev, TOperationContext& context, bool ignore) {
    return TStringBuilder() << (ignore ? "Unexpected" : "Ignore") << " message"
        << ": tablet# " << context.SS->SelfTabletId()
        << ", ev# " << ev;
}

#define DefaultHandleReply(TEvType, ...) \
    bool ISubOperationState::HandleReply(TEvType::TPtr& ev, TOperationContext& context) { \
        const auto msg = LogMessage(DebugReply(ev), context, false);                      \
        LOG_CRIT_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, msg);               \
        Y_FAIL_S(msg);                                                                    \
    } \
    \
    bool TSubOperationState::HandleReply(TEvType::TPtr& ev, TOperationContext& context) { \
        const bool ignore = MsgToIgnore.contains(TEvType::EventType);                     \
        const auto msg = LogMessage(DebugReply(ev), context, ignore);                     \
        if (ignore) {                                                                     \
            LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, msg);           \
            return false;                                                                 \
        }                                                                                 \
        LOG_CRIT_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, msg);               \
        Y_FAIL_S(msg);                                                                    \
    } \
    \
    bool TSubOperation::HandleReply(TEvType::TPtr& ev, TOperationContext& context) { \
        return Progress(context, &ISubOperationState::HandleReply, ev, context);     \
    }

    SCHEMESHARD_INCOMING_EVENTS(DefaultHandleReply)
#undef DefaultHandleReply

void TSubOperationState::IgnoreMessages(TString debugHint, TSet<ui32> mgsIds) {
    LogHint = debugHint;
    MsgToIgnore.swap(mgsIds);
}

}
