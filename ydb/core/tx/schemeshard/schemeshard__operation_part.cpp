#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"
#include "schemeshard_path_describer.h"

namespace NKikimr {
namespace NSchemeShard {

template<class T>
struct TDebugEvent {
    static TString ToString(const typename T::TPtr& ev) {
        return ev->Get()->Record.ShortDebugString();
    }
};

template<>
struct TDebugEvent<NBackgroundTasks::TEvAddTaskResult> {
    static TString ToString(const NBackgroundTasks::TEvAddTaskResult::TPtr& ev) {
        return ev->Get()->GetDebugString();
    }
};

template <>
struct TDebugEvent<TEvPrivate::TEvOperationPlan> {
    static TString ToString (const TEvPrivate::TEvOperationPlan::TPtr& ev) {
        return TStringBuilder() << "TEvOperationPlan {"
                                << " StepId: " << ev->Get()->StepId
                                << " TxId: " << ev->Get()->TxId
                                << " }";
    }
};

template <>
struct TDebugEvent<TEvPrivate::TEvCompletePublication> {
    static TString ToString (const TEvPrivate::TEvCompletePublication::TPtr& ev) {
        return ev->Get()->ToString();
    }
};

template <>
struct TDebugEvent<TEvPrivate::TEvCompleteBarrier> {
    static TString ToString (const TEvPrivate::TEvCompleteBarrier::TPtr& ev) {
        return ev->Get()->ToString();
    }
};

template <>
struct TDebugEvent<TEvPrivate::TEvCommitTenantUpdate> {
    static TString ToString (const TEvPrivate::TEvCommitTenantUpdate::TPtr& /*ev*/) {
        return TStringBuilder() << "TEvCommitTenantUpdate {" << " }";
    }
};

template <>
struct TDebugEvent<TEvPrivate::TEvUndoTenantUpdate> {
    static TString ToString (const TEvPrivate::TEvUndoTenantUpdate::TPtr& /*ev*/) {
        return TStringBuilder() << "TEvUndoTenantUpdate {" << " }";
    }
};

#define DefaultDebugReply(TEvType, ...)                 \
    TString IOperationBase::DebugReply(const TEvType::TPtr& ev) {  \
        return TDebugEvent<TEvType>::ToString(ev);                  \
    }                                                               \
    TString TSubOperationState::DebugReply(const TEvType::TPtr& ev) {  \
       return TDebugEvent<TEvType>::ToString(ev);                   \
    }

    SCHEMESHARD_INCOMING_EVENTS(DefaultDebugReply)
#undef DefaultDebugReply


#define DefaultHandleReply(TEvType, ...)  \
    void IOperationBase::HandleReply(TEvType::TPtr& ev, TOperationContext& context) {   \
        TStringBuilder msg;                                                    \
        msg << "Unexpected message,"                                           \
            << " TEvType# " << #TEvType                                        \
            << " debug msg# " << DebugReply(ev)                                \
            << " at tablet# " << context.SS->SelfTabletId();                       \
        LOG_CRIT_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, msg);    \
        Y_FAIL_S(msg);                                                         \
    }                                                                          \
    bool TSubOperationState::HandleReply(TEvType::TPtr& ev, TOperationContext& context) {   \
        if (!MsgToIgnore.empty() && MsgToIgnore.contains(TEvType::EventType)) {             \
            LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,      \
               "Superflous message, " << LogHint                               \
               << " TEvType# " << #TEvType                                     \
               << " debug msg# " << DebugReply(ev)                             \
               << " at tablet# " << context.SS->TabletID());                   \
            return false;                                                      \
        }                                                                      \
        TStringBuilder msg;                                                    \
        msg << "Unexpected message, "                                          \
            << LogHint                                                         \
            << " TEvType# " << #TEvType                                        \
            << " debug msg# " << DebugReply(ev)                                \
            << " at tablet# " << context.SS->SelfTabletId();                       \
        LOG_CRIT_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, msg);    \
        Y_FAIL_S(msg);                                                         \
        return false;                                                          \
    }

    SCHEMESHARD_INCOMING_EVENTS(DefaultHandleReply)
#undef DefaultHandleReply

void TSubOperationState::IgnoreMessages(TString debugHint, TSet<ui32> mgsIds) {
    LogHint = debugHint;
    MsgToIgnore.swap(mgsIds);
}

}
}
