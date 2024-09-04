#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"
#include "schemeshard_path.h"

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

ISubOperation::TPtr CascadeDropTableChildren(TVector<ISubOperation::TPtr>& result, const TOperationId& id, const TPath& table) {
    for (const auto& [childName, childPathId] : table.Base()->GetChildren()) {
        TPath child = table.Child(childName);
        {
            TPath::TChecker checks = child.Check();
            checks
                .NotEmpty()
                .IsResolved();

            if (checks) {
                if (child.IsDeleted()) {
                    continue;
                }
            }

            if (child.IsTableIndex()) {
                checks.IsTableIndex();
            } else if (child.IsCdcStream()) {
                checks.IsCdcStream();
            } else if (child.IsSequence()) {
                checks.IsSequence();
            }

            checks.NotDeleted()
                .NotUnderDeleting()
                .NotUnderOperation();

            if (!checks) {
                return CreateReject(id, checks.GetStatus(), checks.GetError());
            }
        }
        Y_ABORT_UNLESS(child.Base()->PathId == childPathId);

        if (child.IsSequence()) {
            auto dropSequence = TransactionTemplate(table.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropSequence);
            dropSequence.MutableDrop()->SetName(ToString(child->Name));

            result.push_back(CreateDropSequence(NextPartId(id, result), dropSequence));
            continue;
        } else if (child.IsTableIndex()) {
            auto dropIndex = TransactionTemplate(table.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropTableIndex);
            dropIndex.MutableDrop()->SetName(ToString(child.Base()->Name));

            result.push_back(CreateDropTableIndex(NextPartId(id, result), dropIndex));
        } else if (child.IsCdcStream()) {
            auto dropStream = TransactionTemplate(table.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStreamImpl);
            dropStream.MutableDrop()->SetName(ToString(child.Base()->Name));

            result.push_back(CreateDropCdcStreamImpl(NextPartId(id, result), dropStream));
        }

        for (auto& [implName, implPathId] : child.Base()->GetChildren()) {
            Y_ABORT_UNLESS(NTableIndex::IsImplTable(implName) 
                        || implName == "streamImpl"
                , "unexpected name %s", implName.c_str());

            TPath implPath = child.Child(implName);
            {
                TPath::TChecker checks = implPath.Check();
                checks
                    .NotEmpty()
                    .IsResolved()
                    .NotDeleted()
                    .NotUnderDeleting()
                    .NotUnderOperation();

                if (checks) {
                    if (implPath.Base()->IsTable()) {
                        checks
                            .IsTable()
                            .IsInsideTableIndexPath();
                    } else if (implPath.Base()->IsPQGroup()) {
                        checks
                            .IsPQGroup()
                            .IsInsideCdcStreamPath();
                    }
                }

                if (!checks) {
                    return CreateReject(id, checks.GetStatus(), checks.GetError());
                }
            }
            Y_ABORT_UNLESS(implPath.Base()->PathId == implPathId);

            if (implPath.Base()->IsTable()) {
                auto dropIndexTable = TransactionTemplate(child.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropTable);
                dropIndexTable.MutableDrop()->SetName(ToString(implPath.Base()->Name));

                result.push_back(CreateDropTable(NextPartId(id, result), dropIndexTable));
                if (auto reject = CascadeDropTableChildren(result, id, implPath)) {
                    return reject;
                }
            } else if (implPath.Base()->IsPQGroup()) {
                auto dropPQGroup = TransactionTemplate(child.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup);
                dropPQGroup.MutableDrop()->SetName(ToString(implPath.Base()->Name));

                result.push_back(CreateDropPQ(NextPartId(id, result), dropPQGroup));
            }
        }
    }

    return nullptr;
}

}
