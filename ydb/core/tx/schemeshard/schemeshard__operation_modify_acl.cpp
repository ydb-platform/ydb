#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TModifyACL: public ISubOperationBase {
private:
    const TOperationId OperationId;
    const TTxTransaction Transaction;

public:
    TModifyACL(TOperationId id, const TTxTransaction& tx)
        : OperationId(id)
        , Transaction(tx)
    {
    }

    TModifyACL(TOperationId id)
        : OperationId(id)
    {
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = Transaction.GetModifyACL().GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TModifyACL Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", operationId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusSuccess, ui64(OperationId.GetTxId()), ui64(ssId));

        const TString acl = Transaction.GetModifyACL().GetDiffACL();

        TPath path = TPath::Resolve(parentPathStr, context.SS).Dive(name);
        {
            TPath::TChecker checks = path.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsCommonSensePath()
                .IsValidACL(acl);

            if (!checks) {
                TString explain = TStringBuilder() << "path table fail checks"
                                                   << ", path: " << path.PathString();
                auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
                return result;
            }
        }

        TString errStr;

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        NIceDb::TNiceDb db(context.GetDB());

        const TString owner = Transaction.GetModifyACL().GetNewOwner();

        if (!acl.empty()) {
            ++path.Base()->ACLVersion;
            path.Base()->ApplyACL(acl);
            context.SS->PersistACL(db, path.Base());

            auto subtree = context.SS->ListSubThee(path.Base()->PathId, context.Ctx);
            for (const TPathId pathId : subtree) {
                if (context.SS->PathsById.at(pathId)->IsMigrated()) {
                    continue;
                }
                context.OnComplete.PublishToSchemeBoard(OperationId, pathId);
            }

            if (!path.Base()->IsPQGroup()) {
                // YDBOPS-1328
                // its better we do not republish parent but
                // it is possible when we do not show ALC for children
                TPath parent = path.Parent();
                ++parent.Base()->DirAlterVersion;
                context.SS->PersistPathDirAlterVersion(db, parent.Base());
                context.SS->ClearDescribePathCaches(parent.Base());
                context.OnComplete.PublishToSchemeBoard(OperationId, parent.Base()->PathId);
            }

            context.OnComplete.UpdateTenants(std::move(subtree));
        }

        if (!owner.empty()) {
            path.Base()->Owner = owner;
            context.SS->PersistOwner(db, path.Base());

            ++path.Base()->DirAlterVersion;
            context.SS->PersistPathDirAlterVersion(db, path.Base());

            context.OnComplete.PublishToSchemeBoard(OperationId, path.Base()->PathId);

            TPath parent = path.Parent(); // we show owner in children listing, so we have to update it
            ++parent.Base()->DirAlterVersion;
            context.SS->PersistPathDirAlterVersion(db, parent.Base());
            context.SS->ClearDescribePathCaches(parent.Base());
            context.OnComplete.PublishToSchemeBoard(OperationId, parent.Base()->PathId);

            context.OnComplete.UpdateTenants({path.Base()->PathId});
        }

        context.OnComplete.DoneOperation(OperationId);
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_FAIL("no AbortPropose for TModifyACL");
    }

    void ProgressState(TOperationContext&) override {
        Y_FAIL("no progress state for modify acl");
    }

    void AbortUnsafe(TTxId, TOperationContext&) override {
        Y_FAIL("no AbortUnsafe for modify acl");
    }
};

}

namespace NKikimr {
namespace NSchemeShard {

ISubOperationBase::TPtr CreateModifyACL(TOperationId id, const TTxTransaction& tx) {
    return new TModifyACL(id, tx);
}

ISubOperationBase::TPtr CreateModifyACL(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state == TTxState::Invalid);
    return new TModifyACL(id);
}

}
}
