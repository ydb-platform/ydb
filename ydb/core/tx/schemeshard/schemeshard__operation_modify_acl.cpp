#include "schemeshard__operation_helpers.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_path.h"

#include <ydb/core/base/auth.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

bool CheckSidExistsOrIsNonYdb(const TSchemeShard& ss, const TString& sid) {
    // non-YDB user's sid format is <login>@<subsystem>
    return sid.Contains('@') || NOperationHelpers::SidExists(ss, sid);
}

class TModifyACL: public TSubOperationBase {
public:
    using TSubOperationBase::TSubOperationBase;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = NOperationHelpers::GetTabletId(*context.SS);
        const TString databaseName = NOperationHelpers::GetRootPath(*context.SS);

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const auto& op = Transaction.GetModifyACL();
        const auto& name = op.GetName();
        const auto& acl = op.GetDiffACL();
        const auto& owner = op.GetNewOwner();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TModifyACL Propose"
            << ", path: " << parentPathStr << "/" << name
            << ", operationId: " << OperationId
            << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusSuccess, ui64(OperationId.GetTxId()), ui64(ssId));

        const auto path = TPath::Resolve(parentPathStr, context.SS).Dive(name);
        {
            const auto checks = path.Check();
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
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TString errStr;
        if (!NOperationHelpers::CheckApplyIf(*context.SS, Transaction, errStr, path->PathType)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        bool isAdmin = (context.UserToken && IsAdministrator(AppData(), context.UserToken.Get()));

        if (acl && NOperationHelpers::IsStrictAclCheckEnabled()) {
            NACLib::TDiffACL diffACL(acl);
            for (const NACLibProto::TDiffACE& diffACE : diffACL.GetDiffACE()) {
                if (static_cast<NACLib::EDiffType>(diffACE.GetDiffType()) == NACLib::EDiffType::Add) {
                    // add diff type is allowed if:
                    // - subject is a cluster administrator
                    // - or target sid is an external one (not a ydb-local)
                    // - or target sid is a local one and exist in this database
                    const auto& targetSid = diffACE.GetACE().GetSID();
                    bool allowed = (isAdmin || CheckSidExistsOrIsNonYdb(*context.SS, targetSid));
                    if (!allowed) {
                        result->SetError(NKikimrScheme::StatusPreconditionFailed,
                            TStringBuilder() << "SID " << targetSid << " not found in database `" << databaseName << "`");
                        return result;
                    }
                } // remove diff type is allowed in any case
            }
        }
        if (owner && NOperationHelpers::IsStrictAclCheckEnabled()) {
            // ownership transfer is allowed if:
            // - subject is a cluster administrator
            // - or target sid is an external one (not a ydb-local)
            // - or target sid is a local one and exist in this database
            bool allowed = (isAdmin || CheckSidExistsOrIsNonYdb(*context.SS, owner));
            if (!allowed) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed,
                    TStringBuilder() << "Owner SID " << owner << " not found in database `" << databaseName << "`");
                return result;
            }
        }

        THashSet<TPathId> subTree;
        if (acl || (owner && path.Base()->IsTable())) {
            subTree = NOperationHelpers::ListSubTree(*context.SS, path.Base()->PathId, context.Ctx);
        }

        THashSet<TPathId> affectedPaths;
        auto& db = context.GetDB();

        if (acl) {
            ++path.Base()->ACLVersion;
            path.Base()->ApplyACL(acl);
            NOperationHelpers::PersistACL(*context.SS, db, path.Base());

            for (const auto& pathId : subTree) {
                context.OnComplete.PublishToSchemeBoard(OperationId, pathId);
            }

            affectedPaths.insert(subTree.begin(), subTree.end());
        }

        if (owner) {
            THashSet<TPathId> pathIds = {path.Base()->PathId};
            if (path.Base()->IsTable()) {
                pathIds = subTree;
            }

            for (const auto& pathId : pathIds) {
                auto pathEl = NOperationHelpers::FindPathElement(*context.SS, pathId);
                if (!pathEl) {
                    Y_VERIFY_DEBUG_S(false, "unreachable");
                    continue;
                }

                pathEl->Owner = owner;
                NOperationHelpers::PersistOwner(*context.SS, db, pathEl);

                ++pathEl->DirAlterVersion;
                NOperationHelpers::PersistPathDirAlterVersion(*context.SS, db, pathEl);

                NOperationHelpers::ClearDescribePathCaches(*context.SS, pathEl);
                context.OnComplete.PublishToSchemeBoard(OperationId, pathId);
            }

            affectedPaths.insert(pathIds.begin(), pathIds.end());
        }

        if ((acl && !path.Base()->IsPQGroup()) || owner) {
            const auto parent = path.Parent();
            ++parent.Base()->DirAlterVersion;
            NOperationHelpers::PersistPathDirAlterVersion(*context.SS, db, parent.Base());
            NOperationHelpers::ClearDescribePathCaches(*context.SS, parent.Base());
            context.OnComplete.PublishToSchemeBoard(OperationId, parent.Base()->PathId);
        }

        context.OnComplete.UpdateTenants(std::move(affectedPaths));
        context.OnComplete.DoneOperation(OperationId);

        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TModifyACL");
    }

    bool ProgressState(TOperationContext&) override {
        Y_ABORT("no ProgressState for TModifyACL");
    }

    void AbortUnsafe(TTxId, TOperationContext&) override {
        Y_ABORT("no AbortUnsafe for TModifyACL");
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateModifyACL(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TModifyACL>(id, tx);
}

ISubOperation::TPtr CreateModifyACL(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state == TTxState::Invalid);
    return MakeSubOperation<TModifyACL>(id);
}

}
