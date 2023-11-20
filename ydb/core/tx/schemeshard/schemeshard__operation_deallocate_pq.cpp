#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TDeallocatePQ: public TSubOperationBase {
public:
    using TSubOperationBase::TSubOperationBase;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = Transaction.GetDeallocatePersQueueGroup().GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TDeallocatePQ Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

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
                .NotUnderOperation()
                .IsPQGroup()
                .NotUnderOperation();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (path.IsResolved() && path.Base()->IsPQGroup() && path.Base()->PlannedToDrop()) {
                    result->SetPathDropTxId(ui64(path.Base()->DropTxId));
                    result->SetPathId(path.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        TPath parent = path.Parent();
        {
            TPath::TChecker checks = parent.Check();
            checks
                .NotEmpty()
                .IsResolved()
                .NotDeleted();

            if (checks) {
                if (parent.Base()->IsCdcStream()) {
                    checks
                        .IsCdcStream()
                        .IsInsideCdcStreamPath()
                        .IsUnderDeleting(TEvSchemeShard::EStatus::StatusNameConflict)
                        .IsUnderTheSameOperation(OperationId.GetTxId());
                } else {
                    checks
                        .IsLikeDirectory()
                        .IsCommonSensePath()
                        .NotUnderDeleting();
                }
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        auto pathId = path.Base()->PathId;
        TTopicInfo::TPtr pqGroup = context.SS->Topics.at(pathId);
        Y_ABORT_UNLESS(pqGroup);

        if (pqGroup->AlterData) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "Deallocate over Create/Alter");
            return result;
        }

        NIceDb::TNiceDb db(context.GetDB());

        path.Base()->LastTxId = OperationId.GetTxId();
        TStepId fakeStep = TStepId(TAppData::TimeProvider->Now().MilliSeconds());
        path->SetDropped(fakeStep, OperationId.GetTxId());
        context.SS->PersistDropStep(db, pathId, fakeStep, OperationId);

        context.SS->TabletCounters->Simple()[COUNTER_PQ_GROUP_COUNT].Sub(1);

        auto tabletConfig = pqGroup->TabletConfig;
        NKikimrPQ::TPQTabletConfig config;
        Y_ABORT_UNLESS(!tabletConfig.empty());
        bool parseOk = ParseFromStringNoSizeLimit(config, tabletConfig);
        Y_ABORT_UNLESS(parseOk);

        const PQGroupReserve reserve(config, pqGroup->TotalPartitionCount);

        auto domainInfo = context.SS->ResolveDomainInfo(pathId);
        domainInfo->DecPathsInside();
        domainInfo->DecPQPartitionsInside(pqGroup->TotalPartitionCount);
        domainInfo->DecPQReservedStorage(reserve.Storage);
        domainInfo->AggrDiskSpaceUsage({}, pqGroup->Stats);

        context.SS->ChangeDiskSpaceTopicsTotalBytes(domainInfo->GetPQAccountStorage());
        context.SS->TabletCounters->Simple()[COUNTER_STREAM_RESERVED_THROUGHPUT].Sub(reserve.Throughput);
        context.SS->TabletCounters->Simple()[COUNTER_STREAM_RESERVED_STORAGE].Sub(reserve.Storage);

        context.SS->TabletCounters->Simple()[COUNTER_STREAM_SHARDS_COUNT].Sub(pqGroup->TotalPartitionCount);

        parent->DecAliveChildren();

        if (!AppData()->DisableSchemeShardCleanupOnDropForTest) {
            context.SS->PersistRemovePersQueueGroup(db, pathId);
        }

        context.SS->TabletCounters->Simple()[COUNTER_USER_ATTRIBUTES_COUNT].Sub(path->UserAttrs->Size());
        context.SS->PersistUserAttributes(db, path->PathId, path->UserAttrs, nullptr);

        auto parentDir = path.Parent();
        ++parentDir.Base()->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDir.Base());
        context.SS->ClearDescribePathCaches(parentDir.Base());
        context.SS->ClearDescribePathCaches(path.Base());

        if (!context.SS->DisablePublicationsOfDropping) {
            context.OnComplete.PublishToSchemeBoard(OperationId, parentDir.Base()->PathId);
            context.OnComplete.PublishToSchemeBoard(OperationId, path.Base()->PathId);
        }

        context.OnComplete.DoneOperation(OperationId);
        return result;
    }

    bool ProgressState(TOperationContext&) override {
        Y_ABORT("no progress state for TDeallocatePQ");
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TDeallocatePQ");
    }

    void AbortUnsafe(TTxId, TOperationContext&) override {
        Y_ABORT("no AbortUnsafe for TDeallocatePQ");
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateDeallocatePQ(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDeallocatePQ>(id, tx);
}

ISubOperation::TPtr CreateDeallocatePQ(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state == TTxState::Invalid);
    return MakeSubOperation<TDeallocatePQ>(id);
}

}
