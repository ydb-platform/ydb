#include "schemeshard__operation_part.h"
#include "schemeshard__operation_iface.h"
#include "schemeshard__operation_common.h"

#include "schemeshard_utils.h"  // for TransactionTemplate

#include <ydb/core/base/path.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/algorithm.h>

NKikimrSchemeOp::TModifyScheme CopyTableTask(NKikimr::NSchemeShard::TPath& src, NKikimr::NSchemeShard::TPath& dst, const NKikimrSchemeOp::TCopyTableConfig& descr) {
    using namespace NKikimr::NSchemeShard;

    auto scheme = TransactionTemplate(dst.Parent().PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
    scheme.SetFailOnExist(true);

    auto operation = scheme.MutableCreateTable();
    operation->SetName(dst.LeafName());
    operation->SetCopyFromTable(src.PathString());
    operation->SetOmitFollowers(descr.GetOmitFollowers());
    operation->SetIsBackup(descr.GetIsBackup());
    operation->SetAllowUnderSameOperation(descr.GetAllowUnderSameOperation());
    if (descr.HasCreateSrcCdcStream()) {
        auto* coOp = scheme.MutableCreateCdcStream();
        coOp->CopyFrom(descr.GetCreateSrcCdcStream());
    }

    return scheme;
}

NKikimrSchemeOp::TModifyScheme CreateIndexTask(NKikimr::NSchemeShard::TTableIndexInfo::TPtr indexInfo, NKikimr::NSchemeShard::TPath& dst) {
    using namespace NKikimr::NSchemeShard;

    auto scheme = TransactionTemplate(dst.Parent().PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex);
    scheme.SetFailOnExist(true);

    auto operation = scheme.MutableCreateTableIndex();
    operation->SetName(dst.LeafName());

    operation->SetType(indexInfo->Type);
    Y_ABORT_UNLESS(indexInfo->Type != NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree);
    for (const auto& keyName: indexInfo->IndexKeys) {
        *operation->MutableKeyColumnNames()->Add() = keyName;
    }

    for (const auto& dataColumn: indexInfo->IndexDataColumns) {
        *operation->MutableDataColumnNames()->Add() = dataColumn;
    }

    return scheme;
}

namespace NKikimr::NSchemeShard {

bool CreateConsistentCopyTables(
    TOperationId nextId,
    const TTxTransaction& tx,
    TOperationContext& context,
    TVector<ISubOperation::TPtr>& result)
{
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateConsistentCopyTables);

    const auto& op = tx.GetCreateConsistentCopyTables();

    if (0 == op.CopyTableDescriptionsSize()) {
        TString msg = TStringBuilder() << "no task to do, empty list CopyTableDescriptions";
        result = {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, msg)};
        return false;
    }

    TPath firstPath = TPath::Resolve(op.GetCopyTableDescriptions(0).GetSrcPath(), context.SS);
    {
        auto checks = TPath::TChecker(firstPath);
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard();

        if (!checks) {
            result = {CreateReject(nextId, checks.GetStatus(), checks.GetError())};
            return false;
        }
    }

    const auto allForBackup = AllOf(op.GetCopyTableDescriptions(), [](const auto& item) {
        return item.GetIsBackup();
    });

    const auto& limits = firstPath.DomainInfo()->GetSchemeLimits();
    const auto limit = allForBackup
        ? Max(limits.MaxObjectsInBackup, limits.MaxConsistentCopyTargets)
        : limits.MaxConsistentCopyTargets;

    if (op.CopyTableDescriptionsSize() > limit) {
        result = {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, TStringBuilder()
            << "Consistent copy object count limit exceeded"
                << ", limit: " << limit
                << ", objects: " << op.CopyTableDescriptionsSize()
        )};
        return false;
    }

    TString errStr;
    if (!context.SS->CheckApplyIf(tx, errStr)) {
        result = {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, errStr)};
        return false;
    }

    for (const auto& descr: op.GetCopyTableDescriptions()) {
        const auto& srcStr = descr.GetSrcPath();
        const auto& dstStr = descr.GetDstPath();

        TPath srcPath = TPath::Resolve(srcStr, context.SS);
        {
            TPath::TChecker checks = srcPath.Check();
            checks.IsResolved()
                  .NotDeleted()
                  .IsTable()
                  .IsCommonSensePath()
                  .IsTheSameDomain(firstPath);

            if (!checks) {
                result = {CreateReject(nextId, checks.GetStatus(), checks.GetError())};
                return false;
            }
        }

        TPath dstPath = TPath::Resolve(dstStr, context.SS);
        TPath dstParentPath = dstPath.Parent();

        THashSet<TString> sequences;
        for (const auto& child: srcPath.Base()->GetChildren()) {
            auto name = child.first;
            auto pathId = child.second;

            TPath childPath = srcPath.Child(name);
            if (!childPath.IsSequence() || childPath.IsDeleted()) {
                continue;
            }

            Y_ABORT_UNLESS(childPath.Base()->PathId == pathId);

            TSequenceInfo::TPtr sequenceInfo = context.SS->Sequences.at(pathId);
            const auto& sequenceDesc = sequenceInfo->Description;
            const auto& sequenceName = sequenceDesc.GetName();

            sequences.emplace(sequenceName);
        }

        result.push_back(CreateCopyTable(
                             NextPartId(nextId, result),
                             CopyTableTask(srcPath, dstPath, descr),
                             sequences));

        TVector<NKikimrSchemeOp::TSequenceDescription> sequenceDescriptions;
        for (const auto& child: srcPath.Base()->GetChildren()) {
            const auto& name = child.first;
            const auto& pathId = child.second;

            TPath srcIndexPath = srcPath.Child(name);
            TPath dstIndexPath = dstPath.Child(name);

            if (srcIndexPath.IsDeleted()) {
                continue;
            }

            if (srcIndexPath.IsSequence()) {
                TSequenceInfo::TPtr sequenceInfo = context.SS->Sequences.at(pathId);
                const auto& sequenceDesc = sequenceInfo->Description;
                sequenceDescriptions.push_back(sequenceDesc);
                continue;
            }

            if (descr.GetOmitIndexes()) {
                continue;
            }

            if (!srcIndexPath.IsTableIndex()) {
                continue;
            }

            Y_ABORT_UNLESS(srcIndexPath.Base()->PathId == pathId);
            TTableIndexInfo::TPtr indexInfo = context.SS->Indexes.at(pathId);
            if (indexInfo->Type == NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree) {
                result = {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter,
                                       "Consistent copy table doesn't support table with vector index")};
                return false;
            }
            Y_VERIFY_S(srcIndexPath.Base()->GetChildren().size() == 1, srcIndexPath.PathString() << " has children " << srcIndexPath.Base()->GetChildren().size() << " but 1 expected");

            result.push_back(CreateNewTableIndex(NextPartId(nextId, result), CreateIndexTask(indexInfo, dstIndexPath)));

            TString srcImplTableName = srcIndexPath.Base()->GetChildren().begin()->first;
            TPath srcImplTable = srcIndexPath.Child(srcImplTableName);
            Y_ABORT_UNLESS(srcImplTable.Base()->PathId == srcIndexPath.Base()->GetChildren().begin()->second);
            TPath dstImplTable = dstIndexPath.Child(srcImplTableName);

            result.push_back(CreateCopyTable(
                                 NextPartId(nextId, result),
                                 CopyTableTask(srcImplTable, dstImplTable, descr)));
        }

        for (auto&& sequenceDescription : sequenceDescriptions) {
            auto scheme = TransactionTemplate(
                dstPath.PathString(),
                NKikimrSchemeOp::EOperationType::ESchemeOpCreateSequence);
            scheme.SetFailOnExist(true);

            auto* copySequence = scheme.MutableCopySequence();
            copySequence->SetCopyFrom(srcPath.PathString() + "/" + sequenceDescription.GetName());
            *scheme.MutableSequence() = std::move(sequenceDescription);

            result.push_back(CreateCopySequence(NextPartId(nextId, result), scheme));
        }
    }

    return true;
}

TVector<ISubOperation::TPtr> CreateConsistentCopyTables(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    TVector<ISubOperation::TPtr> result;

    CreateConsistentCopyTables(nextId, tx, context, result);

    return result;
}

}
