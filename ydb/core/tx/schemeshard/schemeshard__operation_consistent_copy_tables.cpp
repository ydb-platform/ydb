#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_utils.h"  // for TransactionTemplate

#include <ydb/core/base/path.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <util/generic/algorithm.h>

static NKikimrSchemeOp::TModifyScheme CopyTableTask(NKikimr::NSchemeShard::TPath& src, NKikimr::NSchemeShard::TPath& dst, const NKikimrSchemeOp::TCopyTableConfig& descr) {
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
    if (descr.HasTargetPathTargetState()) {
        operation->SetPathState(descr.GetTargetPathTargetState());
    }

    return scheme;
}

static std::optional<NKikimrSchemeOp::TModifyScheme> CreateIndexTask(NKikimr::NSchemeShard::TTableIndexInfo::TPtr indexInfo, NKikimr::NSchemeShard::TPath& dst) {
    using namespace NKikimr::NSchemeShard;

    auto scheme = TransactionTemplate(dst.Parent().PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex);
    scheme.SetFailOnExist(true);

    auto operation = scheme.MutableCreateTableIndex();
    operation->SetName(dst.LeafName());

    operation->SetType(indexInfo->Type);

    for (const auto& keyName: indexInfo->IndexKeys) {
        *operation->MutableKeyColumnNames()->Add() = keyName;
    }

    for (const auto& dataColumn: indexInfo->IndexDataColumns) {
        *operation->MutableDataColumnNames()->Add() = dataColumn;
    }

    switch (indexInfo->Type) {
        case NKikimrSchemeOp::EIndexTypeGlobal:
        case NKikimrSchemeOp::EIndexTypeGlobalAsync:
        case NKikimrSchemeOp::EIndexTypeGlobalUnique:
            // no specialized index description
            Y_ASSERT(std::holds_alternative<std::monostate>(indexInfo->SpecializedIndexDescription));
            break;
        case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree:
            *operation->MutableVectorIndexKmeansTreeDescription() =
                std::get<NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>(indexInfo->SpecializedIndexDescription);
            break;
        case NKikimrSchemeOp::EIndexTypeGlobalFulltext:
            *operation->MutableFulltextIndexDescription() =
                std::get<NKikimrSchemeOp::TFulltextIndexDescription>(indexInfo->SpecializedIndexDescription);
            break;
        default:
            return {}; // reject
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

        THashSet<TString> sequences = GetLocalSequences(context, srcPath);

        if (descr.HasTargetPathTargetState()) {
            result.push_back(CreateCopyTable(
                                NextPartId(nextId, result),
                                CopyTableTask(srcPath, dstPath, descr),
                                sequences,
                                descr.GetTargetPathTargetState()));
        } else {
            result.push_back(CreateCopyTable(
                                NextPartId(nextId, result),
                                CopyTableTask(srcPath, dstPath, descr),
                                sequences));
        }

        for (const auto& child: srcPath.Base()->GetChildren()) {
            const auto& name = child.first;
            const auto& pathId = child.second;

            TPath srcIndexPath = srcPath.Child(name);
            TPath dstIndexPath = dstPath.Child(name);

            if (srcIndexPath.IsDeleted()) {
                continue;
            }

            if (srcIndexPath.IsSequence()) {
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
            auto scheme = CreateIndexTask(indexInfo, dstIndexPath);
            if (!scheme) {
                result = {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter,
                                       TStringBuilder{} << "Consistent copy table doesn't support table with index type " << indexInfo->Type)};
                return false;
            }
            result.push_back(CreateNewTableIndex(NextPartId(nextId, result), *scheme));

            for (const auto& [srcImplTableName, srcImplTablePathId] : srcIndexPath.Base()->GetChildren()) {
                TPath srcImplTable = srcIndexPath.Child(srcImplTableName);
                Y_ABORT_UNLESS(srcImplTable.Base()->PathId == srcImplTablePathId);
                TPath dstImplTable = dstIndexPath.Child(srcImplTableName);

                result.push_back(CreateCopyTable(NextPartId(nextId, result),
                    CopyTableTask(srcImplTable, dstImplTable, descr), GetLocalSequences(context, srcImplTable)));
                AddCopySequences(nextId, tx, context, result, srcImplTable, dstImplTable.PathString());
            }
        }

        AddCopySequences(nextId, tx, context, result, srcPath, dstPath.PathString());
    }

    return true;
}

THashSet<TString> GetLocalSequences(TOperationContext& context, const TPath& srcPath) {
    THashSet<TString> sequences;
    for (const auto& [name, pathId] : srcPath.Base()->GetChildren()) {
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
    return sequences;
}

void AddCopySequences(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context,
    TVector<ISubOperation::TPtr>& result, const TPath& srcTable, const TString& dstPath)
{
    for (const auto& [subName, subPathId] : srcTable.Base()->GetChildren()) {
        TPath subPath = srcTable.Child(subName);
        if (subPath.IsSequence() && !subPath.IsDeleted()) {
            TSequenceInfo::TPtr sequenceInfo = context.SS->Sequences.at(subPathId);
            const auto& sequenceDesc = sequenceInfo->Description;

            auto scheme = TransactionTemplate(dstPath, NKikimrSchemeOp::EOperationType::ESchemeOpCreateSequence);
            scheme.SetFailOnExist(tx.GetFailOnExist());

            auto* copySequence = scheme.MutableCopySequence();
            copySequence->SetCopyFrom(subPath.PathString());
            *scheme.MutableSequence() = sequenceDesc;

            result.push_back(CreateCopySequence(NextPartId(nextId, result), scheme));
        }
    }
}

TVector<ISubOperation::TPtr> CreateConsistentCopyTables(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    TVector<ISubOperation::TPtr> result;

    CreateConsistentCopyTables(nextId, tx, context, result);

    return result;
}

}
