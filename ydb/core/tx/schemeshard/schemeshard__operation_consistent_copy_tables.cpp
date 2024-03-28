#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"
#include "schemeshard_path_element.h"

#include <ydb/core/base/path.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/algorithm.h>

NKikimrSchemeOp::TModifyScheme CopyTableTask(NKikimr::NSchemeShard::TPath& src, NKikimr::NSchemeShard::TPath& dst, bool omitFollowers, bool isBackup) {
    using namespace NKikimr::NSchemeShard;

    auto scheme = TransactionTemplate(dst.Parent().PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
    scheme.SetFailOnExist(true);

    auto operation = scheme.MutableCreateTable();
    operation->SetName(dst.LeafName());
    operation->SetCopyFromTable(src.PathString());
    operation->SetOmitFollowers(omitFollowers);
    operation->SetIsBackup(isBackup);

    return scheme;
}

NKikimrSchemeOp::TModifyScheme CreateIndexTask(NKikimr::NSchemeShard::TTableIndexInfo::TPtr indexInfo, NKikimr::NSchemeShard::TPath& dst) {
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

    return scheme;
}

namespace NKikimr::NSchemeShard {

TVector<ISubOperation::TPtr> CreateConsistentCopyTables(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateConsistentCopyTables);

    const auto& op = tx.GetCreateConsistentCopyTables();

    if (0 == op.CopyTableDescriptionsSize()) {
        TString msg = TStringBuilder() << "no task to do, empty list CopyTableDescriptions";
        return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, msg)};
    }

    TPath firstPath = TPath::Resolve(op.GetCopyTableDescriptions(0).GetSrcPath(), context.SS);
    {
        auto checks = TPath::TChecker(firstPath);
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard();

        if (!checks) {
            return {CreateReject(nextId, checks.GetStatus(), checks.GetError())};
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
        return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, TStringBuilder()
            << "Consistent copy object count limit exceeded"
                << ", limit: " << limit
                << ", objects: " << op.CopyTableDescriptionsSize()
        )};
    }

    TString errStr;
    if (!context.SS->CheckApplyIf(tx, errStr)) {
        return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, errStr)};
    }

    TVector<ISubOperation::TPtr> result;

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
                return {CreateReject(nextId, checks.GetStatus(), checks.GetError())};
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

        result.push_back(CreateCopyTable(NextPartId(nextId, result),
            CopyTableTask(srcPath, dstPath, descr.GetOmitFollowers(), descr.GetIsBackup()), sequences));

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
            Y_VERIFY_S(srcIndexPath.Base()->GetChildren().size() == 1, srcIndexPath.PathString() << " has children " << srcIndexPath.Base()->GetChildren().size() << " but 1 expected");

            TTableIndexInfo::TPtr indexInfo = context.SS->Indexes.at(pathId);
            result.push_back(CreateNewTableIndex(NextPartId(nextId, result), CreateIndexTask(indexInfo, dstIndexPath)));

            TString srcImplTableName = srcIndexPath.Base()->GetChildren().begin()->first;
            TPath srcImplTable = srcIndexPath.Child(srcImplTableName);
            Y_ABORT_UNLESS(srcImplTable.Base()->PathId == srcIndexPath.Base()->GetChildren().begin()->second);
            TPath dstImplTable = dstIndexPath.Child(srcImplTableName);

            result.push_back(CreateCopyTable(NextPartId(nextId, result),
                CopyTableTask(srcImplTable, dstImplTable, descr.GetOmitFollowers(), descr.GetIsBackup())));
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

    return result;
}

}
