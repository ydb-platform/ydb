#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_path_element.h"

#include "schemeshard_impl.h"

#include <ydb/core/base/path.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

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


namespace NKikimr {
namespace NSchemeShard {

TVector<ISubOperationBase::TPtr> CreateConsistentCopyTables(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    Y_VERIFY(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateConsistentCopyTables);

    auto consistentCopying = tx.GetCreateConsistentCopyTables();

    if (0 == consistentCopying.CopyTableDescriptionsSize()) {
        TString msg = TStringBuilder() << "no task to do, empty list CopyTableDescriptions";
        return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, msg)};
    }

    TPath firstPath = TPath::Resolve(consistentCopying.GetCopyTableDescriptions(0).GetSrcPath(), context.SS);
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

    TSubDomainInfo::TPtr domainInfo = firstPath.DomainInfo();
    if (consistentCopying.CopyTableDescriptionsSize() > domainInfo->GetSchemeLimits().MaxConsistentCopyTargets) {
        auto msg = TStringBuilder() << "Targets count has reached maximum value"
                                    << ", limit for consistent copying in domain: " << domainInfo->GetSchemeLimits().MaxConsistentCopyTargets
                                    << ", intention to consistent copy: " << consistentCopying.CopyTableDescriptionsSize();
        return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, msg)};
    }

    {
        TString errStr;
        if (!context.SS->CheckApplyIf(tx, errStr)) {
            return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, errStr)};
        }
    }

    TVector<ISubOperationBase::TPtr> result;

    for (auto& descr: consistentCopying.GetCopyTableDescriptions()) {
        auto& srcStr = descr.GetSrcPath();
        auto& dstStr = descr.GetDstPath();

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

        result.push_back(CreateCopyTable(TOperationId(nextId.GetTxId(),
                                                      nextId.GetSubTxId() + result.size()),
                                         CopyTableTask(srcPath, dstPath, descr.GetOmitFollowers(), descr.GetIsBackup())));

        if (descr.GetOmitIndexes()) {
            continue;
        }

        for (const auto& child: srcPath.Base()->GetChildren()) {
            const auto& name = child.first;
            const auto& pathId = child.second;

            TPath srcIndexPath = srcPath.Child(name);
            TPath dstIndexPath = dstPath.Child(name);

            if (srcIndexPath.IsDeleted()) {
                continue;
            }

            if (!srcIndexPath.IsTableIndex()) {
                continue;
            }

            Y_VERIFY(srcIndexPath.Base()->PathId == pathId);
            Y_VERIFY_S(srcIndexPath.Base()->GetChildren().size() == 1, srcIndexPath.PathString() << " has children " << srcIndexPath.Base()->GetChildren().size() << " but 1 expected");

            TTableIndexInfo::TPtr indexInfo = context.SS->Indexes.at(pathId);
            result.push_back(CreateNewTableIndex(NextPartId(nextId, result), CreateIndexTask(indexInfo, dstIndexPath)));

            TString srcImplTableName = srcIndexPath.Base()->GetChildren().begin()->first;
            TPath srcImplTable = srcIndexPath.Child(srcImplTableName);
            Y_VERIFY(srcImplTable.Base()->PathId == srcIndexPath.Base()->GetChildren().begin()->second);
            TPath dstImplTable = dstIndexPath.Child(srcImplTableName);

            result.push_back(CreateCopyTable(NextPartId(nextId, result),
                CopyTableTask(srcImplTable, dstImplTable, descr.GetOmitFollowers(), descr.GetIsBackup())));
        }
    }

    return result;
}

}
}
