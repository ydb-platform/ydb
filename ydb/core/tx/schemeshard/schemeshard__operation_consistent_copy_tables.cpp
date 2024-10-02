#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"
#include "schemeshard_path_element.h"

#include <ydb/core/base/path.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/algorithm.h>

NKikimrSchemeOp::TModifyScheme CopyTableTask(
    NKikimr::NSchemeShard::TPath& src,
    const TString& dstWorkingDir,
    const TString& dstLeaf,
    bool omitFollowers,
    bool isBackup,
    const NKikimrSchemeOp::TCopyTableConfig& descr)
{
    using namespace NKikimr::NSchemeShard;

    auto scheme = TransactionTemplate(dstWorkingDir, NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
    scheme.SetFailOnExist(true);

    auto operation = scheme.MutableCreateTable();
    operation->SetName(dstLeaf);
    operation->SetCopyFromTable(src.PathString());
    operation->SetOmitFollowers(omitFollowers);
    operation->SetIsBackup(isBackup);
    if (descr.HasCreateCdcStream()) {
        operation->MutableCreateCdcStream()->CopyFrom(descr.GetCreateCdcStream());
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
    for (const auto& keyName: indexInfo->IndexKeys) {
        *operation->MutableKeyColumnNames()->Add() = keyName;
    }

    for (const auto& dataColumn: indexInfo->IndexDataColumns) {
        *operation->MutableDataColumnNames()->Add() = dataColumn;
    }

    return scheme;
}

namespace NKikimr::NSchemeShard {

void CreateConsistentCopyTables(
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
        return;
    }

    TPath firstPath = TPath::Resolve(op.GetCopyTableDescriptions(0).GetSrcPath(), context.SS);
    // {
    //     auto checks = TPath::TChecker(firstPath);
    //     checks
    //         .NotEmpty()
    //         .NotUnderDomainUpgrade()
    //         .IsAtLocalSchemeShard();

    //     if (!checks) {
    //         return {CreateReject(nextId, checks.GetStatus(), checks.GetError())};
    //     }
    // }

    // const auto allForBackup = AllOf(op.GetCopyTableDescriptions(), [](const auto& item) {
    //     return item.GetIsBackup();
    // });

    // const auto& limits = firstPath.DomainInfo()->GetSchemeLimits();
    // const auto limit = allForBackup
    //     ? Max(limits.MaxObjectsInBackup, limits.MaxConsistentCopyTargets)
    //     : limits.MaxConsistentCopyTargets;

    // if (op.CopyTableDescriptionsSize() > limit) {
    //     return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, TStringBuilder()
    //         << "Consistent copy object count limit exceeded"
    //             << ", limit: " << limit
    //             << ", objects: " << op.CopyTableDescriptionsSize()
    //     )};
    // }

    TString errStr;
    if (!context.SS->CheckApplyIf(tx, errStr)) {
        result = {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, errStr)};
        return;
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
                Y_ABORT("%s, %s", srcPath.PathString().c_str(), checks.GetError().c_str());
                result = {CreateReject(nextId, checks.GetStatus(), checks.GetError())};
                return;
            }
        }

        if (op.HasDstBasePath()) {
            TVector<TTxTransaction> mkdirs;

            auto dstBaseStr = op.GetDstBasePath();
            TPath path = TPath::Resolve(JoinPath({dstBaseStr, dstStr}), context.SS).Parent();

            const TPath parentPath = TPath::Resolve(dstBaseStr, context.SS);
            {
                TPath::TChecker checks = parentPath.Check();
                checks
                    .NotUnderDomainUpgrade()
                    .IsAtLocalSchemeShard()
                    .IsResolved()
                    .NotDeleted()
                    .NotUnderDeleting()
                    .IsCommonSensePath()
                    .IsLikeDirectory();

                if (!checks) {
                    Y_ABORT("%s", checks.GetError().c_str());
                    // result.Transactions.push_back(tx);
                    // return result;
                }
            }

            while (path != parentPath) {
                TPath::TChecker checks = path.Check();
                checks
                    .NotUnderDomainUpgrade()
                    .IsAtLocalSchemeShard();

                if (path.IsResolved()) {
                    checks.IsResolved();

                    if (path.IsDeleted()) {
                        checks.IsDeleted();
                    } else {
                        checks
                            .NotDeleted()
                            .NotUnderDeleting()
                            .IsCommonSensePath()
                            .IsLikeDirectory();

                        if (checks) {
                            break;
                        }
                    }
                } else {
                    checks
                        .NotEmpty()
                        .NotResolved();
                }

                if (checks) {
                    checks.IsValidLeafName();
                }

                if (!checks) {
                    Y_ABORT("%s", checks.GetError().c_str());
                    // result.Status = checks.GetStatus();
                    // result.Reason = checks.GetError();
                    // mkdirs.clear();
                    // mkdirs.push_back(tx);
                    // return result;
                }

                const TString name = path.LeafName();
                path.Rise();

                TTxTransaction mkdir;
                mkdir.SetFailOnExist(false);
                mkdir.SetAllowCreateInTempDir(tx.GetAllowCreateInTempDir());
                mkdir.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMkDir);
                mkdir.SetWorkingDir(path.PathString());
                mkdir.MutableMkDir()->SetName(name);
                mkdirs.push_back(mkdir);
                Cerr << " @ add @ " << name << " to " << path.PathString() << Endl;
            }

            for (auto it = mkdirs.rbegin(); it != mkdirs.rend(); ++it) {
                result.push_back(CreateMkDir(NextPartId(nextId, result), std::move(*it)));
            }
        }

        Cerr << "<----- CopyTables" << Endl;
        TPath dstPath = op.HasDstBasePath() ? TPath::Resolve(JoinPath({op.GetDstBasePath(), dstStr}), context.SS) : TPath::Resolve(dstStr, context.SS);

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
            CopyTableTask(srcPath,
                          // op.HasDstBasePath() ? op.GetDstBasePath() : dstPath.Parent().PathString(),
                          // op.HasDstBasePath() ? dstStr : dstPath.LeafName(),
                          dstPath.Parent().PathString(),
                          dstPath.LeafName(),
                          descr.GetOmitFollowers(),
                          descr.GetIsBackup(),
                          descr),
                                         sequences));

        // TVector<NKikimrSchemeOp::TSequenceDescription> sequenceDescriptions;
        // for (const auto& child: srcPath.Base()->GetChildren()) {
        //     const auto& name = child.first;
        //     const auto& pathId = child.second;

        //     TPath srcIndexPath = srcPath.Child(name);
        //     TPath dstIndexPath = dstPath.Child(name);

        //     if (srcIndexPath.IsDeleted()) {
        //         continue;
        //     }

        //     if (srcIndexPath.IsSequence()) {
        //         TSequenceInfo::TPtr sequenceInfo = context.SS->Sequences.at(pathId);
        //         const auto& sequenceDesc = sequenceInfo->Description;
        //         sequenceDescriptions.push_back(sequenceDesc);
        //         continue;
        //     }

        //     if (descr.GetOmitIndexes()) {
        //         continue;
        //     }

        //     if (!srcIndexPath.IsTableIndex()) {
        //         continue;
        //     }

        //     Y_ABORT_UNLESS(srcIndexPath.Base()->PathId == pathId);
        //     Y_VERIFY_S(srcIndexPath.Base()->GetChildren().size() == 1, srcIndexPath.PathString() << " has children " << srcIndexPath.Base()->GetChildren().size() << " but 1 expected");

        //     TTableIndexInfo::TPtr indexInfo = context.SS->Indexes.at(pathId);
        //     result.push_back(CreateNewTableIndex(NextPartId(nextId, result), CreateIndexTask(indexInfo, dstIndexPath)));

        //     TString srcImplTableName = srcIndexPath.Base()->GetChildren().begin()->first;
        //     TPath srcImplTable = srcIndexPath.Child(srcImplTableName);
        //     Y_ABORT_UNLESS(srcImplTable.Base()->PathId == srcIndexPath.Base()->GetChildren().begin()->second);
        //     TPath dstImplTable = dstIndexPath.Child(srcImplTableName);

        //     result.push_back(CreateCopyTable(NextPartId(nextId, result),
        //         CopyTableTask(
        //             srcImplTable,
        //             dstImplTable.Parent().PathString(),
        //             dstImplTable.LeafName(),
        //             descr.GetOmitFollowers(),
        //             descr.GetIsBackup())));
        // }

        // for (auto&& sequenceDescription : sequenceDescriptions) {
        //     auto scheme = TransactionTemplate(
        //         dstPath.PathString(),
        //         NKikimrSchemeOp::EOperationType::ESchemeOpCreateSequence);
        //     scheme.SetFailOnExist(true);

        //     auto* copySequence = scheme.MutableCopySequence();
        //     copySequence->SetCopyFrom(srcPath.PathString() + "/" + sequenceDescription.GetName());
        //     *scheme.MutableSequence() = std::move(sequenceDescription);

        //     result.push_back(CreateCopySequence(NextPartId(nextId, result), scheme));
        // }
    }
}

TVector<ISubOperation::TPtr> CreateConsistentCopyTables(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    TVector<ISubOperation::TPtr> result;

    CreateConsistentCopyTables(nextId, tx, context, result);

    return result;
}

}
