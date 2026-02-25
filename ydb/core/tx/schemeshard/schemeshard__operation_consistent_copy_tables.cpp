#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"

#include <ydb/core/base/path.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <util/generic/algorithm.h>

static bool ShouldOmitAutomaticIndexProcessing(const NKikimrSchemeOp::TCopyTableConfig& descr) {
    if (descr.GetOmitIndexes()) {
        return true;  // User explicitly wants to skip indexes
    }

    if (!descr.GetIndexImplTableCdcStreams().empty()) {
        return true;  // Incremental backup - manual handling required
    }

    return false;  // Regular copy - let CreateCopyTable handle indexes automatically
}

static NKikimrSchemeOp::TModifyScheme CopyAnyTableTask(NKikimr::NSchemeShard::TPath& src, NKikimr::NSchemeShard::TPath& dst, const NKikimrSchemeOp::TCopyTableConfig& descr) {
    using namespace NKikimr::NSchemeShard;

    auto scheme = TransactionTemplate(dst.Parent().PathString(), src->IsTable() ? NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable : NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnTable);
    scheme.SetFailOnExist(true);

    if (src->IsTable()) {
        auto operation = scheme.MutableCreateTable();
        operation->SetName(dst.LeafName());
        operation->SetCopyFromTable(src.PathString());
        operation->SetOmitFollowers(descr.GetOmitFollowers());
        operation->SetIsBackup(descr.GetIsBackup());
        operation->SetAllowUnderSameOperation(descr.GetAllowUnderSameOperation());
        operation->SetOmitIndexes(ShouldOmitAutomaticIndexProcessing(descr));
        if (descr.HasCreateSrcCdcStream()) {
            auto* coOp = scheme.MutableCreateCdcStream();
            coOp->CopyFrom(descr.GetCreateSrcCdcStream());
        }
        if (descr.HasDropSrcCdcStream()) {
            operation->MutableDropSrcCdcStream()->CopyFrom(descr.GetDropSrcCdcStream());
        }
        if (descr.HasTargetPathTargetState()) {
            operation->SetPathState(descr.GetTargetPathTargetState());
        }
    } else {
        auto operation = scheme.MutableCreateColumnTable();
        operation->SetName(dst.LeafName());
        operation->SetCopyFromTable(src.PathString());
        operation->SetIsBackup(descr.GetIsBackup());
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
    operation->SetState(indexInfo->State);

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
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain:
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance:
            *operation->MutableFulltextIndexDescription() =
                std::get<NKikimrSchemeOp::TFulltextIndexDescription>(indexInfo->SpecializedIndexDescription);
            break;
        default:
            return {}; // reject
    }

    return scheme;
}

static NKikimr::NSchemeShard::ISubOperation::TPtr CreateCopyAnyTable(
        const NKikimr::NSchemeShard::TPath& srcPath, NKikimr::NSchemeShard::TOperationId id, const NKikimr::NSchemeShard::TTxTransaction& tx,
        const THashSet<TString>& localSequences = { }, TMaybe<NKikimr::NSchemeShard::TPathElement::EPathState> targetState = {}) {
    if (srcPath->IsTable()) {
        return NKikimr::NSchemeShard::CreateCopyTable(id, tx, localSequences, targetState);
    }
    return NKikimr::NSchemeShard::CreateReadOnlyCopyColumnTable(id, tx);
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
                  .Or(&TPath::TChecker::IsColumnTable, &TPath::TChecker::IsTable);

            // Allow copying index impl tables when feature flag is enabled
            if (!srcPath.ShouldSkipCommonPathCheckForIndexImplTable()) {
                checks.IsCommonSensePath();
            }

            checks.IsTheSameDomain(firstPath);

            if (!checks) {
                result = {CreateReject(nextId, checks.GetStatus(), checks.GetError())};
                return false;
            }
        }

        TPath dstPath = TPath::Resolve(dstStr, context.SS);
        TPath dstParentPath = dstPath.Parent();

        THashSet<TString> sequences = GetLocalSequences(context, srcPath);

        if (descr.HasTargetPathTargetState()) {
            result.push_back(CreateCopyAnyTable(
                                srcPath,
                                NextPartId(nextId, result),
                                CopyAnyTableTask(srcPath, dstPath, descr),
                                sequences,
                                descr.GetTargetPathTargetState()));
        } else {
            result.push_back(CreateCopyAnyTable(
                                srcPath,
                                NextPartId(nextId, result),
                                CopyAnyTableTask(srcPath, dstPath, descr),
                                sequences));
        }

        // Log information about the table being copied
        LOG_TRACE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "CreateConsistentCopyTables: Processing table"
            << ", srcPath: " << srcPath.PathString()
            << ", dstPath: " << dstPath.PathString()
            << ", pathId: " << srcPath.Base()->PathId
            << ", childrenCount: " << srcPath.Base()->GetChildren().size()
            << ", omitIndexes: " << descr.GetOmitIndexes());

        // Log table info if available
        if (context.SS->Tables.contains(srcPath.Base()->PathId)) {
            TTableInfo::TPtr tableInfo = context.SS->Tables.at(srcPath.Base()->PathId);
            const auto& tableDesc = tableInfo->TableDescription;
            LOG_TRACE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "CreateConsistentCopyTables: Table info"
                << ", tableIndexesSize: " << tableDesc.TableIndexesSize()
                << ", isBackup: " << tableInfo->IsBackup);

            for (size_t i = 0; i < static_cast<size_t>(tableDesc.TableIndexesSize()); ++i) {
                const auto& indexDesc = tableDesc.GetTableIndexes(i);
                LOG_TRACE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "CreateConsistentCopyTables: Table has index in description"
                    << ", indexName: " << indexDesc.GetName()
                    << ", indexType: " << NKikimrSchemeOp::EIndexType_Name(indexDesc.GetType()));
            }
        }
        
        // Log column table info if available
        if (context.SS->ColumnTables.contains(srcPath.Base()->PathId)) {
            TColumnTableInfo::TPtr tableInfo = context.SS->ColumnTables.at(srcPath.Base()->PathId).GetPtr();
            const auto& tableDesc = tableInfo->Description;
            LOG_TRACE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "CreateConsistentCopyTables: Column Table info"
                << ", isBackup: " << tableDesc.GetIsBackup());
        }

        // Log all children
        for (const auto& child: srcPath.Base()->GetChildren()) {
            const auto& name = child.first;
            const auto& pathId = child.second;
            TPath childPath = srcPath.Child(name);

            LOG_TRACE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "CreateConsistentCopyTables: Child found"
                << ", name: " << name
                << ", pathId: " << pathId
                << ", isResolved: " << childPath.IsResolved()
                << ", isDeleted: " << childPath.IsDeleted()
                << ", isSequence: " << childPath.IsSequence()
                << ", isTableIndex: " << childPath.IsTableIndex());
        }

        for (const auto& child: srcPath.Base()->GetChildren()) {
            const auto& name = child.first;
            const auto& pathId = child.second;

            TPath srcIndexPath = srcPath.Child(name);
            TPath dstIndexPath = dstPath.Child(name);

            if (srcIndexPath.IsDeleted()) {
                LOG_TRACE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "CreateConsistentCopyTables: Skipping deleted child: " << name);
                continue;
            }

            if (srcIndexPath.IsSequence()) {
                LOG_TRACE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "CreateConsistentCopyTables: Skipping sequence child: " << name);
                continue;
            }

            if (descr.GetOmitIndexes()) {
                LOG_TRACE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "CreateConsistentCopyTables: Skipping due to OmitIndexes: " << name);
                continue;
            }

            if (!srcIndexPath.IsTableIndex()) {
                LOG_TRACE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "CreateConsistentCopyTables: Skipping non-index child: " << name);
                continue;
            }

            LOG_TRACE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "CreateConsistentCopyTables: Creating index copy operation for: " << name);

            Y_ABORT_UNLESS(srcIndexPath.Base()->PathId == pathId);
            TTableIndexInfo::TPtr indexInfo = context.SS->Indexes.at(pathId);
            auto scheme = CreateIndexTask(indexInfo, dstIndexPath);
            if (!scheme) {
                result = {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter,
                                       TStringBuilder{} << "Consistent copy table doesn't support table with index type " << indexInfo->Type)};
                return false;
            }
            scheme->SetInternal(tx.GetInternal());
            result.push_back(CreateNewTableIndex(NextPartId(nextId, result), *scheme));

            for (const auto& [srcImplTableName, srcImplTablePathId] : srcIndexPath.Base()->GetChildren()) {
                TPath srcImplTable = srcIndexPath.Child(srcImplTableName);
                Y_ABORT_UNLESS(srcImplTable.Base()->PathId == srcImplTablePathId);
                TPath dstImplTable = dstIndexPath.Child(srcImplTableName);

                LOG_TRACE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "CreateConsistentCopyTables: Creating index impl table copy"
                    << ", srcImplTable: " << srcImplTable.PathString()
                    << ", dstImplTable: " << dstImplTable.PathString());

                NKikimrSchemeOp::TCopyTableConfig indexDescr;

                indexDescr.SetOmitFollowers(descr.GetOmitFollowers());
                indexDescr.SetIsBackup(descr.GetIsBackup());
                indexDescr.SetAllowUnderSameOperation(descr.GetAllowUnderSameOperation());

                if (descr.HasTargetPathTargetState()) {
                    indexDescr.SetTargetPathTargetState(descr.GetTargetPathTargetState());
                }

                indexDescr.SetOmitIndexes(true);

                auto itCreate = descr.GetIndexImplTableCdcStreams().find(name);
                if (itCreate != descr.GetIndexImplTableCdcStreams().end()) {
                    indexDescr.MutableCreateSrcCdcStream()->CopyFrom(itCreate->second);
                }

                auto itDrop = descr.GetIndexImplTableDropCdcStreams().find(name);
                if (itDrop != descr.GetIndexImplTableDropCdcStreams().end()) {
                    indexDescr.MutableDropSrcCdcStream()->CopyFrom(itDrop->second);
                }

                result.push_back(CreateCopyAnyTable(srcImplTable, NextPartId(nextId, result),
                    CopyAnyTableTask(srcImplTable, dstImplTable, indexDescr), GetLocalSequences(context, srcImplTable)));
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
