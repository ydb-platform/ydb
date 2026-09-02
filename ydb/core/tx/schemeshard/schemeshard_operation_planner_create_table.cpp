#include "schemeshard_operation_planner_impl.h"

#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/table_index.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>

namespace NKikimr::NSchemeShard {

// The auto-MkDir split of a relative Name. Mirrors what TOperation::SplitIntoTransactions does
// for CreateDirsFromName, including the cases where it silently leaves the request alone so
// that Propose reports the error it would have reported anyway.
//
// On success `create` is the rewritten transaction and `mkdirs` holds one MkDir per missing
// directory, outermost first.
bool TOperationPlanner::SplitCreateTable(const TTxTransaction& tx, TTxTransaction& create, TVector<TTxTransaction>& mkdirs) {
    create = tx;
    const TString& targetName = tx.GetCreateTable().GetName();

    if (!targetName || targetName.StartsWith('/') || targetName.EndsWith('/')) {
        return true;
    }

    const TPath parentPath = TPath::Resolve(tx.GetWorkingDir(), SS);
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
            return true;
        }
    }

    TPath path = TPath::Resolve(Join(tx.GetWorkingDir(), targetName), SS);
    bool exists = false;
    {
        TPath::TChecker checks = path.Check();
        checks.IsAtLocalSchemeShard();

        if (path.IsResolved()) {
            checks.IsResolved();
            exists = !path.IsDeleted();
        } else {
            checks
                .NotEmpty()
                .NotResolved();
        }

        if (!checks) {
            return FailAt(checks);
        }
    }

    const TString name = path.LeafName();
    path.Rise();

    create.SetWorkingDir(path.PathString());
    create.SetFailOnExist(tx.GetFailOnExist());
    create.MutableCreateTable()->SetName(name);

    if (exists) {
        return true;
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

        if (!checks) {
            return FailAt(checks);
        }

        const TString dirName = path.LeafName();
        path.Rise();

        TTxTransaction mkdir;
        mkdir.SetFailOnExist(true);
        mkdir.SetAllowCreateInTempDir(tx.GetAllowCreateInTempDir());
        mkdir.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMkDir);
        mkdir.SetWorkingDir(path.PathString());
        mkdir.MutableMkDir()->SetName(dirName);
        mkdirs.push_back(std::move(mkdir));
    }

    Reverse(mkdirs.begin(), mkdirs.end());
    return true;
}

bool TOperationPlanner::PlanCreateTable(ui32 requestIdx, const TTxTransaction& tx) {
    if (tx.GetWorkingDir().empty()) {
        return Fail(NKikimrScheme::StatusPathDoesNotExist, "WorkingDir is empty");
    }

    TTxTransaction create;
    TVector<TTxTransaction> mkdirs;
    if (!SplitCreateTable(tx, create, mkdirs)) {
        return false;
    }

    // --- generated directories: physical writes only ----------------------------------------
    THashMap<TString, TPhysicalWriteId> generatedDirs;
    for (const auto& mkdir : mkdirs) {
        const TString containerAbs = CanonizePath(mkdir.GetWorkingDir());
        const TString dirAbs = Join(containerAbs, mkdir.GetMkDir().GetName());

        TPhysicalWriteId container;
        if (auto it = generatedDirs.find(containerAbs); it != generatedDirs.end()) {
            container = it->second;
        } else {
            auto rel = Relative(containerAbs);
            if (!rel) {
                return false;
            }
            const TPath containerPath = TPath::Resolve(containerAbs, SS);
            container = Builder.AddPhysicalWrite(*rel, Leaf(containerAbs), PathIdOf(containerPath),
                EPlanObservation::MustWrite, EPhysicalWriteReason::GeneratedDirectoryContainer, std::nullopt);
        }

        auto dirRel = Relative(dirAbs);
        if (!dirRel) {
            return false;
        }
        const TPhysicalWriteId dir = Builder.AddPhysicalWrite(*dirRel, Leaf(dirAbs), std::nullopt,
            EPlanObservation::MustWrite, EPhysicalWriteReason::GeneratedDirectory, std::nullopt);
        generatedDirs[dirAbs] = dir;

        Builder.AddGeneratedDirPart(requestIdx, mkdir, TMkDirPartBindings{dir, container});
    }

    // --- the table and the directory that gains it -------------------------------------------
    const auto& copying = create.GetCreateTable();
    const TString containerAbs = CanonizePath(create.GetWorkingDir());
    const TString targetAbs = Join(containerAbs, copying.GetName());

    const TPath container = TPath::Resolve(containerAbs, SS);
    const TPath target = TPath::Resolve(targetAbs, SS);

    const auto targetEffect = AddWrittenEffect(targetAbs, copying.GetName(), PathIdOf(target),
        EPlanEffect::Create, EPlanRole::Target, EPlanOrigin::RequestNamed);
    if (!targetEffect) {
        return false;
    }
    // A container this operation generates will be a directory.
    const auto containerEffect = AddContainerEffect(container, generatedDirs.contains(containerAbs));
    if (!containerEffect) {
        return false;
    }

    if (!copying.HasCopyFromTable()) {
        Builder.AddPart(requestIdx, create, TCreateTablePartBindings{*targetEffect, *containerEffect});
        return true;
    }

    // --- copy: the source and what is dropped beneath it -------------------------------------
    const TString srcAbs = CanonizePath(copying.GetCopyFromTable());
    const TPath srcPath = TPath::Resolve(srcAbs, SS);
    {
        TPath::TChecker checks = srcPath.Check();
        checks.NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .NotUnderDeleting()
            .IsTable();

        if (checks && !srcPath.ShouldSkipCommonPathCheckForIndexImplTable()) {
            checks.IsCommonSensePath();
        }

        if (!copying.GetAllowUnderSameOperation()) {
            checks.NotUnderOperation();
        }

        if (!checks) {
            return FailAt(checks);
        }
    }

    // A copy never crosses databases. Checked here against the destination's domain so a
    // source in another subdomain is rejected before it is written into a plan whose root
    // cannot express it.
    {
        TPath domainProbe = container;
        if (!domainProbe.IsEmpty()) {
            domainProbe.RiseUntilExisted();
        }
        if (!domainProbe.IsEmpty() && domainProbe.IsResolved()
            && domainProbe.GetPathIdForDomain() != srcPath.GetPathIdForDomain())
        {
            return Fail(NKikimrScheme::StatusInvalidParameter, TStringBuilder()
                << "only paths to a single subdomain are allowed"
                << ", another path: " << srcPath.PathString());
        }
    }

    auto srcRel = Relative(srcAbs);
    if (!srcRel) {
        return false;
    }
    // A reference, not an alteration: the copy sets the source to Copying and restores it at
    // completion, so the row is written but the object is not logically changed.
    const TPlanEffectId sourceEffect = Builder.AddReference(*srcRel, srcPath.LeafName(), PathIdOf(srcPath),
        EPlanRole::Source, EPlanOrigin::RequestNamed);
    Builder.AddPhysicalWrite(*srcRel, srcPath.LeafName(), PathIdOf(srcPath),
        EPlanObservation::MustWrite, EPhysicalWriteReason::SourceStateFlip, sourceEffect);

    TVector<TPlanEffectId> dropStreams;
    for (const auto& streamName : copying.GetDropSrcCdcStream().GetStreamName()) {
        const TString streamAbs = Join(srcAbs, streamName);
        const auto drop = AddWrittenEffect(streamAbs, streamName, PathIdOf(TPath::Resolve(streamAbs, SS)),
            EPlanEffect::Drop, EPlanRole::Source, EPlanOrigin::RequestNamed);
        if (!drop) {
            return false;
        }
        dropStreams.push_back(*drop);
    }

    {
        auto schema = TransactionTemplate(create.GetWorkingDir(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
        schema.SetFailOnExist(create.GetFailOnExist());

        auto* operation = schema.MutableCreateTable();
        operation->SetName(copying.GetName());
        operation->SetCopyFromTable(copying.GetCopyFromTable());
        operation->SetOmitFollowers(copying.GetOmitFollowers());
        operation->SetIsBackup(copying.GetIsBackup());
        operation->MutablePartitionConfig()->CopyFrom(copying.GetPartitionConfig());
        if (create.HasCreateCdcStream()) {
            schema.MutableCreateCdcStream()->CopyFrom(create.GetCreateCdcStream());
        }
        if (copying.HasDropSrcCdcStream()) {
            operation->MutableDropSrcCdcStream()->CopyFrom(copying.GetDropSrcCdcStream());
        }

        Builder.AddPart(requestIdx, std::move(schema),
            TCopyTablePartBindings{*targetEffect, *containerEffect, sourceEffect, std::move(dropStreams)});
    }

    // --- copy: indexes, their impl tables, and sequences -------------------------------------
    // Same order as the former vector overload of CreateCopyTable, so TxPartId assignment is
    // unchanged: per ready index its index part, then per impl table its copy part and that
    // impl table's sequences; finally the main table's sequences.
    for (const auto& [name, pathId] : srcPath.Base()->GetChildren()) {
        TPath childPath = srcPath.Child(name);
        if (childPath.IsDeleted() || childPath.IsSequence() || !childPath.IsTableIndex()) {
            continue;
        }
        Y_ABORT_UNLESS(childPath.Base()->PathId == pathId);

        TTableIndexInfo::TPtr indexInfo = SS->Indexes.at(pathId);
        if (indexInfo->State != NKikimrSchemeOp::EIndexState::EIndexStateReady) {
            continue;
        }

        const TString indexAbs = Join(targetAbs, name);
        auto srcIndexRel = Relative(childPath.PathString());
        if (!srcIndexRel) {
            return false;
        }

        auto schema = TransactionTemplate(targetAbs, NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex);
        schema.SetFailOnExist(create.GetFailOnExist());
        if (!FillIndexDescription(*schema.MutableCreateTableIndex(), name, *indexInfo)) {
            return false;
        }

        const auto indexEffect = AddWrittenEffect(indexAbs, name, std::nullopt,
            EPlanEffect::Create, EPlanRole::Target, EPlanOrigin::RequestImplied);
        if (!indexEffect) {
            return false;
        }
        const TPlanEffectId srcIndexEffect = Builder.AddReference(*srcIndexRel, name, PathIdOf(childPath),
            EPlanRole::Source, EPlanOrigin::RequestImplied);

        Builder.AddPart(requestIdx, std::move(schema),
            TCreateIndexPartBindings{*indexEffect, *targetEffect, srcIndexEffect});

        if (TTableIndexInfo::IsLocalIndex(indexInfo->Type)) {
            continue; // local indexes have no impl tables
        }

        // Impl table copies are skipped under OmitIndexes; CreateConsistentCopyTables handles
        // them for incremental backups with CDC.
        if (copying.GetOmitIndexes()) {
            continue;
        }

        for (const auto& [implTableName, implTablePathId] : childPath.Base()->GetChildren()) {
            TPath implTable = childPath.Child(implTableName);
            if (implTable.IsDeleted()) {
                continue;
            }
            Y_ABORT_UNLESS(implTable.Base()->PathId == implTablePathId);

            const TString implAbs = Join(indexAbs, implTableName);
            auto srcImplRel = Relative(implTable.PathString());
            if (!srcImplRel) {
                return false;
            }

            NKikimrSchemeOp::TModifyScheme implSchema;
            implSchema.SetFailOnExist(create.GetFailOnExist());
            implSchema.SetWorkingDir(indexAbs);
            implSchema.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
            auto* implOperation = implSchema.MutableCreateTable();
            implOperation->SetName(implTableName);
            implOperation->SetCopyFromTable(implTable.PathString());
            implOperation->SetOmitFollowers(copying.GetOmitFollowers());
            implOperation->SetIsBackup(copying.GetIsBackup());

            const auto implEffect = AddWrittenEffect(implAbs, implTableName, std::nullopt,
                EPlanEffect::Create, EPlanRole::Target, EPlanOrigin::PartDerived);
            if (!implEffect) {
                return false;
            }
            const TPlanEffectId srcImplEffect = Builder.AddReference(*srcImplRel, implTableName, PathIdOf(implTable),
                EPlanRole::Source, EPlanOrigin::PartDerived);
            Builder.AddPhysicalWrite(*srcImplRel, implTableName, PathIdOf(implTable),
                EPlanObservation::MustWrite, EPhysicalWriteReason::SourceStateFlip, srcImplEffect);

            Builder.AddPart(requestIdx, std::move(implSchema),
                TCopyTablePartBindings{*implEffect, *indexEffect, srcImplEffect, {}});

            if (!PlanCopySequences(requestIdx, create, implTable, implAbs, *implEffect, EPlanOrigin::PartDerived)) {
                return false;
            }
        }
    }

    return PlanCopySequences(requestIdx, create, srcPath, targetAbs, *targetEffect, EPlanOrigin::RequestImplied);
}

bool TOperationPlanner::PlanCopySequences(ui32 requestIdx, const TTxTransaction& create, const TPath& srcTable,
        const TString& dstAbs, TPlanEffectId containerEffect, EPlanOrigin origin)
{
    for (const auto& [subName, subPathId] : srcTable.Base()->GetChildren()) {
        TPath subPath = srcTable.Child(subName);
        if (!subPath.IsSequence() || subPath.IsDeleted()) {
            continue;
        }

        TSequenceInfo::TPtr sequenceInfo = SS->Sequences.at(subPathId);
        const auto& sequenceDesc = sequenceInfo->Description;

        const TString seqAbs = Join(dstAbs, subName);
        auto srcSeqRel = Relative(subPath.PathString());
        if (!srcSeqRel) {
            return false;
        }

        auto scheme = TransactionTemplate(dstAbs, NKikimrSchemeOp::EOperationType::ESchemeOpCreateSequence);
        scheme.SetFailOnExist(create.GetFailOnExist());
        scheme.MutableCopySequence()->SetCopyFrom(subPath.PathString());
        *scheme.MutableSequence() = sequenceDesc;

        const auto seqEffect = AddWrittenEffect(seqAbs, subName, std::nullopt,
            EPlanEffect::Create, EPlanRole::Target, origin);
        if (!seqEffect) {
            return false;
        }
        const TPlanEffectId srcSeqEffect = Builder.AddReference(*srcSeqRel, subName, PathIdOf(subPath),
            EPlanRole::Source, origin);

        Builder.AddPart(requestIdx, std::move(scheme),
            TCopySequencePartBindings{*seqEffect, containerEffect, srcSeqEffect});
    }
    return true;
}

bool TOperationPlanner::FillIndexDescription(NKikimrSchemeOp::TIndexCreationConfig& operation, const TString& name,
        const TTableIndexInfo& indexInfo)
{
    operation.SetName(name);
    operation.SetType(indexInfo.Type);
    operation.SetState(indexInfo.State);
    for (const auto& keyName : indexInfo.IndexKeys) {
        *operation.MutableKeyColumnNames()->Add() = keyName;
    }
    for (const auto& dataColumn : indexInfo.IndexDataColumns) {
        *operation.MutableDataColumnNames()->Add() = dataColumn;
    }

    switch (indexInfo.Type) {
        case NKikimrSchemeOp::EIndexTypeGlobal:
        case NKikimrSchemeOp::EIndexTypeGlobalAsync:
        case NKikimrSchemeOp::EIndexTypeGlobalUnique:
        case NKikimrSchemeOp::EIndexTypeLocalMinMax:
        case NKikimrSchemeOp::EIndexTypeLocalCountMinSketch:
            // no specialized index description
            Y_ASSERT(std::holds_alternative<std::monostate>(indexInfo.SpecializedIndexDescription));
            break;
        case NKikimrSchemeOp::EIndexTypeGlobalJson:
        case NKikimrSchemeOp::EIndexTypeGlobalJsonCompact:
            // JSON indexes carry a fulltext description only in rowid mode (__ydb_row_id as doc_id).
            if (const auto* ft = std::get_if<NKikimrSchemeOp::TFulltextIndexDescription>(&indexInfo.SpecializedIndexDescription)) {
                *operation.MutableFulltextIndexDescription() = *ft;
            } else {
                Y_ASSERT(std::holds_alternative<std::monostate>(indexInfo.SpecializedIndexDescription));
            }
            break;
        case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree:
            *operation.MutableVectorIndexKmeansTreeDescription() =
                std::get<NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>(indexInfo.SpecializedIndexDescription);
            break;
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain:
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance:
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompact:
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompactRelevance:
            *operation.MutableFulltextIndexDescription() =
                std::get<NKikimrSchemeOp::TFulltextIndexDescription>(indexInfo.SpecializedIndexDescription);
            break;
        case NKikimrSchemeOp::EIndexTypeLocalBloomFilter:
            *operation.MutableBloomFilterDescription() =
                std::get<NKikimrSchemeOp::TBloomFilter>(indexInfo.SpecializedIndexDescription);
            break;
        case NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter:
            *operation.MutableBloomNGrammFilterDescription() =
                std::get<NKikimrSchemeOp::TBloomNGrammFilter>(indexInfo.SpecializedIndexDescription);
            break;
        default:
            return Fail(NKikimrScheme::EStatus::StatusInvalidParameter, NTableIndex::InvalidIndexType(indexInfo.Type));
    }
    return true;
}

} // namespace NKikimr::NSchemeShard
