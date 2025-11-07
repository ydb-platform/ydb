#include "schemeshard__backup_collection_common.h"
#include "schemeshard__op_traits.h"
#include "schemeshard__operation_common.h"
#include "schemeshard__operation_create_cdc_stream.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_utils.h"
#include "schemeshard_impl.h"


namespace NKikimr::NSchemeShard {

using TTag = TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpBackupBackupCollection>;

namespace NOperation {

template <>
std::optional<THashMap<TString, THashSet<TString>>> GetRequiredPaths<TTag>(
    TTag,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    const auto& backupOp = tx.GetBackupBackupCollection();
    return NBackup::GetBackupRequiredPaths(tx, backupOp.GetTargetDir(), backupOp.GetName(), context);
}

template <>
bool Rewrite(TTag, TTxTransaction& tx) {
    auto now = NBackup::ToX509String(TlsActivationContext->AsActorContext().Now());
    tx.MutableBackupBackupCollection()->SetTargetDir(now + "_full");
    return true;
}

} // namespace NOperation

TVector<ISubOperation::TPtr> CreateBackupBackupCollection(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    TVector<ISubOperation::TPtr> result;

    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables);
    modifyScheme.SetInternal(true);

    auto& cct = *modifyScheme.MutableCreateConsistentCopyTables();
    auto& copyTables = *cct.MutableCopyTableDescriptions();
    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);

    TString bcPathStr = JoinPath({tx.GetWorkingDir(), tx.GetBackupBackupCollection().GetName()});

    const TPath& bcPath = TPath::Resolve(bcPathStr, context.SS);
    {
        auto checks = bcPath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotUnderDeleting()
            .NotUnderOperation()
            .IsBackupCollection();

        if (!checks) {
            result = {CreateReject(opId, checks.GetStatus(), checks.GetError())};
            return result;
        }
    }

    Y_ABORT_UNLESS(context.SS->BackupCollections.contains(bcPath->PathId));
    const auto& bc = context.SS->BackupCollections[bcPath->PathId];
    bool incrBackupEnabled = bc->Description.HasIncrementalBackupConfig();
    
    bool omitIndexes = bc->Description.GetOmitIndexes() || 
                       (incrBackupEnabled && bc->Description.GetIncrementalBackupConfig().GetOmitIndexes());
    
    TString streamName = NBackup::ToX509String(TlsActivationContext->AsActorContext().Now()) + "_continuousBackupImpl";

    for (const auto& item : bc->Description.GetExplicitEntryList().GetEntries()) {
        auto& desc = *copyTables.Add();
        desc.SetSrcPath(item.GetPath());
        std::pair<TString, TString> paths;
        TString err;
        if (!TrySplitPathByDb(item.GetPath(), bcPath.GetDomainPathString(), paths, err)) {
            result = {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, err)};
            return {};
        }
        auto& relativeItemPath = paths.second;
        desc.SetDstPath(JoinPath({tx.GetWorkingDir(), tx.GetBackupBackupCollection().GetName(), tx.GetBackupBackupCollection().GetTargetDir(), relativeItemPath}));
        
        // For incremental backups, always omit indexes from table copy (backed up separately via CDC)
        // For full backups, respect the OmitIndexes configuration
        if (incrBackupEnabled) {
            desc.SetOmitIndexes(true);
        } else {
            desc.SetOmitIndexes(omitIndexes);
        }
        
        desc.SetOmitFollowers(true);
        desc.SetAllowUnderSameOperation(true);

        if (incrBackupEnabled) {
            NKikimrSchemeOp::TCreateCdcStream createCdcStreamOp;
            createCdcStreamOp.SetTableName(item.GetPath());
            auto& streamDescription = *createCdcStreamOp.MutableStreamDescription();
            streamDescription.SetName(streamName);
            streamDescription.SetMode(NKikimrSchemeOp::ECdcStreamModeUpdate);
            streamDescription.SetFormat(NKikimrSchemeOp::ECdcStreamFormatProto);

            const auto sPath = TPath::Resolve(item.GetPath(), context.SS);
            
            {
                auto checks = sPath.Check();
                checks
                    .IsResolved()
                    .NotDeleted()
                    .IsTable();
                
                if (!checks) {
                    result = {CreateReject(opId, checks.GetStatus(), checks.GetError())};
                    return result;
                }
            }
            
            NCdc::DoCreateStreamImpl(result, createCdcStreamOp, opId, sPath, false, false);

            desc.MutableCreateSrcCdcStream()->CopyFrom(createCdcStreamOp);
        }
    }

    if (!CreateConsistentCopyTables(opId, modifyScheme, context, result)) {
        return result;
    }

    if (incrBackupEnabled) {
        for (const auto& item : bc->Description.GetExplicitEntryList().GetEntries()) {
            NKikimrSchemeOp::TCreateCdcStream createCdcStreamOp;
            createCdcStreamOp.SetTableName(item.GetPath());
            auto& streamDescription = *createCdcStreamOp.MutableStreamDescription();
            streamDescription.SetName(streamName);
            streamDescription.SetMode(NKikimrSchemeOp::ECdcStreamModeUpdate);
            streamDescription.SetFormat(NKikimrSchemeOp::ECdcStreamFormatProto);

            const auto sPath = TPath::Resolve(item.GetPath(), context.SS);
            
            {
                auto checks = sPath.Check();
                checks
                    .IsResolved()
                    .NotDeleted()
                    .IsTable();
                
                if (!checks) {
                    result = {CreateReject(opId, checks.GetStatus(), checks.GetError())};
                    return result;
                }
            }
            
            auto table = context.SS->Tables.at(sPath.Base()->PathId);

            TVector<TString> boundaries;
            const auto& partitions = table->GetPartitions();
            boundaries.reserve(partitions.size() - 1);

            for (ui32 i = 0; i < partitions.size(); ++i) {
                const auto& partition = partitions.at(i);
                if (i != partitions.size() - 1) {
                    boundaries.push_back(partition.EndOfRange);
                }
            }

            const auto streamPath = sPath.Child(streamName);

            NCdc::DoCreatePqPart(result, createCdcStreamOp, opId, streamPath, streamName, table, boundaries, false);
        }

        if (!omitIndexes) {
            for (const auto& item : bc->Description.GetExplicitEntryList().GetEntries()) {
                const auto tablePath = TPath::Resolve(item.GetPath(), context.SS);
                
                // Iterate through table's children to find indexes
                for (const auto& [childName, childPathId] : tablePath.Base()->GetChildren()) {
                    auto childPath = context.SS->PathsById.at(childPathId);
                    
                    // Skip non-index children (CDC streams, etc.)
                    if (childPath->PathType != NKikimrSchemeOp::EPathTypeTableIndex) {
                        continue;
                    }
                    
                    // Skip deleted indexes
                    if (childPath->Dropped()) {
                        continue;
                    }
                    
                    // Get index info and filter for global sync only
                    // We need more complex logic for vector indexes in future
                    auto indexInfo = context.SS->Indexes.at(childPathId);
                    if (indexInfo->Type != NKikimrSchemeOp::EIndexTypeGlobal) {
                        continue;
                    }
                
                    // Get index implementation table (the only child of index)
                    auto indexPath = TPath::Init(childPathId, context.SS);
                    Y_ABORT_UNLESS(indexPath.Base()->GetChildren().size() == 1);
                    auto [implTableName, implTablePathId] = *indexPath.Base()->GetChildren().begin();
                    
                    auto indexTablePath = indexPath.Child(implTableName);
                    auto indexTable = context.SS->Tables.at(implTablePathId);
                    
                    // Create CDC stream on index impl table
                    NKikimrSchemeOp::TCreateCdcStream createCdcStreamOp;
                    createCdcStreamOp.SetTableName(implTableName);
                    auto& streamDescription = *createCdcStreamOp.MutableStreamDescription();
                    streamDescription.SetName(streamName);
                    streamDescription.SetMode(NKikimrSchemeOp::ECdcStreamModeUpdate);
                    streamDescription.SetFormat(NKikimrSchemeOp::ECdcStreamFormatProto);
                    
                    NCdc::DoCreateStreamImpl(result, createCdcStreamOp, opId, indexTablePath, false, false);
                    
                    // Create AtTable operation to notify datashard (without schema change)
                    {
                        auto outTx = TransactionTemplate(indexPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStreamAtTable);
                        auto& cdcOp = *outTx.MutableCreateCdcStream();
                        cdcOp.CopyFrom(createCdcStreamOp);
                        result.push_back(CreateNewCdcStreamAtTable(NextPartId(opId, result), outTx, false));
                    }
                    
                    // Create PQ part for index CDC stream
                    TVector<TString> boundaries;
                    const auto& partitions = indexTable->GetPartitions();
                    boundaries.reserve(partitions.size() - 1);
                    for (ui32 i = 0; i < partitions.size(); ++i) {
                        const auto& partition = partitions.at(i);
                        if (i != partitions.size() - 1) {
                            boundaries.push_back(partition.EndOfRange);
                        }
                    }
                    
                    const auto streamPath = indexTablePath.Child(streamName);
                    NCdc::DoCreatePqPart(result, createCdcStreamOp, opId, streamPath, streamName, indexTable, boundaries, false);
                }
            }
        }
    }

    return result;
}

} // namespace NKikimr::NSchemeShard
