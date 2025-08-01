#include "schemeshard__backup_collection_common.h"
#include "schemeshard__op_traits.h"
#include "schemeshard__operation_common.h"
#include "schemeshard__operation_create_cdc_stream.h"
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
        desc.SetOmitIndexes(true);
        desc.SetOmitFollowers(true);
        desc.SetAllowUnderSameOperation(true);

        if (incrBackupEnabled) {
            NKikimrSchemeOp::TCreateCdcStream createCdcStreamOp;
            createCdcStreamOp.SetTableName(item.GetPath());
            auto& streamDescription = *createCdcStreamOp.MutableStreamDescription();
            streamDescription.SetName(streamName);
            streamDescription.SetMode(NKikimrSchemeOp::ECdcStreamModeNewImage);
            streamDescription.SetFormat(NKikimrSchemeOp::ECdcStreamFormatProto);

            const auto sPath = TPath::Resolve(item.GetPath(), context.SS);
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
            streamDescription.SetMode(NKikimrSchemeOp::ECdcStreamModeNewImage);
            streamDescription.SetFormat(NKikimrSchemeOp::ECdcStreamFormatProto);

            const auto sPath = TPath::Resolve(item.GetPath(), context.SS);
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
    }

    return result;
}

} // namespace NKikimr::NSchemeShard
