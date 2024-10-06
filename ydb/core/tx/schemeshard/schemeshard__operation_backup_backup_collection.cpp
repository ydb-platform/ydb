#include "schemeshard__operation_common.h"
#include "schemeshard__operation_create_cdc_stream.h"
#include "schemeshard_impl.h"

#include <ydb/core/tx/schemeshard/backup/constants.h>

#define LOG_I(stream) LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define RETURN_RESULT_UNLESS(x) if (!(x)) return result;

namespace NKikimr::NSchemeShard {

TString ToX509String(const TInstant& datetime) {
    return datetime.FormatLocalTime("%Y%m%d%H%M%SZ");
}

TVector<ISubOperation::TPtr> CreateBackupBackupCollection(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_UNUSED(opId, tx, context);

    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables);
    modifyScheme.SetInternal(true);

    auto& cct = *modifyScheme.MutableCreateConsistentCopyTables();
    auto& copyTables = *cct.MutableCopyTableDescriptions();
    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);

    // Y_ABORT("%s %s", tx.GetWorkingDir().c_str(), tx.GetBackupBackupCollection().GetName().c_str());
    // FIXME(+active)

    TString bcPathStr = JoinPath({tx.GetWorkingDir().c_str(), tx.GetBackupBackupCollection().GetName().c_str()});
    // TString targetPathStr = JoinPath({tx.GetWorkingDir().c_str(), tx.GetBackupBackupCollection().GetName().c_str(), "0"});

    const TPath& bcPath = TPath::Resolve(bcPathStr, context.SS);
    const auto& bc = context.SS->BackupCollections[bcPath->PathId];
    // copyTables.Reserve(exportInfo->Items.size());

    // const TPath exportPath = TPath::Init(exportInfo->ExportPathId, ss);
    // const TString& exportPathName = exportPath.PathString();

    cct.SetDstBasePath(bcPathStr);

    TVector<ISubOperation::TPtr> result;

    size_t cutLen = bcPath.GetDomainPathString().size() + 1;

    auto now = ToX509String(TlsActivationContext->AsActorContext().Now());

    for (const auto& item : bc->Properties.GetExplicitEntryList().GetEntries()) {
        NKikimrSchemeOp::TCreateCdcStream createCdcStreamOp;
        createCdcStreamOp.SetTableName(item.GetPath());
        auto& streamDescription = *createCdcStreamOp.MutableStreamDescription();
        streamDescription.SetName(NBackup::CB_CDC_STREAM_NAME);
        streamDescription.SetMode(NKikimrSchemeOp::ECdcStreamModeUpdate);
        streamDescription.SetFormat(NKikimrSchemeOp::ECdcStreamFormatProto);

        const auto sPath = TPath::Resolve(item.GetPath(), context.SS);
        NCdc::DoCreateStreamImpl(result, createCdcStreamOp, opId, workingDirPath, sPath, false, false);

       auto& desc = *copyTables.Add();
        desc.SetSrcPath(item.GetPath());
        desc.SetDstPath(now + "_full/" + item.GetPath().substr(cutLen, item.GetPath().size() - cutLen));
        desc.SetOmitIndexes(true);
        desc.SetOmitFollowers(true);
        // desc.SetIsBackup(true);
        desc.MutableCreateCdcStream()->CopyFrom(createCdcStreamOp);
    }

    CreateConsistentCopyTables(opId, modifyScheme, context, result);

    for (const auto& item : bc->Properties.GetExplicitEntryList().GetEntries()) {
        NKikimrSchemeOp::TCreateCdcStream createCdcStreamOp;
        createCdcStreamOp.SetTableName(item.GetPath());
        auto& streamDescription = *createCdcStreamOp.MutableStreamDescription();
        streamDescription.SetName(NBackup::CB_CDC_STREAM_NAME);
        streamDescription.SetMode(NKikimrSchemeOp::ECdcStreamModeUpdate);
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

        const auto streamPath = sPath.Child(NBackup::CB_CDC_STREAM_NAME);

        NCdc::DoCreatePqPart(result, createCdcStreamOp, opId, streamPath, NBackup::CB_CDC_STREAM_NAME, table, boundaries, false);
    }

    return result;
}

TVector<ISubOperation::TPtr> CreateBackupIncrementalBackupCollection(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_UNUSED(opId, tx, context);

    TString bcPathStr = JoinPath({tx.GetWorkingDir().c_str(), tx.GetBackupIncrementalBackupCollection().GetName().c_str()});

    const TPath& bcPath = TPath::Resolve(bcPathStr, context.SS);
    const auto& bc = context.SS->BackupCollections[bcPath->PathId];

    size_t cutLen = bcPath.GetDomainPathString().size() + 1;

    TVector<ISubOperation::TPtr> result;

    auto now = ToX509String(TlsActivationContext->AsActorContext().Now());

    for (const auto& item : bc->Properties.GetExplicitEntryList().GetEntries()) {
        NKikimrSchemeOp::TModifyScheme modifyScheme;
        modifyScheme.SetWorkingDir(tx.GetWorkingDir());
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterContinuousBackup);
        modifyScheme.SetInternal(true);
        auto& cb = *modifyScheme.MutableAlterContinuousBackup();
        cb.SetTableName(item.GetPath().substr(cutLen, item.GetPath().size() - cutLen));
        auto& ib = *cb.MutableTakeIncrementalBackup();
        ib.SetDstPath(bcPathStr.substr(cutLen, bcPathStr.size() - cutLen) + "/" + now + "_incremental/" + item.GetPath().substr(cutLen, item.GetPath().size() - cutLen));

        if (!CreateAlterContinuousBackup(opId, modifyScheme, context, result)) {
            return result;
        }
    }

    return result;
}

TVector<ISubOperation::TPtr> CreateRestoreBackupCollection(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_UNUSED(opId, tx, context);

    TString bcPathStr = JoinPath({tx.GetWorkingDir().c_str(), tx.GetRestoreBackupCollection().GetName().c_str()});

    const TPath& bcPath = TPath::Resolve(bcPathStr, context.SS);

    const auto& bc = context.SS->BackupCollections[bcPath->PathId];

    // (1) iterate over bc children and find last __full__
    // (2) save __full__ to var and all consequent __incremental__ to array
    // (3) run consistent copy-tables in __full__
    // (3.1) expand consistent copy-tables with restore-list

    // # restore-list (O(n^2) if ds sends to next ds, better orchestrate through schemeshard O(n))
    // (1) send propose to all __full__ and __incremental__
    // (2) when __full__ finish with full backup it strongly sends *continue* to first __incremental__
    // (3) when __incremental__ finishes restore it sends *continue* to next __incremental__
    // (4) when all parts finished schemeshard finishes the operation

    TString lastFullBackupName;
    TVector<TString> incBackupNames;

    if (!bcPath.Base()->GetChildren().size()) {
        return {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, TStringBuilder() << "Nothing to restore")};
    } else {
        for (auto& [child, _] : bcPath.Base()->GetChildren()) {
            if (child.EndsWith("_full")) {
                lastFullBackupName = child;
                incBackupNames.clear();
            } else if (child.EndsWith("_incremental")) {
                incBackupNames.push_back(child);
            }
        }
    }

    Cerr << "going to restore:" << Endl;
    Cerr << "\tFullBackup:" << Endl;
    Cerr << "\t\t" << bcPath.Child(lastFullBackupName).PathString() << Endl;
    Cerr << "\tIncrBackups" << Endl;
    Cerr << "\t\t" << ::JoinSeq(", ", incBackupNames) << Endl;

    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables);
    modifyScheme.SetInternal(true);

    auto& cct = *modifyScheme.MutableCreateConsistentCopyTables();
    auto& copyTables = *cct.MutableCopyTableDescriptions();
    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);

    // cct.SetDstBasePath(bcPath.GetDomainPathString());

    TVector<ISubOperation::TPtr> result;

    size_t cutLen = bcPath.GetDomainPathString().size() + 1;

    for (const auto& item : bc->Properties.GetExplicitEntryList().GetEntries()) {
        Y_UNUSED(item, copyTables);

       auto& desc = *copyTables.Add();
        desc.SetSrcPath(bcPath.Child(lastFullBackupName).PathString() + item.GetPath().substr(cutLen - 1, item.GetPath().size() - cutLen + 1));
        desc.SetDstPath(item.GetPath());
        desc.SetOmitIndexes(true);
        desc.SetOmitFollowers(true);
        // desc.SetIsBackup(true);
    }

    CreateConsistentCopyTables(opId, modifyScheme, context, result);

    // oh...

    // //

    // for (const auto& item : bc->Properties.GetExplicitEntryList().GetEntries()) {
    //     NKikimrSchemeOp::TModifyScheme modifyScheme2;
    //     modifyScheme2.SetOperationType(NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups);
    //     modifyScheme2.SetInternal(true);

    //     auto& desc = *modifyScheme2.MutableRestoreMultipleIncrementalBackups();
    //     // for (const auto& srcTableName : srcTableNames) {
    //     //     desc.AddSrcTableNames(srcTableName);
    //     // }
    //     desc.SetDstTableName(item.GetPath());

    //     CreateRestoreMultipleIncrementalBackups(opId, modifyScheme2, context, result);
    // }
    // //

    return result;
}

}  // namespace NKikimr::NSchemeShard
