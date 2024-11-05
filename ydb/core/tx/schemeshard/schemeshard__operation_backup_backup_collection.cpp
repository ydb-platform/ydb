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

    for (const auto& item : bc->Description.GetExplicitEntryList().GetEntries()) {
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
        desc.MutableCreateSrcCdcStream()->CopyFrom(createCdcStreamOp);
    }

    if (!CreateConsistentCopyTables(opId, modifyScheme, context, result)) {
        return result;
    }

    for (const auto& item : bc->Description.GetExplicitEntryList().GetEntries()) {
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

} // namespace NKikimr::NSchemeShard
