#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#define LOG_I(stream) LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define RETURN_RESULT_UNLESS(x) if (!(x)) return result;

namespace NKikimr::NSchemeShard {

TVector<ISubOperation::TPtr> CreateBackupBackupCollection(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_UNUSED(opId, tx, context);

    NKikimrSchemeOp::TModifyScheme modifyScheme;

    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables);
    modifyScheme.SetInternal(true);

    auto& cct = *modifyScheme.MutableCreateConsistentCopyTables();
    auto& copyTables = *cct.MutableCopyTableDescriptions();
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

    for (const auto& item : bc->Properties.GetExplicitEntryList().GetEntries()) {

        auto& desc = *copyTables.Add();
        desc.SetSrcPath(item.GetPath());
        // desc.SetDstPath(JoinPath({targetPathStr, item.GetPath().substr(1, item.GetPath().size() - 1)}));
        desc.SetDstPath("0" + item.GetPath());
        desc.SetOmitIndexes(true);
        desc.SetOmitFollowers(true);
        desc.SetIsBackup(true);
    }

    // Y_ABORT("%s", modifyScheme.DebugString().c_str());

    auto res = CreateConsistentCopyTables(opId, modifyScheme, context);

    // FIXME(+active) it is impossible to combine two datashard ops, we have to write new one
    // if (bc->Properties.HasIncrementalBackupConfig()) {
    //     for (const auto& item : bc->Properties.GetExplicitEntryList().GetEntries()) {
    //         NKikimrSchemeOp::TModifyScheme tx2;

    //         tx2.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateContinuousBackup);
    //         tx2.SetInternal(true);
    //         auto& cb = *tx2.MutableCreateContinuousBackup();
    //         cb.SetTableName(item.GetPath());
    //         cb.MutableContinuousBackupDescription();

    //         auto res2 = CreateNewContinuousBackup(opId, tx2, context, res);

    //         if (!res2) {
    //             return res;
    //         }
    //     }
    // }

    return res;
}

}  // namespace NKikimr::NSchemeShard
