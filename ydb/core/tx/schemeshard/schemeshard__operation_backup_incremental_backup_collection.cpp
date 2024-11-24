#include "schemeshard__operation_common.h"
#include "schemeshard__operation_create_cdc_stream.h"
#include "schemeshard_impl.h"

#include <ydb/core/tx/schemeshard/backup/constants.h>

namespace NKikimr::NSchemeShard {

TVector<ISubOperation::TPtr> CreateBackupIncrementalBackupCollection(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    TString bcPathStr = JoinPath({tx.GetWorkingDir().c_str(), tx.GetBackupIncrementalBackupCollection().GetName().c_str()});

    const TPath& bcPath = TPath::Resolve(bcPathStr, context.SS);
    const auto& bc = context.SS->BackupCollections[bcPath->PathId];

    size_t cutLen = bcPath.GetDomainPathString().size() + 1;

    TVector<ISubOperation::TPtr> result;

    for (const auto& item : bc->Description.GetExplicitEntryList().GetEntries()) {
        NKikimrSchemeOp::TModifyScheme modifyScheme;
        modifyScheme.SetWorkingDir(tx.GetWorkingDir());
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterContinuousBackup);
        modifyScheme.SetInternal(true);
        auto& cb = *modifyScheme.MutableAlterContinuousBackup();
        cb.SetTableName(item.GetPath().substr(cutLen, item.GetPath().size() - cutLen));
        auto& ib = *cb.MutableTakeIncrementalBackup();
        ib.SetDstPath(tx.GetBackupIncrementalBackupCollection().GetTargetDir() + item.GetPath().substr(cutLen, item.GetPath().size() - cutLen));

        if (!CreateAlterContinuousBackup(opId, modifyScheme, context, result)) {
            return result;
        }
    }

    return result;
}

} // namespace NKikimr::NSchemeShard
