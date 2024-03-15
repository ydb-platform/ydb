#include "selector.h"
#include <ydb/core/tx/columnshard/export/session/selector/snapshot/selector.h>

namespace NKikimr::NOlap::NExport {

NKikimr::TConclusion<TSelectorContainer> TSelectorContainer::BuildFromProto(const NKikimrTxColumnShard::TBackupTxBody& proto) {
    TSnapshot ss(proto.GetBackupTask().GetSnapshotStep(), proto.GetBackupTask().GetSnapshotTxId());
    return std::make_shared<TSnapshotSelector>(ss);
}

}