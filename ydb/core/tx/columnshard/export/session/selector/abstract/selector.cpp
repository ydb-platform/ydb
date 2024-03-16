#include "selector.h"
#include <ydb/core/tx/columnshard/export/session/selector/snapshot/selector.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NOlap::NExport {

NKikimr::TConclusion<TSelectorContainer> TSelectorContainer::BuildFromProto(const NKikimrTxColumnShard::TBackupTxBody& proto) {
    TSnapshot ss(proto.GetBackupTask().GetSnapshotStep(), proto.GetBackupTask().GetSnapshotTxId());
    return TSelectorContainer(std::make_shared<TSnapshotSelector>(ss));
}

}