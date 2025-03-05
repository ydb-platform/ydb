#include "selector.h"
#include <ydb/core/tx/columnshard/export/session/selector/backup/selector.h>
#include <ydb/core/protos/tx_columnshard.pb.h>

namespace NKikimr::NOlap::NExport {

NKikimr::TConclusion<TSelectorContainer> TSelectorContainer::BuildFromProto(const NKikimrTxColumnShard::TBackupTxBody& proto) {
    auto parsed = TBackupSelector::BuildFromProto(proto.GetBackupTask());
    if (!parsed) {
        return parsed.GetError();
    }
    return TSelectorContainer(std::make_shared<TBackupSelector>(parsed.DetachResult()));
}

}
