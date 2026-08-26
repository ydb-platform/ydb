#include "types.h"

#include <ydb/core/protos/blobstorage_ddisk.pb.h>

namespace NKikimr::NBsController {

    TDDiskId::TDDiskId(const NKikimrBlobStorage::NDDisk::TDDiskId& pb)
        : NodeId(pb.GetNodeId())
        , PDiskId(pb.GetPDiskId())
        , DDiskSlotId(pb.GetDDiskSlotId())
    {}

    void TDDiskId::Serialize(NKikimrBlobStorage::NDDisk::TDDiskId *pb) const {
        pb->SetNodeId(NodeId);
        pb->SetPDiskId(PDiskId);
        pb->SetDDiskSlotId(DDiskSlotId);
    }

} // NKikimr::NBsController
