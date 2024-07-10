#include "meta.h"

namespace NKikimr::NOlap::NIndexes {

bool IIndexMeta::DeserializeFromProto(const NKikimrSchemeOp::TOlapSecondaryData& proto) {
    if (!proto.GetId()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse secondary data builder")("reason", "incorrect id - 0");
        return false;
    }
    if (!proto.GetName()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse secondary data builder")("reason", "incorrect name - empty string");
        return false;
    }
    EntityId = proto.GetId();
    Name = proto.GetName();
    StorageId = proto.GetStorageId() ? proto.GetStorageId() : IStoragesManager::DefaultStorageId;
    return DoDeserializeFromProto(proto);
}

void IIndexMeta::SerializeToProto(NKikimrSchemeOp::TOlapSecondaryData& proto) const {
    AFL_VERIFY(EntityId);
    proto.SetId(EntityId);
    AFL_VERIFY(Name);
    proto.SetName(Name);
    if (StorageId) {
        proto.SetStorageId(StorageId);
    }
    return DoSerializeToProto(proto);
}

}   // namespace NKikimr::NOlap::NIndexes