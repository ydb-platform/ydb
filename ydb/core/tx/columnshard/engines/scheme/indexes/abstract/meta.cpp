#include "meta.h"

namespace NKikimr::NOlap::NIndexes {

bool IIndexMeta::DeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) {
    IndexId = proto.GetId();
    AFL_VERIFY(IndexId);
    IndexName = proto.GetName();
    AFL_VERIFY(IndexName);
    StorageId = proto.GetStorageId() ? proto.GetStorageId() : IStoragesManager::DefaultStorageId;
    return DoDeserializeFromProto(proto);
}

}   // namespace NKikimr::NOlap::NIndexes