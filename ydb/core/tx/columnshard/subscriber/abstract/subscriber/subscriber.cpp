#include "subscriber.h"

namespace NKikimr::NColumnShard::NSubscriber {

bool IIndexMeta::DeserializeFromProto(const NKikimrColumnShardSubscriberProto::TSubscriberState& proto) {
    return DoDeserializeFromProto(proto);
}

void IIndexMeta::SerializeToProto(NKikimrColumnShardSubscriberProto::TSubscriberState& proto) const {
    return DoSerializeToProto(proto);
}

}
