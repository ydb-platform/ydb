#pragma once

#include <ydb/public/api/protos/ydb_topic.pb.h>

namespace NKikimr::NGRpcProxy::V1::NTopic {

template <class T>
void SetProtoTime(T* proto, const ui64 ms) {
    proto->set_seconds(ms / 1000);
    proto->set_nanos((ms % 1000) * 1'000'000);
}

template <class T>
void UpdateProtoTime(T& proto, const T& time, bool storeMin) {
    bool cmp = proto.seconds() > time.seconds() || (proto.seconds() == time.seconds() && proto.nanos() > time.nanos());
    if (cmp == storeMin) {
        proto.CopyFrom(time);
    }
}

} // namespace NKikimr::NGRpcProxy::V1::NTopic
