#include "impl.h"

#include <ydb/public/api/protos/ydb_scheme.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

#include <util/generic/map.h>


namespace NYdb {

TParams::TImpl::TImpl(::google::protobuf::Map<TString, Ydb::TypedValue>&& paramsMap) {
    ParamsMap_.swap(paramsMap);
}

bool TParams::TImpl::Empty() const {
    return ParamsMap_.empty();
}

TMap<TString, TValue> TParams::TImpl::GetValues() const {
    TMap<TString, TValue> valuesMap;
    for (auto it = ParamsMap_.begin(); it != ParamsMap_.end(); ++it) {
        auto paramType = TType(it->second.type());
        auto paramValue = TValue(paramType, it->second.value());

        valuesMap.emplace(it->first, paramValue);
    }

    return valuesMap;
}

TMaybe<TValue> TParams::TImpl::GetValue(const TString& name) const {
    auto it = ParamsMap_.find(name);
    if (it != ParamsMap_.end()) {
        auto paramType = TType(it->second.type());
        return TValue(paramType, it->second.value());
    }

    return TMaybe<TValue>();
}

::google::protobuf::Map<TString, Ydb::TypedValue>* TParams::TImpl::GetProtoMapPtr() {
    return &ParamsMap_;
}

const ::google::protobuf::Map<TString, Ydb::TypedValue>& TParams::TImpl::GetProtoMap() const {
    return ParamsMap_;
}

} // namespace NYdb
