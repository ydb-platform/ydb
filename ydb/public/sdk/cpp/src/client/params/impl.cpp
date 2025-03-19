#include "impl.h"

#include <ydb/public/api/protos/ydb_scheme.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>


namespace NYdb::inline Dev {

TParams::TImpl::TImpl(::google::protobuf::Map<TStringType, Ydb::TypedValue>&& paramsMap) {
    ParamsMap_.swap(paramsMap);
}

bool TParams::TImpl::Empty() const {
    return ParamsMap_.empty();
}

std::map<std::string, TValue> TParams::TImpl::GetValues() const {
    std::map<std::string, TValue> valuesMap;
    for (auto it = ParamsMap_.begin(); it != ParamsMap_.end(); ++it) {
        auto paramType = TType(it->second.type());
        auto paramValue = TValue(paramType, it->second.value());

        valuesMap.emplace(it->first, paramValue);
    }

    return valuesMap;
}

std::optional<TValue> TParams::TImpl::GetValue(const std::string& name) const {
    auto it = ParamsMap_.find(name);
    if (it != ParamsMap_.end()) {
        auto paramType = TType(it->second.type());
        return TValue(paramType, it->second.value());
    }

    return std::optional<TValue>();
}

::google::protobuf::Map<TStringType, Ydb::TypedValue>* TParams::TImpl::GetProtoMapPtr() {
    return &ParamsMap_;
}

const ::google::protobuf::Map<TStringType, Ydb::TypedValue>& TParams::TImpl::GetProtoMap() const {
    return ParamsMap_;
}

} // namespace NYdb
