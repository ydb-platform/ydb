#pragma once

#include <ydb-cpp-sdk/client/params/params.h>

namespace NYdb {

class TParams::TImpl {
public:
    TImpl(::google::protobuf::Map<std::string, Ydb::TypedValue>&& paramsMap);

    bool Empty() const;
    std::map<std::string, TValue> GetValues() const;
    std::optional<TValue> GetValue(const std::string& name) const;
    ::google::protobuf::Map<std::string, Ydb::TypedValue>* GetProtoMapPtr();
    const ::google::protobuf::Map<std::string, Ydb::TypedValue>& GetProtoMap() const;

private:
    ::google::protobuf::Map<std::string, Ydb::TypedValue> ParamsMap_;
};

} // namespace NYdb
