#pragma once

#include <ydb-cpp-sdk/client/params/params.h>

namespace NYdb::inline V3 {

class TParams::TImpl {
public:
    TImpl(::google::protobuf::Map<TStringType, NYdbProtos::TypedValue>&& paramsMap);

    bool Empty() const;
    std::map<std::string, TValue> GetValues() const;
    std::optional<TValue> GetValue(const std::string& name) const;
    ::google::protobuf::Map<TStringType, NYdbProtos::TypedValue>* GetProtoMapPtr();
    const ::google::protobuf::Map<TStringType, NYdbProtos::TypedValue>& GetProtoMap() const;

private:
    ::google::protobuf::Map<TStringType, NYdbProtos::TypedValue> ParamsMap_;
};

} // namespace NYdb
