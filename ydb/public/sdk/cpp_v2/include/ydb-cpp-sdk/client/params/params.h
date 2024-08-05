#pragma once

#include <ydb-cpp-sdk/client/value/value.h>

#include <google/protobuf/map.h>

namespace Ydb {
    class TypedValue;
}

namespace NYdb {

namespace NScripting {
    class TScriptingClient;
}

class TProtoAccessor;

namespace NTable {
    class TTableClient;
    class TSession;
    class TDataQuery;
}

namespace NExperimental {
    class TStreamQueryClient;
}

namespace NQuery {
    class TExecQueryImpl;
    class TQueryClient;
}

class TParamsBuilder;

class TParams {
    friend class TParamsBuilder;
    friend class NTable::TTableClient;
    friend class NTable::TSession;
    friend class NTable::TDataQuery;
    friend class NScripting::TScriptingClient;
    friend class NExperimental::TStreamQueryClient;
    friend class NQuery::TExecQueryImpl;
    friend class NQuery::TQueryClient;
    friend class NYdb::TProtoAccessor;
public:
    bool Empty() const;

    std::map<std::string, TValue> GetValues() const;
    std::optional<TValue> GetValue(const std::string& name) const;

private:
    TParams(::google::protobuf::Map<std::string, Ydb::TypedValue>&& protoMap);

    ::google::protobuf::Map<std::string, Ydb::TypedValue>* GetProtoMapPtr();
    const ::google::protobuf::Map<std::string, Ydb::TypedValue>& GetProtoMap() const;

    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

class TParamValueBuilder : public TValueBuilderBase<TParamValueBuilder> {
    friend class TParamsBuilder;
public:
    TParamsBuilder& Build();
    bool Finished();

private:
    TParamValueBuilder(TParamsBuilder& owner, Ydb::Type& typeProto, Ydb::Value& valueProto);

    TParamsBuilder& Owner_;
    bool Finished_;
};

class TParamsBuilder : public TMoveOnly {
    friend class NTable::TDataQuery;
public:
    TParamsBuilder(TParamsBuilder&&);
    TParamsBuilder();
    TParamsBuilder(const std::map<std::string, TType>& typeInfo);

    ~TParamsBuilder();

    TParamValueBuilder& AddParam(const std::string& name);
    TParamsBuilder& AddParam(const std::string& name, const TValue& value);

    bool HasTypeInfo() const;

    TParams Build();

private:
    TParamsBuilder(const ::google::protobuf::Map<std::string, Ydb::Type>& typeInfo);

    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

} // namespace NYdb
