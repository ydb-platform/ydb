#pragma once

#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

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

    TMap<TString, TValue> GetValues() const;
    TMaybe<TValue> GetValue(const TString& name) const;

private:
    TParams(::google::protobuf::Map<TString, Ydb::TypedValue>&& protoMap);

    ::google::protobuf::Map<TString, Ydb::TypedValue>* GetProtoMapPtr();
    const ::google::protobuf::Map<TString, Ydb::TypedValue>& GetProtoMap() const;

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
    TParamsBuilder(const TMap<TString, TType>& typeInfo);

    ~TParamsBuilder();

    TParamValueBuilder& AddParam(const TString& name);
    TParamsBuilder& AddParam(const TString& name, const TValue& value);

    bool HasTypeInfo() const;

    TParams Build();

private:
    TParamsBuilder(const ::google::protobuf::Map<TString, Ydb::Type>& typeInfo);

    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

} // namespace NYdb
