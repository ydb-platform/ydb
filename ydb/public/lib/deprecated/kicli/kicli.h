#pragma once

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/protos/ydb_result_set_old.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/tx_proxy.pb.h>
#include <ydb/public/lib/deprecated/client/grpc_client.h>
#include <ydb/public/lib/deprecated/client/msgbus_client_config.h>
#include <ydb/public/lib/base/msgbus_status.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <ydb/public/lib/value/value.h>

#include <ydb/library/actors/core/interconnect.h>
#include <library/cpp/messagebus/message_status.h>
#include <library/cpp/messagebus/message.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/queue.h>
#include <util/system/mutex.h>
#include <util/system/condvar.h>
#include <util/system/thread.h>

#include <google/protobuf/text_format.h>

#include <utility>

namespace NBus {
    template<class TBufferRecord, int MType>
    class TBusBufferMessage;
}

namespace NKikimr {

namespace NMsgBusProxy {
    class TMsgBusClient;
}

namespace NClient {

class TResult;
class TQueryResult;
class TPrepareResult;
class TSchemaObject;
class TQuery;
class TTextQuery;
class TPreparedQuery;
class TUnbindedQuery;
class TRetryQueue;
class TKikimr;

using TTablePartitionConfig = NKikimrSchemeOp::TPartitionConfig;
using TModifyScheme = NKikimrSchemeOp::TModifyScheme;

class TType {
public:
    static const TType Int64;
    static const TType Uint64;
    static const TType Int32;
    static const TType Uint32;
    static const TType Double;
    static const TType Float;
    static const TType Bool;
    static const TType Utf8;
    static const TType String;
    static const TType String4k;
    static const TType String2m;
    static const TType Yson;
    static const TType Json;
    static const TType JsonDocument;
    static const TType Timestamp;

    const TString& GetName() const;
    ui16 GetId() const;

    TType(const TString& typeName, ui16 typeId);
    TType(ui16 typeId);

protected:
    TString TypeName;
    ui16 TypeId;
};

class TColumn {
    friend class TSchemaObject;
public:
    TString Name;
    TType Type;
    bool Key;
    ui32 Partitions;

    // generic column of a table, used in schema operations
    TColumn(const TString& name, const TType& type);

protected:
    TColumn(const TString& name, const TType& type, bool key, ui32 partitions);
};

class TKeyColumn : public TColumn {
public:
    // column which automatically will be added to table key
    TKeyColumn(const TString& name, const TType& type);

protected:
    TKeyColumn(const TString& name, const TType& type, ui32 partitions);
};

class TKeyPartitioningColumn : public TKeyColumn {
public:
    // uniform partitioning column, the type should be either Uint64 or Uint32
    // the column should be first in a create table's list
    TKeyPartitioningColumn(const TString& name, const TType& type, ui32 partitions);
};

template <typename MemberType>
class TStructMemberValue {
public:
    TString Name;
    MemberType Value;

    explicit TStructMemberValue(const TString& name, MemberType value)
        : Name(name)
        , Value(value)
    {}

    void StoreType(NKikimrMiniKQL::TStructType& type) const {
        auto& member = *type.AddMember();
        member.SetName(Name);
        auto& memberType = *member.MutableType();
        Value.StoreType(memberType);
    }

    void StoreValue(NKikimrMiniKQL::TValue& value) const {
        auto& structValue = *value.AddStruct();
        Value.StoreValue(structValue);
    }
};

template <typename... MemberTypes>
class TStructValue {
public:
    std::tuple<MemberTypes...> Members;

    TStructValue(MemberTypes... members)
        : Members(members...)
    {}

    void StoreType(NKikimrMiniKQL::TType& type) const {
        type.SetKind(NKikimrMiniKQL::ETypeKind::Struct);
        auto& structType = *type.MutableStruct();
        StoreMembersTypes(structType, std::make_index_sequence<sizeof...(MemberTypes)>());
    }

    void StoreValue(NKikimrMiniKQL::TValue& value) const {
        StoreMembersValues(value, std::make_index_sequence<sizeof...(MemberTypes)>());
    }

protected:
    static void StoreMemberType(NKikimrMiniKQL::TStructType&) {}
    static void StoreMemberValue(NKikimrMiniKQL::TValue&) {}

    template <typename MemberType, typename... OtherMemberTypes>
    static void StoreMemberType(NKikimrMiniKQL::TStructType& type, const MemberType& member, OtherMemberTypes... others) {
        member.StoreType(type);
        StoreMemberType(type, others...);
    }

    template <typename MemberType, typename... OtherMemberTypes>
    static void StoreMemberValue(NKikimrMiniKQL::TValue& value, const MemberType& member, OtherMemberTypes... others) {
        member.StoreValue(value);
        StoreMemberValue(value, others...);
    }

    template <std::size_t... N>
    void StoreMembersTypes(NKikimrMiniKQL::TStructType& type, std::index_sequence<N...>) const {
        StoreMemberType(type, std::get<N>(Members)...);
    }

    template <std::size_t... N>
    void StoreMembersValues(NKikimrMiniKQL::TValue& value, std::index_sequence<N...>) const {
        StoreMemberValue(value, std::get<N>(Members)...);
    }
};

template <typename ValueType, bool HaveValue = true>
class TOptionalValue {
public:
    ValueType Value;

    TOptionalValue(ValueType value)
        : Value(value)
    {}

    void StoreType(NKikimrMiniKQL::TType& type) const {
        type.SetKind(NKikimrMiniKQL::ETypeKind::Optional);
        auto& optionalType = *type.MutableOptional()->MutableItem();
        Value.StoreType(optionalType);
    }

    void StoreValue(NKikimrMiniKQL::TValue& value) const {
        auto& optionalValue = *value.MutableOptional();
        if (HaveValue) {
            Value.StoreValue(optionalValue);
        }
    }
};

// for compatibility
template <typename ParameterType, NScheme::TTypeId SchemeType = SchemeMapper<ParameterType>::SchemeType>
class TParameterValue : public TStructMemberValue<TDataValue<ParameterType, SchemeType>> {
public:
    TParameterValue(const TString& name, ParameterType parameter)
        : TStructMemberValue<TDataValue<ParameterType, SchemeType>>(name, TDataValue<ParameterType, SchemeType>(parameter))
    {}
};

template <typename DataType>
TDataValue<DataType> TData(DataType value) {
    return TDataValue<DataType>(value);
}

template <typename MemberType>
TStructMemberValue<MemberType> TStructMember(const TString& name, MemberType value) {
    return TStructMemberValue<MemberType>(name, value);
}

template <typename... MemberTypes>
TStructValue<MemberTypes...> TStruct(MemberTypes... members) {
    return TStructValue<MemberTypes...>(members...);
}

template <typename ValueType>
TOptionalValue<ValueType> TOptional(ValueType value) {
    return TOptionalValue<ValueType>(value);
}

template <typename ValueType>
TOptionalValue<ValueType, false> TEmptyOptional(ValueType value = ValueType()) {
    return TOptionalValue<ValueType, false>(value);
}

// syntax helpers
// TParameter(name, value) = TStructMemberValue(name, TDataValue(value))

template <typename ParameterType>
TParameterValue<ParameterType, SchemeMapper<ParameterType>::SchemeType> TParameter(const TString& name, ParameterType value) {
    return TParameterValue<ParameterType, SchemeMapper<ParameterType>::SchemeType>(name, value);
}

template <typename... MemberTypes>
TStructMemberValue<TStructValue<MemberTypes...>> TParameter(const TString& name, TStructValue<MemberTypes...> value) {
    return TStructMemberValue<TStructValue<MemberTypes...>>(name, value);
}

template <typename ValueType, bool HaveValue>
TStructMemberValue<TOptionalValue<ValueType, HaveValue>> TParameter(const TString& name, TOptionalValue<ValueType, HaveValue> value) {
    return TStructMemberValue<TOptionalValue<ValueType, HaveValue>>(name, value);
}


class TParameters : public NKikimrMiniKQL::TParams, public TWriteValue {
    friend class TTextQuery;
    friend class TPreparedQuery;
public:
    TParameters() : TWriteValue(*MutableValue(), *MutableType()) {}
};

class TError {
    friend class TResult;
public:
    enum class EFacility : ui16 {
        FacilityMessageBus,
        FacilityExecutionEngine,
        FacilityTxProxy,
        FacilityMsgBusProxy,
    };

    TError() = default;
    TError(TError&&) = default;
    TError(const TError&) = default;
    TError& operator = (TError&&) = default;
    TError& operator = (const TError&) = default;

    bool Success() const;
    bool Error() const { return !Success(); }
    bool Permanent() const;
    bool Temporary() const { return !Permanent(); }
    bool Timeout() const;
    bool Rejected() const;
    TString GetCode() const;
    TString GetMessage() const;
    void Throw() const;
    EFacility GetFacility() const;
    // Returns YDB status
    Ydb::StatusIds::StatusCode GetYdbStatus() const;

protected:
    TError(const TResult& result);
    NMsgBusProxy::EResponseStatus GetMsgBusProxyStatus() const;

    TString Message;
    EFacility Facility;
    ui16 Code;
    Ydb::StatusIds::StatusCode YdbStatus;
};

/// Owns query result data
class TResult {
    // generic polymorphic result of server request
    // supports many different run-time conversions with error checking
    friend class TKikimr;
    friend class TSchemaObject;
    friend class TError;
    friend class TRetryQueue;
public:
    TResult(TResult&&) = default;
    TResult(const TResult&) = default;
    TResult& operator = (TResult&&) = default;
    TResult& operator = (const TResult&) = default;

    // status of the operation
    NMsgBusProxy::EResponseStatus GetStatus() const;
    // detailed error of the operation
    TError GetError() const;
    // mostly for internal use
    ui16 GetType() const;
    template <typename T>
    const T& GetResult() const; // protobuf record

    template <typename T>
    bool HaveResponse() const { // msgbus event
        return Reply.Get()->GetHeader()->Type == T::MessageType;
    }

    template <typename T>
    const T& GetResponse() const { // msgbus event
        Y_ABORT_UNLESS(HaveResponse<T>());
        return *static_cast<T*>(Reply.Get());
    }

    NBus::EMessageStatus GetTransportStatus() const {
        return TransportStatus;
    }

protected:
    TResult(NBus::EMessageStatus transportStatus);
    TResult(NBus::EMessageStatus transportStatus, const TString& message);
    TResult(TAutoPtr<NBus::TBusMessage> reply);

    NBus::EMessageStatus TransportStatus;
    TString TransportErrorMessage;
    TAtomicSharedPtr<NBus::TBusMessage> Reply;
};

class TQueryResult : public TResult {
    friend class TKikimr;
    friend class TTextQuery;
    friend class TPreparedQuery;
public:
    TQueryResult(TQueryResult&&) = default;
    TQueryResult(const TQueryResult&) = default;
    TQueryResult& operator = (TQueryResult&&) = default;
    TQueryResult& operator = (const TQueryResult&) = default;

    // two different ways are available to get content of the query result:
    // (1) it could be converted to use with minikql_result_lib @sa NResultLib::ConvertResult

    // (2) or used with built-in TValue class
    TValue GetValue() const;
    // (1) strict and detailed
    // (2) easy

protected:
    TQueryResult(const TResult& result);
};

class TReadTableResult : public TResult {
    friend class TTableStream;
public:
    TReadTableResult(TReadTableResult&&) = default;
    TReadTableResult(const TReadTableResult&) = default;
    TReadTableResult& operator=(TReadTableResult&&) = default;
    TReadTableResult& operator=(const TReadTableResult&) = default;

    const YdbOld::ResultSet &GetResultSet() const;

    template<typename TFormat>
    TString GetTypeText(const TFormat &format) const;
    template<typename TFormat>
    TString GetValueText(const TFormat &format) const;

protected:
    TReadTableResult(const TResult& result);

    template<typename TFormat>
    static TString ValueToString(const YdbOld::Value &value, const YdbOld::DataType &type);
    template<typename TFormat>
    static TString ValueToString(const YdbOld::Value &value, const YdbOld::Type &type);

    mutable YdbOld::ResultSet Result;
    mutable bool Parsed = false;
};

class TPrepareResult : public TResult {
    friend class TKikimr;
    friend class TTextQuery;
public:
    TPrepareResult(TPrepareResult&&) = default;
    TPrepareResult(const TPrepareResult&) = default;
    TPrepareResult& operator = (TPrepareResult&&) = default;
    TPrepareResult& operator = (const TPrepareResult&) = default;

    TPreparedQuery GetQuery() const;

protected:
    TPrepareResult(const TResult& result, const TQuery& query);

    const TQuery* Query;
};

class TQuery {
    // base class for KiKiMR DB query
    friend class TKikimr;
    friend class TTextQuery;
    friend class TPreparedQuery;
public:
    TQuery(TQuery&&) = default;
    TQuery& operator = (TQuery&&) = default;
    TQuery& operator = (const TQuery&) = delete;

    template <typename... ParametersType> static void StoreParameters(NKikimrMiniKQL::TParams& params, const ParametersType&... parameters) {
        TStructValue<ParametersType...> structValue(parameters...);
        auto& type = *params.MutableType();
        auto& value = *params.MutableValue();
        structValue.StoreType(type);
        structValue.StoreValue(value);
    }

    static void ParseTextParameters(NKikimrMiniKQL::TParams& params, const TString& parameters);

protected:
    TQuery(const TQuery&) = default;
    TQuery(TKikimr& kikimr);

    TKikimr* Kikimr;
};

class TTextQuery : public TQuery {
    // text representation of KiKiMR DB query
    // could be prepared (compiled) to use later, or run directly in text
    friend class TKikimr;
public:
    TTextQuery(TTextQuery&&) = default;
    TTextQuery& operator = (TTextQuery&&) = default;
    TTextQuery(const TTextQuery&) = delete;
    TTextQuery& operator = (const TTextQuery&) = delete;

    // prepare query using round-trip to server
    TPrepareResult SyncPrepare() const;
    NThreading::TFuture<TPrepareResult> AsyncPrepare() const;

    // directly execute query on the server (also one round-trip)
    template <typename... ParametersType>
    TQueryResult SyncExecute(const ParametersType&... parameters) const {
        NKikimrMiniKQL::TParams params;
        StoreParameters(params, parameters...);
        return SyncExecute(params);
    }

    template <typename... ParametersType>
    NThreading::TFuture<TQueryResult> AsyncExecute(const ParametersType&... parameters) const {
        NKikimrMiniKQL::TParams params;
        StoreParameters(params, parameters...);
        return AsyncExecute(params);
    }

    TQueryResult SyncExecute(const TParameters& parameters) const {
        return SyncExecute((const NKikimrMiniKQL::TParams&)parameters);
    }

    NThreading::TFuture<TQueryResult> AsyncExecute(const TParameters& parameters) const {
        return AsyncExecute((const NKikimrMiniKQL::TParams&)parameters);
    }

    TQueryResult SyncExecute(const TString& parameters) const;
    NThreading::TFuture<TQueryResult> AsyncExecute(const TString& parameters) const;

    TQueryResult SyncExecute(const NKikimrMiniKQL::TParams& parameters) const;
    NThreading::TFuture<TQueryResult> AsyncExecute(const NKikimrMiniKQL::TParams& parameters) const;

protected:
    TTextQuery(TKikimr& kikimr, const TString& program);

    TString TextProgram;
};

class TUnbindedQuery {
    friend class TKikimr;
    friend class TPreparedQuery;
public:
    TUnbindedQuery(TUnbindedQuery&&) = default;
    TUnbindedQuery& operator = (TUnbindedQuery&&) = default;
    TUnbindedQuery(const TUnbindedQuery&) = delete;
    TUnbindedQuery& operator = (const TUnbindedQuery&) = delete;

protected:
    TUnbindedQuery(const TString& program);

    TString CompiledProgram;
};

class TPreparedQuery : public TQuery {
    // binary (compiled) representation of KiKiMR DB query
    // could be stored for use later
    friend class TKikimr;
    friend class TPrepareResult;
public:
    TPreparedQuery(TPreparedQuery&&) = default;
    TPreparedQuery& operator = (TPreparedQuery&&) = default;
    TPreparedQuery(const TPreparedQuery&) = delete;
    TPreparedQuery& operator = (const TPreparedQuery&) = delete;

    // execute query on the server (also one round-trip)
    template <typename... ParametersType>
    TQueryResult SyncExecute(const ParametersType&... parameters) const {
        NKikimrMiniKQL::TParams params;
        StoreParameters(params, parameters...);
        return SyncExecute(params);
    }

    template <typename... ParametersType>
    NThreading::TFuture<TQueryResult> AsyncExecute(const ParametersType&... parameters) const {
        NKikimrMiniKQL::TParams params;
        StoreParameters(params, parameters...);
        return AsyncExecute(params);
    }

    TQueryResult SyncExecute(const TParameters& parameters) const {
        return SyncExecute((const NKikimrMiniKQL::TParams&)parameters);
    }

    NThreading::TFuture<TQueryResult> AsyncExecute(const TParameters& parameters) const {
        return AsyncExecute((const NKikimrMiniKQL::TParams&)parameters);
    }

    TQueryResult SyncExecute(const TString& parameters) const;
    NThreading::TFuture<TQueryResult> AsyncExecute(const TString& parameters) const;

    TQueryResult SyncExecute(const NKikimrMiniKQL::TParams& parameters) const;
    NThreading::TFuture<TQueryResult> AsyncExecute(const NKikimrMiniKQL::TParams& parameters) const;

    TUnbindedQuery Unbind() const;

protected:
    TPreparedQuery(const TQuery& textQuery, const TString& program);

    TString CompiledProgram;
};

struct TSchemaObjectStats {
    ui32 PartitionsCount;
};

class TSchemaObject {
    // KiKiMR DB schema object, used for schema construction and initialization
    friend class TKikimr;
public:
    /// @sa NKikimrSchemeOp::EPathType
    enum class EPathType : ui32 {
        Unknown = 0,
        Directory,
        Table,
        PersQueueGroup,
        SubDomain,
        RtmrVolume,
        BlockStoreVolume,
        Kesus,
        SolomonVolume,
        FileStore,
        OlapStore,
        OlapTable,
        Sequence,
        Replication,
        BlobDepot,
        ExternalTable,
        ExternalDataSource,
        View,
        ResourcePool
    };

    TSchemaObject(TSchemaObject&&) = default;
    TSchemaObject(const TSchemaObject&) = default;

    void Drop();
    void ModifySchema(const TModifyScheme& schema);
    TSchemaObject MakeDirectory(const TString& name);
    TSchemaObject CreateTable(const TString& name, const TVector<TColumn>& columns);
    TSchemaObject CreateTable(const TString& name, const TVector<TColumn>& columns,
                              const TTablePartitionConfig& partitionConfig);
    TSchemaObject GetChild(const TString& name) const;
    TString GetName() const;
    TString GetPath() const;
    TVector<TSchemaObject> GetChildren() const;
    TVector<TColumn> GetColumns() const;
    TSchemaObjectStats GetStats() const;

    EPathType GetPathType() const { return PathType; }
    bool IsDirectory() const { return PathType == EPathType::Directory; }
    bool IsTable() const { return PathType == EPathType::Table; }
    bool IsPersQueueGroup() const { return PathType == EPathType::PersQueueGroup; }

protected:
    TSchemaObject(TKikimr& kikimr, const TString& path, const TString& name, ui64 pathId = 0,
                  EPathType pathType = EPathType::Unknown);

    TSchemaObject DoCreateTable(const TString& name, const TVector<TColumn>& columns,
                                const TTablePartitionConfig* partitionConfig);

    TKikimr& Kikimr;
    TString Path;
    TString Name;
    ui64 PathId;
    EPathType PathType;
};

class TRegistrationResult : public TResult {
    friend class TNodeRegistrant;
public:
    TRegistrationResult(const TRegistrationResult& other) = default;
    TRegistrationResult(TRegistrationResult&& other) = default;
    TRegistrationResult& operator=(const TRegistrationResult& other) = default;
    TRegistrationResult& operator=(TRegistrationResult&& other) = default;

    bool IsSuccess() const;
    TString GetErrorMessage() const;

    ui32 GetNodeId() const;
    NActors::TScopeId GetScopeId() const;

    const NKikimrClient::TNodeRegistrationResponse& Record() const;

private:
    TRegistrationResult(const TResult& result);
};

class TNodeRegistrant {
    friend class TKikimr;
public:
    TRegistrationResult SyncRegisterNode(const TString& domainPath, const TString& host, ui16 port,
                                         const TString& address, const TString& resolveHost,
                                         const NActors::TNodeLocation& location,
                                         bool fixedNodeId = false, TMaybe<TString> path = {}) const;

private:
    TNodeRegistrant(TKikimr& kikimr);

private:
    TKikimr* Kikimr;
};

class TTableStream {
    friend class TKikimr;
public:
    NThreading::TFuture<TResult> AsyncRead(const TString &path, bool ordered,
                                           std::function<void(NClient::TReadTableResult)> processPart,
                                           const TVector<TString> &columns = TVector<TString>(),
                                           const NKikimrTxUserProxy::TKeyRange &range = NKikimrTxUserProxy::TKeyRange(),
                                           ui64 limit = 0);

private:
    TTableStream(TKikimr& kikimr);

private:
    TKikimr* Kikimr;
};

class TConfigurationResult : public TResult {
    friend class TNodeConfigurator;
public:
    TConfigurationResult(const TConfigurationResult& other) = default;
    TConfigurationResult(TConfigurationResult&& other) = default;
    TConfigurationResult& operator=(const TConfigurationResult& other) = default;
    TConfigurationResult& operator=(TConfigurationResult&& other) = default;

    bool IsSuccess() const;
    TString GetErrorMessage() const;

    const NKikimrConfig::TAppConfig &GetConfig() const;
    bool HasYamlConfig() const;
    const TString& GetYamlConfig() const;
    TMap<ui64, TString> GetVolatileYamlConfigs() const;

    const NKikimrClient::TConsoleResponse &Record() const;

private:
    TConfigurationResult(const TResult& result);
};

class TNodeConfigurator {
    friend class TKikimr;
public:
    TConfigurationResult SyncGetNodeConfig(ui32 nodeId,
                                           const TString &host,
                                           const TString &tenant,
                                           const TString &nodeType,
                                           const TString& domain = "",
                                           const TString& token = "",
                                           bool serveYaml = false,
                                           ui64 version = 0) const;

private:
    TNodeConfigurator(TKikimr& kikimr);

private:
    TKikimr* Kikimr;
};

struct TRetryPolicy {
    ui32 RetryLimitCount;
    TDuration MinRetryTime;
    TDuration MaxRetryTime;
    ui32 BackoffMultiplier;
    bool DoFirstRetryInstantly;

    TRetryPolicy(ui32 retryLimitCount = 0, TDuration minRetryTime = TDuration::MilliSeconds(10), TDuration maxRetryTime = TDuration::Minutes(10), ui32 backoffMultiplier = 2, bool doFirstRetryInstantly = true)
        : RetryLimitCount(retryLimitCount)
        , MinRetryTime(minRetryTime)
        , MaxRetryTime(maxRetryTime)
        , BackoffMultiplier(backoffMultiplier)
        , DoFirstRetryInstantly(doFirstRetryInstantly)
    {}
};

struct TConnectionPolicy {
    TRetryPolicy RetryPolicy;
    bool ChooseProxy; // use to switch from primary connection endpoint to any available proxy upon first connection (balancer strategy)

    TConnectionPolicy()
        : RetryPolicy(2, TDuration::MilliSeconds(500), TDuration::Seconds(1), 2, true)
        , ChooseProxy(false)
    {}

    TConnectionPolicy(const TRetryPolicy& retryPolicy)
        : RetryPolicy(retryPolicy)
        , ChooseProxy(false)
    {}
};

class TKikimr {
    friend class TQuery;
    friend class TTextQuery;
    friend class TPreparedQuery;
    friend class TSchemaObject;
    friend class TNodeRegistrant;
    friend class TNodeConfigurator;
    class TImpl;
    class TMsgBusImpl;
    class TGRpcImpl;
    class TRetryQueue;
public:
    static ui32 POLLING_TIMEOUT; // Polling timeout used during long-polling of long datashard/schemeshard commands = 10000;
    static bool DUMP_REQUESTS; // Dump all request and responds to stderr = false;

    // primary interface class to KiKiMR, producer of Queries and SchemaObjects
    // the only, who handles MsgBus interface
    TKikimr(const NMsgBusProxy::TMsgBusClientConfig& clientConfig, const TConnectionPolicy& policy = TConnectionPolicy());
    TKikimr(const NGRpcProxy::TGRpcClientConfig& clientConfig, const TConnectionPolicy& policy = TConnectionPolicy());
    TKikimr(TKikimr&& kikimr);
    ~TKikimr();

    TSchemaObject GetSchemaRoot(const TString& name = TString());
    TSchemaObject GetSchemaObject(const TString& name);
    TTextQuery Query(const TString& program);
    TPreparedQuery Query(const TUnbindedQuery& query);
    // sets security token
    void SetSecurityToken(const TString& securityToken);
    TString GetCurrentLocation() const;

    TNodeRegistrant GetNodeRegistrant();
    TNodeConfigurator GetNodeConfigurator();

    // execute arbitrary message bus request
    template <typename RecordType, int MessageType>
    NThreading::TFuture<TResult> ExecuteRequest(NBus::TBusBufferMessage<RecordType, MessageType>* request) {
        PrepareRequest(request->Record);
        DumpRequest(request->Record);
        return ExecuteRequest(TAutoPtr<NBus::TBusMessage>(request));
    }

    // execute general message bus request
    NThreading::TFuture<TResult> ExecuteRequest(TAutoPtr<NBus::TBusMessage> request);

protected:
    NThreading::TFuture<TQueryResult> ExecuteQuery(const TTextQuery& query, const NKikimrMiniKQL::TParams& parameters);
    NThreading::TFuture<TQueryResult> ExecuteQuery(const TTextQuery& query, const TString& parameters);
    NThreading::TFuture<TQueryResult> ExecuteQuery(const TPreparedQuery& query, const NKikimrMiniKQL::TParams& parameters);
    NThreading::TFuture<TQueryResult> ExecuteQuery(const TPreparedQuery& query, const TString& parameters);
    NThreading::TFuture<TPrepareResult> PrepareQuery(const TTextQuery& query);
    NThreading::TFuture<TResult> DescribeObject(const TSchemaObject& object);
    NThreading::TFuture<TResult> ModifySchema(const TModifyScheme& schema);
    NThreading::TFuture<TResult> MakeDirectory(const TSchemaObject& object, const TString& name);
    NThreading::TFuture<TResult> CreateTable(TSchemaObject& object, const TString& name, const TVector<TColumn>& columns,
                                             const TTablePartitionConfig* partitionConfig);
    NBus::EMessageStatus ExecuteRequestInternal(NThreading::TPromise<TResult> promise, TAutoPtr<NBus::TBusMessage> request);
    NThreading::TFuture<TResult> RegisterNode(const TString& domainPath, const TString& host, ui16 port,
                                              const TString& address, const TString& resolveHost,
                                              const NActors::TNodeLocation& location,
                                              bool fixedNodeId, TMaybe<TString> path);
    NThreading::TFuture<TResult> GetNodeConfig(ui32 nodeId,
                                               const TString &host,
                                               const TString &tenant,
                                               const TString &nodeType,
                                               const TString& domain,
                                               const TString& token = TString(),
                                               bool serveYaml = false,
                                               ui64 version = 0);

    template <typename T> static void DumpRequest(const T& pb) {
        if (DUMP_REQUESTS) {
            TString res;
            ::google::protobuf::TextFormat::PrintToString(pb, &res);
            Cerr << "<-- " << TypeName<T>() << Endl << res << Endl;
        }
    }

    template <typename T> static void DumpResponse(const T& pb) {
        if (DUMP_REQUESTS) {
            TString res;
            ::google::protobuf::TextFormat::PrintToString(pb, &res);
            Cerr << "--> " << TypeName<T>() << Endl << res << Endl;
        }
    }

    template <typename T>
    void PrepareRequest(T&) const {}

    void PrepareRequest(NKikimrClient::TRequest& request) const {
        if (!SecurityToken.empty()) {
            request.SetSecurityToken(SecurityToken);
        }
    }

    void PrepareRequest(NKikimrClient::TCmsRequest& request) const {
        if (!SecurityToken.empty()) {
            request.SetSecurityToken(SecurityToken);
        }
    }

    void PrepareRequest(NKikimrClient::TConsoleRequest& request) const {
        if (!SecurityToken.empty()) {
            request.SetSecurityToken(SecurityToken);
        }
    }

    void PrepareRequest(NKikimrClient::TSchemeDescribe& request) const {
        if (!SecurityToken.empty()) {
            request.SetSecurityToken(SecurityToken);
        }
    }

    void PrepareRequest(NKikimrClient::TSchemeOperation& request) const {
        if (!SecurityToken.empty()) {
            request.SetSecurityToken(SecurityToken);
        }
    }

    void PrepareRequest(NKikimrClient::TLocalMKQL& request) const {
        if (!SecurityToken.empty()) {
            request.SetSecurityToken(SecurityToken);
        }
    }

    void PrepareRequest(NKikimrClient::TLocalSchemeTx& request) const {
        if (!SecurityToken.empty()) {
            request.SetSecurityToken(SecurityToken);
        }
    }

    TString SecurityToken;
    THolder<TImpl> Impl;
};

} // NClient
} // NKikimr
