#pragma once

#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>

#include <util/generic/ptr.h>
#include <util/generic/guid.h>
#include <google/protobuf/arena.h>

#include <vector>

namespace NKqpProto {
class TKqpPhyParamBinding;
}

namespace Ydb {
class TypedValue;
}

namespace NYql {
namespace NDqProto {
class TData;
}
}

namespace NKikimr::NKqp {

using TTypedUnboxedValue = std::pair<NKikimr::NMiniKQL::TType*, NUdf::TUnboxedValue>;
using TNamedUnboxedValue = std::pair<const TString, TTypedUnboxedValue>;

using TUnboxedParamsMap = absl::flat_hash_map<
    TString,
    TTypedUnboxedValue,
    std::hash<TString>,
    std::equal_to<TString>,
    NKikimr::NMiniKQL::TMKQLAllocator<TNamedUnboxedValue>
>;

struct TPartitionParam {
    NKikimr::NMiniKQL::TType *ItemType;
    NYql::NDqProto::TData Data;
    NKikimr::NMiniKQL::TUnboxedValueVector Values;

    TPartitionParam(NKikimr::NMiniKQL::TType *itemType,
        NKikimr::NMiniKQL::TUnboxedValueVector&& values)
      : ItemType(itemType)
      , Values(std::move(values))
    {}
};

using TPartitionedParamWithKey = std::pair<const std::pair<ui64, TString>, TPartitionParam>;

using TPartitionedParamMap = absl::flat_hash_map<
    std::pair<ui64, TString>,
    TPartitionParam,
    THash<std::pair<ui64, TString>>
>;

using TTypedUnboxedValueVector = std::vector<
    TTypedUnboxedValue,
    NKikimr::NMiniKQL::TMKQLAllocator<TTypedUnboxedValue>
>;

using TTxResultVector = std::vector<
    TTypedUnboxedValueVector,
    NKikimr::NMiniKQL::TMKQLAllocator<TTypedUnboxedValueVector>
>;

struct TKqpExecuterTxResult {
    bool IsStream = true;
    NKikimr::NMiniKQL::TType* MkqlItemType;
    const TVector<ui32>* ColumnOrder = nullptr;
    TMaybe<ui32> QueryResultIndex = 0;
    NKikimr::NMiniKQL::TUnboxedValueBatch Rows;
    Ydb::ResultSet TrailingResult;
    bool HasTrailingResult = false;

    explicit TKqpExecuterTxResult(
        bool isStream,
        NKikimr::NMiniKQL::TType* mkqlItemType,
        const TVector<ui32>* сolumnOrder,
        const TMaybe<ui32>& queryResultIndex)
        : IsStream(isStream)
        , MkqlItemType(mkqlItemType)
        , ColumnOrder(сolumnOrder)
        , QueryResultIndex(queryResultIndex)
    {}

    TTypedUnboxedValue GetUV(const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
        const NKikimr::NMiniKQL::THolderFactory& factory);
    NKikimrMiniKQL::TResult* GetMkql(google::protobuf::Arena* arena);
    NKikimrMiniKQL::TResult GetMkql();
    Ydb::ResultSet* GetYdb(google::protobuf::Arena* arena, TMaybe<ui64> rowsLimitPerWrite);
    Ydb::ResultSet* ExtractTrailingYdb(google::protobuf::Arena* arena);

    void FillMkql(NKikimrMiniKQL::TResult* mkqlResult);
    void FillYdb(Ydb::ResultSet* ydbResult, TMaybe<ui64> rowsLimitPerWrite);
};

struct TTimeAndRandomProvider {
    TIntrusivePtr<ITimeProvider> TimeProvider;
    TIntrusivePtr<IRandomProvider> RandomProvider;

    std::optional<ui64> CachedNow;
    std::tuple<std::optional<ui64>, std::optional<double>, std::optional<TGUID>> CachedRandom;

    ui64 GetCachedNow() {
        if (!CachedNow) {
            CachedNow = TimeProvider->Now().GetValue();
        }

        return *CachedNow;
    }

    ui64 GetCachedDate() {
        return std::min<ui64>(NYql::NUdf::MAX_DATE - 1u, GetCachedNow() / 86400000000ul);
    }

    ui64 GetCachedDatetime() {
        return std::min<ui64>(NYql::NUdf::MAX_DATETIME - 1u, GetCachedNow() / 1000000ul);
    }

    ui64 GetCachedTimestamp() {
        return std::min<ui64>(NYql::NUdf::MAX_TIMESTAMP - 1u, GetCachedNow());
    }

    template <typename T>
    T GetRandom() const {
        if constexpr (std::is_same_v<T, double>) {
            return RandomProvider->GenRandReal2();
        }
        if constexpr (std::is_same_v<T, ui64>) {
            return RandomProvider->GenRand64();
        }
        if constexpr (std::is_same_v<T, TGUID>) {
            return RandomProvider->GenUuid4();
        }
    }

    template <typename T>
    T GetCachedRandom() {
        auto& cached = std::get<std::optional<T>>(CachedRandom);
        if (!cached) {
            cached = GetRandom<T>();
        }

        return *cached;
    }

    void Reset() {
        CachedNow.reset();
        std::get<0>(CachedRandom).reset();
        std::get<1>(CachedRandom).reset();
        std::get<2>(CachedRandom).reset();
    }
};

class TTxAllocatorState: public TTimeAndRandomProvider {
public:
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    NKikimr::NMiniKQL::TTypeEnvironment TypeEnv;
    NKikimr::NMiniKQL::TMemoryUsageInfo MemInfo;
    NKikimr::NMiniKQL::THolderFactory HolderFactory;

    using TPtr = std::shared_ptr<TTxAllocatorState>;

    TTxAllocatorState(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        TIntrusivePtr<ITimeProvider> timeProvider, TIntrusivePtr<IRandomProvider> randomProvider);
    ~TTxAllocatorState();
    std::pair<NKikimr::NMiniKQL::TType*, NUdf::TUnboxedValue> GetInternalBindingValue(const NKqpProto::TKqpPhyParamBinding& paramBinding);
};

class TQueryData : NMiniKQL::ITerminator {
private:
    using TTypedUnboxedValue = std::pair<NKikimr::NMiniKQL::TType*, NUdf::TUnboxedValue>;
    using TNamedUnboxedValue = std::pair<const TString, TTypedUnboxedValue>;

    using TParamMap = absl::flat_hash_map<
        TString,
        NKikimrMiniKQL::TParams
    >;

    using TParamProtobufMap = ::google::protobuf::Map<
        TString,
        Ydb::TypedValue
    >;

    using TParamProvider = std::function<
        bool(std::string_view name, NKikimr::NMiniKQL::TType* type, const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
            const NKikimr::NMiniKQL::THolderFactory& holderFactory, NUdf::TUnboxedValue& value)
    >;

    TParamMap Params;
    TParamProtobufMap ParamsProtobuf;
    TUnboxedParamsMap UnboxedData;
    THashMap<ui32, TVector<TKqpExecuterTxResult>> TxResults;
    TVector<TVector<TKqpPhyTxHolder::TConstPtr>> TxHolders;
    TTxAllocatorState::TPtr AllocState;
    mutable TPartitionedParamMap PartitionedParams;

public:
    using TPtr = std::shared_ptr<TQueryData>;

    TQueryData(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        TIntrusivePtr<ITimeProvider> timeProvider, TIntrusivePtr<IRandomProvider> randomProvider);
    TQueryData(TTxAllocatorState::TPtr allocatorState);
    ~TQueryData();

    const TParamMap& GetParams();

    const TParamProtobufMap& GetParamsProtobuf();

    const NKikimr::NMiniKQL::TTypeEnvironment& TypeEnv();

    TTxAllocatorState::TPtr GetAllocState() { return AllocState; }
    NKikimr::NMiniKQL::TType* GetParameterType(const TString& name);
    bool AddUVParam(const TString& name, NKikimr::NMiniKQL::TType* type, const NUdf::TUnboxedValue& value);
    bool AddMkqlParam(const TString& name, const NKikimrMiniKQL::TType& t, const NKikimrMiniKQL::TValue& v);
    bool AddTypedValueParam(const TString& name, const Ydb::TypedValue& p);

    bool MaterializeParamValue(bool ensure, const NKqpProto::TKqpPhyParamBinding& paramBinding);
    void AddTxResults(ui32 txIndex, TVector<TKqpExecuterTxResult>&& results);
    void AddTxHolders(TVector<TKqpPhyTxHolder::TConstPtr>&& holders);

    bool AddShardParam(ui64 shardId, const TString& name, NKikimr::NMiniKQL::TType* type, NKikimr::NMiniKQL::TUnboxedValueVector&& value);
    void ClearPrunedParams();
    NYql::NDqProto::TData GetShardParam(ui64 shardId, const TString& name);

    bool HasResult(ui32 txIndex, ui32 resultIndex) {
        if (!TxResults.contains(txIndex))
            return false;

        return resultIndex < TxResults[txIndex].size();
    }

    void ValidateParameter(const TString& name, const NKikimrMiniKQL::TType& type, NMiniKQL::TTypeEnvironment& txTypeEnv);
    void PrepareParameters(const TKqpPhyTxHolder::TConstPtr& tx, const TPreparedQueryHolder::TConstPtr& preparedQuery,
        NMiniKQL::TTypeEnvironment& txTypeEnv);
    void CreateKqpValueMap(const TKqpPhyTxHolder::TConstPtr& tx);

    void ParseParameters(const google::protobuf::Map<TBasicString<char>, Ydb::TypedValue>& params);
    void ParseParameters(const NKikimrMiniKQL::TParams& parameters);

    TTypedUnboxedValue GetTxResult(ui32 txIndex, ui32 resultIndex);
    NKikimrMiniKQL::TResult* GetMkqlTxResult(const NKqpProto::TKqpPhyResultBinding& rb, google::protobuf::Arena* arena);
    Ydb::ResultSet* GetYdbTxResult(const NKqpProto::TKqpPhyResultBinding& rb, google::protobuf::Arena* arena, TMaybe<ui64> rowsLimitPerWrite);
    Ydb::ResultSet* ExtractTrailingTxResult(const NKqpProto::TKqpPhyResultBinding& rb, google::protobuf::Arena* arena);

    std::pair<NKikimr::NMiniKQL::TType*, NUdf::TUnboxedValue> GetInternalBindingValue(const NKqpProto::TKqpPhyParamBinding& paramBinding);
    TTypedUnboxedValue& GetParameterUnboxedValue(const TString& name);
    TTypedUnboxedValue* GetParameterUnboxedValuePtr(const TString& name);
    const NKikimrMiniKQL::TParams* GetParameterMiniKqlValue(const TString& name);
    const Ydb::TypedValue* GetParameterTypedValue(const TString& name);

    NYql::NDqProto::TData SerializeParamValue(const TString& name);
    void Clear();

    static TParamProvider GetParameterProvider(const std::shared_ptr<TQueryData>& queryData) {
        return [queryData](std::string_view name, NMiniKQL::TType* type,
            const NMiniKQL::TTypeEnvironment& typeEnv, const NMiniKQL::THolderFactory& holderFactory,
            NUdf::TUnboxedValue& value)
        {
            Y_UNUSED(typeEnv);
            Y_UNUSED(holderFactory);

            if (TTypedUnboxedValue* param = queryData->GetParameterUnboxedValuePtr(TString(name))) {
                std::tie(type, value) = *param;
                return true;
            }


            return false;
        };
    }

    void Terminate(const char* message) const final;
};



} // namespace NKikimr::NKqp
