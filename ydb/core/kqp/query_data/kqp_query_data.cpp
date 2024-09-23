#include "kqp_query_data.h"

#include <ydb/core/protos/kqp_physical.pb.h>

#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NKikimr::NKqp {

using namespace NKikimr::NMiniKQL;
using namespace NYql;
using namespace NYql::NUdf;

TTypedUnboxedValue TKqpExecuterTxResult::GetUV(
    const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
    const NKikimr::NMiniKQL::THolderFactory& factory)
{
    YQL_ENSURE(!Rows.IsWide());
    if (IsStream) {
        auto* listOfItemType = NKikimr::NMiniKQL::TListType::Create(MkqlItemType, typeEnv);

        NUdf::TUnboxedValue* itemsPtr = nullptr;
        auto value = factory.CreateDirectArrayHolder(Rows.RowCount(), itemsPtr);
        Rows.ForEachRow([&](NUdf::TUnboxedValue& value) {
            *itemsPtr++ = std::move(value);
        });
        return {listOfItemType, value};
    } else {
        YQL_ENSURE(Rows.RowCount() == 1, "Actual buffer size: " << Rows.RowCount());
        return {MkqlItemType, *Rows.Head()};
    }
}

NKikimrMiniKQL::TResult* TKqpExecuterTxResult::GetMkql(google::protobuf::Arena* arena) {
    NKikimrMiniKQL::TResult* mkqlResult = google::protobuf::Arena::CreateMessage<NKikimrMiniKQL::TResult>(arena);
    FillMkql(mkqlResult);
    return mkqlResult;
}

NKikimrMiniKQL::TResult TKqpExecuterTxResult::GetMkql() {
    NKikimrMiniKQL::TResult mkqlResult;
    FillMkql(&mkqlResult);
    return mkqlResult;
}

void TKqpExecuterTxResult::FillMkql(NKikimrMiniKQL::TResult* mkqlResult) {
    YQL_ENSURE(!Rows.IsWide());
    if (IsStream) {
        mkqlResult->MutableType()->SetKind(NKikimrMiniKQL::List);
        ExportTypeToProto(
            MkqlItemType,
            *mkqlResult->MutableType()->MutableList()->MutableItem(),
            ColumnOrder);

        Rows.ForEachRow([&](NUdf::TUnboxedValue& value) {
            ExportValueToProto(
                MkqlItemType, value, *mkqlResult->MutableValue()->AddList(),
                ColumnOrder);
        });
    } else {
        YQL_ENSURE(Rows.RowCount() == 1, "Actual buffer size: " << Rows.RowCount());
        ExportTypeToProto(MkqlItemType, *mkqlResult->MutableType());
        ExportValueToProto(MkqlItemType, *Rows.Head(), *mkqlResult->MutableValue());
    }
}

Ydb::ResultSet* TKqpExecuterTxResult::GetYdb(google::protobuf::Arena* arena, TMaybe<ui64> rowsLimitPerWrite) {
    Ydb::ResultSet* ydbResult = google::protobuf::Arena::CreateMessage<Ydb::ResultSet>(arena);
    FillYdb(ydbResult, rowsLimitPerWrite);
    return ydbResult;
}

Ydb::ResultSet* TKqpExecuterTxResult::ExtractTrailingYdb(google::protobuf::Arena* arena) {
    if (!HasTrailingResult)
        return nullptr;

    Ydb::ResultSet* ydbResult = google::protobuf::Arena::CreateMessage<Ydb::ResultSet>(arena);
    ydbResult->Swap(&TrailingResult);

    return ydbResult;
}


void TKqpExecuterTxResult::FillYdb(Ydb::ResultSet* ydbResult, TMaybe<ui64> rowsLimitPerWrite) {
    YQL_ENSURE(ydbResult);
    YQL_ENSURE(!Rows.IsWide());
    YQL_ENSURE(MkqlItemType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Struct);
    const auto* mkqlSrcRowStructType = static_cast<const TStructType*>(MkqlItemType);

    for (ui32 idx = 0; idx < mkqlSrcRowStructType->GetMembersCount(); ++idx) {
        auto* column = ydbResult->add_columns();
        ui32 memberIndex = (!ColumnOrder || ColumnOrder->empty()) ? idx : (*ColumnOrder)[idx];
        column->set_name(TString(mkqlSrcRowStructType->GetMemberName(memberIndex)));
        ExportTypeToProto(mkqlSrcRowStructType->GetMemberType(memberIndex), *column->mutable_type());
    }

    Rows.ForEachRow([&](const NUdf::TUnboxedValue& value) -> bool {
        if (rowsLimitPerWrite) {
            if (*rowsLimitPerWrite == 0) {
                ydbResult->set_truncated(true);
                return false;
            }
            --(*rowsLimitPerWrite);
        }
        ExportValueToProto(MkqlItemType, value, *ydbResult->add_rows(), ColumnOrder);
        return true;
    });
}


TTxAllocatorState::TTxAllocatorState(const IFunctionRegistry* functionRegistry,
    TIntrusivePtr<ITimeProvider> timeProvider, TIntrusivePtr<IRandomProvider> randomProvider)
    : Alloc(std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), functionRegistry->SupportsSizedAllocators()))
    , TypeEnv(*Alloc)
    , MemInfo("TQueryData")
    , HolderFactory(Alloc->Ref(), MemInfo, functionRegistry)
{
    Alloc->Release();
    TimeProvider = timeProvider;
    RandomProvider = randomProvider;
}

TTxAllocatorState::~TTxAllocatorState()
{
    Alloc->Acquire();
}

std::pair<NKikimr::NMiniKQL::TType*, NUdf::TUnboxedValue> TTxAllocatorState::GetInternalBindingValue(
    const NKqpProto::TKqpPhyParamBinding& paramBinding)
{
    auto& internalBinding = paramBinding.GetInternalBinding();
    switch (internalBinding.GetType()) {
        case NKqpProto::TKqpPhyInternalBinding::PARAM_NOW:
            return {TypeEnv.GetUi64Lazy(), TUnboxedValuePod(ui64(GetCachedNow()))};
        case NKqpProto::TKqpPhyInternalBinding::PARAM_CURRENT_DATE: {
            ui32 date = GetCachedDate();
            YQL_ENSURE(date <= Max<ui32>());
            return {TypeEnv.GetUi32Lazy(), TUnboxedValuePod(ui32(date))};
        }
        case NKqpProto::TKqpPhyInternalBinding::PARAM_CURRENT_DATETIME: {
            ui64 datetime = GetCachedDatetime();
            YQL_ENSURE(datetime <= Max<ui32>());
            return {TypeEnv.GetUi32Lazy(), TUnboxedValuePod(ui32(datetime))};
        }
        case NKqpProto::TKqpPhyInternalBinding::PARAM_CURRENT_TIMESTAMP:
            return {TypeEnv.GetUi64Lazy(), TUnboxedValuePod(ui64(GetCachedTimestamp()))};
        case NKqpProto::TKqpPhyInternalBinding::PARAM_RANDOM_NUMBER:
            return {TypeEnv.GetUi64Lazy(), TUnboxedValuePod(ui64(GetCachedRandom<ui64>()))};
        case NKqpProto::TKqpPhyInternalBinding::PARAM_RANDOM:
            return {NKikimr::NMiniKQL::TDataType::Create(NUdf::TDataType<double>::Id, TypeEnv),
                TUnboxedValuePod(double(GetCachedRandom<double>()))};
        case NKqpProto::TKqpPhyInternalBinding::PARAM_RANDOM_UUID: {
            auto uuid = GetCachedRandom<TGUID>();
            const auto ptr = reinterpret_cast<ui8*>(uuid.dw);
            union {
                ui64 half[2];
                char bytes[16];
            } buf;
            buf.half[0] = *reinterpret_cast<ui64*>(ptr);
            buf.half[1] = *reinterpret_cast<ui64*>(ptr + 8);
            return {NKikimr::NMiniKQL::TDataType::Create(NUdf::TDataType<NUdf::TUuid>::Id, TypeEnv), MakeString(TStringRef(buf.bytes, 16))};
        }
        default:
            YQL_ENSURE(false, "Unexpected internal parameter type: " << (ui32)internalBinding.GetType());
    }
}

TQueryData::TQueryData(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    TIntrusivePtr<ITimeProvider> timeProvider, TIntrusivePtr<IRandomProvider> randomProvider)
    : TQueryData(std::make_shared<TTxAllocatorState>(functionRegistry, timeProvider, randomProvider))
{
}

TQueryData::TQueryData(TTxAllocatorState::TPtr allocatorState)
    : AllocState(std::move(allocatorState))
{
}

TQueryData::~TQueryData() {
    {
        auto g = TypeEnv().BindAllocator();
        THashMap<ui32, TVector<TKqpExecuterTxResult>> emptyResultMap;
        TxResults.swap(emptyResultMap);
        TUnboxedParamsMap emptyMap;
        UnboxedData.swap(emptyMap);

        TPartitionedParamMap empty;
        empty.swap(PartitionedParams);
    }
}

const TQueryData::TParamMap& TQueryData::GetParams() {
    for(auto& [name, _] : UnboxedData) {
        GetParameterMiniKqlValue(name);
    }

    return Params;
}

const TQueryData::TParamProtobufMap& TQueryData::GetParamsProtobuf() {
    for(auto& [name, _] : UnboxedData) {
        GetParameterTypedValue(name);
    }

    return ParamsProtobuf;
}

NKikimr::NMiniKQL::TType* TQueryData::GetParameterType(const TString& name) {
    auto it = UnboxedData.find(name);
    if (it == UnboxedData.end()) {
        return nullptr;
    }

    return it->second.first;
}

std::pair<NKikimr::NMiniKQL::TType*, NUdf::TUnboxedValue> TQueryData::GetTxResult(ui32 txIndex, ui32 resultIndex) {
    return TxResults[txIndex][resultIndex].GetUV(
        TypeEnv(), AllocState->HolderFactory);
}

NKikimrMiniKQL::TResult* TQueryData::GetMkqlTxResult(const NKqpProto::TKqpPhyResultBinding& rb, google::protobuf::Arena* arena) {
    auto txIndex = rb.GetTxResultBinding().GetTxIndex();
    auto resultIndex = rb.GetTxResultBinding().GetResultIndex();

    YQL_ENSURE(HasResult(txIndex, resultIndex));
    auto g = TypeEnv().BindAllocator();
    return TxResults[txIndex][resultIndex].GetMkql(arena);
}

Ydb::ResultSet* TQueryData::ExtractTrailingTxResult(const NKqpProto::TKqpPhyResultBinding& rb, google::protobuf::Arena* arena) {
    auto txIndex = rb.GetTxResultBinding().GetTxIndex();
    auto resultIndex = rb.GetTxResultBinding().GetResultIndex();
    return TxResults[txIndex][resultIndex].ExtractTrailingYdb(arena);
}


Ydb::ResultSet* TQueryData::GetYdbTxResult(const NKqpProto::TKqpPhyResultBinding& rb, google::protobuf::Arena* arena, TMaybe<ui64> rowsLimitPerWrite) {
    auto txIndex = rb.GetTxResultBinding().GetTxIndex();
    auto resultIndex = rb.GetTxResultBinding().GetResultIndex();

    YQL_ENSURE(HasResult(txIndex, resultIndex));
    auto g = TypeEnv().BindAllocator();
    return TxResults[txIndex][resultIndex].GetYdb(arena, rowsLimitPerWrite);
}


void TQueryData::AddTxResults(ui32 txIndex, TVector<TKqpExecuterTxResult>&& results) {
    auto g = TypeEnv().BindAllocator();
    TxResults.emplace(std::make_pair(txIndex, std::move(results)));
}

void TQueryData::AddTxHolders(TVector<TKqpPhyTxHolder::TConstPtr>&& holders) {
    TxHolders.emplace_back(std::move(holders));
}

void TQueryData::ValidateParameter(const TString& name, const NKikimrMiniKQL::TType& type, NMiniKQL::TTypeEnvironment& txTypeEnv) {
    auto parameterType = GetParameterType(name);
    if (!parameterType) {
        if (type.GetKind() == NKikimrMiniKQL::ETypeKind::Optional) {
            NKikimrMiniKQL::TValue value;
            AddMkqlParam(name, type, value);
            return;
        }
        ythrow yexception() << "Missing value for parameter: " << name;
    }

    auto pType = ImportTypeFromProto(type, txTypeEnv);
    if (pType == nullptr || !parameterType->IsSameType(*pType)) {
        ythrow yexception() << "Parameter " << name
            << " type mismatch, expected: " << type << ", actual: " << *parameterType;
    }
}

void TQueryData::PrepareParameters(const TKqpPhyTxHolder::TConstPtr& tx, const TPreparedQueryHolder::TConstPtr& preparedQuery,
    NMiniKQL::TTypeEnvironment& txTypeEnv)
{
    for (const auto& paramDesc : preparedQuery->GetParameters()) {
        ValidateParameter(paramDesc.GetName(), paramDesc.GetType(), txTypeEnv);
    }

    for(const auto& paramBinding: tx->GetParamBindings()) {
        MaterializeParamValue(true, paramBinding);
    }
}

void TQueryData::CreateKqpValueMap(const TKqpPhyTxHolder::TConstPtr& tx) {
    for (const auto& paramBinding : tx->GetParamBindings()) {
        MaterializeParamValue(true, paramBinding);
    }
}

void TQueryData::ParseParameters(const google::protobuf::Map<TBasicString<char>, Ydb::TypedValue>& params) {
    for(const auto& [name, param] : params) {
        auto success = AddTypedValueParam(name, param);
        YQL_ENSURE(success, "Duplicate parameter: " << name);
    }
}

void TQueryData::ParseParameters(const NKikimrMiniKQL::TParams& parameters) {
    if (!parameters.HasType()) {
        return;
    }

    YQL_ENSURE(parameters.GetType().GetKind() == NKikimrMiniKQL::Struct, "Expected struct as query parameters type");
    auto& structType = parameters.GetType().GetStruct();
    for (ui32 i = 0; i < structType.MemberSize(); ++i) {
        const auto& memberName = structType.GetMember(i).GetName();
        YQL_ENSURE(i < parameters.GetValue().StructSize(), "Missing value for parameter: " << memberName);
        auto success = AddMkqlParam(memberName, structType.GetMember(i).GetType(), parameters.GetValue().GetStruct(i));
        YQL_ENSURE(success, "Duplicate parameter: " << memberName);
    }
}

bool TQueryData::AddUVParam(const TString& name, NKikimr::NMiniKQL::TType* type, const NUdf::TUnboxedValue& value) {
    auto g = TypeEnv().BindAllocator();
    auto [_, success] = UnboxedData.emplace(name, std::make_pair(type, value));
    return success;
}

bool TQueryData::AddTypedValueParam(const TString& name, const Ydb::TypedValue& param) {
    auto guard = TypeEnv().BindAllocator();
    const TBindTerminator bind(this);
    auto [typeFromProto, value] = ImportValueFromProto(
        param.type(), param.value(), TypeEnv(), AllocState->HolderFactory);
    return AddUVParam(name, typeFromProto, value);
}

bool TQueryData::AddMkqlParam(const TString& name, const NKikimrMiniKQL::TType& t, const NKikimrMiniKQL::TValue& v) {
    auto guard = TypeEnv().BindAllocator();
    auto [typeFromProto, value] = ImportValueFromProto(t, v, TypeEnv(), AllocState->HolderFactory);
    return AddUVParam(name, typeFromProto, value);
}

std::pair<NKikimr::NMiniKQL::TType*, NUdf::TUnboxedValue> TQueryData::GetInternalBindingValue(
    const NKqpProto::TKqpPhyParamBinding& paramBinding)
{
    return AllocState->GetInternalBindingValue(paramBinding);
}

TQueryData::TTypedUnboxedValue& TQueryData::GetParameterUnboxedValue(const TString& name) {
    auto it = UnboxedData.find(name);
    YQL_ENSURE(it != UnboxedData.end(), "Param " << name << " not found");
    return it->second;
}

TQueryData::TTypedUnboxedValue* TQueryData::GetParameterUnboxedValuePtr(const TString& name) {
    auto it = UnboxedData.find(name);
    if (it == UnboxedData.end()) {
        return nullptr;
    }

    return &it->second;
}

const NKikimrMiniKQL::TParams* TQueryData::GetParameterMiniKqlValue(const TString& name) {
    if (UnboxedData.find(name) == UnboxedData.end())
        return nullptr;

    auto it = Params.find(name);
    if (it == Params.end()) {
        with_lock(*AllocState->Alloc) {
            const auto& [type, uv] = GetParameterUnboxedValue(name);
            NKikimrMiniKQL::TParams param;
            ExportTypeToProto(type, *param.MutableType());
            ExportValueToProto(type, uv, *param.MutableValue());

            auto [nit, success] = Params.emplace(name, std::move(param));
            YQL_ENSURE(success);

            return &(nit->second);
        }
    }

    return &(it->second);
}

const Ydb::TypedValue* TQueryData::GetParameterTypedValue(const TString& name) {
    if (UnboxedData.find(name) == UnboxedData.end())
        return nullptr;

    auto it = ParamsProtobuf.find(name);
    if (it == ParamsProtobuf.end()) {
        with_lock(*AllocState->Alloc) {
            const auto& [type, uv] = GetParameterUnboxedValue(name);

            auto& tv = ParamsProtobuf[name];

            ExportTypeToProto(type, *tv.mutable_type());
            ExportValueToProto(type, uv, *tv.mutable_value());

            return &tv;
        }
    }

    return &(it->second);
}

const NKikimr::NMiniKQL::TTypeEnvironment& TQueryData::TypeEnv() {
    return AllocState->TypeEnv;
}

bool TQueryData::MaterializeParamValue(bool ensure, const NKqpProto::TKqpPhyParamBinding& paramBinding) {
    switch (paramBinding.GetTypeCase()) {
        case NKqpProto::TKqpPhyParamBinding::kExternalBinding: {
            const auto* clientParam = GetParameterType(paramBinding.GetName());
            if (clientParam) {
                return true;
            }
            Y_ENSURE(!ensure || clientParam, "Parameter not found: " << paramBinding.GetName());
            return false;
        }
        case NKqpProto::TKqpPhyParamBinding::kTxResultBinding: {
            auto& txResultBinding = paramBinding.GetTxResultBinding();
            auto txIndex = txResultBinding.GetTxIndex();
            auto resultIndex = txResultBinding.GetResultIndex();

            if (HasResult(txIndex, resultIndex)) {
                auto guard = TypeEnv().BindAllocator();
                auto [type, value] = GetTxResult(txIndex, resultIndex);
                AddUVParam(paramBinding.GetName(), type, value);
                return true;
            }

            if (ensure) {
                YQL_ENSURE(HasResult(txIndex, resultIndex));
            }
            return false;
        }
        case NKqpProto::TKqpPhyParamBinding::kInternalBinding: {
            auto guard = TypeEnv().BindAllocator();
            auto [type, value] = GetInternalBindingValue(paramBinding);
            AddUVParam(paramBinding.GetName(), type, value);
            return true;
        }
        default:
            YQL_ENSURE(false, "Unexpected parameter binding type: " << (ui32)paramBinding.GetTypeCase());
    }

    return false;
}

bool TQueryData::AddShardParam(ui64 shardId, const TString& name, NKikimr::NMiniKQL::TType* type, NKikimr::NMiniKQL::TUnboxedValueVector&& value) {
    auto guard = TypeEnv().BindAllocator();
    auto [it, inserted] = PartitionedParams.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(shardId, name),
        std::forward_as_tuple(type, std::move(value)));
    return inserted;
}

void TQueryData::ClearPrunedParams() {
    if (PartitionedParams.empty())
        return;

    auto guard = TypeEnv().BindAllocator();
    for(auto& [key, value]: PartitionedParams) {
        TUnboxedValueVector emptyVector;
        value.Values.swap(emptyVector);
    }

    TPartitionedParamMap emptyMap;
    emptyMap.swap(PartitionedParams);
}

NDqProto::TData TQueryData::GetShardParam(ui64 shardId, const TString& name) {
    auto kv = std::make_pair(shardId, name);
    auto it = PartitionedParams.find(kv);
    if (it == PartitionedParams.end()) {
        return SerializeParamValue(name);
    }

    auto guard = TypeEnv().BindAllocator();
    NDq::TDqDataSerializer dataSerializer{AllocState->TypeEnv, AllocState->HolderFactory, NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_PICKLE_1_0};
    NDq::TDqSerializedBatch batch = dataSerializer.Serialize(it->second.Values.begin(), it->second.Values.end(), it->second.ItemType);
    YQL_ENSURE(!batch.IsOOB());
    return batch.Proto;
}

NDqProto::TData TQueryData::SerializeParamValue(const TString& name) {
    auto guard = TypeEnv().BindAllocator();
    const auto& [type, value] = GetParameterUnboxedValue(name);
    return NDq::TDqDataSerializer::SerializeParamValue(type, value);
}

void TQueryData::Clear() {
    {
        auto g = TypeEnv().BindAllocator();
        Params.clear();
        TUnboxedParamsMap emptyMap;
        UnboxedData.swap(emptyMap);

        THashMap<ui32, TVector<TKqpExecuterTxResult>> emptyResultMap;
        TxResults.swap(emptyResultMap);

        for(auto& [key, param]: PartitionedParams) {
            NKikimr::NMiniKQL::TUnboxedValueVector emptyValues;
            param.Values.swap(emptyValues);
        }

        PartitionedParams.clear();

        AllocState->Reset();
    }
}

void TQueryData::Terminate(const char* message) const {
    TStringBuf reason = (message ? TStringBuf(message) : TStringBuf("(unknown)"));
    TString fullMessage = TStringBuilder() <<
        "Terminate was called, reason(" << reason.size() << "): " << reason << Endl;
    AllocState->HolderFactory.CleanupModulesOnTerminate();
    if (std::current_exception()) {
        throw;
    }

    ythrow yexception() << fullMessage;
}

} // namespace NKikimr::NKqp
