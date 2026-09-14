#include "stable_pickle.h"

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <tuple>

namespace NKikimr::NStat {

using namespace NKikimr::NMiniKQL;

namespace {

// (Type, Nullable, DecimalPrecision, DecimalScale) per column — identifies the tuple type shape so
// the built tuple type and packer can be cached and reused across Pack() calls with the same schema.
using TColumnSignature = std::tuple<NScheme::TTypeId, bool, ui8, ui8>;

TColumnSignature MakeSignature(const TPickleColumnValue& col) {
    return {col.Type, col.Nullable, col.DecimalPrecision, col.DecimalScale};
}

TType* MakeElementType(const TPickleColumnValue& col, const TTypeEnvironment& env) {
    TType* dataType = col.Type == NScheme::NTypeIds::Decimal
        ? static_cast<TType*>(TDataDecimalType::Create(col.DecimalPrecision, col.DecimalScale, env))
        : static_cast<TType*>(TDataType::Create(col.Type, env));
    return col.Nullable ? static_cast<TType*>(TOptionalType::Create(dataType, env)) : dataType;
}

// Builds the MiniKQL value for one column. Must be called with the scoped allocator acquired.
NUdf::TUnboxedValue MakeElementValue(const TPickleColumnValue& col) {
    if (col.Nullable && !col.Value) {
        return NUdf::TUnboxedValuePod(); // empty Optional == NULL
    }

    NUdf::TUnboxedValue value;
    if (col.Type == NScheme::NTypeIds::Decimal) {
        value = NUdf::TUnboxedValuePod(
            NYql::NDecimal::FromString(*col.Value, col.DecimalPrecision, col.DecimalScale));
    } else {
        value = ValueFromString(NUdf::GetDataSlot(col.Type), *col.Value);
    }

    return col.Nullable ? value.Release().MakeOptional() : value;
}

} // anonymous namespace

struct TStablePickleTupleBuilder::TImpl {
    TScopedAlloc Alloc;
    TTypeEnvironment Env;
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;

    std::vector<TColumnSignature> CachedSignature;
    TType* CachedTupleType = nullptr;
    std::unique_ptr<TValuePacker> Packer;

    TImpl()
        : Alloc(__LOCATION__)
        , Env(Alloc)
        , MemInfo("StablePickleTuple")
        , HolderFactory(Alloc.Ref(), MemInfo)
    {}

    void EnsurePacker(const std::vector<TPickleColumnValue>& columns) {
        std::vector<TColumnSignature> signature;
        signature.reserve(columns.size());
        for (const auto& col : columns) {
            signature.push_back(MakeSignature(col));
        }
        if (Packer && signature == CachedSignature) {
            return;
        }

        std::vector<TType*> elementTypes;
        elementTypes.reserve(columns.size());
        for (const auto& col : columns) {
            elementTypes.push_back(MakeElementType(col, Env));
        }
        CachedTupleType = TTupleType::Create(elementTypes.size(), elementTypes.data(), Env);
        Packer = std::make_unique<TValuePacker>(/*stable=*/true, CachedTupleType);
        CachedSignature = std::move(signature);
    }

    TString Pack(const std::vector<TPickleColumnValue>& columns) {
        EnsurePacker(columns);

        TUnboxedValueVector values;
        values.reserve(columns.size());
        for (const auto& col : columns) {
            values.emplace_back(MakeElementValue(col));
        }

        NUdf::TUnboxedValue tuple = HolderFactory.VectorAsArray(values);
        return TString(Packer->Pack(tuple));
    }
};

TStablePickleTupleBuilder::TStablePickleTupleBuilder()
    : Impl(std::make_unique<TImpl>())
{}

TStablePickleTupleBuilder::~TStablePickleTupleBuilder() = default;

TString TStablePickleTupleBuilder::Pack(const std::vector<TPickleColumnValue>& columns) {
    return Impl->Pack(columns);
}

TString StablePickleTuple(const std::vector<TPickleColumnValue>& columns) {
    return TStablePickleTupleBuilder().Pack(columns);
}

} // namespace NKikimr::NStat
