#include "pg_compat.h"
#include "arrow.h"
#include "arrow_impl.h"
#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/arrow.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/utils.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/dynumber/dynumber.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>
#include <util/generic/singleton.h>

#include <arrow/compute/cast.h>
#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <util/system/mutex.h>

extern "C" {
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/fmgrprotos.h"
}

namespace NYql {

extern "C" {
Y_PRAGMA_DIAGNOSTIC_PUSH
Y_PRAGMA("GCC diagnostic ignored \"-Wreturn-type-c-linkage\"")
#include "pg_kernels_fwd.inc"
Y_PRAGMA_DIAGNOSTIC_POP
}

struct TExecs {
    static TExecs& Instance() {
        return *Singleton<TExecs>();
    }

    TExecs();
    THashMap<Oid, TExecFunc> Table;
};

TExecFunc FindExec(Oid oid) {
    const auto& table = TExecs::Instance().Table;
    auto it = table.find(oid);
    if (it == table.end()) {
        return nullptr;
    }

    return it->second;
}

bool HasPgKernel(ui32 procOid) {
    return FindExec(procOid) != nullptr;
}

TExecs::TExecs()
{
#define RegisterExec(oid, func) Table[oid] = func
#include "pg_kernels_register.all.inc"
#undef RegisterExec
}

const NPg::TAggregateDesc& ResolveAggregation(const TString& name, NKikimr::NMiniKQL::TTupleType* tupleType, const std::vector<ui32>& argsColumns, NKikimr::NMiniKQL::TType* returnType) {
    using namespace NKikimr::NMiniKQL;
    if (returnType) {
        MKQL_ENSURE(argsColumns.size() == 1, "Expected one column");
        TType* stateType = AS_TYPE(TBlockType, tupleType->GetElementType(argsColumns[0]))->GetItemType();
        TType* returnItemType = AS_TYPE(TBlockType, returnType)->GetItemType();
        return NPg::LookupAggregation(name, AS_TYPE(TPgType, stateType)->GetTypeId(), AS_TYPE(TPgType, returnItemType)->GetTypeId());
    } else {
        TVector<ui32> argTypeIds;
        for (const auto col : argsColumns) {
            argTypeIds.push_back(AS_TYPE(TPgType, AS_TYPE(TBlockType, tupleType->GetElementType(col))->GetItemType())->GetTypeId());
        }

        return NPg::LookupAggregation(name, argTypeIds);
    }
}

std::shared_ptr<arrow::Array> PgConvertBool(const std::shared_ptr<arrow::Array>& value) {
    const auto& data = value->data();
    size_t length = data->length;
    NUdf::TFixedSizeArrayBuilder<ui64, false> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *arrow::default_memory_pool(), length);
    auto input = data->GetValues<ui8>(1, 0);
    builder.UnsafeReserve(length);    
    auto output = builder.MutableData();
    for (size_t i = 0; i < length; ++i) {
        auto fullIndex = i + data->offset;
        output[i] = BoolGetDatum(arrow::BitUtil::GetBit(input, fullIndex));
    }

    auto dataBuffer = builder.Build(true).array()->buffers[1];
    return arrow::MakeArray(arrow::ArrayData::Make(arrow::uint64(), length, { data->buffers[0], dataBuffer }));
}

template <typename T, typename F>
std::shared_ptr<arrow::Array> PgConvertFixed(const std::shared_ptr<arrow::Array>& value, const F& f) {
    const auto& data = value->data();
    size_t length = data->length;
    NUdf::TFixedSizeArrayBuilder<ui64, false> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *arrow::default_memory_pool(), length);
    auto input = data->GetValues<T>(1);
    builder.UnsafeReserve(length);
    auto output = builder.MutableData();
    for (size_t i = 0; i < length; ++i) {
        output[i] = f(input[i]);
    }

    auto dataBuffer = builder.Build(true).array()->buffers[1];
    return arrow::MakeArray(arrow::ArrayData::Make(arrow::uint64(), length, { data->buffers[0], dataBuffer }));
}

template <bool IsCString>
std::shared_ptr<arrow::Array> PgConvertString(const std::shared_ptr<arrow::Array>& value) {
    const auto& data = value->data();
    size_t length = data->length;
    arrow::BinaryBuilder builder;
    ARROW_OK(builder.Reserve(length));
    auto inputDataSize = arrow::BinaryArray(data).total_values_length();
    ARROW_OK(builder.ReserveData(inputDataSize + length * (sizeof(void*) + (IsCString ? 1 : VARHDRSZ))));
    NUdf::TStringBlockReader<arrow::BinaryType, true> reader;
    std::vector<char> tmp;
    for (size_t i = 0; i < length; ++i) {
        auto item = reader.GetItem(*data, i);
        if (!item) {
            ARROW_OK(builder.AppendNull());
            continue;
        }

        auto originalLen = item.AsStringRef().Size();
        ui32 len;
        if constexpr (IsCString) {
            len = sizeof(void*) + 1 + originalLen;
        } else {
            len = sizeof(void*) + VARHDRSZ + originalLen;
        }

        if (Y_UNLIKELY(len < originalLen)) {
            ythrow yexception() << "Too long string";
        }

        if (tmp.capacity() < len) {
            tmp.reserve(Max<ui64>(len, tmp.capacity() * 2));
        }

        tmp.resize(len);
        NUdf::ZeroMemoryContext(tmp.data() + sizeof(void*));
        if constexpr (IsCString) {
            memcpy(tmp.data() + sizeof(void*), item.AsStringRef().Data(), originalLen);
            tmp[len - 1] = 0;
        } else {
            memcpy(tmp.data() + sizeof(void*) + VARHDRSZ, item.AsStringRef().Data(), originalLen);
            UpdateCleanVarSize((text*)(tmp.data() + sizeof(void*)), originalLen);
        }

        ARROW_OK(builder.Append(tmp.data(), len));
    }

    std::shared_ptr<arrow::BinaryArray> ret;
    ARROW_OK(builder.Finish(&ret));
    return ret;
}

Numeric Uint64ToPgNumeric(ui64 value) {
    if (value <= (ui64)Max<i64>()) {
        return int64_to_numeric((i64)value);
    }

    auto ret1 = int64_to_numeric((i64)(value & ~(1ull << 63)));
    auto bit = int64_to_numeric(Min<i64>());
    bool haveError = false;
    auto ret2 = numeric_sub_opt_error(ret1, bit, &haveError);
    Y_ENSURE(!haveError);
    pfree(ret1);
    pfree(bit);
    return ret2;
}

Numeric DecimalToPgNumeric(const NUdf::TUnboxedValuePod& value, ui8 precision, ui8 scale) {
    const auto str = NYql::NDecimal::ToString(value.GetInt128(), precision, scale);
    Y_ENSURE(str);
    return (Numeric)DirectFunctionCall3Coll(numeric_in, DEFAULT_COLLATION_OID, 
        PointerGetDatum(str), Int32GetDatum(0), Int32GetDatum(-1));
}

Numeric DyNumberToPgNumeric(const NUdf::TUnboxedValuePod& value) {
    auto str = NKikimr::NDyNumber::DyNumberToString(value.AsStringRef());
    Y_ENSURE(str);
    return (Numeric)DirectFunctionCall3Coll(numeric_in, DEFAULT_COLLATION_OID,
        PointerGetDatum(str->c_str()), Int32GetDatum(0), Int32GetDatum(-1));
}

Numeric PgFloatToNumeric(double item, ui64 scale, int digits) {
    double intPart, fracPart;
    bool error;
    fracPart = modf(item, &intPart);
    i64 fracInt = round(fracPart * scale);

    // scale compaction: represent 711.56000 as 711.56
    while (digits > 0 && fracInt % 10 == 0) {
        fracInt /= 10;
        digits -= 1;
    }

    if (digits == 0) {
        return int64_to_numeric(intPart);
    } else {
        return numeric_add_opt_error(
            int64_to_numeric(intPart),
            int64_div_fast_to_numeric(fracInt, digits),
            &error);
    }
}

std::shared_ptr<arrow::Array> PgDecimal128ConvertNumeric(const std::shared_ptr<arrow::Array>& value, int32_t precision, int32_t scale) {
    TArenaMemoryContext arena;
    const auto& data = value->data();
    size_t length = data->length;
    arrow::BinaryBuilder builder;

    bool error;
    Numeric high_bits_mul = numeric_mul_opt_error(int64_to_numeric(int64_t(1) << 62), int64_to_numeric(4), &error);

    auto input = data->GetValues<arrow::Decimal128>(1);
    for (size_t i = 0; i < length; ++i) {
        if (value->IsNull(i)) {
            ARROW_OK(builder.AppendNull());
            continue;
        }

        Numeric v = PgDecimal128ToNumeric(input[i], precision, scale, high_bits_mul);
       
        auto datum = NumericGetDatum(v);
        auto ptr = (char*)datum;
        auto len = GetFullVarSize((const text*)datum);
        NUdf::ZeroMemoryContext(ptr);
        ARROW_OK(builder.Append(ptr - sizeof(void*), len + sizeof(void*)));  
    }

    std::shared_ptr<arrow::BinaryArray> ret;
    ARROW_OK(builder.Finish(&ret));
    return ret;
}

Numeric PgDecimal128ToNumeric(arrow::Decimal128 value, int32_t precision, int32_t scale, Numeric high_bits_mul) {
    uint64_t low_bits = value.low_bits();
    int64 high_bits = value.high_bits();

    if (low_bits > INT64_MAX){
        high_bits += 1;
    }

    bool error;
    Numeric low_bits_res  = int64_div_fast_to_numeric(low_bits, scale);
    Numeric high_bits_res = numeric_mul_opt_error(int64_div_fast_to_numeric(high_bits, scale), high_bits_mul, &error);
    MKQL_ENSURE(error == false, "Bad numeric multiplication.");
    
    Numeric res = numeric_add_opt_error(high_bits_res,  low_bits_res, &error);
    MKQL_ENSURE(error == false, "Bad numeric addition.");

    return res;
}

TColumnConverter BuildPgNumericColumnConverter(const std::shared_ptr<arrow::DataType>& originalType) {
    switch (originalType->id()) {
    case arrow::Type::INT16:
        return [](const std::shared_ptr<arrow::Array>& value) {
            return PgConvertNumeric<i16>(value);
        };
    case arrow::Type::INT32:
        return [](const std::shared_ptr<arrow::Array>& value) {
            return PgConvertNumeric<i32>(value);
        };
    case arrow::Type::INT64:
        return [](const std::shared_ptr<arrow::Array>& value) {
            return PgConvertNumeric<i64>(value);
        };
    case arrow::Type::FLOAT:
        return [](const std::shared_ptr<arrow::Array>& value) {
            return PgConvertNumeric<float>(value);
        };
    case arrow::Type::DOUBLE:
        return [](const std::shared_ptr<arrow::Array>& value) {
            return PgConvertNumeric<double>(value);
        };
    case arrow::Type::DECIMAL128: {
        auto decimal128Ptr = std::static_pointer_cast<arrow::Decimal128Type>(originalType);
        int32_t precision = decimal128Ptr->precision();
        int32_t scale     = decimal128Ptr->scale();
        return [precision, scale](const std::shared_ptr<arrow::Array>& value) {
            return PgDecimal128ConvertNumeric(value, precision, scale);
        };
    }
    default:
        return {};
    }
}

template <typename T, typename F>
TColumnConverter BuildPgFixedColumnConverter(const std::shared_ptr<arrow::DataType>& originalType, const F& f) {
    auto primaryType = NKikimr::NMiniKQL::GetPrimitiveDataType<T>();
    if (!originalType->Equals(*primaryType) && !arrow::compute::CanCast(*originalType, *primaryType)) {
        return {};
    }

    return [primaryType, originalType, f](const std::shared_ptr<arrow::Array>& value) {
        auto res = originalType->Equals(*primaryType) ? value : ARROW_RESULT(arrow::compute::Cast(*value, primaryType));
        return PgConvertFixed<T, F>(res, f);
    };
}

Datum MakePgDateFromUint16(ui16 value) {
    return DatumGetDateADT(UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE + value);
}

Datum MakePgTimestampFromInt64(i64 value) {
    return DatumGetTimestamp(USECS_PER_SEC * ((UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE) * SECS_PER_DAY + value));
}

TColumnConverter BuildPgColumnConverter(const std::shared_ptr<arrow::DataType>& originalType, NKikimr::NMiniKQL::TPgType* targetType) {
    switch (targetType->GetTypeId()) {
    case BOOLOID: {
        auto primaryType = arrow::boolean();
        if (!originalType->Equals(*primaryType) && !arrow::compute::CanCast(*originalType, *primaryType)) {
            return {};
        }

        return [primaryType, originalType](const std::shared_ptr<arrow::Array>& value) {
            auto res = originalType->Equals(*primaryType) ? value : ARROW_RESULT(arrow::compute::Cast(*value, primaryType));
            return PgConvertBool(res);
        };
    }
    case INT2OID: {
        return BuildPgFixedColumnConverter<i16>(originalType, [](auto value){ return Int16GetDatum(value); });
    }
    case INT4OID: {
        return BuildPgFixedColumnConverter<i32>(originalType, [](auto value){ return Int32GetDatum(value); });
    }    
    case INT8OID: {
        return BuildPgFixedColumnConverter<i64>(originalType, [](auto value){ return Int64GetDatum(value); });
    }
    case FLOAT4OID: {
        return BuildPgFixedColumnConverter<float>(originalType, [](auto value){ return Float4GetDatum(value); });
    }
    case FLOAT8OID: {
        return BuildPgFixedColumnConverter<double>(originalType, [](auto value){ return Float8GetDatum(value); });
    }
    case NUMERICOID: {
        return BuildPgNumericColumnConverter(originalType);
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID:
    case CSTRINGOID: {
        auto primaryType = (targetType->GetTypeId() == BYTEAOID) ? arrow::binary() : arrow::utf8();
        if (!arrow::compute::CanCast(*originalType, *primaryType)) {
            return {};
        }

        return [primaryType, originalType, isCString = NPg::LookupType(targetType->GetTypeId()).TypeLen == -2](const std::shared_ptr<arrow::Array>& value) {
            auto res = originalType->Equals(*primaryType) ? value : ARROW_RESULT(arrow::compute::Cast(*value, primaryType));
            if (isCString) {
                return PgConvertString<true>(res);
            } else {
                return PgConvertString<false>(res);
            }
        };
    }
    case DATEOID: {
        if (originalType->Equals(arrow::uint16())) {
            return [](const std::shared_ptr<arrow::Array>& value) {
                return PgConvertFixed<ui16>(value, [](auto value){ return MakePgDateFromUint16(value); });
            };
        } else if (originalType->Equals(arrow::date32())) {
            return [](const std::shared_ptr<arrow::Array>& value) {
                return PgConvertFixed<i32>(value, [](auto value){ return MakePgDateFromUint16(value); });
            };
        } else {
            return {};
        }
    }
    case TIMESTAMPOID: {
        if (originalType->Equals(arrow::int64())) {
            return [](const std::shared_ptr<arrow::Array>& value) {
                return PgConvertFixed<i64>(value, [](auto value){ return MakePgTimestampFromInt64(value); });
            };
        } else {
            return {};
        }
    }    
    }
    return {};
}

}
