#include "pg_compat.h"
#include "arrow.h"
#include "arrow_impl.h"
#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/arrow.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/utils.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/codec.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/dynumber/dynumber.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>
#include <util/generic/singleton.h>
#include <arrow/compute/cast.h>
#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <util/system/mutex.h>
#include <util/system/align.h>

extern "C" {
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/fmgrprotos.h"
#include "lib/stringinfo.h"
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

NUdf::TBlockItem BlockItemFromDatum(Datum datum, const NPg::TTypeDesc& desc, std::vector<char>& tmp) {
    if (desc.PassByValue) {
        return NUdf::TBlockItem((ui64)datum);
    }
    auto typeLen = desc.TypeLen;
    ui32 len;
    if (typeLen == -1) {
        len = GetFullVarSize((const text*)datum);
    } else if (typeLen == -2) {
        len = 1 + strlen((const char*)datum);
    } else {
        len = typeLen;
    }
    len += sizeof(void*);
    len = AlignUp<i32>(len, 8);
    tmp.resize(len);
    memcpy(tmp.data() + sizeof(void*), (const char*) datum, len);
    return NUdf::TBlockItem(std::string_view(tmp.data(), len));
}

NUdf::TBlockItem PgBlockItemFromNativeBinary(const TStringBuf binary, ui32 pgTypeId, std::vector<char>& tmp) {
    NKikimr::NMiniKQL::TPAllocScope call;
    StringInfoData stringInfo;
    stringInfo.data = (char*)binary.Data();
    stringInfo.len = binary.Size();
    stringInfo.maxlen = binary.Size();
    stringInfo.cursor = 0;

    const auto& typeInfo = NPg::LookupType(pgTypeId);
    auto typeIOParam = MakeTypeIOParam(typeInfo);
    auto receiveFuncId = typeInfo.ReceiveFuncId;
    if (typeInfo.TypeId == typeInfo.ArrayTypeId) {
        receiveFuncId = NPg::LookupProc("array_recv", { 0,0,0 }).ProcId;
    }

    {
        FmgrInfo finfo;
        Zero(finfo);
        Y_ENSURE(receiveFuncId);
        fmgr_info(receiveFuncId, &finfo);
        Y_ENSURE(!finfo.fn_retset);
        Y_ENSURE(finfo.fn_addr);
        Y_ENSURE(finfo.fn_nargs >= 1 && finfo.fn_nargs <= 3);
        LOCAL_FCINFO(callInfo, 3);
        Zero(*callInfo);
        callInfo->flinfo = &finfo;
        callInfo->nargs = 3;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { (Datum)&stringInfo, false };
        callInfo->args[1] = { ObjectIdGetDatum(typeIOParam), false };
        callInfo->args[2] = { Int32GetDatum(-1), false };

        auto x = finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        if (stringInfo.cursor != stringInfo.len) {
            TStringBuilder errMsg;
            errMsg << "Not all data has been consumed by 'recv' function: " << NPg::LookupProc(receiveFuncId).Name << ", data size: " << stringInfo.len << ", consumed size: " << stringInfo.cursor;
            UdfTerminate(errMsg.c_str());
        }
        return BlockItemFromDatum(x, typeInfo, tmp);
    }
}

template<typename T>
constexpr Datum FixedToDatum(T v) {
    if constexpr (std::is_same_v<T, bool>) {
        return BoolGetDatum(v);
    } else if constexpr (std::is_same_v<T, i16>) {
        return Int16GetDatum(v);
    } else if constexpr (std::is_same_v<T, i32>) {
        return Int32GetDatum(v);
    } else if constexpr (std::is_same_v<T, i64>) {
        return Int64GetDatum(v);
    } else if constexpr (std::is_same_v<T, float>) {
        return Float4GetDatum(v);
    } else if constexpr (std::is_same_v<T, double>) {
        return Float8GetDatum(v);
    }
}

template<bool Native, typename T>
class PgYsonFixedColumnReader : public IYsonBlockReaderWithNativeFlag<Native> {
public:
    NUdf::TBlockItem GetItem(TYsonReaderDetails& buf) override final {
        return this->GetNullableItem(buf);
    }

    NUdf::TBlockItem GetNotNull(TYsonReaderDetails& buf) override final {
        Datum val;
        if constexpr (std::is_same_v<T, bool>) {
            Y_ENSURE(buf.Current() == NYson::NDetail::FalseMarker || buf.Current() == NYson::NDetail::TrueMarker);
            val = FixedToDatum<T>(buf.Current() == NYson::NDetail::TrueMarker);
            buf.Next();
        } else if constexpr (std::is_integral_v<T>) {
            if constexpr (std::is_signed_v<T>) {
                Y_ENSURE(buf.Current() == NYson::NDetail::Int64Marker);
                buf.Next();
                val = FixedToDatum<T>(buf.ReadVarI64());
            } else {
                Y_ENSURE(buf.Current() == NYson::NDetail::Uint64Marker);
                buf.Next();
                val = FixedToDatum<T>(buf.ReadVarUI64());
            }
        } else {
            val = FixedToDatum<T>(buf.NextDouble());
        }
        return NUdf::TBlockItem(val);
    }
};

template<bool Native, bool IsCString, bool Fixed>
class PgYsonStringColumnReader : public IYsonBlockReaderWithNativeFlag<Native> {
public:
    PgYsonStringColumnReader() {
        static_assert(!Fixed, "PgYsonStringColumnReader ctor w/o typeLen available only on non-fixed types");
    }

    PgYsonStringColumnReader(i32 typeLen) : TypeSize_(typeLen) {
        static_assert(Fixed, "PgYsonStringColumnReader ctor with typeLen available only on fixed types");
    }

    NUdf::TBlockItem GetItem(TYsonReaderDetails& buf) override final {
        return this->GetNullableItem(buf);
    }

    NUdf::TBlockItem GetNotNull(TYsonReaderDetails& buf) override final {
        Y_ENSURE(buf.Current() == NYson::NDetail::StringMarker);
        buf.Next();
        const i32 originalLen = buf.ReadVarI32();
        auto res = buf.Data();
        buf.Skip(originalLen);

        ui32 len;
        if constexpr (IsCString) {
            len = 1 + originalLen + sizeof(void*);
        } else if constexpr (Fixed) {
            len = TypeSize_;
        } else {
            len = VARHDRSZ + originalLen +  + sizeof(void*);
        }

        if (Tmp_.capacity() < len) {
            Tmp_.reserve(Max<ui64>(len, Tmp_.capacity() * 2));
        }
        len = AlignUp<ui32>(len, 8);
        Tmp_.resize(len);
        if constexpr (IsCString) {
            memcpy(Tmp_.data() + sizeof(void*), res, originalLen);
            Tmp_[len - 1] = 0;
        } else if constexpr (Fixed) {
            memcpy(Tmp_.data() +  sizeof(void*), res, originalLen);
            memset(Tmp_.data() + originalLen + sizeof(void*), 0, len - originalLen);
        } else {
            memcpy(Tmp_.data() + VARHDRSZ + sizeof(void*), res, originalLen);
            UpdateCleanVarSize((text*)(Tmp_.data() + sizeof(void*)), originalLen);
        }
        return NUdf::TBlockItem(NUdf::TStringRef(Tmp_.data(), len));
    }
private:
    std::vector<char> Tmp_;
    i32 TypeSize_;
};

template<bool Native>
class PgYsonOtherColumnReader : public IYsonBlockReaderWithNativeFlag<Native> {
public:
    PgYsonOtherColumnReader(Oid typeId) : TypeId_(typeId) {}
    NUdf::TBlockItem GetItem(TYsonReaderDetails& buf) override final {
        return this->GetNullableItem(buf);
    }

    NUdf::TBlockItem GetNotNull(TYsonReaderDetails& buf) override final {
        Y_ENSURE(buf.Current() == NYson::NDetail::StringMarker);
        buf.Next();
        const i32 len = buf.ReadVarI32();
        auto ptr = buf.Data();
        buf.Skip(len);
        return PgBlockItemFromNativeBinary(TStringBuf(ptr, len), TypeId_, Tmp_);
    }
private:
    Oid TypeId_;
    std::vector<char> Tmp_;
};

template<bool Native>
std::unique_ptr<IYsonBlockReader> BuildPgYsonColumnReader(const NUdf::TPgTypeDescription& desc) {
    switch (desc.TypeId) {
    case BOOLOID: {
        return std::make_unique<PgYsonFixedColumnReader<Native, bool>>();
    }
    case INT2OID: {
        return std::make_unique<PgYsonFixedColumnReader<Native, i16>>();
    }
    case INT4OID: {
        return std::make_unique<PgYsonFixedColumnReader<Native, i32>>();
    }
    case INT8OID: {
        return std::make_unique<PgYsonFixedColumnReader<Native, i64>>();
    }
    case FLOAT4OID: {
        return std::make_unique<PgYsonFixedColumnReader<Native, float>>();
    }
    case FLOAT8OID: {
        return std::make_unique<PgYsonFixedColumnReader<Native, double>>();
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID:
    case CSTRINGOID: {
        auto typeLen = NPg::LookupType(desc.TypeId).TypeLen;
        if (typeLen == -2) {
            return std::make_unique<PgYsonStringColumnReader<Native, true, false>>();
        } else if (typeLen == -1) {
            return std::make_unique<PgYsonStringColumnReader<Native, false, false>>();
        } else {
            return std::make_unique<PgYsonStringColumnReader<Native, false, true>>(typeLen);
        }
    }
    default:
        return std::make_unique<PgYsonOtherColumnReader<Native>>(desc.TypeId);
    }
}

std::unique_ptr<IYsonBlockReader> BuildPgYsonColumnReader(bool Native, const NUdf::TPgTypeDescription& desc) {
    return BuildPgYsonColumnReader<true>(desc);
}

template<typename T, arrow::Type::type Expected, typename ArrType>
class PgTopLevelFixedColumnReader : public ITopLevelBlockReader {
public:
    using Fn = Datum(*)(const T&);
    PgTopLevelFixedColumnReader(std::unique_ptr<NKikimr::NUdf::IArrayBuilder>&& builder) : Builder_(std::move(builder)) {}

    arrow::Datum Convert(std::shared_ptr<arrow::ArrayData> data) override final {
        if (arrow::Type::DICTIONARY == data->type->id()) {
            auto valType = static_cast<const arrow::DictionaryType&>(*data->type).value_type();
            Y_ENSURE(Expected == valType->id());
            return ConvertDict(data);
        } else {
            Y_ENSURE(Expected == data->type->id());
            return ConvertNonDict(data);
        }
    }

    arrow::Datum ConvertNonDict(std::shared_ptr<arrow::ArrayData> data) {
        ArrType arr(data);
        if (arr.null_count()) {
            for (i64 i = 0; i < data->length; ++i) {
                if (arr.IsNull(i)) {
                    Builder_->Add(NUdf::TBlockItem{});
                } else {
                    Builder_->Add(NUdf::TBlockItem(FixedToDatum<T>(arr.Value(i))));
                }
            }
        } else {
            for (i64 i = 0; i < data->length; ++i) {
                Builder_->Add(NUdf::TBlockItem(FixedToDatum<T>(arr.Value(i))));
            }
        }
        return Builder_->Build(false);
    }

    arrow::Datum ConvertDict(std::shared_ptr<arrow::ArrayData> data) {
        arrow::DictionaryArray dict(data);
        auto values = data->dictionary->GetValues<T>(0);
        auto indices = dict.indices()->data()->GetValues<ui32>(1);
        if (dict.null_count()) {
            for (i64 i = 0; i < data->length; ++i) {
                if (dict.IsNull(i)) {
                    Builder_->Add(NUdf::TBlockItem{});
                } else {
                    Builder_->Add(NUdf::TBlockItem(FixedToDatum<T>(values[indices[i]])));
                }
            }
        } else {
            for (i64 i = 0; i < data->length; ++i) {
                Builder_->Add(NUdf::TBlockItem(FixedToDatum<T>(values[indices[i]])));
            }
        }
        return Builder_->Build(false);
    }
private:
    std::unique_ptr<NKikimr::NUdf::IArrayBuilder> Builder_;
};

template<bool IsCString, bool Fixed>
class PgTopLevelStringColumnReader : public ITopLevelBlockReader {
public:
    PgTopLevelStringColumnReader(std::unique_ptr<NKikimr::NUdf::IArrayBuilder>&& builder) : Builder_(std::move(builder)) {
        static_assert(!Fixed, "can't call PgTopLevelStringColumnReader without typeLen on fixed type");
    }

    PgTopLevelStringColumnReader(std::unique_ptr<NKikimr::NUdf::IArrayBuilder>&& builder, i32 typeLen) : Builder_(std::move(builder)), TypeSize_(typeLen) {
        static_assert(Fixed, "can't call PgTopLevelStringColumnReader with typeLen on non-fixed type");
    }

    constexpr NUdf::TBlockItem ConvertOnce(const uint8_t* res, size_t originalLen) {
        ui32 len;
        if constexpr (IsCString) {
            len = 1 + originalLen + sizeof(void*);
        } else if constexpr (Fixed) {
            len = TypeSize_;
        } else {
            len = VARHDRSZ + originalLen +  + sizeof(void*);
        }

        if (Tmp_.capacity() < len) {
            Tmp_.reserve(Max<ui64>(len, Tmp_.capacity() * 2));
        }
        len = AlignUp<ui32>(len, 8);
        Tmp_.resize(len);
        if constexpr (IsCString) {
            memcpy(Tmp_.data() + sizeof(void*), res, originalLen);
            Tmp_[len - 1] = 0;
        } else if constexpr (Fixed) {
            memcpy(Tmp_.data() +  sizeof(void*), res, originalLen);
            memset(Tmp_.data() + originalLen + sizeof(void*), 0, len - originalLen);
        } else {
            memcpy(Tmp_.data() + VARHDRSZ + sizeof(void*), res, originalLen);
            UpdateCleanVarSize((text*)(Tmp_.data() + sizeof(void*)), originalLen);
        }
        return NUdf::TBlockItem(NUdf::TStringRef(Tmp_.data(), len));
    }

    arrow::Datum Convert(std::shared_ptr<arrow::ArrayData> data) override final {
        if (arrow::Type::DICTIONARY == data->type->id()) {
            auto valType = static_cast<const arrow::DictionaryType&>(*data->type).value_type();
            Y_ENSURE(arrow::Type::BINARY == valType->id() || arrow::Type::STRING == valType->id());
            return ConvertDict(data);
        } else {
            if (arrow::Type::STRING == data->type->id()) {
                auto res = arrow::compute::Cast(data, std::make_shared<arrow::BinaryType>());
                Y_ENSURE(res.ok());
                data = res->array();
            }
            Y_ENSURE(arrow::Type::BINARY == data->type->id());
            return ConvertNonDict(data);
        }
    }

    arrow::Datum ConvertNonDict(std::shared_ptr<arrow::ArrayData> data) {
        arrow::BinaryArray arr(data);
        if (arr.null_count()) {
            for (i64 i = 0; i < data->length; ++i) {
                if (arr.IsNull(i)) {
                    Builder_->Add(NUdf::TBlockItem{});
                } else {
                    i32 len;
                    auto res = arr.GetValue(i, &len);
                    Builder_->Add(ConvertOnce(res, len));
                }
            }
        } else {
            for (i64 i = 0; i < data->length; ++i) {
                i32 len;
                auto res = arr.GetValue(i, &len);
                Builder_->Add(ConvertOnce(res, len));
            }
        }
        return Builder_->Build(false);
    }

    arrow::Datum ConvertDict(std::shared_ptr<arrow::ArrayData> data) {
        arrow::DictionaryArray dict(data);
        if (arrow::Type::STRING == data->dictionary->type->id()) {
            auto res = arrow::compute::Cast(data->dictionary, std::make_shared<arrow::BinaryType>());
            Y_ENSURE(res.ok());
            data->dictionary = res->array();
        }
        arrow::BinaryArray arr(data->dictionary);
        auto indices = dict.indices()->data()->GetValues<ui32>(1);
        if (dict.null_count()) {
            for (i64 i = 0; i < data->length; ++i) {
                if (dict.IsNull(i)) {
                    Builder_->Add(NUdf::TBlockItem{});
                } else {
                    i32 len;
                    auto res = arr.GetValue(indices[i], &len);
                    Builder_->Add(NUdf::TBlockItem(ConvertOnce(res, len)));
                }
            }
        } else {
            for (i64 i = 0; i < data->length; ++i) {
                i32 len;
                auto res = arr.GetValue(indices[i], &len);
                Builder_->Add(NUdf::TBlockItem(ConvertOnce(res, len)));
            }
        }
        return Builder_->Build(false);
    }
private:
    std::unique_ptr<NKikimr::NUdf::IArrayBuilder> Builder_;
    std::vector<char> Tmp_;
    i32 TypeSize_;
};

class PgTopLevelOtherColumnReader : public ITopLevelBlockReader {
public:
    PgTopLevelOtherColumnReader(std::unique_ptr<NKikimr::NUdf::IArrayBuilder>&& builder, Oid typeId) : Builder_(std::move(builder)), TypeId_(typeId) {}

    inline NUdf::TBlockItem ConvertOnce(const uint8_t* res, size_t len) {
        return PgBlockItemFromNativeBinary(TStringBuf(reinterpret_cast<const char*>(res), len), TypeId_, Tmp_);
    }

    arrow::Datum Convert(std::shared_ptr<arrow::ArrayData> data) override final {
        if (arrow::Type::DICTIONARY == data->type->id()) {
            auto valType = static_cast<const arrow::DictionaryType&>(*data->type).value_type();
            Y_ENSURE(arrow::Type::BINARY == valType->id() || arrow::Type::STRING == valType->id());
            return ConvertDict(data);
        } else {
            Y_ENSURE(arrow::Type::BINARY == data->type->id() || arrow::Type::STRING == data->type->id());
            return ConvertNonDict(data);
        }
    }

    arrow::Datum ConvertNonDict(std::shared_ptr<arrow::ArrayData> data) {
        arrow::BinaryArray arr(data);
        if (arr.null_count()) {
            for (i64 i = 0; i < data->length; ++i) {
                if (arr.IsNull(i)) {
                    Builder_->Add(NUdf::TBlockItem{});
                } else {
                    i32 len;
                    auto res = arr.GetValue(i, &len);
                    Builder_->Add(NUdf::TBlockItem(ConvertOnce(res, len)));
                }
            }
        } else {
            for (i64 i = 0; i < data->length; ++i) {
                i32 len;
                auto res = arr.GetValue(i, &len);
                Builder_->Add(NUdf::TBlockItem(ConvertOnce(res, len)));
            }
        }
        return Builder_->Build(false);
    }

    arrow::Datum ConvertDict(std::shared_ptr<arrow::ArrayData> data) {
        arrow::DictionaryArray dict(data);
        if (arrow::Type::STRING == data->dictionary->type->id()) {
            auto res = arrow::compute::Cast(data->dictionary, std::make_shared<arrow::BinaryType>());
            Y_ENSURE(res.ok());
            data->dictionary = res->array();
        }
        arrow::BinaryArray arr(data->dictionary);
        auto indices = dict.indices()->data()->GetValues<ui32>(1);
        if (dict.null_count()) {
            for (i64 i = 0; i < data->length; ++i) {
                if (dict.IsNull(i)) {
                    Builder_->Add(NUdf::TBlockItem{});
                } else {
                    i32 len;
                    auto res = arr.GetValue(indices[i], &len);
                    Builder_->Add(NUdf::TBlockItem(ConvertOnce(res, len)));
                }
            }
        } else {
            for (i64 i = 0; i < data->length; ++i) {
                i32 len;
                auto res = arr.GetValue(indices[i], &len);
                Builder_->Add(NUdf::TBlockItem(ConvertOnce(res, len)));
            }
        }
        return Builder_->Build(false);
    }
private:
    std::unique_ptr<NKikimr::NUdf::IArrayBuilder> Builder_;
    Oid TypeId_;
    std::vector<char> Tmp_;
};

std::unique_ptr<ITopLevelBlockReader> BuildPgTopLevelColumnReader(std::unique_ptr<NKikimr::NUdf::IArrayBuilder>&& builder,const  NKikimr::NMiniKQL::TPgType* targetType) {
    if (!targetType) {
        return {};
    }
    switch (targetType->GetTypeId()) {
    case BOOLOID: {
        return std::make_unique<PgTopLevelFixedColumnReader<bool, arrow::Type::BOOL, arrow::BooleanArray>>(std::move(builder));
    }
    case INT2OID: {
        return std::make_unique<PgTopLevelFixedColumnReader<i16, arrow::Type::INT16, arrow::Int16Array>>(std::move(builder));
    }
    case INT4OID: {
        return std::make_unique<PgTopLevelFixedColumnReader<i32, arrow::Type::INT32, arrow::Int32Array>>(std::move(builder));
    }
    case INT8OID: {
        return std::make_unique<PgTopLevelFixedColumnReader<i64, arrow::Type::INT64, arrow::Int64Array>>(std::move(builder));
    }
    case FLOAT4OID: {
        return std::make_unique<PgTopLevelFixedColumnReader<float, arrow::Type::DOUBLE, arrow::DoubleArray>>(std::move(builder));
    }
    case FLOAT8OID: {
        return std::make_unique<PgTopLevelFixedColumnReader<double, arrow::Type::DOUBLE, arrow::DoubleArray>>(std::move(builder));
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID:
    case CSTRINGOID: {
        auto typeLen = NPg::LookupType(targetType->GetTypeId()).TypeLen;
        if (typeLen == -2) {
            return std::make_unique<PgTopLevelStringColumnReader<true, false>>(std::move(builder));
        } else if (typeLen == -1) {
            return std::make_unique<PgTopLevelStringColumnReader<false, false>>(std::move(builder));
        } else {
            return std::make_unique<PgTopLevelStringColumnReader<false, true>>(std::move(builder), typeLen);
        }
    }
    default:
        return std::make_unique<PgTopLevelOtherColumnReader>(std::move(builder), targetType->GetTypeId());
    }
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
