#include "arrow.h"
#include <ydb/library/yql/parser/pg_wrapper/interface/arrow.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/utils.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <util/generic/singleton.h>

#include <arrow/compute/cast.h>
#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <util/system/mutex.h>

extern "C" {
#include "utils/date.h"
#include "utils/timestamp.h"
}

namespace NYql {

extern "C" {
#include "pg_kernels_fwd.inc"
}

struct TExecs {
    static TExecs& Instance() {
        return *Singleton<TExecs>();
    }

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

void RegisterExec(Oid oid, TExecFunc func) {
    auto& table = TExecs::Instance().Table;
    table[oid] = func;
}

bool HasPgKernel(ui32 procOid) {
    return FindExec(procOid) != nullptr;
}

namespace {
    TAtomicCounter Initialized = 0;
    TMutex InitializationLock;
}

void RegisterPgKernels() {
    if (Initialized.Val()) {
        return;
    }
    TGuard<TMutex> g(InitializationLock);
    if (Initialized.Val()) {
        return;
    }
#include "pg_kernels_register.0.inc"
#include "pg_kernels_register.1.inc"
#include "pg_kernels_register.2.inc"
#include "pg_kernels_register.3.inc"
    Initialized = 1;
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
            builder.AppendNull();
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
