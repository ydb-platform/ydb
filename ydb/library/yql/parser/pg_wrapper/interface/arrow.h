#pragma once
#include <ydb/library/yql/providers/yt/comp_nodes/dq/arrow_converter.h>

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/public/udf/arrow/block_item.h>
#include <arrow/datum.h>

namespace NYql {

arrow::Datum MakePgScalar(NKikimr::NMiniKQL::TPgType* type, const NKikimr::NUdf::TUnboxedValuePod& value, arrow::MemoryPool& pool);
arrow::Datum MakePgScalar(NKikimr::NMiniKQL::TPgType* type, const NUdf::TBlockItem& value, arrow::MemoryPool& pool);

using TColumnConverter = std::function<std::shared_ptr<arrow::Array>(const std::shared_ptr<arrow::Array>&)>;
TColumnConverter BuildPgColumnConverter(const std::shared_ptr<arrow::DataType>& originalType, NKikimr::NMiniKQL::TPgType* targetType);

class TYsonReaderDetails {
public:
    TYsonReaderDetails(const std::string_view& s) : Data_(s.data()), Available_(s.size()) {}

    constexpr char Next() {
        return *(++Data_);
    }

    constexpr char Current() {
        return *Data_;
    }

    template<typename T>
    constexpr T ReadVarSlow() {
        T shift = 0;
        T value = Current() & 0x7f;
        for (;;) {
            shift += 7;
            value |= T(Next() & 0x7f) << shift;
            if (!(Current() & 0x80)) {
                break;
            }
        }
        Next();
        return value;
    }

    ui32 ReadVarUI32() {
        char prev = Current();
        if (Y_LIKELY(!(prev & 0x80))) {
            Next();
            return prev;
        }

        return ReadVarSlow<ui32>();
    }

    ui64 ReadVarUI64() {
        char prev = Current();
        if (Y_LIKELY(!(prev & 0x80))) {
            Next();
            return prev;
        }

        return ReadVarSlow<ui64>();
    }

    i32 ReadVarI32() {
        return NYson::ZigZagDecode32(ReadVarUI32());
    }

    i64 ReadVarI64() {
        return NYson::ZigZagDecode64(ReadVarUI64());
    }

    double NextDouble() {
        double val = *reinterpret_cast<const double*>(Data_);
        Data_ += sizeof(double);
        return val;
    }

    void Skip(i32 cnt) {
        Data_ += cnt;
    }

    const char* Data() {
        return Data_;
    }

    size_t Available() const {
        return Available_;
    }
private:
    const char* Data_;
    size_t Available_;
};

class IYsonBlockReader {
public:
    virtual NUdf::TBlockItem GetItem(TYsonReaderDetails& buf) = 0;
    virtual ~IYsonBlockReader() = default;
};

template<bool Native>
class IYsonBlockReaderWithNativeFlag : public IYsonBlockReader {
public:
    virtual NUdf::TBlockItem GetNotNull(TYsonReaderDetails&) = 0;
    NUdf::TBlockItem GetNullableItem(TYsonReaderDetails& buf) {
        char prev = buf.Current();
        if constexpr (Native) {
            if (prev == NYson::NDetail::EntitySymbol) {
                buf.Next();
                return NUdf::TBlockItem();
            }
            return GetNotNull(buf).MakeOptional();
        }
        buf.Next();
        if (prev == NYson::NDetail::EntitySymbol) {
            return NUdf::TBlockItem();
        }
        if (prev != NYson::NDetail::BeginListSymbol) {
            Y_ENSURE(prev == NYson::NDetail::BeginListSymbol);
        }
        auto result = GetNotNull(buf);
        if (buf.Current() == NYson::NDetail::ListItemSeparatorSymbol) {
            buf.Next();
        }
        Y_ENSURE(buf.Current() == NYson::NDetail::EndListSymbol);
        buf.Next();
        return result.MakeOptional();
    }
};

class IYsonBlockReaderForPg : public IYsonBlockReader {
public:
    virtual NUdf::TBlockItem GetNotNull(TYsonReaderDetails&) = 0;
    NUdf::TBlockItem GetNullableItem(TYsonReaderDetails& buf) {
        char prev = buf.Current();
        if (prev == NYson::NDetail::EntitySymbol) {
            buf.Next();
            return NUdf::TBlockItem();
        }
        return GetNotNull(buf);
    }
};

class ITopLevelBlockReader {
public:
    virtual arrow::Datum Convert(std::shared_ptr<arrow::ArrayData>) = 0;
    virtual ~ITopLevelBlockReader() = default;
};

std::unique_ptr<IYsonBlockReader> BuildPgYsonColumnReader(bool Native, const NUdf::TPgTypeDescription&);
std::unique_ptr<ITopLevelBlockReader> BuildPgTopLevelColumnReader(std::unique_ptr<NKikimr::NUdf::IArrayBuilder>&& builder, const NKikimr::NMiniKQL::TPgType* targetType);

} // NYql

namespace NKikimr {
namespace NMiniKQL {

class IBlockAggregatorFactory;
void RegisterPgBlockAggs(THashMap<TString, std::unique_ptr<IBlockAggregatorFactory>>& registry);

}
}
