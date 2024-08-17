#include "arrow_converter.h"

#include <ydb/library/yql/public/udf/arrow/defs.h>
#include <ydb/library/yql/public/udf/arrow/block_builder.h>
#include <ydb/library/yql/public/udf/arrow/block_reader.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/detail.h>
#include <library/cpp/yson/varint.h>
#include <util/stream/mem.h>

#include <arrow/array/data.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <arrow/compute/cast.h>

namespace NYql::NDqs {

template<typename T>
struct TypeHelper {
    using Type = T;
};

#define GEN_TYPE(type)\
    NumericConverterImpl<arrow::type ## Type>

#define GEN_TYPE_STR(type)\
    StringConverterImpl<arrow::type ## Type>

template<typename T>
arrow::Datum NumericConverterImpl(NUdf::IArrayBuilder* builder, std::shared_ptr<arrow::ArrayData> block) {
    arrow::DictionaryArray dict(block);
    typename ::arrow::TypeTraits<T>::ArrayType val(dict.dictionary()->data());
    auto data = dict.indices()->data()->GetValues<ui32>(1);
    if (dict.null_count()) {
        for (i64 i = 0; i < block->length; ++i) {
            if (dict.IsNull(i)) {
                builder->Add(NUdf::TBlockItem{});
            } else {
                if constexpr (std::is_same_v<decltype(val.Value(data[i])), bool>) {
                    builder->Add(NUdf::TBlockItem((ui8)val.Value(data[i])));
                } else {
                    builder->Add(NUdf::TBlockItem(val.Value(data[i])));
                }
            }
        }
    } else {
        for (i64 i = 0; i < block->length; ++i) {
            if constexpr (std::is_same_v<decltype(val.Value(data[i])), bool>) {
                builder->Add(NUdf::TBlockItem((ui8)val.Value(data[i])));
            } else {
                builder->Add(NUdf::TBlockItem(val.Value(data[i])));
            }
        }
    }
    return builder->Build(false);
}

template<typename T>
arrow::Datum StringConverterImpl(NUdf::IArrayBuilder* builder, std::shared_ptr<arrow::ArrayData> block) {
    arrow::DictionaryArray dict(block);
    typename ::arrow::TypeTraits<T>::ArrayType val(dict.dictionary()->data());
    auto data = dict.indices()->data()->GetValues<ui32>(1);
    if (dict.null_count()) {
        for (i64 i = 0; i < block->length; ++i) {
            if (dict.IsNull(i)) {
                builder->Add(NUdf::TBlockItem{});
            } else {
                i32 len;
                auto ptr = reinterpret_cast<const char*>(val.GetValue(data[i], &len));
                builder->Add(NUdf::TBlockItem(std::string_view(ptr, len)));
            }
        }
    } else {
        for (i64 i = 0; i < block->length; ++i) {
                i32 len;
                auto ptr = reinterpret_cast<const char*>(val.GetValue(data[i], &len));
                builder->Add(NUdf::TBlockItem(std::string_view(ptr, len)));
        }
    }
    return builder->Build(false);
}

using namespace NKikimr::NMiniKQL;
using namespace NYson::NDetail;

class TYsonReaderDetails {
public:
    TYsonReaderDetails(const std::string_view& s) : Data_(s.data()), Available_(s.size()) {}

    constexpr char Next() {
        YQL_ENSURE(Available_-- > 0);
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

namespace {
void SkipYson(TYsonReaderDetails& buf) {
    switch (buf.Current()) {
    case BeginListSymbol: {
        buf.Next();
        for (;;) {
            SkipYson(buf);
            if (buf.Current() == ListItemSeparatorSymbol) {
                buf.Next();
            }
            if (buf.Current() == EndListSymbol) {
                break;
            }
        }
        buf.Next();
        break;
    }
    case BeginAttributesSymbol:
    case BeginMapSymbol: {
        auto originalEnd = buf.Current()  == BeginMapSymbol ? EndMapSymbol : EndAttributesSymbol;
        buf.Next();
        for (;;) {
            SkipYson(buf);
            YQL_ENSURE(buf.Current() == KeyValueSeparatorSymbol);
            buf.Next();
            SkipYson(buf);
            if (buf.Current() == KeyedItemSeparatorSymbol) {
                buf.Next();
            }
            if (buf.Current() == originalEnd) {
                break;
            }
        }
        buf.Next();
        break;
    }
    case StringMarker:
        buf.Next();
        buf.Skip(buf.ReadVarI32());
        break;
    case Uint64Marker:
    case Int64Marker:
        buf.Next();
        Y_UNUSED(buf.ReadVarI64());
        break;
    case TrueMarker:
    case FalseMarker:
        buf.Next();
        break;
    case DoubleMarker:
        buf.Next();
        Y_UNUSED(buf.NextDouble());
        break;
    default:
        YQL_ENSURE(false, "Unexpected char: " + std::string{buf.Current()});
    }
}

NUdf::TBlockItem ReadYson(TYsonReaderDetails& buf) {
    const char* beg = buf.Data();
    SkipYson(buf);
    return NUdf::TBlockItem(std::string_view(beg, buf.Data() - beg));
}
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
            if (prev == EntitySymbol) {
                buf.Next();
                return NUdf::TBlockItem();
            }
            return GetNotNull(buf).MakeOptional();
        }
        buf.Next();
        if (prev == EntitySymbol) {
            return NUdf::TBlockItem();
        }
        YQL_ENSURE(prev == BeginListSymbol);
        auto result = GetNotNull(buf);
        if (buf.Current() == ListItemSeparatorSymbol) {
            buf.Next();
        }
        YQL_ENSURE(buf.Current() == EndListSymbol);
        buf.Next();
        return result.MakeOptional();
    }
private:
};

template<bool Nullable, bool Native>
class TYsonTupleBlockReader final : public IYsonBlockReaderWithNativeFlag<Native> {
public:
    TYsonTupleBlockReader(TVector<std::unique_ptr<IYsonBlockReader>>&& children)
        : Children_(std::move(children))
        , Items_(Children_.size())
    {}

    NUdf::TBlockItem GetItem(TYsonReaderDetails& buf) override final {
        if constexpr (Nullable) {
            return this->GetNullableItem(buf);
        }
        return GetNotNull(buf);
    }
    NUdf::TBlockItem GetNotNull(TYsonReaderDetails& buf) override final {
        YQL_ENSURE(buf.Current() == BeginListSymbol);
        buf.Next();
        for (ui32 i = 0; i < Children_.size(); ++i) {
            Items_[i] = Children_[i]->GetItem(buf);
            if (buf.Current() == ListItemSeparatorSymbol) {
                buf.Next();
            }
        }
        YQL_ENSURE(buf.Current() == EndListSymbol);
        buf.Next();
        return NUdf::TBlockItem(Items_.data());
    }
private:
    const TVector<std::unique_ptr<IYsonBlockReader>> Children_;
    TVector<NUdf::TBlockItem> Items_;
};

template<typename T, bool Nullable, NKikimr::NUdf::EDataSlot OriginalT, bool Native>
class TYsonStringBlockReader final : public IYsonBlockReaderWithNativeFlag<Native> {
public:
    NUdf::TBlockItem GetItem(TYsonReaderDetails& buf) override final {
        if constexpr (Nullable) {
            return this->GetNullableItem(buf);
        }
        return GetNotNull(buf);
    }

    NUdf::TBlockItem GetNotNull(TYsonReaderDetails& buf) override final {
        if constexpr (NUdf::EDataSlot::Yson != OriginalT) {
            YQL_ENSURE(buf.Current() == StringMarker);
            buf.Next();
            const i32 length = buf.ReadVarI32();
            auto res = NUdf::TBlockItem(NUdf::TStringRef(buf.Data(), length));
            buf.Skip(length);
            return res;
        } else {
            return ReadYson(buf);
        }
    }
};

template<typename T, bool Nullable, bool Native>
class TYsonTzDateBlockReader final : public IYsonBlockReaderWithNativeFlag<Native> {
public:
    NUdf::TBlockItem GetItem(TYsonReaderDetails& buf) override final {
        if constexpr (Nullable) {
            return this->GetNullableItem(buf);
        }
        return GetNotNull(buf);
    }

    NUdf::TBlockItem GetNotNull(TYsonReaderDetails& buf) override final {
        using TLayout = typename NUdf::TDataType<T>::TLayout;
        size_t length = sizeof(TLayout) + sizeof(NUdf::TTimezoneId);
        Y_ASSERT(buf.Available() == length);

        TLayout date;
        NUdf::TTimezoneId tz;

        if constexpr (std::is_same_v<T, NUdf::TTzDate>) {
            DeserializeTzDate({buf.Data(), length}, date, tz);
        } else if constexpr (std::is_same_v<T, NUdf::TTzDatetime>) {
            DeserializeTzDatetime({buf.Data(), length}, date, tz);
        } else if constexpr (std::is_same_v<T, NUdf::TTzTimestamp>) {
            DeserializeTzTimestamp({buf.Data(), length}, date, tz);
        } else if constexpr (std::is_same_v<T, NUdf::TTzDate32>) {
            DeserializeTzDate32({buf.Data(), length}, date, tz);
        } else if constexpr (std::is_same_v<T, NUdf::TTzDatetime64>) {
            DeserializeTzDatetime64({buf.Data(), length}, date, tz);
        } else if constexpr (std::is_same_v<T, NUdf::TTzTimestamp64>) {
            DeserializeTzTimestamp64({buf.Data(), length}, date, tz);
        } else {
            static_assert(sizeof(T) == 0, "Unsupported tz date type");
        }

        buf.Skip(length);
        NUdf::TBlockItem res {date};
        res.SetTimezoneId(tz);
        return res;
    }
};

namespace {
struct TYtColumnConverterSettings {
    TYtColumnConverterSettings(NKikimr::NMiniKQL::TType* type, const NUdf::IPgBuilder* pgBuilder, arrow::MemoryPool& pool, bool isNative);
    NKikimr::NMiniKQL::TType* Type;
    const NUdf::IPgBuilder* PgBuilder;
    arrow::MemoryPool& Pool;
    const bool IsNative;
    const bool IsTopOptional;
    std::shared_ptr<arrow::DataType> ArrowType;
    std::unique_ptr<NKikimr::NUdf::IArrayBuilder> Builder;
};
}

template<typename T, bool Nullable, bool Native>
class TYsonFixedSizeBlockReader final : public IYsonBlockReaderWithNativeFlag<Native> {
public:
    NUdf::TBlockItem GetItem(TYsonReaderDetails& buf) override final {
        if constexpr (Nullable) {
            return this->GetNullableItem(buf);
        }
        return GetNotNull(buf);
    }

    NUdf::TBlockItem GetNotNull(TYsonReaderDetails& buf) override final {
        if constexpr (std::is_same_v<T, bool>) {
            YQL_ENSURE(buf.Current() == FalseMarker || buf.Current() == TrueMarker);
            bool res = buf.Current() == TrueMarker;
            buf.Next();
            return NUdf::TBlockItem(res);
        }

        if constexpr (std::is_same_v<T, unsigned char>) {
            if (buf.Current() == FalseMarker || buf.Current() == TrueMarker) {
                bool res = buf.Current() == TrueMarker;
                buf.Next();
                return NUdf::TBlockItem(T(res));
            }
        }

        if constexpr (std::is_integral_v<T>) {
            if constexpr (std::is_signed_v<T>) {
                YQL_ENSURE(buf.Current() == Int64Marker);
                buf.Next();
                return NUdf::TBlockItem(T(buf.ReadVarI64()));
            } else {
                YQL_ENSURE(buf.Current() == Uint64Marker);
                buf.Next();
                return NUdf::TBlockItem(T(buf.ReadVarUI64()));
            }
        } else if constexpr (std::is_floating_point_v<T>) {
            YQL_ENSURE(buf.Current() == DoubleMarker);
            buf.Next();                
            return NUdf::TBlockItem(T(buf.NextDouble()));
        } else {
            static_assert(std::is_floating_point_v<T>);
        }
    }
};

template<bool Native>
class TYsonExternalOptBlockReader final : public IYsonBlockReaderWithNativeFlag<Native> {
public:
    TYsonExternalOptBlockReader(std::unique_ptr<IYsonBlockReader>&& inner)
        : Inner_(std::move(inner))
    {}

    NUdf::TBlockItem GetItem(TYsonReaderDetails& buf) final {
        char prev = buf.Current();
        buf.Next();
        if (prev == EntitySymbol) {
            return NUdf::TBlockItem();
        }
        YQL_ENSURE(prev == BeginListSymbol);
        if constexpr (!Native) {
            if (buf.Current() == EndListSymbol) {
                buf.Next();
                return NUdf::TBlockItem();
            }
        }
        auto result = Inner_->GetItem(buf);
        if (buf.Current() == ListItemSeparatorSymbol) {
            buf.Next();
        }
        YQL_ENSURE(buf.Current() == EndListSymbol);
        buf.Next();
        return result.MakeOptional();
    }

    NUdf::TBlockItem GetNotNull(TYsonReaderDetails& buf) override final {
        YQL_ENSURE(false, "Can't be called");
    }
private:
    std::unique_ptr<IYsonBlockReader> Inner_;
};

template<bool Native>
struct TYsonBlockReaderTraits {
    using TResult = IYsonBlockReader;
    template <bool Nullable>
    using TTuple = TYsonTupleBlockReader<Nullable, Native>;
    template <typename T, bool Nullable>
    using TFixedSize = TYsonFixedSizeBlockReader<T, Nullable, Native>;
    template <typename TStringType, bool Nullable, NKikimr::NUdf::EDataSlot OriginalT>
    using TStrings = TYsonStringBlockReader<TStringType, Nullable, OriginalT, Native>;
    using TExtOptional = TYsonExternalOptBlockReader<Native>;

    static std::unique_ptr<TResult> MakePg(const NUdf::TPgTypeDescription& desc, const NUdf::IPgBuilder* pgBuilder) {
        Y_UNUSED(pgBuilder);
        if (desc.PassByValue) {
            return std::make_unique<TFixedSize<ui64, true>>();
        } else {
            return std::make_unique<TStrings<arrow::BinaryType, true, NKikimr::NUdf::EDataSlot::String>>();
        }
    }

    static std::unique_ptr<TResult> MakeResource(bool isOptional) {
        Y_UNUSED(isOptional);
        ythrow yexception() << "Yson reader not implemented for block resources";
    }   

    template<typename TTzDate>
    static std::unique_ptr<TResult> MakeTzDate(bool isOptional) {
        Y_UNUSED(isOptional);
        if (isOptional) {
            using TTzDateReader = TYsonTzDateBlockReader<TTzDate, true, Native>;
            return std::make_unique<TTzDateReader>();
        } else {
            using TTzDateReader = TYsonTzDateBlockReader<TTzDate, false, Native>;
            return std::make_unique<TTzDateReader>();
        }
    }   
};

template<bool IsDictionary>
class TPrimitiveColumnConverter {
public:
    TPrimitiveColumnConverter(TYtColumnConverterSettings& settings) : Settings_(settings) {
        if constexpr (IsDictionary) {
            switch (Settings_.ArrowType->id()) {
            case arrow::Type::BOOL:     PrimitiveConverterImpl_ = GEN_TYPE(Boolean); break;
            case arrow::Type::INT8:     PrimitiveConverterImpl_ = GEN_TYPE(Int8); break;
            case arrow::Type::UINT8:    PrimitiveConverterImpl_ = GEN_TYPE(UInt8); break;
            case arrow::Type::INT16:    PrimitiveConverterImpl_ = GEN_TYPE(Int16); break;
            case arrow::Type::UINT16:   PrimitiveConverterImpl_ = GEN_TYPE(UInt16); break;
            case arrow::Type::INT32:    PrimitiveConverterImpl_ = GEN_TYPE(Int32); break;
            case arrow::Type::UINT32:   PrimitiveConverterImpl_ = GEN_TYPE(UInt32); break;
            case arrow::Type::INT64:    PrimitiveConverterImpl_ = GEN_TYPE(Int64); break;
            case arrow::Type::UINT64:   PrimitiveConverterImpl_ = GEN_TYPE(UInt64); break;
            case arrow::Type::DOUBLE:   PrimitiveConverterImpl_ = GEN_TYPE(Double); break;
            case arrow::Type::FLOAT:    PrimitiveConverterImpl_ = GEN_TYPE(Float); break;
            case arrow::Type::STRING:   PrimitiveConverterImpl_ = GEN_TYPE_STR(String); break;
            case arrow::Type::BINARY:   PrimitiveConverterImpl_ = GEN_TYPE_STR(Binary); break;
            default:
                return; // will check in runtime
            };
        }
    }
    arrow::Datum Convert(std::shared_ptr<arrow::ArrayData> block) {
        if constexpr (IsDictionary) {
            return PrimitiveConverterImpl_(Settings_.Builder.get(), block);
        }
        return block;
    }
private:
    TYtColumnConverterSettings& Settings_;
    arrow::Datum (*PrimitiveConverterImpl_)(NUdf::IArrayBuilder*, std::shared_ptr<arrow::ArrayData>);
};

template<bool Native, bool IsTopOptional, bool IsDictionary>
class TYtYsonColumnConverter {
public:
    TYtYsonColumnConverter(TYtColumnConverterSettings& settings) : Settings_(settings) {
        Reader_ = NUdf::MakeBlockReaderImpl<TYsonBlockReaderTraits<Native>>(TTypeInfoHelper(), settings.Type, settings.PgBuilder);
    }

    arrow::Datum Convert(std::shared_ptr<arrow::ArrayData> block) {
        if constexpr(!IsDictionary) {
            arrow::BinaryArray binary(block);
            if (block->GetNullCount()) {
                for (i64 i = 0; i < block->length; ++i) {
                    if (binary.IsNull(i)) {
                        Settings_.Builder->Add(NUdf::TBlockItem{});
                    } else {
                        i32 len;
                        auto ptr = reinterpret_cast<const char*>(binary.GetValue(i, &len));
                        TYsonReaderDetails inp(std::string_view(ptr, len));
                        auto res = Reader_->GetItem(inp);
                        if constexpr (!Native && IsTopOptional) {
                            res = res.MakeOptional();
                        }
                        Settings_.Builder->Add(std::move(res));
                    }
                }
            } else {
                for (i64 i = 0; i < block->length; ++i) {
                    i32 len;
                    auto ptr = reinterpret_cast<const char*>(binary.GetValue(i, &len));
                    TYsonReaderDetails inp(std::string_view(ptr, len));
                    auto res = Reader_->GetItem(inp);
                    if constexpr (!Native && IsTopOptional) {
                        res = res.MakeOptional();
                    }
                    Settings_.Builder->Add(std::move(res));
                }
            }
            return Settings_.Builder->Build(false);
        }
        arrow::DictionaryArray dict(block);
        arrow::BinaryArray binary(block->dictionary);
        auto data = dict.indices()->data()->GetValues<ui32>(1);
        if (dict.null_count()) {
            for (i64 i = 0; i < block->length; ++i) {
                if (dict.IsNull(i)) {
                    Settings_.Builder->Add(NUdf::TBlockItem{});
                } else {
                    i32 len;
                    auto ptr = reinterpret_cast<const char*>(binary.GetValue(data[i], &len));
                    TYsonReaderDetails inp(std::string_view(ptr, len));
                    auto res = Reader_->GetItem(inp);
                    if constexpr (!Native && IsTopOptional) {
                        res = res.MakeOptional();
                    }
                    Settings_.Builder->Add(std::move(res));
                }
            }
        } else {
            for (i64 i = 0; i < block->length; ++i) {
                i32 len;
                auto ptr = reinterpret_cast<const char*>(binary.GetValue(data[i], &len));
                TYsonReaderDetails inp(std::string_view(ptr, len));
                auto res = Reader_->GetItem(inp);
                if constexpr (!Native && IsTopOptional) {
                    res = res.MakeOptional();
                }
                Settings_.Builder->Add(std::move(res));
            }
        }
        return Settings_.Builder->Build(false);
    }

private:
    std::shared_ptr<typename TYsonBlockReaderTraits<Native>::TResult> Reader_;
    TYtColumnConverterSettings& Settings_;
};

template<bool Native, bool IsTopOptional>
class TYtColumnConverter final : public IYtColumnConverter {
public:
    TYtColumnConverter(TYtColumnConverterSettings&& settings) 
        : Settings_(std::move(settings))
        , DictYsonConverter_(Settings_)
        , YsonConverter_(Settings_)
        , DictPrimitiveConverter_(Settings_) {}

    arrow::Datum Convert(std::shared_ptr<arrow::ArrayData> block) override {
        if (arrow::Type::DICTIONARY == block->type->id()) {
            if (static_cast<const arrow::DictionaryType&>(*block->type).value_type()->Equals(Settings_.ArrowType)) {
                return DictPrimitiveConverter_.Convert(block);
            } else {
                return DictYsonConverter_.Convert(block);
            }
        } else {
            auto blockType = block->type;
            auto noConvert = blockType->Equals(Settings_.ArrowType);
            if (noConvert) {
                return block;
            } else if (arrow::Type::UINT8 == Settings_.ArrowType->id() && arrow::Type::BOOL == blockType->id()) {
                auto result = arrow::compute::Cast(arrow::Datum(*block), Settings_.ArrowType);
                Y_ENSURE(result.ok());
                return *result;
            } else {
                YQL_ENSURE(arrow::Type::BINARY == blockType->id());
                return YsonConverter_.Convert(block);
            }
        }
    }
private:
    TYtColumnConverterSettings Settings_;
    TYtYsonColumnConverter<Native, IsTopOptional, true> DictYsonConverter_;
    TYtYsonColumnConverter<Native, IsTopOptional, false> YsonConverter_;
    TPrimitiveColumnConverter<true> DictPrimitiveConverter_;
};

TYtColumnConverterSettings::TYtColumnConverterSettings(NKikimr::NMiniKQL::TType* type, const NUdf::IPgBuilder* pgBuilder, arrow::MemoryPool& pool, bool isNative) 
    : Type(type), PgBuilder(pgBuilder), Pool(pool), IsNative(isNative), IsTopOptional(!isNative && type->IsOptional())
{
    if (!isNative) {
        if (Type->IsOptional()) {
            Type = static_cast<NKikimr::NMiniKQL::TOptionalType*>(Type)->GetItemType();
        }
    }
    YQL_ENSURE(ConvertArrowType(type, ArrowType), "Can't convert type to arrow");
    size_t maxBlockItemSize = CalcMaxBlockItemSize(type);
    size_t maxBlockLen = CalcBlockLen(maxBlockItemSize);
    Builder = std::move(NUdf::MakeArrayBuilder(
                    TTypeInfoHelper(), type,
                    pool,
                    maxBlockLen,
                    pgBuilder
                ));
}

template<typename Common, template <bool...> typename T, typename Args, bool... Acc>
struct TBoolDispatcher {

  std::unique_ptr<Common> Dispatch(Args&& args) const {
    return std::make_unique<T<Acc...>>(std::forward<Args&&>(args));
  }

  template <typename... Bools>
  auto Dispatch(Args&& args, bool head, Bools... tail) const {
    return head ?
      TBoolDispatcher<Common, T, Args, Acc..., true >().Dispatch(std::forward<Args&&>(args), tail...) :
      TBoolDispatcher<Common, T, Args, Acc..., false>().Dispatch(std::forward<Args&&>(args), tail...);
  }
};

std::unique_ptr<IYtColumnConverter> MakeYtColumnConverter(NKikimr::NMiniKQL::TType* type, const NUdf::IPgBuilder* pgBuilder, arrow::MemoryPool& pool, bool isNative) {
    TYtColumnConverterSettings settings(type, pgBuilder, pool, isNative);
    bool isTopOptional = settings.IsTopOptional;
    return TBoolDispatcher<IYtColumnConverter, TYtColumnConverter, TYtColumnConverterSettings>().Dispatch(std::move(settings), isNative, isTopOptional);
}
}
