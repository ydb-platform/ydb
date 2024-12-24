#include "yt_arrow_converter.h"
#include "yt_arrow_converter_details.h"

#include <yql/essentials/public/udf/arrow/defs.h>
#include <yql/essentials/public/udf/arrow/block_builder.h>
#include <yql/essentials/public/udf/arrow/block_reader.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/detail.h>
#include <library/cpp/yson/varint.h>
#include <util/stream/mem.h>

#include <arrow/array/data.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <arrow/compute/cast.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;
namespace {
struct TYtColumnConverterSettings {
    TYtColumnConverterSettings(TType* type, const NUdf::IPgBuilder* pgBuilder, arrow::MemoryPool& pool, bool isNative);
    TType* Type;
    const NUdf::IPgBuilder* PgBuilder;
    arrow::MemoryPool& Pool;
    const bool IsNative;
    const bool IsTopOptional;
    const bool IsTopLevelYson;
    std::shared_ptr<arrow::DataType> ArrowType;
    std::unique_ptr<NKikimr::NUdf::IArrayBuilder> Builder;
};
}

std::string_view GetNotNullString(auto& data, i64 idx) {
    i32 len;
    auto ptr = reinterpret_cast<const char*>(data.GetValue(idx, &len));
    return std::string_view(ptr, len);
}

using YTDictIndexType = ui32;

#define GEN_TYPE(type)\
    NumericConverterImpl<arrow::type ## Type>

#define GEN_TYPE_STR(type, isTopLevelYson)\
    StringConverterImpl<arrow::type ## Type, isTopLevelYson>

template<typename T>
Y_FORCE_INLINE void AddNumber(NUdf::IArrayBuilder* builder, T&& value) {
    if constexpr (std::is_same_v<T, bool>) {
        builder->Add(NUdf::TBlockItem((ui8)value));
#if defined(_darwin_) && defined(_64_)
    } else if constexpr (std::is_same_v<T, unsigned long long>) {
        builder->Add(NUdf::TBlockItem((ui64)value));
    } else if constexpr (std::is_same_v<T, long long>) {
        builder->Add(NUdf::TBlockItem((i64)value));
#endif
    } else {
        builder->Add(NUdf::TBlockItem(value));
    }
}

// Unpack dictionary with right type
template<typename T>
arrow::Datum NumericConverterImpl(NUdf::IArrayBuilder* builder, std::shared_ptr<arrow::ArrayData> block) {
    arrow::DictionaryArray dict(block);
    typename ::arrow::TypeTraits<T>::ArrayType val(dict.dictionary()->data());
    auto data = dict.indices()->data()->GetValues<YTDictIndexType>(1);
    if (dict.null_count()) {
        for (i64 i = 0; i < block->length; ++i) {
            if (dict.IsNull(i)) {
                builder->Add(NUdf::TBlockItem{});
            } else {
                AddNumber(builder, val.Value(data[i]));
            }
        }
    } else {
        for (i64 i = 0; i < block->length; ++i) {
            AddNumber(builder, val.Value(data[i]));
        }
    }
    return builder->Build(false);
}

// There is no support of the non-optional Yson in YT now.
// Because of it, isTopLevelYson must indicate whether a type we want is the top-level yson or no.
// If it is, just put entity symbol string ("#") instead of empty TBlockItem
template<typename T, bool IsTopLevelYson>
arrow::Datum StringConverterImpl(NUdf::IArrayBuilder* builder, std::shared_ptr<arrow::ArrayData> block) {
    arrow::DictionaryArray dict(block);
    typename ::arrow::TypeTraits<T>::ArrayType val(dict.dictionary()->data());
    auto data = dict.indices()->data()->GetValues<YTDictIndexType>(1);
    if (dict.null_count()) {
        for (i64 i = 0; i < block->length; ++i) {
            if (dict.IsNull(i)) {
                if constexpr(IsTopLevelYson) {
                    builder->Add(NUdf::TBlockItem(std::string_view("#")));
                } else {
                    builder->Add(NUdf::TBlockItem{});
                }
            } else {
                builder->Add(NUdf::TBlockItem(GetNotNullString(val, data[i])));
            }
        }
    } else {
        for (i64 i = 0; i < block->length; ++i) {
            builder->Add(NUdf::TBlockItem(GetNotNullString(val, data[i])));
        }
    }
    return builder->Build(false);
}

template<bool IsDictionary, bool IsTopLevelYson>
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
            case arrow::Type::STRING:   PrimitiveConverterImpl_ = GEN_TYPE_STR(Binary, IsTopLevelYson); break; // all strings from yt are in binary format
            case arrow::Type::BINARY:   PrimitiveConverterImpl_ = GEN_TYPE_STR(Binary, IsTopLevelYson); break;
            default:
                return; // will check in runtime
            };
        }
    }

    arrow::Datum Convert(std::shared_ptr<arrow::ArrayData> block) {
        if constexpr (IsDictionary) {
            return PrimitiveConverterImpl_(Settings_.Builder.get(), block);
        }
        if constexpr (IsTopLevelYson) {
            auto builder = Settings_.Builder.get();
            arrow::BinaryArray binary(block);
            if (binary.null_count()) {
                for (int64_t i = 0; i < binary.length(); ++i) {
                    if (binary.IsNull(i)) {
                        builder->Add(NUdf::TBlockItem(std::string_view("#")));
                    } else {
                        builder->Add(NUdf::TBlockItem(GetNotNullString(binary, i)));
                    }
                }
            } else {
                for (int64_t i = 0; i < binary.length(); ++i) {
                    builder->Add(NUdf::TBlockItem(GetNotNullString(binary, i)));
                }
            }
            return builder->Build(false);
        }
        return block;
    }
private:
    TYtColumnConverterSettings& Settings_;
    arrow::Datum (*PrimitiveConverterImpl_)(NUdf::IArrayBuilder*, std::shared_ptr<arrow::ArrayData>);
};

namespace {
void SkipYson(TYsonBuffer& buf) {
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
    case EntitySymbol:
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
};

NUdf::TBlockItem ReadYson(TYsonBuffer& buf) {
    const char* beg = buf.Data();
    SkipYson(buf);
    return NUdf::TBlockItem(std::string_view(beg, buf.Data() - beg));
}

template<bool Nullable, bool Native>
class TTupleYsonReader final : public IYsonComplexTypeReader<Native> {
public:
    using TIReaderPtr = std::unique_ptr<IYsonComplexTypeReader<Native>>;
    TTupleYsonReader(TVector<TIReaderPtr>&& children)
        : Children_(std::move(children))
        , Items_(Children_.size())
    {}

    NUdf::TBlockItem GetItem(TYsonBuffer& buf) override final {
        if constexpr (Nullable) {
            return this->GetNullableItem(buf);
        }
        return GetNotNull(buf);
    }
    NUdf::TBlockItem GetNotNull(TYsonBuffer& buf) override final {
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
    const TVector<TIReaderPtr> Children_;
    TVector<NUdf::TBlockItem> Items_;
};

template<typename T, bool Nullable, NKikimr::NUdf::EDataSlot OriginalT, bool Native>
class TStringYsonReader final : public IYsonComplexTypeReader<Native> {
public:
    NUdf::TBlockItem GetItem(TYsonBuffer& buf) override final {
        if constexpr (Nullable) {
            return this->GetNullableItem(buf);
        }
        return GetNotNull(buf);
    }

    NUdf::TBlockItem GetNotNull(TYsonBuffer& buf) override final {
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
class TTzDateYsonReader final : public IYsonComplexTypeReader<Native> {
public:
    NUdf::TBlockItem GetItem(TYsonBuffer& buf) override final {
        if constexpr (Nullable) {
            return this->GetNullableItem(buf);
        }
        return GetNotNull(buf);
    }

    NUdf::TBlockItem GetNotNull(TYsonBuffer& buf) override final {
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

template<typename T, bool Nullable, bool Native>
class TFixedSizeYsonReader final : public IYsonComplexTypeReader<Native> {
public:
    NUdf::TBlockItem GetItem(TYsonBuffer& buf) override final {
        if constexpr (Nullable) {
            return this->GetNullableItem(buf);
        }
        return GetNotNull(buf);
    }

    NUdf::TBlockItem GetNotNull(TYsonBuffer& buf) override final {
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

        if constexpr (std::is_integral_v<T> && !std::is_same_v<T, NYql::NDecimal::TInt128>) {
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
class TExternalOptYsonReader final : public IYsonComplexTypeReader<Native> {
public:
    using TIReaderPtr = std::unique_ptr<IYsonComplexTypeReader<Native>>;
    TExternalOptYsonReader(TIReaderPtr&& inner)
        : Underlying_(std::move(inner))
    {}

    NUdf::TBlockItem GetItem(TYsonBuffer& buf) final {
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
        auto result = Underlying_->GetItem(buf);
        if (buf.Current() == ListItemSeparatorSymbol) {
            buf.Next();
        }
        YQL_ENSURE(buf.Current() == EndListSymbol);
        buf.Next();
        return result.MakeOptional();
    }

    NUdf::TBlockItem GetNotNull(TYsonBuffer&) override final {
        Y_ABORT("Can't be called");
    }
private:
    TIReaderPtr Underlying_;
};

template<bool Native>
struct TComplexTypeYsonReaderTraits {
    using TResult = IYsonComplexTypeReader<Native>;
    template <bool Nullable>
    using TTuple = TTupleYsonReader<Nullable, Native>;
    // TODO: Implement reader for decimals
    template <typename T, bool Nullable, typename = std::enable_if_t<!std::is_same_v<T, NYql::NDecimal::TInt128> && (std::is_integral_v<T> || std::is_floating_point_v<T>)>>
    using TFixedSize = TFixedSizeYsonReader<T, Nullable, Native>;
    template <typename TStringType, bool Nullable, NKikimr::NUdf::EDataSlot OriginalT>
    using TStrings = TStringYsonReader<TStringType, Nullable, OriginalT, Native>;
    using TExtOptional = TExternalOptYsonReader<Native>;

    static std::unique_ptr<TResult> MakePg(const NUdf::TPgTypeDescription& desc, const NUdf::IPgBuilder* pgBuilder) {
        ythrow yexception() << "Complex type Yson reader not implemented for block resources";
    }

    static std::unique_ptr<TResult> MakeResource(bool) {
        ythrow yexception() << "Complex type Yson reader not implemented for block resources";
    }

    template<typename TTzDate>
    static std::unique_ptr<TResult> MakeTzDate(bool isOptional) {
        if (isOptional) {
            using TTzDateReader = TTzDateYsonReader<TTzDate, true, Native>;
            return std::make_unique<TTzDateReader>();
        } else {
            using TTzDateReader = TTzDateYsonReader<TTzDate, false, Native>;
            return std::make_unique<TTzDateReader>();
        }
    }
};

template<bool Native, bool IsTopOptional>
Y_FORCE_INLINE void AddFromYson(auto& reader, auto& builder, std::string_view yson) {
    TYsonBuffer inp(yson);
    auto res = reader->GetItem(inp);
    if constexpr (!Native && IsTopOptional) {
        res = res.MakeOptional();
    }
    builder->Add(std::move(res));
}

template<bool Native, bool IsTopOptional, bool IsDictionary>
class TComplexTypeYsonColumnConverter {
public:
    TComplexTypeYsonColumnConverter(TYtColumnConverterSettings& settings) : Settings_(settings) {
        Reader_ = NUdf::MakeBlockReaderImpl<TComplexTypeYsonReaderTraits<Native>>(TTypeInfoHelper(), settings.Type, settings.PgBuilder);
    }

    arrow::Datum Convert(std::shared_ptr<arrow::ArrayData> block) {
        auto& builder = Settings_.Builder;
        if constexpr(!IsDictionary) {
            arrow::BinaryArray binary(block);
            if (block->GetNullCount()) {
                for (i64 i = 0; i < block->length; ++i) {
                    if (binary.IsNull(i)) {
                        builder->Add(NUdf::TBlockItem{});
                    } else {
                        AddFromYson<Native, IsTopOptional>(Reader_, builder, GetNotNullString(binary, i));
                    }
                }
            } else {
                for (i64 i = 0; i < block->length; ++i) {
                    AddFromYson<Native, IsTopOptional>(Reader_, builder, GetNotNullString(binary, i));
                }
            }
            return builder->Build(false);
        }
        arrow::DictionaryArray dict(block);
        arrow::BinaryArray binary(block->dictionary);
        auto data = dict.indices()->data()->GetValues<YTDictIndexType>(1);
        if (dict.null_count()) {
            for (i64 i = 0; i < block->length; ++i) {
                if (dict.IsNull(i)) {
                    Settings_.Builder->Add(NUdf::TBlockItem{});
                } else {
                    AddFromYson<Native, IsTopOptional>(Reader_, builder, GetNotNullString(binary, data[i]));
                }
            }
        } else {
            for (i64 i = 0; i < block->length; ++i) {
                AddFromYson<Native, IsTopOptional>(Reader_, builder, GetNotNullString(binary, data[i]));
            }
        }
        return Settings_.Builder->Build(false);
    }

private:
    std::shared_ptr<typename TComplexTypeYsonReaderTraits<Native>::TResult> Reader_;
    TYtColumnConverterSettings& Settings_;
};

template<bool Native, bool IsTopOptional>
class TYtColumnConverter final : public IYtColumnConverter {
public:
    TYtColumnConverter(TYtColumnConverterSettings&& settings)
        : Settings_(std::move(settings))
        , DictYsonConverter_(Settings_)
        , YsonConverter_(Settings_)
        , DictPrimitiveConverter_(Settings_)
        , TopLevelYsonDictConverter_(Settings_)
        , TopLevelYsonConverter_(Settings_)
    {
        auto type = Settings_.Type;
        IsJson_ = type->IsData() && AS_TYPE(TDataType, type)->GetDataSlot() == NUdf::EDataSlot::Json
            || (Native && type->IsOptional() && AS_TYPE(TOptionalType, type)->GetItemType()->IsData()
            && AS_TYPE(TDataType, AS_TYPE(TOptionalType, type)->GetItemType())->GetDataSlot() == NUdf::EDataSlot::Json);
    }

    arrow::Datum Convert(std::shared_ptr<arrow::ArrayData> block) override {
        if (arrow::Type::DICTIONARY == block->type->id()) {
            auto valType = static_cast<const arrow::DictionaryType&>(*block->type).value_type();
            if (Settings_.IsTopLevelYson) {
                return TopLevelYsonDictConverter_.Convert(block);
            } else if (valType->Equals(Settings_.ArrowType)) {
                // just unpack
                return DictPrimitiveConverter_.Convert(block);
            }  else if (arrow::Type::UINT8 == Settings_.ArrowType->id() && arrow::Type::BOOL == valType->id()) {
                // unpack an cast
                auto result = arrow::compute::Cast(DictPrimitiveConverter_.Convert(block), Settings_.ArrowType);
                YQL_ENSURE(result.ok());
                return *result;
            } else if (IsJson_ && arrow::Type::STRING == Settings_.ArrowType->id() && arrow::Type::BINARY == valType->id()) {
                auto result = arrow::compute::Cast(DictPrimitiveConverter_.Convert(block), Settings_.ArrowType);
                YQL_ENSURE(result.ok());
                return *result;
            } else {
                return DictYsonConverter_.Convert(block);
            }
        } else {
            auto blockType = block->type;
            if (Settings_.IsTopLevelYson) {
                return TopLevelYsonConverter_.Convert(block);
            } else if (blockType->Equals(Settings_.ArrowType)) {
                return block;
            } else if (arrow::Type::UINT8 == Settings_.ArrowType->id() && arrow::Type::BOOL == blockType->id()) {
                auto result = arrow::compute::Cast(arrow::Datum(*block), Settings_.ArrowType);
                YQL_ENSURE(result.ok());
                return *result;
            } else if (IsJson_ && arrow::Type::STRING == Settings_.ArrowType->id() && arrow::Type::BINARY == blockType->id()) {
                auto result = arrow::compute::Cast(arrow::Datum(*block), Settings_.ArrowType);
                YQL_ENSURE(result.ok());
                return *result;
            } else {
                YQL_ENSURE(arrow::Type::BINARY == blockType->id());
                return YsonConverter_.Convert(block);
            }
        }
    }
private:
    TYtColumnConverterSettings Settings_;
    TComplexTypeYsonColumnConverter<Native, IsTopOptional, true> DictYsonConverter_;
    TComplexTypeYsonColumnConverter<Native, IsTopOptional, false> YsonConverter_;
    TPrimitiveColumnConverter<true, false> DictPrimitiveConverter_;
    TPrimitiveColumnConverter<true, true> TopLevelYsonDictConverter_;
    TPrimitiveColumnConverter<false, true> TopLevelYsonConverter_;
    bool IsJson_;
};

TYtColumnConverterSettings::TYtColumnConverterSettings(TType* type, const NUdf::IPgBuilder* pgBuilder, arrow::MemoryPool& pool, bool isNative)
    : Type(type)
    , PgBuilder(pgBuilder)
    , Pool(pool)
    , IsNative(isNative)
    , IsTopOptional(type->IsOptional())
    , IsTopLevelYson(type->IsData() && static_cast<TDataType*>(Type)->GetDataSlot() == NUdf::EDataSlot::Yson)
{
    if (!isNative) {
        if (Type->IsOptional()) {
            Type = static_cast<TOptionalType*>(Type)->GetItemType();
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
