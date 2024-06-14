#include "mkql_block_reader.h"

#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <ydb/library/yql/public/udf/udf_type_inspection.h>

#include <arrow/array/array_binary.h>
#include <arrow/chunked_array.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <typename T, bool Nullable>
class TFixedSizeBlockItemConverter : public IBlockItemConverter {
public:
    NUdf::TUnboxedValuePod MakeValue(TBlockItem item, const THolderFactory& holderFactory) const final {
        Y_UNUSED(holderFactory);
        if constexpr (Nullable) {
            if (!item) {
                return {};
            }
        }

        return NUdf::TUnboxedValuePod(item.As<T>());
    }

    TBlockItem MakeItem(const NUdf::TUnboxedValuePod& value) const final {
        if constexpr (Nullable) {
            if (!value) {
                return {};
            }
        }

        return TBlockItem(value.Get<T>());
    }
};

template <bool Nullable>
class TResourceBlockItemConverter : public IBlockItemConverter {
public:
    NUdf::TUnboxedValuePod MakeValue(TBlockItem item, const THolderFactory& holderFactory) const final {
        Y_UNUSED(holderFactory);
        if constexpr (Nullable) {
            if (!item) {
                return {};
            }
        }

        if (item.IsEmbedded()) {
            NUdf::TUnboxedValuePod embedded;
            std::memcpy(embedded.GetRawPtr(), item.GetRawPtr(), sizeof(NYql::NUdf::TUnboxedValuePod));
            return embedded;
        } else if (item.IsBoxed()) {
            return NYql::NUdf::TUnboxedValuePod(item.GetBoxed());
        } else {
            return NYql::NUdf::TUnboxedValuePod(item.AsStringValue());
        }
    }

    TBlockItem MakeItem(const NUdf::TUnboxedValuePod& value) const final {
        if constexpr (Nullable) {
            if (!value) {
                return {};
            }
        }

        if (value.IsEmbedded()) {
            TBlockItem embedded;
            std::memcpy(embedded.GetRawPtr(), value.GetRawPtr(), sizeof(TBlockItem));
            return embedded;
        } else if (value.IsBoxed()) {
            return TBlockItem(value.AsBoxed());
        } else {
            return TBlockItem(value.AsStringValue());
        }
    }
};

template<typename TStringType, bool Nullable, NUdf::EPgStringType PgString>
class TStringBlockItemConverter : public IBlockItemConverter {
public:
    void SetPgBuilder(const NUdf::IPgBuilder* pgBuilder, ui32 pgTypeId, i32 typeLen) {
        Y_ENSURE(PgString != NUdf::EPgStringType::None);
        PgBuilder = pgBuilder;
        PgTypeId = pgTypeId;
        TypeLen = typeLen;
    }

    NUdf::TUnboxedValuePod MakeValue(TBlockItem item, const THolderFactory& holderFactory) const final {
        Y_UNUSED(holderFactory);
        if constexpr (Nullable) {
            if (!item) {
                return {};
            }
        }

        if constexpr (PgString == NUdf::EPgStringType::CString) {
             return PgBuilder->MakeCString(item.AsStringRef().Data() + sizeof(void*)).Release();
        } else if constexpr (PgString == NUdf::EPgStringType::Text) {
             return PgBuilder->MakeText(item.AsStringRef().Data() + sizeof(void*)).Release();
        } else if constexpr (PgString == NUdf::EPgStringType::Fixed) {
            auto str = item.AsStringRef().Data() + sizeof(void*);
            auto len = item.AsStringRef().Size() - sizeof(void*);
            Y_DEBUG_ABORT_UNLESS(ui32(TypeLen) <= len);
            return PgBuilder->NewString(TypeLen, PgTypeId, NUdf::TStringRef(str, TypeLen)).Release();
        } else {
            return MakeString(item.AsStringRef());
        }
    }

    TBlockItem MakeItem(const NUdf::TUnboxedValuePod& value) const final {
        if constexpr (Nullable) {
            if (!value) {
                return {};
            }
        }

        if constexpr (PgString == NUdf::EPgStringType::CString) {
            auto buf = PgBuilder->AsCStringBuffer(value);
            return TBlockItem(NYql::NUdf::TStringRef(buf.Data() - sizeof(void*), buf.Size() + sizeof(void*)));
        } else if constexpr (PgString == NUdf::EPgStringType::Text) {
            auto buf = PgBuilder->AsTextBuffer(value);
            return TBlockItem(NYql::NUdf::TStringRef(buf.Data() - sizeof(void*), buf.Size() + sizeof(void*)));
        } else if constexpr (PgString == NUdf::EPgStringType::Fixed) {
            auto buf = PgBuilder->AsFixedStringBuffer(value, (ui32)TypeLen);
            return TBlockItem(NYql::NUdf::TStringRef(buf.Data() - sizeof(void*), buf.Size() + sizeof(void*)));
        } else {
            return TBlockItem(value.AsStringRef());
        }
    }

private:
    const NUdf::IPgBuilder* PgBuilder = nullptr;
    ui32 PgTypeId = 0;
    i32 TypeLen = 0;
};

template <bool Nullable>
class TTupleBlockItemConverter : public IBlockItemConverter {
public:
    TTupleBlockItemConverter(TVector<std::unique_ptr<IBlockItemConverter>>&& children)
        : Children(std::move(children))
    {
        Items.resize(Children.size());
        Unboxed.resize(Children.size());
    }

    NUdf::TUnboxedValuePod MakeValue(TBlockItem item, const THolderFactory& holderFactory) const final {
        if constexpr (Nullable) {
            if (!item) {
                return {};
            }
        }

        NUdf::TUnboxedValue* values;
        auto result = holderFactory.CreateDirectArrayHolder(Children.size(), values);
        const TBlockItem* childItems = item.AsTuple();
        for (ui32 i = 0; i < Children.size(); ++i) {
            values[i] = Children[i]->MakeValue(childItems[i], holderFactory);
        }

        return result;
    }

    TBlockItem MakeItem(const NUdf::TUnboxedValuePod& value) const final {
        if constexpr (Nullable) {
            if (!value) {
                return {};
            }
        }

        auto elements = value.GetElements();
        if (!elements) {
            for (ui32 i = 0; i < Children.size(); ++i) {
                Unboxed[i] = value.GetElement(i);
            }

            elements = Unboxed.data();
        }

        for (ui32 i = 0; i < Children.size(); ++i) {
            Items[i] = Children[i]->MakeItem(elements[i]);
        }

        return TBlockItem{ Items.data() };
    }

private:
    const TVector<std::unique_ptr<IBlockItemConverter>> Children;
    mutable TVector<NUdf::TUnboxedValue> Unboxed;
    mutable TVector<TBlockItem> Items;
};

template <typename TTzDate, bool Nullable>
class TTzDateBlockItemConverter : public IBlockItemConverter {
public:
    using TLayout = typename NYql::NUdf::TDataType<TTzDate>::TLayout;

    NUdf::TUnboxedValuePod MakeValue(TBlockItem item, const THolderFactory& holderFactory) const final {
        Y_UNUSED(holderFactory);
        if constexpr (Nullable) {
            if (!item) {
                return {};
            }
        }

        NUdf::TUnboxedValuePod value {item.Get<TLayout>()};
        value.SetTimezoneId(item.GetTimezoneId());
        return value;
    }

    TBlockItem MakeItem(const NUdf::TUnboxedValuePod& value) const final {
        if constexpr (Nullable) {
            if (!value) {
                return {};
            }
        }

        TBlockItem item {value.Get<TLayout>()};
        item.SetTimezoneId(value.GetTimezoneId());
        return item;
    }
};

class TExternalOptionalBlockItemConverter : public IBlockItemConverter {
public:
    TExternalOptionalBlockItemConverter(std::unique_ptr<IBlockItemConverter>&& inner)
        : Inner(std::move(inner))
    {}

    NUdf::TUnboxedValuePod MakeValue(TBlockItem item, const THolderFactory& holderFactory) const final {
        if (!item) {
            return {};
        }
        return Inner->MakeValue(item.GetOptionalValue(), holderFactory).MakeOptional();
    }

    TBlockItem MakeItem(const NUdf::TUnboxedValuePod& value) const final {
        if (!value) {
            return {};
        }

        return Inner->MakeItem(value.GetOptionalValue()).MakeOptional();
    }

private:
    const std::unique_ptr<IBlockItemConverter> Inner;
};

struct TConverterTraits {
    using TResult = IBlockItemConverter;
    template <bool Nullable>
    using TTuple = TTupleBlockItemConverter<Nullable>;
    template <typename T, bool Nullable>
    using TFixedSize = TFixedSizeBlockItemConverter<T, Nullable>;
    template <typename TStringType, bool Nullable, NUdf::EDataSlot TOriginal = NUdf::EDataSlot::String, NUdf::EPgStringType PgString = NUdf::EPgStringType::None>
    using TStrings = TStringBlockItemConverter<TStringType, Nullable, PgString>;
    using TExtOptional = TExternalOptionalBlockItemConverter;
    template<typename TTzDate, bool Nullable>
    using TTzDateConverter = TTzDateBlockItemConverter<TTzDate, Nullable>;

    static std::unique_ptr<TResult> MakePg(const NUdf::TPgTypeDescription& desc, const NUdf::IPgBuilder* pgBuilder) {
        if (desc.PassByValue) {
            return std::make_unique<TFixedSize<ui64, true>>();
        } else {
            if (desc.Typelen == -1) {
                auto ret = std::make_unique<TStrings<arrow::BinaryType, true, NUdf::EDataSlot::String, NUdf::EPgStringType::Text>>();
                ret->SetPgBuilder(pgBuilder, desc.TypeId, desc.Typelen);
                return ret;
            } else if (desc.Typelen == -2) {
                auto ret = std::make_unique<TStrings<arrow::BinaryType, true, NUdf::EDataSlot::String, NUdf::EPgStringType::CString>>();
                ret->SetPgBuilder(pgBuilder, desc.TypeId, desc.Typelen);
                return ret;
            } else {
                auto ret = std::make_unique<TStrings<arrow::BinaryType, true, NUdf::EDataSlot::String, NUdf::EPgStringType::Fixed>>();
                ret->SetPgBuilder(pgBuilder, desc.TypeId, desc.Typelen);
                return ret;
            }
        }
    }

    static std::unique_ptr<TResult> MakeResource(bool isOptional) {
        Y_UNUSED(isOptional);
        if (isOptional) {
            return std::make_unique<TResourceBlockItemConverter<true>>();
        } else {
            return std::make_unique<TResourceBlockItemConverter<false>>();
        }
    }

    template<typename TTzDate>
    static std::unique_ptr<TResult> MakeTzDate(bool isOptional) {
        if (isOptional) {
            return std::make_unique<TTzDateConverter<TTzDate, true>>();
        } else {
            return std::make_unique<TTzDateConverter<TTzDate, false>>();
        }
    }
};

} // namespace

std::unique_ptr<IBlockItemConverter> MakeBlockItemConverter(const NYql::NUdf::ITypeInfoHelper& typeInfoHelper, const NYql::NUdf::TType* type, const NUdf::IPgBuilder& pgBuilder) {
    return NYql::NUdf::MakeBlockReaderImpl<TConverterTraits>(typeInfoHelper, type, &pgBuilder);
}

} // namespace NMiniKQL
} // namespace NKikimr
