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

    TBlockItem MakeItem(NUdf::TUnboxedValuePod value) const final {
        if constexpr (Nullable) {
            if (!value) {
                return {};
            }
        }

        return TBlockItem(value.Get<T>());
    }
};

template<typename TStringType, bool Nullable>
class TStringBlockItemConverter : public IBlockItemConverter {
public:
    NUdf::TUnboxedValuePod MakeValue(TBlockItem item, const THolderFactory& holderFactory) const final {
        Y_UNUSED(holderFactory);
        if constexpr (Nullable) {
            if (!item) {
                return {};
            }
        }

        return MakeString(item.AsStringRef());
    }

    TBlockItem MakeItem(NUdf::TUnboxedValuePod value) const final {
        if constexpr (Nullable) {
            if (!value) {
                return {};
            }
        }

        return TBlockItem(value.AsStringRef());
    }
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
        auto result = Cache.NewArray(holderFactory, Children.size(), values);
        const TBlockItem* childItems = item.AsTuple();
        for (ui32 i = 0; i < Children.size(); ++i) {
            values[i] = Children[i]->MakeValue(childItems[i], holderFactory);
        }

        return result;
    }

    TBlockItem MakeItem(NUdf::TUnboxedValuePod value) const final {
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
    mutable TPlainContainerCache Cache;
    mutable TVector<NUdf::TUnboxedValue> Unboxed;
    mutable TVector<TBlockItem> Items;
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

    TBlockItem MakeItem(NUdf::TUnboxedValuePod value) const final {
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
    template <typename TStringType, bool Nullable>
    using TStrings = TStringBlockItemConverter<TStringType, Nullable>;
    using TExtOptional = TExternalOptionalBlockItemConverter;
};

} // namespace

std::unique_ptr<IBlockItemConverter> MakeBlockItemConverter(const NYql::NUdf::ITypeInfoHelper& typeInfoHelper, const NYql::NUdf::TType* type) {
    return NYql::NUdf::MakeBlockReaderImpl<TConverterTraits>(typeInfoHelper, type);
}

} // namespace NMiniKQL
} // namespace NKikimr
