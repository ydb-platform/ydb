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

template <typename T>
class TFixedSizeBlockItemConverter : public IBlockItemConverter {
public:
    NUdf::TUnboxedValuePod MakeValue(TBlockItem item, const THolderFactory& holderFactory) const final {
        Y_UNUSED(holderFactory);
        return item ? NUdf::TUnboxedValuePod(item.As<T>()) : NUdf::TUnboxedValuePod{};
    }
};

template<typename TStringType>
class TStringBlockItemConverter : public IBlockItemConverter {
public:
    NUdf::TUnboxedValuePod MakeValue(TBlockItem item, const THolderFactory& holderFactory) const final {
        Y_UNUSED(holderFactory);
        if (!item) {
            return {};
        }
        return MakeString(item.AsStringRef());
    }
};

class TTupleBlockItemConverter : public IBlockItemConverter {
public:
    TTupleBlockItemConverter(TVector<std::unique_ptr<IBlockItemConverter>>&& children)
        : Children(std::move(children))
    {}

    NUdf::TUnboxedValuePod MakeValue(TBlockItem item, const THolderFactory& holderFactory) const final {
        if (!item) {
            return {};
        }

        NUdf::TUnboxedValue* values;
        auto result = Cache.NewArray(holderFactory, Children.size(), values);
        const TBlockItem* childItems = item.AsTuple();
        for (ui32 i = 0; i < Children.size(); ++i) {
            values[i] = Children[i]->MakeValue(childItems[i], holderFactory);
        }

        return result;
    }

private:
    const TVector<std::unique_ptr<IBlockItemConverter>> Children;
    mutable TPlainContainerCache Cache;
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

private:
    const std::unique_ptr<IBlockItemConverter> Inner;
};

struct TConverterTraits {
    using TResult = IBlockItemConverter;
    using TTuple = TTupleBlockItemConverter;
    template <typename T>
    using TFixedSize = TFixedSizeBlockItemConverter<T>;
    template <typename TStringType>
    using TStrings = TStringBlockItemConverter<TStringType>;
    using TExtOptional = TExternalOptionalBlockItemConverter;
};

} // namespace

std::unique_ptr<IBlockItemConverter> MakeBlockItemConverter(const NYql::NUdf::ITypeInfoHelper& typeInfoHelper, const NYql::NUdf::TType* type) {
    return NYql::NUdf::MakeBlockReaderImpl<TConverterTraits>(typeInfoHelper, type);
}

} // namespace NMiniKQL
} // namespace NKikimr
