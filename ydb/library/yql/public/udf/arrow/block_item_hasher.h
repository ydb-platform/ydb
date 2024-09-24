#pragma once

#include "block_item.h"

#include <ydb/library/yql/public/udf/udf_ptr.h>
#include <ydb/library/yql/public/udf/udf_type_inspection.h>
#include <ydb/library/yql/public/udf/udf_type_ops.h>
#include <ydb/library/yql/public/udf/udf_type_size_check.h>

namespace NYql::NUdf {

// ABI stable
class IBlockItemHasher {
public:
    using TPtr = TUniquePtr<IBlockItemHasher>;

    virtual ~IBlockItemHasher() = default;
    virtual ui64 Hash(TBlockItem value) const = 0;
};

UDF_ASSERT_TYPE_SIZE(IBlockItemHasher, 8);

template <typename TDerived, bool Nullable>
class TBlockItemHasherBase : public IBlockItemHasher {
public:
    const TDerived* Derived() const {
        return static_cast<const TDerived*>(this);
    }

    ui64 Hash(TBlockItem value) const final {
        // keep hash computation in sync with
        // ydb/library/yql/minikql/mkql_type_builder.cpp: THash<NMiniKQL::TType::EKind::Optional>::Hash()
        if constexpr (Nullable) {
            if (!value) {
                return 0;
            }
            return CombineHashes(ui64(1), Derived()->DoHash(value));
        } else {
            return Derived()->DoHash(value);
        }
    }
};

template <typename T, bool Nullable>
class TFixedSizeBlockItemHasher : public TBlockItemHasherBase<TFixedSizeBlockItemHasher<T, Nullable>, Nullable> {
public:
    ui64 DoHash(TBlockItem value) const {
        return GetValueHash<TDataType<T>::Slot>(NUdf::TUnboxedValuePod(value.As<T>()));
    }
};

template <bool Nullable>
class TFixedSizeBlockItemHasher<NYql::NDecimal::TInt128, Nullable> : public TBlockItemHasherBase<TFixedSizeBlockItemHasher<NYql::NDecimal::TInt128, Nullable>, Nullable> {
public:
    ui64 DoHash(TBlockItem value) const {
        return GetValueHash<TDataType<NUdf::TDecimal>::Slot>(NUdf::TUnboxedValuePod(value.GetInt128()));
    }
};

template <typename T, bool Nullable>
class TTzDateBlockItemHasher : public TBlockItemHasherBase<TTzDateBlockItemHasher<T, Nullable>, Nullable> {
public:
    ui64 DoHash(TBlockItem value) const {
        using TLayout = typename TDataType<T>::TLayout;
        TUnboxedValuePod uv {value.Get<TLayout>()};
        uv.SetTimezoneId(value.GetTimezoneId());
        return GetValueHash<TDataType<T>::Slot>(uv);
    }
};

template <typename TStringType, bool Nullable>
class TStringBlockItemHasher : public TBlockItemHasherBase<TStringBlockItemHasher<TStringType, Nullable>, Nullable> {
public:
    ui64 DoHash(TBlockItem value) const {
        return GetStringHash(value.AsStringRef());
    }
};

template <bool Nullable>
class TTupleBlockItemHasher : public TBlockItemHasherBase<TTupleBlockItemHasher<Nullable>, Nullable> {
public:
    TTupleBlockItemHasher(TVector<std::unique_ptr<IBlockItemHasher>>&& children)
        : Children_(std::move(children))
    {}

    ui64 DoHash(TBlockItem value) const {
        // keep hash computation in sync with
        // ydb/library/yql/minikql/mkql_type_builder.cpp: TVectorHash::Hash()
        ui64 result = 0ULL;
        auto elements = value.GetElements();
        for (ui32 i = 0; i < Children_.size(); ++i) {
            result = CombineHashes(result, Children_[i]->Hash(elements[i]));
        }
        return result;
    }

private:
    const TVector<std::unique_ptr<IBlockItemHasher>> Children_;
};

class TExternalOptionalBlockItemHasher : public TBlockItemHasherBase<TExternalOptionalBlockItemHasher, true> {
public:
    TExternalOptionalBlockItemHasher(std::unique_ptr<IBlockItemHasher>&& inner)
        : Inner_(std::move(inner))
    {}

    ui64 DoHash(TBlockItem value) const {
        return Inner_->Hash(value.GetOptionalValue());
    }

private:
    const std::unique_ptr<IBlockItemHasher> Inner_;
};

}
