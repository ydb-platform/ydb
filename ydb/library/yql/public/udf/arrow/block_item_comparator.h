#pragma once

#include "block_item.h"
#include "block_reader.h"

#include <ydb/library/yql/public/udf/udf_ptr.h>
#include <ydb/library/yql/public/udf/udf_type_inspection.h>
#include <ydb/library/yql/public/udf/udf_type_size_check.h>

namespace NYql::NUdf {

// ABI stable
class IBlockItemComparator {
public:
    using TPtr = TUniquePtr<IBlockItemComparator>;

    virtual ~IBlockItemComparator() = default;

    virtual i64 Compare(TBlockItem lhs, TBlockItem rhs) const = 0;
    virtual bool Equals(TBlockItem lhs, TBlockItem rhs) const = 0;
    virtual bool Less(TBlockItem lhs, TBlockItem rhs) const = 0;
    inline bool Greater(TBlockItem lhs, TBlockItem rhs) const {
        return Less(rhs, lhs);
    }
};

UDF_ASSERT_TYPE_SIZE(IBlockItemComparator, 8);

template <typename TDerived, bool Nullable>
class TBlockItemComparatorBase : public IBlockItemComparator {
public:
    const TDerived* Derived() const {
        return static_cast<const TDerived*>(this);
    }

    // returns <0 if lhs < rhs
    i64 Compare(TBlockItem lhs, TBlockItem rhs) const final {
        if constexpr (Nullable) {
            if (lhs) {
                if (rhs) {
                    return Derived()->DoCompare(lhs, rhs);
                } else {
                    return +1;
                }
            } else {
                if (rhs) {
                    return -1;
                } else {
                    return 0;
                }
            }
        } else {
            return Derived()->DoCompare(lhs, rhs);
        }
    }

    bool Equals(TBlockItem lhs, TBlockItem rhs) const final {
        if constexpr (Nullable) {
            if (lhs) {
                if (rhs) {
                    return Derived()->DoEquals(lhs, rhs);
                } else {
                    return false;
                }
            } else {
                if (rhs) {
                    return false;
                } else {
                    return true;
                }
            }
        } else {
            return Derived()->DoEquals(lhs, rhs);
        }
    }

    bool Less(TBlockItem lhs, TBlockItem rhs) const final {
        if constexpr (Nullable) {
            if (lhs) {
                if (rhs) {
                    return Derived()->DoLess(lhs, rhs);
                }
                else {
                    return false;
                }
            } else {
                if (rhs) {
                    return true;
                } else {
                    return false;
                }
            }
        } else {
            return Derived()->DoLess(lhs, rhs);
        }
    }
};

template <typename T, bool Nullable>
class TFixedSizeBlockItemComparator : public TBlockItemComparatorBase<TFixedSizeBlockItemComparator<T, Nullable>, Nullable> {
public:
    i64 DoCompare(TBlockItem lhs, TBlockItem rhs) const {
        if constexpr (std::is_integral<T>::value && sizeof(T) < sizeof(i64)) {
            return i64(lhs.As<T>()) - i64(rhs.As<T>());
        } else {
            if constexpr (std::is_floating_point<T>::value) {
                if (std::isunordered(lhs.As<T>(), rhs.As<T>())) {
                    return i64(std::isnan(lhs.As<T>())) - i64(std::isnan(rhs.As<T>()));
                }
            }

            return (lhs.As<T>() > rhs.As<T>()) - (lhs.As<T>() < rhs.As<T>());
        }
    }

    bool DoEquals(TBlockItem lhs, TBlockItem rhs) const {
        if constexpr (std::is_floating_point<T>::value) {
            if (std::isunordered(lhs.As<T>(), rhs.As<T>())) {
                return std::isnan(lhs.As<T>()) == std::isnan(rhs.As<T>());
            }
        }
        return lhs.As<T>() == rhs.As<T>();
    }

    bool DoLess(TBlockItem lhs, TBlockItem rhs) const {
        if constexpr (std::is_floating_point<T>::value) {
            if (std::isunordered(lhs.As<T>(), rhs.As<T>())) {
                return std::isnan(lhs.As<T>()) < std::isnan(rhs.As<T>());
            }
        }
        return lhs.As<T>() < rhs.As<T>();
    }
};

template <typename TStringType, bool Nullable>
class TStringBlockItemComparator : public TBlockItemComparatorBase<TStringBlockItemComparator<TStringType, Nullable>, Nullable> {
public:
    i64 DoCompare(TBlockItem lhs, TBlockItem rhs) const {
        return lhs.AsStringRef().Compare(rhs.AsStringRef());
    }

    bool DoEquals(TBlockItem lhs, TBlockItem rhs) const {
        return lhs.AsStringRef() == rhs.AsStringRef();
    }

    bool DoLess(TBlockItem lhs, TBlockItem rhs) const {
        return lhs.AsStringRef() < rhs.AsStringRef();
    }
};

template <bool Nullable>
class TTupleBlockItemComparator : public TBlockItemComparatorBase<TTupleBlockItemComparator<Nullable>, Nullable> {
public:
    TTupleBlockItemComparator(TVector<std::unique_ptr<IBlockItemComparator>>&& children)
        : Children_(std::move(children))
    {}

public:
    i64 DoCompare(TBlockItem lhs, TBlockItem rhs) const {
        for (ui32 i = 0; i < Children_.size(); ++i) {
            auto res = Children_[i]->Compare(lhs.AsTuple()[i], rhs.AsTuple()[i]);
            if (res != 0) {
                return res;
            }
        }

        return 0;
    }

    bool DoEquals(TBlockItem lhs, TBlockItem rhs) const {
        for (ui32 i = 0; i < Children_.size(); ++i) {
            if (!Children_[i]->Equals(lhs.AsTuple()[i], rhs.AsTuple()[i])) {
                return false;
            }
        }

        return true;
    }

    bool DoLess(TBlockItem lhs, TBlockItem rhs) const {
        for (ui32 i = 0; i < Children_.size(); ++i) {
            auto res = Children_[i]->Compare(lhs.AsTuple()[i], rhs.AsTuple()[i]);
            if (res < 0) {
                return true;
            }

            if (res > 0) {
                return false;
            }
        }

        return false;
    }

private:
    const TVector<std::unique_ptr<IBlockItemComparator>> Children_;
};

class TExternalOptionalBlockItemComparator : public TBlockItemComparatorBase<TExternalOptionalBlockItemComparator, true> {
public:
    TExternalOptionalBlockItemComparator(std::unique_ptr<IBlockItemComparator> inner)
        : Inner_(std::move(inner))
    {}

    i64 DoCompare(TBlockItem lhs, TBlockItem rhs) const {
        return Inner_->Compare(lhs.GetOptionalValue(), rhs.GetOptionalValue());
    }

    bool DoEquals(TBlockItem lhs, TBlockItem rhs) const {
        return Inner_->Equals(lhs.GetOptionalValue(), rhs.GetOptionalValue());
    }

    bool DoLess(TBlockItem lhs, TBlockItem rhs) const {
        return Inner_->Less(lhs.GetOptionalValue(), rhs.GetOptionalValue());
    }

private:
    std::unique_ptr<IBlockItemComparator> Inner_;
};

}
