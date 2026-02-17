#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr::NMiniKQL {

class TCustomListValue: public TComputationValue<TCustomListValue> {
public:
    explicit TCustomListValue(TMemoryUsageInfo* memInfo)
        : TComputationValue(memInfo)
        , Length(Length_)
        , HasItems(HasItems_)
    {
    }

private:
    bool HasFastListLength() const override {
        return bool(Length_);
    }

    ui64 GetListLength() const override {
        if (!Length_) {
            ui64 length = Iterator_ ? 1ULL : 0ULL;
            for (const auto it = Iterator_ ? std::move(Iterator_) : NUdf::TBoxedValueAccessor::GetListIterator(*this); it.Skip();) {
                ++length;
            }

            Length_ = length;
        }

        return *Length_;
    }

    ui64 GetEstimatedListLength() const override {
        return GetListLength();
    }

    bool HasListItems() const override {
        if (HasItems_) {
            return *HasItems_;
        }

        if (Length_) {
            HasItems_ = (*Length_ != 0);
            return *HasItems_;
        }

        auto iter = NUdf::TBoxedValueAccessor::GetListIterator(*this);
        HasItems_ = iter.Skip();
        if (*HasItems_) {
            Iterator_ = std::move(iter);
        }
        return *HasItems_;
    }

protected:
    mutable std::optional<ui64> Length_;
    mutable std::optional<bool> HasItems_;
    mutable NUdf::TUnboxedValue Iterator_;

    // FIXME Remove
    std::optional<ui64>& Length;   // NOLINT(readability-identifier-naming)
    std::optional<bool>& HasItems; // NOLINT(readability-identifier-naming)
};

class TForwardListValue: public TCustomListValue {
public:
    class TIterator: public TComputationValue<TIterator> {
    public:
        TIterator(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& stream);

    private:
        bool Next(NUdf::TUnboxedValue& value) override;

        const NUdf::TUnboxedValue Stream_;
    };

    TForwardListValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& stream);

private:
    NUdf::TUnboxedValue GetListIterator() const override;

    mutable NUdf::TUnboxedValue Stream_;
};

class TExtendListValue: public TCustomListValue {
public:
    class TIterator: public TComputationValue<TIterator> {
    public:
        TIterator(TMemoryUsageInfo* memInfo, TUnboxedValueVector&& iters);
        ~TIterator() override;

    private:
        bool Next(NUdf::TUnboxedValue& value) override;
        bool Skip() override;

        const TUnboxedValueVector Iters_;
        ui32 Index_;
    };

    TExtendListValue(TMemoryUsageInfo* memInfo, TUnboxedValueVector&& lists);

    ~TExtendListValue() override;

private:
    NUdf::TUnboxedValue GetListIterator() const override;
    ui64 GetListLength() const override;
    bool HasListItems() const override;

    const TUnboxedValueVector Lists_;
};

class TExtendStreamValue: public TComputationValue<TExtendStreamValue> {
public:
    using TBase = TComputationValue<TExtendStreamValue>;

    TExtendStreamValue(TMemoryUsageInfo* memInfo, TUnboxedValueVector&& lists);

    ~TExtendStreamValue() override;

private:
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& value) override;

    const TUnboxedValueVector Lists_;
    ui32 Index_ = 0;
};

} // namespace NKikimr::NMiniKQL
