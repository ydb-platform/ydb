#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr {
namespace NMiniKQL {

class TCustomListValue : public TComputationValue<TCustomListValue> {
public:
    TCustomListValue(TMemoryUsageInfo* memInfo)
        : TComputationValue(memInfo)
    {
    }

private:
    bool HasFastListLength() const override {
        return bool(Length);
    }

    ui64 GetListLength() const override {
        if (!Length) {
            ui64 length = Iterator ? 1ULL : 0ULL;
            for (const auto it = Iterator ? std::move(Iterator) : NUdf::TBoxedValueAccessor::GetListIterator(*this); it.Skip();) {
                ++length;
            }

            Length = length;
        }

        return *Length;
    }

    ui64 GetEstimatedListLength() const override {
        return GetListLength();
    }

    bool HasListItems() const override {
        if (HasItems) {
            return *HasItems;
        }

        if (Length) {
            HasItems = (*Length != 0);
            return *HasItems;
        }

        auto iter = NUdf::TBoxedValueAccessor::GetListIterator(*this);
        HasItems = iter.Skip();
        if (*HasItems) {
            Iterator = std::move(iter);
        }
        return *HasItems;
    }

protected:
    mutable std::optional<ui64> Length;
    mutable std::optional<bool> HasItems;
    mutable NUdf::TUnboxedValue Iterator;
};

class TForwardListValue : public TCustomListValue {
public:
    class TIterator : public TComputationValue<TIterator> {
    public:
        TIterator(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& stream);

    private:
        bool Next(NUdf::TUnboxedValue& value) override;

        const NUdf::TUnboxedValue Stream;
    };

    TForwardListValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& stream);

private:
    NUdf::TUnboxedValue GetListIterator() const override;

    mutable NUdf::TUnboxedValue Stream;
};

class TExtendListValue : public TCustomListValue {
public:
    class TIterator : public TComputationValue<TIterator> {
    public:
        TIterator(TMemoryUsageInfo* memInfo, TUnboxedValueVector&& iters);
        ~TIterator();

    private:
        bool Next(NUdf::TUnboxedValue& value) override;
        bool Skip() override;

        const TUnboxedValueVector Iters;
        ui32 Index;
    };

    TExtendListValue(TMemoryUsageInfo* memInfo, TUnboxedValueVector&& lists);

    ~TExtendListValue();

private:
    NUdf::TUnboxedValue GetListIterator() const override;
    ui64 GetListLength() const override;
    bool HasListItems() const override;

    const TUnboxedValueVector Lists;
};

class TExtendStreamValue : public TComputationValue<TExtendStreamValue> {
public:
    using TBase = TComputationValue<TExtendStreamValue>;

    TExtendStreamValue(TMemoryUsageInfo* memInfo, TUnboxedValueVector&& lists);

    ~TExtendStreamValue();

private:
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& value);

    const TUnboxedValueVector Lists;
    ui32 Index = 0;
};

}
}
