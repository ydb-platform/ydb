#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
namespace NKikimr::NMiniKQL::NMatchRecognize {

///Range that includes starting and ending points
///Can not be empty
class TMatchedRange {
public:
    TMatchedRange(ui64 index)
        : FromIndex(index)
        , ToIndex(index)
    {}

    TMatchedRange(ui64 from, ui64 to)
        : FromIndex(from)
        , ToIndex(to)
    {}

    size_t From() const {
        return FromIndex;
    }

    size_t To() const {
        return ToIndex;
    }

    void Extend() {
        ++ToIndex;
    }

private:
    ui64 FromIndex;
    ui64 ToIndex;
};

using TMatchedVar = std::vector<TMatchedRange>;

inline void Extend(TMatchedVar& var, size_t index) {
    if (var.empty()) {
        var.emplace_back(index);
    } else {
        MKQL_ENSURE(index > var.back().To(), "Internal logic error");
        if (var.back().To() + 1 == index) {
            var.back().Extend();
        } else {
            var.emplace_back(index);
        }
    }
}

using TMatchedVars = std::vector<TMatchedVar>;

inline NUdf::TUnboxedValue ToValue(const THolderFactory& holderFactory, const TMatchedRange& range) {
    std::array<NUdf::TUnboxedValue, 2> array = {NUdf::TUnboxedValuePod{range.From()}, NUdf::TUnboxedValuePod{range.To()}};
    return holderFactory.RangeAsArray(cbegin(array), cend(array));
}

inline NUdf::TUnboxedValue ToValue(const THolderFactory& holderFactory, const TMatchedVar& var) {
    TUnboxedValueVector data;
    data.reserve(var.size());
    for (const auto& r: var) {
        data.push_back(ToValue(holderFactory, r));
    }
    return holderFactory.VectorAsVectorHolder(std::move(data));
}

inline NUdf::TUnboxedValue ToValue(const THolderFactory& holderFactory, const TMatchedVars& vars) {
    NUdf::TUnboxedValue* ptr;
    auto result = holderFactory.CreateDirectArrayHolder(vars.size(), ptr);
    for (const auto& v: vars) {
        *ptr++ = ToValue(holderFactory, v);
    }
    return result;
}

///Optimized reference based implementation to be used as an argument
///for lambdas which produce strict result(do not require lazy access to its arguments)
class TMatchedVarsValue : public TComputationValue<TMatchedVarsValue> {
    class TRangeValue: public TComputationValue<TRangeValue> {
    public:
        TRangeValue(TMemoryUsageInfo* memInfo, const TMatchedRange& r)
                : TComputationValue<TRangeValue>(memInfo)
                , Range(r)
        {
        }

        NUdf::TUnboxedValue* GetElements() const override {
            return nullptr;
        }
        NUdf::TUnboxedValue GetElement(ui32 index) const override {
            MKQL_ENSURE(index < 2, "Index out of range");
            switch(index) {
                case 0: return NUdf::TUnboxedValuePod(Range.From());
                case 1: return NUdf::TUnboxedValuePod(Range.To());
            }
            return NUdf::TUnboxedValuePod();
        }
    private:
        const TMatchedRange& Range;
    };

    class TRangeList: public TComputationValue<TRangeList> {
        class TIterator : public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo *memInfo, const std::vector<TMatchedRange>& ranges)
                    : TComputationValue<TIterator>(memInfo)
                    , Ranges(ranges)
                    , Index(0)
            {}

        private:
            bool Next(NUdf::TUnboxedValue& value) override {
                if (Ranges.size() == Index){
                    return false;
                }
                value = NUdf::TUnboxedValuePod(new TRangeValue(GetMemInfo(), Ranges[Index++]));
                return true;
            }

            const std::vector<TMatchedRange>& Ranges;
            size_t Index;
        };

    public:
        TRangeList(TMemoryUsageInfo* memInfo, const TMatchedVar& v)
            : TComputationValue<TRangeList>(memInfo)
            , Var(v)
        {
        }

        bool HasFastListLength() const override {
            return true;
        }

        ui64 GetListLength() const override {
            return Var.size();
        }

        bool HasListItems() const override {
            return !Var.empty();
        }

        NUdf::TUnboxedValue GetListIterator() const override {
            return NUdf::TUnboxedValuePod(new TIterator(GetMemInfo(), Var));
        }
    private:
        const TMatchedVar& Var;
    };
public:
    TMatchedVarsValue(TMemoryUsageInfo* memInfo, const std::vector<TMatchedVar>& vars)
            : TComputationValue<TMatchedVarsValue>(memInfo)
            , Vars(vars)
    {
    }

    NUdf::TUnboxedValue GetElement(ui32 index) const override {
        return NUdf::TUnboxedValuePod(new TRangeList(GetMemInfo(), Vars[index]));
    }
private:
    const std::vector<TMatchedVar>& Vars;
};

}//namespace NKikimr::NMiniKQL::NMatchRecognize
