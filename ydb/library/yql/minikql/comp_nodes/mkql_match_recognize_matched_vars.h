#pragma once
#include "../computation/mkql_computation_node_impl.h"

namespace NKikimr::NMiniKQL::NMatchRecognize {

using TMatchedRange = std::pair<ui64, ui64>;

using TMatchedVar = std::vector<TMatchedRange>;

using TMatchedVars = std::vector<TMatchedVar>;

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
                case 0: return NUdf::TUnboxedValuePod(Range.first);
                case 1: return NUdf::TUnboxedValuePod(Range.second);
            }
            return NUdf::TUnboxedValuePod();
        }
    private:
        const TMatchedRange& Range;
    };

    class TListRangeValue: public TComputationValue<TListRangeValue> {
    public:
        TListRangeValue(TMemoryUsageInfo* memInfo, const TMatchedVar& v)
                : TComputationValue<TListRangeValue>(memInfo)
                , Var(v)
        {
        }
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
        return NUdf::TUnboxedValuePod(new TListRangeValue(GetMemInfo(), Vars[index]));
    }
private:
    const std::vector<TMatchedVar>& Vars;
};

}//namespace NKikimr::NMiniKQL::NMatchRecognize
