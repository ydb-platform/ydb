#pragma once
#include "mkql_match_recognize_list.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr::NMiniKQL::NMatchRecognize {

template <class R>
using TMatchedVar = std::vector<R, TMKQLAllocator<R>>;

template <class R>
void Extend(TMatchedVar<R>& var, const R& r) {
    if (var.empty()) {
        var.emplace_back(r);
    } else {
        MKQL_ENSURE(r.From() > var.back().To(), "Internal logic error");
        if (var.back().To() + 1 == r.From() && var.back().NfaIndex() == r.NfaIndex()) {
            var.back().Extend();
        } else {
            var.emplace_back(r);
        }
    }
}

template <class R>
using TMatchedVars = std::vector<TMatchedVar<R>, TMKQLAllocator<TMatchedVar<R>>>;

template <class R>
NUdf::TUnboxedValue ToValue(const THolderFactory& holderFactory, const R& range) {
    std::array<NUdf::TUnboxedValue, 2> array = {NUdf::TUnboxedValuePod{range.From()}, NUdf::TUnboxedValuePod{range.To()}};
    return holderFactory.RangeAsArray(cbegin(array), cend(array));
}

template <class R>
NUdf::TUnboxedValue ToValue(const THolderFactory& holderFactory, const TMatchedVar<R>& var) {
    TUnboxedValueVector data;
    data.reserve(var.size());
    for (const auto& r : var) {
        data.push_back(ToValue(holderFactory, r));
    }
    return holderFactory.VectorAsVectorHolder(std::move(data));
}

template <class R>
inline NUdf::TUnboxedValue ToValue(const THolderFactory& holderFactory, const TMatchedVars<R>& vars) {
    NUdf::TUnboxedValue* ptr;
    auto result = holderFactory.CreateDirectArrayHolder(vars.size(), ptr);
    for (const auto& v : vars) {
        *ptr++ = ToValue(holderFactory, v);
    }
    return result;
}

/// Optimized reference based implementation to be used as an argument
/// for lambdas which produce strict result(do not require lazy access to its arguments)
template <class R>
class TMatchedVarsValue: public TComputationValue<TMatchedVarsValue<R>> {
    class TRangeList: public TComputationValue<TRangeList> {
        class TIterator: public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, const THolderFactory& holderFactory, const std::vector<R, TMKQLAllocator<R>>& ranges)
                : TComputationValue<TIterator>(memInfo)
                , HolderFactory_(holderFactory)
                , Ranges_(ranges)
                , Index_(0)
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) override {
                if (Ranges_.size() == Index_) {
                    return false;
                }
                value = ToValue(HolderFactory_, Ranges_[Index_++]);
                return true;
            }
            const THolderFactory& HolderFactory_;
            const std::vector<R, TMKQLAllocator<R>>& Ranges_;
            size_t Index_;
        };

    public:
        TRangeList(TMemoryUsageInfo* memInfo, const THolderFactory& holderFactory, const TMatchedVar<R>& v)
            : TComputationValue<TRangeList>(memInfo)
            , HolderFactory_(holderFactory)
            , Var_(v)
        {
        }

        bool HasFastListLength() const override {
            return true;
        }

        ui64 GetListLength() const override {
            return Var_.size();
        }

        bool HasListItems() const override {
            return !Var_.empty();
        }

        NUdf::TUnboxedValue GetListIterator() const override {
            return HolderFactory_.Create<TIterator>(HolderFactory_, Var_);
        }

    private:
        const THolderFactory& HolderFactory_;
        const TMatchedVar<R>& Var_;
    };

public:
    TMatchedVarsValue(TMemoryUsageInfo* memInfo, const THolderFactory& holderFactory, const std::vector<TMatchedVar<R>, TMKQLAllocator<TMatchedVar<R>>>& vars)
        : TComputationValue<TMatchedVarsValue>(memInfo)
        , HolderFactory_(holderFactory)
        , Vars_(vars)
    {
    }

    NUdf::TUnboxedValue GetElement(ui32 index) const override {
        return HolderFactory_.Create<TRangeList>(HolderFactory_, Vars_[index]);
    }

private:
    const THolderFactory& HolderFactory_;
    const std::vector<TMatchedVar<R>, TMKQLAllocator<TMatchedVar<R>>>& Vars_;
};

} // namespace NKikimr::NMiniKQL::NMatchRecognize
