#include "mkql_counting_test_stream.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMiniKQL::NTest {
namespace {

class TTestStreamWrapper: public TMutableComputationNode<TTestStreamWrapper> {
    using TBaseComputation = TMutableComputationNode<TTestStreamWrapper>;

public:
    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, TTestStreamParams& params)
            : TBase(memInfo)
            , CompCtx_(compCtx)
            , Params_(params)
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            ++TotalFetches_;
            ++Params_.Fetches;

            UNIT_ASSERT_LE(TotalFetches_, Params_.MaxAllowedNumberOfFetches);

            if (TotalFetches_ > Params_.StreamSize) {
                return NUdf::EFetchStatus::Finish;
            }

            NUdf::TUnboxedValue* items = nullptr;
            result = CompCtx_.HolderFactory.CreateDirectArrayHolder(2, items);
            items[0] = NUdf::TUnboxedValuePod(TotalFetches_);
            items[1] = MakeString(ToString(TotalFetches_) * 5);

            return NUdf::EFetchStatus::Ok;
        }

        TComputationContext& CompCtx_;
        TTestStreamParams& Params_;
        ui64 TotalFetches_ = 0;
    };

    TTestStreamWrapper(TComputationMutables& mutables, TTestStreamParams& params)
        : TBaseComputation(mutables)
        , Params_(params)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(ctx, Params_);
    }

private:
    void RegisterDependencies() const final {
    }

    TTestStreamParams& Params_;
};

} // namespace

IComputationNode* WrapTestStream(const TComputationNodeFactoryContext& ctx, TTestStreamParams& params) {
    return new TTestStreamWrapper(ctx.Mutables, params);
}

} // namespace NKikimr::NMiniKQL::NTest
