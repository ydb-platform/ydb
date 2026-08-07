#include "mkql_replicate.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_custom_list.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

#include <utility>

namespace NKikimr::NMiniKQL {

namespace {

class TIterableWrapper: public TMutableComputationNode<TIterableWrapper> {
    using TBaseComputation = TMutableComputationNode<TIterableWrapper>;

public:
    class TValue: public TCustomListValue {
    public:
        class TIterator: public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue stream)
                : TComputationValue<TIterator>(memInfo)
                , Stream_(std::move(stream))
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) override {
                auto status = Stream_.Fetch(value);
                MKQL_ENSURE(status != NUdf::EFetchStatus::Yield, "Yield is not supported");
                return status != NUdf::EFetchStatus::Finish;
            }

            bool Skip() override {
                NUdf::TUnboxedValue value;
                auto status = Stream_.Fetch(value);
                MKQL_ENSURE(status != NUdf::EFetchStatus::Yield, "Yield is not supported");
                return status != NUdf::EFetchStatus::Finish;
            }

            NUdf::TUnboxedValue Stream_;
        };

        TValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, IComputationNode* stream, IComputationExternalNode* arg)
            : TCustomListValue(memInfo)
            , Ctx_(ctx)
            , Stream_(stream)
            , Arg_(arg)
        {
        }

    private:
        NUdf::TUnboxedValue GetListIterator() const override {
            auto stream = NewStream();
            return Ctx_.HolderFactory.Create<TIterator>(stream);
        }

        bool HasFastListLength() const override {
            return Length_.Defined();
        }

        ui64 GetListLength() const override {
            if (!Length_) {
                auto stream = NewStream();
                NUdf::TUnboxedValue item;
                ui64 n = 0;
                for (;;) {
                    auto status = stream.Fetch(item);
                    MKQL_ENSURE(status != NUdf::EFetchStatus::Yield, "Yield is not supported");
                    if (status == NUdf::EFetchStatus::Finish) {
                        break;
                    }

                    ++n;
                }

                Length_ = n;
            }

            return *Length_;
        }

        ui64 GetEstimatedListLength() const override {
            return GetListLength();
        }

        bool HasListItems() const override {
            if (!HasItems_) {
                if (Length_) {
                    HasItems_ = *Length_ > 0;
                } else {
                    auto stream = NewStream();
                    NUdf::TUnboxedValue item;
                    auto status = stream.Fetch(item);
                    MKQL_ENSURE(status != NUdf::EFetchStatus::Yield, "Yield is not supported");
                    HasItems_ = (status != NUdf::EFetchStatus::Finish);
                }
            }

            return *HasItems_;
        }

        NUdf::TUnboxedValue NewStream() const {
            Arg_->SetValue(Ctx_, NUdf::TUnboxedValue());
            return Stream_->GetValue(Ctx_);
        }

        TComputationContext& Ctx_;
        IComputationNode* const Stream_;
        IComputationExternalNode* const Arg_;
        mutable TMaybe<ui64> Length_;
        mutable TMaybe<bool> HasItems_;
    };

    TIterableWrapper(TComputationMutables& mutables, IComputationNode* stream, IComputationExternalNode* arg)
        : TBaseComputation(mutables)
        , Stream_(stream)
        , Arg_(arg)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TValue>(ctx, Stream_, Arg_);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Stream_);
        Own(Arg_);
    }

    IComputationNode* const Stream_;
    IComputationExternalNode* const Arg_;
};

} // namespace

IComputationNode* WrapIterable(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    const auto stream = LocateNode(ctx.NodeLocator, callable, 0);
    const auto arg = LocateExternalNode(ctx.NodeLocator, callable, 1);
    return new TIterableWrapper(ctx.Mutables, stream, arg);
}

} // namespace NKikimr::NMiniKQL
