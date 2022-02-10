#include "mkql_replicate.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_custom_list.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TIterableWrapper : public TMutableComputationNode<TIterableWrapper> {
    typedef TMutableComputationNode<TIterableWrapper> TBaseComputation;
public:
    class TValue : public TCustomListValue {
    public:
        class TIterator : public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, const NUdf::TUnboxedValue& stream)
                : TComputationValue<TIterator>(memInfo)
                , Stream(stream)
            {}

        private:
            bool Next(NUdf::TUnboxedValue& value) override {
                auto status = Stream.Fetch(value);
                MKQL_ENSURE(status != NUdf::EFetchStatus::Yield, "Yield is not supported");
                return status != NUdf::EFetchStatus::Finish;
            }

            bool Skip() override {
                NUdf::TUnboxedValue value;
                auto status = Stream.Fetch(value);
                MKQL_ENSURE(status != NUdf::EFetchStatus::Yield, "Yield is not supported");
                return status != NUdf::EFetchStatus::Finish;
            }

            NUdf::TUnboxedValue Stream;
        };

        TValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, IComputationNode* stream, IComputationExternalNode* arg)
            : TCustomListValue(memInfo)
            , Ctx(ctx)
            , Stream(stream)
            , Arg(arg)
        {
        }

    private:
        NUdf::TUnboxedValue GetListIterator() const override {
            auto stream = NewStream();
            return Ctx.HolderFactory.Create<TIterator>(stream);
        }

        bool HasFastListLength() const override {
            return Length.Defined();
        }

        ui64 GetListLength() const override {
            if (!Length) {
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

                Length = n;
            }

            return *Length;
        }

        ui64 GetEstimatedListLength() const override {
            return GetListLength();
        }

        bool HasListItems() const override {
            if (!HasItems) {
                if (Length) {
                    HasItems = *Length > 0;
                } else {
                    auto stream = NewStream();
                    NUdf::TUnboxedValue item;
                    auto status = stream.Fetch(item);
                    MKQL_ENSURE(status != NUdf::EFetchStatus::Yield, "Yield is not supported");
                    HasItems = (status != NUdf::EFetchStatus::Finish);
                }
            }

            return *HasItems;
        }

        NUdf::TUnboxedValue NewStream() const {
            Arg->SetValue(Ctx, NUdf::TUnboxedValue());
            return Stream->GetValue(Ctx);
        }

        TComputationContext& Ctx;
        IComputationNode* const Stream;
        IComputationExternalNode* const Arg;
        mutable TMaybe<ui64> Length;
        mutable TMaybe<bool> HasItems;
    };

    TIterableWrapper(TComputationMutables& mutables, IComputationNode* stream, IComputationExternalNode* arg)
        : TBaseComputation(mutables)
        , Stream(stream)
        , Arg(arg)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TValue>(ctx, Stream, Arg);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Stream);
        Arg->AddDependence(Stream);
    }

    IComputationNode* const Stream;
    IComputationExternalNode* const Arg;
};

}

IComputationNode* WrapIterable(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    const auto stream = LocateNode(ctx.NodeLocator, callable, 0);
    const auto arg = LocateExternalNode(ctx.NodeLocator, callable, 1);
    return new TIterableWrapper(ctx.Mutables, stream, arg);
}

}
}
