#include "yql_mkql_ungrouping_list.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_custom_list.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;

namespace {

class TYtUngroupingListWrapper : public TMutableComputationNode<TYtUngroupingListWrapper> {
    typedef TMutableComputationNode<TYtUngroupingListWrapper> TBaseComputation;
public:
    class TListValue : public TCustomListValue {
    public:
        class TIterator : public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, TComputationContext& ctx, NUdf::TUnboxedValue&& iter, IComputationExternalNode* isKeySwitchNode)
                : TComputationValue<TIterator>(memInfo)
                , ListIterator(std::move(iter))
                , IsKeySwitchNode(isKeySwitchNode)
                , Ctx(ctx)
            {}

        private:
            bool Next(NUdf::TUnboxedValue& value) final {
                for (;;) {
                    if (IsKeySwitchNode) {
                        Ctx.MutableValues[IsKeySwitchNode->GetIndex()] = NUdf::TUnboxedValuePod(!SubListIterator);
                    }
                    if (!SubListIterator) {
                        NUdf::TUnboxedValue pair;
                        if (!ListIterator.Next(pair)) {
                            return false;
                        }
                        SubListIterator = pair.GetListIterator();
                    }
                    if (SubListIterator.Next(value)) {
                        return true;
                    } else {
                        SubListIterator.Clear();
                    }
                }
            }

            const NUdf::TUnboxedValue ListIterator;
            NUdf::TUnboxedValue SubListIterator;
            IComputationExternalNode* const IsKeySwitchNode;
            TComputationContext& Ctx;
        };

        TListValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& list, IComputationExternalNode* isKeySwitchNode, TComputationContext& ctx)
            : TCustomListValue(memInfo), List(std::move(list)), IsKeySwitchNode(isKeySwitchNode), Ctx(ctx)
        {}

    private:
        NUdf::TUnboxedValue GetListIterator() const override {
            return Ctx.HolderFactory.Create<TIterator>(Ctx, List.GetListIterator(), IsKeySwitchNode);
        }

    private:
        const NUdf::TUnboxedValue List;
        IComputationExternalNode* const IsKeySwitchNode;
        TComputationContext& Ctx;
    };

    TYtUngroupingListWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationExternalNode* isKeySwitchNode)
        : TBaseComputation(mutables)
        , List_(list)
        , IsKeySwitchNode_(isKeySwitchNode)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TListValue>(List_->GetValue(ctx), IsKeySwitchNode_, ctx);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(List_);
        Own(IsKeySwitchNode_);
    }

    IComputationNode* const List_;
    IComputationExternalNode* const IsKeySwitchNode_;
};

} // unnamed

///////////////////////////////////////////////////////////////////////////////////////////////////////

IComputationNode* WrapYtUngroupingList(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    YQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");
    const auto listItemType = AS_TYPE(TListType, callable.GetInput(0))->GetItemType();
    const auto subListItemType = AS_TYPE(TListType, listItemType)->GetItemType();
    YQL_ENSURE(subListItemType->IsStruct() || subListItemType->IsVariant());

    const auto list = LocateNode(ctx.NodeLocator, callable, 0);
    const auto isKeySwitch = LocateExternalNode(ctx.NodeLocator, callable, 1, false);
    return new TYtUngroupingListWrapper(ctx.Mutables, list, isKeySwitch);
}

} // NYql
