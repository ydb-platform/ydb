#include "mkql_zip.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/computation/mkql_custom_list.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool All>
class TZipWrapper: public TMutableComputationNode<TZipWrapper<All>> {
    typedef TMutableComputationNode<TZipWrapper<All>> TBaseComputation;

public:
    class TValue: public TCustomListValue {
    public:
        class TIterator: public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, TUnboxedValueVector&& iters, TComputationContext& ctx)
                : TComputationValue<TIterator>(memInfo)
                , Iters(std::move(iters))
                , Ctx(ctx)
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) override {
                bool hasSome = false;
                NUdf::TUnboxedValue* items = nullptr;
                NUdf::TUnboxedValue tuple = ResTuple.NewArray(Ctx.HolderFactory, Iters.size(), items);
                for (auto& iter : Iters) {
                    if (iter) {
                        NUdf::TUnboxedValue item;
                        if (!iter.Next(item)) {
                            if (All) {
                                *items = std::move(item);
                                iter = NUdf::TUnboxedValue();
                            } else {
                                Iters.clear();
                                return false;
                            }
                        } else {
                            *items = All ? NUdf::TUnboxedValue(item.Release().MakeOptional()) : std::move(item);
                            hasSome = true;
                        }
                    } else {
                        if (All) {
                            *items = NUdf::TUnboxedValuePod();
                        } else {
                            Iters.clear();
                            return false;
                        }
                    }
                    ++items;
                }

                if (!hasSome) {
                    return false;
                }
                value = std::move(tuple);
                return true;
            }

            bool Skip() override {
                bool hasSome = false;
                for (size_t i = 0, e = Iters.size(); i < e; i++) {
                    auto& iter = Iters[i];
                    if (iter) {
                        if (!iter.Skip()) {
                            if (All) {
                                Iters[i] = NUdf::TUnboxedValue();
                            } else {
                                Iters.clear();
                                return false;
                            }
                        } else {
                            hasSome = true;
                        }
                    } else if (!All) {
                        return false;
                    }
                }

                return hasSome;
            }

            TUnboxedValueVector Iters;

            TComputationContext& Ctx;
            TPlainContainerCache ResTuple;
        };

        TValue(TMemoryUsageInfo* memInfo, TUnboxedValueVector&& lists, TComputationContext& ctx)
            : TCustomListValue(memInfo)
            , Lists(std::move(lists))
            , Ctx(ctx)
        {
            MKQL_MEM_TAKE(memInfo, &Lists, Lists.capacity() * sizeof(NUdf::TUnboxedValue));
            Y_ASSERT(!Lists.empty());
        }

        ~TValue() {
            MKQL_MEM_RETURN(GetMemInfo(), &Lists, Lists.capacity() * sizeof(NUdf::TUnboxedValue));
        }

    private:
        NUdf::TUnboxedValue GetListIterator() const override {
            if (Lists.empty()) {
                return Ctx.HolderFactory.GetEmptyContainerLazy();
            }

            TUnboxedValueVector iters;
            iters.reserve(Lists.size());
            for (auto& list : Lists) {
                iters.emplace_back(list.GetListIterator());
            }

            return Ctx.HolderFactory.Create<TIterator>(std::move(iters), Ctx);
        }

        ui64 GetListLength() const override {
            if (!Length_) {
                ui64 length = 0;
                if (!Lists.empty()) {
                    if (!All) {
                        length = Max<ui64>();
                    }

                    for (auto& list : Lists) {
                        ui64 partialLength = list.GetListLength();
                        if (All) {
                            length = Max(length, partialLength);
                        } else {
                            length = Min(length, partialLength);
                        }
                    }
                }

                Length_ = length;
            }

            return *Length_;
        }

        bool HasListItems() const override {
            if (!HasItems_) {
                HasItems_ = GetListLength() != 0;
            }

            return *HasItems_;
        }

        TUnboxedValueVector Lists;
        TComputationContext& Ctx;
    };

    TZipWrapper(TComputationMutables& mutables, TComputationNodePtrVector& lists)
        : TBaseComputation(mutables)
        , Lists(std::move(lists))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        TUnboxedValueVector listValues;
        TSmallVec<const NUdf::TUnboxedValue*, TMKQLAllocator<const NUdf::TUnboxedValue*>> arrays;
        listValues.reserve(Lists.size());
        arrays.reserve(Lists.size());
        for (auto& list : Lists) {
            listValues.emplace_back(list->GetValue(ctx));
            arrays.emplace_back(listValues.back().GetElements());
        }

        if (std::any_of(arrays.cbegin(), arrays.cend(), std::logical_not<const NUdf::TUnboxedValue*>())) {
            return ctx.HolderFactory.Create<TValue>(std::move(listValues), ctx);
        }

        TSmallVec<ui64, TMKQLAllocator<ui64>> sizes;
        sizes.reserve(listValues.size());
        std::transform(listValues.cbegin(), listValues.cend(), std::back_inserter(sizes), std::bind(&NUdf::TUnboxedValuePod::GetListLength, std::placeholders::_1));

        const auto size = *(All ? std::max_element(sizes.cbegin(), sizes.cend()) : std::min_element(sizes.cbegin(), sizes.cend()));

        if (!size) {
            return ctx.HolderFactory.GetEmptyContainerLazy();
        }

        NUdf::TUnboxedValue* listItems = nullptr;
        const auto list = ctx.HolderFactory.CreateDirectArrayHolder(size, listItems);

        for (auto i = 0U; i < size; ++i) {
            NUdf::TUnboxedValue* items = nullptr;
            *listItems++ = ctx.HolderFactory.CreateDirectArrayHolder(arrays.size(), items);
            for (auto j = 0U; j < arrays.size(); ++j) {
                if constexpr (All) {
                    if (sizes[j] > i) {
                        *items++ = *arrays[j]++;
                    } else {
                        ++items;
                    }
                } else {
                    *items++ = *arrays[j]++;
                }
            }
        }
        return list;
    }

private:
    void RegisterDependencies() const final {
        std::for_each(Lists.cbegin(), Lists.cend(), std::bind(&TZipWrapper::DependsOn, this, std::placeholders::_1));
    }

    const TComputationNodePtrVector Lists;
};

} // namespace

template <bool All>
IComputationNode* WrapZip(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    TComputationNodePtrVector lists;
    lists.reserve(callable.GetInputsCount());
    for (ui32 i = 0, e = callable.GetInputsCount(); i < e; ++i) {
        auto type = callable.GetInput(i).GetStaticType();
        MKQL_ENSURE(type->IsList() || type->IsEmptyList(), "Unexpected list type");
        lists.push_back(LocateNode(ctx.NodeLocator, callable, i));
    }

    return new TZipWrapper<All>(ctx.Mutables, lists);
}

template IComputationNode* WrapZip<false>(TCallable& callable, const TComputationNodeFactoryContext& ctx);

template IComputationNode* WrapZip<true>(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NMiniKQL
} // namespace NKikimr
