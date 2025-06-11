#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

#include <yql/essentials/minikql/comp_nodes/mkql_rh_hash.h>

#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/defs.h>

namespace NKikimr {
namespace NMiniKQL {
namespace NDqHashOperatorCommon {

extern TStatKey DqHashCombine_FlushesCount;
extern TStatKey DqHashCombine_MaxRowsCount;

using TDependsOn = std::function<void(IComputationNode*)>;
using TOwn = std::function<void(IComputationExternalNode*)>;

struct TCombinerNodes {
    TComputationExternalNodePtrVector ItemNodes, KeyNodes, StateNodes, FinishNodes;
    TComputationNodePtrVector KeyResultNodes, InitResultNodes, UpdateResultNodes, FinishResultNodes;

    std::vector<bool> PasstroughtItems;

    void BuildMaps() {
        PasstroughtItems.resize(ItemNodes.size());
        auto anyResults = KeyResultNodes;
        anyResults.insert(anyResults.cend(), InitResultNodes.cbegin(), InitResultNodes.cend());
        anyResults.insert(anyResults.cend(), UpdateResultNodes.cbegin(), UpdateResultNodes.cend());
        const auto itemsOnResults = GetPasstroughtMap(ItemNodes, anyResults);
        std::transform(itemsOnResults.cbegin(), itemsOnResults.cend(), PasstroughtItems.begin(), [](const TPasstroughtMap::value_type& v) { return v.has_value(); });
    }

    bool IsInputItemNodeUsed(size_t i) const {
        return (ItemNodes[i]->GetDependencesCount() > 0U || PasstroughtItems[i]);
    }

    NUdf::TUnboxedValue* GetUsedInputItemNodePtrOrNull(TComputationContext& ctx, size_t i) const {
        return IsInputItemNodeUsed(i) ?
               &ItemNodes[i]->RefValue(ctx) :
               nullptr;
    }

    void ExtractKey(TComputationContext& ctx, NUdf::TUnboxedValue** values, NUdf::TUnboxedValue* keys) const {
        std::for_each(ItemNodes.cbegin(), ItemNodes.cend(), [&](IComputationExternalNode* item) {
            if (const auto pointer = *values++)
                item->SetValue(ctx, std::move(*pointer));
        });
        for (ui32 i = 0U; i < KeyNodes.size(); ++i) {
            auto& key = KeyNodes[i]->RefValue(ctx);
            *keys++ = key = KeyResultNodes[i]->GetValue(ctx);
        }
    }

    void ConsumeRawData(TComputationContext& /*ctx*/, NUdf::TUnboxedValue* keys, NUdf::TUnboxedValue** from, NUdf::TUnboxedValue* to) const {
        std::fill_n(keys, KeyResultNodes.size(), NUdf::TUnboxedValuePod());
        for (ui32 i = 0U; i < ItemNodes.size(); ++i) {
            if (from[i] && IsInputItemNodeUsed(i)) {
                to[i] = std::move(*(from[i]));
            }
        }
    }

    void ExtractRawData(TComputationContext& ctx, NUdf::TUnboxedValue* from, NUdf::TUnboxedValue* keys) const {
        for (ui32 i = 0U; i != ItemNodes.size(); ++i) {
            if (IsInputItemNodeUsed(i)) {
                ItemNodes[i]->SetValue(ctx, std::move(from[i]));
            }
        }
        for (ui32 i = 0U; i < KeyNodes.size(); ++i) {
            auto& key = KeyNodes[i]->RefValue(ctx);
            *keys++ = key = KeyResultNodes[i]->GetValue(ctx);
        }
    }

    void ProcessItem(TComputationContext& ctx, NUdf::TUnboxedValue* keys, NUdf::TUnboxedValue* state) const {
        if (keys) {
            std::fill_n(keys, KeyResultNodes.size(), NUdf::TUnboxedValuePod());
            auto source = state;
            std::for_each(StateNodes.cbegin(), StateNodes.cend(), [&](IComputationExternalNode* item){ item->SetValue(ctx, std::move(*source++)); });
            std::transform(UpdateResultNodes.cbegin(), UpdateResultNodes.cend(), state, [&](IComputationNode* node) { return node->GetValue(ctx); });
        } else {
            std::transform(InitResultNodes.cbegin(), InitResultNodes.cend(), state, [&](IComputationNode* node) { return node->GetValue(ctx); });
        }
    }

    void FinishItem(TComputationContext& ctx, NUdf::TUnboxedValue* state, NUdf::TUnboxedValue*const* output) const {
        std::for_each(FinishNodes.cbegin(), FinishNodes.cend(), [&](IComputationExternalNode* item) { item->SetValue(ctx, std::move(*state++)); });
        for (const auto node : FinishResultNodes)
            if (const auto out = *output++)
                *out = node->GetValue(ctx);
    }

    void RegisterDependencies(const TDependsOn& dependsOn, const TOwn& own) const {
        std::for_each(ItemNodes.cbegin(), ItemNodes.cend(), own);
        std::for_each(KeyNodes.cbegin(), KeyNodes.cend(), own);
        std::for_each(StateNodes.cbegin(), StateNodes.cend(), own);
        std::for_each(FinishNodes.cbegin(), FinishNodes.cend(), own);

        std::for_each(KeyResultNodes.cbegin(), KeyResultNodes.cend(), dependsOn);
        std::for_each(InitResultNodes.cbegin(), InitResultNodes.cend(), dependsOn);
        std::for_each(UpdateResultNodes.cbegin(), UpdateResultNodes.cend(), dependsOn);
        std::for_each(FinishResultNodes.cbegin(), FinishResultNodes.cend(), dependsOn);
    }
};

}
}
}