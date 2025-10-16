#include "kqp_fulltext_analyze.h"

#include <ydb/core/base/fulltext.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

using namespace NKikimr::NFulltext;

namespace {

class TFulltextAnalyzeWrapper : public TMutableComputationNode<TFulltextAnalyzeWrapper> {
    typedef TMutableComputationNode<TFulltextAnalyzeWrapper> TBaseComputation;
    using TSettings = Ydb::Table::FulltextIndexSettings::Analyzers;

public:
    TFulltextAnalyzeWrapper(TComputationMutables& mutables, IComputationNode* textArg, IComputationNode* settingsArg)
        : TBaseComputation(mutables)
        , TextArg(textArg)
        , SettingsArg(settingsArg)
        , CachedSettingsIndex(mutables.CurValueIndex++)
    {
    }

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        auto text = TextArg->GetValue(ctx);
        if (!text) {
            // If text is null/empty, return empty list
            return ctx.HolderFactory.GetEmptyContainerLazy();
        }

        TSettings* settings = nullptr;
        auto& cachedSettings = ctx.MutableValues[CachedSettingsIndex];
        if (cachedSettings.IsInvalid()) {
            // First time - get, parse, and cache the settings
            auto settingsProto = SettingsArg->GetValue(ctx);
            if (!settingsProto) {
                // If settings is null, return empty list
                return ctx.HolderFactory.GetEmptyContainerLazy();
            }

            auto analyzersPtr = std::make_unique<TSettings>();
            if (!analyzersPtr->ParseFromString(settingsProto.AsStringRef())) {
                // Failed to parse settings, return empty list
                return ctx.HolderFactory.GetEmptyContainerLazy();
            }
            
            settings = analyzersPtr.get();
            cachedSettings = NUdf::TUnboxedValuePod(reinterpret_cast<ui64>(analyzersPtr.release()));
        } else {
            // Reuse cached settings
            settings = reinterpret_cast<TSettings*>(static_cast<ui64>(cachedSettings.Get<ui64>()));
        }

        // Tokenize text using NKikimr::NFulltext::Analyze
        TVector<TString> tokens = Analyze(TString(text.AsStringRef()), *settings);

        // Convert tokens to TUnboxedValue list
        NUdf::TUnboxedValue* items = nullptr;
        auto result = ctx.HolderFactory.CreateDirectArrayHolder(tokens.size(), items);
        for (size_t i = 0; i < tokens.size(); ++i) {
            items[i] = MakeString(tokens[i]);
        }

        return result;
    }

private:
    void RegisterDependencies() const final {
        DependsOn(TextArg);
        DependsOn(SettingsArg);
    }

    IComputationNode* const TextArg;
    IComputationNode* const SettingsArg;
    const ui32 CachedSettingsIndex;
};

} // namespace

IComputationNode* WrapFulltextAnalyze(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "FulltextAnalyze requires exactly 2 arguments");

    auto textArg = LocateNode(ctx.NodeLocator, callable, 0);
    auto settingsArg = LocateNode(ctx.NodeLocator, callable, 1);

    return new TFulltextAnalyzeWrapper(ctx.Mutables, textArg, settingsArg);
}

} // namespace NMiniKQL
} // namespace NKikimr
