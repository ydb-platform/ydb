#include "kqp_fulltext_tokenize.h"

#include <ydb/core/base/fulltext.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

using namespace NKikimr::NFulltext;

namespace {

class TFulltextTokenizeWrapper : public TMutableComputationNode<TFulltextTokenizeWrapper> {
    typedef TMutableComputationNode<TFulltextTokenizeWrapper> TBaseComputation;

public:
    TFulltextTokenizeWrapper(TComputationMutables& mutables, IComputationNode* textArg, IComputationNode* settingsArg)
        : TBaseComputation(mutables)
        , TextArg(textArg)
        , SettingsArg(settingsArg)
    {
    }

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        // Get text argument
        auto text = TextArg->GetValue(ctx);
        if (!text) {
            // If text is null/empty, return empty list
            return ctx.HolderFactory.GetEmptyContainer();
        }

        TString textStr = text.AsStringRef();

        // Get settings argument (serialized proto)
        auto settingsValue = SettingsArg->GetValue(ctx);
        if (!settingsValue) {
            // If settings is null, return empty list
            return ctx.HolderFactory.GetEmptyContainer();
        }

        TString settingsStr = settingsValue.AsStringRef();

        // Deserialize settings
        Ydb::Table::FulltextIndexSettings::Analyzers analyzers;
        if (!analyzers.ParseFromString(settingsStr)) {
            // Failed to parse settings, return empty list
            return ctx.HolderFactory.GetEmptyContainer();
        }

        // Tokenize text using NKikimr::NFulltext::Analyze
        TVector<TString> tokens = Analyze(textStr, analyzers);

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
};

} // namespace

IComputationNode* WrapFulltextTokenize(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "FulltextTokenize requires exactly 2 arguments");

    auto textArg = LocateNode(ctx.NodeLocator, callable, 0);
    auto settingsArg = LocateNode(ctx.NodeLocator, callable, 1);

    return new TFulltextTokenizeWrapper(ctx.Mutables, textArg, settingsArg);
}

} // namespace NMiniKQL
} // namespace NKikimr
