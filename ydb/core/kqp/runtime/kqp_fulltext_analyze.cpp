#include "kqp_fulltext_analyze.h"

#include <ydb/core/base/fulltext.h>

#include <ydb/core/base/json_index.h>
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

    struct TSettings : public TComputationValue<TSettings> {
        using TComputationValue::TComputationValue;

        bool IsValid = false;
        Ydb::Table::FulltextIndexSettings::Analyzers Analyzers;
    };

    enum class EMode {
        Fulltext = 0,
        JsonIndexOverJson = 1,
        JsonIndexOverJsonDocument = 2,
    };

public:
    TFulltextAnalyzeWrapper(TComputationMutables& mutables, IComputationNode* textArg, IComputationNode* settingsArg, IComputationNode* modeArg)
        : TBaseComputation(mutables)
        , TextArg(textArg)
        , SettingsArg(settingsArg)
        , ModeArg(modeArg)
        , CachedSettingsIndex(mutables.CurValueIndex++)
    {
    }

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        auto text = TextArg->GetValue(ctx);
        if (!text) {
            // If text is null/empty, return empty list
            return ctx.HolderFactory.GetEmptyContainerLazy();
        }

        const auto mode = GetMode(ctx);

        // Tokenize text
        TVector<TString> tokens;
        if (mode == EMode::Fulltext) {
            auto& settings = GetSettings(ctx);
            if (!settings.IsValid) {
                // Failed to parse settings, return empty list
                return ctx.HolderFactory.GetEmptyContainerLazy();
            }
            tokens = Analyze(TString(text.AsStringRef()), settings.Analyzers);
        } else if (mode == EMode::JsonIndexOverJson) {
            TString error;
            tokens = NJsonIndex::TokenizeJson(text.AsStringRef(), error);
            if (!error.empty()) {
                // Failed to tokenize JSON, return empty list
                return ctx.HolderFactory.GetEmptyContainerLazy();
            }
        } else if (mode == EMode::JsonIndexOverJsonDocument) {
            tokens = NJsonIndex::TokenizeBinaryJson(text.AsStringRef());
        }

        THashMap<TString, ui32> tokenFreq;
        for (const auto& token : tokens) {
            tokenFreq[token]++;
        }

        // Convert tokens to TUnboxedValue struct (frequency, token) list
        // Frequency comes before token because 'f' < 't'
        NUdf::TUnboxedValue* rows = nullptr;
        auto result = ctx.HolderFactory.CreateDirectArrayHolder(tokenFreq.size(), rows);
        size_t i = 0;
        for (const auto& [token, freq] : tokenFreq) {
            NUdf::TUnboxedValue* rowItems = nullptr;
            auto newValue = ctx.HolderFactory.CreateDirectArrayHolder(2, rowItems);
            rowItems[0] = NUdf::TUnboxedValuePod(freq);
            rowItems[1] = MakeString(token);
            rows[i++] = newValue;
        }

        return result;
    }

    TSettings& GetSettings(TComputationContext& ctx) const {
        auto& cachedSettings = ctx.MutableValues[CachedSettingsIndex];
        if (cachedSettings.IsInvalid()) {
            // First time - get, parse, and cache the settings
            cachedSettings = ctx.HolderFactory.Create<TSettings>();
            auto& settings = GetSettings(cachedSettings);
            auto settingsProto = SettingsArg->GetValue(ctx);
            settings.IsValid = settings.Analyzers.ParseFromString(settingsProto.AsStringRef());
            return settings;
        } else {
            // Return cached settings
            return GetSettings(cachedSettings);
        }
    }

    TSettings& GetSettings(auto& cachedSettings) const {
        return *static_cast<TSettings*>(cachedSettings.AsBoxed().Get());
    }

    EMode GetMode(TComputationContext& ctx) const {
        auto mode = ModeArg->GetValue(ctx);
        if (mode.AsStringRef() == TStringBuf("0")) {
            return EMode::Fulltext;
        } else if (mode.AsStringRef() == TStringBuf("1")) {
            return EMode::JsonIndexOverJson;
        } else if (mode.AsStringRef() == TStringBuf("2")) {
            return EMode::JsonIndexOverJsonDocument;
        }
        return EMode::Fulltext;
    }

private:
    void RegisterDependencies() const final {
        DependsOn(TextArg);
        DependsOn(SettingsArg);
        DependsOn(ModeArg);
    }

    IComputationNode* const TextArg;
    IComputationNode* const SettingsArg;
    IComputationNode* const ModeArg;
    const ui32 CachedSettingsIndex;
};

} // namespace

IComputationNode* WrapFulltextAnalyze(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3, "FulltextAnalyze requires exactly 3 arguments");

    auto textArg = LocateNode(ctx.NodeLocator, callable, 0);
    auto settingsArg = LocateNode(ctx.NodeLocator, callable, 1);
    auto modeArg = LocateNode(ctx.NodeLocator, callable, 2);
    return new TFulltextAnalyzeWrapper(ctx.Mutables, textArg, settingsArg, modeArg);
}

} // namespace NMiniKQL
} // namespace NKikimr
