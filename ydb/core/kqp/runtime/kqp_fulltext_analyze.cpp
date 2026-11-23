#include "kqp_fulltext_analyze.h"

#include <ydb/core/base/fulltext.h>
#include <ydb/core/base/fulltext_query.h>

#include <ydb/library/json_index/json_index.h>
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

public:
    TFulltextAnalyzeWrapper(TComputationMutables& mutables, IComputationNode* textArg, IComputationNode* settingsArg,
        IComputationNode* modeArg, IComputationNode* patternModeArg, IComputationNode* escapeArg)
        : TBaseComputation(mutables)
        , TextArg(textArg)
        , SettingsArg(settingsArg)
        , ModeArg(modeArg)
        , PatternModeArg(patternModeArg)
        , EscapeArg(escapeArg)
        , CachedSettingsIndex(mutables.CurValueIndex++)
    {
    }

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        auto text = TextArg->GetValue(ctx);
        if (!text) {
            // If text is null/empty, return empty list
            return ctx.HolderFactory.GetEmptyContainerLazy();
        }

        TVector<TString> tokens;
        const auto mode = GetMode(ctx);
        MKQL_ENSURE(!PatternModeArg || mode == NFulltext::EIndexMode::FulltextQuery,
            "String patterns require FulltextQuery mode");
        switch (mode) {
            case NFulltext::EIndexMode::Fulltext:
            case NFulltext::EIndexMode::FulltextQuery: {
                auto& settings = GetSettings(ctx);
                if (!settings.IsValid) {
                    // Failed to parse settings, return empty list
                    return ctx.HolderFactory.GetEmptyContainerLazy();
                }
                if (mode == NFulltext::EIndexMode::FulltextQuery) {
                    if (PatternModeArg) {
                        const auto patternMode = PatternModeArg->GetValue(ctx);
                        const auto escapeValue = EscapeArg->GetValue(ctx);
                        const auto escape = escapeValue.AsStringRef();
                        MKQL_ENSURE(escape.Size() <= 1, "Expected an optional single-byte LIKE escape");
                        tokens = BuildStringMatchTerms(text.AsStringRef(), patternMode.AsStringRef(),
                            escape.Size() ? std::optional<char>(escape.Data()[0]) : std::nullopt, settings.Analyzers);
                    } else {
                        for (auto& term : BuildSearchTermsStructured(TString(text.AsStringRef()), settings.Analyzers)) {
                            tokens.push_back(std::move(term.Token));
                        }
                    }
                } else {
                    tokens = Analyze(TString(text.AsStringRef()), settings.Analyzers);
                }
                break;
            }

            case NFulltext::EIndexMode::JsonIndexOverJson: {
                TString error;
                tokens = NJsonIndex::TokenizeJson(text.AsStringRef(), error);
                if (!error.empty()) {
                    // Failed to tokenize JSON, return empty list
                    return ctx.HolderFactory.GetEmptyContainerLazy();
                }
                break;
            }

            case NFulltext::EIndexMode::JsonIndexOverJsonDocument: {
                tokens = NJsonIndex::TokenizeBinaryJson(text.AsStringRef());
                break;
            }

            default: {
                MKQL_ENSURE(false, "Invalid FulltextAnalyze mode");
            }
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

    NFulltext::EIndexMode GetMode(TComputationContext& ctx) const {
        auto mode = ModeArg->GetValue(ctx).Get<ui32>();
        if (mode < 1 || mode > static_cast<ui32>(NFulltext::EIndexMode::FulltextQuery)) {
            return NFulltext::EIndexMode::Invalid;
        }
        return static_cast<NFulltext::EIndexMode>(mode);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(TextArg);
        DependsOn(SettingsArg);
        DependsOn(ModeArg);
        if (PatternModeArg) {
            DependsOn(PatternModeArg);
            DependsOn(EscapeArg);
        }
    }

    IComputationNode* const TextArg;
    IComputationNode* const SettingsArg;
    IComputationNode* const ModeArg;
    IComputationNode* const PatternModeArg;
    IComputationNode* const EscapeArg;
    const ui32 CachedSettingsIndex;
};

} // namespace

IComputationNode* WrapFulltextAnalyze(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3 || callable.GetInputsCount() == 5,
        "FulltextAnalyze requires 3 or 5 arguments");

    auto textArg = LocateNode(ctx.NodeLocator, callable, 0);
    auto settingsArg = LocateNode(ctx.NodeLocator, callable, 1);
    auto modeArg = LocateNode(ctx.NodeLocator, callable, 2);
    auto patternModeArg = callable.GetInputsCount() == 5 ? LocateNode(ctx.NodeLocator, callable, 3) : nullptr;
    auto escapeArg = callable.GetInputsCount() == 5 ? LocateNode(ctx.NodeLocator, callable, 4) : nullptr;
    return new TFulltextAnalyzeWrapper(ctx.Mutables, textArg, settingsArg, modeArg, patternModeArg, escapeArg);
}

} // namespace NMiniKQL
} // namespace NKikimr
