#include "reset_opt.h"

#include <util/generic/strbuf.h>
#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/string/split.h>

#include <utility>

namespace NKikimr::NKqp {

namespace {

using TRequestedOptions = NKikimrSchemeOp::TColumnTableRequestedOptions;

// Resetting a constructor class also clears its features.
constexpr std::pair<TStringBuf, TRequestedOptions::EOption> OptionAliases[] = {
    { "COMPACTION_PLANNER.CLASS_NAME", TRequestedOptions::COMPACTION_PLANNER },
    { "METADATA_MEMORY_MANAGER.CLASS_NAME", TRequestedOptions::METADATA_MEMORY_MANAGER },
};

bool ParseOption(const TString& name, TRequestedOptions::EOption& option) {
    for (const auto& [alias, value] : OptionAliases) {
        if (name == alias) {
            option = value;
            return true;
        }
    }
    return TRequestedOptions::EOption_Parse(name, &option);
}

TString GetKnownOptionsString() {
    const auto* descriptor = TRequestedOptions::EOption_descriptor();
    TVector<TString> names;
    for (int i = 0; i < descriptor->value_count(); ++i) {
        names.emplace_back(descriptor->value(i)->name());
    }
    for (const auto& alias : OptionAliases) {
        names.emplace_back(alias.first);
    }
    return JoinSeq(", ", names);
}

}   // namespace

TConclusionStatus TResetOptionsOperation::DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) {
    auto fValue = features.Extract("NAME");
    if (!fValue) {
        return TConclusionStatus::Fail(TStringBuilder() << "can't find alter parameter NAME; known options: " << GetKnownOptionsString());
    }
    for (auto&& name : StringSplitter(*fValue).SplitBySet(", ").SkipEmpty().ToList<TString>()) {
        EOption option;
        if (!ParseOption(name, option)) {
            return TConclusionStatus::Fail(TStringBuilder() << "unknown option '" << name << "'; known options: " << GetKnownOptionsString());
        }
        Options.emplace(option);
    }
    if (Options.empty()) {
        return TConclusionStatus::Fail(TStringBuilder() << "NAME is empty; known options: " << GetKnownOptionsString());
    }
    return TConclusionStatus::Success();
}

void TResetOptionsOperation::DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const {
    for (const auto option : Options) {
        schemaData.MutableOptions()->AddResetOptions(option);
    }
}

}   // namespace NKikimr::NKqp
