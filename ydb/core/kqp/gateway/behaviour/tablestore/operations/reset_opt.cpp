#include "reset_opt.h"

#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/string/split.h>

namespace NKikimr::NKqp {

namespace {

TString GetKnownOptionsString() {
    const auto* descriptor = NKikimrSchemeOp::TColumnTableRequestedOptions::EOption_descriptor();
    TVector<TString> names;
    for (int i = 0; i < descriptor->value_count(); ++i) {
        names.emplace_back(descriptor->value(i)->name());
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
        if (!NKikimrSchemeOp::TColumnTableRequestedOptions::EOption_Parse(name, &option)) {
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

}
