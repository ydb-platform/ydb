#include "sql_checksum.h"

#include <util/string/ascii.h>

#include <utility>

namespace NSQLTranslationV1 {

bool TChecksumTranslation::ParseSetting(const TRule_table_settings_entry& entry, TMaybe<TString>& algorithm) {
    const TString key = to_lower(Id(entry.GetRule_an_id1(), *this));
    if (key != "algorithm") {
        Error() << "Unknown CHECKSUM TABLE setting: " << key;
        return false;
    }

    if (algorithm.Defined()) {
        Error() << "Duplicate CHECKSUM TABLE setting: ALGORITHM";
        return false;
    }

    TDeferredAtom value;
    if (!StoreString(entry.GetRule_table_setting_value3(), value, Ctx_, "CHECKSUM TABLE ALGORITHM")) {
        return false;
    }

    algorithm = *value.GetLiteral();
    return true;
}

bool TChecksumTranslation::ParseSettings(const TRule_with_table_settings& settings, TMaybe<TString>& algorithm) {
    if (!ParseSetting(settings.GetRule_table_settings_entry3(), algorithm)) {
        return false;
    }

    for (const auto& block : settings.GetBlock4()) {
        if (!ParseSetting(block.GetRule_table_settings_entry2(), algorithm)) {
            return false;
        }
    }

    return true;
}

TNodePtr TChecksumTranslation::Build(const TRule_checksum_table_stmt& rule) {
    if (Ctx_.ParallelModeCount > 0) {
        Error() << "CHECKSUM TABLE statement is not supported in parallel mode";
        return nullptr;
    }

    Ctx_.BodyPart();
    Token(rule.GetToken1());
    const auto pos = Ctx_.Pos();

    TMaybe<TString> algorithm;
    if (rule.HasBlock4() && !ParseSettings(rule.GetBlock4().GetRule_with_table_settings1(), algorithm)) {
        return nullptr;
    }

    TTableRef table;
    if (!SimpleTableRefCoreImpl(rule.GetRule_simple_table_ref_core3(), table)) {
        return nullptr;
    }

    auto hints = GetContextHints(Ctx_);
    if (algorithm.Defined()) {
        hints["algorithm"] = {BuildQuotedAtom(pos, *algorithm)};
    }

    TVector<TNodePtr> prefixes;
    for (const auto& prefix : Ctx_.Settings.ExtraSystemColumnPrefixes) {
        prefixes.push_back(BuildQuotedAtom(pos, prefix));
    }

    if (!prefixes.empty()) {
        hints["systemColumnPrefixes"] = {BuildQuote(pos, BuildList(pos, std::move(prefixes)))};
    }

    table.Options = BuildInputOptions(pos, hints);
    return BuildChecksumTable(pos, table, Ctx_.Scoped);
}

} // namespace NSQLTranslationV1
