#include "sql_checksum.h"

#include <util/generic/hash_set.h>
#include <util/string/ascii.h>

#include <utility>

namespace NSQLTranslationV1 {

bool TChecksumTranslation::ParseChangefeeds(const TRule_table_setting_value& value, TVector<TString>& changefeeds) {
    if (!changefeeds.empty()) {
        Error() << "Duplicate CHECKSUM TABLE setting: PUBLISH_TO_CHANGEFEEDS";
        return false;
    }
    if (value.GetAltCase() != TRule_table_setting_value::kAltTableSettingValue4 ||
        value.GetAlt_table_setting_value4().GetRule_split_boundaries1().GetAltCase() != TRule_split_boundaries::kAltSplitBoundaries2) {
        Error() << "PUBLISH_TO_CHANGEFEEDS requires a parenthesized list of changefeed names";
        return false;
    }

    const auto& names = value.GetAlt_table_setting_value4().GetRule_split_boundaries1().GetAlt_split_boundaries2().GetRule_literal_value_list1();
    THashSet<TString> seen;
    auto add = [&](const TRule_literal_value& item) {
        if (item.GetAltCase() != TRule_literal_value::kAltLiteralValue3) {
            Error() << "Changefeed name must be a string literal";
            return false;
        }
        const auto token = Token(item.GetAlt_literal_value3().GetToken1());
        auto name = StringContent(Ctx_, Ctx_.Pos(), token);
        if (!name) {
            return false;
        }
        if (name->Content.empty()) {
            Error() << "Changefeed name must be nonempty";
            return false;
        }
        if (!seen.insert(name->Content).second) {
            Error() << "Duplicate changefeed name: " << name->Content;
            return false;
        }
        changefeeds.push_back(std::move(name->Content));
        return true;
    };

    if (!add(names.GetRule_literal_value2())) {
        return false;
    }
    for (const auto& next : names.GetBlock3()) {
        if (!add(next.GetRule_literal_value2())) {
            return false;
        }
    }
    return true;
}

bool TChecksumTranslation::ParseSetting(const TRule_table_settings_entry& entry, TMaybe<TString>& algorithm, TVector<TString>& changefeeds) {
    const TString key = to_lower(Id(entry.GetRule_an_id1(), *this));
    if (key == "publish_to_changefeeds") {
        return ParseChangefeeds(entry.GetRule_table_setting_value3(), changefeeds);
    }
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

bool TChecksumTranslation::ParseSettings(const TRule_with_table_settings& settings, TMaybe<TString>& algorithm, TVector<TString>& changefeeds) {
    if (!ParseSetting(settings.GetRule_table_settings_entry3(), algorithm, changefeeds)) {
        return false;
    }

    for (const auto& block : settings.GetBlock4()) {
        if (!ParseSetting(block.GetRule_table_settings_entry2(), algorithm, changefeeds)) {
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
    TVector<TString> changefeeds;
    if (rule.HasBlock4() && !ParseSettings(rule.GetBlock4().GetRule_with_table_settings1(), algorithm, changefeeds)) {
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

    if (!changefeeds.empty()) {
        TVector<TNodePtr> names;
        for (const auto& name : changefeeds) {
            names.push_back(BuildQuotedAtom(pos, name));
        }
        hints["publish_to_changefeeds"] = {BuildQuote(pos, BuildList(pos, std::move(names)))};
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
