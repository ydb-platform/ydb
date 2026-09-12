#include "sql_ddl_resource_pool.h"

#include "object_processing.h"

namespace NSQLTranslationV1 {

namespace {

bool StoreInt(const TRule_table_setting_value& from, TDeferredAtom& to, TContext& ctx, const TString& errorPrefix = {}) {
    switch (from.Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue3: {
            // integer
            to = TDeferredAtom(LiteralNumber(ctx, from.GetAlt_table_setting_value3().GetRule_integer1()), ctx);
            break;
        }
        default:
            ctx.Error() << errorPrefix << " value should be an integer";
            return false;
    }
    return true;
}

} // namespace

bool TResourcePoolTranslation::StoreResourcePoolSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, std::map<TString, TDeferredAtom>& result) {
    YQL_ENSURE(value);

    const TString key = to_lower(id.Name);
    if (result.find(key) != result.end()) {
        Ctx_.Error() << to_upper(key) << " duplicate keys";
        return false;
    }

    switch (value->Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue2:
            return StoreString(*value, result[key], Ctx_, to_upper(key));

        case TRule_table_setting_value::kAltTableSettingValue3:
            return StoreInt(*value, result[key], Ctx_, to_upper(key));

        default:
            Ctx_.Error() << to_upper(key) << " value should be a string literal or integer";
            return false;
    }

    return true;
}

bool TResourcePoolTranslation::StoreResourcePoolSettingsEntry(const TRule_alter_table_setting_entry& entry, std::map<TString, TDeferredAtom>& result) {
    const TIdentifier id = IdEx(entry.GetRule_an_id1(), *this);
    return StoreResourcePoolSettingsEntry(id, &entry.GetRule_table_setting_value3(), result);
}

bool TResourcePoolTranslation::ParseResourcePoolSettings(std::map<TString, TDeferredAtom>& result, const TRule_with_table_settings& settingsNode) {
    const auto& firstEntry = settingsNode.GetRule_table_settings_entry3();
    if (!StoreResourcePoolSettingsEntry(IdEx(firstEntry.GetRule_an_id1(), *this), &firstEntry.GetRule_table_setting_value3(), result)) {
        return false;
    }
    for (const auto& block : settingsNode.GetBlock4()) {
        const auto& entry = block.GetRule_table_settings_entry2();
        if (!StoreResourcePoolSettingsEntry(IdEx(entry.GetRule_an_id1(), *this), &entry.GetRule_table_setting_value3(), result)) {
            return false;
        }
    }
    return true;
}

bool TResourcePoolTranslation::ParseResourcePoolSettings(std::map<TString, TDeferredAtom>& result, std::set<TString>& toReset, const TRule_alter_resource_pool_action& alterAction) {
    switch (alterAction.Alt_case()) {
        case TRule_alter_resource_pool_action::kAltAlterResourcePoolAction1: {
            const auto& action = alterAction.GetAlt_alter_resource_pool_action1().GetRule_alter_table_set_table_setting_compat1();
            if (!StoreResourcePoolSettingsEntry(action.GetRule_alter_table_setting_entry3(), result)) {
                return false;
            }
            for (const auto& entry : action.GetBlock4()) {
                if (!StoreResourcePoolSettingsEntry(entry.GetRule_alter_table_setting_entry2(), result)) {
                    return false;
                }
            }
            return true;
        }
        case TRule_alter_resource_pool_action::kAltAlterResourcePoolAction2: {
            const auto& action = alterAction.GetAlt_alter_resource_pool_action2().GetRule_alter_table_reset_table_setting1();
            const TString firstKey = to_lower(IdEx(action.GetRule_an_id3(), *this).Name);
            toReset.insert(firstKey);
            for (const auto& key : action.GetBlock4()) {
                toReset.insert(to_lower(IdEx(key.GetRule_an_id2(), *this).Name));
            }
            return true;
        }
        case TRule_alter_resource_pool_action::ALT_NOT_SET:
            YQL_ENSURE(false, "Unreachable");
    }
}

bool TResourcePoolTranslation::StoreResourcePoolClassifierSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, std::map<TString, TDeferredAtom>& result) {
    YQL_ENSURE(value);

    const TString key = to_lower(id.Name);
    if (result.find(key) != result.end()) {
        Ctx_.Error() << to_upper(key) << " duplicate keys";
        return false;
    }

    switch (value->Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue2:
            return StoreString(*value, result[key], Ctx_, to_upper(key));

        case TRule_table_setting_value::kAltTableSettingValue3:
            return StoreInt(*value, result[key], Ctx_, to_upper(key));

        default:
            Ctx_.Error() << to_upper(key) << " value should be a string literal or integer";
            return false;
    }

    return true;
}

bool TResourcePoolTranslation::StoreResourcePoolClassifierSettingsEntry(const TRule_alter_table_setting_entry& entry, std::map<TString, TDeferredAtom>& result) {
    const TIdentifier id = IdEx(entry.GetRule_an_id1(), *this);
    return StoreResourcePoolClassifierSettingsEntry(id, &entry.GetRule_table_setting_value3(), result);
}

bool TResourcePoolTranslation::ParseResourcePoolClassifierSettings(std::map<TString, TDeferredAtom>& result, const TRule_with_table_settings& settingsNode) {
    const auto& firstEntry = settingsNode.GetRule_table_settings_entry3();
    if (!StoreResourcePoolClassifierSettingsEntry(IdEx(firstEntry.GetRule_an_id1(), *this), &firstEntry.GetRule_table_setting_value3(), result)) {
        return false;
    }
    for (const auto& block : settingsNode.GetBlock4()) {
        const auto& entry = block.GetRule_table_settings_entry2();
        if (!StoreResourcePoolClassifierSettingsEntry(IdEx(entry.GetRule_an_id1(), *this), &entry.GetRule_table_setting_value3(), result)) {
            return false;
        }
    }
    return true;
}

bool TResourcePoolTranslation::ParseResourcePoolClassifierSettings(std::map<TString, TDeferredAtom>& result, std::set<TString>& toReset, const TRule_alter_resource_pool_classifier_action& alterAction) {
    switch (alterAction.Alt_case()) {
        case TRule_alter_resource_pool_classifier_action::kAltAlterResourcePoolClassifierAction1: {
            const auto& action = alterAction.GetAlt_alter_resource_pool_classifier_action1().GetRule_alter_table_set_table_setting_compat1();
            if (!StoreResourcePoolClassifierSettingsEntry(action.GetRule_alter_table_setting_entry3(), result)) {
                return false;
            }
            for (const auto& entry : action.GetBlock4()) {
                if (!StoreResourcePoolClassifierSettingsEntry(entry.GetRule_alter_table_setting_entry2(), result)) {
                    return false;
                }
            }
            return true;
        }
        case TRule_alter_resource_pool_classifier_action::kAltAlterResourcePoolClassifierAction2: {
            const auto& action = alterAction.GetAlt_alter_resource_pool_classifier_action2().GetRule_alter_table_reset_table_setting1();
            const TString firstKey = to_lower(IdEx(action.GetRule_an_id3(), *this).Name);
            toReset.insert(firstKey);
            for (const auto& key : action.GetBlock4()) {
                toReset.insert(to_lower(IdEx(key.GetRule_an_id2(), *this).Name));
            }
            return true;
        }
        case TRule_alter_resource_pool_classifier_action::ALT_NOT_SET:
            YQL_ENSURE(false, "Unreachable");
    }
}

TNodePtr TResourcePoolTranslation::Build(const TRule_create_resource_pool_stmt& node) {
    // create_resource_pool_stmt: CREATE RESOURCE POOL object_ref with_table_settings;
    TObjectOperatorContext context(Ctx_.Scoped);
    auto objectId = ParseObjectPathIgnoreAt(node.GetRule_object_ref4(), context, /* useTablePrefix = */ false);
    if (!objectId) {
        return {};
    }

    std::map<TString, TDeferredAtom> kv;
    if (!ParseResourcePoolSettings(kv, node.GetRule_with_table_settings5())) {
        return {};
    }

    return BuildCreateResourcePool(Ctx_.Pos(), *objectId, new TObjectFeatureNode(Ctx_.Pos(), kv), context);
}

TNodePtr TResourcePoolTranslation::Build(const TRule_alter_resource_pool_stmt& node) {
    // alter_resource_pool_stmt: ALTER RESOURCE POOL object_ref
    //     alter_resource_pool_action (COMMA alter_resource_pool_action)*;
    Ctx_.BodyPart();
    TObjectOperatorContext context(Ctx_.Scoped);
    auto objectId = ParseObjectPathIgnoreAt(node.GetRule_object_ref4(), context, /* useTablePrefix = */ false);
    if (!objectId) {
        return {};
    }

    std::map<TString, TDeferredAtom> kv;
    std::set<TString> toReset;
    if (!ParseResourcePoolSettings(kv, toReset, node.GetRule_alter_resource_pool_action5())) {
        return {};
    }

    for (const auto& action : node.GetBlock6()) {
        if (!ParseResourcePoolSettings(kv, toReset, action.GetRule_alter_resource_pool_action2())) {
            return {};
        }
    }

    return BuildAlterResourcePool(Ctx_.Pos(), *objectId, new TObjectFeatureNode(Ctx_.Pos(), kv), std::move(toReset), context);
}

TNodePtr TResourcePoolTranslation::Build(const TRule_drop_resource_pool_stmt& node) {
    // drop_resource_pool_stmt: DROP RESOURCE POOL object_ref;
    TObjectOperatorContext context(Ctx_.Scoped);
    auto objectId = ParseObjectPathIgnoreAt(node.GetRule_object_ref4(), context, /* useTablePrefix = */ false);
    if (!objectId) {
        return {};
    }

    return BuildDropResourcePool(Ctx_.Pos(), *objectId, context);
}

TNodePtr TResourcePoolTranslation::Build(const TRule_create_resource_pool_classifier_stmt& node) {
    // create_resource_pool_classifier_stmt: CREATE RESOURCE POOL CLASSIFIER object_ref with_table_settings;
    TObjectOperatorContext context(Ctx_.Scoped);
    auto objectId = ParseObjectPathIgnoreAt(node.GetRule_object_ref5(), context, /* useTablePrefix = */ false);
    if (!objectId) {
        return {};
    }

    std::map<TString, TDeferredAtom> kv;
    if (!ParseResourcePoolClassifierSettings(kv, node.GetRule_with_table_settings6())) {
        return {};
    }

    return BuildCreateResourcePoolClassifier(Ctx_.Pos(), *objectId, new TObjectFeatureNode(Ctx_.Pos(), kv), context);
}

TNodePtr TResourcePoolTranslation::Build(const TRule_alter_resource_pool_classifier_stmt& node) {
    // alter_resource_pool_classifier_stmt: ALTER RESOURCE POOL CLASSIFIER object_ref
    //     alter_resource_pool_classifier_action (COMMA alter_resource_pool_classifier_action)*;
    Ctx_.BodyPart();
    TObjectOperatorContext context(Ctx_.Scoped);
    auto objectId = ParseObjectPathIgnoreAt(node.GetRule_object_ref5(), context, /* useTablePrefix = */ false);
    if (!objectId) {
        return {};
    }

    std::map<TString, TDeferredAtom> kv;
    std::set<TString> toReset;
    if (!ParseResourcePoolClassifierSettings(kv, toReset, node.GetRule_alter_resource_pool_classifier_action6())) {
        return {};
    }

    for (const auto& action : node.GetBlock7()) {
        if (!ParseResourcePoolClassifierSettings(kv, toReset, action.GetRule_alter_resource_pool_classifier_action2())) {
            return {};
        }
    }

    return BuildAlterResourcePoolClassifier(Ctx_.Pos(), *objectId, new TObjectFeatureNode(Ctx_.Pos(), kv), std::move(toReset), context);
}

TNodePtr TResourcePoolTranslation::Build(const TRule_drop_resource_pool_classifier_stmt& node) {
    // drop_resource_pool_classifier_stmt: DROP RESOURCE POOL CLASSIFIER object_ref;
    TObjectOperatorContext context(Ctx_.Scoped);
    auto objectId = ParseObjectPathIgnoreAt(node.GetRule_object_ref5(), context, /* useTablePrefix = */ false);
    if (!objectId) {
        return {};
    }

    return BuildDropResourcePoolClassifier(Ctx_.Pos(), *objectId, context);
}

} // namespace NSQLTranslationV1
