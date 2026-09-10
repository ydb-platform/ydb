#pragma once

#include "ddl_resource_pool.h"
#include "sql_translation.h"

namespace NSQLTranslationV1 {

class TResourcePoolTranslation final: public TSqlTranslation {
public:
    TResourcePoolTranslation(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TNodePtr Build(const TRule_create_resource_pool_stmt& node);
    TNodePtr Build(const TRule_alter_resource_pool_stmt& node);
    TNodePtr Build(const TRule_drop_resource_pool_stmt& node);
    TNodePtr Build(const TRule_create_resource_pool_classifier_stmt& node);
    TNodePtr Build(const TRule_alter_resource_pool_classifier_stmt& node);
    TNodePtr Build(const TRule_drop_resource_pool_classifier_stmt& node);

private:
    bool StoreResourcePoolSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, std::map<TString, TDeferredAtom>& result);
    bool StoreResourcePoolSettingsEntry(const TRule_alter_table_setting_entry& entry, std::map<TString, TDeferredAtom>& result);
    bool StoreResourcePoolClassifierSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, std::map<TString, TDeferredAtom>& result);
    bool StoreResourcePoolClassifierSettingsEntry(const TRule_alter_table_setting_entry& entry, std::map<TString, TDeferredAtom>& result);
    bool ParseResourcePoolSettings(std::map<TString, TDeferredAtom>& result, const TRule_with_table_settings& settings);
    bool ParseResourcePoolSettings(std::map<TString, TDeferredAtom>& result, std::set<TString>& toReset, const TRule_alter_resource_pool_action& alterAction);
    bool ParseResourcePoolClassifierSettings(std::map<TString, TDeferredAtom>& result, const TRule_with_table_settings& settings);
    bool ParseResourcePoolClassifierSettings(std::map<TString, TDeferredAtom>& result, std::set<TString>& toReset, const TRule_alter_resource_pool_classifier_action& alterAction);
};

} // namespace NSQLTranslationV1
