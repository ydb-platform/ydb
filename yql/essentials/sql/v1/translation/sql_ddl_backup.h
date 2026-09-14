#pragma once

#include "ddl_backup.h"
#include "sql_translation.h"

namespace NSQLTranslationV1 {

class TBackupTranslation final: public TSqlTranslation {
public:
    TBackupTranslation(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TNodePtr Build(const TRule_create_backup_collection_stmt& node);
    TNodePtr Build(const TRule_alter_backup_collection_stmt& node);
    TNodePtr Build(const TRule_drop_backup_collection_stmt& node);
    TNodePtr Build(const TRule_backup_stmt& node);
    TNodePtr Build(const TRule_restore_stmt& node);

private:
    bool StoreStringSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, std::map<TString, TDeferredAtom>& result);
    bool StoreStringSettingsEntry(const TRule_alter_table_setting_entry& entry, std::map<TString, TDeferredAtom>& result);
    bool ParseBackupCollectionSettings(std::map<TString, TDeferredAtom>& result, const TRule_backup_collection_settings& settings);
    bool ParseBackupCollectionSettings(std::map<TString, TDeferredAtom>& result, std::set<TString>& toReset, const TRule_alter_backup_collection_actions& actions);
    bool ParseBackupCollectionTables(TVector<TDeferredAtom>& result, const TRule_table_list& tables);
    bool ParseBackupCollectionEntry(
        bool& addDatabase,
        bool& removeDatabase,
        TVector<TDeferredAtom>& addTables,
        TVector<TDeferredAtom>& removeTables,
        const TRule_alter_backup_collection_entry& entry);
    bool ParseBackupCollectionEntries(
        bool& addDatabase,
        bool& removeDatabase,
        TVector<TDeferredAtom>& addTables,
        TVector<TDeferredAtom>& removeTables,
        const TRule_alter_backup_collection_entries& entries);
};

} // namespace NSQLTranslationV1
