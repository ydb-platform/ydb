#include "sql_ddl_backup.h"

#include "object_processing.h"

namespace NSQLTranslationV1 {

namespace {

bool StoreString(const TRule_table_setting_value& from, TDeferredAtom& to, TContext& ctx, const TString& errorPrefix = {}) {
    switch (from.Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue2: {
            // STRING_VALUE
            const TString stringValue(ctx.Token(from.GetAlt_table_setting_value2().GetToken1()));
            auto unescaped = StringContent(ctx, ctx.Pos(), stringValue);
            if (!unescaped) {
                ctx.Error() << errorPrefix << " value cannot be unescaped";
                return false;
            }
            to = TDeferredAtom(ctx.Pos(), unescaped->Content);
            break;
        }
        default:
            ctx.Error() << errorPrefix << " value should be a string literal";
            return false;
    }
    return true;
}

} // namespace

bool TBackupTranslation::StoreStringSettingsEntry(
    const TIdentifier& id,
    const TRule_table_setting_value* value,
    std::map<TString, TDeferredAtom>& result)
{
    YQL_ENSURE(value);

    const TString key = to_lower(id.Name);
    if (result.find(key) != result.end()) {
        Ctx_.Error() << to_upper(key) << " duplicate keys";
        return false;
    }

    switch (value->Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue2:
            return StoreString(*value, result[key], Ctx_, to_upper(key));

        default:
            Ctx_.Error() << to_upper(key) << " value should be a string literal";
            return false;
    }

    return true;
}

bool TBackupTranslation::StoreStringSettingsEntry(
    const TRule_alter_table_setting_entry& entry,
    std::map<TString, TDeferredAtom>& result)
{
    const TIdentifier id = IdEx(entry.GetRule_an_id1(), *this);
    return StoreStringSettingsEntry(id, &entry.GetRule_table_setting_value3(), result);
}

bool TBackupTranslation::ParseBackupCollectionSettings(
    std::map<TString, TDeferredAtom>& result,
    const TRule_backup_collection_settings& settings)
{
    const auto& firstEntry = settings.GetRule_backup_collection_settings_entry1();
    if (!StoreStringSettingsEntry(IdEx(firstEntry.GetRule_an_id1(), *this), &firstEntry.GetRule_table_setting_value3(), result)) {
        return false;
    }
    for (const auto& block : settings.GetBlock2()) {
        const auto& entry = block.GetRule_backup_collection_settings_entry2();
        if (!StoreStringSettingsEntry(IdEx(entry.GetRule_an_id1(), *this), &entry.GetRule_table_setting_value3(), result)) {
            return false;
        }
    }
    return true;
}

bool TBackupTranslation::ParseBackupCollectionSettings(
    std::map<TString, TDeferredAtom>& result,
    std::set<TString>& toReset,
    const TRule_alter_backup_collection_actions& actions)
{
    auto parseAction = [&](auto& actionVariant) {
        switch (actionVariant.Alt_case()) {
            case TRule_alter_backup_collection_action::kAltAlterBackupCollectionAction1: {
                const auto& action = actionVariant.GetAlt_alter_backup_collection_action1().GetRule_alter_table_set_table_setting_compat1();
                if (!StoreStringSettingsEntry(action.GetRule_alter_table_setting_entry3(), result)) {
                    return false;
                }
                for (const auto& entry : action.GetBlock4()) {
                    if (!StoreStringSettingsEntry(entry.GetRule_alter_table_setting_entry2(), result)) {
                        return false;
                    }
                }
                return true;
            }
            case TRule_alter_backup_collection_action::kAltAlterBackupCollectionAction2: {
                const auto& action = actionVariant.GetAlt_alter_backup_collection_action2().GetRule_alter_table_reset_table_setting1();
                const TString firstKey = to_lower(IdEx(action.GetRule_an_id3(), *this).Name);
                toReset.insert(firstKey);
                for (const auto& key : action.GetBlock4()) {
                    toReset.insert(to_lower(IdEx(key.GetRule_an_id2(), *this).Name));
                }
                return true;
            }
            case TRule_alter_backup_collection_action::ALT_NOT_SET:
                YQL_ENSURE(false, "Unreachable");
        }
    };

    const auto& firstAction = actions.GetRule_alter_backup_collection_action1();
    if (!parseAction(firstAction)) {
        return false;
    }

    for (const auto& action : actions.GetBlock2()) {
        if (!parseAction(action.GetRule_alter_backup_collection_action2())) {
            return false;
        }
    }

    return true;
}

bool TBackupTranslation::ParseBackupCollectionTables(
    TVector<TDeferredAtom>& result,
    const TRule_table_list& tables)
{
    const auto& firstEntry = tables.GetRule_an_id_table2();
    result.push_back(TDeferredAtom(Ctx_.Pos(), Id(firstEntry, *this)));
    for (const auto& block : tables.GetBlock3()) {
        const auto& entry = block.GetRule_an_id_table3();
        result.push_back(TDeferredAtom(Ctx_.Pos(), Id(entry, *this)));
    }
    return true;
}

bool TBackupTranslation::ParseBackupCollectionEntry(
    bool& addDatabase,
    bool& removeDatabase,
    TVector<TDeferredAtom>& addTables,
    TVector<TDeferredAtom>& removeTables,
    const TRule_alter_backup_collection_entry& entry)
{
    switch (entry.Alt_case()) {
        case TRule_alter_backup_collection_entry::kAltAlterBackupCollectionEntry1: {
            addDatabase = true;
            return true;
        }
        case TRule_alter_backup_collection_entry::kAltAlterBackupCollectionEntry2: {
            removeDatabase = true;
            return true;
        }
        case TRule_alter_backup_collection_entry::kAltAlterBackupCollectionEntry3: {
            auto table = entry.GetAlt_alter_backup_collection_entry3().GetRule_an_id_table3();
            addTables.push_back(TDeferredAtom(Ctx_.Pos(), Id(table, *this)));
            return true;
        }
        case TRule_alter_backup_collection_entry::kAltAlterBackupCollectionEntry4: {
            auto table = entry.GetAlt_alter_backup_collection_entry4().GetRule_an_id_table3();
            removeTables.push_back(TDeferredAtom(Ctx_.Pos(), Id(table, *this)));
            return true;
        }
        case TRule_alter_backup_collection_entry::ALT_NOT_SET:
            YQL_ENSURE(false, "Unreachable");
    }
    return true;
}

bool TBackupTranslation::ParseBackupCollectionEntries(
    bool& addDatabase,
    bool& removeDatabase,
    TVector<TDeferredAtom>& addTables,
    TVector<TDeferredAtom>& removeTables,
    const TRule_alter_backup_collection_entries& entries)
{
    const auto& firstEntry = entries.GetRule_alter_backup_collection_entry1();
    if (!ParseBackupCollectionEntry(addDatabase, removeDatabase, addTables, removeTables, firstEntry)) {
        return false;
    }
    for (const auto& block : entries.GetBlock2()) {
        const auto& entry = block.GetRule_alter_backup_collection_entry2();
        if (!ParseBackupCollectionEntry(addDatabase, removeDatabase, addTables, removeTables, entry)) {
            return false;
        }
    }
    return true;
}

TNodePtr TBackupTranslation::Build(const TRule_create_backup_collection_stmt& node) {
    TObjectOperatorContext context(Ctx_.Scoped);
    if (node.GetRule_backup_collection2().GetRule_object_ref3().HasBlock1()) {
        if (!ClusterExpr(node.GetRule_backup_collection2().GetRule_object_ref3().GetBlock1().GetRule_cluster_expr1(),
                         /*allowWildcard=*/false,
                         context.ServiceId,
                         context.Cluster)) {
            return {};
        }
    }

    std::map<TString, TDeferredAtom> kv;
    if (!ParseBackupCollectionSettings(kv, node.GetRule_backup_collection_settings6())) {
        return {};
    }

    bool database = false;
    TVector<TDeferredAtom> tables;
    if (node.HasBlock3()) {
        database = node.GetBlock3().GetRule_create_backup_collection_entries1().has_alt_create_backup_collection_entries1();
        if (node.GetBlock3().GetRule_create_backup_collection_entries1().has_alt_create_backup_collection_entries2()) {
            if (!ParseBackupCollectionTables(
                    tables,
                    node
                        .GetBlock3()
                        .GetRule_create_backup_collection_entries1()
                        .alt_create_backup_collection_entries2()
                        .GetRule_create_backup_collection_entries_many1()
                        .GetRule_table_list2()))
            {
                return {};
            }
        }
    }

    const TString& objectId = Id(node.GetRule_backup_collection2().GetRule_object_ref3().GetRule_id_or_at2(), *this).second;
    return BuildCreateBackupCollection(
        Ctx_.Pos(),
        TString(Ctx_.GetPrefixPath(context.ServiceId, context.Cluster)),
        objectId,
        TCreateBackupCollectionParameters{
            .Settings = std::move(kv),
            .Database = database,
            .Tables = tables,
            .ExistingOk = false,
        },
        context);
}

TNodePtr TBackupTranslation::Build(const TRule_alter_backup_collection_stmt& node) {
    TObjectOperatorContext context(Ctx_.Scoped);
    if (node.GetRule_backup_collection2().GetRule_object_ref3().HasBlock1()) {
        if (!ClusterExpr(node.GetRule_backup_collection2().GetRule_object_ref3().GetBlock1().GetRule_cluster_expr1(),
                         /*allowWildcard=*/false,
                         context.ServiceId,
                         context.Cluster)) {
            return {};
        }
    }

    std::map<TString, TDeferredAtom> kv;
    std::set<TString> toReset;

    bool addDatabase = false;
    bool dropDatabase = false;
    TVector<TDeferredAtom> addTables;
    TVector<TDeferredAtom> removeTables;

    switch (node.GetBlock3().Alt_case()) {
        case TRule_alter_backup_collection_stmt_TBlock3::kAlt1: {
            if (!ParseBackupCollectionSettings(kv, toReset, node.GetBlock3().GetAlt1().GetRule_alter_backup_collection_actions1())) {
                return {};
            }
            break;
        }
        case TRule_alter_backup_collection_stmt_TBlock3::kAlt2: {
            if (!ParseBackupCollectionEntries(
                    addDatabase,
                    dropDatabase,
                    addTables,
                    removeTables,
                    node.GetBlock3().GetAlt2().GetRule_alter_backup_collection_entries1()))
            {
                return {};
            }
            break;
        }
        case TRule_alter_backup_collection_stmt_TBlock3::ALT_NOT_SET:
            YQL_ENSURE(false, "Unreachable");
    }

    auto database = addDatabase ? TAlterBackupCollectionParameters::EDatabase::Add : dropDatabase ? TAlterBackupCollectionParameters::EDatabase::Drop
                                                                                                  : TAlterBackupCollectionParameters::EDatabase::Unchanged;

    const TString& objectId = Id(node.GetRule_backup_collection2().GetRule_object_ref3().GetRule_id_or_at2(), *this).second;
    return BuildAlterBackupCollection(
        Ctx_.Pos(),
        TString(Ctx_.GetPrefixPath(context.ServiceId, context.Cluster)),
        objectId,
        TAlterBackupCollectionParameters{
            .Settings = std::move(kv),
            .SettingsToReset = std::move(toReset),
            .Database = database,
            .TablesToAdd = addTables,
            .TablesToDrop = removeTables,
            .MissingOk = false,
        },
        context);
}

TNodePtr TBackupTranslation::Build(const TRule_drop_backup_collection_stmt& node) {
    TObjectOperatorContext context(Ctx_.Scoped);
    if (node.GetRule_backup_collection2().GetRule_object_ref3().HasBlock1()) {
        if (!ClusterExpr(node.GetRule_backup_collection2().GetRule_object_ref3().GetBlock1().GetRule_cluster_expr1(),
                         /*allowWildcard=*/false,
                         context.ServiceId,
                         context.Cluster)) {
            return {};
        }
    }

    const TString& objectId = Id(node.GetRule_backup_collection2().GetRule_object_ref3().GetRule_id_or_at2(), *this).second;
    return BuildDropBackupCollection(
        Ctx_.Pos(),
        TString(Ctx_.GetPrefixPath(context.ServiceId, context.Cluster)),
        objectId,
        TDropBackupCollectionParameters{
            .MissingOk = false,
        },
        context);
}

TNodePtr TBackupTranslation::Build(const TRule_backup_stmt& node) {
    TObjectOperatorContext context(Ctx_.Scoped);
    if (node.GetRule_object_ref2().HasBlock1()) {
        if (!ClusterExpr(node.GetRule_object_ref2().GetBlock1().GetRule_cluster_expr1(),
                         /*allowWildcard=*/false, context.ServiceId, context.Cluster)) {
            return {};
        }
    }

    bool incremental = node.HasBlock3();

    const TString& objectId = Id(node.GetRule_object_ref2().GetRule_id_or_at2(), *this).second;
    return BuildBackup(
        Ctx_.Pos(),
        TString(Ctx_.GetPrefixPath(context.ServiceId, context.Cluster)),
        objectId,
        TBackupParameters{
            .Incremental = incremental,
        },
        context);
}

TNodePtr TBackupTranslation::Build(const TRule_restore_stmt& node) {
    TObjectOperatorContext context(Ctx_.Scoped);
    if (node.GetRule_object_ref2().HasBlock1()) {
        if (!ClusterExpr(node.GetRule_object_ref2().GetBlock1().GetRule_cluster_expr1(),
                         /*allowWildcard=*/false, context.ServiceId, context.Cluster)) {
            return {};
        }
    }

    TString at;
    if (node.HasBlock3()) {
        const TString stringValue = Ctx_.Token(node.GetBlock3().GetToken2());
        const auto unescaped = StringContent(Ctx_, Ctx_.Pos(), stringValue);
        if (!unescaped) {
            return {};
        }
        at = unescaped->Content;
    }

    const TString& objectId = Id(node.GetRule_object_ref2().GetRule_id_or_at2(), *this).second;
    return BuildRestore(
        Ctx_.Pos(),
        TString(Ctx_.GetPrefixPath(context.ServiceId, context.Cluster)),
        objectId,
        TRestoreParameters{
            .At = at,
        },
        context);
}

} // namespace NSQLTranslationV1
