#include "database_config_audit.h"

#include <util/generic/hash_set.h>
#include <util/string/builder.h>

#include <optional>

namespace NKikimr::NKqp {
namespace {

bool IsCall(const NYql::TAstNode* node, TStringBuf name, ui32 size) {
    return node && node->IsListOfSize(size) && node->GetChild(0)->IsAtom()
        && node->GetChild(0)->GetContent() == name;
}

const NYql::TAstNode* Unquote(const NYql::TAstNode* node) {
    return IsCall(node, "quote", 2) ? node->GetChild(1) : node;
}

std::optional<TDatabaseConfigAuditSettings> ExtractSettings(const NYql::TAstNode* node) {
    if (!IsCall(node, "Write!", 6)) {
        return std::nullopt;
    }

    const auto* key = node->GetChild(3);
    if (!IsCall(key, "Key", 2)) {
        return std::nullopt;
    }
    const auto* databaseKey = Unquote(key->GetChild(1));
    if (!databaseKey->IsListOfSize(2)) {
        return std::nullopt;
    }
    const auto* keyName = Unquote(databaseKey->GetChild(0));
    const auto* path = databaseKey->GetChild(1);
    if (!keyName->IsAtom() || keyName->GetContent() != "databasePath" || !IsCall(path, "String", 2)) {
        return std::nullopt;
    }
    const auto* database = Unquote(path->GetChild(1));
    const auto* settings = Unquote(node->GetChild(5));
    if (!database->IsAtom() || !settings->IsList()) {
        return std::nullopt;
    }

    static const THashSet<TStringBuf> legacySettings = {
        "owner", "MAX_SHARDS", "MAX_SHARDS_IN_PATH", "MAX_PATHS", "MAX_CHILDREN_IN_DIR", "TABLES_METRICS_LEVEL"
    };
    bool alterDatabase = false;
    TStringBuilder changes;
    for (const auto* item : settings->GetChildren()) {
        const auto* setting = Unquote(item);
        if (!setting->IsListOfSize(2)) {
            return std::nullopt;
        }
        const auto* name = Unquote(setting->GetChild(0));
        if (!name->IsAtom()) {
            return std::nullopt;
        }
        const auto* value = Unquote(setting->GetChild(1));
        if (name->GetContent() == "mode") {
            alterDatabase = value->IsAtom() && value->GetContent() == "alterDatabase";
            continue;
        }
        if (legacySettings.contains(name->GetContent())) {
            continue;
        }
        if (!changes.empty()) {
            changes << ", ";
        }
        changes << name->GetContent() << '=';
        if (IsCall(value, "Void", 1)) {
            changes << "INHERIT";
        } else if (IsCall(value, "Bool", 2) && Unquote(value->GetChild(1))->IsAtom()) {
            changes << Unquote(value->GetChild(1))->GetContent();
        } else {
            // Preserve the literal's type so failed type validation is visible.
            changes << value->ToString();
        }
    }
    if (!alterDatabase || changes.empty()) {
        return std::nullopt;
    }
    return TDatabaseConfigAuditSettings{TString(database->GetContent()), TString(changes)};
}

} // namespace

TVector<TDatabaseConfigAuditSettings> CollectDatabaseConfigAuditSettings(const NYql::TAstNode* root) {
    TVector<TDatabaseConfigAuditSettings> result;
    TVector<const NYql::TAstNode*> pending;
    if (root) {
        pending.push_back(root);
    }
    while (!pending.empty()) {
        const auto* node = pending.back();
        pending.pop_back();
        if (!node->IsList() || IsCall(node, "quote", 2)) {
            continue;
        }
        // The SQL translator encodes an executable block's statement list as
        // a quoted value. Unwrap only that body; ordinary quoted data stays
        // opaque, including quoted values containing a block or Write! node.
        if (IsCall(node, "block", 2)) {
            pending.push_back(Unquote(node->GetChild(1)));
            continue;
        }
        if (auto settings = ExtractSettings(node)) {
            result.push_back(std::move(*settings));
            continue;
        }
        // Preserve statement order without recursively walking user input.
        for (ui32 index = node->GetChildrenCount(); index > 0; --index) {
            pending.push_back(node->GetChild(index - 1));
        }
    }
    return result;
}

} // namespace NKikimr::NKqp
