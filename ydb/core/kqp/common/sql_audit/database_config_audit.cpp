#include "database_config_audit.h"

#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/sql/v1/proto_parser/parse_tree.h>
#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>

#include <google/protobuf/descriptor.h>

#include <util/generic/hash_set.h>
#include <util/string/builder.h>

namespace NKikimr::NKqp {
namespace {

using namespace NSQLv1Generated;

void AppendChildrenInReverseOrder(const google::protobuf::Message& message,
    TVector<const google::protobuf::Message*>& pending)
{
    const auto* descriptor = message.GetDescriptor();
    const auto* reflection = message.GetReflection();
    // Grammar fields and repeated blocks follow source order.
    for (int index = descriptor->field_count(); index > 0; --index) {
        const auto* field = descriptor->field(index - 1);
        if (field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
            continue;
        }
        if (field->is_repeated()) {
            for (int i = reflection->FieldSize(message, field); i > 0; --i) {
                pending.push_back(&reflection->GetRepeatedMessage(message, field, i - 1));
            }
        } else if (reflection->HasField(message, field)) {
            pending.push_back(&reflection->GetMessage(message, field));
        }
    }
}

TString TokenText(const google::protobuf::Message& root) {
    TStringBuilder text;
    TVector<const google::protobuf::Message*> pending{&root};
    while (!pending.empty()) {
        const auto* message = pending.back();
        pending.pop_back();
        const auto* descriptor = message->GetDescriptor();
        if (descriptor == TToken::GetDescriptor()) {
            text << static_cast<const TToken*>(message)->GetValue();
            continue;
        }
        AppendChildrenInReverseOrder(*message, pending);
    }
    return text;
}

void AppendSetting(TStringBuilder& changes, const TRule_an_id& id, TStringBuf value) {
    const TString expression = TokenText(id);
    TStringBuf name(expression);
    if (name.size() >= 2 && (name.front() == '`' || name.front() == '"' || name.front() == '\'') && name.back() == name.front()) {
        name = name.SubStr(1, name.size() - 2);
    }
    static const THashSet<TString> legacySettings = {
        "MAX_SHARDS", "MAX_SHARDS_IN_PATH", "MAX_PATHS", "MAX_CHILDREN_IN_DIR", "TABLES_METRICS_LEVEL"
    };
    if (legacySettings.contains(to_upper(TString(name)))) {
        return;
    }
    if (!changes.empty()) {
        changes << ", ";
    }
    changes << expression << '=' << value;
}

TVector<const TRule_sql_stmt_core*> ExecutableStatements(const TRule_sql_query& query) {
    const auto statements = NSQLTranslationV1::Statements(query);
    TVector<const google::protobuf::Message*> pending(statements.rbegin(), statements.rend());
    TVector<const TRule_sql_stmt_core*> result;
    while (!pending.empty()) {
        const auto* message = pending.back();
        pending.pop_back();
        if (message->GetDescriptor() == TRule_sql_stmt_core::GetDescriptor()) {
            const auto* statement = static_cast<const TRule_sql_stmt_core*>(message);
            switch (statement->GetAltCase()) {
                case TRule_sql_stmt_core::kAltSqlStmtCore61: // ALTER DATABASE
                    result.push_back(statement);
                    break;
                case TRule_sql_stmt_core::kAltSqlStmtCore17: // DO
                case TRule_sql_stmt_core::kAltSqlStmtCore19: // IF
                case TRule_sql_stmt_core::kAltSqlStmtCore20: // FOR
                    AppendChildrenInReverseOrder(*message, pending);
                    break;
                default:
                    // Definitions and expressions are not executed here. In
                    // particular, do not report an unused action's body as an
                    // attempted configuration change.
                    break;
            }
        } else {
            // Descend through the control statement's grammar wrappers until
            // reaching its inline body's statements. Both conditional branches
            // are inspected because translation validates both before execution.
            AppendChildrenInReverseOrder(*message, pending);
        }
    }
    return result;
}

TVector<TDatabaseConfigAuditSettings> CollectSettings(const TString& query) {
    NSQLTranslation::TTranslationSettings settings;
    settings.InferSyntaxVersion = true;
    NYql::TIssues issues;
    if (!NSQLTranslation::ParseTranslationSettings(query, settings, issues)
        || settings.PgParser || settings.SyntaxVersion != 1)
    {
        return {};
    }
    // Audit extraction must remain bounded even when the original query failed
    // parsing. Apply the limit after reading directives, independently of them.
    settings.MaxParseTreeDepth = NSQLTranslation::SQL_MAX_PARSE_TREE_DEPTH;
    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(false, false, settings.MaxParseTreeDepth);
    parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory(false, false, settings.MaxParseTreeDepth);
    google::protobuf::Arena arena;
    const auto* message = NSQLTranslationV1::SqlAST(parsers, query, "query", issues,
        NSQLTranslation::SQL_MAX_PARSER_ERRORS, settings.AnsiLexer, &arena);
    if (!message || !issues.Empty()) {
        return {};
    }

    TVector<TDatabaseConfigAuditSettings> result;
    const auto& ast = static_cast<const TSQLv1ParserAST&>(*message);
    for (const auto* statement : ExecutableStatements(ast.GetRule_sql_query())) {
        const auto& database = statement->GetAlt_sql_stmt_core61().GetRule_alter_database_stmt1();
        const auto& action = database.GetRule_alter_database_action4();
        TStringBuilder changes;
        if (action.GetAltCase() == TRule_alter_database_action::kAltAlterDatabaseAction2) {
            const auto& settings = action.GetAlt_alter_database_action2()
                .GetRule_set_database_settings1().GetRule_database_settings3();
            const auto append = [&](const TRule_database_setting& setting) {
                AppendSetting(changes, setting.GetRule_an_id1(), TokenText(setting.GetRule_database_setting_value3()));
            };
            append(settings.GetRule_database_setting1());
            for (const auto& entry : settings.GetBlock2()) {
                append(entry.GetRule_database_setting2());
            }
        } else if (action.GetAltCase() == TRule_alter_database_action::kAltAlterDatabaseAction3) {
            const auto& settings = action.GetAlt_alter_database_action3().GetRule_reset_database_settings1();
            AppendSetting(changes, settings.GetRule_an_id3(), "INHERIT");
            for (const auto& entry : settings.GetBlock4()) {
                AppendSetting(changes, entry.GetRule_an_id2(), "INHERIT");
            }
        }
        if (!changes.empty()) {
            result.push_back({TokenText(database.GetRule_an_id_schema3()), TString(changes), true});
        }
    }
    return result;
}

} // namespace

TVector<TDatabaseConfigAuditSettings> CollectDatabaseConfigAuditSettingsFromSql(const TString& query) noexcept {
    try {
        return CollectSettings(query);
    } catch (...) {
        // Preserve the original query failure if parser setup or extraction
        // also fails. The ordinary query audit still records that request.
        return {};
    }
}

} // namespace NKikimr::NKqp
