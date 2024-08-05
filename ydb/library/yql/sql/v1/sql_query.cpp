#include "sql_query.h"
#include "sql_expression.h"
#include "sql_select.h"
#include "sql_into_tables.h"
#include "sql_values.h"
#include "node.h"
#include <ydb/library/yql/parser/proto_ast/gen/v1/SQLv1Lexer.h>
#include <ydb/library/yql/sql/v1/object_processing.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/utils/yql_paths.h>
#include <util/generic/scope.h>
#include <util/string/join.h>

namespace NSQLTranslationV1 {

using NALPDefault::SQLv1LexerTokens;

using namespace NSQLv1Generated;

void FillTargetList(TTranslation& ctx, const TRule_set_target_list& node, TVector<TString>& targetList) {
    targetList.push_back(ColumnNameAsSingleStr(ctx, node.GetRule_set_target2().GetRule_column_name1()));
    for (auto& block: node.GetBlock3()) {
        targetList.push_back(ColumnNameAsSingleStr(ctx, block.GetRule_set_target2().GetRule_column_name1()));
    }
}

bool PackageVersionFromString(const TString& s, ui32& version) {
    if (s == "release") {
        version = 0;
        return true;
    }
    if (s == "draft") {
        version = 1;
        return true;
    }
    return TryFromString(s, version);
}

void TSqlQuery::AddStatementToBlocks(TVector<TNodePtr>& blocks, TNodePtr node) {
    blocks.emplace_back(node);
}

static bool AsyncReplicationSettingsEntry(std::map<TString, TNodePtr>& out,
        const TRule_replication_settings_entry& in, TTranslation& ctx, bool create)
{
    auto key = IdEx(in.GetRule_an_id1(), ctx);
    auto value = BuildLiteralSmartString(ctx.Context(), ctx.Token(in.GetToken3()));

    TSet<TString> configSettings = {
        "connection_string",
        "endpoint",
        "database",
        "token",
        "token_secret_name",
        "user",
        "password",
        "password_secret_name",
    };

    TSet<TString> stateSettings = {
        "state",
        "failover_mode",
    };

    const auto keyName = to_lower(key.Name);
    if (!configSettings.count(keyName) && !stateSettings.count(keyName)) {
        ctx.Context().Error() << "Unknown replication setting: " << key.Name;
        return false;
    }

    if (create && stateSettings.count(keyName)) {
        ctx.Context().Error() << key.Name << " is not supported in CREATE";
        return false;
    }

    if (!out.emplace(keyName, value).second) {
        ctx.Context().Error() << "Duplicate replication setting: " << key.Name;
    }

    return true;
}

static bool AsyncReplicationSettings(std::map<TString, TNodePtr>& out,
        const TRule_replication_settings& in, TTranslation& ctx, bool create)
{
    if (!AsyncReplicationSettingsEntry(out, in.GetRule_replication_settings_entry1(), ctx, create)) {
        return false;
    }

    for (auto& block : in.GetBlock2()) {
        if (!AsyncReplicationSettingsEntry(out, block.GetRule_replication_settings_entry2(), ctx, create)) {
            return false;
        }
    }

    return true;
}

static bool AsyncReplicationTarget(std::vector<std::pair<TString, TString>>& out, TStringBuf prefixPath,
        const TRule_replication_target& in, TTranslation& ctx)
{
    const TString remote = Id(in.GetRule_object_ref1().GetRule_id_or_at2(), ctx).second;
    const TString local = Id(in.GetRule_object_ref3().GetRule_id_or_at2(), ctx).second;
    out.emplace_back(remote, BuildTablePath(prefixPath, local));
    return true;
}

static bool AsyncReplicationAlterAction(std::map<TString, TNodePtr>& settings,
        const TRule_alter_replication_action& in, TTranslation& ctx)
{
    // TODO(ilnaz): support other actions
    return AsyncReplicationSettings(settings, in.GetRule_alter_replication_set_setting1().GetRule_replication_settings3(), ctx, false);
}

bool TSqlQuery::Statement(TVector<TNodePtr>& blocks, const TRule_sql_stmt_core& core) {
    TString internalStatementName;
    TString humanStatementName;
    ParseStatementName(core, internalStatementName, humanStatementName);
    const auto& altCase = core.Alt_case();
    if (Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW && (altCase >= TRule_sql_stmt_core::kAltSqlStmtCore4 &&
        altCase != TRule_sql_stmt_core::kAltSqlStmtCore13)) {
        Error() << humanStatementName << " statement is not supported in limited views";
        return false;
    }

    if (Mode == NSQLTranslation::ESqlMode::SUBQUERY && (altCase >= TRule_sql_stmt_core::kAltSqlStmtCore4 &&
        altCase != TRule_sql_stmt_core::kAltSqlStmtCore13 && altCase != TRule_sql_stmt_core::kAltSqlStmtCore6 &&
        altCase != TRule_sql_stmt_core::kAltSqlStmtCore18)) {
        Error() << humanStatementName << " statement is not supported in subqueries";
        return false;
    }

    switch (altCase) {
        case TRule_sql_stmt_core::kAltSqlStmtCore1: {
            bool success = false;
            TNodePtr nodeExpr = PragmaStatement(core.GetAlt_sql_stmt_core1().GetRule_pragma_stmt1(), success);
            if (!success) {
                return false;
            }
            if (nodeExpr) {
                AddStatementToBlocks(blocks, nodeExpr);
            }
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore2: {
            if (Ctx.ParallelModeCount > 0) {
                Error() << humanStatementName << " statement is not supported in parallel mode";
                return false;
            }

            Ctx.BodyPart();
            TSqlSelect select(Ctx, Mode);
            TPosition pos;
            auto source = select.Build(core.GetAlt_sql_stmt_core2().GetRule_select_stmt1(), pos);
            if (!source) {
                return false;
            }
            blocks.emplace_back(BuildSelectResult(pos, std::move(source),
                Mode != NSQLTranslation::ESqlMode::LIMITED_VIEW && Mode != NSQLTranslation::ESqlMode::SUBQUERY, Mode == NSQLTranslation::ESqlMode::SUBQUERY,
                Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore3: {
            Ctx.BodyPart();
            TVector<TSymbolNameWithPos> names;
            auto nodeExpr = NamedNode(core.GetAlt_sql_stmt_core3().GetRule_named_nodes_stmt1(), names);
            if (!nodeExpr) {
                return false;
            }
            TVector<TNodePtr> nodes;
            auto subquery = nodeExpr->GetSource();
            if (subquery && Mode == NSQLTranslation::ESqlMode::LIBRARY && Ctx.ScopeLevel == 0) {
                for (size_t i = 0; i < names.size(); ++i) {
                    nodes.push_back(BuildInvalidSubqueryRef(subquery->GetPos()));
                }
            } else if (subquery) {
                const auto alias = Ctx.MakeName("subquerynode");
                const auto ref = Ctx.MakeName("subquery");
                blocks.push_back(BuildSubquery(subquery, alias,
                    Mode == NSQLTranslation::ESqlMode::SUBQUERY, names.size() == 1 ? -1 : names.size(), Ctx.Scoped));
                blocks.back()->SetLabel(ref);

                for (size_t i = 0; i < names.size(); ++i) {
                    nodes.push_back(BuildSubqueryRef(blocks.back(), ref, names.size() == 1 ? -1 : i));
                }
            } else if (!Ctx.CompactNamedExprs || nodeExpr->GetUdfNode()) {
                // Unlike other nodes, TUdfNode is not an independent node, but more like a set of parameters which should be
                // applied on UDF call site. For example, TUdfNode can not be Translate()d
                // So we can't add it to blocks and use reference, instead we store the TUdfNode itself as named node
                // TODO: remove this special case
                if (names.size() > 1) {
                    auto tupleRes = BuildTupleResult(nodeExpr, names.size());
                    for (size_t i = 0; i < names.size(); ++i) {
                        nodes.push_back(nodeExpr->Y("Nth", tupleRes, nodeExpr->Q(ToString(i))));
                    }
                } else {
                    nodes.push_back(std::move(nodeExpr));
                }
            } else {
                const auto ref = Ctx.MakeName("namedexprnode");
                blocks.push_back(BuildNamedExpr(names.size() > 1 ? BuildTupleResult(nodeExpr, names.size()) : nodeExpr));
                blocks.back()->SetLabel(ref);
                for (size_t i = 0; i < names.size(); ++i) {
                    nodes.push_back(BuildNamedExprReference(blocks.back(), ref, names.size() == 1 ? TMaybe<size_t>() : i));
                }
            }

            for (size_t i = 0; i < names.size(); ++i) {
                PushNamedNode(names[i].Pos, names[i].Name, nodes[i]);
            }
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore4: {
            Ctx.BodyPart();
            const auto& rule = core.GetAlt_sql_stmt_core4().GetRule_create_table_stmt1();

            bool replaceIfExists = false;
            if (rule.HasBlock2()) { // OR REPLACE
                replaceIfExists = true;
                Y_DEBUG_ABORT_UNLESS(
                    rule.GetBlock2().GetToken1().GetId() == SQLv1LexerTokens::TOKEN_OR &&
                    rule.GetBlock2().GetToken2().GetId() == SQLv1LexerTokens::TOKEN_REPLACE
                );
            }

            const bool isCreateTableAs = rule.HasBlock15();
            const auto& block = rule.GetBlock3();
            ETableType tableType = ETableType::Table;
            bool temporary = false;
            if (block.HasAlt2() && block.GetAlt2().GetToken1().GetId() == SQLv1LexerTokens::TOKEN_TABLESTORE) {
                tableType = ETableType::TableStore;
                if (isCreateTableAs) {
                    Context().Error(GetPos(block.GetAlt2().GetToken1()))
                        << "CREATE TABLE AS is not supported for TABLESTORE";
                    return false;
                }
            } else if (block.HasAlt3() && block.GetAlt3().GetToken1().GetId() == SQLv1LexerTokens::TOKEN_EXTERNAL) {
                tableType = ETableType::ExternalTable;
                if (isCreateTableAs) {
                    Context().Error(GetPos(block.GetAlt3().GetToken1()))
                        << "CREATE TABLE AS is not supported for EXTERNAL TABLE";
                    return false;
                }
            } else if (block.HasAlt4() && block.GetAlt4().GetToken1().GetId() == SQLv1LexerTokens::TOKEN_TEMP ||
                    block.HasAlt5() && block.GetAlt5().GetToken1().GetId() == SQLv1LexerTokens::TOKEN_TEMPORARY) {
                temporary = true;
            }

            bool existingOk = false;
            if (rule.HasBlock4()) { // IF NOT EXISTS
                existingOk = true;
                Y_DEBUG_ABORT_UNLESS(
                    rule.GetBlock4().GetToken1().GetId() == SQLv1LexerTokens::TOKEN_IF &&
                    rule.GetBlock4().GetToken2().GetId() == SQLv1LexerTokens::TOKEN_NOT &&
                    rule.GetBlock4().GetToken3().GetId() == SQLv1LexerTokens::TOKEN_EXISTS
                );
            }

            if (replaceIfExists && tableType != ETableType::ExternalTable) {
                Context().Error(GetPos(rule.GetBlock2().GetToken1()))
                    << "OR REPLACE feature is supported only for EXTERNAL DATA SOURCE and EXTERNAL TABLE";
                return false;
            }

            TTableRef tr;
            if (!SimpleTableRefImpl(rule.GetRule_simple_table_ref5(), tr)) {
                return false;
            }

            TCreateTableParameters params{.TableType=tableType, .Temporary=temporary};
            if (!CreateTableEntry(rule.GetRule_create_table_entry7(), params, isCreateTableAs)) {
                return false;
            }
            for (auto& block: rule.GetBlock8()) {
                if (!CreateTableEntry(block.GetRule_create_table_entry2(), params, isCreateTableAs)) {
                    return false;
                }
            }

            if (rule.HasBlock11()) {
                Context().Error(GetPos(rule.GetBlock11().GetRule_table_inherits1().GetToken1()))
                    << "INHERITS clause is not supported yet";
                return false;
            }

            if (rule.HasBlock12()) {
                if (tableType == ETableType::TableStore) {
                    Context().Error(GetPos(rule.GetBlock12().GetRule_table_partition_by1().GetToken1()))
                        << "PARTITION BY is not supported for TABLESTORE";
                    return false;
                }
                const auto list = rule.GetBlock12().GetRule_table_partition_by1().GetRule_pure_column_list4();
                params.PartitionByColumns.push_back(IdEx(list.GetRule_an_id2(), *this));
                for (auto& node : list.GetBlock3()) {
                    params.PartitionByColumns.push_back(IdEx(node.GetRule_an_id2(), *this));
                }
            }

            if (rule.HasBlock13()) {
                if (!CreateTableSettings(rule.GetBlock13().GetRule_with_table_settings1(), params)) {
                    return false;
                }
            }

            if (rule.HasBlock14()) {
                Context().Error(GetPos(rule.GetBlock14().GetRule_table_tablestore1().GetToken1()))
                    << "TABLESTORE clause is not supported yet";
                return false;
            }

            TSourcePtr tableSource = nullptr;
            if (isCreateTableAs) {
                tableSource = TSqlAsValues(Ctx, Mode).Build(rule.GetBlock15().GetRule_table_as_source1().GetRule_values_source2(), "CreateTableAs");
                if (!tableSource) {
                    return false;
                }
            }

            if (!ValidateExternalTable(params)) {
                return false;
            }

            AddStatementToBlocks(blocks, BuildCreateTable(Ctx.Pos(), tr, existingOk, replaceIfExists, params, std::move(tableSource), Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore5: {
            Ctx.BodyPart();
            const auto& rule = core.GetAlt_sql_stmt_core5().GetRule_drop_table_stmt1();
            const auto& block = rule.GetBlock2();
            ETableType tableType = ETableType::Table;
            if (block.HasAlt2()) {
                tableType = ETableType::TableStore;
            }
            if (block.HasAlt3()) {
                tableType = ETableType::ExternalTable;
            }

            bool missingOk = false;
            if (rule.HasBlock3()) { // IF EXISTS
                missingOk = true;
                Y_DEBUG_ABORT_UNLESS(
                    rule.GetBlock3().GetToken1().GetId() == SQLv1LexerTokens::TOKEN_IF &&
                    rule.GetBlock3().GetToken2().GetId() == SQLv1LexerTokens::TOKEN_EXISTS
                );
            }

            TTableRef tr;
            if (!SimpleTableRefImpl(rule.GetRule_simple_table_ref4(), tr)) {
                return false;
            }

            AddStatementToBlocks(blocks, BuildDropTable(Ctx.Pos(), tr, missingOk, tableType, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore6: {
            const auto& rule = core.GetAlt_sql_stmt_core6().GetRule_use_stmt1();
            Token(rule.GetToken1());
            if (!ClusterExpr(rule.GetRule_cluster_expr2(), true, Ctx.Scoped->CurrService, Ctx.Scoped->CurrCluster)) {
                return false;
            }

            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore7: {
            Ctx.BodyPart();
            TSqlIntoTable intoTable(Ctx, Mode);
            TNodePtr block(intoTable.Build(core.GetAlt_sql_stmt_core7().GetRule_into_table_stmt1()));
            if (!block) {
                return false;
            }
            blocks.emplace_back(block);
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore8: {
            if (Ctx.ParallelModeCount > 0) {
                Error() << humanStatementName << " statement is not supported in parallel mode";
                return false;
            }

            Ctx.BodyPart();
            const auto& rule = core.GetAlt_sql_stmt_core8().GetRule_commit_stmt1();
            Token(rule.GetToken1());
            blocks.emplace_back(BuildCommitClusters(Ctx.Pos()));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore9: {
            Ctx.BodyPart();
            auto updateNode = Build(core.GetAlt_sql_stmt_core9().GetRule_update_stmt1());
            if (!updateNode) {
                return false;
            }
            AddStatementToBlocks(blocks, updateNode);
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore10: {
            Ctx.BodyPart();
            auto deleteNode = Build(core.GetAlt_sql_stmt_core10().GetRule_delete_stmt1());
            if (!deleteNode) {
                return false;
            }
            blocks.emplace_back(deleteNode);
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore11: {
            if (Ctx.ParallelModeCount > 0) {
                Error() << humanStatementName << " statement is not supported in parallel mode";
                return false;
            }

            Ctx.BodyPart();
            const auto& rule = core.GetAlt_sql_stmt_core11().GetRule_rollback_stmt1();
            Token(rule.GetToken1());
            blocks.emplace_back(BuildRollbackClusters(Ctx.Pos()));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore12:
            if (!DeclareStatement(core.GetAlt_sql_stmt_core12().GetRule_declare_stmt1())) {
                return false;
            }
            break;
        case TRule_sql_stmt_core::kAltSqlStmtCore13:
            if (!ImportStatement(core.GetAlt_sql_stmt_core13().GetRule_import_stmt1())) {
                return false;
            }
            break;
        case TRule_sql_stmt_core::kAltSqlStmtCore14:
            if (!ExportStatement(core.GetAlt_sql_stmt_core14().GetRule_export_stmt1())) {
                return false;
            }
            break;
        case TRule_sql_stmt_core::kAltSqlStmtCore15: {
            Ctx.BodyPart();
            const auto& rule = core.GetAlt_sql_stmt_core15().GetRule_alter_table_stmt1();
            const bool isTablestore = rule.GetToken2().GetId() == SQLv1LexerTokens::TOKEN_TABLESTORE;
            TTableRef tr;
            if (!SimpleTableRefImpl(rule.GetRule_simple_table_ref3(), tr)) {
                return false;
            }

            TAlterTableParameters params;
            if (isTablestore) {
                params.TableType = ETableType::TableStore;
            }
            if (!AlterTableAction(rule.GetRule_alter_table_action4(), params)) {
                return false;
            }

            for (auto& block : rule.GetBlock5()) {
                if (!AlterTableAction(block.GetRule_alter_table_action2(), params)) {
                    return false;
                }
            }

            AddStatementToBlocks(blocks, BuildAlterTable(Ctx.Pos(), tr, params, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore16: {
            // alter_external_table_stmt: ALTER EXTERNAL TABLE simple_table_ref alter_external_table_action (COMMA alter_external_table_action)*
            Ctx.BodyPart();
            const auto& rule = core.GetAlt_sql_stmt_core16().GetRule_alter_external_table_stmt1();
            TTableRef tr;
            if (!SimpleTableRefImpl(rule.GetRule_simple_table_ref4(), tr)) {
                return false;
            }

            TAlterTableParameters params;
            params.TableType = ETableType::ExternalTable;
            if (!AlterExternalTableAction(rule.GetRule_alter_external_table_action5(), params)) {
                return false;
            }

            for (auto& block : rule.GetBlock6()) {
                if (!AlterExternalTableAction(block.GetRule_alter_external_table_action2(), params)) {
                    return false;
                }
            }

            AddStatementToBlocks(blocks, BuildAlterTable(Ctx.Pos(), tr, params, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore17: {
            Ctx.BodyPart();
            auto node = DoStatement(core.GetAlt_sql_stmt_core17().GetRule_do_stmt1(), false);
            if (!node) {
                return false;
            }

            blocks.push_back(node);
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore18: {
            Ctx.BodyPart();
            TNodePtr lambda;
            TSymbolNameWithPos nameAndPos;
            const auto& stmt = core.GetAlt_sql_stmt_core18().GetRule_define_action_or_subquery_stmt1();
            const TString kind = to_lower(Ctx.Token(stmt.GetToken2()));
            YQL_ENSURE(kind == "action" || kind == "subquery");
            if (!DefineActionOrSubqueryStatement(stmt, nameAndPos, lambda)) {
                return false;
            }

            if (Ctx.CompactNamedExprs) {
                const auto ref = Ctx.MakeName("named" + kind + "node");
                blocks.push_back(BuildNamedExpr(lambda));
                blocks.back()->SetLabel(ref);
                lambda = BuildNamedExprReference(blocks.back(), ref, {});
            }

            PushNamedNode(nameAndPos.Pos, nameAndPos.Name, lambda);
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore19: {
            Ctx.BodyPart();
            auto node = IfStatement(core.GetAlt_sql_stmt_core19().GetRule_if_stmt1());
            if (!node) {
                return false;
            }

            blocks.push_back(node);
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore20: {
            Ctx.BodyPart();
            auto node = ForStatement(core.GetAlt_sql_stmt_core20().GetRule_for_stmt1());
            if (!node) {
                return false;
            }

            blocks.push_back(node);
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore21: {
            if (Ctx.ParallelModeCount > 0) {
                Error() << humanStatementName << " statement is not supported in parallel mode";
                return false;
            }

            Ctx.BodyPart();
            TSqlValues values(Ctx, Mode);
            TPosition pos;
            auto source = values.Build(core.GetAlt_sql_stmt_core21().GetRule_values_stmt1(), pos, {}, TPosition());
            if (!source) {
                return false;
            }
            blocks.emplace_back(BuildSelectResult(pos, std::move(source),
                Mode != NSQLTranslation::ESqlMode::LIMITED_VIEW && Mode != NSQLTranslation::ESqlMode::SUBQUERY, Mode == NSQLTranslation::ESqlMode::SUBQUERY,
                Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore22: {
            // create_user_stmt: CREATE USER role_name create_user_option?;
            Ctx.BodyPart();
            auto& node = core.GetAlt_sql_stmt_core22().GetRule_create_user_stmt1();

            Ctx.Token(node.GetToken1());
            const TPosition pos = Ctx.Pos();

            TString service = Ctx.Scoped->CurrService;
            TDeferredAtom cluster = Ctx.Scoped->CurrCluster;
            if (cluster.Empty()) {
                Error() << "USE statement is missing - no default cluster is selected";
                return false;
            }

            TDeferredAtom roleName;
            bool allowSystemRoles = false;
            if (!RoleNameClause(node.GetRule_role_name3(), roleName, allowSystemRoles)) {
                return false;
            }

            TMaybe<TRoleParameters> roleParams;
            if (node.HasBlock4()) {
                roleParams.ConstructInPlace();
                if (!RoleParameters(node.GetBlock4().GetRule_create_user_option1(), *roleParams)) {
                    return false;
                }
            }

            AddStatementToBlocks(blocks, BuildCreateUser(pos, service, cluster, roleName, roleParams, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore23: {
            // alter_user_stmt: ALTER USER role_name (WITH? create_user_option | RENAME TO role_name);
            Ctx.BodyPart();
            auto& node = core.GetAlt_sql_stmt_core23().GetRule_alter_user_stmt1();

            Ctx.Token(node.GetToken1());
            const TPosition pos = Ctx.Pos();

            TString service = Ctx.Scoped->CurrService;
            TDeferredAtom cluster = Ctx.Scoped->CurrCluster;
            if (cluster.Empty()) {
                Error() << "USE statement is missing - no default cluster is selected";
                return false;
            }

            TDeferredAtom roleName;
            {
                bool allowSystemRoles = true;
                if (!RoleNameClause(node.GetRule_role_name3(), roleName, allowSystemRoles)) {
                    return false;
                }
            }

            TNodePtr stmt;
            switch (node.GetBlock4().Alt_case()) {
                case TRule_alter_user_stmt_TBlock4::kAlt1: {
                    TRoleParameters roleParams;
                    if (!RoleParameters(node.GetBlock4().GetAlt1().GetRule_create_user_option2(), roleParams)) {
                        return false;
                    }
                    stmt = BuildAlterUser(pos, service, cluster, roleName, roleParams, Ctx.Scoped);
                    break;
                }
                case TRule_alter_user_stmt_TBlock4::kAlt2: {
                    TDeferredAtom tgtRoleName;
                    bool allowSystemRoles = false;
                    if (!RoleNameClause(node.GetBlock4().GetAlt2().GetRule_role_name3(), tgtRoleName, allowSystemRoles)) {
                        return false;
                    }
                    stmt = BuildRenameUser(pos, service, cluster, roleName, tgtRoleName,Ctx.Scoped);
                    break;
                }
                case TRule_alter_user_stmt_TBlock4::ALT_NOT_SET:
                    Y_ABORT("You should change implementation according to grammar changes");
            }

            AddStatementToBlocks(blocks, stmt);
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore24: {
            // create_group_stmt: CREATE GROUP role_name (WITH USER role_name (COMMA role_name)* COMMA?)?;
            Ctx.BodyPart();
            auto& node = core.GetAlt_sql_stmt_core24().GetRule_create_group_stmt1();

            Ctx.Token(node.GetToken1());
            const TPosition pos = Ctx.Pos();

            TString service = Ctx.Scoped->CurrService;
            TDeferredAtom cluster = Ctx.Scoped->CurrCluster;
            if (cluster.Empty()) {
                Error() << "USE statement is missing - no default cluster is selected";
                return false;
            }

            TDeferredAtom roleName;
            bool allowSystemRoles = false;
            if (!RoleNameClause(node.GetRule_role_name3(), roleName, allowSystemRoles)) {
                return false;
            }

            TRoleParameters roleParams;
            if (node.HasBlock4()) {
                auto& addDropNode = node.GetBlock4();
                TVector<TDeferredAtom> roles;
                bool allowSystemRoles = false;
                roleParams.Roles.emplace_back();
                if (!RoleNameClause(addDropNode.GetRule_role_name3(), roleParams.Roles.back(), allowSystemRoles)) {
                    return false;
                }

                for (auto& item : addDropNode.GetBlock4()) {
                    roleParams.Roles.emplace_back();
                    if (!RoleNameClause(item.GetRule_role_name2(), roleParams.Roles.back(), allowSystemRoles)) {
                        return false;
                    }
                }
            }

            AddStatementToBlocks(blocks, BuildCreateGroup(pos, service, cluster, roleName, roleParams, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore25: {
            // alter_group_stmt: ALTER GROUP role_name ((ADD|DROP) USER role_name (COMMA role_name)* COMMA? | RENAME TO role_name);
            Ctx.BodyPart();
            auto& node = core.GetAlt_sql_stmt_core25().GetRule_alter_group_stmt1();

            Ctx.Token(node.GetToken1());
            const TPosition pos = Ctx.Pos();

            TString service = Ctx.Scoped->CurrService;
            TDeferredAtom cluster = Ctx.Scoped->CurrCluster;
            if (cluster.Empty()) {
                Error() << "USE statement is missing - no default cluster is selected";
                return false;
            }

            TDeferredAtom roleName;
            {
                bool allowSystemRoles = true;
                if (!RoleNameClause(node.GetRule_role_name3(), roleName, allowSystemRoles)) {
                    return false;
                }
            }

            TNodePtr stmt;
            switch (node.GetBlock4().Alt_case()) {
                case TRule_alter_group_stmt_TBlock4::kAlt1: {
                    auto& addDropNode = node.GetBlock4().GetAlt1();
                    const bool isDrop = addDropNode.GetToken1().GetId() == SQLv1LexerTokens::TOKEN_DROP;
                    TVector<TDeferredAtom> roles;
                    bool allowSystemRoles = false;
                    roles.emplace_back();
                    if (!RoleNameClause(addDropNode.GetRule_role_name3(), roles.back(), allowSystemRoles)) {
                        return false;
                    }

                    for (auto& item : addDropNode.GetBlock4()) {
                        roles.emplace_back();
                        if (!RoleNameClause(item.GetRule_role_name2(), roles.back(), allowSystemRoles)) {
                            return false;
                        }
                    }

                    stmt = BuildAlterGroup(pos, service, cluster, roleName, roles, isDrop, Ctx.Scoped);
                    break;
                }
                case TRule_alter_group_stmt_TBlock4::kAlt2: {
                    TDeferredAtom tgtRoleName;
                    bool allowSystemRoles = false;
                    if (!RoleNameClause(node.GetBlock4().GetAlt2().GetRule_role_name3(), tgtRoleName, allowSystemRoles)) {
                        return false;
                    }
                    stmt = BuildRenameGroup(pos, service, cluster, roleName, tgtRoleName, Ctx.Scoped);
                    break;
                }
                case TRule_alter_group_stmt_TBlock4::ALT_NOT_SET:
                    Y_ABORT("You should change implementation according to grammar changes");
            }

            AddStatementToBlocks(blocks, stmt);
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore26: {
            // drop_role_stmt: DROP (USER|GROUP) (IF EXISTS)? role_name (COMMA role_name)* COMMA?;
            Ctx.BodyPart();
            auto& node = core.GetAlt_sql_stmt_core26().GetRule_drop_role_stmt1();

            Ctx.Token(node.GetToken1());
            const TPosition pos = Ctx.Pos();

            TString service = Ctx.Scoped->CurrService;
            TDeferredAtom cluster = Ctx.Scoped->CurrCluster;
            if (cluster.Empty()) {
                Error() << "USE statement is missing - no default cluster is selected";
                return false;
            }

            const bool isUser = node.GetToken2().GetId() == SQLv1LexerTokens::TOKEN_USER;
            bool missingOk = false;
            if (node.HasBlock3()) { // IF EXISTS
                missingOk = true;
                Y_DEBUG_ABORT_UNLESS(
                    node.GetBlock3().GetToken1().GetId() == SQLv1LexerTokens::TOKEN_IF &&
                    node.GetBlock3().GetToken2().GetId() == SQLv1LexerTokens::TOKEN_EXISTS
                );
            }

            TVector<TDeferredAtom> roles;
            bool allowSystemRoles = true;
            roles.emplace_back();
            if (!RoleNameClause(node.GetRule_role_name4(), roles.back(), allowSystemRoles)) {
                return false;
            }

            for (auto& item : node.GetBlock5()) {
                roles.emplace_back();
                if (!RoleNameClause(item.GetRule_role_name2(), roles.back(), allowSystemRoles)) {
                    return false;
                }
            }

            AddStatementToBlocks(blocks, BuildDropRoles(pos, service, cluster, roles, isUser, missingOk, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore27: {
            // create_object_stmt: CREATE OBJECT (IF NOT EXISTS)? name (TYPE type [WITH k=v,...]);
            auto& node = core.GetAlt_sql_stmt_core27().GetRule_create_object_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref4().HasBlock1()) {
                if (!ClusterExpr(node.GetRule_object_ref4().GetBlock1().GetRule_cluster_expr1(),
                    false, context.ServiceId, context.Cluster)) {
                    return false;
                }
            }

            bool existingOk = false;
            if (node.HasBlock3()) { // IF NOT EXISTS
                existingOk = true;
                Y_DEBUG_ABORT_UNLESS(
                    node.GetBlock3().GetToken1().GetId() == SQLv1LexerTokens::TOKEN_IF &&
                    node.GetBlock3().GetToken2().GetId() == SQLv1LexerTokens::TOKEN_NOT &&
                    node.GetBlock3().GetToken3().GetId() == SQLv1LexerTokens::TOKEN_EXISTS
                );
            }

            const TString& objectId = Id(node.GetRule_object_ref4().GetRule_id_or_at2(), *this).second;
            const TString& typeId = Id(node.GetRule_object_type_ref7().GetRule_an_id_or_type1(), *this);
            std::map<TString, TDeferredAtom> kv;
            if (node.HasBlock9()) {
                if (!ParseObjectFeatures(kv, node.GetBlock9().GetRule_create_object_features1().GetRule_object_features2())) {
                    return false;
                }
            }

            AddStatementToBlocks(blocks, BuildCreateObjectOperation(Ctx.Pos(), objectId, typeId, existingOk, false, std::move(kv), context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore28: {
            // alter_object_stmt: ALTER OBJECT name (TYPE type [SET k=v,...]);
            auto& node = core.GetAlt_sql_stmt_core28().GetRule_alter_object_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref3().HasBlock1()) {
                if (!ClusterExpr(node.GetRule_object_ref3().GetBlock1().GetRule_cluster_expr1(),
                    false, context.ServiceId, context.Cluster)) {
                    return false;
                }
            }

            const TString& objectId = Id(node.GetRule_object_ref3().GetRule_id_or_at2(), *this).second;
            const TString& typeId = Id(node.GetRule_object_type_ref6().GetRule_an_id_or_type1(), *this);
            std::map<TString, TDeferredAtom> kv;
            if (!ParseObjectFeatures(kv, node.GetRule_alter_object_features8().GetRule_object_features2())) {
                return false;
            }

            AddStatementToBlocks(blocks, BuildAlterObjectOperation(Ctx.Pos(), objectId, typeId, std::move(kv), std::set<TString>(), context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore29: {
            // drop_object_stmt: DROP OBJECT (IF EXISTS)? name (TYPE type [WITH k=v,...]);
            auto& node = core.GetAlt_sql_stmt_core29().GetRule_drop_object_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref4().HasBlock1()) {
                if (!ClusterExpr(node.GetRule_object_ref4().GetBlock1().GetRule_cluster_expr1(),
                    false, context.ServiceId, context.Cluster)) {
                    return false;
                }
            }

            bool missingOk = false;
            if (node.HasBlock3()) { // IF EXISTS
                missingOk = true;
                Y_DEBUG_ABORT_UNLESS(
                    node.GetBlock3().GetToken1().GetId() == SQLv1LexerTokens::TOKEN_IF &&
                    node.GetBlock3().GetToken2().GetId() == SQLv1LexerTokens::TOKEN_EXISTS
                );
            }

            const TString& objectId = Id(node.GetRule_object_ref4().GetRule_id_or_at2(), *this).second;
            const TString& typeId = Id(node.GetRule_object_type_ref7().GetRule_an_id_or_type1(), *this);
            std::map<TString, TDeferredAtom> kv;
            if (node.HasBlock9()) {
                if (!ParseObjectFeatures(kv, node.GetBlock9().GetRule_drop_object_features1().GetRule_object_features2())) {
                    return false;
                }
            }

            AddStatementToBlocks(blocks, BuildDropObjectOperation(Ctx.Pos(), objectId, typeId, missingOk, std::move(kv), context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore30: {
            // create_external_data_source_stmt: CREATE (OR REPLACE)? EXTERNAL DATA SOURCE (IF NOT EXISTS)? name WITH (k=v,...);
            auto& node = core.GetAlt_sql_stmt_core30().GetRule_create_external_data_source_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref7().HasBlock1()) {
                if (!ClusterExpr(node.GetRule_object_ref7().GetBlock1().GetRule_cluster_expr1(),
                    false, context.ServiceId, context.Cluster)) {
                    return false;
                }
            }

            bool replaceIfExists = false;
            if (node.HasBlock2()) { // OR REPLACE
                replaceIfExists = true;
                Y_DEBUG_ABORT_UNLESS(
                    node.GetBlock2().GetToken1().GetId() == SQLv1LexerTokens::TOKEN_OR &&
                    node.GetBlock2().GetToken2().GetId() == SQLv1LexerTokens::TOKEN_REPLACE
                );
            }

            bool existingOk = false;
            if (node.HasBlock6()) { // IF NOT EXISTS
                existingOk = true;
                Y_DEBUG_ABORT_UNLESS(
                    node.GetBlock6().GetToken1().GetId() == SQLv1LexerTokens::TOKEN_IF &&
                    node.GetBlock6().GetToken2().GetId() == SQLv1LexerTokens::TOKEN_NOT &&
                    node.GetBlock6().GetToken3().GetId() == SQLv1LexerTokens::TOKEN_EXISTS
                );
            }

            const TString& objectId = Id(node.GetRule_object_ref7().GetRule_id_or_at2(), *this).second;
            std::map<TString, TDeferredAtom> kv;
            if (!ParseExternalDataSourceSettings(kv, node.GetRule_with_table_settings8())) {
                return false;
            }

            AddStatementToBlocks(blocks, BuildCreateObjectOperation(Ctx.Pos(), BuildTablePath(Ctx.GetPrefixPath(context.ServiceId, context.Cluster), objectId), "EXTERNAL_DATA_SOURCE", existingOk, replaceIfExists, std::move(kv), context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore31: {
            // alter_external_data_source_stmt: ALTER EXTERNAL DATA SOURCE object_ref alter_external_data_source_action (COMMA alter_external_data_source_action)*
            Ctx.BodyPart();
            const auto& node = core.GetAlt_sql_stmt_core31().GetRule_alter_external_data_source_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref5().HasBlock1()) {
                if (!ClusterExpr(node.GetRule_object_ref5().GetBlock1().GetRule_cluster_expr1(),
                    false, context.ServiceId, context.Cluster)) {
                    return false;
                }
            }

            const TString& objectId = Id(node.GetRule_object_ref5().GetRule_id_or_at2(), *this).second;
            std::map<TString, TDeferredAtom> kv;
            std::set<TString> toReset;
            if (!ParseExternalDataSourceSettings(kv, toReset, node.GetRule_alter_external_data_source_action6())) {
                return false;
            }

            for (const auto& action : node.GetBlock7()) {
                if (!ParseExternalDataSourceSettings(kv, toReset, action.GetRule_alter_external_data_source_action2())) {
                    return false;
                }
            }

            AddStatementToBlocks(blocks, BuildAlterObjectOperation(Ctx.Pos(), objectId, "EXTERNAL_DATA_SOURCE", std::move(kv), std::move(toReset), context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore32: {
            // drop_external_data_source_stmt: DROP EXTERNAL DATA SOURCE (IF EXISTS)? name;
            auto& node = core.GetAlt_sql_stmt_core32().GetRule_drop_external_data_source_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref6().HasBlock1()) {
                if (!ClusterExpr(node.GetRule_object_ref6().GetBlock1().GetRule_cluster_expr1(),
                    false, context.ServiceId, context.Cluster)) {
                    return false;
                }
            }

            bool missingOk = false;
            if (node.HasBlock5()) { // IF EXISTS
                missingOk = true;
                Y_DEBUG_ABORT_UNLESS(
                    node.GetBlock5().GetToken1().GetId() == SQLv1LexerTokens::TOKEN_IF &&
                    node.GetBlock5().GetToken2().GetId() == SQLv1LexerTokens::TOKEN_EXISTS
                );
            }

            const TString& objectId = Id(node.GetRule_object_ref6().GetRule_id_or_at2(), *this).second;
            AddStatementToBlocks(blocks, BuildDropObjectOperation(Ctx.Pos(), BuildTablePath(Ctx.GetPrefixPath(context.ServiceId, context.Cluster), objectId), "EXTERNAL_DATA_SOURCE", missingOk, {}, context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore33: {
            // create_replication_stmt: CREATE ASYNC REPLICATION
            auto& node = core.GetAlt_sql_stmt_core33().GetRule_create_replication_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref4().HasBlock1()) {
                const auto& cluster = node.GetRule_object_ref4().GetBlock1().GetRule_cluster_expr1();
                if (!ClusterExpr(cluster, false, context.ServiceId, context.Cluster)) {
                    return false;
                }
            }

            auto prefixPath = Ctx.GetPrefixPath(context.ServiceId, context.Cluster);

            std::vector<std::pair<TString, TString>> targets;
            if (!AsyncReplicationTarget(targets, prefixPath, node.GetRule_replication_target6(), *this)) {
                return false;
            }
            for (auto& block : node.GetBlock7()) {
                if (!AsyncReplicationTarget(targets, prefixPath, block.GetRule_replication_target2(), *this)) {
                    return false;
                }
            }

            std::map<TString, TNodePtr> settings;
            if (!AsyncReplicationSettings(settings, node.GetRule_replication_settings10(), *this, true)) {
                return false;
            }

            const TString id = Id(node.GetRule_object_ref4().GetRule_id_or_at2(), *this).second;
            AddStatementToBlocks(blocks, BuildCreateAsyncReplication(Ctx.Pos(), BuildTablePath(prefixPath, id),
                std::move(targets), std::move(settings), context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore34: {
            // drop_replication_stmt: DROP ASYNC REPLICATION
            auto& node = core.GetAlt_sql_stmt_core34().GetRule_drop_replication_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref4().HasBlock1()) {
                const auto& cluster = node.GetRule_object_ref4().GetBlock1().GetRule_cluster_expr1();
                if (!ClusterExpr(cluster, false, context.ServiceId, context.Cluster)) {
                    return false;
                }
            }

            const TString id = Id(node.GetRule_object_ref4().GetRule_id_or_at2(), *this).second;
            AddStatementToBlocks(blocks, BuildDropAsyncReplication(Ctx.Pos(),
                BuildTablePath(Ctx.GetPrefixPath(context.ServiceId, context.Cluster), id),
                node.HasBlock5(), context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore35: {
            Ctx.BodyPart();
            // create_topic_stmt: CREATE TOPIC topic1 (CONSUMER ...)? [WITH (opt1 = val1, ...]?
            auto& rule = core.GetAlt_sql_stmt_core35().GetRule_create_topic_stmt1();
            TTopicRef tr;
            if (!TopicRefImpl(rule.GetRule_topic_ref3(), tr)) {
                return false;
            }

            TCreateTopicParameters params;
            if (rule.HasBlock4()) { //create_topic_entry (consumers)
                auto& entries = rule.GetBlock4().GetRule_create_topic_entries1();
                auto& firstEntry = entries.GetRule_create_topic_entry2();
                if (!CreateTopicEntry(firstEntry, params)) {
                    return false;
                }
                const auto& list = entries.GetBlock3();
                for (auto& node : list) {
                    if (!CreateTopicEntry(node.GetRule_create_topic_entry2(), params)) {
                        return false;
                    }
                }

            }
            if (rule.HasBlock5()) { // with_topic_settings
                auto& topic_settings_node = rule.GetBlock5().GetRule_with_topic_settings1().GetRule_topic_settings3();
                CreateTopicSettings(topic_settings_node, params.TopicSettings);
            }

            AddStatementToBlocks(blocks, BuildCreateTopic(Ctx.Pos(), tr, params, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore36: {
//            alter_topic_stmt: ALTER TOPIC topic_ref alter_topic_action (COMMA alter_topic_action)*;

            Ctx.BodyPart();
            auto& rule = core.GetAlt_sql_stmt_core36().GetRule_alter_topic_stmt1();
            TTopicRef tr;
            if (!TopicRefImpl(rule.GetRule_topic_ref3(), tr)) {
                return false;
            }

            TAlterTopicParameters params;
            auto& firstEntry = rule.GetRule_alter_topic_action4();
            if (!AlterTopicAction(firstEntry, params)) {
                return false;
            }
            const auto& list = rule.GetBlock5();
            for (auto& node : list) {
                if (!AlterTopicAction(node.GetRule_alter_topic_action2(), params)) {
                    return false;
                }
            }

            AddStatementToBlocks(blocks, BuildAlterTopic(Ctx.Pos(), tr, params, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore37: {
            // drop_topic_stmt: DROP TOPIC
            Ctx.BodyPart();
            const auto& rule = core.GetAlt_sql_stmt_core37().GetRule_drop_topic_stmt1();

            TTopicRef tr;
            if (!TopicRefImpl(rule.GetRule_topic_ref3(), tr)) {
                return false;
            }
            AddStatementToBlocks(blocks, BuildDropTopic(Ctx.Pos(), tr, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore38: {
            // GRANT permission_name_target ON an_id_schema (COMMA an_id_schema)* TO role_name (COMMA role_name)* COMMA? (WITH GRANT OPTION)?;
            Ctx.BodyPart();
            auto& node = core.GetAlt_sql_stmt_core38().GetRule_grant_permissions_stmt1();

            Ctx.Token(node.GetToken1());
            const TPosition pos = Ctx.Pos();

            TString service = Ctx.Scoped->CurrService;
            TDeferredAtom cluster = Ctx.Scoped->CurrCluster;
            if (cluster.Empty()) {
                Error() << "USE statement is missing - no default cluster is selected";
                return false;
            }

            TVector<TDeferredAtom> permissions;
            if (!PermissionNameClause(node.GetRule_permission_name_target2(), permissions, node.has_block10())) {
                return false;
            }

            TVector<TDeferredAtom> schemaPaths;
            schemaPaths.emplace_back(Ctx.Pos(), Id(node.GetRule_an_id_schema4(), *this));
            for (const auto& item : node.GetBlock5()) {
                schemaPaths.emplace_back(Ctx.Pos(), Id(item.GetRule_an_id_schema2(), *this));
            }

            TVector<TDeferredAtom> roleNames;
            const bool allowSystemRoles = false;
            roleNames.emplace_back();
            if (!RoleNameClause(node.GetRule_role_name7(), roleNames.back(), allowSystemRoles)) {
                return false;
            }
            for (const auto& item : node.GetBlock8()) {
                roleNames.emplace_back();
                if (!RoleNameClause(item.GetRule_role_name2(), roleNames.back(), allowSystemRoles)) {
                    return false;
                }
            }

            AddStatementToBlocks(blocks, BuildGrantPermissions(pos, service, cluster, permissions, schemaPaths, roleNames, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore39:
        {
            // REVOKE (GRANT OPTION FOR)? permission_name_target ON an_id_schema (COMMA an_id_schema)* FROM role_name (COMMA role_name)*;
            Ctx.BodyPart();
            auto& node = core.GetAlt_sql_stmt_core39().GetRule_revoke_permissions_stmt1();

            Ctx.Token(node.GetToken1());
            const TPosition pos = Ctx.Pos();

            TString service = Ctx.Scoped->CurrService;
            TDeferredAtom cluster = Ctx.Scoped->CurrCluster;
            if (cluster.Empty()) {
                Error() << "USE statement is missing - no default cluster is selected";
                return false;
            }

            TVector<TDeferredAtom> permissions;
            if (!PermissionNameClause(node.GetRule_permission_name_target3(), permissions, node.HasBlock2())) {
                return false;
            }

            TVector<TDeferredAtom> schemaPaths;
            schemaPaths.emplace_back(Ctx.Pos(), Id(node.GetRule_an_id_schema5(), *this));
            for (const auto& item : node.GetBlock6()) {
                schemaPaths.emplace_back(Ctx.Pos(), Id(item.GetRule_an_id_schema2(), *this));
            }

            TVector<TDeferredAtom> roleNames;
            const bool allowSystemRoles = false;
            roleNames.emplace_back();
            if (!RoleNameClause(node.GetRule_role_name8(), roleNames.back(), allowSystemRoles)) {
                return false;
            }
            for (const auto& item : node.GetBlock9()) {
                roleNames.emplace_back();
                if (!RoleNameClause(item.GetRule_role_name2(), roleNames.back(), allowSystemRoles)) {
                    return false;
                }
            }

            AddStatementToBlocks(blocks, BuildRevokePermissions(pos, service, cluster, permissions, schemaPaths, roleNames, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore40:
        {
            // ALTER TABLESTORE object_ref alter_table_store_action (COMMA alter_table_store_action)*;
            auto& node = core.GetAlt_sql_stmt_core40().GetRule_alter_table_store_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);

            if (node.GetRule_object_ref3().HasBlock1()) {
                if (!ClusterExpr(node.GetRule_object_ref3().GetBlock1().GetRule_cluster_expr1(),
                    false, context.ServiceId, context.Cluster)) {
                    return false;
                }
            }

            const TString& objectId = Id(node.GetRule_object_ref3().GetRule_id_or_at2(), *this).second;
            const TString& typeId = "TABLESTORE";
            std::map<TString, TDeferredAtom> kv;
            if (!ParseTableStoreFeatures(kv, node.GetRule_alter_table_store_action4())) {
                return false;
            }

            AddStatementToBlocks(blocks, BuildAlterObjectOperation(Ctx.Pos(), objectId, typeId, std::move(kv), std::set<TString>(), context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore41:
        {
            // create_object_stmt: UPSERT OBJECT name (TYPE type [WITH k=v,...]);
            auto& node = core.GetAlt_sql_stmt_core41().GetRule_upsert_object_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref3().HasBlock1()) {
                if (!ClusterExpr(node.GetRule_object_ref3().GetBlock1().GetRule_cluster_expr1(),
                    false, context.ServiceId, context.Cluster)) {
                    return false;
                }
            }

            const TString& objectId = Id(node.GetRule_object_ref3().GetRule_id_or_at2(), *this).second;
            const TString& typeId = Id(node.GetRule_object_type_ref6().GetRule_an_id_or_type1(), *this);
            std::map<TString, TDeferredAtom> kv;
            if (node.HasBlock8()) {
                if (!ParseObjectFeatures(kv, node.GetBlock8().GetRule_create_object_features1().GetRule_object_features2())) {
                    return false;
                }
            }

            AddStatementToBlocks(blocks, BuildUpsertObjectOperation(Ctx.Pos(), objectId, typeId, std::move(kv), context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore42: {
            // create_view_stmt: CREATE VIEW name WITH (k = v, ...) AS select_stmt;
            auto& node = core.GetAlt_sql_stmt_core42().GetRule_create_view_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref3().HasBlock1()) {
                if (!ClusterExpr(node.GetRule_object_ref3().GetBlock1().GetRule_cluster_expr1(),
                                 false,
                                 context.ServiceId,
                                 context.Cluster)) {
                    return false;
                }
            }

            std::map<TString, TDeferredAtom> features;
            ParseViewOptions(features, node.GetRule_with_table_settings4());
            ParseViewQuery(features, node.GetRule_select_stmt6());

            const TString objectId = Id(node.GetRule_object_ref3().GetRule_id_or_at2(), *this).second;
            constexpr const char* TypeId = "VIEW";
            AddStatementToBlocks(blocks,
                                 BuildCreateObjectOperation(Ctx.Pos(),
                                                            BuildTablePath(Ctx.GetPrefixPath(context.ServiceId, context.Cluster), objectId),
                                                            TypeId,
                                                            false,
                                                            false,
                                                            std::move(features),
                                                            context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore43: {
            // drop_view_stmt: DROP VIEW name;
            auto& node = core.GetAlt_sql_stmt_core43().GetRule_drop_view_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref3().HasBlock1()) {
                if (!ClusterExpr(node.GetRule_object_ref3().GetBlock1().GetRule_cluster_expr1(),
                                 false,
                                 context.ServiceId,
                                 context.Cluster)) {
                    return false;
                }
            }

            const TString objectId = Id(node.GetRule_object_ref3().GetRule_id_or_at2(), *this).second;
            constexpr const char* TypeId = "VIEW";
            AddStatementToBlocks(blocks,
                                 BuildDropObjectOperation(Ctx.Pos(),
                                                          BuildTablePath(Ctx.GetPrefixPath(context.ServiceId, context.Cluster), objectId),
                                                          TypeId,
                                                          false,
                                                          {},
                                                          context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore44: {
            // alter_replication_stmt: ALTER ASYNC REPLICATION
            auto& node = core.GetAlt_sql_stmt_core44().GetRule_alter_replication_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref4().HasBlock1()) {
                const auto& cluster = node.GetRule_object_ref4().GetBlock1().GetRule_cluster_expr1();
                if (!ClusterExpr(cluster, false, context.ServiceId, context.Cluster)) {
                    return false;
                }
            }

            std::map<TString, TNodePtr> settings;
            if (!AsyncReplicationAlterAction(settings, node.GetRule_alter_replication_action5(), *this)) {
                return false;
            }
            for (auto& block : node.GetBlock6()) {
                if (!AsyncReplicationAlterAction(settings, block.GetRule_alter_replication_action2(), *this)) {
                    return false;
                }
            }

            const TString id = Id(node.GetRule_object_ref4().GetRule_id_or_at2(), *this).second;
            AddStatementToBlocks(blocks, BuildAlterAsyncReplication(Ctx.Pos(),
                BuildTablePath(Ctx.GetPrefixPath(context.ServiceId, context.Cluster), id),
                std::move(settings), context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore45: {
            // create_resource_pool_stmt: CREATE RESOURCE POOL name WITH (k=v,...);
            auto& node = core.GetAlt_sql_stmt_core45().GetRule_create_resource_pool_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref4().HasBlock1()) {
                if (!ClusterExpr(node.GetRule_object_ref4().GetBlock1().GetRule_cluster_expr1(),
                    false, context.ServiceId, context.Cluster)) {
                    return false;
                }
            }

            const TString& objectId = Id(node.GetRule_object_ref4().GetRule_id_or_at2(), *this).second;
            std::map<TString, TDeferredAtom> kv;
            if (!ParseResourcePoolSettings(kv, node.GetRule_with_table_settings5())) {
                return false;
            }

            AddStatementToBlocks(blocks, BuildCreateObjectOperation(Ctx.Pos(), objectId, "RESOURCE_POOL", false, false, std::move(kv), context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore46: {
            // alter_resource_pool_stmt: ALTER RESOURCE POOL object_ref alter_resource_pool_action (COMMA alter_external_data_source_action)*
            Ctx.BodyPart();
            const auto& node = core.GetAlt_sql_stmt_core46().GetRule_alter_resource_pool_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref4().HasBlock1()) {
                if (!ClusterExpr(node.GetRule_object_ref4().GetBlock1().GetRule_cluster_expr1(),
                    false, context.ServiceId, context.Cluster)) {
                    return false;
                }
            }

            const TString& objectId = Id(node.GetRule_object_ref4().GetRule_id_or_at2(), *this).second;
            std::map<TString, TDeferredAtom> kv;
            std::set<TString> toReset;
            if (!ParseResourcePoolSettings(kv, toReset, node.GetRule_alter_resource_pool_action5())) {
                return false;
            }

            for (const auto& action : node.GetBlock6()) {
                if (!ParseResourcePoolSettings(kv, toReset, action.GetRule_alter_resource_pool_action2())) {
                    return false;
                }
            }

            AddStatementToBlocks(blocks, BuildAlterObjectOperation(Ctx.Pos(), objectId, "RESOURCE_POOL", std::move(kv), std::move(toReset), context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore47: {
            // drop_resource_pool_stmt: DROP RESOURCE POOL name;
            auto& node = core.GetAlt_sql_stmt_core47().GetRule_drop_resource_pool_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref4().HasBlock1()) {
                if (!ClusterExpr(node.GetRule_object_ref4().GetBlock1().GetRule_cluster_expr1(),
                    false, context.ServiceId, context.Cluster)) {
                    return false;
                }
            }

            const TString& objectId = Id(node.GetRule_object_ref4().GetRule_id_or_at2(), *this).second;
            AddStatementToBlocks(blocks, BuildDropObjectOperation(Ctx.Pos(), objectId, "RESOURCE_POOL", false, {}, context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore48: {
            // create_backup_collection_stmt: CREATE BACKUP COLLECTION name WITH (k=v,...);
            auto& node = core.GetAlt_sql_stmt_core48().GetRule_create_backup_collection_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_backup_collection2().GetRule_object_ref3().HasBlock1()) {
                if (!ClusterExpr(node.GetRule_backup_collection2().GetRule_object_ref3().GetBlock1().GetRule_cluster_expr1(),
                                 false,
                                 context.ServiceId,
                                 context.Cluster)) {
                    return false;
                }
            }

            std::map<TString, TDeferredAtom> kv;
            if (!ParseBackupCollectionSettings(kv, node.GetRule_backup_collection_settings5())) {
                 return false;
            }

            const TString& objectId = Id(node.GetRule_backup_collection2().GetRule_object_ref3().GetRule_id_or_at2(), *this).second;
            constexpr const char* typeId = "BACKUP_COLLECTION";
            AddStatementToBlocks(blocks,
                                 BuildCreateObjectOperation(Ctx.Pos(),
                                                            BuildTablePath(Ctx.GetPrefixPath(context.ServiceId, context.Cluster), objectId),
                                                            typeId,
                                                            false,
                                                            false,
                                                            std::move(kv),
                                                            context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore49: {
            // alter_backup_collection_stmt: ALTER BACKUP COLLECTION name alter_backup_collection_action (COMMA alter_backup_collection_action)*;
            auto& node = core.GetAlt_sql_stmt_core49().GetRule_alter_backup_collection_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_backup_collection2().GetRule_object_ref3().HasBlock1()) {
                if (!ClusterExpr(node.GetRule_backup_collection2().GetRule_object_ref3().GetBlock1().GetRule_cluster_expr1(),
                                 false,
                                 context.ServiceId,
                                 context.Cluster)) {
                    return false;
                }
            }

            std::map<TString, TDeferredAtom> kv;
            std::set<TString> toReset;
            if (!ParseBackupCollectionSettings(kv, toReset, node.GetRule_alter_backup_collection_actions3())) {
                return false;
            }

            const TString& objectId = Id(node.GetRule_backup_collection2().GetRule_object_ref3().GetRule_id_or_at2(), *this).second;
            constexpr const char* typeId = "BACKUP_COLLECTION";
            AddStatementToBlocks(blocks,
                                 BuildAlterObjectOperation(Ctx.Pos(),
                                                           BuildTablePath(Ctx.GetPrefixPath(context.ServiceId, context.Cluster), objectId),
                                                           typeId,
                                                           std::move(kv),
                                                           std::move(toReset),
                                                           context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore50: {
            // drop_backup_collection_stmt: DROP BACKUP COLLECTION name;
            auto& node = core.GetAlt_sql_stmt_core50().GetRule_drop_backup_collection_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_backup_collection2().GetRule_object_ref3().HasBlock1()) {
                if (!ClusterExpr(node.GetRule_backup_collection2().GetRule_object_ref3().GetBlock1().GetRule_cluster_expr1(),
                                 false,
                                 context.ServiceId,
                                 context.Cluster)) {
                    return false;
                }
            }

            const TString& objectId = Id(node.GetRule_backup_collection2().GetRule_object_ref3().GetRule_id_or_at2(), *this).second;
            constexpr const char* typeId = "BACKUP_COLLECTION";
            AddStatementToBlocks(blocks,
                                 BuildDropObjectOperation(Ctx.Pos(),
                                                          BuildTablePath(Ctx.GetPrefixPath(context.ServiceId, context.Cluster), objectId),
                                                          typeId,
                                                          false,
                                                          {},
                                                          context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore51: {
            // analyze_stmt: ANALYZE table_ref
            Ctx.BodyPart();
            const auto& rule = core.GetAlt_sql_stmt_core51().GetRule_analyze_stmt1();

            if (!rule.GetRule_analyze_table_list2().GetBlock2().empty()) {
                Error() << "ANALYZE with multitables hasn't been implemented yet";
                return false;
            }
            auto analyzeTable = rule.GetRule_analyze_table_list2().GetRule_analyze_table1();

            TVector<TString> columns;
            if (analyzeTable.HasBlock2()) {
                auto columnsNode = 
                    analyzeTable.GetBlock2().GetRule_column_list2();

                if (columnsNode.HasRule_column_name1()) {
                    columns.push_back(Id(columnsNode.GetRule_column_name1().GetRule_an_id2(), *this));
                    for (const auto& columnNode: columnsNode.GetBlock2()) {
                        columns.push_back(Id(columnNode.GetRule_column_name2().GetRule_an_id2(), *this));
                    }
                }
            }

            TTableRef tr;
            if (!SimpleTableRefImpl(rule.GetRule_analyze_table_list2().GetRule_analyze_table1().GetRule_simple_table_ref1(), tr)) {
                return false;
            }

            auto params = TAnalyzeParams{.Table = std::make_shared<TTableRef>(tr), .Columns = std::move(columns)};
            AddStatementToBlocks(blocks, BuildAnalyze(Ctx.Pos(), tr.Service, tr.Cluster, params, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::ALT_NOT_SET:
            Ctx.IncrementMonCounter("sql_errors", "UnknownStatement" + internalStatementName);
            AltNotImplemented("sql_stmt_core", core);
            return false;
    }

    Ctx.IncrementMonCounter("sql_features", internalStatementName);
    return !Ctx.HasPendingErrors;
}

bool TSqlQuery::DeclareStatement(const TRule_declare_stmt& stmt) {
    TNodePtr defaultValue;
    if (stmt.HasBlock5()) {
        TSqlExpression sqlExpr(Ctx, Mode);
        auto exprOrId = sqlExpr.LiteralExpr(stmt.GetBlock5().GetRule_literal_value2());
        if (!exprOrId) {
            return false;
        }
        if (!exprOrId->Expr) {
            Ctx.Error() << "Identifier is not expected here";
            return false;
        }
        defaultValue = exprOrId->Expr;
    }
    if (defaultValue) {
        Error() << "DEFAULT value not supported yet";
        return false;
    }
    if (!Ctx.IsParseHeading()) {
        Error() << "DECLARE statement should be in beginning of query, but it's possible to use PRAGMA or USE before it";
        return false;
    }

    TString varName;
    if (!NamedNodeImpl(stmt.GetRule_bind_parameter2(), varName, *this)) {
        return false;
    }
    const auto varPos = Ctx.Pos();
    const auto typeNode = TypeNode(stmt.GetRule_type_name4());
    if (!typeNode) {
        return false;
    }
    if (IsAnonymousName(varName)) {
        Ctx.Error(varPos) << "Can not use anonymous name '" << varName << "' in DECLARE statement";
        return false;
    }

    if (Ctx.IsAlreadyDeclared(varName)) {
        Ctx.Warning(varPos, TIssuesIds::YQL_DUPLICATE_DECLARE) << "Duplicate declaration of '" << varName << "' will be ignored";
    } else {
        PushNamedAtom(varPos, varName);
        Ctx.DeclareVariable(varName, varPos, typeNode);
    }
    return true;
}

bool TSqlQuery::ExportStatement(const TRule_export_stmt& stmt) {
    if (Mode != NSQLTranslation::ESqlMode::LIBRARY || !TopLevel) {
        Error() << "EXPORT statement should be used only in a library on the top level";
        return false;
    }

    TVector<TSymbolNameWithPos> bindNames;
    if (!BindList(stmt.GetRule_bind_parameter_list2(), bindNames)) {
        return false;
    }

    for (auto& bindName : bindNames) {
        if (!Ctx.AddExport(bindName.Pos, bindName.Name)) {
            return false;
        }
    }
    return true;
}

bool TSqlQuery::AlterTableAction(const TRule_alter_table_action& node, TAlterTableParameters& params) {
    if (params.RenameTo) {
        // rename action is followed by some other actions
        Error() << "RENAME TO can not be used together with another table action";
        return false;
    }

    switch (node.Alt_case()) {
    case TRule_alter_table_action::kAltAlterTableAction1: {
        // ADD COLUMN
        const auto& addRule = node.GetAlt_alter_table_action1().GetRule_alter_table_add_column1();
        if (!AlterTableAddColumn(addRule, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction2: {
        // DROP COLUMN
        const auto& dropRule = node.GetAlt_alter_table_action2().GetRule_alter_table_drop_column1();
        if (!AlterTableDropColumn(dropRule, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction3: {
        // ALTER COLUMN
        const auto& alterRule = node.GetAlt_alter_table_action3().GetRule_alter_table_alter_column1();
        if (!AlterTableAlterColumn(alterRule, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction4: {
        // ADD FAMILY
        const auto& familyEntry = node.GetAlt_alter_table_action4().GetRule_alter_table_add_column_family1()
            .GetRule_family_entry2();
        if (!AlterTableAddFamily(familyEntry, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction5: {
        // ALTER FAMILY
        const auto& alterRule = node.GetAlt_alter_table_action5().GetRule_alter_table_alter_column_family1();
        if (!AlterTableAlterFamily(alterRule, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction6: {
        // SET (uncompat)
        const auto& setRule = node.GetAlt_alter_table_action6().GetRule_alter_table_set_table_setting_uncompat1();
        if (!AlterTableSetTableSetting(setRule, params.TableSettings, params.TableType)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction7: {
        // SET (compat)
        const auto& setRule = node.GetAlt_alter_table_action7().GetRule_alter_table_set_table_setting_compat1();
        if (!AlterTableSetTableSetting(setRule, params.TableSettings, params.TableType)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction8: {
        // RESET
        const auto& setRule = node.GetAlt_alter_table_action8().GetRule_alter_table_reset_table_setting1();
        if (!AlterTableResetTableSetting(setRule, params.TableSettings, params.TableType)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction9: {
        // ADD INDEX
        const auto& addIndex = node.GetAlt_alter_table_action9().GetRule_alter_table_add_index1();
        if (!AlterTableAddIndex(addIndex, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction10: {
        // DROP INDEX
        const auto& dropIndex = node.GetAlt_alter_table_action10().GetRule_alter_table_drop_index1();
        AlterTableDropIndex(dropIndex, params);
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction11: {
        // RENAME TO
        if (!params.IsEmpty()) {
            // rename action follows some other actions
            Error() << "RENAME TO can not be used together with another table action";
            return false;
        }

        const auto& renameTo = node.GetAlt_alter_table_action11().GetRule_alter_table_rename_to1();
        AlterTableRenameTo(renameTo, params);
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction12: {
        // ADD CHANGEFEED
        const auto& rule = node.GetAlt_alter_table_action12().GetRule_alter_table_add_changefeed1();
        if (!AlterTableAddChangefeed(rule, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction13: {
        // ALTER CHANGEFEED
        const auto& rule = node.GetAlt_alter_table_action13().GetRule_alter_table_alter_changefeed1();
        if (!AlterTableAlterChangefeed(rule, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction14: {
        // DROP CHANGEFEED
        const auto& rule = node.GetAlt_alter_table_action14().GetRule_alter_table_drop_changefeed1();
        AlterTableDropChangefeed(rule, params);
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction15: {
        // RENAME INDEX TO
        if (!params.IsEmpty()) {
            // rename action follows some other actions
            Error() << "RENAME INDEX TO can not be used together with another table action";
            return false;
        }

        const auto& renameTo = node.GetAlt_alter_table_action15().GetRule_alter_table_rename_index_to1();
        AlterTableRenameIndexTo(renameTo, params);
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction16: {
        // ALTER INDEX
        const auto& rule = node.GetAlt_alter_table_action16().GetRule_alter_table_alter_index1();
        if (!AlterTableAlterIndex(rule, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction17: {
        // ALTER COLUMN id DROP NOT NULL
        const auto& alterRule = node.GetAlt_alter_table_action17().GetRule_alter_table_alter_column_drop_not_null1();

        if (!AlterTableAlterColumnDropNotNull(alterRule, params)) {
            return false;
        }

        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction18: {
        // ALTER COLUMN id SET NOT NULL
        const auto& alterRule = node.GetAlt_alter_table_action18().GetRule_alter_table_alter_column_set_not_null1();

        if (!AlterTableAlterColumnSetNotNull(alterRule, params)) {
            return false;
        }

        break;
    }

    case TRule_alter_table_action::ALT_NOT_SET: {
        AltNotImplemented("alter_table_action", node);
        return false;
    }
    }
    return true;
}

bool TSqlQuery::AlterExternalTableAction(const TRule_alter_external_table_action& node, TAlterTableParameters& params) {
    if (params.RenameTo) {
        // rename action is followed by some other actions
        Error() << "RENAME TO can not be used together with another table action";
        return false;
    }

    switch (node.Alt_case()) {
    case TRule_alter_external_table_action::kAltAlterExternalTableAction1: {
        // ADD COLUMN
        const auto& addRule = node.GetAlt_alter_external_table_action1().GetRule_alter_table_add_column1();
        if (!AlterTableAddColumn(addRule, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_external_table_action::kAltAlterExternalTableAction2: {
        // DROP COLUMN
        const auto& dropRule = node.GetAlt_alter_external_table_action2().GetRule_alter_table_drop_column1();
        if (!AlterTableDropColumn(dropRule, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_external_table_action::kAltAlterExternalTableAction3: {
        // SET (uncompat)
        const auto& setRule = node.GetAlt_alter_external_table_action3().GetRule_alter_table_set_table_setting_uncompat1();
        if (!AlterTableSetTableSetting(setRule, params.TableSettings, params.TableType)) {
            return false;
        }
        break;
    }
    case TRule_alter_external_table_action::kAltAlterExternalTableAction4: {
        // SET (compat)
        const auto& setRule = node.GetAlt_alter_external_table_action4().GetRule_alter_table_set_table_setting_compat1();
        if (!AlterTableSetTableSetting(setRule, params.TableSettings, params.TableType)) {
            return false;
        }
        break;
    }
    case TRule_alter_external_table_action::kAltAlterExternalTableAction5: {
        // RESET
        const auto& setRule = node.GetAlt_alter_external_table_action5().GetRule_alter_table_reset_table_setting1();
        if (!AlterTableResetTableSetting(setRule, params.TableSettings, params.TableType)) {
            return false;
        }
        break;
    }

    case TRule_alter_external_table_action::ALT_NOT_SET:
        AltNotImplemented("alter_external_table_action", node);
        return false;
    }
    return true;
}

bool TSqlQuery::AlterTableAddColumn(const TRule_alter_table_add_column& node, TAlterTableParameters& params) {
    auto columnSchema = ColumnSchemaImpl(node.GetRule_column_schema3());
    if (!columnSchema) {
        return false;
    }
    if (columnSchema->Families.size() > 1) {
        Ctx.Error() << "Several column families for a single column are not yet supported";
        return false;
    }
    params.AddColumns.push_back(*columnSchema);
    return true;
}

bool TSqlQuery::AlterTableDropColumn(const TRule_alter_table_drop_column& node, TAlterTableParameters& params) {
    TString name = Id(node.GetRule_an_id3(), *this);
    params.DropColumns.push_back(name);
    return true;
}

bool TSqlQuery::AlterTableAlterColumn(const TRule_alter_table_alter_column& node,
        TAlterTableParameters& params)
{
    TString name = Id(node.GetRule_an_id3(), *this);
    const TPosition pos(Context().Pos());
    TVector<TIdentifier> families;
    const auto& familyRelation = node.GetRule_family_relation5();
    families.push_back(IdEx(familyRelation.GetRule_an_id2(), *this));
    params.AlterColumns.emplace_back(pos, name, nullptr, false, families, false, nullptr, TColumnSchema::ETypeOfChange::SetFamily);
    return true;
}

bool TSqlQuery::AlterTableAddFamily(const TRule_family_entry& node, TAlterTableParameters& params) {
    TFamilyEntry family(IdEx(node.GetRule_an_id2(), *this));
    if (!FillFamilySettings(node.GetRule_family_settings3(), family)) {
        return false;
    }
    params.AddColumnFamilies.push_back(family);
    return true;
}

bool TSqlQuery::AlterTableAlterFamily(const TRule_alter_table_alter_column_family& node,
        TAlterTableParameters& params)
{
    TFamilyEntry* entry = nullptr;
    TIdentifier name = IdEx(node.GetRule_an_id3(), *this);
    for (auto& family : params.AlterColumnFamilies) {
        if (family.Name.Name == name.Name) {
            entry = &family;
            break;
        }
    }
    if (!entry) {
        entry = &params.AlterColumnFamilies.emplace_back(name);
    }
    TIdentifier settingName = IdEx(node.GetRule_an_id5(), *this);
    const TRule_family_setting_value& value = node.GetRule_family_setting_value6();
    if (to_lower(settingName.Name) == "data") {
        if (entry->Data) {
            Ctx.Error() << "Redefinition of 'data' setting for column family '" << name.Name
                << "' in one alter";
            return false;
        }
        const TString stringValue(Ctx.Token(value.GetToken1()));
        entry->Data = BuildLiteralSmartString(Ctx, stringValue);
    } else if (to_lower(settingName.Name) == "compression") {
        if (entry->Compression) {
            Ctx.Error() << "Redefinition of 'compression' setting for column family '" << name.Name
                << "' in one alter";
            return false;
        }
        const TString stringValue(Ctx.Token(value.GetToken1()));
        entry->Compression = BuildLiteralSmartString(Ctx, stringValue);
    } else {
        Ctx.Error() << "Unknown table setting: " << settingName.Name;
        return false;
    }
    return true;
}

bool TSqlQuery::AlterTableSetTableSetting(
    const TRule_alter_table_set_table_setting_uncompat& node, TTableSettings& tableSettings, ETableType tableType
) {
    return StoreTableSettingsEntry(
        IdEx(node.GetRule_an_id2(), *this),
        node.GetRule_table_setting_value3(),
        tableSettings,
        tableType,
        true
    );
}

bool TSqlQuery::AlterTableSetTableSetting(
    const TRule_alter_table_set_table_setting_compat& node, TTableSettings& tableSettings, ETableType tableType
) {
    const auto storeSetting = [&](const TRule_alter_table_setting_entry& entry) {
        return StoreTableSettingsEntry(
            IdEx(entry.GetRule_an_id1(), *this),
            entry.GetRule_table_setting_value3(),
            tableSettings,
            tableType,
            true
        );
    };

    const auto& firstEntry = node.GetRule_alter_table_setting_entry3();
    if (!storeSetting(firstEntry)) {
        return false;
    }
    for (const auto& block : node.GetBlock4()) {
        const auto& entry = block.GetRule_alter_table_setting_entry2();
        if (!storeSetting(entry)) {
            return false;
        }
    }
    return true;
}

bool TSqlQuery::AlterTableResetTableSetting(
    const TRule_alter_table_reset_table_setting& node, TTableSettings& tableSettings, ETableType tableType
) {
    const auto resetSetting = [&](const TRule_an_id& id) {
        return ResetTableSettingsEntry(IdEx(id, *this), tableSettings, tableType);
    };

    const auto& firstEntry = node.GetRule_an_id3();
    if (!resetSetting(firstEntry)) {
        return false;
    }
    for (const auto& block : node.GetBlock4()) {
        const auto& entry = block.GetRule_an_id2();
        if (!resetSetting(entry)) {
            return false;
        }
    }
    return true;
}

bool TSqlQuery::AlterTableAddIndex(const TRule_alter_table_add_index& node, TAlterTableParameters& params) {
    if (!CreateTableIndex(node.GetRule_table_index2(), params.AddIndexes)) {
        return false;
    }
    return true;
}

void TSqlQuery::AlterTableDropIndex(const TRule_alter_table_drop_index& node, TAlterTableParameters& params) {
    params.DropIndexes.emplace_back(IdEx(node.GetRule_an_id3(), *this));
}

void TSqlQuery::AlterTableRenameTo(const TRule_alter_table_rename_to& node, TAlterTableParameters& params) {
    params.RenameTo = IdEx(node.GetRule_an_id_table3(), *this);
}

void TSqlQuery::AlterTableRenameIndexTo(const TRule_alter_table_rename_index_to& node, TAlterTableParameters& params) {
    auto src = IdEx(node.GetRule_an_id3(), *this);
    auto dst = IdEx(node.GetRule_an_id5(), *this);

    params.RenameIndexTo = std::make_pair(src, dst);
}

bool TSqlQuery::AlterTableAlterIndex(const TRule_alter_table_alter_index& node, TAlterTableParameters& params) {
    const auto indexName = IdEx(node.GetRule_an_id3(), *this);
    params.AlterIndexes.emplace_back(indexName);
    TTableSettings& indexTableSettings = params.AlterIndexes.back().TableSettings;

    const auto& action = node.GetRule_alter_table_alter_index_action4();

    switch (action.Alt_case()) {
    case TRule_alter_table_alter_index_action::kAltAlterTableAlterIndexAction1: {
        // SET setting value
        const auto& rule = action.GetAlt_alter_table_alter_index_action1().GetRule_alter_table_set_table_setting_uncompat1();
        if (!AlterTableSetTableSetting(rule, indexTableSettings, params.TableType)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_alter_index_action::kAltAlterTableAlterIndexAction2: {
        // SET (setting1 = value1, ...)
        const auto& rule = action.GetAlt_alter_table_alter_index_action2().GetRule_alter_table_set_table_setting_compat1();
        if (!AlterTableSetTableSetting(rule, indexTableSettings, params.TableType)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_alter_index_action::kAltAlterTableAlterIndexAction3: {
        // RESET (setting1, ...)
        const auto& rule = action.GetAlt_alter_table_alter_index_action3().GetRule_alter_table_reset_table_setting1();
        if (!AlterTableResetTableSetting(rule, indexTableSettings, params.TableType)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_alter_index_action::ALT_NOT_SET:
        AltNotImplemented("alter_table_alter_index_action", action);
        return false;
    }

    return true;
}

bool TSqlQuery::AlterTableAlterColumnDropNotNull(const TRule_alter_table_alter_column_drop_not_null& node, TAlterTableParameters& params) {
    TString name = Id(node.GetRule_an_id3(), *this);
    const TPosition pos(Context().Pos());
    params.AlterColumns.emplace_back(pos, name, nullptr, false, TVector<TIdentifier>(), false, nullptr, TColumnSchema::ETypeOfChange::DropNotNullConstraint);
    return true;
}

bool TSqlQuery::AlterTableAlterColumnSetNotNull(const TRule_alter_table_alter_column_set_not_null& node, TAlterTableParameters& params) {
    TString name = Id(node.GetRule_an_id3(), *this);
    const TPosition pos(Context().Pos());
    params.AlterColumns.emplace_back(pos, name, nullptr, false, TVector<TIdentifier>(), false, nullptr, TColumnSchema::ETypeOfChange::SetNotNullConstraint);
    return true;
}

bool TSqlQuery::AlterTableAddChangefeed(const TRule_alter_table_add_changefeed& node, TAlterTableParameters& params) {
    TSqlExpression expr(Ctx, Mode);
    return CreateChangefeed(node.GetRule_changefeed2(), expr, params.AddChangefeeds);
}

bool TSqlQuery::AlterTableAlterChangefeed(const TRule_alter_table_alter_changefeed& node, TAlterTableParameters& params) {
    params.AlterChangefeeds.emplace_back(IdEx(node.GetRule_an_id3(), *this));

    const auto& alter = node.GetRule_changefeed_alter_settings4();
    switch (alter.Alt_case()) {
        case TRule_changefeed_alter_settings::kAltChangefeedAlterSettings1: {
            // DISABLE
            params.AlterChangefeeds.back().Disable = true;
            break;
        }
        case TRule_changefeed_alter_settings::kAltChangefeedAlterSettings2: {
            // SET
            const auto& rule = alter.GetAlt_changefeed_alter_settings2().GetRule_changefeed_settings3();
            TSqlExpression expr(Ctx, Mode);
            if (!ChangefeedSettings(rule, expr, params.AlterChangefeeds.back().Settings, true)) {
                return false;
            }
            break;
        }

        case TRule_changefeed_alter_settings::ALT_NOT_SET:
            AltNotImplemented("changefeed_alter_settings", alter);
            return false;
    }

    return true;
}

void TSqlQuery::AlterTableDropChangefeed(const TRule_alter_table_drop_changefeed& node, TAlterTableParameters& params) {
    params.DropChangefeeds.emplace_back(IdEx(node.GetRule_an_id3(), *this));
}

TNodePtr TSqlQuery::PragmaStatement(const TRule_pragma_stmt& stmt, bool& success) {
    success = false;
    const TString& prefix = OptIdPrefixAsStr(stmt.GetRule_opt_id_prefix_or_type2(), *this);
    const TString& lowerPrefix = to_lower(prefix);
    const TString pragma(Id(stmt.GetRule_an_id3(), *this));
    TString normalizedPragma(pragma);
    TMaybe<TIssue> normalizeError = NormalizeName(Ctx.Pos(), normalizedPragma);
    if (!normalizeError.Empty()) {
        Error() << normalizeError->GetMessage();
        Ctx.IncrementMonCounter("sql_errors", "NormalizePragmaError");
        return {};
    }

    TVector<TDeferredAtom> values;
    TVector<const TRule_pragma_value*> pragmaValues;
    bool pragmaValueDefault = false;
    if (stmt.GetBlock4().HasAlt1()) {
        pragmaValues.push_back(&stmt.GetBlock4().GetAlt1().GetRule_pragma_value2());
    }
    else if (stmt.GetBlock4().HasAlt2()) {
        pragmaValues.push_back(&stmt.GetBlock4().GetAlt2().GetRule_pragma_value2());
        for (auto& additionalValue : stmt.GetBlock4().GetAlt2().GetBlock3()) {
            pragmaValues.push_back(&additionalValue.GetRule_pragma_value2());
        }
    }

    const bool withConfigure = prefix || normalizedPragma == "file" || normalizedPragma == "folder" || normalizedPragma == "udf";
    static const THashSet<TStringBuf> lexicalScopePragmas = {
        "classicdivision",
        "strictjoinkeytypes",
        "disablestrictjoinkeytypes",
        "checkedops",
        "unicodeliterals",
        "disableunicodeliterals",
        "warnuntypedstringliterals",
        "disablewarnuntypedstringliterals",
    };
    const bool hasLexicalScope = withConfigure || lexicalScopePragmas.contains(normalizedPragma);
    const bool withFileAlias = normalizedPragma == "file" || normalizedPragma == "folder" || normalizedPragma == "library" || normalizedPragma == "udf";
    for (auto pragmaValue : pragmaValues) {
        if (pragmaValue->HasAlt_pragma_value3()) {
            auto value = Token(pragmaValue->GetAlt_pragma_value3().GetToken1());
            auto parsed = StringContentOrIdContent(Ctx, Ctx.Pos(), value);
            if (!parsed) {
                return {};
            }

            TString prefix;
            if (withFileAlias && (values.size() == 0)) {
                prefix = Ctx.Settings.FileAliasPrefix;
            }

            values.push_back(TDeferredAtom(Ctx.Pos(), prefix + parsed->Content));
        }
        else if (pragmaValue->HasAlt_pragma_value2()
            && pragmaValue->GetAlt_pragma_value2().GetRule_id1().HasAlt_id2()
            && "default" == to_lower(Id(pragmaValue->GetAlt_pragma_value2().GetRule_id1(), *this)))
        {
            pragmaValueDefault = true;
        }
        else if (withConfigure && pragmaValue->HasAlt_pragma_value5()) {
            TString bindName;
            if (!NamedNodeImpl(pragmaValue->GetAlt_pragma_value5().GetRule_bind_parameter1(), bindName, *this)) {
                return {};
            }
            auto namedNode = GetNamedNode(bindName);
            if (!namedNode) {
                return {};
            }

            TString prefix;
            if (withFileAlias && (values.size() == 0)) {
                prefix = Ctx.Settings.FileAliasPrefix;
            }

            TDeferredAtom atom;
            MakeTableFromExpression(Ctx.Pos(), Ctx, namedNode, atom, prefix);
            values.push_back(atom);
        } else {
            Error() << "Expected string" << (withConfigure ? ", named parameter" : "") << " or 'default' keyword as pragma value for pragma: " << pragma;
            Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
            return {};
        }
    }

    if (prefix.empty()) {
        if (!TopLevel && !hasLexicalScope) {
            Error() << "This pragma '" << pragma << "' is not allowed to be used in actions or subqueries";
            Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
            return{};
        }

        if (normalizedPragma == "refselect") {
            Ctx.PragmaRefSelect = true;
            Ctx.IncrementMonCounter("sql_pragma", "RefSelect");
        } else if (normalizedPragma == "sampleselect") {
            Ctx.PragmaSampleSelect = true;
            Ctx.IncrementMonCounter("sql_pragma", "SampleSelect");
        } else if (normalizedPragma == "allowdotinalias") {
            Ctx.PragmaAllowDotInAlias = true;
            Ctx.IncrementMonCounter("sql_pragma", "AllowDotInAlias");
        } else if (normalizedPragma == "udf") {
            if ((values.size() != 1 && values.size() != 2) || pragmaValueDefault) {
                Error() << "Expected file alias as pragma value";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            if (Ctx.Settings.FileAliasPrefix) {
                if (values.size() == 1) {
                    values.emplace_back(TDeferredAtom(Ctx.Pos(), ""));
                }

                TString prefix;
                if (!values[1].GetLiteral(prefix, Ctx)) {
                    Error() << "Expected literal UDF module prefix in views";
                    Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                    return {};
                }

                values[1] = TDeferredAtom(Ctx.Pos(), Ctx.Settings.FileAliasPrefix + prefix);
            }

            Ctx.IncrementMonCounter("sql_pragma", "udf");
            success = true;
            return BuildPragma(Ctx.Pos(), TString(ConfigProviderName), "ImportUdfs", values, false);
        } else if (normalizedPragma == "packageversion") {
            if (values.size() != 2 || pragmaValueDefault) {
                Error() << "Expected package name and version";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            ui32 version = 0;
            TString versionString;
            TString packageName;
            if (!values[0].GetLiteral(packageName, Ctx) || !values[1].GetLiteral(versionString, Ctx)) {
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            if (!PackageVersionFromString(versionString, version)) {
                Error() << "Unable to parse package version, possible values 0, 1, draft, release";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            Ctx.SetPackageVersion(packageName, version);
            Ctx.IncrementMonCounter("sql_pragma", "PackageVersion");
            success = true;
            return BuildPragma(Ctx.Pos(), TString(ConfigProviderName), "SetPackageVersion", TVector<TDeferredAtom>{ values[0], TDeferredAtom(values[1].Build()->GetPos(), ToString(version)) }, false);
        } else if (normalizedPragma == "file") {
            if (values.size() < 2U || values.size() > 3U || pragmaValueDefault) {
                Error() << "Expected file alias, url and optional token name as pragma values";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            Ctx.IncrementMonCounter("sql_pragma", "file");
            success = true;
            return BuildPragma(Ctx.Pos(), TString(ConfigProviderName), "AddFileByUrl", values, false);
        } else if (normalizedPragma == "fileoption") {
            if (values.size() < 3U) {
                Error() << "Expected file alias, option key and value";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            Ctx.IncrementMonCounter("sql_pragma", "FileOption");
            success = true;
            return BuildPragma(Ctx.Pos(), TString(ConfigProviderName), "SetFileOption", values, false);
        } else if (normalizedPragma == "folder") {
            if (values.size() < 2U || values.size() > 3U || pragmaValueDefault) {
                Error() << "Expected folder alias, url and optional token name as pragma values";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.IncrementMonCounter("sql_pragma", "folder");
            success = true;
            return BuildPragma(Ctx.Pos(), TString(ConfigProviderName), "AddFolderByUrl", values, false);
        } else if (normalizedPragma == "library") {
            if (values.size() < 1) {
                Error() << "Expected non-empty file alias";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return{};
            }
            if (values.size() > 3) {
                Error() << "Expected file alias and optional url and token name as pragma values";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return{};
            }

            TString alias;
            if (!values.front().GetLiteral(alias, Ctx)) {
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return{};
            }

            TContext::TLibraryStuff library;
            std::get<TPosition>(library) = values.front().Build()->GetPos();
            if (values.size() > 1) {
                auto& first = std::get<1U>(library);
                first.emplace();
                first->second = values[1].Build()->GetPos();
                if (!values[1].GetLiteral(first->first, Ctx)) {
                    Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                    return{};
                }

                TSet<TString> names;
                SubstParameters(first->first, Nothing(), &names);
                for (const auto& name : names) {
                    auto namedNode = GetNamedNode(name);
                    if (!namedNode) {
                        return{};
                    }
                }
                if (values.size() > 2) {
                    auto& second = std::get<2U>(library);
                    second.emplace();
                    second->second = values[2].Build()->GetPos();
                    if (!values[2].GetLiteral(second->first, Ctx)) {
                        Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                        return{};
                    }
                }
            }

            Ctx.Libraries[alias] = std::move(library);
            Ctx.IncrementMonCounter("sql_pragma", "library");
        } else if (normalizedPragma == "package") {
            if (values.size() < 2U || values.size() > 3U) {
                Error() << "Expected package name, url and optional token name as pragma values";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            TString packageName;
            if (!values.front().GetLiteral(packageName, Ctx)) {
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            TContext::TPackageStuff package;
            std::get<TPosition>(package) = values.front().Build()->GetPos();

            auto fillLiteral = [&](auto& literal, size_t index) {
                if (values.size() <= index) {
                    return true;
                }

                constexpr bool optional = std::is_base_of_v<
                    std::optional<TContext::TLiteralWithPosition>,
                    std::decay_t<decltype(literal)>
                >;

                TContext::TLiteralWithPosition* literalPtr;

                if constexpr (optional) {
                    literal.emplace();
                    literalPtr = &*literal;
                } else {
                    literalPtr = &literal;
                }

                literalPtr->second = values[index].Build()->GetPos();

                if (!values[index].GetLiteral(literalPtr->first, Ctx)) {
                    Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                    return false;
                }

                return true;
            };

            // fill url
            auto& urlLiteral = std::get<1U>(package);
            if (!fillLiteral(urlLiteral, 1U)) {
                return {};
            }

            TSet<TString> names;
            SubstParameters(urlLiteral.first, Nothing(), &names);
            for (const auto& name : names) {
                auto namedNode = GetNamedNode(name);
                if (!namedNode) {
                    return {};
                }
            }

            // fill token
            if (!fillLiteral(std::get<2U>(package), 2U)) {
                return {};
            }

            Ctx.Packages[packageName] = std::move(package);
            Ctx.IncrementMonCounter("sql_pragma", "package");
        } else if (normalizedPragma == "overridelibrary") {
            if (values.size() != 1U) {
                Error() << "Expected override library alias as pragma value";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            TString alias;
            if (!values.front().GetLiteral(alias, Ctx)) {
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            TContext::TOverrideLibraryStuff overrideLibrary;
            std::get<TPosition>(overrideLibrary) = values.front().Build()->GetPos();

            Ctx.OverrideLibraries[alias] = std::move(overrideLibrary);
            Ctx.IncrementMonCounter("sql_pragma", "overridelibrary");
        } else if (normalizedPragma == "directread") {
            Ctx.PragmaDirectRead = true;
            Ctx.IncrementMonCounter("sql_pragma", "DirectRead");
        } else if (normalizedPragma == "equijoin") {
            Ctx.IncrementMonCounter("sql_pragma", "EquiJoin");
        } else if (normalizedPragma == "autocommit") {
            Ctx.PragmaAutoCommit = true;
            Ctx.IncrementMonCounter("sql_pragma", "AutoCommit");
        } else if (normalizedPragma == "usetableprefixforeach") {
            Ctx.PragmaUseTablePrefixForEach = true;
            Ctx.IncrementMonCounter("sql_pragma", "UseTablePrefixForEach");
        } else if (normalizedPragma == "tablepathprefix") {
            TString value;
            TMaybe<TString> arg;

            if (values.size() == 1 || values.size() == 2) {
                if (!values.front().GetLiteral(value, Ctx)) {
                    Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                    return {};
                }

                if (values.size() == 2) {
                    arg = value;
                    if (!values.back().GetLiteral(value, Ctx)) {
                        Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                        return {};
                    }
                }

                if (!Ctx.SetPathPrefix(value, arg)) {
                    return {};
                }
            } else {
                Error() << "Expected path prefix or tuple of (Provider, PathPrefix) or"
                        << " (Cluster, PathPrefix) as pragma value";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            Ctx.IncrementMonCounter("sql_pragma", "PathPrefix");
        } else if (normalizedPragma == "groupbylimit") {
            if (values.size() != 1 || !values[0].GetLiteral() || !TryFromString(*values[0].GetLiteral(), Ctx.PragmaGroupByLimit)) {
                Error() << "Expected unsigned integer literal as a single argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.IncrementMonCounter("sql_pragma", "GroupByLimit");
        } else if (normalizedPragma == "groupbycubelimit") {
            if (values.size() != 1 || !values[0].GetLiteral() || !TryFromString(*values[0].GetLiteral(), Ctx.PragmaGroupByCubeLimit)) {
                Error() << "Expected unsigned integer literal as a single argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.IncrementMonCounter("sql_pragma", "GroupByCubeLimit");
        } else if (normalizedPragma == "simplecolumns") {
            Ctx.SimpleColumns = true;
            Ctx.IncrementMonCounter("sql_pragma", "SimpleColumns");
        } else if (normalizedPragma == "disablesimplecolumns") {
            Ctx.SimpleColumns = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableSimpleColumns");
        } else if (normalizedPragma == "coalescejoinkeysonqualifiedall") {
            Ctx.CoalesceJoinKeysOnQualifiedAll = true;
            Ctx.IncrementMonCounter("sql_pragma", "CoalesceJoinKeysOnQualifiedAll");
        } else if (normalizedPragma == "disablecoalescejoinkeysonqualifiedall") {
            Ctx.CoalesceJoinKeysOnQualifiedAll = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableCoalesceJoinKeysOnQualifiedAll");
        } else if (normalizedPragma == "resultrowslimit") {
            if (values.size() != 1 || !values[0].GetLiteral() || !TryFromString(*values[0].GetLiteral(), Ctx.ResultRowsLimit)) {
                Error() << "Expected unsigned integer literal as a single argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            Ctx.IncrementMonCounter("sql_pragma", "ResultRowsLimit");
        } else if (normalizedPragma == "resultsizelimit") {
            if (values.size() != 1 || !values[0].GetLiteral() || !TryFromString(*values[0].GetLiteral(), Ctx.ResultSizeLimit)) {
                Error() << "Expected unsigned integer literal as a single argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            Ctx.IncrementMonCounter("sql_pragma", "ResultSizeLimit");
        } else if (normalizedPragma == "warning") {
            if (values.size() != 2U || values.front().Empty() || values.back().Empty()) {
                Error() << "Expected arguments <action>, <issueId> for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            TString action;
            TString codePattern;
            if (!values[0].GetLiteral(action, Ctx) || !values[1].GetLiteral(codePattern, Ctx)) {
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            TWarningRule rule;
            TString parseError;
            auto parseResult = TWarningRule::ParseFrom(codePattern, action, rule, parseError);
            switch (parseResult) {
                case TWarningRule::EParseResult::PARSE_OK:
                    break;
                case TWarningRule::EParseResult::PARSE_PATTERN_FAIL:
                case TWarningRule::EParseResult::PARSE_ACTION_FAIL:
                    Ctx.Error() << parseError;
                    return {};
                default:
                    Y_ENSURE(false, "Unknown parse result");
            }

            Ctx.WarningPolicy.AddRule(rule);
            if (rule.GetPattern() == "*" && rule.GetAction() == EWarningAction::ERROR) {
                // Keep 'unused symbol' warning as warning unless explicitly set to error
                Ctx.SetWarningPolicyFor(TIssuesIds::YQL_UNUSED_SYMBOL, EWarningAction::DEFAULT);
            }

            Ctx.IncrementMonCounter("sql_pragma", "warning");
        } else if (normalizedPragma == "greetings") {
            if (values.size() > 1) {
                Error() << "Multiple arguments are not expected for " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            if (values.empty()) {
                values.emplace_back(TDeferredAtom(Ctx.Pos(), "Hello, world! And best wishes from the YQL Team!"));
            }

            TString arg;
            if (!values.front().GetLiteral(arg, Ctx)) {
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.Info(Ctx.Pos()) << arg;
        } else if (normalizedPragma == "warningmsg") {
            if (values.size() != 1 || !values[0].GetLiteral()) {
                Error() << "Expected string literal as a single argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.Warning(Ctx.Pos(), TIssuesIds::YQL_PRAGMA_WARNING_MSG) << *values[0].GetLiteral();
        } else if (normalizedPragma == "errormsg") {
            if (values.size() != 1 || !values[0].GetLiteral()) {
                Error() << "Expected string literal as a single argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.Error(Ctx.Pos()) << *values[0].GetLiteral();
        } else if (normalizedPragma == "classicdivision") {
            if (values.size() != 1 || !values[0].GetLiteral() || !TryFromString(*values[0].GetLiteral(), Ctx.Scoped->PragmaClassicDivision)) {
                Error() << "Expected boolean literal as a single argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.IncrementMonCounter("sql_pragma", "ClassicDivision");
        } else if (normalizedPragma == "checkedops") {
            if (values.size() != 1 || !values[0].GetLiteral() || !TryFromString(*values[0].GetLiteral(), Ctx.Scoped->PragmaCheckedOps)) {
                Error() << "Expected boolean literal as a single argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.IncrementMonCounter("sql_pragma", "CheckedOps");
        } else if (normalizedPragma == "disableunordered") {
            Ctx.Warning(Ctx.Pos(), TIssuesIds::YQL_DEPRECATED_PRAGMA)
                << "Use of deprecated DisableUnordered pragma. It will be dropped soon";
        } else if (normalizedPragma == "pullupflatmapoverjoin") {
            Ctx.PragmaPullUpFlatMapOverJoin = true;
            Ctx.IncrementMonCounter("sql_pragma", "PullUpFlatMapOverJoin");
        } else if (normalizedPragma == "disablepullupflatmapoverjoin") {
            Ctx.PragmaPullUpFlatMapOverJoin = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisablePullUpFlatMapOverJoin");
        } else if (normalizedPragma == "filterpushdownoverjoinoptionalside") {
            Ctx.FilterPushdownOverJoinOptionalSide = true;
            Ctx.IncrementMonCounter("sql_pragma", "FilterPushdownOverJoinOptionalSide");
        } else if (normalizedPragma == "disablefilterpushdownoverjoinoptionalside") {
            Ctx.FilterPushdownOverJoinOptionalSide = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableFilterPushdownOverJoinOptionalSide");
        } else if (normalizedPragma == "rotatejointree") {
            if (values.size() != 1 || !values[0].GetLiteral() || !TryFromString(*values[0].GetLiteral(), Ctx.RotateJoinTree)) {
                Error() << "Expected boolean literal as a single argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
        } else if (normalizedPragma == "allowunnamedcolumns") {
            Ctx.WarnUnnamedColumns = false;
            Ctx.IncrementMonCounter("sql_pragma", "AllowUnnamedColumns");
        } else if (normalizedPragma == "warnunnamedcolumns") {
            Ctx.WarnUnnamedColumns = true;
            Ctx.IncrementMonCounter("sql_pragma", "WarnUnnamedColumns");
        } else if (normalizedPragma == "discoverymode") {
            Ctx.DiscoveryMode = true;
            Ctx.IncrementMonCounter("sql_pragma", "DiscoveryMode");
        } else if (normalizedPragma == "enablesystemcolumns") {
            if (values.size() != 1 || !values[0].GetLiteral() || !TryFromString(*values[0].GetLiteral(), Ctx.EnableSystemColumns)) {
                Error() << "Expected boolean literal as a single argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.IncrementMonCounter("sql_pragma", "EnableSystemColumns");
        } else if (normalizedPragma == "ansiinforemptyornullableitemscollections") {
            Ctx.AnsiInForEmptyOrNullableItemsCollections = true;
            Ctx.IncrementMonCounter("sql_pragma", "AnsiInForEmptyOrNullableItemsCollections");
        } else if (normalizedPragma == "disableansiinforemptyornullableitemscollections") {
            Ctx.AnsiInForEmptyOrNullableItemsCollections = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableAnsiInForEmptyOrNullableItemsCollections");
        } else if (normalizedPragma == "dqengine" || normalizedPragma == "blockengine") {
            Ctx.IncrementMonCounter("sql_pragma", "DqEngine");
            if (values.size() != 1 || !values[0].GetLiteral()
                || ! (*values[0].GetLiteral() == "disable" || *values[0].GetLiteral() == "auto" || *values[0].GetLiteral() == "force"))
            {
                Error() << "Expected `disable|auto|force' argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            const bool isDqEngine = normalizedPragma == "dqengine";
            auto& enable = isDqEngine ? Ctx.DqEngineEnable : Ctx.BlockEngineEnable;
            auto& force =  isDqEngine ? Ctx.DqEngineForce  : Ctx.BlockEngineForce;
            if (*values[0].GetLiteral() == "disable") {
                enable = false;
                force = false;
            } else if (*values[0].GetLiteral() == "force") {
                enable = true;
                force = true;
            } else if (*values[0].GetLiteral() == "auto") {
                enable = true;
                force = false;
            }
        } else if (normalizedPragma == "ansirankfornullablekeys") {
            Ctx.AnsiRankForNullableKeys = true;
            Ctx.IncrementMonCounter("sql_pragma", "AnsiRankForNullableKeys");
        } else if (normalizedPragma == "disableansirankfornullablekeys") {
            Ctx.AnsiRankForNullableKeys = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableAnsiRankForNullableKeys");
        } else if (normalizedPragma == "ansiorderbylimitinunionall") {
            Ctx.IncrementMonCounter("sql_pragma", "AnsiOrderByLimitInUnionAll");
        } else if (normalizedPragma == "disableansiorderbylimitinunionall") {
            Error() << "DisableAnsiOrderByLimitInUnionAll pragma is deprecated and no longer supported";
            Ctx.IncrementMonCounter("sql_errors", "DeprecatedPragma");
            return {};
        } else if (normalizedPragma == "ansioptionalas") {
            Ctx.AnsiOptionalAs = true;
            Ctx.IncrementMonCounter("sql_pragma", "AnsiOptionalAs");
        } else if (normalizedPragma == "disableansioptionalas") {
            Ctx.AnsiOptionalAs = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableAnsiOptionalAs");
        } else if (normalizedPragma == "warnonansialiasshadowing") {
            Ctx.WarnOnAnsiAliasShadowing = true;
            Ctx.IncrementMonCounter("sql_pragma", "WarnOnAnsiAliasShadowing");
        } else if (normalizedPragma == "disablewarnonansialiasshadowing") {
            Ctx.WarnOnAnsiAliasShadowing = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableWarnOnAnsiAliasShadowing");
        } else if (normalizedPragma == "regexusere2") {
            if (values.size() != 1U || !values.front().GetLiteral() || !TryFromString(*values.front().GetLiteral(), Ctx.PragmaRegexUseRe2)) {
                Error() << "Expected 'true' or 'false' for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.IncrementMonCounter("sql_pragma", "RegexUseRe2");
        } else if (normalizedPragma == "jsonqueryreturnsjsondocument") {
            Ctx.JsonQueryReturnsJsonDocument = true;
            Ctx.IncrementMonCounter("sql_pragma", "JsonQueryReturnsJsonDocument");
        } else if (normalizedPragma == "disablejsonqueryreturnsjsondocument") {
            Ctx.JsonQueryReturnsJsonDocument = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableJsonQueryReturnsJsonDocument");
        } else if (normalizedPragma == "orderedcolumns") {
            Ctx.OrderedColumns = true;
            Ctx.IncrementMonCounter("sql_pragma", "OrderedColumns");
        } else if (normalizedPragma == "disableorderedcolumns") {
            Ctx.OrderedColumns = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableOrderedColumns");
        } else if (normalizedPragma == "positionalunionall") {
            Ctx.PositionalUnionAll = true;
            // PositionalUnionAll implies OrderedColumns
            Ctx.OrderedColumns = true;
            Ctx.IncrementMonCounter("sql_pragma", "PositionalUnionAll");
        } else if (normalizedPragma == "pqreadby") {
            if (values.size() != 1 || !values[0].GetLiteral()) {
                Error() << "Expected string literal as a single argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            // special guard to raise error on situation:
            // use cluster1;
            // pragma PqReadPqBy="cluster2";
            const TString* currentClusterLiteral = Ctx.Scoped->CurrCluster.GetLiteral();
            if (currentClusterLiteral && *values[0].GetLiteral() != "dq" && *currentClusterLiteral != *values[0].GetLiteral()) {
                Error() << "Cluster in PqReadPqBy pragma differs from cluster specified in USE statement: " << *values[0].GetLiteral() << " != " << *currentClusterLiteral;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            Ctx.PqReadByRtmrCluster = *values[0].GetLiteral();
            Ctx.IncrementMonCounter("sql_pragma", "PqReadBy");
        } else if (normalizedPragma == "bogousstaringroupbyoverjoin") {
            Ctx.BogousStarInGroupByOverJoin = true;
            Ctx.IncrementMonCounter("sql_pragma", "BogousStarInGroupByOverJoin");
        } else if (normalizedPragma == "strictjoinkeytypes") {
            Ctx.Scoped->StrictJoinKeyTypes = true;
            Ctx.IncrementMonCounter("sql_pragma", "StrictJoinKeyTypes");
        } else if (normalizedPragma == "disablestrictjoinkeytypes") {
            Ctx.Scoped->StrictJoinKeyTypes = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableStrictJoinKeyTypes");
        } else if (normalizedPragma == "unicodeliterals") {
            Ctx.Scoped->UnicodeLiterals = true;
            Ctx.IncrementMonCounter("sql_pragma", "UnicodeLiterals");
        } else if (normalizedPragma == "disableunicodeliterals") {
            Ctx.Scoped->UnicodeLiterals = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableUnicodeLiterals");
        } else if (normalizedPragma == "warnuntypedstringliterals") {
            Ctx.Scoped->WarnUntypedStringLiterals = true;
            Ctx.IncrementMonCounter("sql_pragma", "WarnUntypedStringLiterals");
        } else if (normalizedPragma == "disablewarnuntypedstringliterals") {
            Ctx.Scoped->WarnUntypedStringLiterals = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableWarnUntypedStringLiterals");
        } else if (normalizedPragma == "unorderedsubqueries") {
            Ctx.UnorderedSubqueries = true;
            Ctx.IncrementMonCounter("sql_pragma", "UnorderedSubqueries");
        } else if (normalizedPragma == "disableunorderedsubqueries") {
            Ctx.UnorderedSubqueries = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableUnorderedSubqueries");
        } else if (normalizedPragma == "datawatermarks") {
            if (values.size() != 1 || !values[0].GetLiteral()
                || ! (*values[0].GetLiteral() == "enable" || *values[0].GetLiteral() == "disable"))
            {
                Error() << "Expected `enable|disable' argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            if (*values[0].GetLiteral() == "enable") {
                Ctx.PragmaDataWatermarks = true;
            } else if (*values[0].GetLiteral() == "disable") {
                Ctx.PragmaDataWatermarks = false;
            }

            Ctx.IncrementMonCounter("sql_pragma", "DataWatermarks");
        } else if (normalizedPragma == "flexibletypes") {
            Ctx.FlexibleTypes = true;
            Ctx.IncrementMonCounter("sql_pragma", "FlexibleTypes");
        } else if (normalizedPragma == "disableflexibletypes") {
            Ctx.FlexibleTypes = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableFlexibleTypes");
        } else if (normalizedPragma == "ansicurrentrow") {
            Ctx.AnsiCurrentRow = true;
            Ctx.IncrementMonCounter("sql_pragma", "AnsiCurrentRow");
        } else if (normalizedPragma == "disableansicurrentrow") {
            Ctx.AnsiCurrentRow = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableAnsiCurrentRow");
        } else if (normalizedPragma == "emitaggapply") {
            Ctx.EmitAggApply = true;
            Ctx.IncrementMonCounter("sql_pragma", "EmitAggApply");
        } else if (normalizedPragma == "disableemitaggapply") {
            Ctx.EmitAggApply = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableEmitAggApply");
        } else if (normalizedPragma == "useblocks") {
            Ctx.UseBlocks = true;
            Ctx.IncrementMonCounter("sql_pragma", "UseBlocks");
        } else if (normalizedPragma == "disableuseblocks") {
            Ctx.UseBlocks = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableUseBlocks");
        } else if (normalizedPragma == "ansilike") {
            Ctx.AnsiLike = true;
            Ctx.IncrementMonCounter("sql_pragma", "AnsiLike");
        } else if (normalizedPragma == "disableansilike") {
            Ctx.AnsiLike = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableAnsiLike");
        } else if (normalizedPragma == "unorderedresult") {
            Ctx.UnorderedResult = true;
            Ctx.IncrementMonCounter("sql_pragma", "UnorderedResult");
        } else if (normalizedPragma == "disableunorderedresult") {
            Ctx.UnorderedResult = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableUnorderedResult");
        } else if (normalizedPragma == "featurer010") {
            if (values.size() == 1 && values[0].GetLiteral()) {
                const auto& value = *values[0].GetLiteral();
                if ("prototype" == value)
                    Ctx.FeatureR010 = true;
                else {
                    Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                    return {};
                }
            }
            else {
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.IncrementMonCounter("sql_pragma", "FeatureR010");
        } else if (normalizedPragma == "compactgroupby") {
            Ctx.CompactGroupBy = true;
            Ctx.IncrementMonCounter("sql_pragma", "CompactGroupBy");
        } else if (normalizedPragma == "disablecompactgroupby") {
            Ctx.CompactGroupBy = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableCompactGroupBy");
        } else if (normalizedPragma == "costbasedoptimizer") {
            Ctx.IncrementMonCounter("sql_pragma", "CostBasedOptimizer");
            if (values.size() == 1 && values[0].GetLiteral()) {
                Ctx.CostBasedOptimizer = to_lower(*values[0].GetLiteral());
            }
            if (values.size() != 1 || !values[0].GetLiteral()
                || ! (Ctx.CostBasedOptimizer == "disable" || Ctx.CostBasedOptimizer == "pg" || Ctx.CostBasedOptimizer == "native"))
            {
                Error() << "Expected `disable|pg|native' argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
        } else if (normalizedPragma == "compactnamedexprs") {
            Ctx.CompactNamedExprs = true;
            Ctx.IncrementMonCounter("sql_pragma", "CompactNamedExprs");
        } else if (normalizedPragma == "disablecompactnamedexprs") {
            Ctx.CompactNamedExprs = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableCompactNamedExprs");
        } else if (normalizedPragma == "validateunusedexprs") {
            Ctx.ValidateUnusedExprs = true;
            Ctx.IncrementMonCounter("sql_pragma", "ValidateUnusedExprs");
        } else if (normalizedPragma == "disablevalidateunusedexprs") {
            Ctx.ValidateUnusedExprs = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableValidateUnusedExprs");
        } else if (normalizedPragma == "ansiimplicitcrossjoin") {
            Ctx.AnsiImplicitCrossJoin = true;
            Ctx.IncrementMonCounter("sql_pragma", "AnsiImplicitCrossJoin");
        } else if (normalizedPragma == "disableansiimplicitcrossjoin") {
            Ctx.AnsiImplicitCrossJoin = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableAnsiImplicitCrossJoin");
        } else {
            Error() << "Unknown pragma: " << pragma;
            Ctx.IncrementMonCounter("sql_errors", "UnknownPragma");
            return {};
        }
    } else {
        if (lowerPrefix == "yson") {
            if (!TopLevel) {
                Error() << "This pragma '" << pragma << "' is not allowed to be used in actions";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            if (normalizedPragma == "fast") {
                Ctx.Warning(Ctx.Pos(), TIssuesIds::YQL_DEPRECATED_PRAGMA)
                    << "Use of deprecated yson.Fast pragma. It will be dropped soon";
                success = true;
                return {};
            } else if (normalizedPragma == "autoconvert") {
                Ctx.PragmaYsonAutoConvert = true;
                success = true;
                return {};
            } else if (normalizedPragma == "strict") {
                if (values.size() == 0U) {
                    Ctx.PragmaYsonStrict = true;
                    success = true;
                } else if (values.size() == 1U && values.front().GetLiteral() && TryFromString(*values.front().GetLiteral(), Ctx.PragmaYsonStrict)) {
                    success = true;
                } else {
                    Error() << "Expected 'true', 'false' or no parameter for: " << pragma;
                    Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                }
                return {};
            } else if (normalizedPragma == "disablestrict") {
                if (values.size() == 0U) {
                    Ctx.PragmaYsonStrict = false;
                    success = true;
                    return {};
                }
                bool pragmaYsonDisableStrict;
                if (values.size() == 1U && values.front().GetLiteral() && TryFromString(*values.front().GetLiteral(), pragmaYsonDisableStrict)) {
                    Ctx.PragmaYsonStrict = !pragmaYsonDisableStrict;
                    success = true;
                } else {
                    Error() << "Expected 'true', 'false' or no parameter for: " << pragma;
                    Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                }
                return {};
            } else if (normalizedPragma == "casttostring" || normalizedPragma == "disablecasttostring") {
                const bool allow = normalizedPragma == "casttostring";
                if (values.size() == 0U) {
                    Ctx.YsonCastToString = allow;
                    success = true;
                    return {};
                }
                bool pragmaYsonCastToString;
                if (values.size() == 1U && values.front().GetLiteral() && TryFromString(*values.front().GetLiteral(), pragmaYsonCastToString)) {
                    Ctx.PragmaYsonStrict = allow ? pragmaYsonCastToString : !pragmaYsonCastToString;
                    success = true;
                } else {
                    Error() << "Expected 'true', 'false' or no parameter for: " << pragma;
                    Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                }
                return {};
            } else {
                Error() << "Unknown pragma: '" << pragma << "'";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

        } else if (std::find(Providers.cbegin(), Providers.cend(), lowerPrefix) == Providers.cend()) {
            if (!Ctx.HasCluster(prefix)) {
                Error() << "Unknown pragma prefix: " << prefix << ", please use cluster name or one of provider " <<
                    JoinRange(", ", Providers.cbegin(), Providers.cend());
                Ctx.IncrementMonCounter("sql_errors", "UnknownPragma");
                return {};
            }
        }

        if (normalizedPragma != "flags" && normalizedPragma != "packageversion") {
            if (values.size() > 1) {
                Error() << "Expected at most one value in the pragma";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
        } else {
            if (pragmaValueDefault || values.size() < 1) {
                Error() << "Expected at least one value in the pragma";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
        }

        success = true;
        Ctx.IncrementMonCounter("sql_pragma", pragma);
        return BuildPragma(Ctx.Pos(), lowerPrefix, normalizedPragma, values, pragmaValueDefault);
    }
    success = true;
    return {};
}

TNodePtr TSqlQuery::Build(const TRule_delete_stmt& stmt) {
    TTableRef table;
    if (!SimpleTableRefImpl(stmt.GetRule_simple_table_ref3(), table)) {
        return nullptr;
    }

    const bool isKikimr = table.Service == KikimrProviderName;
    if (!isKikimr) {
        Ctx.Error(GetPos(stmt.GetToken1())) << "DELETE is unsupported for " << table.Service;
        return nullptr;
    }

    TSourcePtr source = BuildTableSource(Ctx.Pos(), table);

    TNodePtr options = nullptr;
    if (stmt.HasBlock5()) {
        options = ReturningList(stmt.GetBlock5().GetRule_returning_columns_list1());
        options = options->Y(options);
    }

    if (stmt.HasBlock4()) {
        switch (stmt.GetBlock4().Alt_case()) {
            case TRule_delete_stmt_TBlock4::kAlt1: {
                const auto& alt = stmt.GetBlock4().GetAlt1();

                TColumnRefScope scope(Ctx, EColumnRefState::Allow);
                TSqlExpression sqlExpr(Ctx, Mode);
                auto whereExpr = sqlExpr.Build(alt.GetRule_expr2());
                if (!whereExpr) {
                    return nullptr;
                }
                source->AddFilter(Ctx, whereExpr);
                break;
            }

            case TRule_delete_stmt_TBlock4::kAlt2: {
                const auto& alt = stmt.GetBlock4().GetAlt2();

                auto values = TSqlIntoValues(Ctx, Mode).Build(alt.GetRule_into_values_source2(), "DELETE ON");
                if (!values) {
                    return nullptr;
                }

                return BuildWriteColumns(Ctx.Pos(), Ctx.Scoped, table, EWriteColumnMode::DeleteOn, std::move(values), options);
            }

            case TRule_delete_stmt_TBlock4::ALT_NOT_SET:
                return nullptr;
        }
    }

    return BuildDelete(Ctx.Pos(), Ctx.Scoped, table, std::move(source), options);
}

TNodePtr TSqlQuery::Build(const TRule_update_stmt& stmt) {
    TTableRef table;
    if (!SimpleTableRefImpl(stmt.GetRule_simple_table_ref2(), table)) {
        return nullptr;
    }

    const bool isKikimr = table.Service == KikimrProviderName;

    if (!isKikimr) {
        Ctx.Error(GetPos(stmt.GetToken1())) << "UPDATE is unsupported for " << table.Service;
        return nullptr;
    }

    TNodePtr options = nullptr;
    if (stmt.HasBlock4()) {
        options = ReturningList(stmt.GetBlock4().GetRule_returning_columns_list1());
        options = options->Y(options);
    }

    switch (stmt.GetBlock3().Alt_case()) {
        case TRule_update_stmt_TBlock3::kAlt1: {
            const auto& alt = stmt.GetBlock3().GetAlt1();
            TSourcePtr values = Build(alt.GetRule_set_clause_choice2());
            auto source = BuildTableSource(Ctx.Pos(), table);

            if (alt.HasBlock3()) {
                TColumnRefScope scope(Ctx, EColumnRefState::Allow);
                TSqlExpression sqlExpr(Ctx, Mode);
                auto whereExpr = sqlExpr.Build(alt.GetBlock3().GetRule_expr2());
                if (!whereExpr) {
                    return nullptr;
                }
                source->AddFilter(Ctx, whereExpr);
            }

            return BuildUpdateColumns(Ctx.Pos(), Ctx.Scoped, table, std::move(values), std::move(source), options);
        }

        case TRule_update_stmt_TBlock3::kAlt2: {
            const auto& alt = stmt.GetBlock3().GetAlt2();

            auto values = TSqlIntoValues(Ctx, Mode).Build(alt.GetRule_into_values_source2(), "UPDATE ON");
            if (!values) {
                return nullptr;
            }

            return BuildWriteColumns(Ctx.Pos(), Ctx.Scoped, table, EWriteColumnMode::UpdateOn, std::move(values), options);
        }

        case TRule_update_stmt_TBlock3::ALT_NOT_SET:
            return nullptr;
    }
}

TSourcePtr TSqlQuery::Build(const TRule_set_clause_choice& stmt) {
    switch (stmt.Alt_case()) {
        case TRule_set_clause_choice::kAltSetClauseChoice1:
            return Build(stmt.GetAlt_set_clause_choice1().GetRule_set_clause_list1());
        case TRule_set_clause_choice::kAltSetClauseChoice2:
            return Build(stmt.GetAlt_set_clause_choice2().GetRule_multiple_column_assignment1());
        case TRule_set_clause_choice::ALT_NOT_SET:
            AltNotImplemented("set_clause_choice", stmt);
            return nullptr;
    }
}

bool TSqlQuery::FillSetClause(const TRule_set_clause& node, TVector<TString>& targetList, TVector<TNodePtr>& values) {
    targetList.push_back(ColumnNameAsSingleStr(*this, node.GetRule_set_target1().GetRule_column_name1()));
    TColumnRefScope scope(Ctx, EColumnRefState::Allow);
    TSqlExpression sqlExpr(Ctx, Mode);
    if (!Expr(sqlExpr, values, node.GetRule_expr3())) {
        return false;
    }
    return true;
}

TSourcePtr TSqlQuery::Build(const TRule_set_clause_list& stmt) {
    TVector<TString> targetList;
    TVector<TNodePtr> values;
    const TPosition pos(Ctx.Pos());
    if (!FillSetClause(stmt.GetRule_set_clause1(), targetList, values)) {
        return nullptr;
    }
    for (auto& block: stmt.GetBlock2()) {
        if (!FillSetClause(block.GetRule_set_clause2(), targetList, values)) {
            return nullptr;
        }
    }
    Y_DEBUG_ABORT_UNLESS(targetList.size() == values.size());
    return BuildUpdateValues(pos, targetList, values);
}

TSourcePtr TSqlQuery::Build(const TRule_multiple_column_assignment& stmt) {
    TVector<TString> targetList;
    FillTargetList(*this, stmt.GetRule_set_target_list1(), targetList);
    auto simpleValuesNode = stmt.GetRule_simple_values_source4();
    const TPosition pos(Ctx.Pos());
    switch (simpleValuesNode.Alt_case()) {
        case TRule_simple_values_source::kAltSimpleValuesSource1: {
            TVector<TNodePtr> values;
            TSqlExpression sqlExpr(Ctx, Mode);
            if (!ExprList(sqlExpr, values, simpleValuesNode.GetAlt_simple_values_source1().GetRule_expr_list1())) {
                return nullptr;
            }
            return BuildUpdateValues(pos, targetList, values);
        }
        case TRule_simple_values_source::kAltSimpleValuesSource2: {
            TSqlSelect select(Ctx, Mode);
            TPosition selectPos;
            auto source = select.Build(simpleValuesNode.GetAlt_simple_values_source2().GetRule_select_stmt1(), selectPos);
            if (!source) {
                return nullptr;
            }
            return BuildWriteValues(pos, "UPDATE", targetList, std::move(source));
        }
        case TRule_simple_values_source::ALT_NOT_SET:
            Ctx.IncrementMonCounter("sql_errors", "UnknownSimpleValuesSourceAlt");
            AltNotImplemented("simple_values_source", simpleValuesNode);
            return nullptr;
    }
}

TNodePtr TSqlQuery::Build(const TSQLv1ParserAST& ast) {
    if (Mode == NSQLTranslation::ESqlMode::QUERY) {
        // inject externally declared named expressions
        for (auto [name, type] : Ctx.Settings.DeclaredNamedExprs) {
            if (name.empty()) {
                Error() << "Empty names for externally declared expressions are not allowed";
                return nullptr;
            }
            TString varName = "$" + name;
            if (IsAnonymousName(varName)) {
                Error() << "Externally declared name '" << name << "' is anonymous";
                return nullptr;
            }

            auto parsed = ParseType(type, *Ctx.Pool, Ctx.Issues, Ctx.Pos());
            if (!parsed) {
                Error() << "Failed to parse type for externally declared name '" << name << "'";
                return nullptr;
            }

            TNodePtr typeNode = BuildBuiltinFunc(Ctx, Ctx.Pos(), "ParseType", { BuildLiteralRawString(Ctx.Pos(), type) });
            PushNamedAtom(Ctx.Pos(), varName);
            // no duplicates are possible at this stage
            bool isWeak = true;
            Ctx.DeclareVariable(varName, {}, typeNode, isWeak);
            // avoid 'Symbol is not used' warning for externally declared expression
            YQL_ENSURE(GetNamedNode(varName));
        }
    }

    const auto& query = ast.GetRule_sql_query();
    TVector<TNodePtr> blocks;
    Ctx.PushCurrentBlocks(&blocks);
    Y_DEFER {
        Ctx.PopCurrentBlocks();
    };
    if (query.Alt_case() == TRule_sql_query::kAltSqlQuery1) {
        const auto& statements = query.GetAlt_sql_query1().GetRule_sql_stmt_list1();
        if (!Statement(blocks, statements.GetRule_sql_stmt2().GetRule_sql_stmt_core2())) {
            return nullptr;
        }
        for (auto block: statements.GetBlock3()) {
            if (!Statement(blocks, block.GetRule_sql_stmt2().GetRule_sql_stmt_core2())) {
                return nullptr;
            }
        }
    }

    ui32 topLevelSelects = 0;
    bool hasTailOps = false;
    for (auto& block : blocks) {
        if (block->SubqueryAlias()) {
            continue;
        }

        if (block->HasSelectResult()) {
            ++topLevelSelects;
        } else if (topLevelSelects) {
            hasTailOps = true;
        }
    }

    if ((Mode == NSQLTranslation::ESqlMode::SUBQUERY || Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW) && (topLevelSelects != 1 || hasTailOps)) {
        Error() << "Strictly one select/process/reduce statement is expected at the end of "
            << (Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW ? "view" : "subquery");
        return nullptr;
    }

     if (!Ctx.PragmaAutoCommit && Ctx.Settings.EndOfQueryCommit && IsQueryMode(Mode)) {
        AddStatementToBlocks(blocks, BuildCommitClusters(Ctx.Pos()));
    }

    auto result = BuildQuery(Ctx.Pos(), blocks, true, Ctx.Scoped);
    WarnUnusedNodes();
    return result;
}

TNodePtr TSqlQuery::Build(const std::vector<::NSQLv1Generated::TRule_sql_stmt_core>& statements) {
    if (Mode == NSQLTranslation::ESqlMode::QUERY) {
        // inject externally declared named expressions
        for (auto [name, type] : Ctx.Settings.DeclaredNamedExprs) {
            if (name.empty()) {
                Error() << "Empty names for externally declared expressions are not allowed";
                return nullptr;
            }
            TString varName = "$" + name;
            if (IsAnonymousName(varName)) {
                Error() << "Externally declared name '" << name << "' is anonymous";
                return nullptr;
            }

            auto parsed = ParseType(type, *Ctx.Pool, Ctx.Issues, Ctx.Pos());
            if (!parsed) {
                Error() << "Failed to parse type for externally declared name '" << name << "'";
                return nullptr;
            }

            TNodePtr typeNode = BuildBuiltinFunc(Ctx, Ctx.Pos(), "ParseType", { BuildLiteralRawString(Ctx.Pos(), type) });
            PushNamedAtom(Ctx.Pos(), varName);
            // no duplicates are possible at this stage
            bool isWeak = true;
            Ctx.DeclareVariable(varName, {}, typeNode, isWeak);
            // avoid 'Symbol is not used' warning for externally declared expression
            YQL_ENSURE(GetNamedNode(varName));
        }
    }

    TVector<TNodePtr> blocks;
    Ctx.PushCurrentBlocks(&blocks);
    Y_DEFER {
        Ctx.PopCurrentBlocks();
    };
    for (const auto& statement : statements) {
        if (!Statement(blocks, statement)) {
            return nullptr;
        }
    }

    ui32 topLevelSelects = 0;
    bool hasTailOps = false;
    for (auto& block : blocks) {
        if (block->SubqueryAlias()) {
            continue;
        }

        if (block->HasSelectResult()) {
            ++topLevelSelects;
        } else if (topLevelSelects) {
            hasTailOps = true;
        }
    }

    if ((Mode == NSQLTranslation::ESqlMode::SUBQUERY || Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW) && (topLevelSelects != 1 || hasTailOps)) {
        Error() << "Strictly one select/process/reduce statement is expected at the end of "
            << (Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW ? "view" : "subquery");
        return nullptr;
    }

     if (!Ctx.PragmaAutoCommit && Ctx.Settings.EndOfQueryCommit && IsQueryMode(Mode)) {
        AddStatementToBlocks(blocks, BuildCommitClusters(Ctx.Pos()));
    }

    auto result = BuildQuery(Ctx.Pos(), blocks, true, Ctx.Scoped);
    return result;
}
namespace {

    static bool BuildColumnFeatures(std::map<TString, TDeferredAtom>& result, const TRule_column_schema& columnSchema, const NYql::TPosition& pos, TSqlTranslation& translation) {
        const TString columnName(Id(columnSchema.GetRule_an_id_schema1(), translation));
        TString columnType;

        const auto constraints = ColumnConstraints(columnSchema, translation);
        if (!constraints) {
            return false;
        }

        auto& typeBind = columnSchema.GetRule_type_name_or_bind2();
        switch (typeBind.Alt_case()) {
            case TRule_type_name_or_bind::kAltTypeNameOrBind1:
            {
                auto& typeNameOrBind = typeBind.GetAlt_type_name_or_bind1().GetRule_type_name1();
                if (typeNameOrBind.Alt_case() != TRule_type_name::kAltTypeName2) {
                    return false;
                }
                auto& alt = typeNameOrBind.GetAlt_type_name2();
                auto& block = alt.GetBlock1();
                auto& simpleType = block.GetAlt2().GetRule_type_name_simple1();
                columnType = Id(simpleType.GetRule_an_id_pure1(), translation);
                if (columnType.empty()) {
                    return false;
                }
                break;
            }
            case TRule_type_name_or_bind::kAltTypeNameOrBind2:
                return false;
            case TRule_type_name_or_bind::ALT_NOT_SET:
                Y_ABORT("You should change implementation according to grammar changes");
        }

        result["NAME"] = TDeferredAtom(pos, columnName);
        YQL_ENSURE(columnType, "Unknown column type");
        result["TYPE"] = TDeferredAtom(pos, columnType);
        if (!constraints->Nullable) {
            result["NOT_NULL"] = TDeferredAtom(pos, "true");
        }
        return true;
    }
}

bool TSqlQuery::ParseTableStoreFeatures(std::map<TString, TDeferredAtom> & result, const TRule_alter_table_store_action & actions) {
    switch (actions.Alt_case()) {
        case TRule_alter_table_store_action::kAltAlterTableStoreAction1: {
            // ADD COLUMN
            const auto& addRule = actions.GetAlt_alter_table_store_action1().GetRule_alter_table_add_column1();
            if (!BuildColumnFeatures(result, addRule.GetRule_column_schema3(), Ctx.Pos(), *this)) {
                return false;
            }
            result["ACTION"] = TDeferredAtom(Ctx.Pos(), "NEW_COLUMN");
            break;
        }
        case TRule_alter_table_store_action::kAltAlterTableStoreAction2: {
            // DROP COLUMN
            const auto& dropRule = actions.GetAlt_alter_table_store_action2().GetRule_alter_table_drop_column1();
            TString columnName = Id(dropRule.GetRule_an_id3(), *this);
            if (!columnName) {
                return false;
            }
            result["NAME"] = TDeferredAtom(Ctx.Pos(), columnName);
            result["ACTION"] = TDeferredAtom(Ctx.Pos(), "DROP_COLUMN");
            break;
        }
        case TRule_alter_table_store_action::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }
    return true;
}

} // namespace NSQLTranslationV1
