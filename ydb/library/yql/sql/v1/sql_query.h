#pragma once

#include "sql_translation.h"

#include <ydb/library/yql/parser/proto_ast/gen/v1_proto/SQLv1Parser.pb.h>
#include <util/string/split.h>

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

class TSqlQuery: public TSqlTranslation {
public:
    TSqlQuery(TContext& ctx, NSQLTranslation::ESqlMode mode, bool topLevel)
        : TSqlTranslation(ctx, mode)
        , TopLevel(topLevel)
    {
    }

    TNodePtr Build(const TSQLv1ParserAST& ast);

    bool Statement(TVector<TNodePtr>& blocks, const TRule_sql_stmt_core& core);
private:
    bool DeclareStatement(const TRule_declare_stmt& stmt);
    bool ExportStatement(const TRule_export_stmt& stmt);
    bool AlterTableAction(const TRule_alter_table_action& node, TAlterTableParameters& params);
    bool AlterTableAddColumn(const TRule_alter_table_add_column& node, TAlterTableParameters& params);
    bool AlterTableDropColumn(const TRule_alter_table_drop_column& node, TAlterTableParameters& params);
    bool AlterTableAlterColumn(const TRule_alter_table_alter_column& node, TAlterTableParameters& params);
    bool AlterTableAddFamily(const TRule_family_entry& node, TAlterTableParameters& params);
    bool AlterTableAlterFamily(const TRule_alter_table_alter_column_family& node, TAlterTableParameters& params);
    bool AlterTableSetTableSetting(const TRule_alter_table_set_table_setting_uncompat& node, TAlterTableParameters& params);
    bool AlterTableSetTableSetting(const TRule_alter_table_set_table_setting_compat& node, TAlterTableParameters& params);
    bool AlterTableResetTableSetting(const TRule_alter_table_reset_table_setting& node, TAlterTableParameters& params);
    bool AlterTableAddIndex(const TRule_alter_table_add_index& node, TAlterTableParameters& params);
    void AlterTableDropIndex(const TRule_alter_table_drop_index& node, TAlterTableParameters& params);
    void AlterTableRenameTo(const TRule_alter_table_rename_to& node, TAlterTableParameters& params);
    bool AlterTableAddChangefeed(const TRule_alter_table_add_changefeed& node, TAlterTableParameters& params);
    bool AlterTableAlterChangefeed(const TRule_alter_table_alter_changefeed& node, TAlterTableParameters& params);
    void AlterTableDropChangefeed(const TRule_alter_table_drop_changefeed& node, TAlterTableParameters& params);
    void AlterTableRenameIndexTo(const TRule_alter_table_rename_index_to& node, TAlterTableParameters& params);
    TNodePtr PragmaStatement(const TRule_pragma_stmt& stmt, bool& success);
    void AddStatementToBlocks(TVector<TNodePtr>& blocks, TNodePtr node);
    bool ParseTableStoreFeatures(std::map<TString, TDeferredAtom> & result, const TRule_alter_table_store_action & actions);

    TNodePtr Build(const TRule_delete_stmt& stmt);

    TNodePtr Build(const TRule_update_stmt& stmt);
    TSourcePtr Build(const TRule_set_clause_choice& stmt);
    bool FillSetClause(const TRule_set_clause& node, TVector<TString>& targetList, TVector<TNodePtr>& values);
    TSourcePtr Build(const TRule_set_clause_list& stmt);
    TSourcePtr Build(const TRule_multiple_column_assignment& stmt);

    template<class TNode>
    void ParseStatementName(const TNode& node, TString& internalStatementName, TString& humanStatementName) {
        internalStatementName.clear();
        humanStatementName.clear();
        const auto& descr = AltDescription(node);
        TVector<TString> parts;
        const auto pos = descr.find(": ");
        Y_VERIFY_DEBUG(pos != TString::npos);
        Split(TString(descr.begin() + pos + 2, descr.end()), "_", parts);
        Y_VERIFY_DEBUG(parts.size() > 1);
        parts.pop_back();
        for (auto& part: parts) {
            part.to_upper(0, 1);
            internalStatementName += part;
            if (!humanStatementName.empty()) {
                humanStatementName += ' ';
            }
            humanStatementName += to_upper(part);
        }
    }

    const bool TopLevel;
};

} // namespace NSQLTranslationV1
