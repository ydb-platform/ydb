#pragma once
#include "context.h"
#include <ydb/library/yql/parser/proto_ast/gen/v1_proto_split/SQLv1Parser.pb.main.h>
#include <library/cpp/charset/ci_string.h>

namespace NSQLTranslationV1 {

using namespace NYql;
using namespace NSQLv1Generated;

inline TPosition GetPos(const TToken& token) {
    return TPosition(token.GetColumn(), token.GetLine());
}

template <typename TToken>
TIdentifier GetIdentifier(TTranslation& ctx, const TToken& node) {
    auto token = node.GetToken1();
    return TIdentifier(TPosition(token.GetColumn(), token.GetLine()), ctx.Identifier(token));
}

TIdentifier GetKeywordId(TTranslation& ctx, const TRule_keyword& node);

inline TString GetKeyword(TTranslation& ctx, const TRule_keyword& node) {
    return GetKeywordId(ctx, node).Name;
}

template <typename TRule>
inline TString GetKeyword(TTranslation& ctx, const TRule& node) {
    return GetIdentifier(ctx, node).Name;
}

inline TString Id(const TRule_identifier& node, TTranslation& ctx) {
    // identifier: ID_PLAIN | ID_QUOTED;
    return ctx.Identifier(node.GetToken1());
}

TString Id(const TRule_id& node, TTranslation& ctx);

TString Id(const TRule_id_or_type& node, TTranslation& ctx);

TString Id(const TRule_id_as_compat& node, TTranslation& ctx);

TString Id(const TRule_an_id_as_compat& node, TTranslation& ctx);

TString Id(const TRule_id_schema& node, TTranslation& ctx);

TString Id(const TRule_an_id_or_type& node, TTranslation& ctx);

std::pair<bool, TString> Id(const TRule_id_or_at& node, TTranslation& ctx);

TString Id(const TRule_id_table& node, TTranslation& ctx);

TString Id(const TRule_an_id_table& node, TTranslation& ctx);

TString Id(const TRule_id_table_or_type& node, TTranslation& ctx);

TString Id(const TRule_id_expr& node, TTranslation& ctx);

bool IsQuotedId(const TRule_id_expr& node, TTranslation& ctx);

TString Id(const TRule_id_expr_in& node, TTranslation& ctx);

TString Id(const TRule_id_window& node, TTranslation& ctx);

TString Id(const TRule_id_without& node, TTranslation& ctx);

TString Id(const TRule_id_hint& node, TTranslation& ctx);

TString Id(const TRule_an_id& node, TTranslation& ctx);

TString Id(const TRule_an_id_schema& node, TTranslation& ctx);

TString Id(const TRule_an_id_expr& node, TTranslation& ctx);

TString Id(const TRule_an_id_window& node, TTranslation& ctx);

TString Id(const TRule_an_id_without& node, TTranslation& ctx);

TString Id(const TRule_an_id_hint& node, TTranslation& ctx);

TString Id(const TRule_an_id_pure& node, TTranslation& ctx);

template<typename TRule>
inline TIdentifier IdEx(const TRule& node, TTranslation& ctx) {
    const TString name(Id(node, ctx));
    const TPosition pos(ctx.Context().Pos());
    return TIdentifier(pos, name);
}

bool NamedNodeImpl(const TRule_bind_parameter& node, TString& name, TTranslation& ctx);

TString OptIdPrefixAsStr(const TRule_opt_id_prefix& node, TTranslation& ctx, const TString& defaultStr = {});

TString OptIdPrefixAsStr(const TRule_opt_id_prefix_or_type& node, TTranslation& ctx, const TString& defaultStr = {});

void PureColumnListStr(const TRule_pure_column_list& node, TTranslation& ctx, TVector<TString>& outList);

bool NamedNodeImpl(const TRule_opt_bind_parameter& node, TString& name, bool& isOptional, TTranslation& ctx);

TDeferredAtom PureColumnOrNamed(const TRule_pure_column_or_named& node, TTranslation& ctx);

bool PureColumnOrNamedListStr(const TRule_pure_column_or_named_list& node, TTranslation& ctx, TVector<TDeferredAtom>& outList);

std::pair<TString, TViewDescription> TableKeyImpl(const std::pair<bool, TString>& nameWithAt, TViewDescription view, TTranslation& ctx);

std::pair<TString, TViewDescription> TableKeyImpl(const TRule_table_key& node, TTranslation& ctx, bool hasAt);

TMaybe<TColumnConstraints> ColumnConstraints(const TRule_column_schema& node, TTranslation& ctx);

/// \return optional prefix
TString ColumnNameAsStr(TTranslation& ctx, const TRule_column_name& node, TString& id);

TString ColumnNameAsSingleStr(TTranslation& ctx, const TRule_column_name& node);

class TSqlQuery;

struct TSymbolNameWithPos {
    TString Name;
    TPosition Pos;
};

class TSqlTranslation: public TTranslation {
protected:
    TSqlTranslation(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TTranslation(ctx)
        , Mode(mode)
    {
        /// \todo remove NSQLTranslation::ESqlMode params
        YQL_ENSURE(ctx.Settings.Mode == mode);
    }

protected:
    enum class EExpr {
        Regular,
        GroupBy,
        SqlLambdaParams,
    };
    TNodePtr NamedExpr(const TRule_named_expr& node, EExpr exprMode = EExpr::Regular);
    bool NamedExprList(const TRule_named_expr_list& node, TVector<TNodePtr>& exprs, EExpr exprMode = EExpr::Regular);
    bool BindList(const TRule_bind_parameter_list& node, TVector<TSymbolNameWithPos>& bindNames);
    bool ActionOrSubqueryArgs(const TRule_action_or_subquery_args& node, TVector<TSymbolNameWithPos>& bindNames, ui32& optionalArgsCount);
    bool ModulePath(const TRule_module_path& node, TVector<TString>& path);
    bool NamedBindList(const TRule_named_bind_parameter_list& node, TVector<TSymbolNameWithPos>& names,
        TVector<TSymbolNameWithPos>& aliases);
    bool NamedBindParam(const TRule_named_bind_parameter& node, TSymbolNameWithPos& name, TSymbolNameWithPos& alias);
    TNodePtr NamedNode(const TRule_named_nodes_stmt& rule, TVector<TSymbolNameWithPos>& names);

    bool ImportStatement(const TRule_import_stmt& stmt, TVector<TString>* namesPtr = nullptr);
    TNodePtr DoStatement(const TRule_do_stmt& stmt, bool makeLambda, const TVector<TString>& args = {});
    bool DefineActionOrSubqueryStatement(const TRule_define_action_or_subquery_stmt& stmt, TSymbolNameWithPos& nameAndPos, TNodePtr& lambda);
    bool DefineActionOrSubqueryBody(TSqlQuery& query, TBlocks& blocks, const TRule_define_action_or_subquery_body& body);
    TNodePtr IfStatement(const TRule_if_stmt& stmt);
    TNodePtr ForStatement(const TRule_for_stmt& stmt);
    TMaybe<TTableArg> TableArgImpl(const TRule_table_arg& node);
    bool TableRefImpl(const TRule_table_ref& node, TTableRef& result, bool unorderedSubquery);
    TMaybe<TSourcePtr> AsTableImpl(const TRule_table_ref& node);
    bool ClusterExpr(const TRule_cluster_expr& node, bool allowWildcard, TString& service, TDeferredAtom& cluster);
    bool ClusterExprOrBinding(const TRule_cluster_expr& node, TString& service, TDeferredAtom& cluster, bool& isBinding);
    bool ApplyTableBinding(const TString& binding, TTableRef& tr, TTableHints& hints);

    TMaybe<TColumnSchema> ColumnSchemaImpl(const TRule_column_schema& node);
    bool CreateTableEntry(const TRule_create_table_entry& node, TCreateTableParameters& params, const bool isCreateTableAs);

    bool FillFamilySettingsEntry(const TRule_family_settings_entry& settingNode, TFamilyEntry& family);
    bool FillFamilySettings(const TRule_family_settings& settingsNode, TFamilyEntry& family);
    bool CreateTableSettings(const TRule_with_table_settings& settingsNode, TCreateTableParameters& params);
    bool StoreTableSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, TTableSettings& settings,
        ETableType tableType, bool alter, bool reset);
    bool StoreTableSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, TTableSettings& settings,
        bool alter, bool reset);
    bool StoreExternalTableSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, TTableSettings& settings,
        bool alter, bool reset);
    bool StoreTableSettingsEntry(const TIdentifier& id, const TRule_table_setting_value& value, TTableSettings& settings, ETableType tableType, bool alter = false);
    bool StoreDataSourceSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, std::map<TString, TDeferredAtom>& result);
    bool StoreDataSourceSettingsEntry(const TRule_alter_table_setting_entry& entry, std::map<TString, TDeferredAtom>& result);
    bool StoreResourcePoolSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, std::map<TString, TDeferredAtom>& result);
    bool StoreResourcePoolSettingsEntry(const TRule_alter_table_setting_entry& entry, std::map<TString, TDeferredAtom>& result);
    bool ResetTableSettingsEntry(const TIdentifier& id, TTableSettings& settings, ETableType tableType);

    bool CreateTableIndex(const TRule_table_index& node, TVector<TIndexDescription>& indexes);
    bool CreateIndexSettings(const TRule_with_index_settings& settingsNode, TIndexDescription::EType indexType, TIndexDescription::TIndexSettings& indexSettings);
    bool CreateIndexSettingEntry(const TIdentifier& id, const TRule_index_setting_value& value, TIndexDescription::EType indexType, TIndexDescription::TIndexSettings& indexSettings);
    template<typename T>
    std::tuple<bool, T, TString> GetIndexSettingValue(const TRule_index_setting_value& node);

    TIdentifier GetTopicConsumerId(const TRule_topic_consumer_ref& node);
    bool CreateConsumerSettings(const TRule_topic_consumer_settings& settingsNode, TTopicConsumerSettings& settings);
    bool CreateTopicSettings(const TRule_topic_settings& node, TTopicSettings& params);
    bool CreateTopicConsumer(const TRule_topic_create_consumer_entry& node,
                             TVector<TTopicConsumerDescription>& consumers);
    bool CreateTopicEntry(const TRule_create_topic_entry& node, TCreateTopicParameters& params);

    bool AlterTopicConsumer(const TRule_alter_topic_alter_consumer& node,
                            THashMap<TString, TTopicConsumerDescription>& alterConsumers);

    bool AlterTopicConsumerEntry(const TRule_alter_topic_alter_consumer_entry& node,
                                 TTopicConsumerDescription& alterConsumer);


    bool AlterTopicAction(const TRule_alter_topic_action& node, TAlterTopicParameters& params);


    TNodePtr TypeSimple(const TRule_type_name_simple& node, bool onlyDataAllowed);
    TNodePtr TypeDecimal(const TRule_type_name_decimal& node);
    TNodePtr AddOptionals(const TNodePtr& node, size_t optionalCount);
    TMaybe<std::pair<TVector<TNodePtr>, bool>> CallableArgList(const TRule_callable_arg_list& argList, bool namedArgsStarted);

    TNodePtr IntegerOrBind(const TRule_integer_or_bind& node);
    TNodePtr TypeNameTag(const TRule_type_name_tag& node);
    TNodePtr TypeNodeOrBind(const TRule_type_name_or_bind& node);
    TNodePtr SerialTypeNode(const TRule_type_name_or_bind& node);
    TNodePtr TypeNode(const TRule_type_name& node);
    TNodePtr TypeNode(const TRule_type_name_composite& node);
    TNodePtr ValueConstructorLiteral(const TRule_value_constructor_literal& node);
    TNodePtr ValueConstructor(const TRule_value_constructor& node);
    TNodePtr ListLiteral(const TRule_list_literal& node);
    TNodePtr DictLiteral(const TRule_dict_literal& node);
    TNodePtr StructLiteral(const TRule_struct_literal& node);
    TMaybe<TTableHints> TableHintsImpl(const TRule_table_hints& node, const TString& provider, const TString& keyFunc = "");
    bool TableHintImpl(const TRule_table_hint& rule, TTableHints& hints, const TString& provider, const TString& keyFunc = "");
    bool SimpleTableRefImpl(const TRule_simple_table_ref& node, TTableRef& result);
    bool TopicRefImpl(const TRule_topic_ref& node, TTopicRef& result);
    TWindowSpecificationPtr WindowSpecification(const TRule_window_specification_details& rule);
    bool OrderByClause(const TRule_order_by_clause& node, TVector<TSortSpecificationPtr>& orderBy);
    bool SortSpecificationList(const TRule_sort_specification_list& node, TVector<TSortSpecificationPtr>& sortSpecs);

    bool IsDistinctOptSet(const TRule_opt_set_quantifier& node) const;
    bool IsDistinctOptSet(const TRule_opt_set_quantifier& node, TPosition& distinctPos) const;

    bool AddObjectFeature(std::map<TString, TDeferredAtom>& result, const TRule_object_feature& feature);
    bool BindParameterClause(const TRule_bind_parameter& node, TDeferredAtom& result);
    bool ObjectFeatureValueClause(const TRule_object_feature_value& node, TDeferredAtom& result);
    bool ParseObjectFeatures(std::map<TString, TDeferredAtom>& result, const TRule_object_features& features);
    bool ParseExternalDataSourceSettings(std::map<TString, TDeferredAtom>& result, const TRule_with_table_settings& settings);
    bool ParseExternalDataSourceSettings(std::map<TString, TDeferredAtom>& result, std::set<TString>& toReset, const TRule_alter_external_data_source_action& alterActions);
    bool ParseViewOptions(std::map<TString, TDeferredAtom>& features, const TRule_with_table_settings& options);
    bool ParseViewQuery(std::map<TString, TDeferredAtom>& features, const TRule_select_stmt& query);
    bool ParseResourcePoolSettings(std::map<TString, TDeferredAtom>& result, const TRule_with_table_settings& settings);
    bool ParseResourcePoolSettings(std::map<TString, TDeferredAtom>& result, std::set<TString>& toReset, const TRule_alter_resource_pool_action& alterAction);
    bool RoleNameClause(const TRule_role_name& node, TDeferredAtom& result, bool allowSystemRoles);
    bool RoleParameters(const TRule_create_user_option& node, TRoleParameters& result);
    bool PermissionNameClause(const TRule_permission_name_target& node, TVector<TDeferredAtom>& result, bool withGrantOption);
    bool PermissionNameClause(const TRule_permission_name& node, TDeferredAtom& result);
    bool PermissionNameClause(const TRule_permission_id& node, TDeferredAtom& result);
    bool StoreStringSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, std::map<TString, TDeferredAtom>& result);
    bool StoreStringSettingsEntry(const TRule_alter_table_setting_entry& entry, std::map<TString, TDeferredAtom>& result);
    bool ParseBackupCollectionSettings(std::map<TString, TDeferredAtom>& result, const TRule_backup_collection_settings& settings);
    bool ParseBackupCollectionSettings(std::map<TString, TDeferredAtom>& result, std::set<TString>& toReset, const TRule_alter_backup_collection_actions& actions);

    bool ValidateAuthMethod(const std::map<TString, TDeferredAtom>& result);
    bool ValidateExternalTable(const TCreateTableParameters& params);

    TNodePtr ReturningList(const ::NSQLv1Generated::TRule_returning_columns_list& columns);
private:
    bool SimpleTableRefCoreImpl(const TRule_simple_table_ref_core& node, TTableRef& result);
    static bool IsValidFrameSettings(TContext& ctx, const TFrameSpecification& frameSpec, size_t sortSpecSize);
    static TString FrameSettingsToString(EFrameSettings settings, bool isUnbounded);

    bool FrameBound(const TRule_window_frame_bound& rule, TFrameBoundPtr& bound);
    bool FrameClause(const TRule_window_frame_clause& node, TFrameSpecificationPtr& frameSpec, size_t sortSpecSize);
    bool SortSpecification(const TRule_sort_specification& node, TVector<TSortSpecificationPtr>& sortSpecs);

    bool ClusterExpr(const TRule_cluster_expr& node, bool allowWildcard, bool allowBinding, TString& service, TDeferredAtom& cluster, bool& isBinding);
    bool StructLiteralItem(TVector<TNodePtr>& labels, const TRule_expr& label, TVector<TNodePtr>& values, const TRule_expr& value);
protected:
    NSQLTranslation::ESqlMode Mode;
};

TNodePtr LiteralNumber(TContext& ctx, const TRule_integer& node);

template<typename TChar>
struct TPatternComponent {
        TBasicString<TChar> Prefix;
        TBasicString<TChar> Suffix;
        bool IsSimple = true;

        void AppendPlain(TChar c) {
            if (IsSimple) {
                Prefix.push_back(c);
            }
            Suffix.push_back(c);
        }

        void AppendAnyChar() {
            IsSimple = false;
            Suffix.clear();
        }
};

template<typename TChar>
TVector<TPatternComponent<TChar>> SplitPattern(const TBasicString<TChar>& pattern, TMaybe<char> escape, bool& inEscape) {
        inEscape = false;
        TVector<TPatternComponent<TChar>> result;
        TPatternComponent<TChar> current;
        bool prevIsPercentChar = false;
        for (const TChar c : pattern) {
            if (inEscape) {
                current.AppendPlain(c);
                inEscape = false;
                prevIsPercentChar = false;
            } else if (escape && c == static_cast<TChar>(*escape)) {
                inEscape = true;
            } else if (c == '%') {
                if (!prevIsPercentChar) {
                    result.push_back(std::move(current));
                }
                current = {};
                prevIsPercentChar = true;
            } else if (c == '_') {
                current.AppendAnyChar();
                prevIsPercentChar = false;
            } else {
                current.AppendPlain(c);
                prevIsPercentChar = false;
            }
        }
        result.push_back(std::move(current));
        return result;
}

bool ParseNumbers(TContext& ctx, const TString& strOrig, ui64& value, TString& suffix);

} // namespace NSQLTranslationV1
