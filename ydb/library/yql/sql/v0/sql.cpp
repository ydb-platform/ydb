#include "sql.h"

#include "context.h"
#include "node.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/parser/proto_ast/collect_issues/collect_issues.h>
#include <ydb/library/yql/parser/proto_ast/gen/v0/SQLLexer.h>
#include <ydb/library/yql/parser/proto_ast/gen/v0/SQLParser.h>
#include <ydb/library/yql/parser/proto_ast/gen/v0_proto_split/SQLParser.pb.main.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/sql/settings/translation_settings.h>

#include <library/cpp/charset/ci_string.h>

#include <google/protobuf/repeated_field.h>

#include <util/generic/array_ref.h>
#include <util/generic/set.h>
#include <util/string/ascii.h>
#include <util/string/cast.h>
#include <util/string/reverse.h>
#include <util/string/split.h>
#include <util/string/hex.h>
#include <util/string/join.h>

#if defined(_tsan_enabled_)
#include <util/system/mutex.h>
#endif

using namespace NYql;

namespace NSQLTranslationV0 {

using NALP::SQLLexerTokens;

#if defined(_tsan_enabled_)
    TMutex SanitizerSQLTranslationMutex;
#endif

using namespace NSQLGenerated;

static TPosition GetPos(const TToken& token) {
    return TPosition(token.GetColumn(), token.GetLine());
}

template <typename TToken>
TIdentifier GetIdentifier(TTranslation& ctx, const TToken& node) {
    auto token = node.GetToken1();
    return TIdentifier(TPosition(token.GetColumn(), token.GetLine()), ctx.Identifier(token));
}

inline TIdentifier GetKeywordId(TTranslation& ctx, const TRule_keyword_restricted& node) {
    switch (node.Alt_case()) {
        case TRule_keyword_restricted::kAltKeywordRestricted1:
            return GetIdentifier(ctx, node.GetAlt_keyword_restricted1().GetRule_keyword_compat1());
        case TRule_keyword_restricted::kAltKeywordRestricted2:
            return GetIdentifier(ctx, node.GetAlt_keyword_restricted2().GetRule_keyword_expr_uncompat1());
        case TRule_keyword_restricted::kAltKeywordRestricted3:
            return GetIdentifier(ctx, node.GetAlt_keyword_restricted3().GetRule_keyword_select_uncompat1());
        case TRule_keyword_restricted::kAltKeywordRestricted4:
            return GetIdentifier(ctx, node.GetAlt_keyword_restricted4().GetRule_keyword_in_uncompat1());
        default:
            Y_ABORT("You should change implementation according grammar changes");
    }
}

inline TIdentifier GetKeywordId(TTranslation& ctx, const TRule_keyword& node) {
    switch (node.Alt_case()) {
        case TRule_keyword::kAltKeyword1:
            return GetKeywordId(ctx, node.GetAlt_keyword1().GetRule_keyword_restricted1());
        case TRule_keyword::kAltKeyword2:
            return GetIdentifier(ctx, node.GetAlt_keyword2().GetRule_keyword_alter_uncompat1());
        case TRule_keyword::kAltKeyword3:
            return GetIdentifier(ctx, node.GetAlt_keyword3().GetRule_keyword_table_uncompat1());
        default:
            Y_ABORT("You should change implementation according grammar changes");
    }
}

inline TString GetKeyword(TTranslation& ctx, const TRule_keyword& node) {
    return GetKeywordId(ctx, node).Name;
}

inline TString GetKeyword(TTranslation& ctx, const TRule_keyword_restricted& node) {
    return GetKeywordId(ctx, node).Name;
}

static TString Id(const TRule_id& node, TTranslation& ctx) {
    // id: IDENTIFIER | keyword;
    switch (node.Alt_case()) {
        case TRule_id::kAltId1:
            return ctx.Identifier(node.GetAlt_id1().GetToken1());
        case TRule_id::kAltId2:
            return GetKeyword(ctx, node.GetAlt_id2().GetRule_keyword1());
        default:
            Y_ABORT("You should change implementation according grammar changes");
    }
}

static TString Id(const TRule_id_schema& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
        case TRule_id_schema::kAltIdSchema1:
            return ctx.Identifier(node.GetAlt_id_schema1().GetToken1());
        case TRule_id_schema::kAltIdSchema2:
            return GetKeyword(ctx, node.GetAlt_id_schema2().GetRule_keyword_restricted1());
        default:
            Y_ABORT("You should change implementation according grammar changes");
    }
}

static std::pair<bool, TString> Id(const TRule_id_or_at& node, TTranslation& ctx) {
    bool hasAt = node.HasBlock1();
    return std::make_pair(hasAt, Id(node.GetRule_id2(), ctx) );
}

static TString Id(const TRule_id_table& node, TTranslation& ctx) {
    // id_table: IDENTIFIER | keyword_restricted;
    switch (node.Alt_case()) {
    case TRule_id_table::kAltIdTable1:
        return ctx.Identifier(node.GetAlt_id_table1().GetToken1());
    case TRule_id_table::kAltIdTable2:
        return GetKeyword(ctx, node.GetAlt_id_table2().GetRule_keyword_restricted1());
    default:
        Y_ABORT("You should change implementation according grammar changes");
    }
}

static std::pair<bool, TString> Id(const TRule_id_table_or_at& node, TTranslation& ctx) {
    // id_table_or_at: AT? id_table;
    bool hasAt = node.HasBlock1();
    return std::make_pair(hasAt, Id(node.GetRule_id_table2(), ctx));
}

static TString Id(const TRule_id_expr& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
        case TRule_id_expr::kAltIdExpr1:
            return ctx.Identifier(node.GetAlt_id_expr1().GetToken1());
        case TRule_id_expr::kAltIdExpr2:
            return ctx.Token(node.GetAlt_id_expr2().GetRule_keyword_compat1().GetToken1());
        case TRule_id_expr::kAltIdExpr3:
            return ctx.Token(node.GetAlt_id_expr3().GetRule_keyword_alter_uncompat1().GetToken1());
        case TRule_id_expr::kAltIdExpr4:
            return ctx.Token(node.GetAlt_id_expr4().GetRule_keyword_in_uncompat1().GetToken1());
        default:
            Y_ABORT("You should change implementation according grammar changes");
    }
}

static TString Id(const TRule_in_id_expr& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
        case TRule_in_id_expr::kAltInIdExpr1:
            return ctx.Identifier(node.GetAlt_in_id_expr1().GetToken1());
        case TRule_in_id_expr::kAltInIdExpr2:
            return ctx.Token(node.GetAlt_in_id_expr2().GetRule_keyword_compat1().GetToken1());
        case TRule_in_id_expr::kAltInIdExpr3:
            return ctx.Token(node.GetAlt_in_id_expr3().GetRule_keyword_alter_uncompat1().GetToken1());
        default:
            Y_ABORT("You should change implementation according grammar changes");
    }
}

static TIdentifier IdEx(const TRule_id& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
        case TRule_id::kAltId1:
            return GetIdentifier(ctx, node.GetAlt_id1());
        case TRule_id::kAltId2:
            return GetKeywordId(ctx, node.GetAlt_id2().GetRule_keyword1());
        default:
            Y_ABORT("You should change implementation according grammar changes");
    }
}

static TString IdOrString(const TRule_id_or_string& node, TTranslation& ctx, bool rawString = false) {
    switch (node.Alt_case()) {
        case TRule_id_or_string::kAltIdOrString1: {
            return ctx.Identifier(node.GetAlt_id_or_string1().GetToken1());
        }
        case TRule_id_or_string::kAltIdOrString2: {
            auto raw = ctx.Token(node.GetAlt_id_or_string2().GetToken1());
            return rawString ? raw : StringContent(ctx.Context(), raw);
        }
        case TRule_id_or_string::kAltIdOrString3:
            return GetKeyword(ctx, node.GetAlt_id_or_string3().GetRule_keyword1());
        default:
            break;
    }
    Y_ABORT("You should change implementation according grammar changes");
}

static TString IdOrStringAsCluster(const TRule_id_or_string& node, TTranslation& ctx) {
    TString cluster = IdOrString(node, ctx);
    TString normalizedClusterName;
    if (!ctx.Context().GetClusterProvider(cluster, normalizedClusterName)) {
        ctx.Error() << "Unknown cluster: " << cluster;
        return {};
    }
    return normalizedClusterName;
}

static TString OptIdPrefixAsStr(const TRule_opt_id_prefix& node, TTranslation& ctx, const TString& defaultStr = {}) {
    if (!node.HasBlock1()) {
        return defaultStr;
    }
    return IdOrString(node.GetBlock1().GetRule_id_or_string1(), ctx);
}

static TString OptIdPrefixAsClusterStr(const TRule_opt_id_prefix& node, TTranslation& ctx, const TString& defaultStr = {}) {
    if (!node.HasBlock1()) {
        return defaultStr;
    }
    return IdOrStringAsCluster(node.GetBlock1().GetRule_id_or_string1(), ctx);
}

static void PureColumnListStr(const TRule_pure_column_list& node, TTranslation& ctx, TVector<TString>& outList) {
    outList.push_back(IdOrString(node.GetRule_id_or_string2(), ctx));
    for (auto& block: node.GetBlock3()) {
        outList.push_back(IdOrString(block.GetRule_id_or_string2(), ctx));
    }
}

static TString NamedNodeImpl(const TRule_bind_parameter& node, TTranslation& ctx) {
    // bind_parameter: DOLLAR id;
    auto id = Id(node.GetRule_id2(), ctx);
    return ctx.Token(node.GetToken1()) + id;
}

static TDeferredAtom PureColumnOrNamed(const TRule_pure_column_or_named& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
    case TRule_pure_column_or_named::kAltPureColumnOrNamed1: {
        auto named = NamedNodeImpl(node.GetAlt_pure_column_or_named1().GetRule_bind_parameter1(), ctx);
        auto namedNode = ctx.GetNamedNode(named);
        if (!namedNode) {
            return {};
        }

        return TDeferredAtom(namedNode, ctx.Context());
    }

    case TRule_pure_column_or_named::kAltPureColumnOrNamed2:
        return TDeferredAtom(ctx.Context().Pos(), IdOrString(node.GetAlt_pure_column_or_named2().GetRule_id_or_string1(), ctx));
    default:
        Y_ABORT("You should change implementation according grammar changes");
    }
}

static bool PureColumnOrNamedListStr(const TRule_pure_column_or_named_list& node, TTranslation& ctx, TVector<TDeferredAtom>& outList) {
    outList.push_back(PureColumnOrNamed(node.GetRule_pure_column_or_named2(), ctx));
    if (outList.back().Empty()) {
        return false;
    }

    for (auto& block : node.GetBlock3()) {
        outList.push_back(PureColumnOrNamed(block.GetRule_pure_column_or_named2(), ctx));
        if (outList.back().Empty()) {
            return false;
        }
    }

    return true;
}

namespace {

bool IsDistinctOptSet(const TRule_opt_set_quantifier& node) {
    return node.HasBlock1() && node.GetBlock1().GetToken1().GetId() == SQLLexerTokens::TOKEN_DISTINCT;
}

std::pair<TString, bool> FlexType(const TRule_flex_type& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
        case TRule_flex_type::kAltFlexType1:
            return std::make_pair(StringContent(ctx.Context(), ctx.Token(node.GetAlt_flex_type1().GetToken1())), true);
        case TRule_flex_type::kAltFlexType2: {
            const auto& typeName = node.GetAlt_flex_type2().GetRule_type_name1();
            const TString& paramOne = typeName.HasBlock2() ? typeName.GetBlock2().GetRule_integer2().GetToken1().GetValue() : TString();
            const TString& paramTwo = !paramOne.empty() && typeName.GetBlock2().HasBlock3() ? typeName.GetBlock2().GetBlock3().GetRule_integer2().GetToken1().GetValue() : TString();
            TString stringType = Id(typeName.GetRule_id1(), ctx);
            if (!paramOne.empty() && !paramTwo.empty()) {
                TStringStream strm;
                strm << '(' << paramOne << ',' << paramTwo << ')';
                stringType += strm.Str();
            }
            return std::make_pair(stringType, false);
        }
        default:
            Y_ABORT("You should change implementation according to grammar changes");
    }
}

}

static TColumnSchema ColumnSchemaImpl(const TRule_column_schema& node, TTranslation& ctx) {
    const bool nullable = !node.HasBlock3() || !node.GetBlock3().HasBlock1();
    const TString name(Id(node.GetRule_id_schema1(), ctx));
    const TPosition pos(ctx.Context().Pos());
    const auto& type = FlexType(node.GetRule_flex_type2(), ctx);
    return TColumnSchema(pos, name, type.first, nullable, type.second);
}

static bool CreateTableEntry(const TRule_create_table_entry& node, TTranslation& ctx,
    TVector<TColumnSchema>& columns, TVector<TIdentifier>& pkColumns,
    TVector<TIdentifier>& partitionByColumns, TVector<std::pair<TIdentifier, bool>>& orderByColumns)
{
    switch (node.Alt_case()) {
        case TRule_create_table_entry::kAltCreateTableEntry1:
            columns.push_back(ColumnSchemaImpl(node.GetAlt_create_table_entry1().GetRule_column_schema1(), ctx));
            break;

        case TRule_create_table_entry::kAltCreateTableEntry2:
        {
            auto& constraint = node.GetAlt_create_table_entry2().GetRule_table_constraint1();
            switch (constraint.Alt_case()) {
                case TRule_table_constraint::kAltTableConstraint1: {
                    auto& pkConstraint = constraint.GetAlt_table_constraint1();
                    pkColumns.push_back(IdEx(pkConstraint.GetRule_id4(), ctx));
                    for (auto& block : pkConstraint.GetBlock5()) {
                        pkColumns.push_back(IdEx(block.GetRule_id2(), ctx));
                    }
                    break;
                }
                case TRule_table_constraint::kAltTableConstraint2: {
                    auto& pbConstraint = constraint.GetAlt_table_constraint2();
                    partitionByColumns.push_back(IdEx(pbConstraint.GetRule_id4(), ctx));
                    for (auto& block : pbConstraint.GetBlock5()) {
                        partitionByColumns.push_back(IdEx(block.GetRule_id2(), ctx));
                    }
                    break;
                }
                case TRule_table_constraint::kAltTableConstraint3: {
                    auto& obConstraint = constraint.GetAlt_table_constraint3();
                    auto extractDirection = [&ctx] (const TRule_column_order_by_specification& spec, bool& desc) {
                        desc = false;
                        if (!spec.HasBlock2()) {
                            return true;
                        }

                        auto& token = spec.GetBlock2().GetToken1();
                        switch (token.GetId()) {
                            case SQLLexerTokens::TOKEN_ASC:
                                return true;
                            case SQLLexerTokens::TOKEN_DESC:
                                desc = true;
                                return true;
                            default:
                                ctx.Error() << "Unsupported direction token: " << token.GetId();
                                return false;
                        }
                    };

                    bool desc = false;
                    auto& obSpec = obConstraint.GetRule_column_order_by_specification4();
                    if (!extractDirection(obSpec, desc)) {
                        return false;
                    }
                    orderByColumns.push_back(std::make_pair(IdEx(obSpec.GetRule_id1(), ctx), desc));

                    for (auto& block : obConstraint.GetBlock5()) {
                        auto& obSpec = block.GetRule_column_order_by_specification2();
                        if (!extractDirection(obSpec, desc)) {
                            return false;
                        }
                        orderByColumns.push_back(std::make_pair(IdEx(obSpec.GetRule_id1(), ctx), desc));
                    }
                    break;
                }
                default:
                    ctx.AltNotImplemented("table_constraint", constraint);
                    return false;
            }
            break;
        }
        default:
            ctx.AltNotImplemented("create_table_entry", node);
            return false;
    }
    return true;
}

static std::pair<TString, TString> TableKeyImpl(const std::pair<bool, TString>& nameWithAt, TString view, TTranslation& ctx) {
    if (nameWithAt.first) {
        view = "@";
        ctx.Context().IncrementMonCounter("sql_features", "AnonymousTable");
    }

    return std::make_pair(nameWithAt.second, view);
}

static std::pair<TString, TString> TableKeyImpl(const TRule_table_key& node, TTranslation& ctx) {
    auto nameWithAt(Id(node.GetRule_id_table_or_at1(), ctx));
    TString view;
    if (node.HasBlock2()) {
        view = IdOrString(node.GetBlock2().GetRule_id_or_string2(), ctx);
        ctx.Context().IncrementMonCounter("sql_features", "View");
    }

    return TableKeyImpl(nameWithAt, view, ctx);
}

static TVector<TString> TableHintsImpl(const TRule_table_hints& node, TTranslation& ctx) {
    TVector<TString> hints;
    auto& block = node.GetBlock2();
    switch (block.Alt_case()) {
        case TRule_table_hints::TBlock2::kAlt1: {
            hints.push_back(IdOrString(block.GetAlt1().GetRule_id_or_string1(), ctx));
            break;
        }
        case TRule_table_hints::TBlock2::kAlt2: {
            PureColumnListStr(block.GetAlt2().GetRule_pure_column_list1(), ctx, hints);
            break;
        }
        default:
            Y_ABORT("You should change implementation according grammar changes");
    }
    return hints;
}

/// \return optional prefix
static TString ColumnNameAsStr(TTranslation& ctx, const TRule_column_name& node, TString& id) {
    id = IdOrString(node.GetRule_id_or_string2(), ctx);
    return OptIdPrefixAsStr(node.GetRule_opt_id_prefix1(), ctx);
}

static TString ColumnNameAsSingleStr(TTranslation& ctx, const TRule_column_name& node) {
    TString body;
    const TString prefix = ColumnNameAsStr(ctx, node, body);
    return prefix ? prefix + '.' + body : body;
}

static void FillTargetList(TTranslation& ctx, const TRule_set_target_list& node, TVector<TString>& targetList) {
    targetList.push_back(ColumnNameAsSingleStr(ctx, node.GetRule_set_target2().GetRule_column_name1()));
    for (auto& block: node.GetBlock3()) {
        targetList.push_back(ColumnNameAsSingleStr(ctx, block.GetRule_set_target2().GetRule_column_name1()));
    }
}

TVector<TString> GetContextHints(TContext& ctx) {
    TVector<TString> hints;
    if (ctx.PragmaInferSchema) {
        hints.push_back("infer_scheme"); // TODO use yt.InferSchema instead
    }
    if (ctx.PragmaDirectRead) {
        hints.push_back("direct_read");
    }

    return hints;
}

static TVector<TString> GetTableFuncHints(TStringBuf funcName) {
    TCiString func(funcName);
    if (func.StartsWith("range") || func.StartsWith("like") || func.StartsWith("regexp") || func.StartsWith("filter")
        || func.StartsWith("each")) {

        return TVector<TString>{"ignore_non_existing"};
    }

    return {};
}


static TTableRef SimpleTableRefImpl(const TRule_simple_table_ref& node, NSQLTranslation::ESqlMode mode, TTranslation& ctx) {
    TMaybe<TTableRef> tr;
    TString cluster;
    switch (node.GetBlock1().Alt_case()) {
    case TRule_simple_table_ref_TBlock1::AltCase::kAlt1: {
        if (node.GetBlock1().GetAlt1().GetBlock1().HasRule_opt_id_prefix1()) {
            cluster = OptIdPrefixAsClusterStr(node.GetBlock1().GetAlt1().GetBlock1().GetRule_opt_id_prefix1(), ctx, ctx.Context().CurrCluster);
            if (!cluster && ctx.Context().CurrCluster) {
                return TTableRef(ctx.Context().MakeName("table"), ctx.Context().CurrCluster, nullptr);
            }
        }

        tr.ConstructInPlace(ctx.Context().MakeName("table"), cluster.empty() ? ctx.Context().CurrCluster : cluster, nullptr);
        auto tableOrAt = Id(node.GetBlock1().GetAlt1().GetBlock1().GetRule_id_or_at2(), ctx);
        auto tableAndView = TableKeyImpl(tableOrAt, "", ctx);
        tr->Keys = BuildTableKey(ctx.Context().Pos(), tr->Cluster, TDeferredAtom(ctx.Context().Pos(), tableAndView.first), tableAndView.second);
        break;
    }
    case TRule_simple_table_ref_TBlock1::AltCase::kAlt2: {
        auto at = node.GetBlock1().GetAlt2().HasBlock1();
        auto named = ctx.GetNamedNode(NamedNodeImpl(node.GetBlock1().GetAlt2().GetRule_bind_parameter2(), ctx));
        if (!named) {
            return TTableRef(ctx.Context().MakeName("table"), ctx.Context().CurrCluster, nullptr);
        }

        TDeferredAtom table;
        if (!TryMakeClusterAndTableFromExpression(named, cluster, table, ctx.Context())) {
            ctx.Error() << "Cannot infer cluster and table name";
            return TTableRef(ctx.Context().MakeName("table"), ctx.Context().CurrCluster, nullptr);
        }

        tr.ConstructInPlace(ctx.Context().MakeName("table"), cluster.empty() ? ctx.Context().CurrCluster : cluster, nullptr);
        tr->Keys = BuildTableKey(ctx.Context().Pos(), tr->Cluster, table, at ? "@" : "");
        break;
    }
    default:
        Y_ABORT("You should change implementation according grammar changes");
    }

    if (mode == NSQLTranslation::ESqlMode::LIMITED_VIEW && !cluster.empty()) {
        ctx.Error() << "Cluster should not be used in limited view";
        return TTableRef(ctx.Context().MakeName("table"), ctx.Context().CurrCluster, nullptr);
    }

    if (cluster.empty()) {
        cluster = ctx.Context().CurrCluster;
    }

    TVector<TString> hints = GetContextHints(ctx.Context());
    if (node.HasBlock2()) {
        hints = TableHintsImpl(node.GetBlock2().GetRule_table_hints1(), ctx);
    }

    if (!hints.empty()) {
        tr->Options = BuildInputOptions(ctx.Context().Pos(), hints);
    }

    return *tr;
}

static bool ValidateForCounters(const TString& input) {
    for (auto c : input) {
        if (!(IsAlnum(c) || c == '_')) {
            return false;
        }
    }
    return true;
}

static bool IsColumnsOnly(const TVector<TSortSpecificationPtr>& container) {
    for (const auto& elem: container) {
        if (!elem->OrderExpr->GetColumnName()) {
            return false;
        }
    }
    return true;
}

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
    bool BindList(const TRule_bind_parameter_list& node, TVector<TString>& bindNames);
    bool ModulePath(const TRule_module_path& node, TVector<TString>& path);
    bool NamedBindList(const TRule_named_bind_parameter_list& node, TVector<TNodePtr>& bindNames);
    TNodePtr NamedBindParam(const TRule_named_bind_parameter& node);
    TNodePtr NamedNode(const TRule_named_nodes_stmt& rule, TVector<TString>& names);

    bool ImportStatement(const TRule_import_stmt& stmt, TVector<TString>* namesPtr = nullptr);
    TNodePtr DoStatement(const TRule_do_stmt& stmt, bool makeLambda, const TVector<TString>& args = {});
    bool DefineActionOrSubqueryStatement(const TRule_define_action_or_subquery_stmt& stmt);
    TNodePtr EvaluateIfStatement(const TRule_evaluate_if_stmt& stmt);
    TNodePtr EvaluateForStatement(const TRule_evaluate_for_stmt& stmt);
    TMaybe<TTableArg> TableArgImpl(const TRule_table_arg& node);
    TTableRef TableRefImpl(const TRule_table_ref& node);
    TMaybe<TSourcePtr> AsTableImpl(const TRule_table_ref& node);

    NSQLTranslation::ESqlMode Mode;
};

class TSqlExpression: public TSqlTranslation {
public:
    enum class ESmartParenthesis {
        Default,
        GroupBy,
        InStatement,
        SqlLambdaParams,
    };

    TSqlExpression(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TNodePtr Build(const TRule_expr& node) {
        // expr: or_subexpr (OR or_subexpr)*;
        auto getNode = [](const TRule_expr::TBlock2& b) -> const TRule_or_subexpr& { return b.GetRule_or_subexpr2(); };
        return BinOper("Or", node.GetRule_or_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end());
    }

    TNodePtr WrapExprShortcuts(const TNodePtr& node) {
        return node;
        //if (!ExprShortcuts || !node) {
            //return node;
        //}
        //TIntrusivePtr<TAstListNodeImpl> prepareNode = new TAstListNodeImpl(Ctx.Pos());
        //for (const auto& aliasPair: ExprShortcuts) {
            //prepareNode->Add(prepareNode->Y("let", aliasPair.first, aliasPair.second));
        //}
        //prepareNode->Add(prepareNode->Y("return", node));
        //return prepareNode->Y("block", prepareNode->Q(prepareNode));
    }

    void SetSmartParenthesisMode(ESmartParenthesis mode) {
        SmartParenthesisMode = mode;
    }

    TNodePtr ExprShortcut(const TString& baseName, const TNodePtr& node) {
        return BuildShortcutNode(node, baseName);
        //auto alias = Ctx.MakeName(baseName);
        //ExprShortcuts.emplace(alias, node);
        //return BuildAtom(node->GetPos(), alias, TNodeFlags::Default);
    }

    TNodePtr LiteralExpr(const TRule_literal_value& node);
private:
    template<typename TWindowFunctionRule>
    TNodePtr WindowFunctionRule(const TWindowFunctionRule& rule);
    TNodePtr BindParameterRule(const TRule_bind_parameter& rule);
    TNodePtr LambdaRule(const TRule_lambda& rule);
    TNodePtr CastRule(const TRule_cast_expr& rule);
    TNodePtr BitCastRule(const TRule_bitcast_expr& rule);
    TNodePtr ExistsRule(const TRule_exists_expr& rule);
    TNodePtr CaseRule(const TRule_case_expr& rule);

    TNodePtr AtomExpr(const TRule_atom_expr& node);
    TNodePtr InAtomExpr(const TRule_in_atom_expr& node);

    template<typename TUnarySubExprRule>
    TNodePtr UnaryExpr(const TUnarySubExprRule& node);

    bool SqlLambdaParams(const TNodePtr& node, TVector<TString>& args);
    bool SqlLambdaExprBody(TContext& ctx, const TRule_lambda_body& node, TVector<TNodePtr>& exprSeq);

    TNodePtr KeyExpr(const TRule_key_expr& node) {
        TSqlExpression expr(Ctx, Mode);
        return expr.WrapExprShortcuts(expr.Build(node.GetRule_expr2()));
    }

    TNodePtr SubExpr(const TRule_con_subexpr& node);
    TNodePtr SubExpr(const TRule_xor_subexpr& node);

    TNodePtr SubExpr(const TRule_mul_subexpr& node) {
        // mul_subexpr: con_subexpr (DOUBLE_PIPE con_subexpr)*;
        auto getNode = [](const TRule_mul_subexpr::TBlock2& b) -> const TRule_con_subexpr& { return b.GetRule_con_subexpr2(); };
        return BinOper("Concat", node.GetRule_con_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end());
    }

    TNodePtr SubExpr(const TRule_add_subexpr& node) {
        // add_subexpr: mul_subexpr ((ASTERISK | SLASH | PERCENT) mul_subexpr)*;
        auto getNode = [](const TRule_add_subexpr::TBlock2& b) -> const TRule_mul_subexpr& { return b.GetRule_mul_subexpr2(); };
        return BinOpList(node.GetRule_mul_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end());
    }

    TNodePtr SubExpr(const TRule_bit_subexpr& node) {
        // bit_subexpr: add_subexpr ((PLUS | MINUS) add_subexpr)*;
        auto getNode = [](const TRule_bit_subexpr::TBlock2& b) -> const TRule_add_subexpr& { return b.GetRule_add_subexpr2(); };
        return BinOpList(node.GetRule_add_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end());
    }

    TNodePtr SubExpr(const TRule_neq_subexpr& node) {
        // neq_subexpr: bit_subexpr ((SHIFT_LEFT | SHIFT_RIGHT | ROT_LEFT | ROT_RIGHT | AMPERSAND | PIPE | CARET) bit_subexpr)* (DOUBLE_QUESTION neq_subexpr)?;
        auto getNode = [](const TRule_neq_subexpr::TBlock2& b) -> const TRule_bit_subexpr& { return b.GetRule_bit_subexpr2(); };
        auto result = BinOpList(node.GetRule_bit_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end());
        if (!result) {
            return nullptr;
        }
        if (node.HasBlock3()) {
            auto block = node.GetBlock3();
            TSqlExpression altExpr(Ctx, Mode);
            auto altResult = altExpr.WrapExprShortcuts(SubExpr(block.GetRule_neq_subexpr2()));
            if (!altResult) {
                return nullptr;
            }
            const TVector<TNodePtr> args({result, altResult});
            Token(block.GetToken1());
            result = BuildBuiltinFunc(Ctx, Ctx.Pos(), "Coalesce", args);
        }
        return result;
    }

    TNodePtr SubExpr(const TRule_eq_subexpr& node) {
        // eq_subexpr: neq_subexpr ((LESS | LESS_OR_EQ | GREATER | GREATER_OR_EQ) neq_subexpr)*;
        auto getNode = [](const TRule_eq_subexpr::TBlock2& b) -> const TRule_neq_subexpr& { return b.GetRule_neq_subexpr2(); };
        return BinOpList(node.GetRule_neq_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end());
    }

    TNodePtr SubExpr(const TRule_or_subexpr& node) {
        // or_subexpr: and_subexpr (AND and_subexpr)*;
        auto getNode = [](const TRule_or_subexpr::TBlock2& b) -> const TRule_and_subexpr& { return b.GetRule_and_subexpr2(); };
        return BinOper("And", node.GetRule_and_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end());
    }

    TNodePtr SubExpr(const TRule_and_subexpr& node) {
        // and_subexpr: xor_subexpr (XOR xor_subexpr)*;
        auto getNode = [](const TRule_and_subexpr::TBlock2& b) -> const TRule_xor_subexpr& { return b.GetRule_xor_subexpr2(); };
        return BinOper("Xor", node.GetRule_xor_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end());
    }

    template <typename TNode, typename TGetNode, typename TIter>
    TNodePtr BinOpList(const TNode& node, TGetNode getNode, TIter begin, TIter end);

    TNodePtr BinOperList(const TString& opName, TVector<TNodePtr>::const_iterator begin, TVector<TNodePtr>::const_iterator end) const;

    template <typename TNode, typename TGetNode, typename TIter>
    TNodePtr BinOper(const TString& operName, const TNode& node, TGetNode getNode, TIter begin, TIter end);

    TNodePtr SqlInExpr(const TRule_in_expr& node);
    TNodePtr SmartParenthesis(const TRule_smart_parenthesis& node);

    ESmartParenthesis SmartParenthesisMode = ESmartParenthesis::Default;

    THashMap<TString, TNodePtr> ExprShortcuts;
};

class TSqlCallExpr: public TSqlTranslation {
public:
    TSqlCallExpr(TContext& ctx, NSQLTranslation::ESqlMode mode, TSqlExpression* usedExpr = nullptr)
        : TSqlTranslation(ctx, mode)
        , UsedExpr(usedExpr)
    {
    }

    TSqlCallExpr(const TSqlCallExpr& call, const TVector<TNodePtr>& args)
        : TSqlTranslation(call.Ctx, call.Mode)
        , Pos(call.Pos)
        , Func(call.Func)
        , Module(call.Module)
        , Node(call.Node)
        , Args(args)
        , AggMode(call.AggMode)
    {
    }

    template<typename TCallExprRule>
    bool Init(const TCallExprRule& node);
    void IncCounters();

    TNodePtr BuildUdf(bool withArgsType) {
        auto result = Node ? Node : BuildCallable(Pos, Module, Func, withArgsType ? Args : TVector<TNodePtr>());
        if (to_lower(Module) == "tensorflow" && Func == "RunBatch") {
            Args.erase(Args.begin() + 2);
        }
        return result;
    }

    TNodePtr BuildCall() {
        TVector<TNodePtr> args;
        if (Node) {
            Module = "YQL";
            Func = NamedArgs.empty() ? "Apply" : "NamedApply";
            args.push_back(Node);
        }
        bool mustUseNamed = !NamedArgs.empty();
        if (mustUseNamed) {
            if (Node) {
                mustUseNamed = false;
            }
            args.emplace_back(BuildTuple(Pos, PositionalArgs));
            args.emplace_back(BuildStructure(Pos, NamedArgs));
        } else {
            args.insert(args.end(), Args.begin(), Args.end());
        }
        TFuncPrepareNameNode funcPrepareNameNode;
        if (UsedExpr) {
            funcPrepareNameNode = [this](const TString& baseName, const TNodePtr& node) {
                return UsedExpr->ExprShortcut(baseName, node);
            };
        }
        auto result = BuildBuiltinFunc(Ctx, Pos, Func, args, Module, AggMode, &mustUseNamed, funcPrepareNameNode);
        if (mustUseNamed) {
            Error() << "Named args are used for call, but unsupported by function: " << Func;
            return nullptr;
        }
        return result;
    }

    TPosition GetPos() const {
        return Pos;
    }

    const TVector<TNodePtr>& GetArgs() const {
        return Args;
    }

    bool EnsureNotDistinct(const TString& request) const {
        if (AggMode == EAggregateMode::Distinct) {
            Ctx.Error() << request << " does not allow DISTINCT arguments";
            return false;
        }
        return true;
    }

    void SetOverWindow() {
        YQL_ENSURE(AggMode == EAggregateMode::Normal);
        AggMode = EAggregateMode::OverWindow;
    }

    void SetIgnoreNulls() {
        Func += "_IgnoreNulls";
    }

private:
    TPosition Pos;
    TString Func;
    TString Module;
    TNodePtr Node;
    TVector<TNodePtr> Args;
    TVector<TNodePtr> PositionalArgs;
    TVector<TNodePtr> NamedArgs;
    EAggregateMode AggMode = EAggregateMode::Normal;
    TSqlExpression* UsedExpr = nullptr;
};

TNodePtr TSqlTranslation::NamedExpr(const TRule_named_expr& node, EExpr exprMode) {
    TSqlExpression expr(Ctx, Mode);
    if (exprMode == EExpr::GroupBy) {
        expr.SetSmartParenthesisMode(TSqlExpression::ESmartParenthesis::GroupBy);
    } else if (exprMode == EExpr::SqlLambdaParams) {
        expr.SetSmartParenthesisMode(TSqlExpression::ESmartParenthesis::SqlLambdaParams);
    }
    TNodePtr exprNode(expr.WrapExprShortcuts(expr.Build(node.GetRule_expr1())));
    if (!exprNode) {
        Ctx.IncrementMonCounter("sql_errors", "NamedExprInvalid");
        return nullptr;
    }
    if (node.HasBlock2()) {
        exprNode = SafeClone(exprNode);
        exprNode->SetLabel(IdOrString(node.GetBlock2().GetRule_id_or_string2(), *this));
    }
    return exprNode;
}

bool TSqlTranslation::NamedExprList(const TRule_named_expr_list& node, TVector<TNodePtr>& exprs, EExpr exprMode) {
    exprs.emplace_back(NamedExpr(node.GetRule_named_expr1(), exprMode));
    if (!exprs.back()) {
        return false;
    }
    for (auto& b: node.GetBlock2()) {
        exprs.emplace_back(NamedExpr(b.GetRule_named_expr2(), exprMode));
        if (!exprs.back()) {
            return false;
        }
    }
    return true;
}

bool TSqlTranslation::BindList(const TRule_bind_parameter_list& node, TVector<TString>& bindNames) {
    bindNames.emplace_back(NamedNodeImpl(node.GetRule_bind_parameter1(), *this));
    for (auto& b: node.GetBlock2()) {
        bindNames.emplace_back(NamedNodeImpl(b.GetRule_bind_parameter2(), *this));
    }
    return true;
}

bool TSqlTranslation::ModulePath(const TRule_module_path& node, TVector<TString>& path) {
    if (node.HasBlock1()) {
        path.emplace_back(TString());
    }
    path.emplace_back(Id(node.GetRule_id2(), *this));
    for (auto& b: node.GetBlock3()) {
        path.emplace_back(Id(b.GetRule_id2(), *this));
    }
    return true;
}

bool TSqlTranslation::NamedBindList(const TRule_named_bind_parameter_list& node, TVector<TNodePtr>& bindNames) {
    bindNames.emplace_back(NamedBindParam(node.GetRule_named_bind_parameter1()));
    for (auto& b: node.GetBlock2()) {
        bindNames.emplace_back(NamedBindParam(b.GetRule_named_bind_parameter2()));
    }
    return std::find(bindNames.begin(), bindNames.end(), nullptr) == bindNames.end();
}

TNodePtr TSqlTranslation::NamedBindParam(const TRule_named_bind_parameter& node) {
    auto bindName = NamedNodeImpl(node.GetRule_bind_parameter1(), *this);
    auto result = BuildAtom(Ctx.Pos(), bindName, NYql::TNodeFlags::Default);
    if (node.HasBlock2()) {
        result->SetLabel(NamedNodeImpl(node.GetBlock2().GetRule_bind_parameter2(), *this));
    }
    return result;
}

TMaybe<TTableArg> TSqlTranslation::TableArgImpl(const TRule_table_arg& node) {
    TTableArg ret;
    ret.HasAt = node.HasBlock1();
    TSqlExpression expr(Ctx, Mode);
    ret.Expr = expr.Build(node.GetRule_expr2());
    if (!ret.Expr) {
        return Nothing();
    }

    if (node.HasBlock3()) {
        ret.View = IdOrString(node.GetBlock3().GetRule_id_or_string2(), *this);
        Context().IncrementMonCounter("sql_features", "View");
    }

    return ret;
}

TTableRef TSqlTranslation::TableRefImpl(const TRule_table_ref& node) {
    if (Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW && node.GetRule_opt_id_prefix1().HasBlock1()) {
        Ctx.Error() << "Cluster should not be used in limited view";
        return TTableRef(Ctx.MakeName("table"), Ctx.CurrCluster, nullptr);
    }
    auto cluster = OptIdPrefixAsClusterStr(node.GetRule_opt_id_prefix1(), *this, Context().CurrCluster);
    if (!cluster) {
        return TTableRef(Ctx.MakeName("table"), Ctx.CurrCluster, nullptr);
    }

    TTableRef tr(Context().MakeName("table"), cluster, nullptr);
    TPosition pos(Context().Pos());
    TVector<TString> tableHints;
    auto& block = node.GetBlock2();
    switch (block.Alt_case()) {
    case TRule_table_ref::TBlock2::kAlt1: {
        auto pair = TableKeyImpl(block.GetAlt1().GetRule_table_key1(), *this);
        tr.Keys = BuildTableKey(pos, cluster, TDeferredAtom(pos, pair.first), pair.second);
        break;
    }
    case TRule_table_ref::TBlock2::kAlt2: {
        auto& alt = block.GetAlt2();
        const TString func(Id(alt.GetRule_id_expr1(), *this));
        auto arg = TableArgImpl(alt.GetRule_table_arg3());
        if (!arg) {
            return TTableRef(Ctx.MakeName("table"), Ctx.CurrCluster, nullptr);
        }
        TVector<TTableArg> args(1, *arg);
        for (auto& b : alt.GetBlock4()) {
            arg = TableArgImpl(b.GetRule_table_arg2());
            if (!arg) {
                return TTableRef(Ctx.MakeName("table"), Ctx.CurrCluster, nullptr);
            }

            args.push_back(*arg);
        }
        tableHints = GetTableFuncHints(func);
        tr.Keys = BuildTableKeys(pos, cluster, func, args);
        break;
    }
    default:
        Y_ABORT("You should change implementation according grammar changes");
    }
    TVector<TString> hints = GetContextHints(Ctx);
    if (node.HasBlock3()) {
        hints = TableHintsImpl(node.GetBlock3().GetRule_table_hints1(), *this);
    }

    hints.insert(hints.end(), tableHints.begin(), tableHints.end());

    if (!hints.empty()) {
        tr.Options = BuildInputOptions(pos, hints);
    }

    return tr;
}

TMaybe<TSourcePtr> TSqlTranslation::AsTableImpl(const TRule_table_ref& node) {
    const auto& block = node.GetBlock2();

    if (block.Alt_case() == TRule_table_ref::TBlock2::kAlt2) {
        auto& alt = block.GetAlt2();
        TCiString func(Id(alt.GetRule_id_expr1(), *this));

        if (func == "as_table") {
            if (node.GetRule_opt_id_prefix1().HasBlock1()) {
                Ctx.Error() << "Cluster shouldn't be specified for AS_TABLE source";
                return TMaybe<TSourcePtr>(nullptr);
            }

            if (!alt.GetBlock4().empty()) {
                Ctx.Error() << "Expected single argument for AS_TABLE source";
                return TMaybe<TSourcePtr>(nullptr);
            }

            if (node.HasBlock3()) {
                Ctx.Error() << "No hints expected for AS_TABLE source";
                return TMaybe<TSourcePtr>(nullptr);
            }

            auto arg = TableArgImpl(alt.GetRule_table_arg3());
            if (!arg) {
                return TMaybe<TSourcePtr>(nullptr);
            }

            if (arg->Expr->GetSource()) {
                Ctx.Error() << "AS_TABLE shouldn't be used for table sources";
                return TMaybe<TSourcePtr>(nullptr);
            }

            return BuildNodeSource(Ctx.Pos(), arg->Expr);
        }
    }

    return Nothing();
}

bool Expr(TSqlExpression& sqlExpr, TVector<TNodePtr>& exprNodes, const TRule_expr& node) {
    TNodePtr exprNode = sqlExpr.Build(node);
    if (!exprNode) {
        return false;
    }
    exprNodes.push_back(exprNode);
    return true;
}

bool ExprList(TSqlExpression& sqlExpr, TVector<TNodePtr>& exprNodes, const TRule_expr_list& node) {
    if (!Expr(sqlExpr, exprNodes, node.GetRule_expr1())) {
        return false;
    }
    for (auto b: node.GetBlock2()) {
        sqlExpr.Token(b.GetToken1());
        if (!Expr(sqlExpr, exprNodes, b.GetRule_expr2())) {
            return false;
        }
    }
    return true;
}

template<typename TCallExprRule>
bool TSqlCallExpr::Init(const TCallExprRule& node) {
    // call_expr:    ((id_or_string NAMESPACE id_or_string) | id_expr    | bind_parameter) LPAREN (opt_set_quantifier named_expr_list COMMA? | ASTERISK)? RPAREN;
    // OR
    // in_call_expr: ((id_or_string NAMESPACE id_or_string) | in_id_expr | bind_parameter) LPAREN (opt_set_quantifier named_expr_list COMMA? | ASTERISK)? RPAREN;
    const auto& block = node.GetBlock1();
    switch (block.Alt_case()) {
        case TCallExprRule::TBlock1::kAlt1: {
            auto& subblock = block.GetAlt1().GetBlock1();
            Module = IdOrString(subblock.GetRule_id_or_string1(), *this);
            Func = IdOrString(subblock.GetRule_id_or_string3(), *this);
            break;
        }
        case TCallExprRule::TBlock1::kAlt2: {
            if constexpr (std::is_same_v<TCallExprRule, TRule_call_expr>) {
                Func = Id(block.GetAlt2().GetRule_id_expr1(), *this);
            } else {
                Func = Id(block.GetAlt2().GetRule_in_id_expr1(), *this);
            }
            break;
        }
        case TCallExprRule::TBlock1::kAlt3:
            Node = GetNamedNode(NamedNodeImpl(block.GetAlt3().GetRule_bind_parameter1(), *this));
            if (!Node) {
                return false;
            }
            break;
        default:
            Y_ABORT("You should change implementation according grammar changes");
    }
    Pos = Ctx.Pos();
    if (node.HasBlock3()) {
        switch (node.GetBlock3().Alt_case()) {
            case TCallExprRule::TBlock3::kAlt1: {
                const auto& alt = node.GetBlock3().GetAlt1();
                if (IsDistinctOptSet(alt.GetRule_opt_set_quantifier1())) {
                    YQL_ENSURE(AggMode == EAggregateMode::Normal);
                    AggMode = EAggregateMode::Distinct;
                    Ctx.IncrementMonCounter("sql_features", "DistinctInCallExpr");
                }
                if (!NamedExprList(alt.GetRule_named_expr_list2(), Args)) {
                    return false;
                }
                for (const auto& arg: Args) {
                    if (arg->GetLabel()) {
                        NamedArgs.push_back(arg);
                    } else {
                        PositionalArgs.push_back(arg);
                        if (!NamedArgs.empty()) {
                            Ctx.Error(arg->GetPos()) << "Unnamed arguments can not follow after named one";
                            return false;
                        }
                    }
                }
                break;
            }
            case TCallExprRule::TBlock3::kAlt2:
                Args.push_back(BuildColumn(Pos, "*"));
                break;
            default:
                Y_ABORT("You should change implementation according grammar changes");
        }
    }
    return true;
}

void TSqlCallExpr::IncCounters() {
    if (Node) {
        Ctx.IncrementMonCounter("sql_features", "NamedNodeUseApply");
    } else if (!Module.empty()) {
        if (ValidateForCounters(Module)) {
            Ctx.IncrementMonCounter("udf_modules", Module);
            Ctx.IncrementMonCounter("sql_features", "CallUdf");
            if (ValidateForCounters(Func)) {
                auto scriptType = NKikimr::NMiniKQL::ScriptTypeFromStr(Module);
                if (scriptType == NKikimr::NMiniKQL::EScriptType::Unknown) {
                   Ctx.IncrementMonCounter("udf_functions", Module + "." + Func);
                }
            }
        }
    } else if (ValidateForCounters(Func)) {
        Ctx.IncrementMonCounter("sql_builtins", Func);
        Ctx.IncrementMonCounter("sql_features", "CallBuiltin");
    }
}

class TSqlSelect: public TSqlTranslation {
public:
    TSqlSelect(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TSourcePtr Build(const TRule_select_stmt& node, TPosition& selectPos);

private:
    bool SelectTerm(TVector<TNodePtr>& terms, const TRule_result_column& node);
    bool ValidateSelectColumns(const TVector<TNodePtr>& terms);
    bool ColumnName(TVector<TNodePtr>& keys, const TRule_column_name& node);
    bool ColumnList(TVector<TNodePtr>& keys, const TRule_column_list& node);
    bool NamedColumn(TVector<TNodePtr>& columnList, const TRule_named_column& node);
    bool NamedColumnList(TVector<TNodePtr>& columnList, const TRule_named_column_list& node);
    bool SortSpecification(const TRule_sort_specification& node, TVector<TSortSpecificationPtr>& sortSpecs);
    bool SortSpecificationList(const TRule_sort_specification_list& node, TVector<TSortSpecificationPtr>& sortSpecs);
    TSourcePtr SingleSource(const TRule_single_source& node);
    TSourcePtr NamedSingleSource(const TRule_named_single_source& node);
    TVector<TNodePtr> OrdinaryNamedColumnList(const TRule_ordinary_named_column_list& node);
    TSourcePtr FlattenSource(const TRule_flatten_source& node);
    TSourcePtr JoinSource(const TRule_join_source& node);
    bool JoinOp(ISource* join, const TRule_join_source::TBlock2& block);
    TNodePtr JoinExpr(ISource*, const TRule_join_constraint& node);
    TSourcePtr ProcessCore(const TRule_process_core& node, const TWriteSettings& settings, TPosition& selectPos);
    TSourcePtr ReduceCore(const TRule_reduce_core& node, const TWriteSettings& settings, TPosition& selectPos);
    TSourcePtr SelectCore(const TRule_select_core& node, const TWriteSettings& settings, TPosition& selectPos);
    bool FrameStart(const TRule_window_frame_start& rule, TNodePtr& node, bool beginBound);
    bool FrameBound(const TRule_window_frame_bound& rule, TNodePtr& node, bool beginBound);
    bool FrameClause(const TRule_window_frame_clause& node, TMaybe<TFrameSpecification>& winSpecPtr);
    TWindowSpecificationPtr WindowSpecification(const TRule_window_specification_details& rule);
    bool WindowDefenition(const TRule_window_definition& node, TWinSpecs& winSpecs);
    bool WindowClause(const TRule_window_clause& node, TWinSpecs& winSpecs);
    bool OrderByClause(const TRule_order_by_clause& node, TVector<TSortSpecificationPtr>& orderBy);
    TSourcePtr SelectKind(const TRule_select_kind& node, TPosition& selectPos);
    TSourcePtr SelectKind(const TRule_select_kind_partial& node, TPosition& selectPos);
    TSourcePtr SelectKind(const TRule_select_kind_parenthesis& node, TPosition& selectPos);
};

class TGroupByClause: public TSqlTranslation {
    enum class EGroupByFeatures {
        Begin,
        Ordinary = Begin,
        Expression,
        Rollup,
        Cube,
        GroupingSet,
        Empty,
        End,
    };
    typedef TEnumBitSet<EGroupByFeatures, static_cast<int>(EGroupByFeatures::Begin), static_cast<int>(EGroupByFeatures::End)> TGroupingSetFeatures;

    class TGroupByClauseCtx: public TSimpleRefCount<TGroupByClauseCtx> {
    public:
        typedef TIntrusivePtr<TGroupByClauseCtx> TPtr;

        TGroupingSetFeatures GroupFeatures;
        TMap<TString, TNodePtr> NodeAliases;
        size_t UnnamedCount = 0;
    };

public:
    TGroupByClause(TContext& ctx, NSQLTranslation::ESqlMode mode, TGroupByClauseCtx::TPtr groupSetContext = {})
        : TSqlTranslation(ctx, mode)
        , GroupSetContext(groupSetContext ? groupSetContext : TGroupByClauseCtx::TPtr(new TGroupByClauseCtx()))
    {}

    bool Build(const TRule_group_by_clause& node, bool stream);
    bool ParseList(const TRule_grouping_element_list& groupingListNode);

    void SetFeatures(const TString& field) const;
    TVector<TNodePtr>& Content();
    TMap<TString, TNodePtr>& Aliases();
    THoppingWindowSpecPtr GetHoppingWindow();

private:
    TVector<TNodePtr> MultiplyGroupingSets(const TVector<TNodePtr>& lhs, const TVector<TNodePtr>& rhs) const;
    void ResolveGroupByAndGrouping();
    bool GroupingElement(const TRule_grouping_element& node);
    void FeedCollection(const TNodePtr& elem, TVector<TNodePtr>& collection, bool& hasEmpty) const;
    bool OrdinaryGroupingSet(const TRule_ordinary_grouping_set& node);
    bool OrdinaryGroupingSetList(const TRule_ordinary_grouping_set_list& node);
    bool HoppingWindow(const TRule_hopping_window_specification& node);

    bool IsNodeColumnsOrNamedExpression(const TVector<TNodePtr>& content, const TString& construction) const;

    TGroupingSetFeatures& Features();
    const TGroupingSetFeatures& Features() const;
    bool AddAlias(const TString& label, const TNodePtr& node);
    TString GenerateGroupByExprName();
    bool IsAutogenerated(const TString* name) const;

    TVector<TNodePtr> GroupBySet;
    TGroupByClauseCtx::TPtr GroupSetContext;
    THoppingWindowSpecPtr HoppingWindowSpec; // stream queries
    static const TString AutogenerateNamePrefix;
};

const TString TGroupByClause::AutogenerateNamePrefix = "group";

bool ParseNumbers(TContext& ctx, const TString& strOrig, ui64& value, TString& suffix) {
    const auto str = to_lower(strOrig);
    const auto strLen = str.size();
    ui64 base = 10;
    if (strLen > 2 && str[0] == '0') {
        const auto formatChar = str[1];
        if (formatChar == 'x') {
            base = 16;
        } else if (formatChar == 'o') {
            base = 8;
        } else if (formatChar == 'b') {
            base = 2;
        }
    }
    if (strLen > 1) {
        auto iter = str.cend() - 1;
        if (*iter == 'l' || *iter == 's' || *iter == 't' || /* deprecated */ *iter == 'b') {
            --iter;
        }
        if (*iter == 'u') {
            --iter;
        }
        suffix = TString(++iter, str.cend());
    }
    value = 0;
    const TString digString(str.begin() + (base == 10 ? 0 : 2), str.end() - suffix.size());
    for (const char& cur: digString) {
        const ui64 curDigit = Char2DigitTable[static_cast<int>(cur)];
        if (curDigit >= base) {
            ctx.Error(ctx.Pos()) << "Failed to parse number from string: " << strOrig << ", char: '" << cur <<
                "' is out of base: " << base;
            return false;
        }
        const auto curValue = value;
        value *= base;
        value += curDigit;
        if (curValue > value) {
            ctx.Error(ctx.Pos()) << "Failed to parse number from string: " << strOrig << ", number limit overflow";
            return false;
        }
    }
    return true;
}

TNodePtr LiteralNumber(TContext& ctx, const TRule_integer& node) {
    const TString intergerString = ctx.Token(node.GetToken1());
    ui64 value;
    TString suffix;
    if (!ParseNumbers(ctx, intergerString, value, suffix)) {
        return {};
    }

    if (suffix == "ub" || suffix == "b") {
        ctx.Warning(ctx.Pos(), TIssuesIds::YQL_DEPRECATED_TINY_INT_LITERAL_SUFFIX) << "Deprecated suffix 'b' - please use 't' suffix for 8-bit integers";
    }

    const bool noSpaceForInt32 = value >> 31;
    const bool noSpaceForInt64 = value >> 63;
    if (suffix ==  "") {
        if (noSpaceForInt64) {
            return new TLiteralNumberNode<ui64>(ctx.Pos(), "Uint64", ToString(value));
        } else if (noSpaceForInt32) {
            return new TLiteralNumberNode<i64>(ctx.Pos(), "Int64", ToString(value));
        }
        return new TLiteralNumberNode<i32>(ctx.Pos(), "Int32", ToString(value));
    } else if (suffix == "u") {
        return new TLiteralNumberNode<ui32>(ctx.Pos(), "Uint32", ToString(value));
    } else if (suffix == "ul") {
        return new TLiteralNumberNode<ui64>(ctx.Pos(), "Uint64", ToString(value));
    } else if (suffix == "ut" || suffix == "ub") {
        return new TLiteralNumberNode<ui8>(ctx.Pos(), "Uint8", ToString(value));
    } else if (suffix == "t" || suffix == "b") {
        return new TLiteralNumberNode<i8>(ctx.Pos(), "Int8", ToString(value));
    } else if (suffix == "l") {
        return new TLiteralNumberNode<i64>(ctx.Pos(), "Int64", ToString(value));
    } else if (suffix == "us") {
        return new TLiteralNumberNode<ui16>(ctx.Pos(), "Uint16", ToString(value));
    } else if (suffix == "s") {
        return new TLiteralNumberNode<i16>(ctx.Pos(), "Int16", ToString(value));
    } else {
        ctx.Error(ctx.Pos()) << "Failed to parse number from string: " << intergerString << ", invalid suffix: " << suffix;
        return {};
    }
}

TNodePtr LiteralReal(TContext& ctx, const TRule_real& node) {
    const TString value(ctx.Token(node.GetToken1()));
    YQL_ENSURE(!value.empty());
    const auto lastValue = value[value.size() - 1];
    if (lastValue == 'f' || lastValue == 'F') {
        return new TLiteralNumberNode<float>(ctx.Pos(), "Float", value.substr(0, value.size()-1));
    } else {
        return new TLiteralNumberNode<double>(ctx.Pos(), "Double", value);
    }
}

TNodePtr Literal(TContext& ctx, const TRule_unsigned_number& rule) {
    switch (rule.Alt_case()) {
        case TRule_unsigned_number::kAltUnsignedNumber1:
            return LiteralNumber(ctx, rule.GetAlt_unsigned_number1().GetRule_integer1());
        case TRule_unsigned_number::kAltUnsignedNumber2:
            return LiteralReal(ctx, rule.GetAlt_unsigned_number2().GetRule_real1());
        default:
            Y_ABORT("Unsigned number: you should change implementation according grammar changes");
    }
}

TNodePtr TSqlExpression::LiteralExpr(const TRule_literal_value& node) {
    switch (node.Alt_case()) {
        case TRule_literal_value::kAltLiteralValue1: {
            return LiteralNumber(Ctx, node.GetAlt_literal_value1().GetRule_integer1());
        }
        case TRule_literal_value::kAltLiteralValue2: {
            return LiteralReal(Ctx, node.GetAlt_literal_value2().GetRule_real1());
        }
        case TRule_literal_value::kAltLiteralValue3: {
            const TString value(Token(node.GetAlt_literal_value3().GetToken1()));
            return BuildLiteralSmartString(Ctx, value);
        }
        case TRule_literal_value::kAltLiteralValue5: {
            Token(node.GetAlt_literal_value5().GetToken1());
            return BuildLiteralNull(Ctx.Pos());
        }
        case TRule_literal_value::kAltLiteralValue9: {
            const TString value(Token(node.GetAlt_literal_value9().GetRule_bool_value1().GetToken1()));
            return BuildLiteralBool(Ctx.Pos(), value);
        }
        case TRule_literal_value::kAltLiteralValue10: {
            return BuildEmptyAction(Ctx.Pos());
        }
        default:
            AltNotImplemented("literal_value", node);
    }
    return nullptr;
}

template<typename TUnarySubExprType>
TNodePtr TSqlExpression::UnaryExpr(const TUnarySubExprType& node) {
    //unary_subexpr:    (id_expr    | atom_expr   ) key_expr* (DOT (bind_parameter | DIGITS | id_or_string) key_expr*)* (COLLATE id)?;
    // OR
    //in_unary_subexpr: (in_id_expr | in_atom_expr) key_expr* (DOT (bind_parameter | DIGITS | id_or_string) key_expr*)* (COLLATE id)?;
    TVector<INode::TIdPart> ids;
    auto& block = node.GetBlock1();
    switch (block.Alt_case()) {
        case TUnarySubExprType::TBlock1::kAlt1: {
            auto& alt = block.GetAlt1();
            TString name;
            if constexpr (std::is_same_v<TUnarySubExprType, TRule_unary_subexpr>) {
                name = Id(alt.GetRule_id_expr1(), *this);
            } else {
                name = Id(alt.GetRule_in_id_expr1(), *this);
            }
            ids.push_back(BuildColumn(Ctx.Pos()));
            ids.push_back(name);
            break;
        }
        case TUnarySubExprType::TBlock1::kAlt2: {
            TNodePtr expr;
            if constexpr (std::is_same_v<TUnarySubExprType, TRule_unary_subexpr>) {
                expr = AtomExpr(block.GetAlt2().GetRule_atom_expr1());
            } else {
                expr = InAtomExpr(block.GetAlt2().GetRule_in_atom_expr1());
            }

            if (!expr) {
                Ctx.IncrementMonCounter("sql_errors", "BadAtomExpr");
                return nullptr;
            }
            ids.push_back(expr);
            break;
        }
        default:
            Y_ABORT("You should change implementation according grammar changes");
    }
    bool isLookup = false;
    for (auto& b: node.GetBlock2()) {
        auto expr = KeyExpr(b.GetRule_key_expr1());
        if (!expr) {
            Ctx.IncrementMonCounter("sql_errors", "BadKeyExpr");
            return nullptr;
        }
        ids.push_back(expr);
        isLookup = true;
    }
    TPosition pos(Ctx.Pos());
    for (auto& dotBlock: node.GetBlock3()) {
        auto bb = dotBlock.GetBlock2();
        switch (bb.Alt_case()) {
            case TUnarySubExprType::TBlock3::TBlock2::kAlt1: {
                auto named = NamedNodeImpl(bb.GetAlt1().GetRule_bind_parameter1(), *this);
                auto namedNode = GetNamedNode(named);
                if (!namedNode) {
                    return nullptr;
                }

                ids.push_back(named);
                ids.back().Expr = namedNode;
                break;
            }
            case TUnarySubExprType::TBlock3::TBlock2::kAlt2: {
                const TString str(Token(bb.GetAlt2().GetToken1()));
                i32 pos = -1;
                if (!TryFromString<i32>(str, pos)) {
                    Ctx.Error() << "Failed to parse i32 from string: " << str;
                    Ctx.IncrementMonCounter("sql_errors", "FailedToParsePos");
                    return nullptr;
                }
                ids.push_back(pos);
                break;
            }
            case TUnarySubExprType::TBlock3::TBlock2::kAlt3: {
                ids.push_back(IdOrString(bb.GetAlt3().GetRule_id_or_string1(), *this));
                break;
            }
            default:
                Y_ABORT("You should change implementation according grammar changes");
        }
        for (auto& b: dotBlock.GetBlock3()) {
            auto expr = KeyExpr(b.GetRule_key_expr1());
            if (!expr) {
                Ctx.IncrementMonCounter("sql_errors", "BadKeyExpr");
                return nullptr;
            }
            ids.push_back(expr);
            isLookup = true;
        }
    }
    if (node.HasBlock4()) {
        Ctx.IncrementMonCounter("sql_errors", "CollateUnarySubexpr");
        Error() << "unary_subexpr: COLLATE is not implemented yet";
    }
    Y_DEBUG_ABORT_UNLESS(!ids.empty());
    Y_DEBUG_ABORT_UNLESS(ids.front().Expr);
    return ids.size() > 1 ? BuildAccess(pos, ids, isLookup) : ids.back().Expr;
}

TNodePtr TSqlExpression::BindParameterRule(const TRule_bind_parameter& rule) {
    const auto namedArg = NamedNodeImpl(rule, *this);
    if (SmartParenthesisMode == ESmartParenthesis::SqlLambdaParams) {
        Ctx.IncrementMonCounter("sql_features", "LambdaArgument");
        return BuildAtom(Ctx.Pos(), namedArg);
    } else {
        Ctx.IncrementMonCounter("sql_features", "NamedNodeUseAtom");
        return GetNamedNode(namedArg);
    }
}

TNodePtr TSqlExpression::LambdaRule(const TRule_lambda& rule) {
    const auto& alt = rule;
    const bool isSqlLambda = alt.HasBlock2();
    if (!isSqlLambda) {
        return SmartParenthesis(alt.GetRule_smart_parenthesis1());
    }
    TSqlExpression expr(Ctx, Mode);
    expr.SetSmartParenthesisMode(ESmartParenthesis::SqlLambdaParams);
    auto parenthesis = expr.SmartParenthesis(alt.GetRule_smart_parenthesis1());
    if (!parenthesis) {
        return {};
    }
    TVector<TString> args;
    if (!SqlLambdaParams(parenthesis, args)) {
        return {};
    }
    auto bodyBlock = alt.GetBlock2();
    Token(bodyBlock.GetToken1());
    TPosition pos(Ctx.Pos());
    TVector<TNodePtr> exprSeq;
    for (const auto& arg: args) {
        PushNamedNode(arg, BuildAtom(pos, arg, NYql::TNodeFlags::Default));
    }
    const bool ret = SqlLambdaExprBody(Ctx, bodyBlock.GetRule_lambda_body3(), exprSeq);
    for (const auto& arg : args) {
        PopNamedNode(arg);
    }
    if (!ret) {
        return {};
    }
    return BuildSqlLambda(pos, std::move(args), std::move(exprSeq));
}

TNodePtr TSqlExpression::CastRule(const TRule_cast_expr& rule) {
    Ctx.IncrementMonCounter("sql_features", "Cast");
    const auto& alt = rule;
    Token(alt.GetToken1());
    TPosition pos(Ctx.Pos());
    TSqlExpression expr(Ctx, Mode);
    const auto& paramOne = alt.GetRule_type_name5().HasBlock2() ? alt.GetRule_type_name5().GetBlock2().GetRule_integer2().GetToken1().GetValue() : TString();
    const auto& paramTwo = !paramOne.empty() && alt.GetRule_type_name5().GetBlock2().HasBlock3() ? alt.GetRule_type_name5().GetBlock2().GetBlock3().GetRule_integer2().GetToken1().GetValue() : TString();
    return BuildCast(Ctx, pos, expr.Build(alt.GetRule_expr3()), Id(alt.GetRule_type_name5().GetRule_id1(), *this), paramOne, paramTwo);
}

TNodePtr TSqlExpression::BitCastRule(const TRule_bitcast_expr& rule) {
    Ctx.IncrementMonCounter("sql_features", "BitCast");
    const auto& alt = rule;
    Token(alt.GetToken1());
    TPosition pos(Ctx.Pos());
    TSqlExpression expr(Ctx, Mode);
    const auto& paramOne = alt.GetRule_type_name5().HasBlock2() ? alt.GetRule_type_name5().GetBlock2().GetRule_integer2().GetToken1().GetValue() : TString();
    const auto& paramTwo = !paramOne.empty() && alt.GetRule_type_name5().GetBlock2().HasBlock3() ? alt.GetRule_type_name5().GetBlock2().GetBlock3().GetRule_integer2().GetToken1().GetValue() : TString();
    return BuildBitCast(Ctx, pos, expr.Build(alt.GetRule_expr3()), Id(alt.GetRule_type_name5().GetRule_id1(), *this), paramOne, paramTwo);
}

TNodePtr TSqlExpression::ExistsRule(const TRule_exists_expr& rule) {
    Ctx.IncrementMonCounter("sql_features", "Exists");
    const auto& alt = rule;

    Token(alt.GetToken2());
    TSqlSelect select(Ctx, Mode);
    TPosition pos;
    auto source = select.Build(alt.GetRule_select_stmt3(), pos);
    if (!source) {
        Ctx.IncrementMonCounter("sql_errors", "BadSource");
        return nullptr;
    }
    const bool checkExist = true;
    return BuildBuiltinFunc(Ctx, Ctx.Pos(), "ListHasItems", {BuildSourceNode(pos, std::move(source), checkExist)});
}

TNodePtr TSqlExpression::CaseRule(const TRule_case_expr& rule) {
    Ctx.IncrementMonCounter("sql_features", "Case");
    const auto& alt = rule;
    Token(alt.GetToken1());
    TNodePtr elseExpr;
    if (alt.HasBlock4()) {
        Token(alt.GetBlock4().GetToken1());
        TSqlExpression expr(Ctx, Mode);
        elseExpr = expr.Build(alt.GetBlock4().GetRule_expr2());
    } else {
        Ctx.IncrementMonCounter("sql_errors", "ElseIsRequired");
        Error() << "ELSE is required";
        return nullptr;
    }
    TVector<TNodePtr> args;
    for (int i = alt.Block3Size() - 1; i >= 0; --i) {
        const auto& block = alt.GetBlock3(i).GetRule_when_expr1();
        args.clear();
        Token(block.GetToken1());
        TSqlExpression condExpr(Ctx, Mode);
        args.push_back(condExpr.Build(block.GetRule_expr2()));
        if (alt.HasBlock2()) {
            TSqlExpression expr(Ctx, Mode);
            args.back() = BuildBinaryOp(Ctx.Pos(), "==", expr.Build(alt.GetBlock2().GetRule_expr1()), args.back());
        }
        Token(block.GetToken3());
        TSqlExpression thenExpr(Ctx, Mode);
        args.push_back(thenExpr.Build(block.GetRule_expr4()));
        args.push_back(elseExpr);
        if (i > 0) {
            elseExpr = BuildBuiltinFunc(Ctx, Ctx.Pos(), "If", args);
        }
    }
    return BuildBuiltinFunc(Ctx, Ctx.Pos(), "If", args);
}

template<typename TWindowFunctionType>
TNodePtr TSqlExpression::WindowFunctionRule(const TWindowFunctionType& rule) {
    // window_function: call_expr (null_treatment? OVER window_name_or_specification)?;
    // OR
    // in_window_function: in_call_expr (null_treatment? OVER window_name_or_specification)?;
    const bool overWindow = rule.HasBlock2();
    TSqlCallExpr call(Ctx, Mode, this);

    bool initResult;
    if constexpr (std::is_same_v<TWindowFunctionType, TRule_window_function>) {
        initResult = call.Init(rule.GetRule_call_expr1());
    } else {
        initResult = call.Init(rule.GetRule_in_call_expr1());
    }
    if (!initResult) {
        return {};
    }

    call.IncCounters();
    if (!overWindow) {
        return call.BuildCall();
    }
    auto funcSpec = rule.GetBlock2();
    call.SetOverWindow();
    auto winRule = funcSpec.GetRule_window_name_or_specification3();
    if (winRule.Alt_case() != TRule_window_name_or_specification::kAltWindowNameOrSpecification1) {
        Error() << "Inline window specification is not supported yet! You can define window function in WINDOW clause.";
        return {};
    }
    if (funcSpec.HasBlock1() && funcSpec.GetBlock1().GetRule_null_treatment1().Alt_case() == TRule_null_treatment::kAltNullTreatment2) {
        call.SetIgnoreNulls();
    }
    const TString windowName = Id(winRule.GetAlt_window_name_or_specification1().GetRule_window_name1().GetRule_id1(), *this);
    Ctx.IncrementMonCounter("sql_features", "WindowFunctionOver");
    return BuildCalcOverWindow(Ctx.Pos(), windowName, call.BuildCall());
}

TNodePtr TSqlExpression::AtomExpr(const TRule_atom_expr& node) {
    // atom_expr:
    //     literal_value
    //   | bind_parameter
    //   | window_function
    //   | lambda
    //   | cast_expr
    //   | exists_expr
    //   | case_expr
    //   | id_or_string NAMESPACE id_or_string
    // ;

    switch (node.Alt_case()) {
        case TRule_atom_expr::kAltAtomExpr1:
            Ctx.IncrementMonCounter("sql_features", "LiteralExpr");
            return LiteralExpr(node.GetAlt_atom_expr1().GetRule_literal_value1());
        case TRule_atom_expr::kAltAtomExpr2:
            return BindParameterRule(node.GetAlt_atom_expr2().GetRule_bind_parameter1());
        case TRule_atom_expr::kAltAtomExpr3:
            return WindowFunctionRule(node.GetAlt_atom_expr3().GetRule_window_function1());
        case TRule_atom_expr::kAltAtomExpr4:
            return LambdaRule(node.GetAlt_atom_expr4().GetRule_lambda1());
        case TRule_atom_expr::kAltAtomExpr5:
            return CastRule(node.GetAlt_atom_expr5().GetRule_cast_expr1());
        case TRule_atom_expr::kAltAtomExpr6:
            return ExistsRule(node.GetAlt_atom_expr6().GetRule_exists_expr1());
        case TRule_atom_expr::kAltAtomExpr7:
            return CaseRule(node.GetAlt_atom_expr7().GetRule_case_expr1());
        case TRule_atom_expr::kAltAtomExpr8: {
            const auto& alt = node.GetAlt_atom_expr8();
            const TString module(IdOrString(alt.GetRule_id_or_string1(), *this));
            TPosition pos(Ctx.Pos());
            bool rawString = true;
            const TString name(IdOrString(alt.GetRule_id_or_string3(), *this, rawString));
            return BuildCallable(pos, module, name, {});
        }
        case TRule_atom_expr::kAltAtomExpr9:
            return BitCastRule(node.GetAlt_atom_expr9().GetRule_bitcast_expr1());
        default:
            AltNotImplemented("atom_expr", node);
    }
    return nullptr;
}

TNodePtr TSqlExpression::InAtomExpr(const TRule_in_atom_expr& node) {
    // in_atom_expr:
    //     literal_value
    //   | bind_parameter
    //   | in_window_function
    //   | smart_parenthesis
    //   | cast_expr
    //   | case_expr
    //   | LPAREN select_stmt RPAREN
    // ;

    switch (node.Alt_case()) {
        case TRule_in_atom_expr::kAltInAtomExpr1:
            Ctx.IncrementMonCounter("sql_features", "LiteralExpr");
            return LiteralExpr(node.GetAlt_in_atom_expr1().GetRule_literal_value1());
        case TRule_in_atom_expr::kAltInAtomExpr2:
            return BindParameterRule(node.GetAlt_in_atom_expr2().GetRule_bind_parameter1());
        case TRule_in_atom_expr::kAltInAtomExpr3:
            return WindowFunctionRule(node.GetAlt_in_atom_expr3().GetRule_in_window_function1());
        case TRule_in_atom_expr::kAltInAtomExpr4:
            return SmartParenthesis(node.GetAlt_in_atom_expr4().GetRule_smart_parenthesis1());
        case TRule_in_atom_expr::kAltInAtomExpr5:
            return CastRule(node.GetAlt_in_atom_expr5().GetRule_cast_expr1());
        case TRule_in_atom_expr::kAltInAtomExpr6:
            return CaseRule(node.GetAlt_in_atom_expr6().GetRule_case_expr1());
        case TRule_in_atom_expr::kAltInAtomExpr7: {
            Token(node.GetAlt_in_atom_expr7().GetToken1());
            TSqlSelect select(Ctx, Mode);
            TPosition pos;
            auto source = select.Build(node.GetAlt_in_atom_expr7().GetRule_select_stmt2(), pos);
            if (!source) {
                Ctx.IncrementMonCounter("sql_errors", "BadSource");
                return {};
            }
            Ctx.IncrementMonCounter("sql_features", "InSubquery");
            return BuildSelectResult(pos, std::move(source), false, Mode == NSQLTranslation::ESqlMode::SUBQUERY);
        }
        case TRule_in_atom_expr::kAltInAtomExpr8:
            return BitCastRule(node.GetAlt_in_atom_expr8().GetRule_bitcast_expr1());
        default:
            AltNotImplemented("in_atom_expr", node);
    }
    return nullptr;
}

bool TSqlExpression::SqlLambdaParams(const TNodePtr& node, TVector<TString>& args) {
    auto errMsg = TStringBuf("Invalid lambda arguments syntax. Lambda arguments should starts with '$' as named value.");
    auto tupleNodePtr = dynamic_cast<TTupleNode*>(node.Get());
    if (!tupleNodePtr) {
        Ctx.Error(node->GetPos()) << errMsg;
        return false;
    }
    THashSet<TString> dupArgsChecker;
    for (const auto& argPtr: tupleNodePtr->Elements()) {
        auto contentPtr = argPtr->GetAtomContent();
        if (!contentPtr || !contentPtr->StartsWith("$")) {
            Ctx.Error(argPtr->GetPos()) << errMsg;
            return false;
        }
        if (!dupArgsChecker.insert(*contentPtr).second) {
            Ctx.Error(argPtr->GetPos()) << "Duplicate lambda argument parametr: '" << *contentPtr << "'.";
            return false;
        }
        args.push_back(*contentPtr);
    }
    return true;
}

bool TSqlExpression::SqlLambdaExprBody(TContext& ctx, const TRule_lambda_body& node, TVector<TNodePtr>& exprSeq) {
    TSqlExpression expr(ctx, ctx.Settings.Mode);
    TVector<TString> localNames;
    bool hasError = false;
    for (auto& block: node.GetBlock1()) {
        const auto& rule = block.GetRule_lambda_stmt1();
        switch (rule.Alt_case()) {
            case TRule_lambda_stmt::kAltLambdaStmt1: {
                TVector<TString> names;
                auto nodeExpr = NamedNode(rule.GetAlt_lambda_stmt1().GetRule_named_nodes_stmt1(), names);
                if (!nodeExpr) {
                    hasError = true;
                    continue;
                } else if (nodeExpr->GetSource()) {
                    ctx.Error() << "SELECT is not supported inside lambda body";
                    hasError = true;
                    continue;
                }

                if (names.size() > 1) {
                    auto ref = ctx.MakeName("tie");
                    exprSeq.push_back(nodeExpr->Y("EnsureTupleSize", nodeExpr, nodeExpr->Q(ToString(names.size()))));
                    exprSeq.back()->SetLabel(ref);
                    for (size_t i = 0; i < names.size(); ++i) {
                        TNodePtr nthExpr = nodeExpr->Y("Nth", ref, nodeExpr->Q(ToString(i)));
                        nthExpr->SetLabel(names[i]);
                        localNames.push_back(names[i]);
                        PushNamedNode(names[i], BuildAtom(nodeExpr->GetPos(), names[i], NYql::TNodeFlags::Default));
                        exprSeq.push_back(nthExpr);
                    }
                } else {
                    nodeExpr->SetLabel(names.front());
                    localNames.push_back(names.front());
                    PushNamedNode(names.front(), BuildAtom(nodeExpr->GetPos(), names.front(), NYql::TNodeFlags::Default));
                    exprSeq.push_back(nodeExpr);
                }
                break;
            }
            case TRule_lambda_stmt::kAltLambdaStmt2: {
                if (!ImportStatement(rule.GetAlt_lambda_stmt2().GetRule_import_stmt1(), &localNames)) {
                    hasError = true;
                }
                break;
            }
            default:
                Y_ABORT("SampleClause: does not correspond to grammar changes");
        }
    }

    TNodePtr nodeExpr;
    if (!hasError) {
        nodeExpr = expr.Build(node.GetRule_expr3());
    }

    for (const auto& name : localNames) {
        PopNamedNode(name);
    }

    if (!nodeExpr) {
        return false;
    }
    exprSeq.push_back(nodeExpr);
    return true;
}

TNodePtr TSqlExpression::SubExpr(const TRule_con_subexpr& node) {
    // con_subexpr: unary_subexpr | unary_op unary_subexpr;
    switch (node.Alt_case()) {
        case TRule_con_subexpr::kAltConSubexpr1:
            return UnaryExpr(node.GetAlt_con_subexpr1().GetRule_unary_subexpr1());
        case TRule_con_subexpr::kAltConSubexpr2: {
            Ctx.IncrementMonCounter("sql_features", "UnaryOperation");
            TString opName;
            auto token = node.GetAlt_con_subexpr2().GetRule_unary_op1().GetToken1();
            Token(token);
            TPosition pos(Ctx.Pos());
            switch (token.GetId()) {
                case SQLLexerTokens::TOKEN_NOT: opName = "Not"; break;
                case SQLLexerTokens::TOKEN_PLUS: opName = "Plus"; break;
                case SQLLexerTokens::TOKEN_MINUS: opName = "Minus"; break;
                case SQLLexerTokens::TOKEN_TILDA: opName = "BitNot"; break;
                default:
                    Ctx.IncrementMonCounter("sql_errors", "UnsupportedUnaryOperation");
                    Error() << "Unsupported unary operation: " << token.GetValue();
                    return nullptr;
            }
            Ctx.IncrementMonCounter("sql_unary_operations", opName);
            return BuildUnaryOp(pos, opName, UnaryExpr(node.GetAlt_con_subexpr2().GetRule_unary_subexpr2()));
        }
        default:
            Y_ABORT("You should change implementation according grammar changes");
    }
    return nullptr;
}

TNodePtr TSqlExpression::SubExpr(const TRule_xor_subexpr& node) {
    // xor_subexpr: eq_subexpr cond_expr?;
    TNodePtr res(SubExpr(node.GetRule_eq_subexpr1()));
    if (!res) {
        return {};
    }
    TPosition pos(Ctx.Pos());
    if (node.HasBlock2()) {
        auto cond = node.GetBlock2().GetRule_cond_expr1();
        switch (cond.Alt_case()) {
            case TRule_cond_expr::kAltCondExpr1: {
                const auto& matchOp = cond.GetAlt_cond_expr1();
                const bool notMatch = matchOp.HasBlock1();
                const TCiString& opName = Token(matchOp.GetRule_match_op2().GetToken1());
                const auto& pattern = SubExpr(cond.GetAlt_cond_expr1().GetRule_eq_subexpr3());
                if (!pattern) {
                    return {};
                }
                TNodePtr isMatch;
                if (opName == "like" || opName == "ilike") {
                    const TString* escapeLiteral = nullptr;
                    TNodePtr escapeNode;
                    const auto& escaper = BuildUdf(Ctx, pos, "Re2", "PatternFromLike", {});
                    TVector<TNodePtr> escaperArgs({ escaper, pattern });

                    if (matchOp.HasBlock4()) {
                        const auto& escapeBlock = matchOp.GetBlock4();
                        TNodePtr escapeExpr = SubExpr(escapeBlock.GetRule_eq_subexpr2());
                        if (!escapeExpr) {
                            return {};
                        }
                        escapeLiteral = escapeExpr->GetLiteral("String");
                        escapeNode = escapeExpr;
                        if (escapeLiteral) {
                            Ctx.IncrementMonCounter("sql_features", "LikeEscape");
                            if (escapeLiteral->size() != 1) {
                                Ctx.IncrementMonCounter("sql_errors", "LikeMultiCharEscape");
                                Error() << "ESCAPE clause requires single character argument";
                                return nullptr;
                            }
                            if (escapeLiteral[0] == "%" || escapeLiteral[0] == "_" || escapeLiteral[0] == "\\") {
                                Ctx.IncrementMonCounter("sql_errors", "LikeUnsupportedEscapeChar");
                                Error() << "'%', '_' and '\\' are currently not supported in ESCAPE clause, ";
                                Error() << "please choose any other character";
                                return nullptr;
                            }
                            escaperArgs.push_back(BuildLiteralRawString(pos, *escapeLiteral));
                        } else {
                            Ctx.IncrementMonCounter("sql_errors", "LikeNotLiteralEscape");
                            Error() << "ESCAPE clause requires String literal argument";
                            return nullptr;
                        }
                    }

                    auto re2options = BuildUdf(Ctx, pos, "Re2", "Options", {});
                    TString csMode;
                    if (opName == "ilike") {
                        Ctx.IncrementMonCounter("sql_features", "CaseInsensitiveLike");
                        csMode = "false";
                    } else {
                        csMode = "true";
                    }
                    auto csModeLiteral = BuildLiteralBool(pos, csMode);
                    csModeLiteral->SetLabel("CaseSensitive");
                    auto csOption = BuildStructure(pos, { csModeLiteral });
                    auto optionsApply = new TCallNodeImpl(pos, "NamedApply", { re2options, BuildTuple(pos, {}), csOption });

                    const TNodePtr escapedPattern = new TCallNodeImpl(pos, "Apply", { escaperArgs });
                    auto list = new TAstListNodeImpl(pos, { escapedPattern, optionsApply });
                    auto runConfig = new TAstListNodeImpl(pos, { new TAstAtomNodeImpl(pos, "quote", 0), list });

                    const auto& matcher = BuildUdf(Ctx, pos, "Re2", "Match", { runConfig });
                    isMatch = new TCallNodeImpl(pos, "Apply", { matcher, res });

                    const TString* literalPattern = pattern->GetLiteral("String");
                    if (literalPattern) {
                        TStringBuilder lowerBound;
                        bool inEscape = false;
                        bool hasPattern = false;
                        for (const char c : *literalPattern) {
                            if (escapeLiteral && c == escapeLiteral->at(0)) {
                                if (inEscape) {
                                    lowerBound.append(c);
                                    inEscape = false;
                                } else {
                                    inEscape = true;
                                }
                            } else {
                                if (c == '%' || c == '_') {
                                    if (inEscape) {
                                        lowerBound.append(c);
                                        inEscape = false;
                                    } else {
                                        hasPattern = true;
                                        break;
                                    }
                                } else {
                                    if (inEscape) {
                                        Ctx.IncrementMonCounter("sql_errors", "LikeEscapeNormalSymbol");
                                        Error() << "Escape symbol should be used twice consecutively in LIKE pattern to be considered literal";
                                        return nullptr;
                                    } else {
                                        lowerBound.append(c);
                                    }
                                }
                            }
                        }
                        if (inEscape) {
                            Ctx.IncrementMonCounter("sql_errors", "LikeEscapeSymbolEnd");
                            Error() << "LIKE pattern should not end with escape symbol";
                            return nullptr;
                        }
                        if (opName != "ilike") {
                            if (!hasPattern) {
                                isMatch = BuildBinaryOp(pos, "==", res, BuildLiteralSmartString(Ctx,
                                    TStringBuilder() << "@@" << lowerBound << "@@"));
                            } else if (!lowerBound.empty()) {
                                const auto& lowerBoundOp = BuildBinaryOp(pos, ">=", res, BuildLiteralSmartString(Ctx,
                                    TStringBuilder() << "@@" << lowerBound << "@@"));
                                auto& isMatchCopy = isMatch;
                                TStringBuilder upperBound;
                                bool madeIncrement = false;

                                for (i64 i = lowerBound.size() - 1; i >=0 ; --i) {
                                    if (!madeIncrement) {
                                        upperBound.append(lowerBound[i] + 1);
                                        madeIncrement = true;
                                    } else {
                                        upperBound.append(lowerBound[i]);
                                    }
                                }
                                if (madeIncrement) {
                                    ReverseInPlace(upperBound);
                                    const auto& between = BuildBinaryOp(
                                        pos,
                                        "And",
                                        lowerBoundOp,
                                        BuildBinaryOp(pos, "<", res, BuildLiteralSmartString(Ctx,
                                            TStringBuilder() << "@@" << upperBound << "@@"))
                                    );
                                    isMatch = BuildBinaryOp(pos, "And", between, isMatchCopy);
                                } else {
                                    isMatch = BuildBinaryOp(pos, "And", lowerBoundOp, isMatchCopy);
                                }
                            }
                        }
                    }

                    Ctx.IncrementMonCounter("sql_features", notMatch ? "NotLike" : "Like");

                } else if (opName == "regexp" || opName == "rlike" || opName == "match") {
                    if (matchOp.HasBlock4()) {
                        Ctx.IncrementMonCounter("sql_errors", "RegexpEscape");
                        TString opNameUpper(opName);
                        opNameUpper.to_upper();
                        Error() << opName << " and ESCAPE clauses should not be used together";
                        return nullptr;
                    }

                    const auto& matcher = BuildUdf(Ctx, pos, "Pcre", opName == "match" ? "BacktrackingMatch" : "BacktrackingGrep", { pattern });
                    isMatch = new TCallNodeImpl(pos, "Apply", { matcher, res });
                    if (opName != "match") {
                        Ctx.IncrementMonCounter("sql_features", notMatch ? "NotRegexp" : "Regexp");
                    } else {
                        Ctx.IncrementMonCounter("sql_features", notMatch ? "NotMatch" : "Match");
                    }
                } else {
                    Ctx.IncrementMonCounter("sql_errors", "UnknownMatchOp");
                    AltNotImplemented("match_op", cond);
                    return nullptr;
                }
                return notMatch ? BuildUnaryOp(pos, "Not", isMatch) : isMatch;
            }
            case TRule_cond_expr::kAltCondExpr2: {
                auto altInExpr = cond.GetAlt_cond_expr2();
                const bool notIn = altInExpr.HasBlock1();
                auto hints = BuildTuple(pos, {});
                if (altInExpr.HasBlock3()) {
                    Ctx.IncrementMonCounter("sql_features", "IsCompactHint");
                    auto sizeHint = BuildTuple(pos, { BuildQuotedAtom(pos, "isCompact", NYql::TNodeFlags::Default) });
                    hints = BuildTuple(pos, { sizeHint });
                }
                TSqlExpression inSubexpr(Ctx, Mode);
                auto inRight = inSubexpr.SqlInExpr(altInExpr.GetRule_in_expr4());
                auto isIn = BuildBuiltinFunc(Ctx, pos, "In", {res, inRight, hints});
                Ctx.IncrementMonCounter("sql_features", notIn ? "NotIn" : "In");
                return notIn ? BuildUnaryOp(pos, "Not", isIn) : isIn;
            }
            case TRule_cond_expr::kAltCondExpr3: {
                auto altCase = cond.GetAlt_cond_expr3().GetBlock1().Alt_case();
                const bool notNoll =
                    altCase == TRule_cond_expr::TAlt3::TBlock1::kAlt2 ||
                    altCase == TRule_cond_expr::TAlt3::TBlock1::kAlt4
                ;

                if (altCase == TRule_cond_expr::TAlt3::TBlock1::kAlt4 &&
                    !cond.GetAlt_cond_expr3().GetBlock1().GetAlt4().HasBlock1())
                {
                    Ctx.Warning(Ctx.Pos(), TIssuesIds::YQL_MISSING_IS_BEFORE_NOT_NULL) << "Missing IS keyword before NOT NULL";
                }

                auto isNull = BuildIsNullOp(pos, res);
                Ctx.IncrementMonCounter("sql_features", notNoll ? "NotNull" : "Null");
                return notNoll ? BuildUnaryOp(pos, "Not", isNull) : isNull;
            }
            case TRule_cond_expr::kAltCondExpr4: {
                auto alt = cond.GetAlt_cond_expr4();
                if (alt.HasBlock1()) {
                    Ctx.IncrementMonCounter("sql_features", "NotBetween");
                    return BuildBinaryOp(
                        pos,
                        "Or",
                        BuildBinaryOp(pos, "<", res, SubExpr(alt.GetRule_eq_subexpr3())),
                        BuildBinaryOp(pos, ">", res, SubExpr(alt.GetRule_eq_subexpr5()))
                                        );
                } else {
                    Ctx.IncrementMonCounter("sql_features", "Between");
                    return BuildBinaryOp(
                        pos,
                        "And",
                        BuildBinaryOp(pos, ">=", res, SubExpr(alt.GetRule_eq_subexpr3())),
                        BuildBinaryOp(pos, "<=", res, SubExpr(alt.GetRule_eq_subexpr5()))
                    );
                }
            }
            case TRule_cond_expr::kAltCondExpr5: {
                auto alt = cond.GetAlt_cond_expr5();
                auto getNode = [](const TRule_cond_expr::TAlt5::TBlock1& b) -> const TRule_eq_subexpr& { return b.GetRule_eq_subexpr2(); };
                return BinOpList(node.GetRule_eq_subexpr1(), getNode, alt.GetBlock1().begin(), alt.GetBlock1().end());
            }
            default:
                Ctx.IncrementMonCounter("sql_errors", "UnknownConditionExpr");
                AltNotImplemented("cond_expr", cond);
                return nullptr;
        }
    }
    return res;
}

TNodePtr TSqlExpression::BinOperList(const TString& opName, TVector<TNodePtr>::const_iterator begin, TVector<TNodePtr>::const_iterator end) const {
    TPosition pos(Ctx.Pos());
    const size_t opCount = end - begin;
    Y_DEBUG_ABORT_UNLESS(opCount >= 2);
    if (opCount == 2) {
        return BuildBinaryOp(pos, opName, *begin, *(begin+1));
    } if (opCount == 3) {
        return BuildBinaryOp(pos, opName, BuildBinaryOp(pos, opName, *begin, *(begin+1)), *(begin+2));
    } else {
        auto mid = begin + opCount / 2;
        return BuildBinaryOp(pos, opName, BinOperList(opName, begin, mid), BinOperList(opName, mid, end));
    }
}

template <typename TNode, typename TGetNode, typename TIter>
TNodePtr TSqlExpression::BinOper(const TString& opName, const TNode& node, TGetNode getNode, TIter begin, TIter end) {
    if (begin == end) {
        return SubExpr(node);
    }
    Ctx.IncrementMonCounter("sql_binary_operations", opName);
    const size_t listSize = end - begin;
    TVector<TNodePtr> nodes;
    nodes.reserve(1 + listSize);
    nodes.push_back(SubExpr(node));
    for (; begin != end; ++begin) {
        nodes.push_back(SubExpr(getNode(*begin)));
    }
    return BinOperList(opName, nodes.begin(), nodes.end());
}

template <typename TNode, typename TGetNode, typename TIter>
TNodePtr TSqlExpression::BinOpList(const TNode& node, TGetNode getNode, TIter begin, TIter end) {
    TNodePtr partialResult = SubExpr(node);
    while (begin != end) {
        Ctx.IncrementMonCounter("sql_features", "BinaryOperation");
        Token(begin->GetToken1());
        TPosition pos(Ctx.Pos());
        TString opName;
        auto tokenId = begin->GetToken1().GetId();
        switch (tokenId) {
            case SQLLexerTokens::TOKEN_LESS:
                Ctx.IncrementMonCounter("sql_binary_operations", "Less");
                opName = "<";
                break;
            case SQLLexerTokens::TOKEN_LESS_OR_EQ:
                opName = "<=";
                Ctx.IncrementMonCounter("sql_binary_operations", "LessOrEq");
                break;
            case SQLLexerTokens::TOKEN_GREATER:
                opName = ">";
                Ctx.IncrementMonCounter("sql_binary_operations", "Greater");
                break;
            case SQLLexerTokens::TOKEN_GREATER_OR_EQ:
                opName = ">=";
                Ctx.IncrementMonCounter("sql_binary_operations", "GreaterOrEq");
                break;
            case SQLLexerTokens::TOKEN_PLUS:
                opName = "+";
                Ctx.IncrementMonCounter("sql_binary_operations", "Plus");
                break;
            case SQLLexerTokens::TOKEN_MINUS:
                opName = "-";
                Ctx.IncrementMonCounter("sql_binary_operations", "Minus");
                break;
            case SQLLexerTokens::TOKEN_ASTERISK:
                opName = "*";
                Ctx.IncrementMonCounter("sql_binary_operations", "Multiply");
                break;
            case SQLLexerTokens::TOKEN_SLASH:
                opName = "/";
                Ctx.IncrementMonCounter("sql_binary_operations", "Divide");
                if (!Ctx.PragmaClassicDivision) {
                    partialResult = BuildCast(Ctx, pos, partialResult, "Double");
                }
                break;
            case SQLLexerTokens::TOKEN_PERCENT:
                opName = "%";
                Ctx.IncrementMonCounter("sql_binary_operations", "Mod");
                break;
            case SQLLexerTokens::TOKEN_EQUALS:
                Ctx.IncrementMonCounter("sql_binary_operations", "Equals");
                [[fallthrough]];
            case SQLLexerTokens::TOKEN_EQUALS2:
                Ctx.IncrementMonCounter("sql_binary_operations", "Equals2");
                opName = "==";
                break;
            case SQLLexerTokens::TOKEN_NOT_EQUALS:
                Ctx.IncrementMonCounter("sql_binary_operations", "NotEquals");
                [[fallthrough]];
            case SQLLexerTokens::TOKEN_NOT_EQUALS2:
                Ctx.IncrementMonCounter("sql_binary_operations", "NotEquals2");
                opName = "!=";
                break;
            case SQLLexerTokens::TOKEN_AMPERSAND:
                opName = "BitAnd";
                Ctx.IncrementMonCounter("sql_binary_operations", "BitAnd");
                break;
            case SQLLexerTokens::TOKEN_PIPE:
                opName = "BitOr";
                Ctx.IncrementMonCounter("sql_binary_operations", "BitOr");
                break;
            case SQLLexerTokens::TOKEN_CARET:
                opName = "BitXor";
                Ctx.IncrementMonCounter("sql_binary_operations", "BitXor");
                break;
            case SQLLexerTokens::TOKEN_SHIFT_LEFT:
                opName = "ShiftLeft";
                Ctx.IncrementMonCounter("sql_binary_operations", "ShiftLeft");
                break;
            case SQLLexerTokens::TOKEN_SHIFT_RIGHT:
                opName = "ShiftRight";
                Ctx.IncrementMonCounter("sql_binary_operations", "ShiftRight");
                break;
            case SQLLexerTokens::TOKEN_ROT_LEFT:
                opName = "RotLeft";
                Ctx.IncrementMonCounter("sql_binary_operations", "RotLeft");
                break;
            case SQLLexerTokens::TOKEN_ROT_RIGHT:
                opName = "RotRight";
                Ctx.IncrementMonCounter("sql_binary_operations", "RotRight");
                break;
            default:
                Ctx.IncrementMonCounter("sql_errors", "UnsupportedBinaryOperation");
                Error() << "Unsupported binary operation token: " << tokenId;
                return nullptr;
        }

        partialResult = BuildBinaryOp(pos, opName, partialResult, SubExpr(getNode(*begin)));
        ++begin;
    }

    return partialResult;
}

TNodePtr TSqlExpression::SqlInExpr(const TRule_in_expr& node) {
    TSqlExpression expr(Ctx, Mode);
    expr.SetSmartParenthesisMode(TSqlExpression::ESmartParenthesis::InStatement);
    auto result = expr.WrapExprShortcuts(expr.UnaryExpr(node.GetRule_in_unary_subexpr1()));
    return result;
}

TNodePtr TSqlExpression::SmartParenthesis(const TRule_smart_parenthesis& node) {
    TVector<TNodePtr> exprs;
    Token(node.GetToken1());
    const TPosition pos(Ctx.Pos());
    const bool isTuple = node.HasBlock3();
    bool expectTuple = SmartParenthesisMode == ESmartParenthesis::InStatement;
    EExpr mode = EExpr::Regular;
    if (SmartParenthesisMode == ESmartParenthesis::GroupBy) {
        mode = EExpr::GroupBy;
    } else if (SmartParenthesisMode == ESmartParenthesis::SqlLambdaParams) {
        mode = EExpr::SqlLambdaParams;
        expectTuple = true;
    }
    if (node.HasBlock2() && !NamedExprList(node.GetBlock2().GetRule_named_expr_list1(), exprs, mode)) {
        return {};
    }

    bool hasAliases = false;
    bool hasUnnamed = false;
    for (const auto& expr: exprs) {
        if (expr->GetLabel()) {
            hasAliases = true;
        } else {
            hasUnnamed = true;
        }
        if (hasAliases && hasUnnamed && SmartParenthesisMode != ESmartParenthesis::GroupBy) {
            Ctx.IncrementMonCounter("sql_errors", "AnonymousStructMembers");
            Ctx.Error(pos) << "Structure does not allow anonymous members";
            return nullptr;
        }
    }
    if (exprs.size() == 1 && hasUnnamed && !isTuple && !expectTuple) {
        return exprs.back();
    }
    if (SmartParenthesisMode == ESmartParenthesis::GroupBy) {
        /// \todo support nested tuple\struct
        if (isTuple) {
            Ctx.IncrementMonCounter("sql_errors", "SimpleTupleInGroupBy");
            Ctx.Error(pos) << "Unable to use tuple in group by clause";
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", "ListOfNamedNode");
        return BuildListOfNamedNodes(pos, std::move(exprs));
    }
    Ctx.IncrementMonCounter("sql_features", hasUnnamed ? "SimpleTuple" : "SimpleStruct");
    return hasUnnamed || expectTuple ? BuildTuple(pos, exprs) : BuildStructure(pos, exprs);
}

TNodePtr TSqlTranslation::NamedNode(const TRule_named_nodes_stmt& rule, TVector<TString>& names) {
    // named_nodes_stmt: bind_parameter_list EQUALS (expr | LPAREN select_stmt RPAREN);
    BindList(rule.GetRule_bind_parameter_list1(), names);

    TNodePtr nodeExpr = nullptr;
    switch (rule.GetBlock3().Alt_case()) {
    case TRule_named_nodes_stmt::TBlock3::kAlt1: {
        TSqlExpression expr(Ctx, Mode);
        return expr.Build(rule.GetBlock3().GetAlt1().GetRule_expr1());
    }

    case TRule_named_nodes_stmt::TBlock3::kAlt2: {
        TSqlSelect expr(Ctx, Mode);
        TPosition pos;
        auto source = expr.Build(rule.GetBlock3().GetAlt2().GetRule_select_stmt2(), pos);
        if (!source) {
            return {};
        }
        return BuildSourceNode(pos, std::move(source));
    }

    default:
        AltNotImplemented("named_node", rule.GetBlock3());
        Ctx.IncrementMonCounter("sql_errors", "UnknownNamedNode");
        return nullptr;
    }
}

bool TSqlTranslation::ImportStatement(const TRule_import_stmt& stmt, TVector<TString>* namesPtr) {
    TVector<TString> modulePath;
    if (!ModulePath(stmt.GetRule_module_path2(), modulePath)) {
        return false;
    }
    TVector<TNodePtr> bindNames;
    if (!NamedBindList(stmt.GetRule_named_bind_parameter_list4(), bindNames)) {
        return false;
    }
    const TString moduleAlias = Ctx.AddImport(std::move(modulePath));
    if (!moduleAlias) {
        return false;
    }
    for (const TNodePtr& name: bindNames) {
        const TString* contentPtr = name->GetAtomContent();
        YQL_ENSURE(contentPtr);
        const TString& nameAlias = name->GetLabel();
        const auto varName = nameAlias ? nameAlias : *contentPtr;
        PushNamedNode(varName, name->Y("bind", moduleAlias, name->Q(*contentPtr)));
        if (namesPtr) {
            namesPtr->push_back(varName);
        }
    }
    return true;
}

TNodePtr TSqlTranslation::DoStatement(const TRule_do_stmt& stmt, bool makeLambda, const TVector<TString>& args) {
    TNodePtr action;
    switch (stmt.GetBlock2().GetAltCase()) {
    case TRule_do_stmt_TBlock2::kAlt1: {
        auto bindName = NamedNodeImpl(stmt.GetBlock2().GetAlt1().GetRule_bind_parameter1(), *this);
        action = GetNamedNode(bindName);
        if (!action) {
            return nullptr;
        }
        break;
    }
    case TRule_do_stmt_TBlock2::kAlt2:
        action = BuildEmptyAction(Ctx.Pos());
        break;
    default:
        Ctx.IncrementMonCounter("sql_errors", "UnknownDoStmt");
        AltNotImplemented("do_stmt", stmt.GetBlock2());
        return nullptr;
    }

    TVector<TNodePtr> values;
    values.push_back(new TAstAtomNodeImpl(Ctx.Pos(), "Apply", TNodeFlags::Default));
    values.push_back(action);
    values.push_back(new TAstAtomNodeImpl(Ctx.Pos(), "world", TNodeFlags::Default));

    TSqlExpression sqlExpr(Ctx, Mode);
    if (stmt.HasBlock4() && !ExprList(sqlExpr, values, stmt.GetBlock4().GetRule_expr_list1())) {
        return nullptr;
    }

    TNodePtr apply = new TAstListNodeImpl(Ctx.Pos(), std::move(values));
    if (!makeLambda) {
        return BuildDoCall(Ctx.Pos(), apply);
    }

    TNodePtr params = new TAstListNodeImpl(Ctx.Pos());
    params->Add("world");
    for (const auto& arg : args) {
        params->Add(new TAstAtomNodeImpl(Ctx.Pos(), arg, TNodeFlags::ArbitraryContent));
    }

    return BuildDoCall(Ctx.Pos(), BuildLambda(Ctx.Pos(), params, apply));
}

bool TSqlSelect::JoinOp(ISource* join, const TRule_join_source::TBlock2& block) {
    const auto& node = block.GetRule_join_op1();
    switch (node.Alt_case()) {
        case TRule_join_op::kAltJoinOp1:
            Ctx.IncrementMonCounter("sql_join_operations", "CartesianProduct");
            Error() << "Cartesian product of tables is forbidden";
            return false;
        case TRule_join_op::kAltJoinOp2: {
            auto alt = node.GetAlt_join_op2();
            if (alt.HasBlock1()) {
                Ctx.IncrementMonCounter("sql_join_operations", "Natural");
                Error() << "Natural join is not implemented yet";
                return false;
            }
            TString joinOp("Inner");
            switch (alt.GetBlock2().Alt_case()) {
                case TRule_join_op::TAlt2::TBlock2::kAlt1:
                    if (alt.GetBlock2().GetAlt1().HasBlock1()) {
                        auto block = alt.GetBlock2().GetAlt1().GetBlock1();
                        switch (block.Alt_case()) {
                        case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::kAlt1:
                            // left
                            joinOp = Token(block.GetAlt1().GetToken1());
                            if (block.GetAlt1().HasBlock2()) {
                                joinOp += " " + Token(block.GetAlt1().GetBlock2().GetToken1());
                            }
                            break;
                        case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::kAlt2:
                            // right
                            joinOp = Token(block.GetAlt2().GetToken1());
                            if (block.GetAlt2().HasBlock2()) {
                                joinOp += " " + Token(block.GetAlt2().GetBlock2().GetToken1());
                            }

                            break;
                        case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::kAlt3:
                            // exclusion
                            joinOp = Token(block.GetAlt3().GetToken1());
                            break;
                        case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::kAlt4:
                            // full
                            joinOp = Token(block.GetAlt4().GetToken1());
                            break;
                        default:
                            Ctx.IncrementMonCounter("sql_errors", "UnknownJoinOperation");
                            AltNotImplemented("join_op", node);
                            return false;
                        }
                    }
                    break;
                case TRule_join_op::TAlt2::TBlock2::kAlt2:
                    joinOp = Token(alt.GetBlock2().GetAlt2().GetToken1());
                    break;
                case TRule_join_op::TAlt2::TBlock2::kAlt3:
                    joinOp = Token(alt.GetBlock2().GetAlt3().GetToken1());
                    break;
                default:
                    Ctx.IncrementMonCounter("sql_errors", "UnknownJoinOperation");
                    AltNotImplemented("join_op", node);
                    return false;
            }

            joinOp = NormalizeJoinOp(joinOp);
            Ctx.IncrementMonCounter("sql_features", "Join");
            Ctx.IncrementMonCounter("sql_join_operations", joinOp);

            TNodePtr joinKeyExpr;
            if (block.HasBlock3()) {
                if (joinOp == "Cross") {
                    Error() << "Cross join should not have ON or USING expression";
                    Ctx.IncrementMonCounter("sql_errors", "BadJoinExpr");
                    return false;
                }

                joinKeyExpr = JoinExpr(join, block.GetBlock3().GetRule_join_constraint1());
                if (!joinKeyExpr) {
                    Ctx.IncrementMonCounter("sql_errors", "BadJoinExpr");
                    return false;
                }
            }
            else {
                if (joinOp != "Cross") {
                    Error() << "Expected ON or USING expression";
                    Ctx.IncrementMonCounter("sql_errors", "BadJoinExpr");
                    return false;
                }
            }

            Y_DEBUG_ABORT_UNLESS(join->GetJoin());
            join->GetJoin()->SetupJoin(joinOp, joinKeyExpr);
            break;
        }
        default:
            Ctx.IncrementMonCounter("sql_errors", "UnknownJoinOperation2");
            AltNotImplemented("join_op", node);
            return false;
    }
    return true;
}

TNodePtr TSqlSelect::JoinExpr(ISource* join, const TRule_join_constraint& node) {
    switch (node.Alt_case()) {
        case TRule_join_constraint::kAltJoinConstraint1: {
            auto& alt = node.GetAlt_join_constraint1();
            Token(alt.GetToken1());
            TSqlExpression expr(Ctx, Mode);
            return expr.Build(alt.GetRule_expr2());
        }
        case TRule_join_constraint::kAltJoinConstraint2: {
            auto& alt = node.GetAlt_join_constraint2();
            Token(alt.GetToken1());
            TPosition pos(Ctx.Pos());
            TVector<TDeferredAtom> names;
            if (!PureColumnOrNamedListStr(alt.GetRule_pure_column_or_named_list2(), *this, names)) {
                return nullptr;
            }

            Y_DEBUG_ABORT_UNLESS(join->GetJoin());
            return join->GetJoin()->BuildJoinKeys(Ctx, names);
        }
        default:
            Ctx.IncrementMonCounter("sql_errors", "UnknownJoinConstraint");
            AltNotImplemented("join_constraint", node);
            break;
    }
    return nullptr;
}

TVector<TNodePtr> TSqlSelect::OrdinaryNamedColumnList(const TRule_ordinary_named_column_list& node) {
    TVector<TNodePtr> result;
    switch (node.Alt_case()) {
        case TRule_ordinary_named_column_list::kAltOrdinaryNamedColumnList1:
            if (!NamedColumn(result, node.GetAlt_ordinary_named_column_list1().GetRule_named_column1())) {
                return {};
            }
            break;
        case TRule_ordinary_named_column_list::kAltOrdinaryNamedColumnList2:
            if (!NamedColumnList(result, node.GetAlt_ordinary_named_column_list2().GetRule_named_column_list2())) {
                return {};
            }
            break;
        default:
            Ctx.IncrementMonCounter("sql_errors", "UnknownOrdinaryNamedColumn");
            AltNotImplemented("ordinary_named_column_list", node);
    }
    return result;
}

TSourcePtr TSqlSelect::FlattenSource(const TRule_flatten_source& node) {
    auto source = NamedSingleSource(node.GetRule_named_single_source1());
    if (!source) {
        return nullptr;
    }
    if (node.HasBlock2()) {
        auto flatten = node.GetBlock2();
        auto flatten2 = flatten.GetBlock2();
        switch (flatten2.Alt_case()) {
        case TRule_flatten_source::TBlock2::TBlock2::kAlt1: {
            TString mode = "auto";
            if (flatten2.GetAlt1().HasBlock1()) {
                mode = to_lower(Token(flatten2.GetAlt1().GetBlock1().GetToken1()));
            }

            auto flattenColumns = OrdinaryNamedColumnList(flatten2.GetAlt1().GetRule_ordinary_named_column_list3());
            if (flattenColumns.empty()) {
                return nullptr;
            }

            Ctx.IncrementMonCounter("sql_features", "FlattenByColumns");
            if (!source->AddExpressions(Ctx, flattenColumns, EExprSeat::FlattenBy)) {
                return nullptr;
            }

            source->SetFlattenByMode(mode);
            break;
        }
        case TRule_flatten_source::TBlock2::TBlock2::kAlt2: {
            Ctx.IncrementMonCounter("sql_features", "FlattenColumns");
            source->MarkFlattenColumns();
            break;
        }

        default:
            Ctx.IncrementMonCounter("sql_errors", "UnknownOrdinaryNamedColumn");
            AltNotImplemented("flatten_source", flatten2);
        }
    }
    return source;
}

TSourcePtr TSqlSelect::JoinSource(const TRule_join_source& node) {
    TSourcePtr source(FlattenSource(node.GetRule_flatten_source1()));
    if (!source) {
        return nullptr;
    }
    if (node.Block2Size()) {
        TPosition pos(Ctx.Pos());
        TVector<TSourcePtr> sources;
        sources.emplace_back(std::move(source));
        for (auto& block: node.GetBlock2()) {
            sources.emplace_back(FlattenSource(block.GetRule_flatten_source2()));
            if (!sources.back()) {
                Ctx.IncrementMonCounter("sql_errors", "NoJoinWith");
                return nullptr;
            }
        }
        source = BuildEquiJoin(pos, std::move(sources));
        for (auto& block: node.GetBlock2()) {
            if (!JoinOp(source.Get(), block)) {
                Ctx.IncrementMonCounter("sql_errors", "NoJoinOp");
                return nullptr;
            }
        }
    }
    return source;
}

bool TSqlSelect::SelectTerm(TVector<TNodePtr>& terms, const TRule_result_column& node) {
    // result_column:
    //     opt_id_prefix ASTERISK
    //   | expr (AS id_or_string)?
    // ;
    switch (node.Alt_case()) {
        case TRule_result_column::kAltResultColumn1: {
            auto alt = node.GetAlt_result_column1();

            Token(alt.GetToken2());
            auto idAsteriskQualify = OptIdPrefixAsStr(alt.GetRule_opt_id_prefix1(), *this);
            Ctx.IncrementMonCounter("sql_features", idAsteriskQualify ? "QualifyAsterisk" : "Asterisk");
            terms.push_back(BuildColumn(Ctx.Pos(), "*", idAsteriskQualify));
            break;
        }
        case TRule_result_column::kAltResultColumn2: {
            auto alt = node.GetAlt_result_column2();
            TSqlExpression expr(Ctx, Mode);
            TNodePtr term(expr.Build(alt.GetRule_expr1()));
            if (!term) {
                Ctx.IncrementMonCounter("sql_errors", "NoTerm");
                return false;
            }
            if (alt.HasBlock2()) {
                term->SetLabel(IdOrString(alt.GetBlock2().GetRule_id_or_string2(), *this));
            }
            terms.push_back(term);
            break;
        }
        default:
            Ctx.IncrementMonCounter("sql_errors", "UnknownResultColumn");
            AltNotImplemented("result_column", node);
            return false;
    }
    return true;
}

bool TSqlSelect::ValidateSelectColumns(const TVector<TNodePtr>& terms) {
    TSet<TString> labels;
    TSet<TString> asteriskSources;
    for (const auto& term: terms) {
        const auto& label = term->GetLabel();
        if (!Ctx.PragmaAllowDotInAlias && label.find('.') != TString::npos) {
            Ctx.Error(term->GetPos()) << "Unable to use '.' in column name. Invalid column name: " << label;
            return false;
        }
        if (!label.empty()) {
            if (!labels.insert(label).second) {
                Ctx.Error(term->GetPos()) << "Unable to use duplicate column names. Collision in name: " << label;
                return false;
            }
        }
        if (term->IsAsterisk()) {
            const auto& source = *term->GetSourceName();
            if (source.empty() && terms.ysize() > 1) {
                Ctx.Error(term->GetPos()) << "Unable to use general '*' with other columns, either specify concrete table like '<table>.*', either specify concrete columns.";
                return false;
            } else if (!asteriskSources.insert(source).second) {
                Ctx.Error(term->GetPos()) << "Unable to use twice same quialified asterisk. Invalid source: " << source;
                return false;
            }
        } else if (label.empty()) {
            const auto* column = term->GetColumnName();
            if (column && !column->empty()) {
                const auto& source = *term->GetSourceName();
                const auto usedName = source.empty() ? *column : source + '.' + *column;
                if (!labels.insert(usedName).second) {
                    Ctx.Error(term->GetPos()) << "Unable to use duplicate column names. Collision in name: " << usedName;
                    return false;
                }
            }
        }
    }
    return true;
}

TSourcePtr TSqlSelect::SingleSource(const TRule_single_source& node) {
    switch (node.Alt_case()) {
        case TRule_single_source::kAltSingleSource1: {
            const auto& alt = node.GetAlt_single_source1();
            const auto& table_ref = alt.GetRule_table_ref1();

            if (auto maybeSource = AsTableImpl(table_ref)) {
                auto source = *maybeSource;
                if (!source) {
                    return nullptr;
                }

                if (!source->Init(Ctx, source.Get())) {
                    return nullptr;
                }

                return source;
            } else {
                TTableRef table(TableRefImpl(alt.GetRule_table_ref1()));
                TPosition pos(Ctx.Pos());
                Ctx.IncrementMonCounter("sql_select_clusters", table.Cluster);
                if (!table.Check(Ctx)) {
                    return nullptr;
                }
                const auto serviceName = to_lower(table.ServiceName(Ctx));
                const bool stream = serviceName == RtmrProviderName;

                return BuildTableSource(pos, table, stream);
            }
        }
        case TRule_single_source::kAltSingleSource2: {
            const auto& alt = node.GetAlt_single_source2();
            Token(alt.GetToken1());
            TSqlSelect innerSelect(Ctx, Mode);
            TPosition pos;
            auto source = innerSelect.Build(alt.GetRule_select_stmt2(), pos);
            if (!source) {
                return nullptr;
            }
            return BuildInnerSource(pos, BuildSourceNode(pos, std::move(source)));
        }
        case TRule_single_source::kAltSingleSource3: {
            const auto& alt = node.GetAlt_single_source3();
            Ctx.IncrementMonCounter("sql_features", "NamedNodeUseSource");
            auto named = NamedNodeImpl(alt.GetRule_bind_parameter2(), *this);
            auto at = alt.HasBlock1();
            if (at) {
                if (alt.HasBlock3()) {
                    Ctx.Error() << "Subquery must not be used as anonymous table name";
                    return nullptr;
                }

                auto namedNode = GetNamedNode(named);
                if (!namedNode) {
                    return nullptr;
                }

                auto source = TryMakeSourceFromExpression(Ctx, namedNode, "@");
                if (!source) {
                    Ctx.Error() << "Cannot infer cluster and table name";
                    return nullptr;
                }

                return source;
            }
            auto node = GetNamedNode(named);
            if (!node) {
                Ctx.IncrementMonCounter("sql_errors", "NamedNodeSourceError");
                return nullptr;
            }
            if (alt.HasBlock3()) {
                TVector<TNodePtr> values;
                values.push_back(new TAstAtomNodeImpl(Ctx.Pos(), "Apply", TNodeFlags::Default));
                values.push_back(node);
                values.push_back(new TAstAtomNodeImpl(Ctx.Pos(), "world", TNodeFlags::Default));

                TSqlExpression sqlExpr(Ctx, Mode);
                if (alt.GetBlock3().HasBlock2() && !ExprList(sqlExpr, values, alt.GetBlock3().GetBlock2().GetRule_expr_list1())) {
                    return nullptr;
                }

                TNodePtr apply = new TAstListNodeImpl(Ctx.Pos(), std::move(values));
                return BuildNodeSource(Ctx.Pos(), apply);
            }

            return BuildInnerSource(Ctx.Pos(), node);
        }
        default:
            AltNotImplemented("single_source", node);
            Ctx.IncrementMonCounter("sql_errors", "UnknownSingleSource");
            return nullptr;
    }
}

TSourcePtr TSqlSelect::NamedSingleSource(const TRule_named_single_source& node) {
    auto singleSource = SingleSource(node.GetRule_single_source1());
    if (!singleSource) {
        return nullptr;
    }
    if (node.HasBlock2()) {
        const auto label = IdOrString(node.GetBlock2().GetRule_id_or_string2(), *this);
        singleSource->SetLabel(label);
    }
    if (node.HasBlock3()) {
        ESampleMode mode = ESampleMode::Auto;
        TSqlExpression expr(Ctx, Mode);
        TNodePtr samplingRateNode;
        TNodePtr samplingSeedNode;
        const auto& sampleBlock = node.GetBlock3();
        TPosition pos;
        switch (sampleBlock.Alt_case()) {
        case TRule_named_single_source::TBlock3::kAlt1:
            {
                const auto& sampleExpr = sampleBlock.GetAlt1().GetRule_sample_clause1().GetRule_expr2();
                samplingRateNode = expr.Build(sampleExpr);
                if (!samplingRateNode) {
                    return nullptr;
                }
                pos = GetPos(sampleBlock.GetAlt1().GetRule_sample_clause1().GetToken1());
                Ctx.IncrementMonCounter("sql_features", "SampleClause");
            }
            break;
        case TRule_named_single_source::TBlock3::kAlt2:
            {
                const auto& tableSampleClause = sampleBlock.GetAlt2().GetRule_tablesample_clause1();
                const auto& modeToken = tableSampleClause.GetRule_sampling_mode2().GetToken1();
                const TCiString& token = Token(modeToken);
                if (token == "system") {
                    mode = ESampleMode::System;
                } else if (token == "bernoulli") {
                    mode = ESampleMode::Bernoulli;
                } else {
                    Ctx.Error(GetPos(modeToken)) << "Unsupported sampling mode: " << token;
                    Ctx.IncrementMonCounter("sql_errors", "UnsupportedSamplingMode");
                    return nullptr;
                }
                const auto& tableSampleExpr = tableSampleClause.GetRule_expr4();
                samplingRateNode = expr.Build(tableSampleExpr);
                if (!samplingRateNode) {
                    return nullptr;
                }
                if (tableSampleClause.HasBlock6()) {
                    const auto& repeatableExpr = tableSampleClause.GetBlock6().GetRule_repeatable_clause1().GetRule_expr3();
                    samplingSeedNode = expr.Build(repeatableExpr);
                    if (!samplingSeedNode) {
                        return nullptr;
                    }
                }
                pos = GetPos(sampleBlock.GetAlt2().GetRule_tablesample_clause1().GetToken1());
                Ctx.IncrementMonCounter("sql_features", "SampleClause");
            }
            break;
        default:
            Y_ABORT("SampleClause: does not corresond to grammar changes");
        }
        if (!singleSource->SetSamplingOptions(Ctx, pos, mode, samplingRateNode, samplingSeedNode)) {
            Ctx.IncrementMonCounter("sql_errors", "IncorrectSampleClause");
            return nullptr;
        }
    }
    return singleSource;
}

bool TSqlSelect::ColumnName(TVector<TNodePtr>& keys, const TRule_column_name& node) {
    const auto sourceName = OptIdPrefixAsStr(node.GetRule_opt_id_prefix1(), *this);
    const auto columnName = IdOrString(node.GetRule_id_or_string2(), *this);
    YQL_ENSURE(!columnName.empty());
    keys.push_back(BuildColumn(Ctx.Pos(), columnName, sourceName));
    return true;
}

bool TSqlSelect::ColumnList(TVector<TNodePtr>& keys, const TRule_column_list& node) {
    if (!ColumnName(keys, node.GetRule_column_name1())) {
        return false;
    }
    for (auto b: node.GetBlock2()) {
        Token(b.GetToken1());
        if (!ColumnName(keys, b.GetRule_column_name2())) {
            return false;
        }
    }
    return true;
}

bool TSqlSelect::NamedColumn(TVector<TNodePtr>& columnList, const TRule_named_column& node) {
    if (!ColumnName(columnList, node.GetRule_column_name1())) {
        return false;
    }
    if (node.HasBlock2()) {
        const auto label = IdOrString(node.GetBlock2().GetRule_id_or_string2(), *this);
        columnList.back()->SetLabel(label);
    }
    return true;
}

bool TSqlSelect::NamedColumnList(TVector<TNodePtr>& columnList, const TRule_named_column_list& node) {
    if (!NamedColumn(columnList, node.GetRule_named_column1())) {
        return false;
    }
    for (auto b: node.GetBlock2()) {
        if (!NamedColumn(columnList, b.GetRule_named_column2())) {
            return false;
        }
    }
    return true;
}

bool TSqlSelect::SortSpecification(const TRule_sort_specification& node, TVector<TSortSpecificationPtr>& sortSpecs) {
    bool asc = true;
    TSqlExpression expr(Ctx, Mode);
    TNodePtr exprNode = expr.Build(node.GetRule_expr1());
    if (!exprNode) {
        return false;
    }
    if (node.HasBlock2()) {
        const auto& token = node.GetBlock2().GetToken1();
        Token(token);
        switch (token.GetId()) {
            case SQLLexerTokens::TOKEN_ASC:
                Ctx.IncrementMonCounter("sql_features", "OrderByAsc");
                break;
            case SQLLexerTokens::TOKEN_DESC:
                asc = false;
                Ctx.IncrementMonCounter("sql_features", "OrderByDesc");
                break;
            default:
                Ctx.IncrementMonCounter("sql_errors", "UnknownOrderBy");
                Error() << "Unsupported direction token: " << token.GetId();
                return false;
        }
    } else {
        Ctx.IncrementMonCounter("sql_features", "OrderByDefault");
    }
    auto sortSpecPtr = MakeIntrusive<TSortSpecification>();
    sortSpecPtr->OrderExpr = exprNode;
    sortSpecPtr->Ascending = asc;
    sortSpecs.emplace_back(sortSpecPtr);
    return true;
}

bool TSqlSelect::SortSpecificationList(const TRule_sort_specification_list& node, TVector<TSortSpecificationPtr>& sortSpecs) {
    if (!SortSpecification(node.GetRule_sort_specification1(), sortSpecs)) {
        return false;
    }
    for (auto sortSpec: node.GetBlock2()) {
        Token(sortSpec.GetToken1());
        if (!SortSpecification(sortSpec.GetRule_sort_specification2(), sortSpecs)) {
            return false;
        }
    }
    return true;
}

TSourcePtr TSqlSelect::ProcessCore(const TRule_process_core& node, const TWriteSettings& settings, TPosition& selectPos) {
    // PROCESS STREAM? named_single_source (COMMA named_single_source)* (USING call_expr (AS id_or_string)?
    // (WHERE expr)? (HAVING expr)?)?

    Token(node.GetToken1());
    TPosition startPos(Ctx.Pos());

    const bool stream = node.HasBlock2();
    if (!selectPos) {
        selectPos = startPos;
    }

    TSourcePtr source(NamedSingleSource(node.GetRule_named_single_source3()));
    if (!source) {
        return nullptr;
    }
    if (node.GetBlock4().size()) {
        TVector<TSourcePtr> sources(1, source);
        for (auto& s: node.GetBlock4()) {
            sources.push_back(NamedSingleSource(s.GetRule_named_single_source2()));
            if (!sources.back()) {
                return nullptr;
            }
        }
        auto pos = source->GetPos();
        source = BuildMuxSource(pos, std::move(sources));
    }

    bool hasUsing = node.HasBlock5();
    if (!hasUsing) {
        return BuildProcess(startPos, std::move(source), nullptr, {}, true, stream, settings);
    }

    const auto& block5 = node.GetBlock5();
    if (block5.HasBlock4()) {
        TSqlExpression expr(Ctx, Mode);
        TNodePtr where = expr.Build(block5.GetBlock4().GetRule_expr2());
        if (!where || !source->AddFilter(Ctx, where)) {
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", "ProcessWhere");
    } else {
        Ctx.IncrementMonCounter("sql_features", stream ? "ProcessStream" : "Process");
    }

    if (block5.HasBlock5()) {
        Ctx.Error() << "PROCESS does not allow HAVING yet! You may request it on yql@ maillist.";
        return nullptr;
    }

    /// \todo other solution
    PushNamedNode(TArgPlaceholderNode::ProcessRows, BuildArgPlaceholder(Ctx.Pos(), TArgPlaceholderNode::ProcessRows));
    PushNamedNode(TArgPlaceholderNode::ProcessRow, BuildArgPlaceholder(Ctx.Pos(), TArgPlaceholderNode::ProcessRow));

    bool listCall = false;
    TSqlCallExpr call(Ctx, Mode);
    bool initRet = call.Init(block5.GetRule_call_expr2());
    if (initRet) {
        call.IncCounters();
    }

    PopNamedNode(TArgPlaceholderNode::ProcessRows);
    PopNamedNode(TArgPlaceholderNode::ProcessRow);
    if (!initRet) {
        return nullptr;
    }

    auto args = call.GetArgs();

    /// SIN: special processing of binds
    for (auto& arg: args) {
        auto placeholder = dynamic_cast<TArgPlaceholderNode*>(arg.Get());
        if (placeholder) {
            auto name = placeholder->GetName();
            if (name == TArgPlaceholderNode::ProcessRows) {
                if (listCall) {
                    Ctx.Error(arg->GetPos()) << "Only single instance of " << name << " is allowed.";
                    return nullptr;
                }
                listCall = true;
                arg = new TAstAtomNodeImpl(arg->GetPos(), "inputRowsList", 0);
            } else if (name == TArgPlaceholderNode::ProcessRow) {
                arg = BuildColumn(arg->GetPos(), "*");
            }
        }
    }

    TSqlCallExpr finalCall(call, args);

    TNodePtr with(finalCall.BuildUdf(true));
    if (!with || !finalCall.EnsureNotDistinct("PROCESS")) {
        return {};

    }
    args = finalCall.GetArgs();

    if (block5.HasBlock3()) {
        with->SetLabel(IdOrString(block5.GetBlock3().GetRule_id_or_string2(), *this));
    }

    return BuildProcess(startPos, std::move(source), with, std::move(args), listCall, stream, settings);
}

TSourcePtr TSqlSelect::ReduceCore(const TRule_reduce_core& node, const TWriteSettings& settings, TPosition& selectPos) {
    // REDUCE named_single_source (COMMA named_single_source)* (PRESORT sort_specification_list)?
    // ON column_list USING ALL? call_expr (AS id_or_string)?
    // (WHERE expr)? (HAVING expr)?
    Token(node.GetToken1());
    TPosition startPos(Ctx.Pos());
    if (!selectPos) {
        selectPos = startPos;
    }

    TSourcePtr source(NamedSingleSource(node.GetRule_named_single_source2()));
    if (!source) {
        return {};
    }
    if (node.GetBlock3().size()) {
        TVector<TSourcePtr> sources(1, source);
        for (auto& s: node.GetBlock3()) {
            sources.push_back(NamedSingleSource(s.GetRule_named_single_source2()));
            if (!sources.back()) {
                return nullptr;
            }
        }
        auto pos = source->GetPos();
        source = BuildMuxSource(pos, std::move(sources));
    }

    TVector<TSortSpecificationPtr> orderBy;
    if (node.HasBlock4()) {
        if (!SortSpecificationList(node.GetBlock4().GetRule_sort_specification_list2(), orderBy)) {
            return {};
        }
    }

    TVector<TNodePtr> keys;
    if (!ColumnList(keys, node.GetRule_column_list6())) {
        return nullptr;
    }

    if (node.HasBlock11()) {
        TSqlExpression expr(Ctx, Mode);
        TNodePtr where = expr.Build(node.GetBlock11().GetRule_expr2());
        if (!where || !source->AddFilter(Ctx, where)) {
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", "ReduceWhere");
    } else {
        Ctx.IncrementMonCounter("sql_features", "Reduce");
    }

    TNodePtr having;
    if (node.HasBlock12()) {
        TSqlExpression expr(Ctx, Mode);
        having = expr.Build(node.GetBlock12().GetRule_expr2());
        if (!having) {
            return nullptr;
        }
    }

    PushNamedNode(TArgPlaceholderNode::ProcessRow, BuildColumn(Ctx.Pos(), "*"));

    TSqlCallExpr call(Ctx, Mode);
    bool initRet = call.Init(node.GetRule_call_expr9());
    if (initRet) {
        call.IncCounters();
    }

    PopNamedNode(TArgPlaceholderNode::ProcessRow);
    if (!initRet) {
        return nullptr;
    }

    auto args = call.GetArgs();

    TSqlCallExpr finalCall(call, args);

    TNodePtr udf(finalCall.BuildUdf(false));
    if (!udf || !finalCall.EnsureNotDistinct("REDUCE")) {
        return {};
    }

    if (node.HasBlock10()) {
        udf->SetLabel(IdOrString(node.GetBlock10().GetRule_id_or_string2(), *this));
    }

    const auto reduceMode = node.HasBlock8() ? ReduceMode::ByAll : ReduceMode::ByPartition;
    return BuildReduce(startPos, reduceMode, std::move(source), std::move(orderBy), std::move(keys), std::move(args), udf, having, settings);
}

TSourcePtr TSqlSelect::SelectCore(const TRule_select_core& node, const TWriteSettings& settings, TPosition& selectPos) {
    // (FROM join_source)? SELECT STREAM? opt_set_quantifier result_column (COMMA result_column)* (WITHOUT column_list)? (FROM join_source)? (WHERE expr)?
    // group_by_clause? (HAVING expr)? window_clause? order_by_clause?
    if (node.HasBlock1()) {
        Token(node.GetBlock1().GetToken1());
    } else {
        Token(node.GetToken2());
    }

    TPosition startPos(Ctx.Pos());
    if (!selectPos) {
        selectPos = Ctx.Pos();
    }

    const bool stream = node.HasBlock3();
    const bool distinct = IsDistinctOptSet(node.GetRule_opt_set_quantifier4());
    if (distinct) {
        Ctx.IncrementMonCounter("sql_features", "DistinctInSelect");
    }

    TSourcePtr source(BuildFakeSource(selectPos));
    if (node.HasBlock1() && node.HasBlock8()) {
        Token(node.GetBlock8().GetToken1());
        Ctx.IncrementMonCounter("sql_errors", "DoubleFrom");
        Ctx.Error() << "Only one FROM clause is allowed";
        return nullptr;
    }
    if (node.HasBlock1()) {
        source = JoinSource(node.GetBlock1().GetRule_join_source2());
        Ctx.IncrementMonCounter("sql_features", "FromInFront");
    } else if (node.HasBlock8()) {
        source = JoinSource(node.GetBlock8().GetRule_join_source2());
    }
    if (!source) {
        return nullptr;
    }

    TVector<TNodePtr> without;
    if (node.HasBlock7()) {
        if (!ColumnList(without, node.GetBlock7().GetRule_column_list2())) {
            return nullptr;
        }
    }
    if (node.HasBlock9()) {
        auto block = node.GetBlock9();
        Token(block.GetToken1());
        TPosition pos(Ctx.Pos());
        TSqlExpression expr(Ctx, Mode);
        TNodePtr where = expr.WrapExprShortcuts(expr.Build(block.GetRule_expr2()));
        if (!where) {
            Ctx.IncrementMonCounter("sql_errors", "WhereInvalid");
            return nullptr;
        }
        if (!source->AddFilter(Ctx, where)) {
            Ctx.IncrementMonCounter("sql_errors", "WhereNotSupportedBySource");
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", "Where");
    }

    /// \todo merge gtoupByExpr and groupBy in one
    TVector<TNodePtr> groupByExpr, groupBy;
    THoppingWindowSpecPtr hoppingWindowSpec;
    if (node.HasBlock10()) {
        TGroupByClause clause(Ctx, Mode);
        if (!clause.Build(node.GetBlock10().GetRule_group_by_clause1(), stream)) {
            return nullptr;
        }
        for (const auto& exprAlias: clause.Aliases()) {
            YQL_ENSURE(exprAlias.first == exprAlias.second->GetLabel());
            groupByExpr.emplace_back(exprAlias.second);
        }
        groupBy = std::move(clause.Content());
        clause.SetFeatures("sql_features");
        hoppingWindowSpec = clause.GetHoppingWindow();
    }

    TNodePtr having;
    if (node.HasBlock11()) {
        TSqlExpression expr(Ctx, Mode);
        having = expr.Build(node.GetBlock11().GetRule_expr2());
        if (!having) {
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", "Having");
    }

    TWinSpecs windowSpec;
    if (node.HasBlock12()) {
        if (stream) {
            Ctx.Error() << "WINDOW is not allowed in streaming queries";
            return nullptr;
        }
        if (!WindowClause(node.GetBlock12().GetRule_window_clause1(), windowSpec)) {
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", "WindowClause");
    }

    TVector<TSortSpecificationPtr> orderBy;
    if (node.HasBlock13()) {
        if (stream) {
            Ctx.Error() << "ORDER BY is not allowed in streaming queries";
            return nullptr;
        }
        if (!OrderByClause(node.GetBlock13().GetRule_order_by_clause1(), orderBy)) {
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", IsColumnsOnly(orderBy) ? "OrderBy" : "OrderByExpr");
    }
    TVector<TNodePtr> terms;
    if (!SelectTerm(terms, node.GetRule_result_column5())) {
        return nullptr;
    }
    for (auto block: node.GetBlock6()) {
        if (!SelectTerm(terms, block.GetRule_result_column2())) {
            return nullptr;
        }
    }
    if (!ValidateSelectColumns(terms)) {
        return nullptr;
    }
    return BuildSelectCore(Ctx, startPos, std::move(source), groupByExpr, groupBy, orderBy, having,
        std::move(windowSpec), hoppingWindowSpec, std::move(terms), distinct, std::move(without), stream, settings);
}

bool TSqlSelect::FrameStart(const TRule_window_frame_start& rule, TNodePtr& node, bool beginBound) {
    switch (rule.Alt_case()) {
        case TRule_window_frame_start::kAltWindowFrameStart1:
            if (!beginBound) {
                Ctx.Error() << "Unable to use UNBOUNDED PRECEDING after BETWEEN ... AND";
                return false;
            }
            node = BuildLiteralVoid(Ctx.Pos());
            return true;
        case TRule_window_frame_start::kAltWindowFrameStart2:
            if (beginBound) {
                Ctx.Error() << "Unable to use FOLLOWING before AND in BETWEEN ... AND syntax";
                return false;
            }
            {
                auto precedingRule = rule.GetAlt_window_frame_start2().GetRule_window_frame_preceding1();
                node = Literal(Ctx, precedingRule.GetRule_unsigned_number1());
                return true;
            }
        case TRule_window_frame_start::kAltWindowFrameStart3:
            return new TLiteralNumberNode<i32>(Ctx.Pos(), "Int32", ToString("0"));
            return true;
        default:
            Y_ABORT("FrameClause: frame start not corresond to grammar changes");
    }
}

bool TSqlSelect::FrameBound(const TRule_window_frame_bound& rule, TNodePtr& node, bool beginBound) {
    switch (rule.Alt_case()) {
        case TRule_window_frame_bound::kAltWindowFrameBound1:
            return FrameStart(rule.GetAlt_window_frame_bound1().GetRule_window_frame_start1(), node, beginBound);
        case TRule_window_frame_bound::kAltWindowFrameBound2:
            if (beginBound) {
                Ctx.Error() << "Unable to use UNBOUNDED FOLLOWING before AND";
                return false;
            }
            node = BuildLiteralVoid(Ctx.Pos());
            return true;
        case TRule_window_frame_bound::kAltWindowFrameBound3:
            if (beginBound) {
                Ctx.Error() << "Unable to use FOLLOWING before AND";
                return false;
            }
            {
                auto followRule = rule.GetAlt_window_frame_bound3().GetRule_window_frame_following1();
                node = Literal(Ctx, followRule.GetRule_unsigned_number1());
                return true;
            }
        default:
            Y_ABORT("FrameClause: frame bound not corresond to grammar changes");
    }
}

bool TSqlSelect::FrameClause(const TRule_window_frame_clause& rule, TMaybe<TFrameSpecification>& frameSpecLink) {
    TFrameSpecification frameSpec;
    const TString frameUnitStr = to_lower(Token(rule.GetRule_window_frame_units1().GetToken1()));
    if (frameUnitStr == "rows") {
        frameSpec.FrameType = EFrameType::FrameByRows;
    } else if (frameUnitStr == "range") {
        frameSpec.FrameType = EFrameType::FrameByRange;
    } else {
        Ctx.Error() << "Unknown frame type in window specification: " << frameUnitStr;
        return false;
    }
    auto frameExtent = rule.GetRule_window_frame_extent2();
    switch (frameExtent.Alt_case()) {
        case TRule_window_frame_extent::kAltWindowFrameExtent1:
            if (!FrameStart(frameExtent.GetAlt_window_frame_extent1().GetRule_window_frame_start1(), frameSpec.FrameBegin, true)) {
                return false;
            }
            break;
        case TRule_window_frame_extent::kAltWindowFrameExtent2: {
            auto between = frameExtent.GetAlt_window_frame_extent2().GetRule_window_frame_between1();
            if (!FrameBound(between.GetRule_window_frame_bound2(), frameSpec.FrameBegin, true)) {
                return false;
            }
            if (!FrameBound(between.GetRule_window_frame_bound4(), frameSpec.FrameEnd, false)) {
                return false;
            }
            break;
        }
        default:
            Y_ABORT("FrameClause: frame extent not corresond to grammar changes");
    }
    if (rule.HasBlock3()) {
        switch (rule.GetBlock3().GetRule_window_frame_exclusion1().Alt_case()) {
            case TRule_window_frame_exclusion::kAltWindowFrameExclusion1:
                frameSpec.FrameExclusion = FrameExclCurRow;
                break;
            case TRule_window_frame_exclusion::kAltWindowFrameExclusion2:
                frameSpec.FrameExclusion = FrameExclGroup;
                break;
            case TRule_window_frame_exclusion::kAltWindowFrameExclusion3:
                frameSpec.FrameExclusion = FrameExclTies;
                break;
            case TRule_window_frame_exclusion::kAltWindowFrameExclusion4:
                frameSpec.FrameExclusion = FrameExclNoOthers;
                break;
            default:
                Y_ABORT("FrameClause: frame exclusion not corresond to grammar changes");
        }
    }
    frameSpecLink = frameSpec;
    return true;
}

TWindowSpecificationPtr TSqlSelect::WindowSpecification(const TRule_window_specification_details& rule) {
    TWindowSpecificationPtr winSpecPtr = new TWindowSpecification;
    if (rule.HasBlock1()) {
        Ctx.Error() << "Existing window name is not supported in window specification yet!";
        return {};
    }
    if (rule.HasBlock2()) {
        if (!NamedExprList(rule.GetBlock2().GetRule_window_partition_clause1().GetRule_named_expr_list3(), winSpecPtr->Partitions)) {
            return {};
        }
    }
    if (rule.HasBlock3()) {
        if (!OrderByClause(rule.GetBlock3().GetRule_window_order_clause1().GetRule_order_by_clause1(), winSpecPtr->OrderBy)) {
            return {};
        }
    }
    if (rule.HasBlock4()) {
        if (!FrameClause(rule.GetBlock4().GetRule_window_frame_clause1(), winSpecPtr->Frame)) {
            return {};
        }
    }
    return winSpecPtr;
}

bool TSqlSelect::WindowDefenition(const TRule_window_definition& rule, TWinSpecs& winSpecs) {
    const TString windowName = Id(rule.GetRule_new_window_name1().GetRule_window_name1().GetRule_id1(), *this);
    if (winSpecs.contains(windowName)) {
        Ctx.Error() << "Unable to declare window with same name: " << windowName;
        return false;
    }
    auto windowSpec = WindowSpecification(rule.GetRule_window_specification3().GetRule_window_specification_details2());
    if (!windowSpec) {
        return false;
    }
    winSpecs.emplace(windowName, std::move(windowSpec));
    return true;
}

bool TSqlSelect::WindowClause(const TRule_window_clause& rule, TWinSpecs& winSpecs) {
    auto windowList = rule.GetRule_window_definition_list2();
    if (!WindowDefenition(windowList.GetRule_window_definition1(), winSpecs)) {
        return false;
    }
    for (auto& block: windowList.GetBlock2()) {
        if (!WindowDefenition(block.GetRule_window_definition2(), winSpecs)) {
            return false;
        }
    }
    return true;
}

bool TSqlSelect::OrderByClause(const TRule_order_by_clause& node, TVector<TSortSpecificationPtr>& orderBy) {
    return SortSpecificationList(node.GetRule_sort_specification_list3(), orderBy);
}

bool TGroupByClause::Build(const TRule_group_by_clause& node, bool stream) {
    const bool distinct = IsDistinctOptSet(node.GetRule_opt_set_quantifier3());
    if (distinct) {
        Ctx.Error() << "DISTINCT is not supported in GROUP BY clause yet!";
        Ctx.IncrementMonCounter("sql_errors", "DistinctInGroupByNotSupported");
        return false;
    }
    if (!ParseList(node.GetRule_grouping_element_list4())) {
        return false;
    }
    ResolveGroupByAndGrouping();
    if (stream && !HoppingWindowSpec) {
        Ctx.Error() << "Streaming group by query must have a hopping window specification.";
        return false;
    }
    if (!stream && HoppingWindowSpec) {
        Ctx.Error() << "Hopping window specification is not supported in a non-streaming query.";
        return false;
    }
    return true;
}

bool TGroupByClause::ParseList(const TRule_grouping_element_list& groupingListNode) {
    if (!GroupingElement(groupingListNode.GetRule_grouping_element1())) {
        return false;
    }
    for (auto b: groupingListNode.GetBlock2()) {
        if (!GroupingElement(b.GetRule_grouping_element2())) {
            return false;
        }
    }
    return true;
}

void TGroupByClause::SetFeatures(const TString& field) const {
    Ctx.IncrementMonCounter(field, "GroupBy");
    const auto& features = Features();
    if (features.Test(EGroupByFeatures::Ordinary)) {
        Ctx.IncrementMonCounter(field, "GroupByOrdinary");
    }
    if (features.Test(EGroupByFeatures::Expression)) {
        Ctx.IncrementMonCounter(field, "GroupByExpression");
    }
    if (features.Test(EGroupByFeatures::Rollup)) {
        Ctx.IncrementMonCounter(field, "GroupByRollup");
    }
    if (features.Test(EGroupByFeatures::Cube)) {
        Ctx.IncrementMonCounter(field, "GroupByCube");
    }
    if (features.Test(EGroupByFeatures::GroupingSet)) {
        Ctx.IncrementMonCounter(field, "GroupByGroupingSet");
    }
    if (features.Test(EGroupByFeatures::Empty)) {
        Ctx.IncrementMonCounter(field, "GroupByEmpty");
    }
}

TVector<TNodePtr>& TGroupByClause::Content() {
    return GroupBySet;
}

TMap<TString, TNodePtr>& TGroupByClause::Aliases() {
    return GroupSetContext->NodeAliases;
}

THoppingWindowSpecPtr TGroupByClause::GetHoppingWindow() {
    return HoppingWindowSpec;
}

TVector<TNodePtr> TGroupByClause::MultiplyGroupingSets(const TVector<TNodePtr>& lhs, const TVector<TNodePtr>& rhs) const {
    TVector<TNodePtr> content;
    for (const auto& leftNode: lhs) {
        auto leftPtr = leftNode->ContentListPtr();
        YQL_ENSURE(leftPtr, "Unable to multiply grouping sets");
        for (const auto& rightNode: rhs) {
            TVector<TNodePtr> mulItem(leftPtr->begin(), leftPtr->end());
            auto rightPtr = rightNode->ContentListPtr();
            YQL_ENSURE(rightPtr, "Unable to multiply grouping sets");
            mulItem.insert(mulItem.end(), rightPtr->begin(), rightPtr->end());
            content.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(mulItem)));
        }
    }
    return content;
}

void TGroupByClause::ResolveGroupByAndGrouping() {
    auto listPos = std::find_if(GroupBySet.begin(), GroupBySet.end(), [](const TNodePtr& node) {
        return node->ContentListPtr();
    });
    if (listPos == GroupBySet.end()) {
        return;
    }
    auto curContent = *(*listPos)->ContentListPtr();
    if (listPos != GroupBySet.begin()) {
        TVector<TNodePtr> emulate(GroupBySet.begin(), listPos);
        TVector<TNodePtr> emulateContent(1, BuildListOfNamedNodes(Ctx.Pos(), std::move(emulate)));
        curContent = MultiplyGroupingSets(emulateContent, curContent);
    }
    for (++listPos; listPos != GroupBySet.end(); ++listPos) {
        auto newElem = (*listPos)->ContentListPtr();
        if (newElem) {
            curContent = MultiplyGroupingSets(curContent, *newElem);
        } else {
            TVector<TNodePtr> emulate(1, *listPos);
            TVector<TNodePtr> emulateContent(1, BuildListOfNamedNodes(Ctx.Pos(), std::move(emulate)));
            curContent = MultiplyGroupingSets(curContent, emulateContent);
        }
    }
    TVector<TNodePtr> result(1, BuildListOfNamedNodes(Ctx.Pos(), std::move(curContent)));
    std::swap(result, GroupBySet);
}

bool TGroupByClause::GroupingElement(const TRule_grouping_element& node) {
    TSourcePtr res;
    TVector<TNodePtr> emptyContent;
    switch (node.Alt_case()) {
        case TRule_grouping_element::kAltGroupingElement1:
            if (!OrdinaryGroupingSet(node.GetAlt_grouping_element1().GetRule_ordinary_grouping_set1())) {
                return false;
            }
            Features().Set(EGroupByFeatures::Ordinary);
            break;
        case TRule_grouping_element::kAltGroupingElement2: {
            TGroupByClause subClause(Ctx, Mode, GroupSetContext);
            if (!subClause.OrdinaryGroupingSetList(node.GetAlt_grouping_element2().GetRule_rollup_list1().GetRule_ordinary_grouping_set_list3())) {
                return false;
            }
            auto& content = subClause.Content();
            if (!IsNodeColumnsOrNamedExpression(content, "ROLLUP")) {
                return false;
            }
            TVector<TNodePtr> collection;
            for (auto limit = content.end(), begin = content.begin(); limit != begin; --limit) {
                TVector<TNodePtr> grouping(begin, limit);
                collection.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(grouping)));
            }
            collection.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(emptyContent)));
            GroupBySet.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(collection)));
            Ctx.IncrementMonCounter("sql_features", TStringBuilder() << "GroupByRollup" << content.size());
            Features().Set(EGroupByFeatures::Rollup);
            break;
        }
        case TRule_grouping_element::kAltGroupingElement3: {
            TGroupByClause subClause(Ctx, Mode, GroupSetContext);
            if (!subClause.OrdinaryGroupingSetList(node.GetAlt_grouping_element3().GetRule_cube_list1().GetRule_ordinary_grouping_set_list3())) {
                return false;
            }
            auto& content = subClause.Content();
            if (!IsNodeColumnsOrNamedExpression(content, "CUBE")) {
                return false;
            }
            if (content.size() > Ctx.PragmaGroupByCubeLimit) {
                Ctx.Error() << "GROUP BY CUBE is allowed only for " << Ctx.PragmaGroupByCubeLimit << " columns, but you use " << content.size();
                return false;
            }
            TVector<TNodePtr> collection;
            for (unsigned mask = (1 << content.size()) - 1; mask > 0; --mask) {
                TVector<TNodePtr> grouping;
                for (unsigned index = 0; index < content.size(); ++index) {
                    if (mask & (1 << index)) {
                        grouping.push_back(content[content.size() - index - 1]);
                    }
                }
                collection.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(grouping)));
            }
            collection.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(emptyContent)));
            GroupBySet.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(collection)));
            Ctx.IncrementMonCounter("sql_features", TStringBuilder() << "GroupByCube" << content.size());
            Features().Set(EGroupByFeatures::Cube);
            break;
        }
        case TRule_grouping_element::kAltGroupingElement4: {
            auto listNode = node.GetAlt_grouping_element4().GetRule_grouping_sets_specification1().GetRule_grouping_element_list4();
            TGroupByClause subClause(Ctx, Mode, GroupSetContext);
            if (!subClause.ParseList(listNode)) {
                return false;
            }
            auto& content = subClause.Content();
            if (!IsNodeColumnsOrNamedExpression(content, "GROUPING SETS")) {
                return false;
            }
            TVector<TNodePtr> collection;
            bool hasEmpty = false;
            for (auto& elem: content) {
                auto elemContent = elem->ContentListPtr();
                if (elemContent) {
                    if (!elemContent->empty() && elemContent->front()->ContentListPtr()) {
                        for (auto& sub: *elemContent) {
                            FeedCollection(sub, collection, hasEmpty);
                        }
                    } else {
                        FeedCollection(elem, collection, hasEmpty);
                    }
                } else {
                    TVector<TNodePtr> elemList(1, std::move(elem));
                    collection.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(elemList)));
                }
            }
            GroupBySet.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(collection)));
            Features().Set(EGroupByFeatures::GroupingSet);
            break;
        }
        case TRule_grouping_element::kAltGroupingElement5: {
            if (!HoppingWindow(node.GetAlt_grouping_element5().GetRule_hopping_window_specification1())) {
                return false;
            }
            break;
        }
        default:
            Y_ABORT("You should change implementation according grammar changes");
    }
    return true;
}

void TGroupByClause::FeedCollection(const TNodePtr& elem, TVector<TNodePtr>& collection, bool& hasEmpty) const {
    auto elemContentPtr = elem->ContentListPtr();
    if (elemContentPtr && elemContentPtr->empty()) {
        if (hasEmpty) {
            return;
        }
        hasEmpty = true;
    }
    collection.push_back(elem);
}

bool TGroupByClause::OrdinaryGroupingSet(const TRule_ordinary_grouping_set& node) {
    auto namedExprNode = NamedExpr(node.GetRule_named_expr1(), EExpr::GroupBy);
    if (!namedExprNode) {
        return false;
    }
    auto nodeLabel = namedExprNode->GetLabel();
    auto contentPtr = namedExprNode->ContentListPtr();
    if (contentPtr) {
        if (nodeLabel && (contentPtr->size() != 1 || contentPtr->front()->GetLabel())) {
            Ctx.Error() << "Unable to use aliases for list of named expressions";
            Ctx.IncrementMonCounter("sql_errors", "GroupByAliasForListOfExpressions");
            return false;
        }
        for (auto& content: *contentPtr) {
            auto label = content->GetLabel();
            if (!label) {
                if (content->GetColumnName()) {
                    namedExprNode->AssumeColumn();
                    continue;
                }

                content->SetLabel(label = GenerateGroupByExprName());
            }
            if (!AddAlias(label, content)) {
                return false;
            }
            content = BuildColumn(content->GetPos(), label);
        }
    } else {
        if (!nodeLabel && namedExprNode->GetColumnName()) {
            namedExprNode->AssumeColumn();
        }

        if (!nodeLabel && !namedExprNode->GetColumnName()) {
            namedExprNode->SetLabel(nodeLabel = GenerateGroupByExprName());
        }
        if (nodeLabel) {
            if (!AddAlias(nodeLabel, namedExprNode)) {
                return false;
            }
            namedExprNode = BuildColumn(namedExprNode->GetPos(), nodeLabel);
        }
    }
    GroupBySet.emplace_back(std::move(namedExprNode));
    return true;
}

bool TGroupByClause::OrdinaryGroupingSetList(const TRule_ordinary_grouping_set_list& node) {
    if (!OrdinaryGroupingSet(node.GetRule_ordinary_grouping_set1())) {
        return false;
    }
    for (auto& block: node.GetBlock2()) {
        if (!OrdinaryGroupingSet(block.GetRule_ordinary_grouping_set2())) {
            return false;
        }
    }
    return true;
}

bool TGroupByClause::HoppingWindow(const TRule_hopping_window_specification& node) {
    if (HoppingWindowSpec) {
        Ctx.Error() << "Duplicate hopping window specification.";
        return false;
    }
    HoppingWindowSpec = new THoppingWindowSpec;
    {
        TSqlExpression expr(Ctx, Mode);
        HoppingWindowSpec->TimeExtractor = expr.Build(node.GetRule_expr3());
        if (!HoppingWindowSpec->TimeExtractor) {
            return false;
        }
    }
    auto processIntervalParam = [&] (const TRule_expr& rule) -> TNodePtr {
        TSqlExpression expr(Ctx, Mode);
        auto node = expr.Build(rule);
        if (!node) {
            return nullptr;
        }

        auto literal = node->GetLiteral("String");
        if (!literal) {
            return new TAstListNodeImpl(Ctx.Pos(), {
                new TAstAtomNodeImpl(Ctx.Pos(), "EvaluateExpr", TNodeFlags::Default),
                node
            });
        }

        const auto out = NKikimr::NMiniKQL::ValueFromString(NKikimr::NUdf::EDataSlot::Interval, *literal);
        if (!out) {
            Ctx.Error(node->GetPos()) << "Expected interval in ISO 8601 format";
            return nullptr;
        }

        if ('T' == literal->back()) {
            Ctx.Warning(node->GetPos(), TIssuesIds::YQL_DEPRECATED_INTERVAL_CONSTANT) << "Time prefix 'T' at end of interval contant";
        }

        return new TAstListNodeImpl(Ctx.Pos(), {
            new TAstAtomNodeImpl(Ctx.Pos(), "Interval", TNodeFlags::Default),
            new TAstListNodeImpl(Ctx.Pos(), {
                new TAstAtomNodeImpl(Ctx.Pos(), "quote", TNodeFlags::Default),
                new TAstAtomNodeImpl(Ctx.Pos(), ToString(out.Get<i64>()), TNodeFlags::Default)
            })
        });
    };

    HoppingWindowSpec->Hop = processIntervalParam(node.GetRule_expr5());
    if (!HoppingWindowSpec->Hop) {
        return false;
    }
    HoppingWindowSpec->Interval = processIntervalParam(node.GetRule_expr7());
    if (!HoppingWindowSpec->Interval) {
        return false;
    }
    HoppingWindowSpec->Delay = processIntervalParam(node.GetRule_expr9());
    if (!HoppingWindowSpec->Delay) {
        return false;
    }

    return true;
}

bool TGroupByClause::IsNodeColumnsOrNamedExpression(const TVector<TNodePtr>& content, const TString& construction) const {
    for (const auto& node: content) {
        if (IsAutogenerated(node->GetColumnName())) {
            Ctx.Error() << "You should use in " << construction << " either expression with required alias either column name or used alias.";
            Ctx.IncrementMonCounter("sql_errors", "GroupBySetNoAliasOrColumn");
            return false;
        }
    }
    return true;
}

TGroupByClause::TGroupingSetFeatures& TGroupByClause::Features() {
    return GroupSetContext->GroupFeatures;
}

const TGroupByClause::TGroupingSetFeatures& TGroupByClause::Features() const {
    return GroupSetContext->GroupFeatures;
}

bool TGroupByClause::AddAlias(const TString& label, const TNodePtr& node) {
    if (Aliases().contains(label)) {
        Ctx.Error() << "Duplicated aliases not allowed";
        Ctx.IncrementMonCounter("sql_errors", "GroupByDuplicateAliases");
        return false;
    }
    Aliases().emplace(label, node);
    return true;
}

TString TGroupByClause::GenerateGroupByExprName() {
    return TStringBuilder() << AutogenerateNamePrefix << GroupSetContext->UnnamedCount++;
}

bool TGroupByClause::IsAutogenerated(const TString* name) const {
    return name && name->StartsWith(AutogenerateNamePrefix);
}

TSourcePtr TSqlSelect::SelectKind(const TRule_select_kind_partial& node, TPosition& selectPos) {
    auto source = SelectKind(node.GetRule_select_kind1(), selectPos);
    if (!source) {
        return {};
    }
    TPosition startPos(Ctx.Pos());
    /// LIMIT INTEGER block
    TNodePtr skipTake;
    if (node.HasBlock2()) {
        auto block = node.GetBlock2();

        Token(block.GetToken1());
        TPosition pos(Ctx.Pos());

        TSqlExpression takeExpr(Ctx, Mode);
        auto take = takeExpr.Build(block.GetRule_expr2());
        if (!take) {
            return{};
        }

        TNodePtr skip;
        if (block.HasBlock3()) {
            TSqlExpression skipExpr(Ctx, Mode);
            skip = skipExpr.Build(block.GetBlock3().GetRule_expr2());
            if (!skip) {
                return {};
            }
            if (Token(block.GetBlock3().GetToken1()) == ",") {
                // LIMIT skip, take
                skip.Swap(take);
                Ctx.IncrementMonCounter("sql_features", "LimitSkipTake");
            } else {
                Ctx.IncrementMonCounter("sql_features", "LimitOffset");
            }
        }
        skipTake = BuildSkipTake(pos, skip, take);
        Ctx.IncrementMonCounter("sql_features", "Limit");
    }
    return BuildSelect(startPos, std::move(source), skipTake);
}

TSourcePtr TSqlSelect::SelectKind(const TRule_select_kind& node, TPosition& selectPos) {
    const bool discard = node.HasBlock1();
    const bool hasLabel = node.HasBlock3();
    if ((discard || hasLabel) && (Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW || Mode == NSQLTranslation::ESqlMode::SUBQUERY)) {
        Ctx.Error() << "DISCARD and INTO RESULT are not allowed in current mode";
        return {};
    }

    if (discard && hasLabel) {
        Ctx.Error() << "DISCARD and INTO RESULT cannot be used at the same time";
        return {};
    }

    if (discard && !selectPos) {
        selectPos = Ctx.TokenPosition(node.GetBlock1().GetToken1());
    }

    TWriteSettings settings;
    settings.Discard = discard;
    if (hasLabel) {
        settings.Label = PureColumnOrNamed(node.GetBlock3().GetRule_pure_column_or_named3(), *this);
    }

    TSourcePtr res;
    switch (node.GetBlock2().Alt_case()) {
        case TRule_select_kind_TBlock2::kAlt1:
            res = ProcessCore(node.GetBlock2().GetAlt1().GetRule_process_core1(), settings, selectPos);
            break;
        case TRule_select_kind_TBlock2::kAlt2:
            res = ReduceCore(node.GetBlock2().GetAlt2().GetRule_reduce_core1(), settings, selectPos);
            break;
        case TRule_select_kind_TBlock2::kAlt3:
            res = SelectCore(node.GetBlock2().GetAlt3().GetRule_select_core1(), settings, selectPos);
            break;
        default:
            Y_ABORT("You should change implementation according grammar changes");
    }

    return res;
}

TSourcePtr TSqlSelect::SelectKind(const TRule_select_kind_parenthesis& node, TPosition& selectPos) {
    if (node.Alt_case() == TRule_select_kind_parenthesis::kAltSelectKindParenthesis1) {
        return SelectKind(node.GetAlt_select_kind_parenthesis1().GetRule_select_kind_partial1(), selectPos);
    } else {
        return SelectKind(node.GetAlt_select_kind_parenthesis2().GetRule_select_kind_partial2(), selectPos);
    }
}

TSourcePtr TSqlSelect::Build(const TRule_select_stmt& node, TPosition& selectPos) {
    auto res = SelectKind(node.GetRule_select_kind_parenthesis1(), selectPos);
    if (!res) {
        return nullptr;
    }

    TPosition unionPos = selectPos; // Position of first select
    TVector<TSourcePtr> sources;
    sources.emplace_back(std::move(res));
    for (auto& b: node.GetBlock2()) {
        auto next = SelectKind(b.GetRule_select_kind_parenthesis2(), selectPos);
        if (!next) {
            return nullptr;
        }
        switch (b.GetRule_select_op1().Alt_case()) {
            case TRule_select_op::kAltSelectOp1: {
                const  bool isUnionAll = b.GetRule_select_op1().GetAlt_select_op1().HasBlock2();
                if (!isUnionAll) {
                    Token(b.GetRule_select_op1().GetAlt_select_op1().GetToken1());
                    Ctx.Error() << "UNION without quantifier ALL is not supported yet. Did you mean UNION ALL?";
                    return nullptr;
                } else {
                    sources.emplace_back(std::move(next));
                }
                break;
            }
            default:
                Ctx.Error() << "INTERSECT and EXCEPT are not implemented yet";
                return nullptr;
        }
    }

    if (sources.size() == 1) {
        return std::move(sources[0]);
    }

    res = BuildUnionAll(unionPos, std::move(sources));
    return res;
}

class TSqlIntoValues: public TSqlTranslation {
public:
    TSqlIntoValues(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TSourcePtr Build(const TRule_into_values_source& node, const TString& operationName);

private:
    bool BuildValuesRow(const TRule_values_source_row& inRow, TVector<TNodePtr>& outRow);
    TSourcePtr ValuesSource(const TRule_values_source& node, TVector<TString>& columnsHint,
        const TString& operationName);
};

TSourcePtr TSqlIntoValues::Build(const TRule_into_values_source& node, const TString& operationName) {
    switch (node.Alt_case()) {
        case TRule_into_values_source::kAltIntoValuesSource1: {
            auto alt = node.GetAlt_into_values_source1();
            TVector<TString> columnsHint;
            if (alt.HasBlock1()) {
                PureColumnListStr(alt.GetBlock1().GetRule_pure_column_list1(), *this, columnsHint);
            }
            return ValuesSource(alt.GetRule_values_source2(), columnsHint, operationName);
        }
        default:
            Ctx.IncrementMonCounter("sql_errors", "DefaultValuesOrOther");
            AltNotImplemented("into_values_source", node);
            return nullptr;
    }
}

bool TSqlIntoValues::BuildValuesRow(const TRule_values_source_row& inRow, TVector<TNodePtr>& outRow){
    TSqlExpression sqlExpr(Ctx, Mode);
    return ExprList(sqlExpr, outRow, inRow.GetRule_expr_list2());
}

TSourcePtr TSqlIntoValues::ValuesSource(const TRule_values_source& node, TVector<TString>& columnsHint,
    const TString& operationName)
{
    Ctx.IncrementMonCounter("sql_features", "ValuesSource");
    TPosition pos(Ctx.Pos());
    switch (node.Alt_case()) {
        case TRule_values_source::kAltValuesSource1: {
            TVector<TVector<TNodePtr>> rows {{}};
            const auto& rowList = node.GetAlt_values_source1().GetRule_values_source_row_list2();

            if (!BuildValuesRow(rowList.GetRule_values_source_row1(), rows.back())) {
                return nullptr;
            }

            for (const auto& valuesSourceRow: rowList.GetBlock2()) {
                rows.push_back({});
                if (!BuildValuesRow(valuesSourceRow.GetRule_values_source_row2(), rows.back())) {
                    return nullptr;
                }
            }

            return BuildWriteValues(pos, operationName, columnsHint, rows);
        }
        case TRule_values_source::kAltValuesSource2: {
            TSqlSelect select(Ctx, Mode);
            TPosition selectPos;
            auto source = select.Build(node.GetAlt_values_source2().GetRule_select_stmt1(), selectPos);
            if (!source) {
                return nullptr;
            }
            return BuildWriteValues(pos, "UPDATE", columnsHint, std::move(source));
        }
        default:
            Ctx.IncrementMonCounter("sql_errors", "UnknownValuesSource");
            AltNotImplemented("values_source", node);
            return nullptr;
    }
}

class TSqlIntoTable: public TSqlTranslation {
public:
    TSqlIntoTable(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TNodePtr Build(const TRule_into_table_stmt& node);

private:
    //bool BuildValuesRow(const TRule_values_source_row& inRow, TVector<TNodePtr>& outRow);
    //TSourcePtr ValuesSource(const TRule_values_source& node, TVector<TString>& columnsHint);
    //TSourcePtr IntoValuesSource(const TRule_into_values_source& node);

    bool ValidateServiceName(const TRule_into_table_stmt& node, const TTableRef& table, ESQLWriteColumnMode mode,
        const TPosition& pos);
    TString SqlIntoModeStr;
    TString SqlIntoUserModeStr;
};

TNodePtr TSqlIntoTable::Build(const TRule_into_table_stmt& node) {
    static const TMap<TString, ESQLWriteColumnMode> str2Mode = {
        {"InsertInto", ESQLWriteColumnMode::InsertInto},
        {"InsertOrAbortInto", ESQLWriteColumnMode::InsertOrAbortInto},
        {"InsertOrIgnoreInto", ESQLWriteColumnMode::InsertOrIgnoreInto},
        {"InsertOrRevertInto", ESQLWriteColumnMode::InsertOrRevertInto},
        {"UpsertInto", ESQLWriteColumnMode::UpsertInto},
        {"ReplaceInto", ESQLWriteColumnMode::ReplaceInto},
        {"InsertIntoWithTruncate", ESQLWriteColumnMode::InsertIntoWithTruncate}
    };

    auto& modeBlock = node.GetBlock1();

    TVector<TToken> modeTokens;
    switch (modeBlock.Alt_case()) {
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt1:
            modeTokens = {modeBlock.GetAlt1().GetToken1()};
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt2:
            modeTokens = {
                modeBlock.GetAlt2().GetToken1(),
                modeBlock.GetAlt2().GetToken2(),
                modeBlock.GetAlt2().GetToken3()
            };
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt3:
            modeTokens = {
                modeBlock.GetAlt3().GetToken1(),
                modeBlock.GetAlt3().GetToken2(),
                modeBlock.GetAlt3().GetToken3()
            };
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt4:
            modeTokens = {
                modeBlock.GetAlt4().GetToken1(),
                modeBlock.GetAlt4().GetToken2(),
                modeBlock.GetAlt4().GetToken3()
            };
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt5:
            modeTokens = {modeBlock.GetAlt5().GetToken1()};
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt6:
            modeTokens = {modeBlock.GetAlt6().GetToken1()};
            break;
        default:
            Y_ABORT("You should change implementation according grammar changes");
    }

    TVector<TString> modeStrings;
    modeStrings.reserve(modeTokens.size());
    TVector<TString> userModeStrings;
    userModeStrings.reserve(modeTokens.size());

    for (auto& token : modeTokens) {
        auto tokenStr = Token(token);

        auto modeStr = tokenStr;
        modeStr.to_lower();
        modeStr.to_upper(0, 1);
        modeStrings.push_back(modeStr);

        auto userModeStr = tokenStr;
        userModeStr.to_upper();
        userModeStrings.push_back(userModeStr);
    }

    modeStrings.push_back("Into");
    userModeStrings.push_back("INTO");

    SqlIntoModeStr = JoinRange("", modeStrings.begin(), modeStrings.end());
    SqlIntoUserModeStr = JoinRange(" ", userModeStrings.begin(), userModeStrings.end());

    auto intoTableRef = node.GetRule_into_simple_table_ref3();
    auto tableRef = intoTableRef.GetRule_simple_table_ref1();

    auto cluster = Ctx.CurrCluster;
    std::pair<bool, TDeferredAtom> nameOrAt;
    if (tableRef.HasBlock1()) {
        switch (tableRef.GetBlock1().Alt_case()) {
        case TRule_simple_table_ref_TBlock1::AltCase::kAlt1: {
            cluster = OptIdPrefixAsClusterStr(tableRef.GetBlock1().GetAlt1().GetBlock1().GetRule_opt_id_prefix1(), *this, Ctx.CurrCluster);
            if (!cluster && Ctx.CurrCluster) {
                return nullptr;
            }
            auto id = Id(tableRef.GetBlock1().GetAlt1().GetBlock1().GetRule_id_or_at2(), *this);
            nameOrAt = std::make_pair(id.first, TDeferredAtom(Ctx.Pos(), id.second));
            break;
        }
        case TRule_simple_table_ref_TBlock1::AltCase::kAlt2: {
            auto at = tableRef.GetBlock1().GetAlt2().HasBlock1();
            auto named = GetNamedNode(NamedNodeImpl(tableRef.GetBlock1().GetAlt2().GetRule_bind_parameter2(), *this));
            if (!named) {
                return nullptr;
            }

            TDeferredAtom table;
            if (!TryMakeClusterAndTableFromExpression(named, cluster, table, Ctx)) {
                Ctx.Error() << "Cannot infer cluster and table name";
                return nullptr;
            }

            cluster = cluster.empty() ? Ctx.CurrCluster : cluster;
            nameOrAt = std::make_pair(at, table);
            break;
        }
        default:
            Y_ABORT("You should change implementation according grammar changes");
        }
    }

    bool withTruncate = false;
    if (tableRef.HasBlock2()) {
        auto hints = TableHintsImpl(tableRef.GetBlock2().GetRule_table_hints1(), *this);
        for (const auto& hint : hints) {
            if (to_upper(hint) == "TRUNCATE") {
                withTruncate = true;
            } else {
                Ctx.Error() << "Unsupported hint: " << hint;
                return nullptr;
            }
        }
    }

    TVector<TString> eraseColumns;
    if (intoTableRef.HasBlock2()) {
        auto service = Ctx.GetClusterProvider(cluster);
        if (!service) {
            Ctx.Error() << "Unknown cluster name: " << cluster;
            return nullptr;
        }

        if (*service != StatProviderName) {
            Ctx.Error() << "ERASE BY is unsupported for " << *service;
            return nullptr;
        }

        PureColumnListStr(
            intoTableRef.GetBlock2().GetRule_pure_column_list3(), *this, eraseColumns
        );
    }

    if (withTruncate) {
        if (SqlIntoModeStr != "InsertInto") {
            Error() << "Unable " << SqlIntoUserModeStr << " with truncate mode";
            return nullptr;
        }
        SqlIntoModeStr += "WithTruncate";
        SqlIntoUserModeStr += " ... WITH TRUNCATE";
    }
    const auto iterMode = str2Mode.find(SqlIntoModeStr);
    YQL_ENSURE(iterMode != str2Mode.end(), "Invalid sql write mode string: " << SqlIntoModeStr);
    const auto SqlIntoMode = iterMode->second;

    TPosition pos(Ctx.Pos());
    TNodePtr tableKey = BuildTableKey(pos, cluster, nameOrAt.second, nameOrAt.first ? "@" : "");

    TTableRef table(Ctx.MakeName("table"), cluster, tableKey);
    Ctx.IncrementMonCounter("sql_insert_clusters", table.Cluster);

    auto values = TSqlIntoValues(Ctx, Mode).Build(node.GetRule_into_values_source4(), SqlIntoUserModeStr);
    if (!values) {
        return nullptr;
    }
    if (!ValidateServiceName(node, table, SqlIntoMode, GetPos(modeTokens[0]))) {
        return nullptr;
    }
    Ctx.IncrementMonCounter("sql_features", SqlIntoModeStr);

    TNodePtr options;
    if (eraseColumns) {
        options = BuildEraseColumns(pos, std::move(eraseColumns));
    }

    return BuildWriteColumns(pos, table, ToWriteColumnsMode(SqlIntoMode), std::move(values), std::move(options));
}

bool TSqlIntoTable::ValidateServiceName(const TRule_into_table_stmt& node, const TTableRef& table,
    ESQLWriteColumnMode mode, const TPosition& pos) {
    Y_UNUSED(node);
    if (!table.Check(Ctx)) {
        return false;
    }
    auto serviceName = to_lower(table.ServiceName(Ctx));
    const bool isMapReduce = serviceName == YtProviderName;
    const bool isKikimr = serviceName == KikimrProviderName;
    const bool isRtmr = serviceName == RtmrProviderName;
    const bool isStat = serviceName == StatProviderName;

    if (!isKikimr) {
        if (mode == ESQLWriteColumnMode::InsertOrAbortInto ||
            mode == ESQLWriteColumnMode::InsertOrIgnoreInto ||
            mode == ESQLWriteColumnMode::InsertOrRevertInto ||
            mode == ESQLWriteColumnMode::UpsertInto && !isStat)
        {
            Ctx.Error(pos) << SqlIntoUserModeStr << " is not supported for " << serviceName << " tables";
            Ctx.IncrementMonCounter("sql_errors", TStringBuilder() << SqlIntoUserModeStr << "UnsupportedFor" << serviceName);
            return false;
        }
    }

    if (isMapReduce) {
        if (mode == ESQLWriteColumnMode::ReplaceInto) {
            Ctx.Error(pos) << "Meaning of REPLACE INTO has been changed, now you should use INSERT INTO <table> WITH TRUNCATE ... for " << serviceName;
            Ctx.IncrementMonCounter("sql_errors", "ReplaceIntoConflictUsage");
            return false;
        }
    } else if (isKikimr) {
        if (mode == ESQLWriteColumnMode::InsertIntoWithTruncate) {
            Ctx.Error(pos) << "INSERT INTO WITH TRUNCATE is not supported for " << serviceName << " tables";
            Ctx.IncrementMonCounter("sql_errors", TStringBuilder() << SqlIntoUserModeStr << "UnsupportedFor" << serviceName);
            return false;
        }
    } else if (isRtmr) {
        if (mode != ESQLWriteColumnMode::InsertInto) {
            Ctx.Error(pos) << SqlIntoUserModeStr << " is unsupported for " << serviceName;
            Ctx.IncrementMonCounter("sql_errors", TStringBuilder() << SqlIntoUserModeStr << "UnsupportedFor" << serviceName);
            return false;
        }
    } else if (isStat) {
        if (mode != ESQLWriteColumnMode::UpsertInto) {
            Ctx.Error(pos) << SqlIntoUserModeStr << " is unsupported for " << serviceName;
            Ctx.IncrementMonCounter("sql_errors", TStringBuilder() << SqlIntoUserModeStr << "UnsupportedFor" << serviceName);
            return false;
        }
    }

    return true;
}

class TSqlQuery: public TSqlTranslation {
public:
    TSqlQuery(TContext& ctx, NSQLTranslation::ESqlMode mode, bool topLevel)
        : TSqlTranslation(ctx, mode)
        , TopLevel(topLevel)
    {
    }

    TNodePtr Build(const TSQLParserAST& ast);

    bool Statement(TVector<TNodePtr>& blocks, const TRule_sql_stmt_core& core);
private:
    bool DeclareStatement(const TRule_declare_stmt& stmt);
    bool ExportStatement(const TRule_export_stmt& stmt);
    bool AlterTableAddColumns(TVector<TNodePtr>& blocks, const TRule_alter_table_add_column& node, const TTableRef& tr);
    bool AlterTableDropColumn(TVector<TNodePtr>& blocks, const TRule_alter_table_drop_column& node, const TTableRef& tr);
    TNodePtr PragmaStatement(const TRule_pragma_stmt& stmt, bool& success);
    void AddStatementToBlocks(TVector<TNodePtr>& blocks, TNodePtr node);

    TNodePtr Build(const TRule_delete_stmt& stmt);

    TNodePtr Build(const TRule_update_stmt& stmt);
    TSourcePtr Build(const TRule_set_clause_choice& stmt);
    bool FillSetClause(const TRule_set_clause& node, TVector<TString>& targetList, TVector<TNodePtr>& values);
    TSourcePtr Build(const TRule_set_clause_list& stmt);
    TSourcePtr Build(const TRule_multiple_column_assignment& stmt);
    TNodePtr FlexType(TTranslation& ctx, const TRule_flex_type& node);

    template<class TNode>
    void ParseStatementName(const TNode& node, TString& internalStatementName, TString& humanStatementName) {
        internalStatementName.clear();
        humanStatementName.clear();
        const auto& descr = AltDescription(node);
        TVector<TString> parts;
        const auto pos = descr.find(": ");
        Y_DEBUG_ABORT_UNLESS(pos != TString::npos);
        Split(TString(descr.begin() + pos + 2, descr.end()), "_", parts);
        Y_DEBUG_ABORT_UNLESS(parts.size() > 1);
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

void TSqlQuery::AddStatementToBlocks(TVector<TNodePtr>& blocks, TNodePtr node) {
    blocks.emplace_back(node);
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
        altCase != TRule_sql_stmt_core::kAltSqlStmtCore17)) {
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
            Ctx.BodyPart();
            TSqlSelect select(Ctx, Mode);
            TPosition pos;
            auto source = select.Build(core.GetAlt_sql_stmt_core2().GetRule_select_stmt1(), pos);
            if (!source) {
                return false;
            }
            blocks.emplace_back(BuildSelectResult(pos, std::move(source),
                Mode != NSQLTranslation::ESqlMode::LIMITED_VIEW && Mode != NSQLTranslation::ESqlMode::SUBQUERY, Mode == NSQLTranslation::ESqlMode::SUBQUERY));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore3: {
            Ctx.BodyPart();
            TVector<TString> names;
            auto nodeExpr = NamedNode(core.GetAlt_sql_stmt_core3().GetRule_named_nodes_stmt1(), names);
            if (!nodeExpr) {
                return false;
            }
            TVector<TNodePtr> nodes;
            auto subquery = nodeExpr->GetSource();
            if (subquery) {
                const auto alias = Ctx.MakeName("subquerynode");
                const auto ref = Ctx.MakeName("subquery");
                blocks.push_back(BuildSubquery(subquery, alias, Mode == NSQLTranslation::ESqlMode::SUBQUERY, names.size() == 1 ? -1 : names.size()));
                blocks.back()->SetLabel(ref);

                for (size_t i = 0; i < names.size(); ++i) {
                    nodes.push_back(BuildSubqueryRef(blocks.back(), ref, names.size() == 1 ? -1 : i));
                }
            } else {
                if (names.size() > 1) {
                    const auto ref = Ctx.MakeName("tie");
                    blocks.push_back(BuildTupleResult(nodeExpr, names.size()));
                    blocks.back()->SetLabel(ref);
                    for (size_t i = 0; i < names.size(); ++i) {
                        nodes.push_back(nodeExpr->Y("Nth", ref, nodeExpr->Q(ToString(i))));
                    }
                } else {
                    nodes.push_back(std::move(nodeExpr));
                }
            }

            for (size_t i = 0; i < names.size(); ++i) {
                PushNamedNode(names[i], nodes[i]);
            }
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore4: {
            Ctx.BodyPart();
            const auto& rule = core.GetAlt_sql_stmt_core4().GetRule_create_table_stmt1();
            TTableRef tr(SimpleTableRefImpl(rule.GetRule_simple_table_ref3(), Mode, *this));

            TVector<TColumnSchema> columns;
            TVector<TIdentifier> pkColumns;
            TVector<TIdentifier> partitionByColumns;
            TVector<std::pair<TIdentifier, bool>> orderByColumns;

            if (!CreateTableEntry(rule.GetRule_create_table_entry5(), *this, columns, pkColumns, partitionByColumns, orderByColumns)) {
                return false;
            }
            for (auto& block: rule.GetBlock6()) {
                if (!CreateTableEntry(block.GetRule_create_table_entry2(), *this, columns, pkColumns, partitionByColumns, orderByColumns)) {
                    return false;
                }
            }

            AddStatementToBlocks(blocks, BuildCreateTable(Ctx.Pos(), tr, columns, pkColumns, partitionByColumns, orderByColumns));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore5: {
            Ctx.BodyPart();
            const auto& rule = core.GetAlt_sql_stmt_core5().GetRule_drop_table_stmt1();
            if (rule.HasBlock3()) {
                Context().Error(GetPos(rule.GetToken1())) << "IF EXISTS in " << humanStatementName
                    << " is not supported.";
                return false;
            }
            TTableRef tr(SimpleTableRefImpl(rule.GetRule_simple_table_ref4(), Mode, *this));
            AddStatementToBlocks(blocks, BuildDropTable(Ctx.Pos(), tr));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore6: {
            const auto& rule = core.GetAlt_sql_stmt_core6().GetRule_use_stmt1();
            Token(rule.GetToken1());
            Ctx.CurrCluster = IdOrStringAsCluster(rule.GetRule_id_or_string2(), *this);
            if (!Ctx.CurrCluster) {
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
            TTableRef tr(SimpleTableRefImpl(rule.GetRule_simple_table_ref3(), Mode, *this));
            const auto& ruleAction = rule.GetRule_alter_table_action4();
            switch (ruleAction.Alt_case()) {
                case TRule_alter_table_action::kAltAlterTableAction1: {
                    const auto& addRule = ruleAction.GetAlt_alter_table_action1().GetRule_alter_table_add_column1();
                    if (!AlterTableAddColumns(blocks, addRule, tr)) {
                        return false;
                    }
                    break;
                }
                case TRule_alter_table_action::kAltAlterTableAction2: {
                    const auto& dropRule = ruleAction.GetAlt_alter_table_action2().GetRule_alter_table_drop_column1();
                    if (!AlterTableDropColumn(blocks, dropRule, tr)) {
                        return false;
                    }
                    break;
                }
                default:
                    AltNotImplemented("alter_table_action", core);
                    return false;
            }
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore16: {
            Ctx.BodyPart();
            auto node = DoStatement(core.GetAlt_sql_stmt_core16().GetRule_do_stmt1(), false);
            if (!node) {
                return false;
            }

            blocks.push_back(node);
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore17: {
            Ctx.BodyPart();
            if (!DefineActionOrSubqueryStatement(core.GetAlt_sql_stmt_core17().GetRule_define_action_or_subquery_stmt1())) {
                return false;
            }

            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore18: {
            Ctx.BodyPart();
            auto node = EvaluateIfStatement(core.GetAlt_sql_stmt_core18().GetRule_evaluate_if_stmt1());
            if (!node) {
                return false;
            }

            blocks.push_back(node);
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore19: {
            Ctx.BodyPart();
            auto node = EvaluateForStatement(core.GetAlt_sql_stmt_core19().GetRule_evaluate_for_stmt1());
            if (!node) {
                return false;
            }

            blocks.push_back(node);
            break;
        }
        default:
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
        if (!(defaultValue = sqlExpr.LiteralExpr(stmt.GetBlock5().GetRule_literal_value2()))) {
            return false;
        }
    }
    if (defaultValue) {
        Error() << "DEFAULT value not supported yet";
        return false;
    }
    if (!Ctx.IsParseHeading()) {
        Error() << "DECLARE statement should be in beginning of query, but it's possible to use PRAGMA or USE before it";
        return false;
    }
    const auto varName = NamedNodeImpl(stmt.GetRule_bind_parameter2(), *this);
    const auto varPos = Ctx.Pos();
    const auto typeNode = FlexType(*this, stmt.GetRule_flex_type4());
    if (!typeNode) {
        return false;
    }
    if (!Ctx.DeclareVariable(varName, typeNode)) {
        return false;
    }
    PushNamedNode(varName, BuildAtom(varPos, varName));
    return true;
}

bool TSqlQuery::ExportStatement(const TRule_export_stmt& stmt) {
    if (Mode != NSQLTranslation::ESqlMode::LIBRARY || !TopLevel) {
        Error() << "EXPORT statement should be used only in a library on the top level";
        return false;
    }

    TVector<TString> bindNames;
    if (!BindList(stmt.GetRule_bind_parameter_list2(), bindNames)) {
        return false;
    }
    return Ctx.AddExports(bindNames);
}

bool TSqlQuery::AlterTableAddColumns(TVector<TNodePtr>& blocks, const TRule_alter_table_add_column& rule, const TTableRef& tr) {
    TVector<TColumnSchema> columns;

    columns.push_back(ColumnSchemaImpl(rule.GetRule_column_schema3(), *this));
    for (const auto& block: rule.GetBlock4()) {
        columns.push_back(ColumnSchemaImpl(block.GetRule_column_schema4(), *this));
    }

    AddStatementToBlocks(blocks, BuildAlterTable(Ctx.Pos(), tr, columns, EAlterTableIntentnt::AddColumn));
    return true;
}

bool TSqlQuery::AlterTableDropColumn(TVector<TNodePtr>& blocks, const TRule_alter_table_drop_column& node, const TTableRef& tr) {
    TString name = Id(node.GetRule_id3(), *this);
    TColumnSchema column(Ctx.Pos(), name, "", false, false);
    AddStatementToBlocks(blocks, BuildAlterTable(Ctx.Pos(), tr, TVector<TColumnSchema>{column}, EAlterTableIntentnt::DropColumn));
    return true;
}

TNodePtr TSqlQuery::PragmaStatement(const TRule_pragma_stmt& stmt, bool& success) {
    success = false;
    const TString& prefix = OptIdPrefixAsStr(stmt.GetRule_opt_id_prefix2(), *this);
    const TString& lowerPrefix = to_lower(prefix);
    const TString pragma(IdOrString(stmt.GetRule_id_or_string3(), *this));
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
    const bool hasLexicalScope = withConfigure || normalizedPragma == "classicdivision";
    for (auto pragmaValue : pragmaValues) {
        if (pragmaValue->HasAlt_pragma_value3()) {
            values.push_back(TDeferredAtom(Ctx.Pos(), StringContent(Ctx, pragmaValue->GetAlt_pragma_value3().GetToken1().GetValue())));
        }
        else if (pragmaValue->HasAlt_pragma_value2()
            && pragmaValue->GetAlt_pragma_value2().GetRule_id1().HasAlt_id2()
            && "default" == Id(pragmaValue->GetAlt_pragma_value2().GetRule_id1(), *this))
        {
            pragmaValueDefault = true;
        }
        else if (withConfigure && pragmaValue->HasAlt_pragma_value5()) {
            auto bindName = NamedNodeImpl(pragmaValue->GetAlt_pragma_value5().GetRule_bind_parameter1(), *this);
            auto namedNode = GetNamedNode(bindName);
            if (!namedNode) {
                return {};
            }

            TDeferredAtom atom;
            MakeTableFromExpression(Ctx, namedNode, atom);
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
            if (values.size() != 1 || pragmaValueDefault) {
                Error() << "Expected file alias as pragma value";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            Ctx.IncrementMonCounter("sql_pragma", "udf");
            success = true;
            return BuildPragma(Ctx.Pos(), TString(ConfigProviderName), "ImportUdfs", values, false);
        } else if (normalizedPragma == "file") {
            if (values.size() != 2U || pragmaValueDefault) {
                Error() << "Expected file alias and url as pragma values";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            Ctx.IncrementMonCounter("sql_pragma", "file");
            success = true;
            return BuildPragma(Ctx.Pos(), TString(ConfigProviderName), "AddFileByUrl", values, false);
        } else if (normalizedPragma == "folder") {
            if (values.size() != 2U || pragmaValueDefault) {
                Error() << "Expected folder alias as url as pragma values";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.IncrementMonCounter("sql_pragma", "folder");
            success = true;
            return BuildPragma(Ctx.Pos(), TString(ConfigProviderName), "AddFolderByUrl", values, false);
        } else if (normalizedPragma == "library") {
            if (values.size() != 1) {
                Error() << "Expected non-empty file alias";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return{};
            }

            Ctx.Libraries.emplace(*values.front().GetLiteral());
            Ctx.IncrementMonCounter("sql_pragma", "library");
        } else if (normalizedPragma == "inferscheme" || normalizedPragma == "inferschema") {
            Ctx.Warning(Ctx.Pos(), TIssuesIds::YQL_DEPRECATED_INFERSCHEME) << "PRAGMA InferScheme is deprecated, please use PRAGMA yt.InferSchema instead.";
            Ctx.PragmaInferSchema = true;
            Ctx.IncrementMonCounter("sql_pragma", "InferSchema");
        } else if (normalizedPragma == "directread") {
            Ctx.PragmaDirectRead = true;
            Ctx.IncrementMonCounter("sql_pragma", "DirectRead");
        } else if (normalizedPragma == "equijoin") {
            Ctx.IncrementMonCounter("sql_pragma", "EquiJoin");
        } else if (normalizedPragma == "autocommit") {
            Ctx.PragmaAutoCommit = true;
            Ctx.IncrementMonCounter("sql_pragma", "AutoCommit");
        } else if (normalizedPragma == "tablepathprefix") {
            if (values.size() == 1) {
                if (!Ctx.SetPathPrefix(*values[0].GetLiteral())) {
                    return {};
                }
            } else if (values.size() == 2) {
                if (!Ctx.SetPathPrefix(*values[1].GetLiteral(), *values[0].GetLiteral())) {
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
            if (values.size() != 1 || !TryFromString(*values[0].GetLiteral(), Ctx.PragmaGroupByLimit)) {
                Error() << "Expected single unsigned integer argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.IncrementMonCounter("sql_pragma", "GroupByLimit");
        } else if (normalizedPragma == "groupbycubelimit") {
            if (values.size() != 1 || !TryFromString(*values[0].GetLiteral(), Ctx.PragmaGroupByCubeLimit)) {
                Error() << "Expected single unsigned integer argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.IncrementMonCounter("sql_pragma", "GroupByCubeLimit");
        }
        else if (normalizedPragma == "simplecolumns") {
            Ctx.SimpleColumns = true;
            Ctx.IncrementMonCounter("sql_pragma", "SimpleColumns");
        }
        else if (normalizedPragma == "disablesimplecolumns") {
            Ctx.SimpleColumns = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableSimpleColumns");
        } else if (normalizedPragma == "resultrowslimit") {
            if (values.size() != 1 || !TryFromString(*values[0].GetLiteral(), Ctx.ResultRowsLimit)) {
                Error() << "Expected single unsigned integer argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            Ctx.IncrementMonCounter("sql_pragma", "ResultRowsLimit");
        } else if (normalizedPragma == "resultsizelimit") {
            if (values.size() != 1 || !TryFromString(*values[0].GetLiteral(), Ctx.ResultSizeLimit)) {
                Error() << "Expected single unsigned integer argument for: " << pragma;
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

            TString codePattern = *values[1].GetLiteral();
            TString action = *values[0].GetLiteral();

            TWarningRule rule;
            TString parseError;
            auto parseResult = TWarningRule::ParseFrom(codePattern, action, rule, parseError);
            switch (parseResult) {
                case TWarningRule::EParseResult::PARSE_OK:
                    Ctx.WarningPolicy.AddRule(rule);
                    if (Ctx.Settings.WarnOnV0 && codePattern == "*") {
                        // Add exception for YQL_DEPRECATED_V0_SYNTAX
                        TWarningRule defaultForDeprecatedV0;
                        YQL_ENSURE(TWarningRule::ParseFrom(ToString(TIssueCode(TIssuesIds::YQL_DEPRECATED_V0_SYNTAX)),
                                                           "default", defaultForDeprecatedV0,
                                                           parseError) == TWarningRule::EParseResult::PARSE_OK);
                        Ctx.WarningPolicy.AddRule(defaultForDeprecatedV0);
                    }
                    break;
                case TWarningRule::EParseResult::PARSE_PATTERN_FAIL:
                case TWarningRule::EParseResult::PARSE_ACTION_FAIL:
                    Ctx.Error() << parseError;
                    return {};
                default:
                    Y_ENSURE(false, "Unknown parse result");
            }

            Ctx.IncrementMonCounter("sql_pragma", "warning");
        } else if (normalizedPragma == "greetings") {
            if (values.size() > 1) {
                Error() << "Not expect few arguments for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            } else if (values.empty()) {
                values.emplace_back(TDeferredAtom(Ctx.Pos(), "Hello, world! And best wishes from the YQL Team!"));
            }
            Ctx.Info(Ctx.Pos()) << *values[0].GetLiteral();
        } else if (normalizedPragma == "warningmsg") {
            if (values.size() != 1) {
                Error() << "Expected single string argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.Warning(Ctx.Pos(), TIssuesIds::YQL_PRAGMA_WARNING_MSG) << *values[0].GetLiteral();
        } else if (normalizedPragma == "errormsg") {
            if (values.size() != 1) {
                Error() << "Expected single string argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.Error(Ctx.Pos()) << *values[0].GetLiteral();
        } else if (normalizedPragma == "classicdivision") {
            if (values.size() != 1 || !TryFromString(*values[0].GetLiteral(), Ctx.PragmaClassicDivision)) {
                Error() << "Expected single boolean argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.IncrementMonCounter("sql_pragma", "ClassicDivision");
        } else if (normalizedPragma == "disableunordered") {
            Ctx.Warning(Ctx.Pos(), TIssuesIds::YQL_DEPRECATED_PRAGMA)
                << "Use of deprecated DisableUnordered pragma. It will be dropped soon";
        } else if (normalizedPragma == "pullupflatmapoverjoin") {
            Ctx.PragmaPullUpFlatMapOverJoin = true;
            Ctx.IncrementMonCounter("sql_pragma", "PullUpFlatMapOverJoin");
        } else if (normalizedPragma == "disablepullupflatmapoverjoin") {
            Ctx.PragmaPullUpFlatMapOverJoin = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisablePullUpFlatMapOverJoin");
        } else if (normalizedPragma == "enablesystemcolumns") {
            if (values.size() != 1 || !TryFromString(*values[0].GetLiteral(), Ctx.EnableSystemColumns)) {
                Error() << "Expected single boolean argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.IncrementMonCounter("sql_pragma", "EnableSystemColumns");
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
            if (normalizedPragma == "autoconvert") {
                Ctx.PragmaYsonAutoConvert = true;
                success = true;
                return {};
            } else if (normalizedPragma == "strict") {
                Ctx.PragmaYsonStrict = true;
                success = true;
                return {};
            } else if (normalizedPragma == "disablestrict") {
                Ctx.PragmaYsonStrict = false;
                success = true;
                return {};
            } else {
                Error() << "Unknown pragma: '" << pragma << "'";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

        } else if (std::find(Providers.cbegin(), Providers.cend(), lowerPrefix) == Providers.cend()) {
            if (!Ctx.HasCluster(lowerPrefix)) {
                Error() << "Unknown pragma prefix: " << prefix << ", please use cluster name or one of provider " <<
                    JoinRange(", ", Providers.cbegin(), Providers.cend());
                Ctx.IncrementMonCounter("sql_errors", "UnknownPragma");
                return {};
            }
        }

        if (normalizedPragma != "flags") {
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
    TTableRef table = SimpleTableRefImpl(stmt.GetRule_simple_table_ref3(), Mode, *this);
    if (!table.Check(Ctx)) {
        return nullptr;
    }

    auto serviceName = to_lower(table.ServiceName(Ctx));
    const bool isKikimr = serviceName == KikimrProviderName;

    if (!isKikimr) {
        Ctx.Error(GetPos(stmt.GetToken1())) << "DELETE is unsupported for " << serviceName;
        return nullptr;
    }

    TSourcePtr source = BuildTableSource(Ctx.Pos(), table, false);

    if (stmt.HasBlock4()) {
        switch (stmt.GetBlock4().Alt_case()) {
            case TRule_delete_stmt_TBlock4::kAlt1: {
                const auto& alt = stmt.GetBlock4().GetAlt1();

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

                return BuildWriteColumns(Ctx.Pos(), table, EWriteColumnMode::DeleteOn, std::move(values));
            }

            default:
                return nullptr;
        }
    }

    return BuildDelete(Ctx.Pos(), table, std::move(source));
}

TNodePtr TSqlQuery::Build(const TRule_update_stmt& stmt) {
    TTableRef table = SimpleTableRefImpl(stmt.GetRule_simple_table_ref2(), Mode, *this);
    if (!table.Check(Ctx)) {
        return nullptr;
    }

    auto serviceName = to_lower(table.ServiceName(Ctx));
    const bool isKikimr = serviceName == KikimrProviderName;

    if (!isKikimr) {
        Ctx.Error(GetPos(stmt.GetToken1())) << "UPDATE is unsupported for " << serviceName;
        return nullptr;
    }

    switch (stmt.GetBlock3().Alt_case()) {
        case TRule_update_stmt_TBlock3::kAlt1: {
            const auto& alt = stmt.GetBlock3().GetAlt1();
            TSourcePtr values = Build(alt.GetRule_set_clause_choice2());
            auto source = BuildTableSource(Ctx.Pos(), table, false);

            if (alt.HasBlock3()) {
                TSqlExpression sqlExpr(Ctx, Mode);
                auto whereExpr = sqlExpr.Build(alt.GetBlock3().GetRule_expr2());
                if (!whereExpr) {
                    return nullptr;
                }
                source->AddFilter(Ctx, whereExpr);
            }

            return BuildUpdateColumns(Ctx.Pos(), table, std::move(values), std::move(source));
        }

        case TRule_update_stmt_TBlock3::kAlt2: {
            const auto& alt = stmt.GetBlock3().GetAlt2();

            auto values = TSqlIntoValues(Ctx, Mode).Build(alt.GetRule_into_values_source2(), "UPDATE ON");
            if (!values) {
                return nullptr;
            }

            return BuildWriteColumns(Ctx.Pos(), table, EWriteColumnMode::UpdateOn, std::move(values));
        }

        default:
            return nullptr;
    }
}

TSourcePtr TSqlQuery::Build(const TRule_set_clause_choice& stmt) {
    switch (stmt.Alt_case()) {
        case TRule_set_clause_choice::kAltSetClauseChoice1:
            return Build(stmt.GetAlt_set_clause_choice1().GetRule_set_clause_list1());
        case TRule_set_clause_choice::kAltSetClauseChoice2:
            return Build(stmt.GetAlt_set_clause_choice2().GetRule_multiple_column_assignment1());
        default:
            AltNotImplemented("set_clause_choice", stmt);
            return nullptr;
    }
}

bool TSqlQuery::FillSetClause(const TRule_set_clause& node, TVector<TString>& targetList, TVector<TNodePtr>& values) {
    targetList.push_back(ColumnNameAsSingleStr(*this, node.GetRule_set_target1().GetRule_column_name1()));
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
        default:
            Ctx.IncrementMonCounter("sql_errors", "UnknownSimpleValuesSourceAlt");
            AltNotImplemented("simple_values_source", simpleValuesNode);
            return nullptr;
    }
}

TNodePtr TSqlQuery::Build(const TSQLParserAST& ast) {
    const auto& statements = ast.GetRule_sql_stmt_list();
    TVector<TNodePtr> blocks;

    if (Ctx.Settings.WarnOnV0) {
        if (Ctx.Settings.V0WarnAsError->Allow()) {
            Error() << "SQL v0 syntax is deprecated and no longer supported. Please switch to v1: https://clubs.at.yandex-team.ru/yql/2910";
            return nullptr;
        }

        Ctx.Warning(Ctx.Pos(), TIssuesIds::YQL_DEPRECATED_V0_SYNTAX) <<
            "SQL v0 syntax is deprecated and will stop working soon. Consider switching to v1: https://clubs.at.yandex-team.ru/yql/2910";
    }

    if (Ctx.Settings.V0Behavior == NSQLTranslation::EV0Behavior::Report) {
        AddStatementToBlocks(blocks, BuildPragma(TPosition(), "config", "flags", {
            TDeferredAtom(TPosition(), "SQL"),
            TDeferredAtom(TPosition(), "0")
        }, false));
    }
    if (!Statement(blocks, statements.GetRule_sql_stmt1().GetRule_sql_stmt_core2())) {
        return nullptr;
    }
    for (auto block: statements.GetBlock2()) {
        if (!Statement(blocks, block.GetRule_sql_stmt2().GetRule_sql_stmt_core2())) {
            return nullptr;
        }
    }

    ui32 topLevelSelects = 0;
    for (auto& block : blocks) {
        if (block->IsSelect()) {
            ++topLevelSelects;
        }
    }

    if ((Mode == NSQLTranslation::ESqlMode::SUBQUERY || Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW) && topLevelSelects != 1) {
        Error() << "Strictly one select/process/reduce statement must be used in the "
            << (Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW ? "view" : "subquery");
        return nullptr;
    }

    if (!Ctx.PragmaAutoCommit && Ctx.Settings.EndOfQueryCommit && IsQueryMode(Mode)) {
        AddStatementToBlocks(blocks, BuildCommitClusters(Ctx.Pos()));
    }
    return BuildQuery(Ctx.Pos(), blocks, true);
}

TNodePtr TSqlQuery::FlexType(TTranslation& ctx, const TRule_flex_type& node) {
    const auto& stringType = NSQLTranslationV0::FlexType(node, ctx);
    auto res = TryBuildDataType(Ctx.Pos(), TypeByAlias(stringType.first, !stringType.second));
    if (!res) {
        res = BuildBuiltinFunc(Ctx, Ctx.Pos(), "ParseType", {BuildLiteralRawString(Ctx.Pos(), stringType.first)});
    }
    return res;
}

bool TSqlTranslation::DefineActionOrSubqueryStatement(const TRule_define_action_or_subquery_stmt& stmt) {
    auto kind = Ctx.Token(stmt.GetToken2());
    const bool isSubquery = to_lower(kind) == "subquery";
    if (!isSubquery && Mode == NSQLTranslation::ESqlMode::SUBQUERY) {
        Error() << "Definition of actions is not allowed in the subquery";
        return false;
    }

    auto actionName = NamedNodeImpl(stmt.GetRule_bind_parameter3(), *this);
    TVector<TString> argNames;
    if (stmt.HasBlock5() && !BindList(stmt.GetBlock5().GetRule_bind_parameter_list1(), argNames)) {
        return false;
    }

    auto saveNamedNodes = Ctx.NamedNodes;
    for (const auto& arg : argNames) {
        PushNamedNode(arg, BuildAtom(Ctx.Pos(), arg, NYql::TNodeFlags::Default));
    }

    auto saveCurrCluster = Ctx.CurrCluster;
    auto savePragmaClassicDivision = Ctx.PragmaClassicDivision;
    auto saveMode = Ctx.Settings.Mode;
    if (isSubquery) {
        Ctx.Settings.Mode = NSQLTranslation::ESqlMode::SUBQUERY;
    }

    TSqlQuery query(Ctx, Ctx.Settings.Mode, false);
    TVector<TNodePtr> blocks;
    const auto& body = stmt.GetRule_define_action_or_subquery_body8();
    bool hasError = false;
    for (const auto& nestedStmtItem : body.GetBlock1()) {
        const auto& nestedStmt = nestedStmtItem.GetRule_sql_stmt_core1();
        if (!query.Statement(blocks, nestedStmt)) {
            hasError = true;
            break;
        }
    }

    ui32 topLevelSelects = 0;
    for (auto& block : blocks) {
        if (block->IsSelect()) {
            ++topLevelSelects;
        }
    }

    if (isSubquery && topLevelSelects != 1) {
        Error() << "Strictly one select/process/reduce statement must be used in the subquery";
        return false;
    }

    auto ret = !hasError ? BuildQuery(Ctx.Pos(), blocks, false) : nullptr;
    Ctx.CurrCluster = saveCurrCluster;
    Ctx.PragmaClassicDivision = savePragmaClassicDivision;
    Ctx.NamedNodes = saveNamedNodes;
    Ctx.Settings.Mode = saveMode;

    if (!ret) {
        return false;
    }

    TNodePtr blockNode = new TAstListNodeImpl(Ctx.Pos());
    blockNode->Add("block");
    blockNode->Add(blockNode->Q(ret));

    TNodePtr params = new TAstListNodeImpl(Ctx.Pos());
    params->Add("world");
    for (const auto& arg : argNames) {
        params->Add(arg);
    }

    auto lambda = BuildLambda(Ctx.Pos(), params, blockNode);
    PushNamedNode(actionName, lambda);
    return true;
}

TNodePtr TSqlTranslation::EvaluateIfStatement(const TRule_evaluate_if_stmt& stmt) {
    TSqlExpression expr(Ctx, Mode);
    auto exprNode = expr.Build(stmt.GetRule_expr3());
    if (!exprNode) {
        return {};
    }

    auto thenNode = DoStatement(stmt.GetRule_do_stmt4(), true);
    if (!thenNode) {
        return {};
    }

    TNodePtr elseNode;
    if (stmt.HasBlock5()) {
        elseNode = DoStatement(stmt.GetBlock5().GetRule_do_stmt2(), true);
        if (!elseNode) {
            return {};
        }
    }

    return BuildEvaluateIfNode(Ctx.Pos(), exprNode, thenNode, elseNode);
}

TNodePtr TSqlTranslation::EvaluateForStatement(const TRule_evaluate_for_stmt& stmt) {
    TSqlExpression expr(Ctx, Mode);
    auto itemArgName = NamedNodeImpl(stmt.GetRule_bind_parameter3(), *this);

    auto exprNode = expr.Build(stmt.GetRule_expr5());
    if (!exprNode) {
        return{};
    }

    PushNamedNode(itemArgName, new TAstAtomNodeImpl(Ctx.Pos(), itemArgName, TNodeFlags::Default));
    auto bodyNode = DoStatement(stmt.GetRule_do_stmt6(), true, { itemArgName });
    PopNamedNode(itemArgName);
    if (!bodyNode) {
        return{};
    }

    TNodePtr elseNode;
    if (stmt.HasBlock7()) {
        elseNode = DoStatement(stmt.GetBlock7().GetRule_do_stmt2(), true);
        if (!elseNode) {
            return{};
        }
    }

    return BuildEvaluateForNode(Ctx.Pos(), exprNode, bodyNode, elseNode);
}

google::protobuf::Message* SqlAST(const TString& query, const TString& queryName, TIssues& err, size_t maxErrors, google::protobuf::Arena* arena) {
    YQL_ENSURE(arena);
#if defined(_tsan_enabled_)
    TGuard<TMutex> grd(SanitizerSQLTranslationMutex);
#endif
    NSQLTranslation::TErrorCollectorOverIssues collector(err, maxErrors, "");
    NProtoAST::TProtoASTBuilder<NALP::SQLParser, NALP::SQLLexer> builder(query, queryName, arena);
    return builder.BuildAST(collector);
}

google::protobuf::Message* SqlAST(const TString& query, const TString& queryName, NProtoAST::IErrorCollector& err, google::protobuf::Arena* arena) {
    YQL_ENSURE(arena);
#if defined(_tsan_enabled_)
    TGuard<TMutex> grd(SanitizerSQLTranslationMutex);
#endif
    NProtoAST::TProtoASTBuilder<NALP::SQLParser, NALP::SQLLexer> builder(query, queryName, arena);
    return builder.BuildAST(err);
}

TAstNode* SqlASTToYql(const google::protobuf::Message& protoAst, TContext& ctx) {
    const google::protobuf::Descriptor* d = protoAst.GetDescriptor();
    if (d && d->name() != "TSQLParserAST") {
        ctx.Error() << "Invalid AST structure: " << d->name() << ", expected TSQLParserAST";
        return nullptr;
    }
    TSqlQuery query(ctx, ctx.Settings.Mode, true);
    TNodePtr node(query.Build(static_cast<const TSQLParserAST&>(protoAst)));
    try {
        if (node && node->Init(ctx, nullptr)) {
            return node->Translate(ctx);
        }
    } catch (const NProtoAST::TTooManyErrors&) {
        // do not add error issue, no room for it
    }

    return nullptr;
}

void SqlASTToYqlImpl(NYql::TAstParseResult& res, const google::protobuf::Message& protoAst,
        TContext& ctx) {
    YQL_ENSURE(!ctx.Issues.Size());
    res.Root = SqlASTToYql(protoAst, ctx);
    res.Pool = std::move(ctx.Pool);
    if (!res.Root) {
        if (ctx.Issues.Size()) {
            ctx.IncrementMonCounter("sql_errors", "AstToYqlError");
        } else {
            ctx.IncrementMonCounter("sql_errors", "AstToYqlSilentError");
            ctx.Error() << "Error occurred on parse SQL query, but no error is collected" <<
                ", please send this request over bug report into YQL interface or write on yql@ maillist";
        }
    }
}

NYql::TAstParseResult SqlASTToYql(const google::protobuf::Message& protoAst,
    const NSQLTranslation::TTranslationSettings& settings)
{
    YQL_ENSURE(IsQueryMode(settings.Mode));
    TAstParseResult res;
    TContext ctx(settings, res.Issues);
    SqlASTToYqlImpl(res, protoAst, ctx);
    res.ActualSyntaxType = ESyntaxType::YQLv0;
    return res;
}


NYql::TAstParseResult SqlToYql(const TString& query, const NSQLTranslation::TTranslationSettings& settings, NYql::TWarningRules* warningRules)
{
    TAstParseResult res;
    TContext ctx(settings, res.Issues);
    NSQLTranslation::TErrorCollectorOverIssues collector(res.Issues, settings.MaxErrors, settings.File);

    google::protobuf::Message* ast(SqlAST(query, "query", collector, settings.Arena));
    if (ast) {
        SqlASTToYqlImpl(res, *ast, ctx);
    } else {
        ctx.IncrementMonCounter("sql_errors", "AstError");
    }
    if (warningRules) {
        *warningRules = ctx.WarningPolicy.GetRules();
        ctx.WarningPolicy.Clear();
    }
    res.ActualSyntaxType = NYql::ESyntaxType::YQLv0;
    return res;
}

} // namespace NSQLTranslationV0
