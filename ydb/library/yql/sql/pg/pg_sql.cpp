#include "pg_sql.h"
#include <ydb/library/yql/parser/pg_query_wrapper/wrapper.h>
#include <ydb/library/yql/parser/pg_query_wrapper/enum.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/core/yql_callable_names.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/generic/stack.h>
#include <util/generic/hash_set.h>

namespace NSQLTranslationPG {

using namespace NYql;

class TConverter : public IPGParseEvents {
public:
    static THashMap<TString,TString> BinaryOpTranslation;
    static THashMap<TString, TString> UnaryOpTranslation;
    static THashSet<TString> AggregateFuncs;

    using TFromDesc = std::tuple<TAstNode*, TString, TVector<TString>, bool>;

    struct TExprSettings {
        bool AllowColumns = false;
        bool AllowAggregates = false;
        bool AllowOver = false;
        TVector<TAstNode*>* WindowItems = nullptr;
        TString Scope;
    };

    TConverter(TAstParseResult& astParseResult, const NSQLTranslation::TTranslationSettings& settings)
        : AstParseResult(astParseResult)
        , Settings(settings)
        , DqEngineEnabled(Settings.DqDefaultAuto->Allow())
    {
        for (auto& flag : Settings.Flags) {
            if (flag == "DqEngineEnable") {
                DqEngineEnabled = true;
            } else if (flag == "DqEngineForce") {
                DqEngineForce = true;
            }
        }
    }

    void OnResult(const PgQuery__ParseResult* result) {
        AstParseResult.Pool = std::make_unique<TMemoryPool>(4096);
        AstParseResult.Root = ParseResult(result);
    }

    void OnError(const TIssue& issue) {
        AstParseResult.Issues.AddIssue(issue);
    }

    TAstNode* ParseResult(const PgQuery__ParseResult* value) {
        auto configSource = L(A("DataSource"), QA(TString(NYql::ConfigProviderName)));
        Statements.push_back(L(A("let"), A("world"), L(A(TString(NYql::ConfigureName)), A("world"), configSource,
            QA("OrderedColumns"))));
        if (DqEngineEnabled) {
            Statements.push_back(L(A("let"), A("world"), L(A(TString(NYql::ConfigureName)), A("world"), configSource,
                QA("DqEngine"), QA(DqEngineForce ? "force" : "auto"))));
        }

        for (ui32 i = 0; i < value->n_stmts; ++i) {
           if (!RawStmt(value->stmts[i])) {
               return nullptr;
           }
        }

        Statements.push_back(L(A("return"), A("world")));
        return VL(Statements.data(), Statements.size());
    }

    [[nodiscard]]
    bool RawStmt(const PgQuery__RawStmt* value) {
        auto node = value->stmt;
        switch (node->node_case) {
        case PG_QUERY__NODE__NODE_SELECT_STMT: {
            return SelectStmt(node->select_stmt, false) != nullptr;
        }
        default:
            AltNotImplemented(value, node);
            return false;
        }
    }

    using TTraverseSelectStack = TStack<std::pair<const PgQuery__SelectStmt*, bool>>;
    using TTraverseNodeStack = TStack<std::pair<const PgQuery__Node*, bool>>;

    [[nodiscard]]
    TAstNode* SelectStmt(const PgQuery__SelectStmt* value, bool inner) {
        TTraverseSelectStack traverseSelectStack;
        traverseSelectStack.push({ value, false });

        TVector<const PgQuery__SelectStmt*> setItems;
        TVector<TAstNode*> setOpsNodes;

        while (!traverseSelectStack.empty()) {
            auto& top = traverseSelectStack.top();
            if (top.first->op == PG_QUERY__SET_OPERATION__SETOP_NONE) {
                // leaf
                setItems.push_back(top.first);
                setOpsNodes.push_back(QA("push"));
                traverseSelectStack.pop();
            } else {
                if (!top.first->larg || !top.first->rarg) {
                    AddError("SelectStmt: expected larg and rarg");
                    return nullptr;
                }

                if (!top.second) {
                    traverseSelectStack.push({ top.first->rarg, false });
                    traverseSelectStack.push({ top.first->larg, false });
                    top.second = true;
                } else {
                    TString op;
                    switch (top.first->op) {
                    case PG_QUERY__SET_OPERATION__SETOP_UNION:
                        op = "union"; break;
                    case PG_QUERY__SET_OPERATION__SETOP_INTERSECT:
                        op = "intersect"; break;
                    case PG_QUERY__SET_OPERATION__SETOP_EXCEPT:
                        op = "except"; break;
                    default:
                        AddError(TStringBuilder() << "SetOperation unsupported value: " <<
                            protobuf_c_enum_descriptor_get_value(&pg_query__set_operation__descriptor, value->op)->name);
                        return nullptr;
                    }

                    if (top.first->all) {
                        op += "_all";
                    }

                    setOpsNodes.push_back(QA(op));
                    traverseSelectStack.pop();
                }
            }
        }

        TVector<TAstNode*> setItemNodes;
        for (const auto& x : setItems) {
            if (x->n_distinct_clause) {
                AddError("SelectStmt: not supported distinct_clause");
                return nullptr;
            }

            if (x->into_clause) {
                AddError("SelectStmt: not supported into_clause");
                return nullptr;
            }

            TVector<TAstNode*> fromList;
            TVector<TAstNode*> joinOps;
            for (ui32 i = 0; i < x->n_from_clause; ++i) {
                if (x->from_clause[i]->node_case != PG_QUERY__NODE__NODE_JOIN_EXPR) {
                    auto p = FromClause(x->from_clause[i]);
                    if (!std::get<0>(p)) {
                        return nullptr;
                    }

                    AddFrom(p, fromList);
                    joinOps.push_back(QL());
                } else {
                    TTraverseNodeStack traverseNodeStack;
                    traverseNodeStack.push({ x->from_clause[i], false });
                    TVector<TAstNode*> oneJoinGroup;

                    while (!traverseNodeStack.empty()) {
                        auto& top = traverseNodeStack.top();
                        if (top.first->node_case != PG_QUERY__NODE__NODE_JOIN_EXPR) {
                            // leaf
                            auto p = FromClause(top.first);
                            if (!std::get<0>(p)) {
                                return nullptr;
                            }

                            AddFrom(p, fromList);
                            traverseNodeStack.pop();
                        } else {
                            auto join = top.first->join_expr;
                            if (!join->larg || !join->rarg) {
                                AddError("join_expr: expected larg and rarg");
                                return nullptr;
                            }

                            if (join->alias) {
                                AddError("join_expr: unsupported alias");
                                return nullptr;
                            }

                            if (join->is_natural) {
                                AddError("join_expr: unsupported natural");
                                return nullptr;
                            }

                            if (join->n_using_clause) {
                                AddError("join_expr: unsupported using");
                                return nullptr;
                            }

                            if (!top.second) {
                                traverseNodeStack.push({ join->rarg, false });
                                traverseNodeStack.push({ join->larg, false });
                                top.second = true;
                            } else {
                                TString op;
                                switch (join->jointype) {
                                case PG_QUERY__JOIN_TYPE__JOIN_INNER:
                                    op = join->quals ? "inner" : "cross"; break;
                                case PG_QUERY__JOIN_TYPE__JOIN_LEFT:
                                    op = "left"; break;
                                case PG_QUERY__JOIN_TYPE__JOIN_FULL:
                                    op = "full"; break;
                                case PG_QUERY__JOIN_TYPE__JOIN_RIGHT:
                                    op = "right"; break;
                                default:
                                    AddError(TStringBuilder() << "jointype unsupported value: " <<
                                        protobuf_c_enum_descriptor_get_value(&pg_query__join_type__descriptor, join->jointype)->name);
                                    return nullptr;
                                }

                                if (op != "cross" && !join->quals) {
                                    AddError("join_expr: expected quals for non-cross join");
                                    return nullptr;
                                }

                                if (op == "cross") {
                                    oneJoinGroup.push_back(QL(QA(op)));
                                } else {
                                    TExprSettings settings;
                                    settings.AllowColumns = true;
                                    settings.Scope = "JOIN ON";
                                    auto quals = ParseExpr(join->quals, settings);
                                    if (!quals) {
                                        return nullptr;
                                    }

                                    auto lambda = L(A("lambda"), QL(), quals);
                                    oneJoinGroup.push_back(QL(QA(op), L(A("PgWhere"), L(A("Void")), lambda)));
                                }

                                traverseNodeStack.pop();
                            }
                        }
                    }

                    joinOps.push_back(QVL(oneJoinGroup.data(), oneJoinGroup.size()));
                }
            }

            TAstNode* whereFilter = nullptr;
            if (x->where_clause) {
                TExprSettings settings;
                settings.AllowColumns = true;
                settings.Scope = "WHERE";
                whereFilter = ParseExpr(x->where_clause, settings);
                if (!whereFilter) {
                    return nullptr;
                }
            }

            TAstNode* groupBy = nullptr;
            if (x->n_group_clause) {
                TVector<TAstNode*> groupByItems;
                for (ui32 i = 0; i < x->n_group_clause; ++i) {
                    if (x->group_clause[i]->node_case != PG_QUERY__NODE__NODE_COLUMN_REF) {
                        AltNotImplemented(x, x->group_clause[i]);
                        return nullptr;
                    }

                    auto ref = ColumnRef(x->group_clause[i]->column_ref);
                    if (!ref) {
                        return nullptr;
                    }

                    auto lambda = L(A("lambda"), QL(), ref);
                    groupByItems.push_back(L(A("PgGroup"), L(A("Void")), lambda));
                }

                groupBy = QVL(groupByItems.data(), groupByItems.size());
            }

            TAstNode* having = nullptr;
            if (x->having_clause) {
                TExprSettings settings;
                settings.AllowColumns = true;
                settings.Scope = "HAVING";
                settings.AllowAggregates = true;
                having = ParseExpr(x->having_clause, settings);
                if (!having) {
                    return nullptr;
                }
            }

            TVector<TAstNode*> windowItems;
            if (x->n_window_clause) {
                for (ui32 i = 0; i < x->n_window_clause; ++i) {
                    if (x->window_clause[i]->node_case != PG_QUERY__NODE__NODE_WINDOW_DEF) {
                        AltNotImplemented(x, x->window_clause[i]);
                        return nullptr;
                    }

                    auto win = WindowDef(x->window_clause[i]->window_def);
                    if (!win) {
                        return nullptr;
                    }

                    windowItems.push_back(win);
                }
            }

            if (x->n_values_lists && x->n_from_clause) {
                AddError("SelectStmt: values_lists isn't compatible to from_clause");
                return nullptr;
            }

            if (!x->n_values_lists == !x->n_target_list) {
                AddError("SelectStmt: only one of values_lists and target_list should be specified");
                return nullptr;
            }

            if (x != value && x->n_sort_clause) {
                AddError("SelectStmt: sort_clause should be used only on top");
                return nullptr;
            }

            if (x != value && x->limit_option != PG_QUERY__LIMIT_OPTION__LIMIT_OPTION_DEFAULT) {
                AddError("SelectStmt: limit should be used only on top");
                return nullptr;
            }

            if (x->n_locking_clause) {
                AddError("SelectStmt: not supported locking_clause");
                return nullptr;
            }

            if (x->with_clause) {
                AddError("SelectStmt: not supported with_clause");
                return nullptr;
            }

            TVector<TAstNode*> res;
            ui32 i = 0;
            for (ui32 targetIndex = 0; targetIndex < x->n_target_list; ++targetIndex) {
                if (x->target_list[targetIndex]->node_case != PG_QUERY__NODE__NODE_RES_TARGET) {
                    AltNotImplemented(x, x->target_list[targetIndex]);
                    return nullptr;
                }

                auto r = x->target_list[targetIndex]->res_target;
                if (!r->val) {
                    AddError("SelectStmt: expected val");
                    return nullptr;
                }

                TExprSettings settings;
                settings.AllowColumns = true;
                settings.AllowAggregates = true;
                settings.AllowOver = true;
                settings.WindowItems = &windowItems;
                settings.Scope = "SELECT";
                auto x = ParseExpr(r->val, settings);
                if (!x) {
                    return nullptr;
                }

                bool isStar = false;
                if (r->val->node_case == PG_QUERY__NODE__NODE_COLUMN_REF) {
                    auto ref = r->val->column_ref;
                    for (ui32 fieldNo = 0; fieldNo < ref->n_fields; ++fieldNo) {
                        if (ref->fields[fieldNo]->node_case == PG_QUERY__NODE__NODE_A_STAR) {
                            isStar = true;
                            break;
                        }
                    }
                }

                TString name;
                if (!isStar) {
                    name = r->name;
                    if (name.empty()) {
                        if (r->val->node_case == PG_QUERY__NODE__NODE_COLUMN_REF) {
                            auto ref = r->val->column_ref;
                            auto field = ref->fields[ref->n_fields - 1];
                            if (field->node_case == PG_QUERY__NODE__NODE_STRING) {
                                name = field->string->str;
                            }
                        }
                    }

                    if (name.empty()) {
                        name = "column" + ToString(i++);
                    }
                }

                auto lambda = L(A("lambda"), QL(), x);
                auto columnName = QA(name);
                res.push_back(L(A("PgResultItem"), columnName, L(A("Void")), lambda));
            }

            TVector<TAstNode*> val;
            TVector<TAstNode*> valNames;
            val.push_back(A("AsList"));

            for (ui32 valueIndex = 0; valueIndex < x->n_values_lists; ++valueIndex) {
                TExprSettings settings;
                settings.AllowColumns = false;
                settings.Scope = "VALUES";

                if (x->values_lists[valueIndex]->node_case != PG_QUERY__NODE__NODE_LIST) {
                    AltNotImplemented(x, x->values_lists[valueIndex]);
                    return nullptr;
                }

                auto lst = x->values_lists[valueIndex]->list;
                TVector<TAstNode*> row;
                if (valueIndex == 0) {
                    for (ui32 item = 0; item < lst->n_items; ++item) {
                        valNames.push_back(QA("column" + ToString(i++)));
                    }
                } else {
                    if (lst->n_items != valNames.size()) {
                        AddError("SelectStmt: VALUES lists must all be the same length");
                        return nullptr;
                    }
                }

                for (ui32 item = 0; item < lst->n_items; ++item) {
                    auto cell = ParseExpr(lst->items[item], settings);
                    if (!cell) {
                        return nullptr;
                    }

                    row.push_back(cell);
                }

                val.push_back(QVL(row.data(), row.size()));
            }

            TVector<TAstNode*> setItemOptions;
            if (x->n_target_list) {
                setItemOptions.push_back(QL(QA("result"), QVL(res.data(), res.size())));
            } else {
                setItemOptions.push_back(QL(QA("values"), QVL(valNames.data(), valNames.size()), VL(val.data(), val.size())));
            }

            if (!fromList.empty()) {
                setItemOptions.push_back(QL(QA("from"), QVL(fromList.data(), fromList.size())));
                setItemOptions.push_back(QL(QA("join_ops"), QVL(joinOps.data(), joinOps.size())));
            }

            if (whereFilter) {
                auto lambda = L(A("lambda"), QL(), whereFilter);
                setItemOptions.push_back(QL(QA("where"), L(A("PgWhere"), L(A("Void")), lambda)));
            }

            if (groupBy) {
                setItemOptions.push_back(QL(QA("group_by"), groupBy));
            }

            if (windowItems.size()) {
                auto window = QVL(windowItems.data(), windowItems.size());
                setItemOptions.push_back(QL(QA("window"), window));
            }

            if (having) {
                auto lambda = L(A("lambda"), QL(), having);
                setItemOptions.push_back(QL(QA("having"), L(A("PgWhere"), L(A("Void")), lambda)));
            }

            auto setItem = L(A("PgSetItem"), QVL(setItemOptions.data(), setItemOptions.size()));
            setItemNodes.push_back(setItem);
        }

        if (value->n_distinct_clause) {
            AddError("SelectStmt: not supported distinct_clause");
            return nullptr;
        }

        if (value->into_clause) {
            AddError("SelectStmt: not supported into_clause");
            return nullptr;
        }

        TAstNode* sort = nullptr;
        if (value->n_sort_clause) {
            TVector<TAstNode*> sortItems;
            for (ui32 i = 0; i < value->n_sort_clause; ++i) {
                if (value->sort_clause[i]->node_case != PG_QUERY__NODE__NODE_SORT_BY) {
                    AltNotImplemented(value, value->sort_clause[i]);
                    return nullptr;
                }

                auto sort = SortBy(value->sort_clause[i]->sort_by);
                if (!sort) {
                    return nullptr;
                }

                sortItems.push_back(sort);
            }

            sort = QVL(sortItems.data(), sortItems.size());
        }

        if (value->n_locking_clause) {
            AddError("SelectStmt: not supported locking_clause");
            return nullptr;
        }

        if (value->with_clause) {
            AddError("SelectStmt: not supported with_clause");
            return nullptr;
        }

        TAstNode* limit = nullptr;
        TAstNode* offset = nullptr;
        if (value->limit_option != PG_QUERY__LIMIT_OPTION__LIMIT_OPTION_DEFAULT) {
            if (value->limit_option == PG_QUERY__LIMIT_OPTION__LIMIT_OPTION_COUNT) {
                if (!value->limit_count) {
                    AddError("Expected limit_count");
                    return nullptr;
                }

                TExprSettings settings;
                settings.AllowColumns = false;
                settings.Scope = "LIMIT";
                limit = ParseExpr(value->limit_count, settings);
                if (!limit) {
                    return nullptr;
                }

                if (value->limit_offset) {
                    settings.Scope = "OFFSET";
                    offset = ParseExpr(value->limit_offset, settings);
                    if (!offset) {
                        return nullptr;
                    }
                }
            } else {
                AddError(TStringBuilder() << "LimitOption unsupported value: " <<
                    protobuf_c_enum_descriptor_get_value(&pg_query__limit_option__descriptor, value->limit_option)->name);
                return nullptr;
            }
        }

        TVector<TAstNode*> selectOptions;

        selectOptions.push_back(QL(QA("set_items"), QVL(setItemNodes.data(), setItemNodes.size())));
        selectOptions.push_back(QL(QA("set_ops"), QVL(setOpsNodes.data(), setOpsNodes.size())));

        if (sort) {
            selectOptions.push_back(QL(QA("sort"), sort));
        }

        if (limit) {
            selectOptions.push_back(QL(QA("limit"), limit));
        }

        if (offset) {
            selectOptions.push_back(QL(QA("offset"), offset));
        }

        auto output = L(A("PgSelect"), QVL(selectOptions.data(), selectOptions.size()));

        if (inner) {
            return output;
        }

        auto resOptions = QL(QL(QA("type")));
        Statements.push_back(L(A("let"), A("output"), output));
        Statements.push_back(L(A("let"), A("result_sink"), L(A("DataSink"), QA(TString(NYql::ResultProviderName)))));
        Statements.push_back(L(A("let"), A("world"), L(A("Write!"),
            A("world"), A("result_sink"), L(A("Key")), A("output"), resOptions)));
        Statements.push_back(L(A("let"), A("world"), L(A("Commit!"),
            A("world"), A("result_sink"))));
        return Statements.back();
    }

    TFromDesc FromClause(const PgQuery__Node* node) {
        switch (node->node_case) {
        case PG_QUERY__NODE__NODE_RANGE_VAR:
            return RangeVar(node->range_var);
        case PG_QUERY__NODE__NODE_RANGE_SUBSELECT:
            return RangeSubselect(node->range_subselect);
        default:
            AltNotImplementedDesc(nullptr, node);
            return {};
        }
    }

    void AddFrom(const TFromDesc& p, TVector<TAstNode*>& fromList) {
        auto aliasNode = QA(std::get<1>(p));
        TVector<TAstNode*> colNamesNodes;
        for (const auto& c : std::get<2>(p)) {
            colNamesNodes.push_back(QA(c));
        }

        auto colNamesTuple = QVL(colNamesNodes.data(), colNamesNodes.size());
        if (std::get<3>(p)) {
            auto label = "read" + ToString(ReadIndex);
            Statements.push_back(L(A("let"), A(label), std::get<0>(p)));
            Statements.push_back(L(A("let"), A("world"), L(A("Left!"), A(label))));
            fromList.push_back(QL(L(A("Right!"), A(label)), aliasNode, colNamesTuple));
            ++ReadIndex;
        } else {
            fromList.push_back(QL(std::get<0>(p), aliasNode, colNamesTuple));
        }
    }

    bool ParseAlias(const PgQuery__Alias* alias, TString& res, TVector<TString>& colnames) {
        for (ui32 i = 0; i < alias->n_colnames; ++i) {
            if (alias->colnames[i]->node_case != PG_QUERY__NODE__NODE_STRING) {
                AltNotImplemented(alias, alias->colnames[i]);
                return false;
            }

            colnames.push_back(alias->colnames[i]->string->str);
        }

        res = alias->aliasname;
        return true;
    }

    TFromDesc RangeVar(const PgQuery__RangeVar* value) {
        if (strlen(value->catalogname) > 0) {
            AddError("catalogname is not supported");
            return {};
        }

        if (strlen(value->schemaname) == 0) {
            AddError("schemaname should be specified");
            return {};
        }

        if (strlen(value->relname) == 0) {
            AddError("relname should be specified");
            return {};
        }

        TString alias;
        TVector<TString> colnames;
        if (value->alias) {
            if (!ParseAlias(value->alias, alias, colnames)) {
                return {};
            }
        }

        auto p = Settings.ClusterMapping.FindPtr(value->schemaname);
        if (!p) {
            AddError(TStringBuilder() << "Unknown cluster: " << value->schemaname);
            return {};
        }

        auto source = L(A("DataSource"), QA(*p), QA(value->schemaname));
        return { L(A("Read!"), A("world"), source, L(A("Key"),
            QL(QA("table"), L(A("String"), QA(value->relname)))),
            L(A("Void")),
            QL()), alias, colnames, true };
    }

    TFromDesc RangeSubselect(const PgQuery__RangeSubselect* value) {
        if (value->lateral) {
            AddError("RangeSubselect: unsupported lateral");
            return {};
        }

        if (!value->alias) {
            AddError("RangeSubselect: expected alias");
            return {};
        }

        TString alias;
        TVector<TString> colnames;
        if (!ParseAlias(value->alias, alias, colnames)) {
            return {};
        }

        if (!value->subquery) {
            AddError("RangeSubselect: expected subquery");
            return {};
        }

        if (value->subquery->node_case != PG_QUERY__NODE__NODE_SELECT_STMT) {
            AltNotImplemented(value, value->subquery);
            return {};
        }

        return { SelectStmt(value->subquery->select_stmt, true), alias, colnames, false };
    }

    TAstNode* ParseExpr(const PgQuery__Node* node, const TExprSettings& settings) {
        switch (node->node_case) {
        case PG_QUERY__NODE__NODE_A_CONST: {
            return AConst(node->a_const);
        }
        case PG_QUERY__NODE__NODE_A_EXPR: {
            return AExpr(node->a_expr, settings);
        }
        case PG_QUERY__NODE__NODE_COLUMN_REF: {
            if (!settings.AllowColumns) {
                AddError(TStringBuilder() << "Columns are not allowed in: " << settings.Scope);
                return nullptr;
            }

            return ColumnRef(node->column_ref);
        }
        case PG_QUERY__NODE__NODE_TYPE_CAST: {
            return TypeCast(node->type_cast);
        }
        case PG_QUERY__NODE__NODE_BOOL_EXPR: {
            return BoolExpr(node->bool_expr, settings);
        }
        case PG_QUERY__NODE__NODE_FUNC_CALL: {
            return FuncCall(node->func_call, settings);
        }
        default:
            AltNotImplementedDesc(nullptr, node);
            return nullptr;
        }
    }

    TAstNode* AConst(const PgQuery__AConst* value) {
        auto node = value->val;
        switch (node->node_case) {
        case PG_QUERY__NODE__NODE_INTEGER: {
            return L(A("Just"), L(A("Int32"), QA(ToString(node->integer->ival))));
        }
        case PG_QUERY__NODE__NODE_FLOAT: {
            return L(A("Just"), L(A("Double"), QA(ToString(node->float_->str))));
        }
        case PG_QUERY__NODE__NODE_STRING: {
            return L(A("Just"), L(A("Utf8"), QA(ToString(node->string->str))));
        }
        case PG_QUERY__NODE__NODE_NULL: {
            return L(A("Null"));
        }
        default:
            AltNotImplemented(value, node);
            return nullptr;
        }
    }

    TAstNode* FuncCall(const PgQuery__FuncCall* value, const TExprSettings& settings) {
        if (value->n_agg_order) {
            AddError("FuncCall: unsupported agg_order");
            return nullptr;
        }

        if (value->agg_filter) {
            AddError("FuncCall: unsupported agg_filter");
            return nullptr;
        }

        if (value->agg_within_group) {
            AddError("FuncCall: unsupported agg_within_group");
            return nullptr;
        }

        if (value->agg_distinct) {
            AddError("FuncCall: unsupported agg_distinct");
            return nullptr;
        }

        if (value->func_variadic) {
            AddError("FuncCall: unsupported func_variadic");
            return nullptr;
        }

        TAstNode* window = nullptr;
        if (value->over) {
            if (!settings.AllowOver) {
                AddError(TStringBuilder() << "Over is not allowed in: " << settings.Scope);
                return nullptr;
            }

            if (strlen(value->over->name)) {
                window = QA(value->over->name);
            } else {
                auto index = settings.WindowItems->size();
                auto def = WindowDef(value->over);
                if (!def) {
                    return nullptr;
                }

                window = L(A("PgAnonWindow"), QA(ToString(index)));
                settings.WindowItems->push_back(def);
            }
        }

        TVector<TString> names;
        for (ui32 i = 0; i < value->n_funcname; ++i) {
            auto x = value->funcname[i];
            if (x->node_case != PG_QUERY__NODE__NODE_STRING) {
                AltNotImplemented(value, x);
                return nullptr;
            }

            names.push_back(to_lower(TString(x->string->str)));
        }

        if (names.empty()) {
            AddError("FuncCall: missing function name");
            return nullptr;
        }

        if (names.size() > 2) {
            AddError(TStringBuilder() << "FuncCall: too many name components:: " << names.size());
            return nullptr;
        }

        if (names.size() == 2 && names[0] != "pg_catalog") {
            AddError(TStringBuilder() << "FuncCall: expected pg_catalog, but got: " << names[0]);
            return nullptr;
        }

        auto name = names.back();
        bool isAggregateFunc = AggregateFuncs.count(name) > 0;
        if (isAggregateFunc && !settings.AllowAggregates) {
            AddError(TStringBuilder() << "Aggregate functions are not allowed in: " << settings.Scope);
            return nullptr;
        }

        TVector<TAstNode*> args;
        TString callable;
        if (window) {
            if (isAggregateFunc) {
                callable = "PgAggWindowCall";
            } else {
                callable = "PgWindowCall";
            }
        } else {
            if (isAggregateFunc) {
                callable = "PgAgg";
            } else {
                callable = "PgCall";
            }
        }

        args.push_back(A(callable));
        args.push_back(QA(name));
        if (window) {
            args.push_back(window);
        }

        if (value->agg_star) {
            if (name != "count") {
                AddError("FuncCall: * is expected only in count function");
                return nullptr;
            }
        } else {
            if (name == "count" && value->n_args == 0) {
                AddError("FuncCall: count(*) must be used to call a parameterless aggregate function");
                return nullptr;
            }

            bool hasError = false;
            for (ui32 i = 0; i < value->n_args; ++i) {
                auto x = value->args[i];
                auto arg = ParseExpr(x, settings);
                if (!arg) {
                    hasError = true;
                    continue;
                }

                args.push_back(arg);
            }

            if (hasError) {
                return nullptr;
            }
        }

        return VL(args.data(), args.size());
    }

    TAstNode* TypeCast(const PgQuery__TypeCast* value) {
        if (!value->arg) {
            AddError("Expected arg");
            return nullptr;
        }

        if (!value->type_name) {
            AddError("Expected type_name");
            return nullptr;
        }

        auto arg = value->arg;
        auto typeName = value->type_name;
        if (arg->node_case == PG_QUERY__NODE__NODE_A_CONST &&
            (arg->a_const->val->node_case == PG_QUERY__NODE__NODE_STRING ||
             arg->a_const->val->node_case == PG_QUERY__NODE__NODE_NULL) &&
            typeName->type_oid == 0 &&
            !typeName->setof &&
            !typeName->pct_type &&
            typeName->n_typmods == 0 &&
            typeName->n_array_bounds == 0 &&
            (typeName->n_names == 2 &&
            typeName->names[0]->node_case == PG_QUERY__NODE__NODE_STRING &&
            !strcmp(typeName->names[0]->string->str, "pg_catalog") || typeName->n_names == 1) &&
            typeName->names[typeName->n_names - 1]->node_case == PG_QUERY__NODE__NODE_STRING &&
            typeName->typemod == -1) {
            TStringBuf targetType = typeName->names[typeName->n_names - 1]->string->str;
            if (arg->a_const->val->node_case == PG_QUERY__NODE__NODE_STRING && targetType == "bool") {
                if (!strcmp(arg->a_const->val->string->str, "t")) {
                    return L(A("Just"), L(A("Bool"), QA("1")));
                } else if (!strcmp(arg->a_const->val->string->str, "f")) {
                    return L(A("Just"), L(A("Bool"), QA("0")));
                } else {
                    AddError(TStringBuilder() << "Unsupported boolean literal: " << arg->a_const->val->string->str);
                    return nullptr;
                }
            }

            if (arg->a_const->val->node_case == PG_QUERY__NODE__NODE_NULL) {
                TString yqlType;
                if (targetType == "bool") {
                    yqlType = "Bool";
                } else if (targetType == "int4") {
                    yqlType = "Int32";
                } else if (targetType == "float8") {
                    yqlType = "Double";
                } else if (targetType == "varchar") {
                    yqlType = "Utf8";
                }

                if (yqlType) {
                    return L(A("Nothing"), L(A("OptionalType"), L(A("DataType"), QA(yqlType))));
                }
            }
        }

        AddError("Unsupported form of type cast");
        return nullptr;
    }

    TAstNode* BoolExpr(const PgQuery__BoolExpr* value, const TExprSettings& settings) {
        if (value->xpr) {
            AddError("BoolExpr: not supported xpr");
            return nullptr;
        }

        switch (value->boolop) {
        case PG_QUERY__BOOL_EXPR_TYPE__AND_EXPR: {
            if (value->n_args != 2) {
                AddError("Expected 2 args for AND");
                return nullptr;
            }

            auto lhs = ParseExpr(value->args[0], settings);
            auto rhs = ParseExpr(value->args[1], settings);
            if (!lhs || !rhs) {
                return nullptr;
            }

            return L(A("And"), lhs, rhs);
        }
        case PG_QUERY__BOOL_EXPR_TYPE__OR_EXPR: {
            if (value->n_args != 2) {
                AddError("Expected 2 args for OR");
                return nullptr;
            }

            auto lhs = ParseExpr(value->args[0], settings);
            auto rhs = ParseExpr(value->args[1], settings);
            if (!lhs || !rhs) {
                return nullptr;
            }

            return L(A("Or"), lhs, rhs);
        }
        case PG_QUERY__BOOL_EXPR_TYPE__NOT_EXPR: {
            if (value->n_args != 1) {
                AddError("Expected 1 arg for NOT");
                return nullptr;
            }

            auto arg = ParseExpr(value->args[0], settings);
            if (!arg) {
                return nullptr;
            }

            return L(A("Not"), arg);
        }
        default:
            AddError(TStringBuilder() << "BoolExprType unsupported value: " <<
                protobuf_c_enum_descriptor_get_value(&pg_query__bool_expr_type__descriptor, value->boolop)->name);
            return nullptr;
        }
    }

    TAstNode* WindowDef(const PgQuery__WindowDef* value) {
        auto name = QA(value->name);
        auto refName = QA(value->refname);
        TVector<TAstNode*> sortItems;
        for (ui32 i = 0; i < value->n_order_clause; ++i) {
            if (value->order_clause[i]->node_case != PG_QUERY__NODE__NODE_SORT_BY) {
                AltNotImplemented(value, value->order_clause[i]);
                return nullptr;
            }

            auto sort = SortBy(value->order_clause[i]->sort_by);
            if (!sort) {
                return nullptr;
            }

            sortItems.push_back(sort);
        }

        auto sort = QVL(sortItems.data(), sortItems.size());
        TVector<TAstNode*> groupByItems;
        for (ui32 i = 0; i < value->n_partition_clause; ++i) {
            if (value->partition_clause[i]->node_case != PG_QUERY__NODE__NODE_COLUMN_REF) {
                AltNotImplemented(value, value->partition_clause[i]);
                return nullptr;
            }

            auto ref = ColumnRef(value->partition_clause[i]->column_ref);
            if (!ref) {
                return nullptr;
            }

            auto lambda = L(A("lambda"), QL(), ref);
            groupByItems.push_back(L(A("PgGroup"), L(A("Void")), lambda));
        }

        auto group = QVL(groupByItems.data(), groupByItems.size());
        TVector<TAstNode*> optionItems;
        if (value->frame_options & PG_FRAMEOPTION_NONDEFAULT) {
            TString exclude;
            if (value->frame_options & PG_FRAMEOPTION_EXCLUDE_CURRENT_ROW) {
                if (exclude) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                exclude = "c";
            }

            if (value->frame_options & PG_FRAMEOPTION_EXCLUDE_GROUP) {
                if (exclude) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                exclude = "cp";
            }

            if (value->frame_options & PG_FRAMEOPTION_EXCLUDE_TIES) {
                if (exclude) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                exclude = "p";
            }

            if (exclude) {
                optionItems.push_back(QL(QA("exclude"), QA(exclude)));
            }

            TString type;
            if (value->frame_options & PG_FRAMEOPTION_RANGE) {
                if (type) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                type = "range";
            }

            if (value->frame_options & PG_FRAMEOPTION_ROWS) {
                if (type) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                type = "rows";
            }

            if (value->frame_options & PG_FRAMEOPTION_GROUPS) {
                if (type) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                type = "groups";
            }

            if (!type) {
                AddError("Wrong frame options");
                return nullptr;
            }

            TString from;
            if (value->frame_options & PG_FRAMEOPTION_START_UNBOUNDED_PRECEDING) {
                if (from) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                from = "up";
            }

            if (value->frame_options & PG_FRAMEOPTION_START_OFFSET_PRECEDING) {
                if (from) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                from = "p";
                auto offset = ConvertFrameOffset(value->start_offset);
                if (!offset) {
                    return nullptr;
                }

                optionItems.push_back(QL(QA("from_value"), offset));
            }

            if (value->frame_options & PG_FRAMEOPTION_START_CURRENT_ROW) {
                if (from) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                from = "c";
            }

            if (value->frame_options & PG_FRAMEOPTION_START_OFFSET_FOLLOWING) {
                if (from) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                from = "f";
                auto offset = ConvertFrameOffset(value->start_offset);
                if (!offset) {
                    return nullptr;
                }

                optionItems.push_back(QL(QA("from_value"), offset));
            }

            if (value->frame_options & PG_FRAMEOPTION_START_UNBOUNDED_FOLLOWING) {
                AddError("Wrong frame options");
                return nullptr;
            }

            if (!from) {
                AddError("Wrong frame options");
                return nullptr;
            }

            TString to;
            if (value->frame_options & PG_FRAMEOPTION_END_UNBOUNDED_PRECEDING) {
                AddError("Wrong frame options");
                return nullptr;
            }

            if (value->frame_options & PG_FRAMEOPTION_END_OFFSET_PRECEDING) {
                if (to) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                to = "p";
                auto offset = ConvertFrameOffset(value->end_offset);
                if (!offset) {
                    return nullptr;
                }

                optionItems.push_back(QL(QA("to_value"), offset));
            }

            if (value->frame_options & PG_FRAMEOPTION_END_CURRENT_ROW) {
                if (to) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                to = "c";
            }

            if (value->frame_options & PG_FRAMEOPTION_END_OFFSET_FOLLOWING) {
                if (to) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                to = "f";
                auto offset = ConvertFrameOffset(value->end_offset);
                if (!offset) {
                    return nullptr;
                }

                optionItems.push_back(QL(QA("to_value"), offset));
            }

            if (value->frame_options & PG_FRAMEOPTION_END_UNBOUNDED_FOLLOWING) {
                if (to) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                to = "uf";
            }

            if (!to) {
                AddError("Wrong frame options");
                return nullptr;
            }

            optionItems.push_back(QL(QA("type"), QA(type)));
            optionItems.push_back(QL(QA("from"), QA(from)));
            optionItems.push_back(QL(QA("to"), QA(to)));
        }

        auto options = QVL(optionItems.data(), optionItems.size());
        return L(A("PgWindow"), name, refName, group, sort, options);
    }

    TAstNode* ConvertFrameOffset(const PgQuery__Node* off) {
        if (off->node_case == PG_QUERY__NODE__NODE_A_CONST
            && off->a_const->val->node_case == PG_QUERY__NODE__NODE_INTEGER) {
            return L(A("Int32"), QA(ToString(off->a_const->val->integer->ival)));
        } else {
            TExprSettings settings;
            settings.AllowColumns = false;
            settings.Scope = "FRAME";
            auto offset = ParseExpr(off, settings);
            if (!offset) {
                return nullptr;
            }

            return L(A("EvaluateExpr"), L(A("Unwrap"), offset, L(A("String"), QA("Frame offset must be non-null"))));
        }
    }

    TAstNode* SortBy(const PgQuery__SortBy* value) {
        bool asc = true;
        switch (value->sortby_dir) {
        case PG_QUERY__SORT_BY_DIR__SORTBY_DEFAULT:
            break;
        case PG_QUERY__SORT_BY_DIR__SORTBY_ASC:
            break;
        case PG_QUERY__SORT_BY_DIR__SORTBY_DESC:
            asc = false;
            break;
        default:
            AddError(TStringBuilder() << "sortby_dir unsupported value: " <<
                protobuf_c_enum_descriptor_get_value(&pg_query__sort_by_dir__descriptor, value->sortby_dir)->name);
            return nullptr;
        }

        if (value->sortby_nulls != PG_QUERY__SORT_BY_NULLS__SORTBY_NULLS_DEFAULT) {
            AddError(TStringBuilder() << "sortby_nulls unsupported value: " <<
                protobuf_c_enum_descriptor_get_value(&pg_query__sort_by_nulls__descriptor, value->sortby_nulls)->name);
            return nullptr;
        }

        if (value->n_use_op) {
            AddError("Unsupported operators in sort_by");
            return nullptr;
        }

        TExprSettings settings;
        settings.AllowColumns = true;
        settings.Scope = "ORDER BY";
        auto expr = ParseExpr(value->node, settings);
        if (!expr) {
            return nullptr;
        }

        auto lambda = L(A("lambda"), QL(), expr);
        return L(A("PgSort"), L(A("Void")), lambda, QA(asc ? "asc" : "desc"));
    }

    TAstNode* ColumnRef(const PgQuery__ColumnRef* value) {
        if (value->n_fields == 0) {
            AddError("No fields");
            return nullptr;
        }

        if (value->n_fields > 2) {
            AddError("Too many fields");
            return nullptr;
        }

        bool isStar = false;
        TVector<TString> fields;
        for (ui32 i = 0; i < value->n_fields; ++i) {
            auto x = value->fields[i];
            if (isStar) {
                AddError("Star is already defined");
                return nullptr;
            }

            if (x->node_case == PG_QUERY__NODE__NODE_STRING) {
                fields.push_back(x->string->str);
            } else if (x->node_case == PG_QUERY__NODE__NODE_A_STAR) {
                isStar = true;
            } else {
                AltNotImplemented(value, x);
                return nullptr;
            }
        }

        if (isStar) {
            if (fields.size() == 0) {
                return L(A("PgStar"));
            } else {
                return L(A("PgQualifiedStar"), QA(fields[0]));
            }
        } else if (fields.size() == 1) {
            return L(A("PgColumnRef"), QA(fields[0]));
        } else {
            return L(A("PgColumnRef"), QA(fields[0]), QA(fields[1]));
        }
    }

    TAstNode* AExpr(const PgQuery__AExpr* value, const TExprSettings& settings) {
        if (value->kind != PG_QUERY__A__EXPR__KIND__AEXPR_OP) {
            AddError(TStringBuilder() << "A_Expr_Kind unsupported value: " <<
                protobuf_c_enum_descriptor_get_value(&pg_query__a__expr__kind__descriptor, value->kind)->name);
            return nullptr;
        }

        if (value->n_name != 1) {
            AddError(TStringBuilder() << "Unsupported count of names: " << value->n_name);
            return nullptr;
        }

        auto nameNode = value->name[0];
        if (nameNode->node_case != PG_QUERY__NODE__NODE_STRING) {
            AltNotImplemented(value, nameNode);
            return nullptr;
        }

        auto op = nameNode->string->str;
        if (!value->rexpr) {
            AddError("Missing operands");
            return nullptr;
        }

        if (!value->lexpr) {
            auto opIt = UnaryOpTranslation.find(op);
            if (opIt == UnaryOpTranslation.end()) {
                AddError(TStringBuilder() << "Unsupported unary op: " << op);
                return nullptr;
            }

            auto rhs = ParseExpr(value->rexpr, settings);
            if (!rhs) {
                return nullptr;
            }

            return L(A(opIt->second), rhs);
        } else {
            auto opIt = BinaryOpTranslation.find(op);
            if (opIt == BinaryOpTranslation.end()) {
                AddError(TStringBuilder() << "Unsupported binary op: " << op);
                return nullptr;
            }

            auto lhs = ParseExpr(value->lexpr, settings);
            auto rhs = ParseExpr(value->rexpr, settings);
            if (!lhs || !rhs) {
                return nullptr;
            }

            return L(A(opIt->second), lhs, rhs);
        }
    }

    template <typename T>
    void AltNotImplemented(const T* outer, const PgQuery__Node* node) {
        AltNotImplementedDesc(((const ProtobufCMessage*)outer)->descriptor, node);
    }

    void AltNotImplementedDesc(const ProtobufCMessageDescriptor* outer, const PgQuery__Node* node) {
        TStringBuilder b;
        if (outer) {
            b << outer->name << ": ";
        }

        b << "alternative is not implemented yet : " <<
            protobuf_c_message_descriptor_get_field(node->base.descriptor, node->node_case)->name;
        AddError(b);
    }

    TAstNode* VL(TAstNode** nodes, ui32 size, TPosition pos = {}) {
        return TAstNode::NewList(pos, nodes, size, *AstParseResult.Pool);
    }

    TAstNode* QVL(TAstNode** nodes, ui32 size, TPosition pos = {}) {
        return Q(VL(nodes, size, pos));
    }

    TAstNode* A(const TString& str, TPosition pos = {}) {
        return TAstNode::NewAtom(pos, str, *AstParseResult.Pool);
    }

    TAstNode* Q(TAstNode* node, TPosition pos = {}) {
        return L(A("quote"), node, pos);
    }

    TAstNode* QA(const TString& str, TPosition pos = {}) {
        return Q(A(str, pos));
    }

    template <typename... TNodes>
    TAstNode* L(TNodes... nodes) {
        TLState state;
        LImpl(state, nodes...);
        return TAstNode::NewList(state.Position, state.Nodes.data(), state.Nodes.size(), *AstParseResult.Pool);
    }

    template <typename... TNodes>
    TAstNode* QL(TNodes... nodes) {
        return Q(L(nodes...));
    }

private:
    void AddError(const TString& value) {
        AstParseResult.Issues.AddIssue(TIssue(value));
    }

    struct TLState {
        TPosition Position;
        TVector<TAstNode*> Nodes;
    };

    template <typename... TNodes>
    void LImpl(TLState& state, TNodes... nodes);

    void LImpl(TLState& state) {
        Y_UNUSED(state);
    }

    void LImpl(TLState& state, TPosition pos) {
        state.Position = pos;
    }

    void LImpl(TLState& state, TAstNode* node) {
        state.Nodes.push_back(node);
    }

    template <typename T, typename... TNodes>
    void LImpl(TLState& state, T node, TNodes... nodes) {
        state.Nodes.push_back(node);
        LImpl(state, nodes...);
    }

private:
    TAstParseResult& AstParseResult;
    NSQLTranslation::TTranslationSettings Settings;
    bool DqEngineEnabled = false;
    bool DqEngineForce = false;
    TVector<TAstNode*> Statements;
    ui32 ReadIndex = 0;
};

THashMap<TString, TString> TConverter::BinaryOpTranslation = {
    {"+","+"},
    {"-","-"},
    {"||","Concat"},
    {"=","=="},
    {"<","<"},
    {">",">"},
    {"<=","<="},
    {">=",">="},
    {"<>","!="},
};

THashMap<TString, TString> TConverter::UnaryOpTranslation = {
    {"+","Plus"},
    {"-","Minus"},
};

THashSet<TString> TConverter::AggregateFuncs = {
    "count",
    "min",
    "max",
    "sum"
};

NYql::TAstParseResult PGToYql(const TString& query, const NSQLTranslation::TTranslationSettings& settings) {
    NYql::TAstParseResult result;
    TConverter converter(result, settings);
    NYql::PGParse(query, converter);
    return result;
}

}  // NSQLTranslationPG
