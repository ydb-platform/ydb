#include <ydb/library/yql/sql/pg_sql.h>
#include <ydb/library/yql/parser/pg_wrapper/parser.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/core/yql_callable_names.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/generic/stack.h>
#include <util/generic/hash_set.h>

#ifdef _WIN32
#define __restrict
#endif

#define TypeName PG_TypeName
#define SortBy PG_SortBy
#undef SIZEOF_SIZE_T
extern "C" {
#include "postgres.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "nodes/value.h"
#undef Min
#undef Max
#undef TypeName
#undef SortBy
}

namespace NSQLTranslationPG {

using namespace NYql;

template <typename T>
const T* CastNode(const void* nodeptr, int tag) {
    Y_ENSURE(nodeTag(nodeptr) == tag);
    return static_cast<const T*>(nodeptr);
}

int NodeTag(const Node* node) {
    return nodeTag(node);
}

int NodeTag(const Value& node) {
    return node.type;
}

int IntVal(const Value& node) {
    Y_ENSURE(node.type == T_Integer);
    return intVal(&node);
}

double FloatVal(const Value& node) {
    Y_ENSURE(node.type == T_Float);
    return floatVal(&node);
}

const char* StrFloatVal(const Value& node) {
    Y_ENSURE(node.type == T_Float);
    return strVal(&node);
}

const char* StrVal(const Value& node) {
    Y_ENSURE(node.type == T_String);
    return strVal(&node);
}

int IntVal(const Node* node) {
    Y_ENSURE(node->type == T_Integer);
    return intVal((const Value*)node);
}

double FloatVal(const Node* node) {
    Y_ENSURE(node->type == T_Float);
    return floatVal((const Value*)node);
}

const char* StrFloatVal(const Node* node) {
    Y_ENSURE(node->type == T_Float);
    return strVal((const Value*)node);
}

const char* StrVal(const Node* node) {
    Y_ENSURE(node->type == T_String);
    return strVal((const Value*)node);
}

int ListLength(const List* list) {
    return list_length(list);
}

int StrLength(const char* s) {
    return s ? strlen(s) : 0;
}

int StrCompare(const char* s1, const char* s2) {
    return strcmp(s1 ? s1 : "", s2 ? s2 : "");
}

#define CAST_NODE(nodeType, nodeptr) CastNode<nodeType>(nodeptr, T_##nodeType)
#define CAST_NODE_EXT(nodeType, tag, nodeptr) CastNode<nodeType>(nodeptr, tag)
#define LIST_CAST_NTH(nodeType, list, index) CAST_NODE(nodeType, list_nth(list, i))
#define LIST_CAST_EXT_NTH(nodeType, tag, list, index) CAST_NODE_EXT(nodeType, tag, list_nth(list, i))

const Node* ListNodeNth(const List* list, int index) {
    return static_cast<const Node*>(list_nth(list, index));
}

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

    void OnResult(const List* raw) {
        AstParseResult.Pool = std::make_unique<TMemoryPool>(4096);
        AstParseResult.Root = ParseResult(raw);
    }

    void OnError(const TIssue& issue) {
        AstParseResult.Issues.AddIssue(issue);
    }

    TAstNode* ParseResult(const List* raw) {
        auto configSource = L(A("DataSource"), QA(TString(NYql::ConfigProviderName)));
        Statements.push_back(L(A("let"), A("world"), L(A(TString(NYql::ConfigureName)), A("world"), configSource,
            QA("OrderedColumns"))));
        if (Settings.PgTypes) {
            Statements.push_back(L(A("let"), A("world"), L(A(TString(NYql::ConfigureName)), A("world"), configSource,
                QA("PgTypes"))));
        }

        if (DqEngineEnabled) {
            Statements.push_back(L(A("let"), A("world"), L(A(TString(NYql::ConfigureName)), A("world"), configSource,
                QA("DqEngine"), QA(DqEngineForce ? "force" : "auto"))));
        }

        for (int i = 0; i < ListLength(raw); ++i) {
           if (!ParseRawStmt(LIST_CAST_NTH(RawStmt, raw, i))) {
               return nullptr;
           }
        }

        Statements.push_back(L(A("return"), A("world")));
        return VL(Statements.data(), Statements.size());
    }

    [[nodiscard]]
    bool ParseRawStmt(const RawStmt* value) {
        auto node = value->stmt;
        switch (NodeTag(node)) {
        case T_SelectStmt: {
            return ParseSelectStmt(CAST_NODE(SelectStmt, node), false) != nullptr;
        }
        default:
            NodeNotImplemented(value, node);
            return false;
        }
    }

    using TTraverseSelectStack = TStack<std::pair<const SelectStmt*, bool>>;
    using TTraverseNodeStack = TStack<std::pair<const Node*, bool>>;

    [[nodiscard]]
    TAstNode* ParseSelectStmt(const SelectStmt* value, bool inner) {
        TTraverseSelectStack traverseSelectStack;
        traverseSelectStack.push({ value, false });

        TVector<const SelectStmt*> setItems;
        TVector<TAstNode*> setOpsNodes;

        while (!traverseSelectStack.empty()) {
            auto& top = traverseSelectStack.top();
            if (top.first->op == SETOP_NONE) {
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
                    case SETOP_UNION:
                        op = "union"; break;
                    case SETOP_INTERSECT:
                        op = "intersect"; break;
                    case SETOP_EXCEPT:
                        op = "except"; break;
                    default:
                        AddError(TStringBuilder() << "SetOperation unsupported value: " << (int)top.first->op);
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
            if (x->distinctClause) {
                AddError("SelectStmt: not supported distinctClause");
                return nullptr;
            }

            if (x->intoClause) {
                AddError("SelectStmt: not supported intoClause");
                return nullptr;
            }

            TVector<TAstNode*> fromList;
            TVector<TAstNode*> joinOps;
            for (int i = 0; i < ListLength(x->fromClause); ++i) {
                auto node = ListNodeNth(x->fromClause, i);
                if (NodeTag(node) != T_JoinExpr) {
                    auto p = ParseFromClause(node);
                    if (!std::get<0>(p)) {
                        return nullptr;
                    }

                    AddFrom(p, fromList);
                    joinOps.push_back(QL());
                } else {
                    TTraverseNodeStack traverseNodeStack;
                    traverseNodeStack.push({ node, false });
                    TVector<TAstNode*> oneJoinGroup;

                    while (!traverseNodeStack.empty()) {
                        auto& top = traverseNodeStack.top();
                        if (NodeTag(top.first) != T_JoinExpr) {
                            // leaf
                            auto p = ParseFromClause(top.first);
                            if (!std::get<0>(p)) {
                                return nullptr;
                            }

                            AddFrom(p, fromList);
                            traverseNodeStack.pop();
                        } else {
                            auto join = CAST_NODE(JoinExpr, top.first);
                            if (!join->larg || !join->rarg) {
                                AddError("JoinExpr: expected larg and rarg");
                                return nullptr;
                            }

                            if (join->alias) {
                                AddError("JoinExpr: unsupported alias");
                                return nullptr;
                            }

                            if (join->isNatural) {
                                AddError("JoinExpr: unsupported isNatural");
                                return nullptr;
                            }

                            if (ListLength(join->usingClause) > 0) {
                                AddError("JoinExpr: unsupported using");
                                return nullptr;
                            }

                            if (!top.second) {
                                traverseNodeStack.push({ join->rarg, false });
                                traverseNodeStack.push({ join->larg, false });
                                top.second = true;
                            } else {
                                TString op;
                                switch (join->jointype) {
                                case JOIN_INNER:
                                    op = join->quals ? "inner" : "cross"; break;
                                case JOIN_LEFT:
                                    op = "left"; break;
                                case JOIN_FULL:
                                    op = "full"; break;
                                case JOIN_RIGHT:
                                    op = "right"; break;
                                default:
                                    AddError(TStringBuilder() << "jointype unsupported value: " << (int)join->jointype);
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
            if (x->whereClause) {
                TExprSettings settings;
                settings.AllowColumns = true;
                settings.Scope = "WHERE";
                whereFilter = ParseExpr(x->whereClause, settings);
                if (!whereFilter) {
                    return nullptr;
                }
            }

            TAstNode* groupBy = nullptr;
            if (ListLength(x->groupClause) > 0) {
                TVector<TAstNode*> groupByItems;
                for (int i = 0; i < ListLength(x->groupClause); ++i) {
                    auto node = ListNodeNth(x->groupClause, i);
                    if (NodeTag(node) != T_ColumnRef) {
                        NodeNotImplemented(x, node);
                        return nullptr;
                    }

                    auto ref = ParseColumnRef(CAST_NODE(ColumnRef, node));
                    if (!ref) {
                        return nullptr;
                    }

                    auto lambda = L(A("lambda"), QL(), ref);
                    groupByItems.push_back(L(A("PgGroup"), L(A("Void")), lambda));
                }

                groupBy = QVL(groupByItems.data(), groupByItems.size());
            }

            TAstNode* having = nullptr;
            if (x->havingClause) {
                TExprSettings settings;
                settings.AllowColumns = true;
                settings.Scope = "HAVING";
                settings.AllowAggregates = true;
                having = ParseExpr(x->havingClause, settings);
                if (!having) {
                    return nullptr;
                }
            }

            TVector<TAstNode*> windowItems;
            if (ListLength(x->windowClause) > 0) {
                for (int i = 0; i < ListLength(x->windowClause); ++i) {
                    auto node = ListNodeNth(x->windowClause, i);
                    if (NodeTag(node) != T_WindowDef) {
                        NodeNotImplemented(x, node);
                        return nullptr;
                    }

                    auto win = ParseWindowDef(CAST_NODE(WindowDef, node));
                    if (!win) {
                        return nullptr;
                    }

                    windowItems.push_back(win);
                }
            }

            if (ListLength(x->valuesLists) && ListLength(x->fromClause)) {
                AddError("SelectStmt: values_lists isn't compatible to from_clause");
                return nullptr;
            }

            if (!ListLength(x->valuesLists) == !ListLength(x->targetList)) {
                AddError("SelectStmt: only one of values_lists and target_list should be specified");
                return nullptr;
            }

            if (x != value && ListLength(x->sortClause) > 0) {
                AddError("SelectStmt: sortClause should be used only on top");
                return nullptr;
            }

            if (x != value) {
                if (x->limitOption == LIMIT_OPTION_COUNT || x->limitOption == LIMIT_OPTION_DEFAULT) {
                    if (value->limitCount || value->limitOffset) {
                        AddError("SelectStmt: limit should be used only on top");
                        return nullptr;
                    }
                } else {
                    AddError(TStringBuilder() << "LimitOption unsupported value: " << (int)x->limitOption);
                    return nullptr;
                }
            }

            if (ListLength(x->lockingClause) > 0) {
                AddError("SelectStmt: not supported lockingClause");
                return nullptr;
            }

            if (x->withClause) {
                AddError("SelectStmt: not supported withClause");
                return nullptr;
            }

            TVector<TAstNode*> res;
            ui32 i = 0;
            for (int targetIndex = 0; targetIndex < ListLength(x->targetList); ++targetIndex) {
                auto node = ListNodeNth(x->targetList, targetIndex);
                if (NodeTag(node) != T_ResTarget) {
                    NodeNotImplemented(x, node);
                    return nullptr;
                }

                auto r = CAST_NODE(ResTarget, node);
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
                if (NodeTag(r->val) == T_ColumnRef) {
                    auto ref = CAST_NODE(ColumnRef, r->val);
                    for (int fieldNo = 0; fieldNo < ListLength(ref->fields); ++fieldNo) {
                        if (NodeTag(ListNodeNth(ref->fields, fieldNo)) == T_A_Star) {
                            isStar = true;
                            break;
                        }
                    }
                }

                TString name;
                if (!isStar) {
                    name = r->name;
                    if (name.empty()) {
                        if (NodeTag(r->val) == T_ColumnRef) {
                            auto ref = CAST_NODE(ColumnRef, r->val);
                            auto field = ListNodeNth(ref->fields, ListLength(ref->fields) - 1);
                            if (NodeTag(field) == T_String) {
                                name = StrVal(field);
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

            for (int valueIndex = 0; valueIndex < ListLength(x->valuesLists); ++valueIndex) {
                TExprSettings settings;
                settings.AllowColumns = false;
                settings.Scope = "VALUES";

                auto node = ListNodeNth(x->valuesLists, valueIndex);
                if (NodeTag(node) != T_List) {
                    NodeNotImplemented(x, node);
                    return nullptr;
                }

                auto lst = CAST_NODE(List, node);
                TVector<TAstNode*> row;
                if (valueIndex == 0) {
                    for (int item = 0; item < ListLength(lst); ++item) {
                        valNames.push_back(QA("column" + ToString(i++)));
                    }
                } else {
                    if (ListLength(lst) != (int)valNames.size()) {
                        AddError("SelectStmt: VALUES lists must all be the same length");
                        return nullptr;
                    }
                }

                for (int item = 0; item < ListLength(lst); ++item) {
                    auto cell = ParseExpr(ListNodeNth(lst, item), settings);
                    if (!cell) {
                        return nullptr;
                    }

                    row.push_back(cell);
                }

                val.push_back(QVL(row.data(), row.size()));
            }

            TVector<TAstNode*> setItemOptions;
            if (ListLength(x->targetList) > 0) {
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

        if (value->distinctClause) {
            AddError("SelectStmt: not supported distinctClause");
            return nullptr;
        }

        if (value->intoClause) {
            AddError("SelectStmt: not supported intoClause");
            return nullptr;
        }

        TAstNode* sort = nullptr;
        if (ListLength(value->sortClause) > 0) {
            TVector<TAstNode*> sortItems;
            for (int i = 0; i < ListLength(value->sortClause); ++i) {
                auto node = ListNodeNth(value->sortClause, i);
                if (NodeTag(node) != T_SortBy) {
                    NodeNotImplemented(value, node);
                    return nullptr;
                }

                auto sort = ParseSortBy(CAST_NODE_EXT(PG_SortBy, T_SortBy, node));
                if (!sort) {
                    return nullptr;
                }

                sortItems.push_back(sort);
            }

            sort = QVL(sortItems.data(), sortItems.size());
        }

        if (ListLength(value->lockingClause) > 0) {
            AddError("SelectStmt: not supported lockingClause");
            return nullptr;
        }

        if (value->withClause) {
            AddError("SelectStmt: not supported withClause");
            return nullptr;
        }

        TAstNode* limit = nullptr;
        TAstNode* offset = nullptr;
        if (value->limitOption == LIMIT_OPTION_COUNT || value->limitOption == LIMIT_OPTION_DEFAULT) {
            if (value->limitCount) {
                TExprSettings settings;
                settings.AllowColumns = false;
                settings.Scope = "LIMIT";
                limit = ParseExpr(value->limitCount, settings);
                if (!limit) {
                    return nullptr;
                }
            }

            if (value->limitOffset) {
                TExprSettings settings;
                settings.AllowColumns = false;
                settings.Scope = "OFFSET";
                offset = ParseExpr(value->limitOffset, settings);
                if (!offset) {
                    return nullptr;
                }
            }
        } else {
            AddError(TStringBuilder() << "LimitOption unsupported value: " << (int)value->limitOption);
            return nullptr;
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

        auto resOptions = QL(QL(QA("type")), QL(QA("autoref")));
        Statements.push_back(L(A("let"), A("output"), output));
        Statements.push_back(L(A("let"), A("result_sink"), L(A("DataSink"), QA(TString(NYql::ResultProviderName)))));
        Statements.push_back(L(A("let"), A("world"), L(A("Write!"),
            A("world"), A("result_sink"), L(A("Key")), A("output"), resOptions)));
        Statements.push_back(L(A("let"), A("world"), L(A("Commit!"),
            A("world"), A("result_sink"))));
        return Statements.back();
    }

    TFromDesc ParseFromClause(const Node* node) {
        switch (NodeTag(node)) {
        case T_RangeVar:
            return ParseRangeVar(CAST_NODE(RangeVar, node));
        case T_RangeSubselect:
            return ParseRangeSubselect(CAST_NODE(RangeSubselect, node));
        default:
            NodeNotImplementedImpl<SelectStmt>(node);
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

    bool ParseAlias(const Alias* alias, TString& res, TVector<TString>& colnames) {
        for (int i = 0; i < ListLength(alias->colnames); ++i) {
            auto node = ListNodeNth(alias->colnames, i);
            if (NodeTag(node) != T_String) {
                NodeNotImplemented(alias, node);
                return false;
            }

            colnames.push_back(StrVal(node));
        }

        res = alias->aliasname;
        return true;
    }

    TFromDesc ParseRangeVar(const RangeVar* value) {
        if (StrLength(value->catalogname) > 0) {
            AddError("catalogname is not supported");
            return {};
        }

        if (StrLength(value->schemaname) == 0) {
            AddError("schemaname should be specified");
            return {};
        }

        if (StrLength(value->relname) == 0) {
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

    TFromDesc ParseRangeSubselect(const RangeSubselect* value) {
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

        if (NodeTag(value->subquery) != T_SelectStmt) {
            NodeNotImplemented(value, value->subquery);
            return {};
        }

        return { ParseSelectStmt(CAST_NODE(SelectStmt, value->subquery), true), alias, colnames, false };
    }

    TAstNode* ParseExpr(const Node* node, const TExprSettings& settings) {
        switch (NodeTag(node)) {
        case T_A_Const: {
            return ParseAConst(CAST_NODE(A_Const, node));
        }
        case T_A_Expr: {
            return ParseAExpr(CAST_NODE(A_Expr, node), settings);
        }
        case T_ColumnRef: {
            if (!settings.AllowColumns) {
                AddError(TStringBuilder() << "Columns are not allowed in: " << settings.Scope);
                return nullptr;
            }

            return ParseColumnRef(CAST_NODE(ColumnRef, node));
        }
        case T_TypeCast: {
            return ParseTypeCast(CAST_NODE(TypeCast, node), settings);
        }
        case T_BoolExpr: {
            return ParseBoolExpr(CAST_NODE(BoolExpr, node), settings);
        }
        case T_FuncCall: {
            return ParseFuncCall(CAST_NODE(FuncCall, node), settings);
        }
        default:
            NodeNotImplemented(node);
            return nullptr;
        }
    }

    TAstNode* ParseAConst(const A_Const* value) {
        const auto& val = value->val;
        switch (NodeTag(val)) {
        case T_Integer: {
            return Settings.PgTypes ?
                L(A("PgConst"), QA("int4"), QA(ToString(IntVal(val)))) :
                L(A("Just"), L(A("Int32"), QA(ToString(IntVal(val)))));
        }
        case T_Float: {
            return Settings.PgTypes ?
                L(A("PgConst"), QA("float8"), QA(ToString(StrFloatVal(val)))) :
                L(A("Just"), L(A("Double"), QA(ToString(StrFloatVal(val)))));
        }
        case T_String: {
            return Settings.PgTypes ?
                L(A("PgConst"), QA("text"), QA(ToString(StrVal(val)))) :
                L(A("Just"), L(A("Utf8"), QA(ToString(StrVal(val)))));
        }
        case T_Null: {
            return L(A("Null"));
        }
        default:
            ValueNotImplemented(value, val);
            return nullptr;
        }
    }

    TAstNode* ParseFuncCall(const FuncCall* value, const TExprSettings& settings) {
        if (ListLength(value->agg_order) > 0) {
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

            if (StrLength(value->over->name)) {
                window = QA(value->over->name);
            } else {
                auto index = settings.WindowItems->size();
                auto def = ParseWindowDef(value->over);
                if (!def) {
                    return nullptr;
                }

                window = L(A("PgAnonWindow"), QA(ToString(index)));
                settings.WindowItems->push_back(def);
            }
        }

        TVector<TString> names;
        for (int i = 0; i < ListLength(value->funcname); ++i) {
            auto x = ListNodeNth(value->funcname, i);
            if (NodeTag(x) != T_String) {
                NodeNotImplemented(value, x);
                return nullptr;
            }

            names.push_back(to_lower(TString(StrVal(x))));
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
            if (name == "count" && ListLength(value->args) == 0) {
                AddError("FuncCall: count(*) must be used to call a parameterless aggregate function");
                return nullptr;
            }

            bool hasError = false;
            for (int i = 0; i < ListLength(value->args); ++i) {
                auto x = ListNodeNth(value->args, i);
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

    TAstNode* ParseTypeCast(const TypeCast* value, const TExprSettings& settings) {
        if (!value->arg) {
            AddError("Expected arg");
            return nullptr;
        }

        if (!value->typeName) {
            AddError("Expected type_name");
            return nullptr;
        }

        auto arg = value->arg;
        auto typeName = value->typeName;
        auto supportedTypeName = typeName->typeOid == 0 &&
            !typeName->setof &&
            !typeName->pct_type &&
            ListLength(typeName->typmods) == 0 &&
            (ListLength(typeName->names) == 2 &&
                NodeTag(ListNodeNth(typeName->names, 0)) == T_String &&
                !StrCompare(StrVal(ListNodeNth(typeName->names, 0)), "pg_catalog") || ListLength(typeName->names) == 1) &&
            NodeTag(ListNodeNth(typeName->names, ListLength(typeName->names) - 1)) == T_String &&
            typeName->typemod == -1;

        if (NodeTag(arg) == T_A_Const &&
            (NodeTag(CAST_NODE(A_Const, arg)->val) == T_String ||
            NodeTag(CAST_NODE(A_Const, arg)->val) == T_Null) &&
            supportedTypeName &&
            ListLength(typeName->arrayBounds) == 0) {
            TStringBuf targetType = StrVal(ListNodeNth(typeName->names, ListLength(typeName->names) - 1));
            if (NodeTag(CAST_NODE(A_Const, arg)->val) == T_String && targetType == "bool") {
                auto str = StrVal(CAST_NODE(A_Const, arg)->val);
                if (Settings.PgTypes) {
                    return L(A("PgConst"), QA("bool"), QA(str));
                }

                if (!StrCompare(str, "t")) {
                    return L(A("Just"), L(A("Bool"), QA("1")));
                } else if (!StrCompare(str, "f")) {
                    return L(A("Just"), L(A("Bool"), QA("0")));
                } else {
                    AddError(TStringBuilder() << "Unsupported boolean literal: " << str);
                    return nullptr;
                }
            }

            if (!Settings.PgTypes && NodeTag(CAST_NODE(A_Const, arg)->val) == T_Null) {
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

        if (Settings.PgTypes && supportedTypeName) {
            TStringBuf targetType = StrVal(ListNodeNth(typeName->names, ListLength(typeName->names) - 1));
            auto input = ParseExpr(arg, settings);
            if (!input) {
                return nullptr;
            }

            auto finalType = TString(targetType);
            if (ListLength(typeName->arrayBounds) && !finalType.StartsWith('_')) {
                finalType = "_" + finalType;
            }

            return L(A("PgCast"), QA(finalType), input);
        }

        AddError("Unsupported form of type cast");
        return nullptr;
    }

    TAstNode* ParseBoolExpr(const BoolExpr* value, const TExprSettings& settings) {
        switch (value->boolop) {
        case AND_EXPR: {
            if (ListLength(value->args) != 2) {
                AddError("Expected 2 args for AND");
                return nullptr;
            }

            auto lhs = ParseExpr(ListNodeNth(value->args, 0), settings);
            auto rhs = ParseExpr(ListNodeNth(value->args, 1), settings);
            if (!lhs || !rhs) {
                return nullptr;
            }

            return L(A("And"), lhs, rhs);
        }
        case OR_EXPR: {
            if (ListLength(value->args) != 2) {
                AddError("Expected 2 args for OR");
                return nullptr;
            }

            auto lhs = ParseExpr(ListNodeNth(value->args, 0), settings);
            auto rhs = ParseExpr(ListNodeNth(value->args, 1), settings);
            if (!lhs || !rhs) {
                return nullptr;
            }

            return L(A("Or"), lhs, rhs);
        }
        case NOT_EXPR: {
            if (ListLength(value->args) != 1) {
                AddError("Expected 1 arg for NOT");
                return nullptr;
            }

            auto arg = ParseExpr(ListNodeNth(value->args, 0), settings);
            if (!arg) {
                return nullptr;
            }

            return L(A("Not"), arg);
        }
        default:
            AddError(TStringBuilder() << "BoolExprType unsupported value: " << (int)value->boolop);
            return nullptr;
        }
    }

    TAstNode* ParseWindowDef(const WindowDef* value) {
        auto name = QA(value->name);
        auto refName = QA(value->refname);
        TVector<TAstNode*> sortItems;
        for (int i = 0; i < ListLength(value->orderClause); ++i) {
            auto node = ListNodeNth(value->orderClause, i);
            if (NodeTag(node) != T_SortBy) {
                NodeNotImplemented(value, node);
                return nullptr;
            }

            auto sort = ParseSortBy(CAST_NODE_EXT(PG_SortBy, T_SortBy, node));
            if (!sort) {
                return nullptr;
            }

            sortItems.push_back(sort);
        }

        auto sort = QVL(sortItems.data(), sortItems.size());
        TVector<TAstNode*> groupByItems;
        for (int i = 0; i < ListLength(value->partitionClause); ++i) {
            auto node = ListNodeNth(value->partitionClause, i);
            if (NodeTag(node) != T_ColumnRef) {
                NodeNotImplemented(value, node);
                return nullptr;
            }

            auto ref = ParseColumnRef(CAST_NODE(ColumnRef, node));
            if (!ref) {
                return nullptr;
            }

            auto lambda = L(A("lambda"), QL(), ref);
            groupByItems.push_back(L(A("PgGroup"), L(A("Void")), lambda));
        }

        auto group = QVL(groupByItems.data(), groupByItems.size());
        TVector<TAstNode*> optionItems;
        if (value->frameOptions & FRAMEOPTION_NONDEFAULT) {
            TString exclude;
            if (value->frameOptions & FRAMEOPTION_EXCLUDE_CURRENT_ROW) {
                if (exclude) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                exclude = "c";
            }

            if (value->frameOptions & FRAMEOPTION_EXCLUDE_GROUP) {
                if (exclude) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                exclude = "cp";
            }

            if (value->frameOptions & FRAMEOPTION_EXCLUDE_TIES) {
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
            if (value->frameOptions & FRAMEOPTION_RANGE) {
                if (type) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                type = "range";
            }

            if (value->frameOptions & FRAMEOPTION_ROWS) {
                if (type) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                type = "rows";
            }

            if (value->frameOptions & FRAMEOPTION_GROUPS) {
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
            if (value->frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING) {
                if (from) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                from = "up";
            }

            if (value->frameOptions & FRAMEOPTION_START_OFFSET_PRECEDING) {
                if (from) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                from = "p";
                auto offset = ConvertFrameOffset(value->startOffset);
                if (!offset) {
                    return nullptr;
                }

                optionItems.push_back(QL(QA("from_value"), offset));
            }

            if (value->frameOptions & FRAMEOPTION_START_CURRENT_ROW) {
                if (from) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                from = "c";
            }

            if (value->frameOptions & FRAMEOPTION_START_OFFSET_FOLLOWING) {
                if (from) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                from = "f";
                auto offset = ConvertFrameOffset(value->startOffset);
                if (!offset) {
                    return nullptr;
                }

                optionItems.push_back(QL(QA("from_value"), offset));
            }

            if (value->frameOptions & FRAMEOPTION_START_UNBOUNDED_FOLLOWING) {
                AddError("Wrong frame options");
                return nullptr;
            }

            if (!from) {
                AddError("Wrong frame options");
                return nullptr;
            }

            TString to;
            if (value->frameOptions & FRAMEOPTION_END_UNBOUNDED_PRECEDING) {
                AddError("Wrong frame options");
                return nullptr;
            }

            if (value->frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING) {
                if (to) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                to = "p";
                auto offset = ConvertFrameOffset(value->endOffset);
                if (!offset) {
                    return nullptr;
                }

                optionItems.push_back(QL(QA("to_value"), offset));
            }

            if (value->frameOptions & FRAMEOPTION_END_CURRENT_ROW) {
                if (to) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                to = "c";
            }

            if (value->frameOptions & FRAMEOPTION_END_OFFSET_FOLLOWING) {
                if (to) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                to = "f";
                auto offset = ConvertFrameOffset(value->endOffset);
                if (!offset) {
                    return nullptr;
                }

                optionItems.push_back(QL(QA("to_value"), offset));
            }

            if (value->frameOptions & FRAMEOPTION_END_UNBOUNDED_FOLLOWING) {
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

    TAstNode* ConvertFrameOffset(const Node* off) {
        if (NodeTag(off) == T_A_Const
            && NodeTag(CAST_NODE(A_Const, off)->val) == T_Integer) {
            return L(A("Int32"), QA(ToString(IntVal(CAST_NODE(A_Const, off)->val))));
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

    TAstNode* ParseSortBy(const PG_SortBy* value) {
        bool asc = true;
        switch (value->sortby_dir) {
        case SORTBY_DEFAULT:
            break;
        case SORTBY_ASC:
            break;
        case SORTBY_DESC:
            asc = false;
            break;
        default:
            AddError(TStringBuilder() << "sortby_dir unsupported value: " << (int)value->sortby_dir);
            return nullptr;
        }

        if (value->sortby_nulls != SORTBY_NULLS_DEFAULT) {
            AddError(TStringBuilder() << "sortby_nulls unsupported value: " << (int)value->sortby_nulls);
            return nullptr;
        }

        if (ListLength(value->useOp) > 0) {
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

    TAstNode* ParseColumnRef(const ColumnRef* value) {
        if (ListLength(value->fields) == 0) {
            AddError("No fields");
            return nullptr;
        }

        if (ListLength(value->fields) > 2) {
            AddError("Too many fields");
            return nullptr;
        }

        bool isStar = false;
        TVector<TString> fields;
        for (int i = 0; i < ListLength(value->fields); ++i) {
            auto x = ListNodeNth(value->fields, i);
            if (isStar) {
                AddError("Star is already defined");
                return nullptr;
            }

            if (NodeTag(x) == T_String) {
                fields.push_back(StrVal(x));
            } else if (NodeTag(x) == T_A_Star) {
                isStar = true;
            } else {
                NodeNotImplemented(value, x);
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

    TAstNode* ParseAExpr(const A_Expr* value, const TExprSettings& settings) {
        if (value->kind != AEXPR_OP) {
            AddError(TStringBuilder() << "A_Expr_Kind unsupported value: " << (int)value->kind);
            return nullptr;
        }

        if (ListLength(value->name) != 1) {
            AddError(TStringBuilder() << "Unsupported count of names: " << ListLength(value->name));
            return nullptr;
        }

        auto nameNode = ListNodeNth(value->name, 0);
        if (NodeTag(nameNode) != T_String) {
            NodeNotImplemented(value, nameNode);
            return nullptr;
        }

        auto op = StrVal(nameNode);
        if (!value->rexpr) {
            AddError("Missing operands");
            return nullptr;
        }

        if (!value->lexpr) {
            auto rhs = ParseExpr(value->rexpr, settings);
            if (!rhs) {
                return nullptr;
            }

            if (Settings.PgTypes) {
                return L(A("PgOp"), QA(op), rhs);
            } else {
                auto opIt = UnaryOpTranslation.find(op);
                if (opIt == UnaryOpTranslation.end()) {
                    AddError(TStringBuilder() << "Unsupported unary op: " << op);
                    return nullptr;
                }

                return L(A(opIt->second), rhs);
            }
        } else {
            auto lhs = ParseExpr(value->lexpr, settings);
            auto rhs = ParseExpr(value->rexpr, settings);
            if (!lhs || !rhs) {
                return nullptr;
            }

            if (Settings.PgTypes) {
                return L(A("PgOp"), QA(op), lhs, rhs);
            } else {
                auto opIt = BinaryOpTranslation.find(op);
                if (opIt == BinaryOpTranslation.end()) {
                    AddError(TStringBuilder() << "Unsupported binary op: " << op);
                    return nullptr;
                }

                return L(A(opIt->second), lhs, rhs);
            }
        }
    }

    template <typename T>
    void NodeNotImplementedImpl(const Node* nodeptr) {
        TStringBuilder b;
        b << TypeName<T>() << ": ";
        b << "alternative is not implemented yet : " << NodeTag(nodeptr);
        AddError(b);
    }

    template <typename T>
    void NodeNotImplemented(const T* outer, const Node* nodeptr) {
        Y_UNUSED(outer);
        NodeNotImplementedImpl<T>(nodeptr);
    }

    template <typename T>
    void ValueNotImplementedImpl(const Value& value) {
        TStringBuilder b;
        b << TypeName<T>() << ": ";
        b << "alternative is not implemented yet : " << NodeTag(value);
        AddError(b);
    }

    template <typename T>
    void ValueNotImplemented(const T* outer, const Value& value) {
        Y_UNUSED(outer);
        ValueNotImplementedImpl<T>(value);
    }

    void NodeNotImplemented(const Node* nodeptr) {
        TStringBuilder b;
        b << "alternative is not implemented yet : " << NodeTag(nodeptr);
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
