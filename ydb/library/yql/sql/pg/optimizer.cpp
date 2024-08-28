#include "utils.h"
#include "optimizer.h"

#include <iostream>
#include <ydb/library/yql/parser/pg_wrapper/arena_ctx.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/ast/yql_expr.h>

#include <util/string/builder.h>
#include <util/generic/scope.h>

#ifdef _WIN32
#define __restrict
#endif

#define TypeName PG_TypeName
#define SortBy PG_SortBy
#undef SIZEOF_SIZE_T

extern "C" {
Y_PRAGMA_DIAGNOSTIC_PUSH
#ifdef _win_
Y_PRAGMA("GCC diagnostic ignored \"-Wshift-count-overflow\"")
#endif
Y_PRAGMA("GCC diagnostic ignored \"-Wunused-parameter\"")
#include "postgres.h"
#include "miscadmin.h"
#include "optimizer/paths.h"
#include "nodes/print.h"
#include "utils/selfuncs.h"
#include "utils/palloc.h"
Y_PRAGMA_DIAGNOSTIC_POP
}

#undef Min
#undef Max
#undef TypeName
#undef SortBy

namespace NYql {

namespace {

bool RelationStatsHook(
    PlannerInfo *root,
    RangeTblEntry *rte,
    AttrNumber attnum,
    VariableStatData *vardata)
{
    Y_UNUSED(root);
    Y_UNUSED(rte);
    Y_UNUSED(attnum);
    vardata->statsTuple = nullptr;
    return true;
}

} // namespace

Var* MakeVar(int relno, int varno) {
    Var* v = makeNode(Var);
    v->varno = relno; // table number
    v->varattno = varno; // column number in table

    // ?
    v->vartype = 25; // ?
    v->vartypmod = -1; // ?
    v->varcollid = 0;
    v->varnosyn = v->varno;
    v->varattnosyn = v->varattno;
    v->location = -1;
    return v;
}

RelOptInfo* MakeRelOptInfo(const IOptimizer::TRel& r, int relno) {
    RelOptInfo* rel = makeNode(RelOptInfo);
    rel->rows = r.Rows;
    rel->tuples = r.Rows;
    rel->pages = r.Rows;
    rel->allvisfrac = 1.0;
    rel->relid = relno;
    rel->amflags = 1.0;
    rel->rel_parallel_workers = -1;

    PathTarget* t = makeNode(PathTarget);
    int maxattno = 0;
    for (int i = 0; i < (int)r.TargetVars.size(); i++) {
        t->exprs = lappend(t->exprs, MakeVar(relno, i+1));
        maxattno = i+1;
    }
    t->width = 8;

    rel->reltarget = t;
    rel->max_attr = maxattno;

    Path* p = makeNode(Path);
    p->pathtype = T_SeqScan;
    p->rows = r.Rows;
    p->startup_cost = 0;
    p->total_cost = r.TotalCost;
    p->pathtarget = t;
    p->parent = rel;

    rel->pathlist = list_make1(p);
    rel->cheapest_total_path = p;
    rel->relids = bms_add_member(nullptr, rel->relid);
    rel->attr_needed = (Relids*)palloc0((1+maxattno)*sizeof(Relids));

    return rel;
}

List* MakeRelOptInfoList(const IOptimizer::TInput& input) {
    List* l = nullptr;
    int id = 1;
    for (auto& rel : input.Rels) {
        l = lappend(l, MakeRelOptInfo(rel, id++));
    }
    return l;
}

TPgOptimizer::TPgOptimizer(
    const TInput& input,
    const std::function<void(const TString&)>& log)
    : Input(input)
    , Log(log)
{
    get_relation_stats_hook = RelationStatsHook;
}

TPgOptimizer::~TPgOptimizer()
{ }

TPgOptimizer::TOutput TPgOptimizer::JoinSearch()
{
    TArenaMemoryContext ctx;
    auto prev_work_mem = work_mem;
    work_mem = 4096;
    Y_DEFER {
        work_mem = prev_work_mem;
    };

    auto* rel = JoinSearchInternal();
    return MakeOutput(rel->cheapest_total_path);
}

Var* TPgOptimizer::MakeVar(TVarId varId) {
    auto*& var = Vars[varId];
    return var
        ? var
        : (var = ::NYql::MakeVar(std::get<0>(varId), std::get<1>(varId)));
}

EquivalenceClass* TPgOptimizer::MakeEqClass(int i) {
    EquivalenceClass* eq = makeNode(EquivalenceClass);

    for (auto [relno, varno] : Input.EqClasses[i].Vars) {
        EquivalenceMember* m = makeNode(EquivalenceMember);
        m->em_expr = (Expr*)MakeVar(TVarId{relno, varno});
        m->em_relids = bms_add_member(nullptr, relno);
        m->em_datatype = 20;
        eq->ec_opfamilies = list_make1_oid(1976);
        eq->ec_members = lappend(eq->ec_members, m);
        eq->ec_relids = bms_union(eq->ec_relids, m->em_relids);
    }
    return eq;
}

List* TPgOptimizer::MakeEqClasses() {
    List* l = nullptr;
    for (int i = 0; i < (int)Input.EqClasses.size(); i++) {
        l = lappend(l, MakeEqClass(i));
    }
    return l;
}

void TPgOptimizer::LogNode(const TString& prefix, void* node)
{
    if (Log) {
        auto* str = nodeToString(node);
        auto* fmt = pretty_format_node_dump(str);
        pfree(str);
        Log(TStringBuilder() << prefix << ": " << fmt);
        pfree(fmt);
    }
}

IOptimizer::TOutput TPgOptimizer::MakeOutput(Path* path) {
    TOutput output = {{}, &Input};
    output.Rows = path->rows;
    output.TotalCost = path->total_cost;
    MakeOutputJoin(output, path);
    return output;
}

int TPgOptimizer::MakeOutputJoin(TOutput& output, Path* path) {
    if (path->type == T_MaterialPath) {
        return MakeOutputJoin(output, ((MaterialPath*)path)->subpath);
    }
    int id = output.Nodes.size();
    TJoinNode node = output.Nodes.emplace_back(TJoinNode{});

    int relid = -1;
    while ((relid = bms_next_member(path->parent->relids, relid)) >= 0)
	{
        node.Rels.emplace_back(relid);
    }

    if (path->type != T_Path) {
        node.Strategy = EJoinStrategy::Unknown;
        if (path->type == T_HashPath) {
            node.Strategy = EJoinStrategy::Hash;
        } else if (path->type == T_NestPath) {
            node.Strategy = EJoinStrategy::Loop;
        } else {
            YQL_ENSURE(false, "Uknown pathtype " << (int)path->type);
        }

        JoinPath* jpath = (JoinPath*)path;
        switch (jpath->jointype) {
        case JOIN_INNER:
            node.Mode = EJoinType::Inner;
            break;
        case JOIN_LEFT:
            node.Mode = EJoinType::Left;
            break;
        case JOIN_RIGHT:
            node.Mode = EJoinType::Right;
            break;
        default:
            YQL_ENSURE(false, "Unsupported join type");
            break;
        }

       YQL_ENSURE(list_length(jpath->joinrestrictinfo) >= 1, "Unsupported joinrestrictinfo len");

        for (int i = 0; i < list_length(jpath->joinrestrictinfo); i++) {
            RestrictInfo* rinfo = (RestrictInfo*)jpath->joinrestrictinfo->elements[i].ptr_value;
            Var* left = nullptr;
            Var* right = nullptr;

            if (jpath->jointype == JOIN_INNER) {
                YQL_ENSURE(rinfo->left_em->em_expr->type == T_Var, "Unsupported left em type");
                YQL_ENSURE(rinfo->right_em->em_expr->type == T_Var, "Unsupported right em type");

                left = (Var*)rinfo->left_em->em_expr;
                right = (Var*)rinfo->right_em->em_expr;
            } else if (jpath->jointype == JOIN_LEFT || jpath->jointype == JOIN_RIGHT) {
                YQL_ENSURE(rinfo->clause->type == T_OpExpr);
                OpExpr* expr = (OpExpr*)rinfo->clause;
                YQL_ENSURE(list_length(expr->args) == 2);
                Expr* a1 = (Expr*)list_nth(expr->args, 0);
                Expr* a2 = (Expr*)list_nth(expr->args, 1);
                YQL_ENSURE(a1->type == T_Var, "Unsupported left arg type");
                YQL_ENSURE(a2->type == T_Var, "Unsupported right arg type");

                left = (Var*)a1;
                right = (Var*)a2;
            }

            node.LeftVars.emplace_back(std::make_tuple(left->varno, left->varattno));
            node.RightVars.emplace_back(std::make_tuple(right->varno, right->varattno));

            if (!bms_is_member(left->varno, jpath->outerjoinpath->parent->relids)) {
                std::swap(node.LeftVars.back(), node.RightVars.back());
            }
        }

        node.Inner = MakeOutputJoin(output, jpath->innerjoinpath);
        node.Outer = MakeOutputJoin(output, jpath->outerjoinpath);
    }

    output.Nodes[id] = node;

    return id;
}

void TPgOptimizer::MakeLeftOrRightRestrictions(std::vector<RestrictInfo*>& dst, const std::vector<TEq>& src)
{
    for (const auto& eq : src) {
        YQL_ENSURE(eq.Vars.size() == 2);
        RestrictInfo* ri = makeNode(RestrictInfo);
        ri->can_join = 1;
        ri->norm_selec = -1;
        ri->outer_selec = -1;

        OpExpr* oe = makeNode(OpExpr);
        oe->opno = 410;
        oe->opfuncid = 467;
        oe->opresulttype = 16;
        ri->clause = (Expr*)oe;

        bool left = true;
        for (const auto& [relId, varId] : eq.Vars) {
            ri->required_relids = bms_add_member(ri->required_relids, relId);
            ri->clause_relids = bms_add_member(ri->clause_relids, relId);
            if (left) {
                ri->outer_relids = bms_add_member(nullptr, relId);
                ri->left_relids = bms_add_member(nullptr, relId);
                left = false;
            } else {
                ri->right_relids = bms_add_member(nullptr, relId);
            }
            oe->args = lappend(oe->args, MakeVar(TVarId{relId, varId}));

            RestrictInfos[relId].emplace_back(ri);
        }
        dst.emplace_back(ri);
    }
}

RelOptInfo* TPgOptimizer::JoinSearchInternal() {
    RestrictInfos.clear();
    RestrictInfos.resize(Input.Rels.size()+1);
    LeftRestriction.clear();
    LeftRestriction.reserve(Input.Left.size());
    MakeLeftOrRightRestrictions(LeftRestriction, Input.Left);
    MakeLeftOrRightRestrictions(RightRestriction, Input.Right);

    List* rels = MakeRelOptInfoList(Input);
    ListCell* l;

    int relId = 1;
    foreach (l, rels) {
        RelOptInfo* rel = (RelOptInfo*)lfirst(l);
        for (auto* ri : RestrictInfos[relId++]) {
            rel->joininfo = lappend(rel->joininfo, ri);
        }
    }

    if (Log) {
        int i = 1;
        foreach (l, rels) {
            LogNode(TStringBuilder() << "Input: " << i++, lfirst(l));
        }
    }

    PlannerInfo root;
    memset(&root, 0, sizeof(root));
    root.type = T_PlannerInfo;
    root.query_level = 1;
    root.simple_rel_array_size = rels->length+1;
    root.simple_rel_array = (RelOptInfo**)palloc0(
        root.simple_rel_array_size
        * sizeof(RelOptInfo*));
    root.simple_rte_array = (RangeTblEntry**)palloc0(
        root.simple_rel_array_size * sizeof(RangeTblEntry*)
    );
    for (int i = 0; i <= rels->length; i++) {
        root.simple_rte_array[i] = makeNode(RangeTblEntry);
        root.simple_rte_array[i]->rtekind = RTE_RELATION;
    }
    root.all_baserels = bms_add_range(nullptr, 1, rels->length);
    root.eq_classes = MakeEqClasses();

    for (auto* ri : LeftRestriction) {
        root.left_join_clauses = lappend(root.left_join_clauses, ri);
        root.hasJoinRTEs = 1;
        root.outer_join_rels = bms_add_members(root.outer_join_rels, ri->right_relids);

        SpecialJoinInfo* ji = makeNode(SpecialJoinInfo);
        ji->min_lefthand = bms_add_member(ji->min_lefthand, bms_next_member(ri->left_relids, -1));
        ji->min_righthand = bms_add_member(ji->min_righthand, bms_next_member(ri->right_relids, -1));

        ji->syn_lefthand = bms_add_members(ji->min_lefthand, ri->left_relids);
        ji->syn_righthand = bms_add_members(ji->min_righthand, ri->right_relids);
        ji->jointype = JOIN_LEFT;
        ji->lhs_strict = 1;

        root.join_info_list = lappend(root.join_info_list, ji);
    }

    for (auto* ri : RightRestriction) {
        root.right_join_clauses = lappend(root.right_join_clauses, ri);
        root.hasJoinRTEs = 1;
        root.outer_join_rels = bms_add_members(root.outer_join_rels, ri->left_relids);

        SpecialJoinInfo* ji = makeNode(SpecialJoinInfo);
        ji->min_lefthand = bms_add_member(ji->min_lefthand, bms_next_member(ri->right_relids, -1));
        ji->min_righthand = bms_add_member(ji->min_righthand, bms_next_member(ri->left_relids, -1));

        ji->syn_lefthand = bms_add_members(ji->min_lefthand, ri->right_relids);
        ji->syn_righthand = bms_add_members(ji->min_righthand, ri->left_relids);
        ji->jointype = JOIN_LEFT;
        ji->lhs_strict = 1;

        root.join_info_list = lappend(root.join_info_list, ji);
    }

    root.planner_cxt = CurrentMemoryContext;

    for (int i = 0; i < rels->length; i++) {
        auto* r = (RelOptInfo*)rels->elements[i].ptr_value;
        root.simple_rel_array[i+1] = r;
    }

    for (int eqId = 0; eqId < (int)Input.EqClasses.size(); eqId++) {
        for (auto& [relno, _] : Input.EqClasses[eqId].Vars) {
            root.simple_rel_array[relno]->eclass_indexes = bms_add_member(
                root.simple_rel_array[relno]->eclass_indexes,
                eqId);
        }
    }

    for (int i = 0; i < rels->length; i++) {
        root.simple_rel_array[i+1]->has_eclass_joins = bms_num_members(root.simple_rel_array[i+1]->eclass_indexes) > 1;
    }
    root.ec_merging_done = 1;

    LogNode("Context: ", &root);

    auto* result = standard_join_search(&root, rels->length, rels);
    LogNode("Result: ", result);
    return result;
}

struct TPgOptimizerImpl
{
    TPgOptimizerImpl(
        const std::shared_ptr<TJoinOptimizerNode>& root,
        TExprContext& ctx,
        const std::function<void(const TString&)>& log)
        : Root(root)
        , Ctx(ctx)
        , Log(log)
    { }

    std::shared_ptr<TJoinOptimizerNode> Do() {
        CollectRels(Root);
        if (!CollectOps(Root)) {
            return Root;
        }

        IOptimizer::TInput input;
        input.EqClasses = std::move(EqClasses);
        input.Left = std::move(Left);
        input.Right = std::move(Right);
        input.Rels = std::move(Rels);
        input.Normalize();
        Log("Input: " + input.ToString());

        std::unique_ptr<IOptimizer> opt = std::unique_ptr<IOptimizer>(MakePgOptimizerInternal(input, Log));
        Result = opt->JoinSearch();

        Log("Result: " + Result.ToString());

        std::shared_ptr<IBaseOptimizerNode> res = Convert(0);
        YQL_ENSURE(res);

        return std::static_pointer_cast<TJoinOptimizerNode>(res);
    }

    void OnLeaf(const std::shared_ptr<TRelOptimizerNode>& leaf) {
        int relId = Rels.size() + 1;
        Rels.emplace_back(IOptimizer::TRel{});
        Var2TableCol.emplace_back();
        // rel -> varIds
        VarIds.emplace_back(THashMap<TStringBuf, int>{});
        // rel -> tables
        RelTables.emplace_back(std::vector<TStringBuf>{});
        for (const auto& table : leaf->Labels()) {
            RelTables.back().emplace_back(table);
            Table2RelIds[table].emplace_back(relId);
        }
        auto& rel = Rels[relId - 1];

        rel.Rows = leaf->Stats->Nrows;
        rel.TotalCost = leaf->Stats->Cost;

        int leafIndex = relId - 1;
        if (leafIndex >= static_cast<int>(Leafs.size())) {
            Leafs.resize(leafIndex + 1);
        }
        Leafs[leafIndex] = leaf;
    }

    int GetVarId(int relId, TStringBuf column) {
        int varId = 0;
        auto maybeVarId = VarIds[relId-1].find(column);
        if (maybeVarId != VarIds[relId-1].end()) {
            varId = maybeVarId->second;
        } else {
            varId = Rels[relId - 1].TargetVars.size() + 1;
            VarIds[relId - 1][column] = varId;
            Rels[relId - 1].TargetVars.emplace_back();
            Var2TableCol[relId - 1].emplace_back();
        }
        return varId;
    }

    void ExtractVars(
        std::vector<std::tuple<int,int,TStringBuf,TStringBuf>>& leftVars,
        std::vector<std::tuple<int,int,TStringBuf,TStringBuf>>& rightVars,
        const std::shared_ptr<TJoinOptimizerNode>& op)
    {
        for (auto& [l, r]: op->JoinConditions) {
            auto& ltable = l.RelName;
            auto& lcol = l.AttributeName;
            auto& rtable = r.RelName;
            auto& rcol = r.AttributeName;

            const auto& lrelIds = Table2RelIds[ltable];
            YQL_ENSURE(!lrelIds.empty());
            const auto& rrelIds = Table2RelIds[rtable];
            YQL_ENSURE(!rrelIds.empty());

            for (int relId : lrelIds) {
                int varId = GetVarId(relId, lcol);

                leftVars.emplace_back(std::make_tuple(relId, varId, ltable, lcol));
            }
            for (int relId : rrelIds) {
                int varId = GetVarId(relId, rcol);

                rightVars.emplace_back(std::make_tuple(relId, varId, rtable, rcol));
            }
        }
    }

    IOptimizer::TEq MakeEqClass(const auto& vars) {
        IOptimizer::TEq eqClass;

        for (auto& [relId, varId, table, column] : vars) {
            eqClass.Vars.emplace_back(std::make_tuple(relId, varId));
            Var2TableCol[relId - 1][varId - 1] = std::make_tuple(table, column);
        }

        return eqClass;
    }

    void MakeEqClasses(std::vector<IOptimizer::TEq>& res, const auto& leftVars, const auto& rightVars) {
        for (int i = 0; i < (int)leftVars.size(); i++) {
            auto& [lrelId, lvarId, ltable, lcolumn] = leftVars[i];
            auto& [rrelId, rvarId, rtable, rcolumn] = rightVars[i];

            IOptimizer::TEq eqClass; eqClass.Vars.reserve(2);
            eqClass.Vars.emplace_back(std::make_tuple(lrelId, lvarId));
            eqClass.Vars.emplace_back(std::make_tuple(rrelId, rvarId));

            Var2TableCol[lrelId - 1][lvarId - 1] = std::make_tuple(ltable, lcolumn);
            Var2TableCol[rrelId - 1][rvarId - 1] = std::make_tuple(rtable, rcolumn);

            res.emplace_back(std::move(eqClass));
        }
    }

    bool OnOp(const std::shared_ptr<TJoinOptimizerNode>& op) {
#define CHECK(A, B)                                                     \
        if (Y_UNLIKELY(!(A))) {                                         \
            TIssues issues;                                             \
            issues.AddIssue(TIssue(B).SetCode(0, NYql::TSeverityIds::S_INFO)); \
            Ctx.IssueManager.AddIssues(issues);                         \
            return false;                                               \
        }

        if (op->JoinType == InnerJoin) {
            // relId, varId, table, column
            std::vector<std::tuple<int,int,TStringBuf,TStringBuf>> leftVars;
            std::vector<std::tuple<int,int,TStringBuf,TStringBuf>> rightVars;

            ExtractVars(leftVars, rightVars, op);

            CHECK(leftVars.size() == rightVars.size(), "Left and right labels must have the same size");

            MakeEqClasses(EqClasses, leftVars, rightVars);
        } else if (op->JoinType == LeftJoin || op->JoinType == RightJoin) {
            CHECK(op->JoinConditions.size() == 1, "Only 1 var per join supported");

            std::vector<std::tuple<int,int,TStringBuf,TStringBuf>> leftVars, rightVars;
            ExtractVars(leftVars, rightVars, op);

            IOptimizer::TEq leftEqClass = MakeEqClass(leftVars);
            IOptimizer::TEq rightEqClass = MakeEqClass(rightVars);
            IOptimizer::TEq eqClass = leftEqClass;
            eqClass.Vars.insert(eqClass.Vars.end(), rightEqClass.Vars.begin(), rightEqClass.Vars.end());

            CHECK(eqClass.Vars.size() == 2, "Only a=b left|right join supported yet");

            EqClasses.emplace_back(std::move(leftEqClass));
            EqClasses.emplace_back(std::move(rightEqClass));
            if (op->JoinType == LeftJoin) {
                Left.emplace_back(eqClass);
            } else {
                Right.emplace_back(eqClass);
            }
        } else {
            CHECK(false, "Unsupported join type");
        }

#undef CHECK
        return true;
    }

    bool CollectOps(const std::shared_ptr<IBaseOptimizerNode>& node)
    {
        if (node->Kind == JoinNodeType) {
            auto op = std::static_pointer_cast<TJoinOptimizerNode>(node);
            return OnOp(op)
                && CollectOps(op->LeftArg)
                && CollectOps(op->RightArg);
        }
        return true;
    }

    void CollectRels(const std::shared_ptr<IBaseOptimizerNode>& node) {
        if (node->Kind == JoinNodeType) {
            auto op = std::static_pointer_cast<TJoinOptimizerNode>(node);
            CollectRels(op->LeftArg);
            CollectRels(op->RightArg);
        } else if (node->Kind == RelNodeType) {
            OnLeaf(std::static_pointer_cast<TRelOptimizerNode>(node));
        } else {
            YQL_ENSURE(false, "Unknown node kind");
        }
    }

    std::shared_ptr<IBaseOptimizerNode> Convert(int nodeId) const {
        const auto* node = &Result.Nodes[nodeId];
        if (node->Outer == -1 && node->Inner == -1) {
            YQL_ENSURE(node->Rels.size() == 1);
            auto leaf = Leafs[node->Rels[0]-1];
            return leaf;
        } else if (node->Outer != -1 && node->Inner != -1) {
            EJoinKind joinKind;
            switch (node->Mode) {
            case IOptimizer::EJoinType::Inner:
                joinKind = InnerJoin; break;
            case IOptimizer::EJoinType::Left:
                joinKind = LeftJoin; break;
            case IOptimizer::EJoinType::Right:
                joinKind = RightJoin; break;
            default:
                YQL_ENSURE(false, "Unsupported join type");
                break;
            };

            auto left = Convert(node->Outer);
            auto right = Convert(node->Inner);

            YQL_ENSURE(node->LeftVars.size() == node->RightVars.size());

            std::set<std::pair<NDq::TJoinColumn, NDq::TJoinColumn>> joinConditions;
            for (size_t i = 0; i < node->LeftVars.size(); i++) {
                auto [lrelId, lvarId] = node->LeftVars[i];
                auto [rrelId, rvarId] = node->RightVars[i];
                auto [ltable, lcolumn] = Var2TableCol[lrelId - 1][lvarId - 1];
                auto [rtable, rcolumn] = Var2TableCol[rrelId - 1][rvarId - 1];

                joinConditions.insert({
                    NDq::TJoinColumn{TString(ltable), TString(lcolumn)},
                    NDq::TJoinColumn{TString(rtable), TString(rcolumn)}
                });
            }

            return std::make_shared<TJoinOptimizerNode>(
                left, right,
                joinConditions,
                joinKind,
                EJoinAlgoType::MapJoin
                );
        } else {
            YQL_ENSURE(false, "Wrong CBO node");
        }
        return nullptr;
    }

    std::shared_ptr<TJoinOptimizerNode> Root;
    TExprContext& Ctx;
    std::function<void(const TString&)> Log;

    THashMap<TStringBuf, std::vector<int>> Table2RelIds;
    std::vector<IOptimizer::TRel> Rels;
    std::vector<std::vector<TStringBuf>> RelTables;
    std::vector<std::shared_ptr<TRelOptimizerNode>> Leafs;
    std::vector<std::vector<std::tuple<TStringBuf, TStringBuf>>> Var2TableCol;

    std::vector<THashMap<TStringBuf, int>> VarIds;

    std::vector<IOptimizer::TEq> EqClasses;
    std::vector<IOptimizer::TEq> Left;
    std::vector<IOptimizer::TEq> Right;

    IOptimizer::TOutput Result;
};

class TPgOptimizerNew: public IOptimizerNew
{
public:
    TPgOptimizerNew(IProviderContext& pctx, TExprContext& ctx, const std::function<void(const TString&)>& log)
        : IOptimizerNew(pctx)
        , Ctx(ctx)
        , Log(log)
    { }

    std::shared_ptr<TJoinOptimizerNode> JoinSearch(
        const std::shared_ptr<TJoinOptimizerNode>& joinTree, 
        const TOptimizerHints& hints = {}) override
    {
        Y_UNUSED(hints);
        return TPgOptimizerImpl(joinTree, Ctx, Log).Do();
    }

private:
    TExprContext& Ctx;
    std::function<void(const TString&)> Log;
};

IOptimizer* MakePgOptimizerInternal(const IOptimizer::TInput& input, const std::function<void(const TString&)>& log)
{
    return new TPgOptimizer(input, log);
}

IOptimizerNew* MakePgOptimizerNew(IProviderContext& pctx, TExprContext& ctx, const std::function<void(const TString&)>& log)
{
    return new TPgOptimizerNew(pctx, ctx, log);
}

} // namespace NYql {
