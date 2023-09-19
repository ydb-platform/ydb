#include "utils.h"
#include "optimizer.h"

#include <iostream>
#include <ydb/library/yql/parser/pg_wrapper/arena_ctx.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <util/string/builder.h>

#ifdef _WIN32
#define __restrict
#endif

#define TypeName PG_TypeName
#define SortBy PG_SortBy
#undef SIZEOF_SIZE_T

extern "C" {
#include "postgres.h"
#include "optimizer/paths.h"
#include "nodes/print.h"
#include "utils/selfuncs.h"
#include "utils/palloc.h"
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
            Var* left;
            Var* right;

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
        for (const auto [relId, varId] : eq.Vars) {
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
        root.simple_rel_array_size * sizeof(RangeTblEntry)
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
        root.nullable_baserels = bms_add_members(root.nullable_baserels, ri->right_relids);

        SpecialJoinInfo* ji = makeNode(SpecialJoinInfo);
        ji->min_lefthand = bms_add_member(ji->min_lefthand, bms_first_member(ri->left_relids));
        ji->min_righthand = bms_add_member(ji->min_righthand, bms_first_member(ri->right_relids));

        ji->syn_lefthand = bms_add_members(ji->min_lefthand, ri->left_relids);
        ji->syn_righthand = bms_add_members(ji->min_righthand, ri->right_relids);
        ji->jointype = JOIN_LEFT;
        ji->lhs_strict = 1;

        root.join_info_list = lappend(root.join_info_list, ji);
    }

    for (auto* ri : RightRestriction) {
        root.right_join_clauses = lappend(root.right_join_clauses, ri);
        root.hasJoinRTEs = 1;
        root.nullable_baserels = bms_add_members(root.nullable_baserels, ri->left_relids);

        SpecialJoinInfo* ji = makeNode(SpecialJoinInfo);
        ji->min_lefthand = bms_add_member(ji->min_lefthand, bms_first_member(ri->right_relids));
        ji->min_righthand = bms_add_member(ji->min_righthand, bms_first_member(ri->left_relids));

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

IOptimizer* MakePgOptimizer(const IOptimizer::TInput& input, const std::function<void(const TString&)>& log)
{
    return new TPgOptimizer(input, log);
}

} // namespace NYql {
