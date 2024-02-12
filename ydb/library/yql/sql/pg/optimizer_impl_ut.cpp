#include <ydb/library/yql/parser/pg_wrapper/pg_compat.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/hook/hook.h>

#include <ydb/library/yql/parser/pg_wrapper/arena_ctx.h>

#include "optimizer.h"

extern "C" {
#include <ydb/library/yql/parser/pg_wrapper/thread_inits.h>
}

#ifdef _WIN32
#define __restrict
#endif

#define TypeName PG_TypeName
#define SortBy PG_SortBy
#undef SIZEOF_SIZE_T

extern "C" {
#include "optimizer/paths.h"
}

#undef TypeName
#undef SortBy

using namespace NYql;

Y_UNIT_TEST_SUITE(PgOptimizerImpl) {

Y_TEST_HOOK_BEFORE_RUN(InitTest) {
    pg_thread_init();
}

Y_UNIT_TEST(MakeVar) {
    TArenaMemoryContext ctx;

    auto* v1 = MakeVar(1, 1);
    UNIT_ASSERT(v1);
    UNIT_ASSERT_EQUAL(v1->varno, 1);
    UNIT_ASSERT_EQUAL(v1->varattno, 1);

    auto* v2 = MakeVar(2, 2);
    UNIT_ASSERT(v2);
    UNIT_ASSERT_EQUAL(v2->varno, 2);
    UNIT_ASSERT_EQUAL(v2->varattno, 2);
}

Y_UNIT_TEST(MakeRelOptInfo) {
    TArenaMemoryContext ctx;
    IOptimizer::TRel rel1 = {10, 100, {{}}};
    auto* r1 = MakeRelOptInfo(rel1, 1);
    UNIT_ASSERT(r1);
    UNIT_ASSERT_EQUAL(r1->reltarget->exprs->length, 1);
    UNIT_ASSERT_EQUAL(
        ((Var*)r1->reltarget->exprs->elements[0].ptr_value)->varattno, 1
    );

    UNIT_ASSERT_EQUAL(r1->pathlist->length, 1);
    UNIT_ASSERT_EQUAL(
        ((Path*)r1->pathlist->elements[0].ptr_value)->rows, 10
    );
    UNIT_ASSERT_EQUAL(
        ((Path*)r1->pathlist->elements[0].ptr_value)->total_cost, 100
    );

    IOptimizer::TRel rel2 = {100, 99, {{}, {}}};
    auto* r2 = MakeRelOptInfo(rel2, 2);
    UNIT_ASSERT(r2);
    UNIT_ASSERT_EQUAL(r2->reltarget->exprs->length, 2);
}

Y_UNIT_TEST(MakeRelOptInfoListNull) {
    TArenaMemoryContext ctx;
    auto* l = MakeRelOptInfoList({});
    UNIT_ASSERT(l == nullptr);
}

Y_UNIT_TEST(MakeRelOptInfoList) {
    TArenaMemoryContext ctx;
    IOptimizer::TRel rel1 = {10, 100, {{}}};
    IOptimizer::TRel rel2 = {100, 99, {{}}};
    IOptimizer::TInput input = {.Rels={rel1, rel2}};
    auto* l = MakeRelOptInfoList(input);
    UNIT_ASSERT(l);
    UNIT_ASSERT_EQUAL(l->length, 2);
}

} // PgOptimizerImpl
