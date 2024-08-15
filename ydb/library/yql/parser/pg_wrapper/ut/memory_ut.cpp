#include "../pg_compat.h"

extern "C" {
#include <ydb/library/yql/parser/pg_wrapper/postgresql/src/include/postgres.h>
#include <ydb/library/yql/parser/pg_wrapper/postgresql/src/include/utils/memutils.h>
}

#include "arena_ctx.h"

#include <ydb/library/yql/minikql/mkql_alloc.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TPGMemoryTests) {
    Y_UNIT_TEST(TestArenaContextBasic) {
        TArenaMemoryContext arena;
        auto p1 = palloc(123);
        auto p2 = palloc(456);
        Y_UNUSED(p2);
        pfree(p1);
    }

    Y_UNIT_TEST(TestMkqlContextBasic) {
        NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        auto p1 = palloc(123);
        auto p2 = palloc(456);
        Y_UNUSED(p2);
        pfree(p1);
    }

    Y_UNIT_TEST(TestArenaContextCleanupChild) {
        TArenaMemoryContext arena;
        auto tmpContext = AllocSetContextCreate(CurrentMemoryContext, "Tmp", ALLOCSET_SMALL_SIZES);
        auto oldcontext = MemoryContextSwitchTo(tmpContext);
        auto p1 = palloc(123);
        Y_UNUSED(p1);
        MemoryContextSwitchTo(oldcontext);
    }

    Y_UNIT_TEST(TestMkqlContextCleanupChild) {
        NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        auto tmpContext = AllocSetContextCreate(CurrentMemoryContext, "Tmp", ALLOCSET_SMALL_SIZES);
        auto oldcontext = MemoryContextSwitchTo(tmpContext);
        auto p1 = palloc(123);
        Y_UNUSED(p1);
        MemoryContextSwitchTo(oldcontext);
    }
}

}
