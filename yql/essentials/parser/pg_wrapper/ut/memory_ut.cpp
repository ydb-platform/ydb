#include "../pg_compat.h"

extern "C" {
#include <yql/essentials/parser/pg_wrapper/postgresql/src/include/postgres.h>
#include <yql/essentials/parser/pg_wrapper/postgresql/src/include/utils/memutils.h>
#include <yql/essentials/parser/pg_wrapper/postgresql/src/include/utils/pg_locale.h>
#include <yql/essentials/parser/pg_wrapper/thread_inits.h>
}

#include "arena_ctx.h"

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/system/thread.h>

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

Y_UNIT_TEST(TestCollationThreadCleanup) {
    TThread thread([] {
        pg_thread_init();
        for (const auto* name : {"de-DE-x-icu", "tr-TR-x-icu"}) {
            const auto collation = NPg::LookupCollation(name).Oid;
            const auto locale = pg_newlocale_from_collation(collation);
            UNIT_ASSERT(locale);
            UNIT_ASSERT_VALUES_EQUAL(locale->provider, static_cast<char>(NPg::ECollationProvider::Icu));
            UNIT_ASSERT(locale->info.icu.ucol);
            UNIT_ASSERT(locale == pg_newlocale_from_collation(collation));
        }

#ifdef HAVE_LOCALE_T
        UNIT_ASSERT(pg_newlocale_from_collation(NPg::C_CollationOid));
#endif
        UNIT_ASSERT(!lc_ctype_is_c(NPg::LookupCollation("fr-FR-x-icu").Oid));
    });
    thread.Start();
    thread.Join();
}

Y_UNIT_TEST(TestIcuConverterThreadCleanup) {
    TThread thread([] {
        pg_thread_init();
        UChar* unicode = nullptr;
        UNIT_ASSERT_VALUES_EQUAL(icu_to_uchar(&unicode, "", 0), 0);
        UNIT_ASSERT(unicode);
        UNIT_ASSERT_VALUES_EQUAL(unicode[0], 0);
        pfree(unicode);
    });
    thread.Start();
    thread.Join();
}
} // Y_UNIT_TEST_SUITE(TPGMemoryTests)

} // namespace NYql
