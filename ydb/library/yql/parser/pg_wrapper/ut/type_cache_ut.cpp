#include "../pg_compat.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/hook/hook.h>

// see pg_attribute_printf macro in c.h
// should expand to nothing
#undef __GNUC__
#undef __IBMC__

extern "C" {
#include "postgres.h"
#include "utils/typcache.h"

#undef Min
#undef Max

#include <ydb/library/yql/parser/pg_wrapper/thread_inits.h>
}

#include "type_cache.h"

using namespace ::NYql;

Y_UNIT_TEST_SUITE(TLookupTypeCacheTests) {
    // Defined in pg_type.dat
    constexpr ui32 INT4_TYPEID = 23;
    constexpr ui32 ANYARRAY_TYPEID = 2277;

    TypeCacheEntry* cacheEntry = nullptr;

    // We initialize a basic minimally initialized PG cache entry
    // The tests then fill in various parts of it
    Y_TEST_HOOK_BEFORE_RUN(InitTest) {
        pg_thread_init();

        cacheEntry = lookup_type_cache(INT4_TYPEID, 0);
    }

    Y_UNIT_TEST(TestEqOpr) {
        auto cacheEntry = lookup_type_cache(INT4_TYPEID, TYPECACHE_EQ_OPR | TYPECACHE_EQ_OPR_FINFO);

        UNIT_ASSERT(cacheEntry);

        UNIT_ASSERT(cacheEntry->btree_opf != InvalidOid);
        UNIT_ASSERT(cacheEntry->btree_opintype == INT4_TYPEID);

        UNIT_ASSERT(cacheEntry->eq_opr != InvalidOid);
        UNIT_ASSERT(cacheEntry->eq_opr_finfo.fn_addr);

        UNIT_ASSERT(cacheEntry->flags & (TCFLAGS_CHECKED_BTREE_OPCLASS | TCFLAGS_CHECKED_EQ_OPR));
    }

    Y_UNIT_TEST(TestEqOprArray) {
        auto cacheEntry = lookup_type_cache(ANYARRAY_TYPEID, TYPECACHE_EQ_OPR | TYPECACHE_EQ_OPR_FINFO);

        UNIT_ASSERT(cacheEntry);

        UNIT_ASSERT(cacheEntry->btree_opf != InvalidOid);
        UNIT_ASSERT(cacheEntry->btree_opintype == ANYARRAY_TYPEID);

        UNIT_ASSERT(cacheEntry->eq_opr == InvalidOid);
        UNIT_ASSERT(cacheEntry->eq_opr_finfo.fn_addr == nullptr);

        UNIT_ASSERT(cacheEntry->flags & (TCFLAGS_CHECKED_BTREE_OPCLASS | TCFLAGS_CHECKED_EQ_OPR));
    }

    Y_UNIT_TEST(TestLtOpr) {
        auto cacheEntry = lookup_type_cache(INT4_TYPEID, TYPECACHE_LT_OPR);

        UNIT_ASSERT(cacheEntry);

        UNIT_ASSERT(cacheEntry->btree_opf != InvalidOid);
        UNIT_ASSERT(cacheEntry->btree_opintype == INT4_TYPEID);

        UNIT_ASSERT(cacheEntry->lt_opr != InvalidOid);

        UNIT_ASSERT(cacheEntry->flags & (TCFLAGS_CHECKED_BTREE_OPCLASS | TCFLAGS_CHECKED_LT_OPR));
    }

    Y_UNIT_TEST(TestLtOprArray) {
        auto cacheEntry = lookup_type_cache(ANYARRAY_TYPEID, TYPECACHE_LT_OPR);

        UNIT_ASSERT(cacheEntry);

        UNIT_ASSERT(cacheEntry->btree_opf != InvalidOid);
        UNIT_ASSERT(cacheEntry->btree_opintype == ANYARRAY_TYPEID);

        UNIT_ASSERT(cacheEntry->lt_opr == InvalidOid);

        UNIT_ASSERT(cacheEntry->flags & (TCFLAGS_CHECKED_BTREE_OPCLASS | TCFLAGS_CHECKED_LT_OPR));
    }

    Y_UNIT_TEST(TestGtOpr) {
        auto cacheEntry = lookup_type_cache(INT4_TYPEID, TYPECACHE_GT_OPR);

        UNIT_ASSERT(cacheEntry);

        UNIT_ASSERT(cacheEntry->btree_opf != InvalidOid);
        UNIT_ASSERT(cacheEntry->btree_opintype == INT4_TYPEID);

        UNIT_ASSERT(cacheEntry->gt_opr != InvalidOid);

        UNIT_ASSERT(cacheEntry->flags & (TCFLAGS_CHECKED_BTREE_OPCLASS | TCFLAGS_CHECKED_GT_OPR));
    }

    Y_UNIT_TEST(TestGtOprArray) {
        auto cacheEntry = lookup_type_cache(ANYARRAY_TYPEID, TYPECACHE_GT_OPR);

        UNIT_ASSERT(cacheEntry);

        UNIT_ASSERT(cacheEntry->btree_opf != InvalidOid);
        UNIT_ASSERT(cacheEntry->btree_opintype == ANYARRAY_TYPEID);

        UNIT_ASSERT(cacheEntry->gt_opr == InvalidOid);

        UNIT_ASSERT(cacheEntry->flags & (TCFLAGS_CHECKED_BTREE_OPCLASS | TCFLAGS_CHECKED_GT_OPR));
    }

    Y_UNIT_TEST(TestCmpProc) {
        auto cacheEntry = lookup_type_cache(INT4_TYPEID, TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO);

        UNIT_ASSERT(cacheEntry);

        UNIT_ASSERT(cacheEntry->btree_opf != InvalidOid);
        UNIT_ASSERT(cacheEntry->btree_opintype == INT4_TYPEID);

        UNIT_ASSERT(cacheEntry->cmp_proc != InvalidOid);
        UNIT_ASSERT(cacheEntry->cmp_proc_finfo.fn_addr);

        UNIT_ASSERT(cacheEntry->flags & (TCFLAGS_CHECKED_BTREE_OPCLASS | TCFLAGS_CHECKED_CMP_PROC));
    }

    Y_UNIT_TEST(TestCmpProcArray) {
        auto cacheEntry = lookup_type_cache(ANYARRAY_TYPEID, TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO);

        UNIT_ASSERT(cacheEntry);

        UNIT_ASSERT(cacheEntry->btree_opf != InvalidOid);
        UNIT_ASSERT(cacheEntry->btree_opintype == ANYARRAY_TYPEID);

        UNIT_ASSERT(cacheEntry->cmp_proc == InvalidOid);

        UNIT_ASSERT(cacheEntry->flags & (TCFLAGS_CHECKED_BTREE_OPCLASS | TCFLAGS_CHECKED_CMP_PROC));
    }

    Y_UNIT_TEST(TestHashProc) {
        auto cacheEntry = lookup_type_cache(INT4_TYPEID, TYPECACHE_HASH_PROC | TYPECACHE_HASH_PROC_FINFO);

        UNIT_ASSERT(cacheEntry);

        UNIT_ASSERT(cacheEntry->hash_opf != InvalidOid);
        UNIT_ASSERT(cacheEntry->hash_opintype == INT4_TYPEID);

        UNIT_ASSERT(cacheEntry->hash_proc != InvalidOid);
        UNIT_ASSERT(cacheEntry->hash_proc_finfo.fn_addr);

        UNIT_ASSERT(cacheEntry->flags & (TCFLAGS_CHECKED_HASH_OPCLASS | TCFLAGS_CHECKED_HASH_PROC));
    }

    Y_UNIT_TEST(TestHashProcArray) {
        auto cacheEntry = lookup_type_cache(ANYARRAY_TYPEID, TYPECACHE_HASH_PROC | TYPECACHE_HASH_PROC_FINFO);

        UNIT_ASSERT(cacheEntry);

        UNIT_ASSERT(cacheEntry->hash_opf != InvalidOid);
        UNIT_ASSERT(cacheEntry->hash_opintype == ANYARRAY_TYPEID);

        UNIT_ASSERT(cacheEntry->hash_proc == InvalidOid);

        UNIT_ASSERT(cacheEntry->flags & (TCFLAGS_CHECKED_HASH_OPCLASS | TCFLAGS_CHECKED_HASH_PROC));
    }

    Y_UNIT_TEST(TestHashExtendedProc) {
        auto cacheEntry = lookup_type_cache(INT4_TYPEID, TYPECACHE_HASH_EXTENDED_PROC | TYPECACHE_HASH_EXTENDED_PROC_FINFO);

        UNIT_ASSERT(cacheEntry);

        UNIT_ASSERT(cacheEntry->hash_opf != InvalidOid);
        UNIT_ASSERT(cacheEntry->hash_opintype == INT4_TYPEID);

        UNIT_ASSERT(cacheEntry->hash_extended_proc != InvalidOid);
        UNIT_ASSERT(cacheEntry->hash_extended_proc_finfo.fn_addr);

        UNIT_ASSERT(cacheEntry->flags & (TCFLAGS_CHECKED_HASH_OPCLASS | TCFLAGS_CHECKED_HASH_EXTENDED_PROC));
    }

    Y_UNIT_TEST(TestHashExtendedProcArray) {
        auto cacheEntry = lookup_type_cache(ANYARRAY_TYPEID, TYPECACHE_HASH_EXTENDED_PROC | TYPECACHE_HASH_EXTENDED_PROC_FINFO);

        UNIT_ASSERT(cacheEntry);

        UNIT_ASSERT(cacheEntry->hash_opf != InvalidOid);
        UNIT_ASSERT(cacheEntry->hash_opintype == ANYARRAY_TYPEID);

        UNIT_ASSERT(cacheEntry->hash_extended_proc == InvalidOid);

        UNIT_ASSERT(cacheEntry->flags & (TCFLAGS_CHECKED_HASH_OPCLASS | TCFLAGS_CHECKED_HASH_EXTENDED_PROC));
    }
}
