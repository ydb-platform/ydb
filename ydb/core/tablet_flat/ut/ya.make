UNITTEST_FOR(ydb/core/tablet_flat)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    TIMEOUT(2400)
    TAG(ya:fat)
    SIZE(LARGE)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    datetime_ut.cpp
    decimal_ut.cpp
    flat_cxx_database_ut.cpp
    ut_db_iface.cpp
    ut_db_scheme.cpp
    flat_executor_ut.cpp
    flat_executor_database_ut.cpp
    flat_executor_gclogic_ut.cpp
    flat_executor_leases_ut.cpp
    flat_range_cache_ut.cpp
    flat_row_versions_ut.cpp
    flat_table_part_ut.cpp
    flat_test_db.h
    flat_test_db.cpp
    flat_test_db_helpers.h
    shared_handle_ut.cpp
    ut_btree_index_nodes.cpp
    ut_btree_index_iter_charge.cpp
    ut_self.cpp
    ut_iterator.cpp
    ut_memtable.cpp
    ut_sausage.cpp
    ut_stat.cpp
    ut_comp_gen.cpp
    ut_compaction.cpp
    ut_compaction_multi.cpp
    ut_charge.cpp
    ut_part.cpp
    ut_part_multi.cpp
    ut_proto.cpp
    ut_pages.cpp
    ut_redo.cpp
    ut_rename_table_column.cpp
    ut_other.cpp
    ut_forward.cpp
    ut_screen.cpp
    ut_bloom.cpp
    ut_shared_sausagecache.cpp
    ut_slice.cpp
    ut_slice_loader.cpp
    ut_versions.cpp
)

RESOURCE(
    ../test/data/002_full_part.pages abi/002_full_part.pages
    ../test/data/008_basics_db.redo abi/008_basics_db.redo
)

PEERDIR(
    library/cpp/resource
    ydb/core/scheme
    ydb/core/tablet_flat/test/libs/exec
    ydb/core/tablet_flat/test/libs/table
    ydb/core/testlib/default
    ydb/library/yql/public/udf/service/exception_policy
)

END()
