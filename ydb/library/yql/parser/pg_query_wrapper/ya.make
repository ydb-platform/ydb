LIBRARY()

OWNER(g:yql)

ADDINCL(
    ydb/library/yql/parser/pg_query_wrapper/contrib
    GLOBAL ydb/library/yql/parser/pg_query_wrapper/contrib/vendor
    ydb/library/yql/parser/pg_query_wrapper/contrib/src/postgres/include
)

SRCS(
    wrapper.cpp
    enum.cpp

    contrib/src/pg_query.c
    contrib/src/pg_query_outfuncs_protobuf.c
    contrib/src/pg_query_parse.c

    contrib/src/postgres/guc-file.c
    contrib/src/postgres/src_backend_catalog_namespace.c
    contrib/src/postgres/src_backend_catalog_pg_proc.c
    contrib/src/postgres/src_backend_commands_define.c
    contrib/src/postgres/src_backend_nodes_bitmapset.c
    contrib/src/postgres/src_backend_nodes_copyfuncs.c
    contrib/src/postgres/src_backend_nodes_equalfuncs.c
    contrib/src/postgres/src_backend_nodes_extensible.c
    contrib/src/postgres/src_backend_nodes_list.c
    contrib/src/postgres/src_backend_nodes_makefuncs.c
    contrib/src/postgres/src_backend_nodes_nodeFuncs.c
    contrib/src/postgres/src_backend_nodes_value.c
    contrib/src/postgres/src_backend_parser_gram.c
    contrib/src/postgres/src_backend_parser_parse_expr.c
    contrib/src/postgres/src_backend_parser_parser.c
    contrib/src/postgres/src_backend_parser_scan.c
    contrib/src/postgres/src_backend_parser_scansup.c
    contrib/src/postgres/src_backend_storage_ipc_ipc.c
    contrib/src/postgres/src_backend_storage_lmgr_s_lock.c
    contrib/src/postgres/src_backend_tcop_postgres.c
    contrib/src/postgres/src_backend_utils_adt_datum.c
    contrib/src/postgres/src_backend_utils_adt_expandeddatum.c
    contrib/src/postgres/src_backend_utils_adt_format_type.c
    contrib/src/postgres/src_backend_utils_adt_ruleutils.c
    contrib/src/postgres/src_backend_utils_error_assert.c
    contrib/src/postgres/src_backend_utils_error_elog.c
    contrib/src/postgres/src_backend_utils_fmgr_fmgr.c
    contrib/src/postgres/src_backend_utils_hash_dynahash.c
    contrib/src/postgres/src_backend_utils_init_globals.c
    contrib/src/postgres/src_backend_utils_mb_mbutils.c
    contrib/src/postgres/src_backend_utils_misc_guc.c
    contrib/src/postgres/src_backend_utils_mmgr_aset.c
    contrib/src/postgres/src_backend_utils_mmgr_mcxt.c
    contrib/src/postgres/src_common_encnames.c
    contrib/src/postgres/src_common_hashfn.c
    contrib/src/postgres/src_common_keywords.c
    contrib/src/postgres/src_common_kwlookup.c
    contrib/src/postgres/src_common_psprintf.c
    contrib/src/postgres/src_common_string.c
    contrib/src/postgres/src_common_stringinfo.c
    contrib/src/postgres/src_common_wchar.c
    contrib/src/postgres/src_pl_plpgsql_src_pl_comp.c
    contrib/src/postgres/src_pl_plpgsql_src_pl_funcs.c
    contrib/src/postgres/src_pl_plpgsql_src_pl_gram.c
    contrib/src/postgres/src_pl_plpgsql_src_pl_handler.c
    contrib/src/postgres/src_pl_plpgsql_src_pl_scanner.c
    contrib/src/postgres/src_port_erand48.c
    contrib/src/postgres/src_port_pg_bitutils.c
    contrib/src/postgres/src_port_pgsleep.c
    contrib/src/postgres/src_port_pgstrcasecmp.c
    contrib/src/postgres/src_port_qsort.c
    contrib/src/postgres/src_port_random.c
    contrib/src/postgres/src_port_snprintf.c
    contrib/src/postgres/src_port_strerror.c

    contrib/vendor/xxhash/xxhash.c
    contrib/vendor/protobuf-c/protobuf-c.c

    contrib/protobuf/pg_query.pb-c.c
)

IF(NOT OS_WINDOWS)
SRCS(
    contrib/src/postgres/src_port_strnlen.c
)
ENDIF()

PEERDIR(
    ydb/library/yql/public/issue
)

NO_COMPILER_WARNINGS()

CFLAGS(
    -Dpg_encoding_max_length=pg_encoding_max_length2
    -Dpg_encoding_mblen=pg_encoding_mblen2
    -Dpg_mule_mblen=pg_mule_mblen2
    -Dpg_utf8_islegal=pg_utf8_islegal2
    -Dpg_utf_mblen=pg_utf_mblen2
    -Dpg_wchar_table=pg_wchar_table2
    -Dunicode_to_utf8=unicode_to_utf82
    -Dutf8_to_unicode=utf8_to_unicode2
    -Dpg_enc2name_tbl=pg_enc2name_tbl2
    -Dstrtoint=strtoint2
    -Dpalloc=pallocx
    -Dpalloc0=palloc0x
    -Dpfree=pfreex
    -Dpstrdup=pstrdupx
    -Drepalloc=repallocx
)

IF (OS_LINUX)
CFLAGS(
   -DHAVE_STRCHRNUL
)
ENDIF()

IF (OS_WINDOWS)
CFLAGS(
   "-D__thread=__declspec(thread)"
)
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
