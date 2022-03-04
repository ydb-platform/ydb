LIBRARY()

OWNER(g:yql)

YQL_LAST_ABI_VERSION()

ADDINCL(
    contrib/libs/libiconv/include
    contrib/libs/lz4
    ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/bootstrap
    ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/parser
    ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/replication
    ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/replication/logical
    ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/utils/adt
    ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/utils/misc
    ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/utils/sort
    ydb/library/yql/parser/pg_wrapper/postgresql/src/common
    ydb/library/yql/parser/pg_wrapper/postgresql/src/include
    contrib/libs/postgresql/src/port
)

SRCS(
    parser.cpp
    thread_inits.c
    comp_factory.cpp
)

INCLUDE(pg_sources.inc)

PEERDIR(
    ydb/library/yql/public/issue
    library/cpp/resource
    ydb/library/yql/minikql/computation
    ydb/library/yql/parser/pg_catalog

    contrib/libs/icu
    contrib/libs/libc_compat
    contrib/libs/libiconv
    contrib/libs/libxml
    contrib/libs/lz4
    contrib/libs/openssl
)

CFLAGS(
    -DDLSUFFIX=\".so\"
)

NO_COMPILER_WARNINGS()

IF (OS_WINDOWS)
CFLAGS(
   "-D__thread=__declspec(thread)"
   -Dfstat=microsoft_native_fstat
   -Dstat=microsoft_native_stat
)
ENDIF()

CFLAGS(
    -Darray_length=yql_array_length
    -Dpg_encoding_max_length=yql_pg_encoding_max_length
    -Dpg_encoding_mblen=yql_pg_encoding_mblen
    -Dpg_mule_mblen=yql_pg_mule_mblen
    -Dpg_utf8_islegal=yql_pg_utf8_islegal
    -Dpg_utf_mblen=yql_pg_utf_mblen
    -Dpg_wchar_table=yql_pg_wchar_table
    -Dunicode_to_utf8=yql_unicode_to_utf8
    -Dutf8_to_unicode=yql_utf8_to_unicode
    -Dpg_enc2name_tbl=yql_pg_enc2name_tbl
    -Dstrtoint=yql_strtoint
    -Dpalloc=yql_palloc
    -Dpalloc0=yql_palloc0
    -Dpfree=yql_pfree
    -Dpstrdup=yql_pstrdup
    -Drepalloc=yql_repalloc
    -Dpalloc_extended=yql_palloc_extended
    -Dpg_clean_ascii=yql_pg_clean_ascii
    -Dpg_str_endswith=yql_pg_str_endswith
    -Dpg_strip_crlf=yql_pg_strip_crlf
    -Dpg_encoding_dsplen=yql_pg_encoding_dsplen
    -Dpg_encoding_mblen_bounded=yql_pg_encoding_mblen_bounded
    -Dpg_encoding_verifymb=yql_pg_encoding_verifymb
    -Dunicode_normalize=yql_unicode_normalize
    -Ddurable_rename=yql_durable_rename
    -Dfsync_fname=yql_fsync_fname
    -Dget_encoding_name_for_icu=yql_get_encoding_name_for_icu
    -Dis_encoding_supported_by_icu=yql_is_encoding_supported_by_icu
    -Dpg_char_to_encoding=yql_pg_char_to_encoding
    -Dpg_enc2gettext_tbl=yql_pg_enc2gettext_tbl
    -Dpg_encoding_to_char=yql_pg_encoding_to_char
    -Dpg_valid_client_encoding=yql_pg_valid_client_encoding
    -Dpg_valid_server_encoding=yql_pg_valid_server_encoding
    -Dpg_valid_server_encoding_id=yql_pg_valid_server_encoding_id
    -Dpnstrdup=yql_pnstrdup
    -Dget_ps_display=yql_get_ps_display
    -Dinit_ps_display=yql_init_ps_display
    -Dsave_ps_display_args=yql_save_ps_display_args
    -Dset_ps_display=yql_set_ps_display
    -Dupdate_process_title=yql_update_process_title
    -Dlo_read=yql_lo_read
    -Dlo_write=yql_lo_write
    -Drtrim=yql_rtrim
    -Dstr_tolower=yql_str_tolower
    -Dstr_toupper=yql_str_toupper
    -Dpg_file_create_mode=yql_pg_file_create_mode
    -Dpg_dir_create_mode=yql_pg_dir_create_mode
    -Descape_json=yql_escape_json
    -DSetDataDirectoryCreatePerm=yql_SetDataDirectoryCreatePerm
    -Dpg_mode_mask=yql_pg_mode_mask
    -Dpg_strong_random=yql_pg_strong_random
    -DScanKeywordCategories=yql_ScanKeywordCategories
    -DScanKeywords=yql_ScanKeywords
    -Dscram_ClientKey=yql_scram_ClientKey
    -Dscram_H=yql_scram_H
    -Dscram_SaltedPassword=yql_scram_SaltedPassword
    -Dscram_ServerKey=yql_scram_ServerKey
    -Dscram_build_secret=yql_scram_build_secret
)

IF (OS_LINUX OR OS_DARWIN)
    SRCS(
        ../../../../../contrib/libs/postgresql/src/backend/port/posix_sema.c
        ../../../../../contrib/libs/postgresql/src/backend/port/sysv_shmem.c
    )
ELSEIF (OS_WINDOWS)
    ADDINCL(
        contrib/libs/postgresql/src/include/port
        contrib/libs/postgresql/src/include/port/win32
        contrib/libs/postgresql/src/include/port/win32_msvc
    )
    SRCS(
        ../../../../../contrib/libs/postgresql/src/backend/port/win32/crashdump.c
        ../../../../../contrib/libs/postgresql/src/backend/port/win32/signal.c
        ../../../../../contrib/libs/postgresql/src/backend/port/win32/socket.c
        ../../../../../contrib/libs/postgresql/src/backend/port/win32/timer.c
        ../../../../../contrib/libs/postgresql/src/backend/port/win32_sema.c
        ../../../../../contrib/libs/postgresql/src/backend/port/win32_shmem.c
        ../../../../../contrib/libs/postgresql/src/port/dirmod.c
        ../../../../../contrib/libs/postgresql/src/port/dlopen.c
        ../../../../../contrib/libs/postgresql/src/port/getaddrinfo.c
        ../../../../../contrib/libs/postgresql/src/port/getopt.c
        ../../../../../contrib/libs/postgresql/src/port/getrusage.c
        ../../../../../contrib/libs/postgresql/src/port/gettimeofday.c
        ../../../../../contrib/libs/postgresql/src/port/inet_aton.c
        ../../../../../contrib/libs/postgresql/src/port/kill.c
        ../../../../../contrib/libs/postgresql/src/port/open.c
        ../../../../../contrib/libs/postgresql/src/port/pread.c
        ../../../../../contrib/libs/postgresql/src/port/pwrite.c
        ../../../../../contrib/libs/postgresql/src/port/pwritev.c
        ../../../../../contrib/libs/postgresql/src/port/system.c
        ../../../../../contrib/libs/postgresql/src/port/win32env.c
        ../../../../../contrib/libs/postgresql/src/port/win32error.c
        ../../../../../contrib/libs/postgresql/src/port/win32security.c
        ../../../../../contrib/libs/postgresql/src/port/win32setlocale.c
        ../../../../../contrib/libs/postgresql/src/port/win32stat.c
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
