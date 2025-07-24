RECURSE(dump)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/library/compatibility/versions.inc)

# UNION()
#
#
# RUN_PROGRAM(
#    ydb/tests/library/compatibility/binaries/downloader download $YDB_COMPAT_INTER_REF/release/config-meta.json inter $YDB_COMPAT_INTER_REF
#    OUT_NOAUTO inter inter-name
# )
#
# RUN_PROGRAM(
#     ydb/tests/library/compatibility/binaries/downloader download $YDB_COMPAT_INIT_REF/release/config-meta.json init $YDB_COMPAT_INIT_REF
#     OUT_NOAUTO init init-name
# )
# 
# IF(${YDB_COMPAT_TARGET_REF} != "current")
#     RUN_PROGRAM(
#         ydb/tests/library/compatibility/binaries/downloader download $YDB_COMPAT_TARGET_REF/release/config-meta.json target $YDB_COMPAT_TARGET_REF
#         OUT_NOAUTO target target-name
#     )
# ELSE()
#     RUN_PROGRAM(
#         ydb/tests/library/compatibility/configs/dump/dumper
#         STDOUT_NOAUTO target
#     )
#     RUN(
#         echo current
#         STDOUT_NOAUTO target-name
#     )
# ENDIF()
# 
# END()
