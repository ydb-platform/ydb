set logging enable on
set trace-commands on

set args +IndexBuildTest::VectorIndexDescriptionIsPersisted

break /home/deminds/github/ydb/ydb/core/tx/schemeshard/ut_index_build/ut_index_build.cpp:1070
break /home/deminds/github/ydb/ydb/core/tx/schemeshard/schemeshard__operation_create_table.cpp:80
break /home/deminds/github/ydb/ydb/core/tx/schemeshard/schemeshard_impl.cpp:6627
break /home/deminds/github/ydb/ydb/core/tx/schemeshard/ut_index_build/ut_index_build.cpp:1076
