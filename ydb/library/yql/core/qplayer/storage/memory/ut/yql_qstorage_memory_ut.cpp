#include <ydb/library/yql/core/qplayer/storage/memory/yql_qstorage_memory.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/core/qplayer/storage/ut_common/yql_qstorage_ut_common.h>

using namespace NYql;

Y_UNIT_TEST_SUITE(TQStorageMemoryTests) {
    GENERATE_TESTS(MakeMemoryQStorage, false)
}
