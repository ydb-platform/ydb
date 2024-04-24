#include <ydb/library/yql/core/qplayer/storage/file/yql_qstorage_file.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/core/qplayer/storage/ut_common/yql_qstorage_ut_common.h>

using namespace NYql;

Y_UNIT_TEST_SUITE(TQStorageFileTests) {
    GENERATE_TESTS(MakeFileQStorage)
}
