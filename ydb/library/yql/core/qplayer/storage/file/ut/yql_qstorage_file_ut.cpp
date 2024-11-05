#include <ydb/library/yql/core/qplayer/storage/file/yql_qstorage_file.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/core/qplayer/storage/ut_common/yql_qstorage_ut_common.h>

using namespace NYql;

IQStoragePtr MakeBufferedFileQStorage() {
    TFileQStorageSettings settings;
    settings.BufferUntilCommit = true;
    return MakeFileQStorage({}, settings);
}

IQStoragePtr MakeUnbufferedFileQStorage() {
    TFileQStorageSettings settings;
    settings.BufferUntilCommit = false;
    return MakeFileQStorage({}, settings);
}

IQStoragePtr MakeUnbufferedFileWithFlushIndexQStorage() {
    TFileQStorageSettings settings;
    settings.BufferUntilCommit = false;
    settings.AlwaysFlushIndex = true;
    return MakeFileQStorage({}, settings);
}

Y_UNIT_TEST_SUITE(TQStorageBufferedFileTests) {
    GENERATE_TESTS(MakeBufferedFileQStorage, false)
}

Y_UNIT_TEST_SUITE(TQStorageUnbufferedFileTests) {
    GENERATE_TESTS(MakeUnbufferedFileQStorage, false)
}

Y_UNIT_TEST_SUITE(TQStorageUnbufferedFileWithFlushIndexTests) {
    GENERATE_TESTS(MakeUnbufferedFileWithFlushIndexQStorage, true)
}
