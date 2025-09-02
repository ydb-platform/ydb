#include "compression.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(CompressionTests) {
    void Check(const TString& compression, Ydb::StatusIds::StatusCode expectedStatus) {
        Ydb::StatusIds::StatusCode status;
        TString error;
        CheckCompression(compression, status, error);
        UNIT_ASSERT_VALUES_EQUAL_C(status, expectedStatus, error);
    }

    void Fill(const TString& compression, const TString& expectedCodec, int expectedLevel) {
        NKikimrSchemeOp::TBackupTask::TCompressionOptions proto;
        FillCompression(proto, compression);
        UNIT_ASSERT_VALUES_EQUAL(proto.GetCodec(), expectedCodec);
        UNIT_ASSERT_VALUES_EQUAL(proto.GetLevel(), expectedLevel);
    }

    Y_UNIT_TEST(Zstd) {
        Check("zstd", Ydb::StatusIds::SUCCESS);
        Check("zstd-1", Ydb::StatusIds::SUCCESS);
        Check("zstd-11", Ydb::StatusIds::SUCCESS);
        Check("zstd-22", Ydb::StatusIds::SUCCESS);
    
        Check("zstd--1", Ydb::StatusIds::BAD_REQUEST);
        Check("zstd-foo", Ydb::StatusIds::BAD_REQUEST);
        Check("zstd-", Ydb::StatusIds::BAD_REQUEST);
        Check("zstd-2147483648", Ydb::StatusIds::BAD_REQUEST);
        Check("zstd--2147483649", Ydb::StatusIds::BAD_REQUEST);
        Check("zstd-0", Ydb::StatusIds::BAD_REQUEST);

        Fill("zstd", "zstd", 0);
        Fill("zstd-1", "zstd", 1);
    }

    Y_UNIT_TEST(Unsupported) {
        Check("foo", Ydb::StatusIds::UNSUPPORTED);
        Check("zstdfoo", Ydb::StatusIds::UNSUPPORTED);
    }
}

} // NKikimr
