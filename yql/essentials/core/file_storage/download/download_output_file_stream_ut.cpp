#include "download_limiter.h"
#include "download_output_file_stream.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/stream/file.h>
#include <util/system/tempfile.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TDownloadOutputTests) {
Y_UNIT_TEST(WriteAcrossQuotaRefill) {
    auto limiter = TDownloadLimiter(NSize::TSize(4_B));
    const TString content("\0abcd", 5);
    TTempFileHandle destination;
    TDownloadOutputFileStream output(destination, limiter);
    output.Write(content);
    output.Finish();
    UNIT_ASSERT_VALUES_EQUAL(TFileInput(destination.GetName()).ReadAll(), content);
    UNIT_ASSERT_VALUES_EQUAL(limiter.GetQuota(4), 3);
}

Y_UNIT_TEST(PreservesFilePositionWithoutBuffering) {
    auto limiter = TDownloadLimiter(NSize::TSize(100_B));
    TTempFileHandle destination;
    destination.Write("abc", 3);
    destination.Seek(1, sSet);
    TDownloadOutputFileStream output(destination, limiter);
    output.Write("Z");
    UNIT_ASSERT_VALUES_EQUAL(TFileInput(destination.GetName()).ReadAll(), "aZc");
    UNIT_ASSERT_VALUES_EQUAL(limiter.GetQuota(100), 99);
    output.Finish();
}

Y_UNIT_TEST(EmptyWriteDoesNotReserveQuota) {
    auto limiter = TDownloadLimiter(NSize::TSize(1_B));
    TTempFileHandle destination;
    TDownloadOutputFileStream output(destination, limiter);
    output.Write("");
    output.Finish();
    UNIT_ASSERT_VALUES_EQUAL(destination.GetLength(), 0);
    UNIT_ASSERT_VALUES_EQUAL(limiter.GetQuota(1), 1);
}

Y_UNIT_TEST(ZeroLimitFileOutput) {
    auto limiter = TDownloadLimiter(NSize::TSize(0_B));
    TTempFileHandle destination;
    TDownloadOutputFileStream output(destination, limiter);
    output.Write("abc");
    output.Finish();
    UNIT_ASSERT_VALUES_EQUAL(TFileInput(destination.GetName()).ReadAll(), "abc");
    UNIT_ASSERT_VALUES_EQUAL(limiter.GetQuota(Max<size_t>()), Max<size_t>() - 3);
}

} // Y_UNIT_TEST_SUITE(TDownloadOutputTests)

} // namespace NYql
