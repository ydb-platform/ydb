#include <ydb/library/pdisk_io/file_params.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/file.h>
#include <util/system/tempfile.h>

namespace NKikimr {
namespace {

int NextAvailableDescriptor() {
    const TFileHandle probe("/dev/null", OpenExisting | RdWr);
    UNIT_ASSERT(probe.IsOpen());
    return probe;
}

} // namespace

Y_UNIT_TEST_SUITE(TFileParametersTest) {
    // Keep other tests' threads from opening descriptors during the comparison.
    SIMPLE_UNIT_FORKED_TEST(ClosesDescriptorOnUnsupportedFileType) {
        const int before = NextAvailableDescriptor();
        for (ui32 attempt = 0; attempt < 32; ++attempt) {
            ui64 size = 0;
            bool isBlockDevice = false;
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                DetectFileParameters("/dev/null", size, isBlockDevice),
                yexception, "Unknown file type");
        }
        UNIT_ASSERT_VALUES_EQUAL(NextAvailableDescriptor(), before);
    }

    SIMPLE_UNIT_FORKED_TEST(DetectsRegularFileAndClosesDescriptor) {
        TTempFileHandle file = TTempFileHandle::InCurrentDir("file-params");
        file.Resize(4096);
        const int before = NextAvailableDescriptor();
        ui64 size = 0;
        bool isBlockDevice = true;
        DetectFileParameters(file.Name(), size, isBlockDevice);
        UNIT_ASSERT_VALUES_EQUAL(size, 4096);
        UNIT_ASSERT(!isBlockDevice);
        UNIT_ASSERT_VALUES_EQUAL(NextAvailableDescriptor(), before);
    }
}

} // namespace NKikimr
