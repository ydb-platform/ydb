#include <ydb/services/udf_store/udf_name.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NUdfStore;

Y_UNIT_TEST_SUITE(TUdfNameTest) {

Y_UNIT_TEST(AcceptsPlainModuleNames) {
    UNIT_ASSERT(IsSafeUdfFileName("Md5"));
    UNIT_ASSERT(IsSafeUdfFileName("libmy_udf-2.so"));
    UNIT_ASSERT(IsSafeUdfFileName("d41d8cd98f00b204e9800998ecf8427e"));
}

Y_UNIT_TEST(RejectsNamesThatEscapeTheOutputDirectory) {
    // The name is the modules table primary key, so it is whatever the user
    // typed; joined onto a directory it must not name anything outside it.
    UNIT_ASSERT(!IsSafeUdfFileName(""));
    UNIT_ASSERT(!IsSafeUdfFileName("."));
    UNIT_ASSERT(!IsSafeUdfFileName(".."));
    UNIT_ASSERT(!IsSafeUdfFileName("../../etc/ld.so.preload"));
    UNIT_ASSERT(!IsSafeUdfFileName("nested/name"));
    UNIT_ASSERT(!IsSafeUdfFileName("/absolute"));
    UNIT_ASSERT(!IsSafeUdfFileName(TStringBuf("nul\0byte", 8)));
    UNIT_ASSERT(!IsSafeUdfFileName(TString(4096, 'x')));
}

} // Y_UNIT_TEST_SUITE
