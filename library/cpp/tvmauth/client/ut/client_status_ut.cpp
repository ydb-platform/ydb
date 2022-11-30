#include <library/cpp/tvmauth/client/client_status.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NTvmAuth;

Y_UNIT_TEST_SUITE(ClientStatus) {
    Y_UNIT_TEST(Common) {
        UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, TClientStatus().GetCode());
        UNIT_ASSERT_VALUES_EQUAL("", TClientStatus().GetLastError());

        UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Warning, TClientStatus(TClientStatus::Warning, "kek"));
        UNIT_ASSERT_VALUES_EQUAL("kek",
                                 TClientStatus(TClientStatus::Warning, "kek").GetLastError());
        UNIT_ASSERT_VALUES_EQUAL("2;TvmClient: kek\n",
                                 TClientStatus(TClientStatus::Error, "kek").CreateJugglerMessage());
    }
}
