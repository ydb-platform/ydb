#include "proto_masking.h"

#include <ydb/mvp/core/ut/protos/masking_test.pb.h>
#include <ydb/library/security/util.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

Y_UNIT_TEST_SUITE(Masking) {
    Y_UNIT_TEST(Format) {
        mvp::masking::TestMask req;

        req.set_name("value");
        // req.no_value field doesn't set
        req.set_cred("SECRET_CRED_123456");
        req.set_sens("SECRET_SENS_ABCDEF");
        req.set_int_value(42);
        req.set_int_cred(98765);
        req.set_int_sens(55555);
        req.add_repeated_name("r1");
        req.add_repeated_name("r2");
        req.add_repeated_cred("RCRED1");
        req.add_repeated_cred("RCRED2");
        req.add_repeated_sens("RSENS1");
        auto* nested = req.mutable_nested();
        nested->set_nested_name("nested_value");
        nested->set_nested_cred("NSECRET");
        nested->set_nested_sens("NSECRET2");

        std::string actual = NMVP::MaskedShortDebugString(req);

        std::string maskedCred = NKikimr::MaskTicket(req.cred());
        std::string maskedSens = NKikimr::MaskTicket(req.sens());
        std::string maskedRCred1 = NKikimr::MaskTicket(req.repeated_cred(0));
        std::string maskedRCred2 = NKikimr::MaskTicket(req.repeated_cred(1));
        std::string maskedRSens1 = NKikimr::MaskTicket(req.repeated_sens(0));
        std::string maskedNestedCred = NKikimr::MaskTicket(nested->nested_cred());
        std::string maskedNestedSens = NKikimr::MaskTicket(nested->nested_sens());

        // Build expected string with TStringBuilder for readability
        TStringBuilder b;
        b << "name: \"value\""
          << " cred: \"" << maskedCred << "\""
          << " sens: \"" << maskedSens << "\""
          << " int_value: " << 42
          << " repeated_name: \"r1\""
          << " repeated_name: \"r2\""
          << " repeated_cred: \"" << maskedRCred1 << "\""
          << " repeated_cred: \"" << maskedRCred2 << "\""
          << " repeated_sens: \"" << maskedRSens1 << "\""
          << " nested { "
            "nested_name: \"nested_value\""
            " nested_cred: \"" << maskedNestedCred << "\""
            " nested_sens: \"" << maskedNestedSens << "\""
          << " }";
        TString expectedT = b;
        std::string expected(expectedT.data(), expectedT.size());

        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }

}
