#include "proto_masking.h"

#include <ydb/mvp/core/ut/protos/masking_test.pb.h>
#include <ydb/library/security/util.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

Y_UNIT_TEST_SUITE(Masking) {
    Y_UNIT_TEST(Format) {
        std::vector<std::string> repeatedNames;
        std::vector<std::string> maskedRCreds;
        std::vector<std::string> maskedRSens;

        repeatedNames.push_back("r1");
        repeatedNames.push_back("r2");
        maskedRCreds.push_back("RCRED1");
        maskedRCreds.push_back("RCRED2");
        maskedRSens.push_back("RSENS1");
        maskedRSens.push_back("RSENS2");

        mvp::masking::TestMask req;
        req.set_name("value");
        // req.no_value field doesn't set
        req.set_cred("SECRET_CRED_123456");
        req.set_sens("SECRET_SENS_ABCDEF");
        req.set_int_value(42);
        req.set_int_cred(98765);
        req.set_int_sens(55555);
        for (const auto& name : repeatedNames) {
            req.add_repeated_name(name);
        }
        for (const auto& s : maskedRCreds) {
            req.add_repeated_cred(s);
        }
        for (const auto& s : maskedRSens) {
            req.add_repeated_sens(s);
        }
        auto* nested = req.mutable_nested();
        nested->set_nested_name("nested_value");
        nested->set_nested_cred("NSECRET");
        nested->set_nested_sens("NSECRET2");

        std::string actual = NMVP::MaskedShortDebugString(req);

        // Build expected string with TStringBuilder for readability
        TStringBuilder b;
        b << "name: \"value\""
          << " cred: \"" << NKikimr::MaskTicket(req.cred()) << "\""
          << " sens: \"" << NKikimr::MaskTicket(req.sens()) << "\""
          << " int_value: " << 42;
        for (const auto& name : repeatedNames) {
            b << " repeated_name: \"" << name << "\"";
        }
        for (const auto& s : maskedRCreds) {
            b << " repeated_cred: \"" << NKikimr::MaskTicket(s) << "\"";
        }
        for (const auto& s : maskedRSens) {
            b << " repeated_sens: \"" << NKikimr::MaskTicket(s) << "\"";
        }
        b << " nested { "
          << "nested_name: \"nested_value\""
          << " nested_cred: \"" << NKikimr::MaskTicket(nested->nested_cred()) << "\""
          << " nested_sens: \"" << NKikimr::MaskTicket(nested->nested_sens()) << "\""
          << " }";
        TString expectedT = b;
        std::string expected(expectedT.data(), expectedT.size());

        UNIT_ASSERT(actual.find("int_cred:") == std::string::npos);
        UNIT_ASSERT(actual.find("int_sens:") == std::string::npos);
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }

}
