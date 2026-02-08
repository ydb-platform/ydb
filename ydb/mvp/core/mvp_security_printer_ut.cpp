#include "mvp_security_printer.h"

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
        req.set_nc_cred("SECRET_CRED_123456");
        req.set_nc_sens("SECRET_SENS_ABCDEF");
        req.set_yc_sens("SECRET_YC_SENS_XYZ");
        req.set_int_value(42);
        req.set_int_nc_cred(98765);
        req.set_int_nc_sens(55555);
        req.set_int_yc_sens(45222);
        for (const auto& name : repeatedNames) {
            req.add_repeated_name(name);
        }
        for (const auto& s : maskedRCreds) {
            req.add_repeated_nc_cred(s);
        }
        for (const auto& s : maskedRSens) {
            req.add_repeated_nc_sens(s);
        }
        auto* nested = req.mutable_nested();
        nested->set_nested_name("nested_value");
        nested->set_nested_nc_cred("NSECRET");
        nested->set_nested_nc_sens("NSECRET2");
        nested->set_nested_yc_sens("NSECRET3");

        auto secure = NMVP::SecureShortDebugString(req);
        std::string actual(secure.data(), secure.size());

        TStringBuilder b;
        b << "name: \"value\""
          << " nc_cred: \"***\""
          << " nc_sens: \"***\""
          << " yc_sens: \"***\""
          << " int_value: 42"
          << " int_nc_cred: ***"
          << " int_nc_sens: ***"
          << " int_yc_sens: ***";
        for (const auto& name : repeatedNames) {
            b << " repeated_name: \"" << name << "\"";
        }
        for (const auto& s : maskedRCreds) {
            (void)s;
            b << " repeated_nc_cred: \"***\"";
        }
        for (const auto& s : maskedRSens) {
            (void)s;
            b << " repeated_nc_sens: \"***\"";
        }
        b << " nested {"
          << " nested_name: \"nested_value\""
          << " nested_nc_cred: \"***\""
          << " nested_nc_sens: \"***\""
          << " nested_yc_sens: \"***\""
          << " } ";
        TString expectedT = b;
        std::string expected(expectedT.data(), expectedT.size());
        UNIT_ASSERT(actual.find("int_nc_cred:") != std::string::npos);
        UNIT_ASSERT(actual.find("int_nc_sens:") != std::string::npos);
        UNIT_ASSERT(actual.find("int_yc_sens:") != std::string::npos);
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }

}
