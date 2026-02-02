#include "proto_masking.h"

#include <ydb/mvp/core/protos/masking_test.pb.h>
#include <ydb/library/security/util.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

Y_UNIT_TEST_SUITE(Masking) {
    Y_UNIT_TEST(Format) {
        mvp::masking::TestMask req;

        req.set_name("value1");
        // req.no_value field doesn't set
        req.set_cred("SECRET_CRED_123456");
        req.set_sens("SECRET_SENS_ABCDEF");

        std::string actual = NMVP::MaskedShortDebugString(req);

        std::string maskedCred = NKikimr::MaskTicket(req.cred());
        std::string maskedSens = NKikimr::MaskTicket(req.sens());

        // Build expected string with TStringBuilder for readability
        TStringBuilder b;
        b << "name: \"value1\""
          << " cred: \"" << maskedCred << "\""
          << " sens: \"" << maskedSens << "\"";
        TString expectedT = b;
        std::string expected(expectedT.data(), expectedT.size());

        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }

}
