#include "user_info.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

  Y_UNIT_TEST_SUITE(TPQUserInfoTest) {
    Y_UNIT_TEST(UserDataDeprecatedSerializaion) {
        ui64 offset = 1234556789012345678ULL;
        ui32 gen = 987654321;
        ui32 step = 1234567890;
        const TString session = "some_source_id";
        TBuffer buffer = NDeprecatedUserData::Serialize(offset, gen, step, session);
        TString data(buffer.data(), buffer.size());
        {
            UNIT_ASSERT_EQUAL(data.size(), sizeof(offset) + sizeof(gen) + sizeof(step) + session.size());
            UNIT_ASSERT_EQUAL(offset, *reinterpret_cast<const ui64*>(data.c_str()));
            UNIT_ASSERT_EQUAL(gen, reinterpret_cast<const ui32*>(data.c_str() + sizeof(ui64))[0]);
            UNIT_ASSERT_EQUAL(step, reinterpret_cast<const ui32*>(data.c_str() + sizeof(ui64))[1]);
            UNIT_ASSERT_EQUAL(session, data.substr(sizeof(ui64) + 2 * sizeof(ui32)));
        }
        {
            ui64 parsedOffset = 0;
            ui32 parsedGen = 0;
            ui32 parsedStep = 0;
            TString parsedSession;
            NDeprecatedUserData::Parse(data, parsedOffset, parsedGen, parsedStep, parsedSession);
            UNIT_ASSERT_EQUAL(offset, parsedOffset);
            UNIT_ASSERT_EQUAL(gen, parsedGen);
            UNIT_ASSERT_EQUAL(step, parsedStep);
            UNIT_ASSERT_EQUAL(session, parsedSession);
        }
    }
  }
} // namespace NKikimr::NPQ
