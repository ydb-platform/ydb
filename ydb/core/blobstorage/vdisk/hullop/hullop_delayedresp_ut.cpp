#include "hullop_delayedresp.h"
#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/null.h>

#define STR Cnull

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TDelayedResponsesTests) {

        Y_UNIT_TEST(Test) {
            TVector<ui64> res;
            auto action = [&res] (const TActorId &actorId, ui64 cookie, NWilson::TTraceId, IEventBase *msg) {
                Y_UNUSED(actorId);
                STR << "cookie# " << cookie << "\n";
                res.push_back(cookie);
                delete msg;
            };

            auto dr = std::make_unique<TDelayedResponses>();
            dr->Put(nullptr, TActorId(), 1, {}, 500);
            dr->Put(nullptr, TActorId(), 2, {}, 500);
            dr->Put(nullptr, TActorId(), 3, {}, 501);
            dr->Put(nullptr, TActorId(), 4, {}, 500);
            dr->ConfirmLsn(500, action);
            dr->Put(nullptr, TActorId(), 5, {}, 502);
            dr->ConfirmLsn(501, action);
            dr->ConfirmLsn(502, action);

            UNIT_ASSERT_EQUAL(res, TVector<ui64>({1, 2, 4, 3, 5}));
        }

    }

} // NKikimr
