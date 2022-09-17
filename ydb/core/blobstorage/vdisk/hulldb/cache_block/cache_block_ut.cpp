#include "cache_block.h"
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>

//#define STR Cerr
#define STR Cnull

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TBlobStorageBlocksCacheTest) {

        Y_UNIT_TEST(LegacyAndModern) {
            TBlocksCache c;
            c.Build(nullptr);

            c.UpdateLegacy(17, {1, 0});
            UNIT_ASSERT(!c.IsBlockedLegacy(17, {2, 0}));
            c.UpdateLegacy(17, {2, 0});
            UNIT_ASSERT(c.IsBlockedLegacy(17, {2, 0}));


            c.UpdateInFlight(17, {3, 0}, 500);
            auto res = c.IsBlocked(17, {2, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);
            res = c.IsBlocked(17, {3, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_INFLIGH && res.Lsn == 500);
            c.CommitInFlight(17, {3, 0}, 500);
            res = c.IsBlocked(17, {3, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);
        }

        Y_UNIT_TEST(DeepInFlight) {
            TBlocksCache c;
            c.Build(nullptr);

            c.UpdateInFlight(17, {1, 0}, 400);
            c.CommitInFlight(17, {1, 0}, 400);
            auto res = c.IsBlocked(17, {1, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);

            c.UpdateInFlight(17, {2, 0}, 500);
            res = c.IsBlocked(17, {2, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_INFLIGH && res.Lsn == 500);

            c.UpdateInFlight(17, {3, 0}, 600);
            res = c.IsBlocked(17, {2, 0});
            // TODO could do better -- 500
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_INFLIGH && res.Lsn == 600);
            res = c.IsBlocked(17, {3, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_INFLIGH && res.Lsn == 600);
            res = c.IsBlocked(17, {4, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::OK);

            c.CommitInFlight(17, {2, 0}, 500);
            res = c.IsBlocked(17, {2, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);
            res = c.IsBlocked(17, {3, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_INFLIGH && res.Lsn == 600);
            res = c.IsBlocked(17, {4, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::OK);

            c.CommitInFlight(17, {3, 0}, 600);
            res = c.IsBlocked(17, {2, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);
            res = c.IsBlocked(17, {3, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);
            res = c.IsBlocked(17, {4, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::OK);
        }

        Y_UNIT_TEST(Repeat) {
            TBlocksCache c;
            c.Build(nullptr);

            c.UpdateInFlight(17, {1, 0}, 400);
            c.CommitInFlight(17, {1, 0}, 400);
            auto res = c.IsBlocked(17, {1, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);

            c.UpdateInFlight(17, {2, 0}, 500);
            res = c.IsBlocked(17, {2, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_INFLIGH && res.Lsn == 500);

            c.UpdateInFlight(17, {2, 0}, 501);
            res = c.IsBlocked(17, {2, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_INFLIGH && res.Lsn == 500);

            c.UpdateInFlight(17, {2, 0}, 502);
            res = c.IsBlocked(17, {2, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_INFLIGH && res.Lsn == 500);

            c.CommitInFlight(17, {2, 0}, 500);
            res = c.IsBlocked(17, {2, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);

            c.CommitInFlight(17, {2, 0}, 501);
            res = c.IsBlocked(17, {2, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);

            c.CommitInFlight(17, {2, 0}, 502);
            res = c.IsBlocked(17, {2, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);
        }

        Y_UNIT_TEST(PutIntoPast) {
            TBlocksCache c;
            c.Build(nullptr);

            c.UpdateInFlight(17, {2, 0}, 400);
            c.CommitInFlight(17, {2, 0}, 400);
            auto res = c.IsBlocked(17, {2, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);

            c.UpdateInFlight(17, {1, 0}, 410);
            c.CommitInFlight(17, {1, 0}, 410);
            res = c.IsBlocked(17, {2, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);

            c.UpdateInFlight(17, {2, 0}, 420);
            c.CommitInFlight(17, {2, 0}, 420);
            res = c.IsBlocked(17, {1, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);

            c.UpdateInFlight(17, {4, 0}, 430);
            res = c.IsBlocked(17, {4, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_INFLIGH && res.Lsn == 430);
            res = c.IsBlocked(17, {5, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::OK);
            c.UpdateInFlight(17, {3, 0}, 440);
            res = c.IsBlocked(17, {3, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_INFLIGH && res.Lsn == 430);

            c.CommitInFlight(17, {4, 0}, 430);
            res = c.IsBlocked(17, {4, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);
            res = c.IsBlocked(17, {3, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);
            res = c.IsBlocked(17, {5, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::OK);
            UNIT_ASSERT(c.IsInFlight());

            c.CommitInFlight(17, {3, 0}, 440);
            res = c.IsBlocked(17, {4, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);
            res = c.IsBlocked(17, {3, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);
            res = c.IsBlocked(17, {5, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::OK);
            UNIT_ASSERT(!c.IsInFlight());
        }

        Y_UNIT_TEST(PutDeepIntoPast) {
            TBlocksCache c;
            c.Build(nullptr);

            c.UpdateInFlight(17, {3, 0}, 400);
            c.CommitInFlight(17, {3, 0}, 400);
            auto res = c.IsBlocked(17, {3, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);

            c.UpdateInFlight(17, {4, 0}, 410);
            res = c.IsBlocked(17, {4, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_INFLIGH && res.Lsn == 410);

            c.UpdateInFlight(17, {2, 0}, 420);
            res = c.IsBlocked(17, {4, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_INFLIGH && res.Lsn == 410);

            c.CommitInFlight(17, {4, 0}, 410);
            res = c.IsBlocked(17, {4, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);

            c.CommitInFlight(17, {2, 0}, 420);
            res = c.IsBlocked(17, {4, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);
            UNIT_ASSERT(!c.IsInFlight());
        }

        Y_UNIT_TEST(MultipleTables) {
            TBlocksCache c;
            c.Build(nullptr);

            c.UpdateInFlight(17, {3, 0}, 400);
            c.UpdateInFlight(20, {5, 0}, 410);

            auto res = c.IsBlocked(17, {3, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_INFLIGH && res.Lsn == 400);
            res = c.IsBlocked(20, {5, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_INFLIGH && res.Lsn == 410);

            c.CommitInFlight(17, {3, 0}, 400);
            res = c.IsBlocked(17, {3, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);
            res = c.IsBlocked(20, {5, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_INFLIGH && res.Lsn == 410);

            c.CommitInFlight(20, {5, 0}, 410);
            res = c.IsBlocked(17, {3, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);
            res = c.IsBlocked(20, {5, 0});
            UNIT_ASSERT(res.Status == TBlocksCache::EStatus::BLOCKED_PERS);
            UNIT_ASSERT(!c.IsInFlight());
        }
    }

} // NKikimr
