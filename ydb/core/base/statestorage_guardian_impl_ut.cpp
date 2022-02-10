#include "defs.h"
#include "statestorage_guardian_impl.h"
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/ptr.h>

namespace NKikimr {
    namespace NStateStorageGuardian {

        Y_UNIT_TEST_SUITE(TGuardianImpl) {
            Y_UNIT_TEST(FollowerTracker) {
                TFollowerTracker tracker(2);

                {
                    TVector<TActorId> followers = { TActorId(1, 1, 1, 1), TActorId(1, 1, 5, 1), TActorId(1, 1, 10, 1) };
                    UNIT_ASSERT(tracker.Merge(0, followers) == true);
                }
                {
                    TVector<TActorId> followers = { TActorId(1, 1, 1, 1), TActorId(1, 1, 5, 1), TActorId(1, 1, 10, 1) };
                    UNIT_ASSERT(tracker.Merge(1, followers) == false);
                }

                {
                    TVector<TActorId> followers = { TActorId(1, 1, 1, 1) };
                    UNIT_ASSERT(tracker.Merge(0, followers) == false);
                }
                {
                    TVector<TActorId> followers = { TActorId(1, 1, 1, 1), TActorId(1, 1, 5, 1) };
                    UNIT_ASSERT(tracker.Merge(1, followers) == true);
                }

                auto merged = tracker.GetMerged();
                UNIT_ASSERT(merged.size() == 2);
                UNIT_ASSERT(merged.FindPtr(TActorId(1, 1, 1, 1)) != nullptr);
                UNIT_ASSERT(merged.FindPtr(TActorId(1, 1, 5, 1)) != nullptr);
                UNIT_ASSERT(merged.FindPtr(TActorId(1, 1, 10, 1)) == nullptr);
            }

            Y_UNIT_TEST(FollowerTrackerDuplicates) {
                TFollowerTracker tracker(1);

                {
                    TVector<TActorId> followers = { TActorId(1, 1, 1, 1), TActorId(1, 1, 5, 1), TActorId(1, 1, 10, 1) };
                    UNIT_ASSERT(tracker.Merge(0, followers) == true);
                }
                {
                    TVector<TActorId> followers = { TActorId(1, 1, 1, 1), TActorId(1, 1, 10, 1), TActorId(1, 1, 10, 1) };
                    UNIT_ASSERT(tracker.Merge(0, followers) == true);
                    UNIT_ASSERT(tracker.GetMerged().size() == 2);
                }
                {
                    TVector<TActorId> followers = { TActorId(1, 1, 1, 1), TActorId(1, 1, 11, 1), TActorId(1, 1, 11, 1) };
                    UNIT_ASSERT(tracker.Merge(0, followers) == true);
                    UNIT_ASSERT(tracker.GetMerged().size() == 2);
                }
                {
                    TVector<TActorId> followers = { TActorId(1, 1, 1, 1), TActorId(1, 1, 5, 1) };
                    UNIT_ASSERT(tracker.Merge(0, followers) == true);
                    UNIT_ASSERT(tracker.GetMerged().size() == 2);
                }
            }
        }
    }
}
