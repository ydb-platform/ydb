#include "fresh_appendix.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>

// change to Cerr if you want logging
#define STR Cnull

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TFreshAppendixTest) {

        static ::NMonitoring::TDynamicCounters DynCounters;

        std::shared_ptr<TFreshAppendix<int, int>> CreateAppendix(const TVector<int> &v) {
            auto group = DynCounters.GetSubgroup("subsystem", "memhull");
            TMemoryConsumer memConsumer(group->GetCounter("MemTotal:FreshIndex"));
            auto result = std::make_shared<TFreshAppendix<int, int>>(memConsumer);
            for (const auto &x : v) {
                result->Add(x, 0);
            }
            return result;
        }

        // add appendix to the tree and track lsns
        void AddAppendix(
                std::shared_ptr<TFreshAppendixTree<int, int>> c,
                std::shared_ptr<TFreshAppendix<int, int>> a,
                ui64 &curLsn) {
            ui64 firstLsn = curLsn;
            ui64 lastLsn = curLsn + a->GetSize() - 1;
            c->AddAppendix(a, firstLsn, lastLsn);
            curLsn += a->GetSize();
        }

        auto PrepareData() {
            auto a1 = CreateAppendix(TVector{4, 6, 7, 90, 102, 567});
            auto a2 = CreateAppendix(TVector{-1, 3, 4, 7, 56, 76,  90, 99, 102, 103, 104});
            auto a3 = CreateAppendix(TVector{7, 80, 90, 98, 102, 600});

            const size_t stagingCapacity = 4;
            auto c = std::make_shared<TFreshAppendixTree<int, int>>(nullptr, stagingCapacity);
            ui64 lsn = 1;
            AddAppendix(c, a1, lsn);
            AddAppendix(c, a2, lsn);
            AddAppendix(c, a3, lsn);

            return c->GetSnapshot();
        }

        // if seekValue is not defined, call SeekToFirst
        TVector<int> IterateForward(TMaybe<int> seekValue) {
            TVector<int> data;
            auto snap = PrepareData();
            TFreshAppendixTreeSnap<int, int>::TForwardIterator it(nullptr, &snap);
            // position iterator
            if (seekValue) {
                it.Seek(*seekValue);
            } else {
                it.SeekToFirst();
            }
            // iterate up to the end
            while (it.Valid()) {
                STR << it.GetCurKey() << " ";
                data.push_back(it.GetCurKey());
                it.Next();
            }
            STR << "\n";
            return data;
        }

        Y_UNIT_TEST(IterateForwardAll) {
            auto data = IterateForward({});
            UNIT_ASSERT_EQUAL(data, TVector<int>({-1, 3, 4, 6, 7, 56, 76, 80, 90, 98, 99, 102, 103, 104, 567, 600}));
        }

        Y_UNIT_TEST(IterateForwardIncluding) {
            auto data = IterateForward(TMaybe<int>(56));
            UNIT_ASSERT_EQUAL(data, TVector<int>({56, 76, 80, 90, 98, 99, 102, 103, 104, 567, 600}));
        }

        Y_UNIT_TEST(IterateForwardExcluding) {
            auto data = IterateForward(TMaybe<int>(60));
            UNIT_ASSERT_EQUAL(data, TVector<int>({76, 80, 90, 98, 99, 102, 103, 104, 567, 600}));
        }

        TVector<int> IterateBackward(int seekValue) {
            TVector<int> data;
            auto snap = PrepareData();
            TFreshAppendixTreeSnap<int, int>::TBackwardIterator it(nullptr, &snap);
            it.Seek(seekValue);
            while (it.Valid()) {
                STR << it.GetCurKey() << " ";
                data.push_back(it.GetCurKey());
                it.Prev();
            }
            STR << "\n";
            return data;
        }

        Y_UNIT_TEST(IterateBackwardAll) {
            auto data = IterateBackward(1000);
            UNIT_ASSERT_EQUAL(data, TVector<int>({600, 567, 104, 103, 102, 99, 98, 90, 80, 76, 56, 7, 6, 4, 3, -1}));
        }

        Y_UNIT_TEST(IterateBackwardIncluding) {
            auto data = IterateBackward(102);
            UNIT_ASSERT_EQUAL(data, TVector<int>({102, 99, 98, 90, 80, 76, 56, 7, 6, 4, 3, -1}));
        }

        Y_UNIT_TEST(IterateBackwardExcluding) {
            auto data = IterateBackward(100);
            UNIT_ASSERT_EQUAL(data, TVector<int>({99, 98, 90, 80, 76, 56, 7, 6, 4, 3, -1}));
        }
    }

} // NKikimr
