#include "hullds_generic_it.h"
#include "hullds_heap_it.h"
#include "hullds_ut.h"
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>

// change to Cerr if you want logging
#define STR Cnull


namespace NKikimr {

    Y_UNIT_TEST_SUITE(THullDsHeapItTest) {

        Y_UNIT_TEST(HeapForwardIteration) {
            TIntContainer ds1(TVector<int>{1, 4, 5, 7, 8, 10});
            TIntContainer ds2(TVector<int>{2, 4, 5, 6, 8, 9, 10, 11, 15});
            TIntContainer ds3(TVector<int>{4, 6, 8, 9});

            TVector<int> allData;
            TVectorIt it1(nullptr, &ds1);
            TVectorIt it2(nullptr, &ds2);
            TVectorIt it3(nullptr, &ds3);
            THeapIterator<int, TVectorIt, true> it;
            
            it1.PutToHeap(it);
            it2.PutToHeap(it);
            it3.PutToHeap(it);

            it.SeekToFirst();
            // full iteration
            while (it.Valid()) {
                STR << it.GetCurKey() << " " << Endl;
                allData.push_back(it.GetCurKey());
                it.Next();
            }
            STR << "\n" << Endl;
            UNIT_ASSERT_EQUAL(allData, TVector<int>({1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 15}));
            
            // selected Seeks
            it.Seek(-1);
            UNIT_ASSERT(it.Valid() && it.GetCurKey() == 1);
            it.Seek(1);
            UNIT_ASSERT(it.Valid() && it.GetCurKey() == 1);
            it.Seek(3);
            UNIT_ASSERT(it.Valid() && it.GetCurKey() == 4);
            it.Seek(10);
            UNIT_ASSERT(it.Valid() && it.GetCurKey() == 10);
            it.Seek(15);
            UNIT_ASSERT(it.Valid() && it.GetCurKey() == 15);
            it.Seek(20);
            UNIT_ASSERT(!it.Valid());
        }

        Y_UNIT_TEST(HeapBackwardIteration) {
            TIntContainer ds1(TVector<int>{1, 4, 5, 9, 12, 14});
            TIntContainer ds2(TVector<int>{2, 4, 6, 8, 9, 15, 18, 19});
            TIntContainer ds3(TVector<int>{3, 11, 20, 22});
            TIntContainer ds4(TVector<int>{1, 5, 7, 11});
            TGenericNWayBackwardIterator<int, TVectorIt> it1(nullptr, {&ds1, &ds2});
            TGenericNWayBackwardIterator<int, TVectorIt> it2(nullptr, {&ds3, &ds4});
            
            TVector<int> allData;
            THeapIterator<int, TVectorIt, false> it;
            it1.PutToHeap(it);
            it2.PutToHeap(it);
            // full iteration
            it.Seek(24);
            while (it.Valid()) {
                STR << it.GetCurKey() << " ";
                allData.push_back(it.GetCurKey());
                it.Prev();
            }
            STR << "\n";
            UNIT_ASSERT_EQUAL(allData, TVector<int>({22, 20, 19, 18, 15, 14, 12, 11, 9, 8, 7, 6, 5, 4, 3, 2, 1}));

            // selected Seeks
            it.Seek(-1);
            UNIT_ASSERT(!it.Valid());
            it.Seek(1);
            UNIT_ASSERT(it.Valid() && it.GetCurKey() == 1);
            it.Seek(10);
            UNIT_ASSERT(it.Valid() && it.GetCurKey() == 9);
            it.Seek(15);
            UNIT_ASSERT(it.Valid() && it.GetCurKey() == 15);
            it.Seek(17);
            UNIT_ASSERT(it.Valid() && it.GetCurKey() == 15);
            it.Seek(25);
            UNIT_ASSERT(it.Valid() && it.GetCurKey() == 22);

            // copy iterator
            // it.Seek(10);
            // STR << "ToString: " << it.ToString() << "\n";
            // TGenericNWayBackwardIterator<int, TVectorIt> it2(it);
            // UNIT_ASSERT(it2.Valid() && it2.GetCurKey() == 10);
            // allData.clear();
            // while (it2.Valid()) {
            //     allData.push_back(it2.GetCurKey());
            //     it2.Prev();
            // }
            // UNIT_ASSERT_EQUAL(allData, TVector<int>({10, 9, 8, 7, 6, 5, 4, 2, 1}));
        }
    }

} // NKikimr

