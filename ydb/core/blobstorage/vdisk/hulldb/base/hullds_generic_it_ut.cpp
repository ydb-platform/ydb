#include "hullds_generic_it.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>

// change to Cerr if you want logging
#define STR Cnull

using namespace NKikimr;

Y_UNIT_TEST_SUITE(THullDsGenericNWayIt) {

    Y_UNIT_TEST(ForwardIteration) {
        TIntContainer ds1(TVector<int>{1, 4, 5, 7, 8, 10});
        TIntContainer ds2(TVector<int>{2, 4, 5, 6, 8, 9, 10, 11, 15});
        TIntContainer ds3(TVector<int>{4, 6, 8, 9});

        TVector<int> allData;
        TGenericNWayForwardIterator<int, TVectorIt> it(nullptr, {&ds1, &ds2, &ds3});
        // full iteration
        it.SeekToFirst();
        while (it.Valid()) {
            STR << it.GetCurKey() << " ";
            allData.push_back(it.GetCurKey());
            it.Next();
        }
        STR << "\n";
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

        // copy iterator
        it.Seek(10);
        STR << "ToString: " << it.ToString() << "\n";
        TGenericNWayForwardIterator<int, TVectorIt> it2(it);
        UNIT_ASSERT(it2.Valid() && it2.GetCurKey() == 10);
        allData.clear();
        while (it2.Valid()) {
            allData.push_back(it2.GetCurKey());
            it2.Next();
        }
        UNIT_ASSERT_EQUAL(allData, TVector<int>({10, 11, 15}));
    }

    Y_UNIT_TEST(BackwardIteration) {
        TIntContainer ds1(TVector<int>{1, 4, 5, 7, 8, 10});
        TIntContainer ds2(TVector<int>{2, 4, 5, 6, 8, 9, 10, 11, 15});
        TIntContainer ds3(TVector<int>{4, 6, 8, 9});

        TVector<int> allData;
        TGenericNWayBackwardIterator<int, TVectorIt> it(nullptr, {&ds1, &ds2, &ds3});
        // full iteration
        it.Seek(17);
        while (it.Valid()) {
            STR << it.GetCurKey() << " ";
            allData.push_back(it.GetCurKey());
            it.Prev();
        }
        STR << "\n";
        UNIT_ASSERT_EQUAL(allData, TVector<int>({15, 11, 10, 9, 8, 7, 6, 5, 4, 2, 1}));

        // selected Seeks
        it.Seek(-1);
        UNIT_ASSERT(!it.Valid());
        it.Seek(1);
        UNIT_ASSERT(it.Valid() && it.GetCurKey() == 1);
        it.Seek(3);
        UNIT_ASSERT(it.Valid() && it.GetCurKey() == 2);
        it.Seek(10);
        UNIT_ASSERT(it.Valid() && it.GetCurKey() == 10);
        it.Seek(15);
        UNIT_ASSERT(it.Valid() && it.GetCurKey() == 15);
        it.Seek(20);
        UNIT_ASSERT(it.Valid() && it.GetCurKey() == 15);

        // copy iterator
        it.Seek(10);
        STR << "ToString: " << it.ToString() << "\n";
        TGenericNWayBackwardIterator<int, TVectorIt> it2(it);
        UNIT_ASSERT(it2.Valid() && it2.GetCurKey() == 10);
        allData.clear();
        while (it2.Valid()) {
            allData.push_back(it2.GetCurKey());
            it2.Prev();
        }
        UNIT_ASSERT_EQUAL(allData, TVector<int>({10, 9, 8, 7, 6, 5, 4, 2, 1}));
    }
}


