#include "circlebufresize.h"
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/ptr.h>

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TResizableCircleBufTest) {

        TVector<int> Traverse(const TAllocFreeQueue<int> &q) {
            TAllocFreeQueue<int>::TIterator it(q);
            it.SeekToFirst();
            TVector<int> res;
            res.reserve(q.Size());
            while (it.Valid()) {
                res.push_back(it.Get());
                it.Next();
            }
            return res;
        }

        Y_UNIT_TEST(Test1) {
            TAllocFreeQueue<int> buf(10);
            UNIT_ASSERT(buf.Empty());
            UNIT_ASSERT(buf.Size() == 0);
            buf.Push(1);
            buf.Push(2);
            UNIT_ASSERT(buf.Back() == 2);
            buf.Push(3);
            UNIT_ASSERT(buf.Top() == 1);
            UNIT_ASSERT(Traverse(buf) == TVector<int>({1, 2, 3}));
            buf.Pop();
            UNIT_ASSERT(buf.Top() == 2);
            buf.Pop();
            UNIT_ASSERT(!buf.Empty());
            UNIT_ASSERT(buf.Size() == 1);
            UNIT_ASSERT(buf.Top() == 3);
            buf.Pop();
            UNIT_ASSERT(buf.Empty());
            UNIT_ASSERT(buf.Size() == 0);
            buf.Push(1);
            buf.Push(2);
            buf.Push(3);
            buf.Push(4);
            buf.Push(5);
            UNIT_ASSERT(Traverse(buf) == TVector<int>({1, 2, 3, 4, 5}));
            buf.Push(6);
            UNIT_ASSERT(buf.Top() == 1 && buf.Back() == 6);
            buf.Push(7);
            UNIT_ASSERT(buf.Top() == 1 && buf.Back() == 7);
            buf.Push(8);
            buf.Push(9);
            UNIT_ASSERT(buf.Size() == 9);
            UNIT_ASSERT(buf.Top() == 1 && buf.Back() == 9);
            buf.Push(10);
            UNIT_ASSERT(buf.Size() == 10);
            UNIT_ASSERT(buf.Top() == 1 && buf.Back() == 10);
            buf.Push(11);
            UNIT_ASSERT(buf.Size() == 11);
            UNIT_ASSERT(buf.Top() == 1 && buf.Back() == 11);
            buf.Pop();
            buf.Pop();
            UNIT_ASSERT(Traverse(buf) == TVector<int>({3, 4, 5, 6, 7, 8, 9, 10, 11}));
        }

        Y_UNIT_TEST(Test2) {
            TAllocFreeQueue<int> buf(8);
            UNIT_ASSERT(buf.Empty());
            buf.Push(1);
            buf.Push(2);
            buf.Push(3);
            for (int i = 0; i < 100; ++i) {
                buf.Push(i);
                buf.Pop();
                UNIT_ASSERT(buf.Size() == 3);
            }
            buf.Push(4);
            buf.Push(5);
            buf.Push(6);
            buf.Push(7);
            buf.Push(8);
            buf.Push(9);
            UNIT_ASSERT(buf.Capacity() == 16);
            UNIT_ASSERT(buf.Size() == 9);
        }
    }

} // NKikimr
