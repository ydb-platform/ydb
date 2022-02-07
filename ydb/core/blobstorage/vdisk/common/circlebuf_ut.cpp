#include "circlebuf.h"
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/ptr.h>


namespace NKikimr {


    Y_UNIT_TEST_SUITE(TCircleBufTest) {

        Y_UNIT_TEST(SimpleTest) {
            TCircleBuf<int> buf(10);
            buf.Push(1);
            buf.Push(2);
            buf.Push(3);

            {
                TCircleBuf<int>::TIterator it = buf.Begin(), e = buf.End();
                UNIT_ASSERT(it != e);
                UNIT_ASSERT(*it == 1);
                ++it;
                UNIT_ASSERT(it != e);
                UNIT_ASSERT(*it == 2);
                ++it;
                UNIT_ASSERT(it != e);
                UNIT_ASSERT(*it == 3);
                ++it;
                UNIT_ASSERT(it == e);
            }

            buf.Push(4);

            {
                TCircleBuf<int>::TIterator it = buf.Begin(), e = buf.End();
                UNIT_ASSERT(it != e);
                UNIT_ASSERT(*it == 1);
                ++it;
                UNIT_ASSERT(it != e);
                UNIT_ASSERT(*it == 2);
                ++it;
                UNIT_ASSERT(it != e);
                UNIT_ASSERT(*it == 3);
                ++it;
                UNIT_ASSERT(it != e);
                UNIT_ASSERT(*it == 4);
                ++it;
                UNIT_ASSERT(it == e);
            }
        }

        Y_UNIT_TEST(EmptyTest) {
            TCircleBuf<int> buf(10);
            TCircleBuf<int>::TIterator it = buf.Begin(), e = buf.End();
            UNIT_ASSERT(it == e);
        }


        Y_UNIT_TEST(OverflowTest) {
            TCircleBuf<int> buf(3);
            buf.Push(1);
            buf.Push(2);
            buf.Push(3);

            {
                TCircleBuf<int>::TIterator it = buf.Begin(), e = buf.End();
                UNIT_ASSERT(it != e);
                UNIT_ASSERT(*it == 1);
                ++it;
                UNIT_ASSERT(it != e);
                UNIT_ASSERT(*it == 2);
                ++it;
                UNIT_ASSERT(it != e);
                UNIT_ASSERT(*it == 3);
                ++it;
                UNIT_ASSERT(it == e);
            }

            buf.Push(4);

            {
                TCircleBuf<int>::TIterator it = buf.Begin(), e = buf.End();
                UNIT_ASSERT(it != e);
                UNIT_ASSERT(*it == 2);
                ++it;
                UNIT_ASSERT(it != e);
                UNIT_ASSERT(*it == 3);
                ++it;
                UNIT_ASSERT(it != e);
                UNIT_ASSERT(*it == 4);
                ++it;
                UNIT_ASSERT(it == e);
            }
        }

        struct TStruct : public TThrRefBase {
            int X;
            TStruct(int x = 0)
                : X(x)
            {}
        };
        typedef TIntrusivePtr<TStruct> TStructPtr;

        Y_UNIT_TEST(PtrTest) {
            TCircleBuf<TStructPtr> buf(3);
            TStructPtr val1(new TStruct());
            buf.Push(val1);
            TStructPtr val2(new TStruct(5));
            buf.Push(val2);

            TCircleBuf<TStructPtr>::TIterator it = buf.Begin(), e = buf.End();
            UNIT_ASSERT(it != e);
            UNIT_ASSERT((*it)->X == 0);
            ++it;
            UNIT_ASSERT(it != e);
            UNIT_ASSERT((*it)->X == 5);
            ++it;
            UNIT_ASSERT(it == e);
        }
    }

} // NKikimr
