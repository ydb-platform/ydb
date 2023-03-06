#include "ptr.h"
#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/testing/gtest/gtest.h>
#include <ydb/library/testlib/unittest_gtest_macro_subst.h>

using namespace NKikimr;

namespace {

    //////////////////////////////////////////////////////////////////////////////////////////
    // Basic test for TAtomicRefCountWithDeleter
    //////////////////////////////////////////////////////////////////////////////////////////
    class TSimpleTestDeleter {
    public:
        TSimpleTestDeleter()
            : Num(new std::atomic<ui64>())
        {
            Num->store(0);
        }

        template <class T>
        inline void Destroy(std::unique_ptr<T> t) noexcept {
            ++(*Num);
            CheckedDelete<T>(t.release());
        }

        ui64 GetNum() const {
            return Num->load();
        }

    private:
        std::shared_ptr<std::atomic<ui64>> Num;
    };

    class TTest1 : public TAtomicRefCountWithDeleter<TTest1, TSimpleTestDeleter> {
    public:
        TTest1(const TSimpleTestDeleter &deleter)
            : TAtomicRefCountWithDeleter<TTest1, TSimpleTestDeleter>(deleter)
        {}
    };

    using TTest1Ptr = TIntrusivePtr<TTest1>;

    TEST(PtrTest, Test1) {
        TSimpleTestDeleter deleter;

        UNIT_ASSERT_EQUAL(deleter.GetNum(), 0u );

        {
            TTest1Ptr ptr1 = new TTest1(deleter);
            TTest1Ptr ptr2(ptr1);
        }
        UNIT_ASSERT_EQUAL(deleter.GetNum(), 1u );

        {
            TTest1Ptr ptr1 = new TTest1(deleter);
            TTest1Ptr ptr2(ptr1);
        }
        UNIT_ASSERT_EQUAL(deleter.GetNum(), 2u );
    }
}

