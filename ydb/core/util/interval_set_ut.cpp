#include "interval_set.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

class TReferenceSet {
    ui64 Bits = 0;
public:

    TReferenceSet() {
        Bits = 0;
    }

    TReferenceSet(ui64 bits) {
        Y_ABORT_UNLESS((bits & 0x7f) == bits);
        Bits = bits;
    }

    TReferenceSet(const TReferenceSet &b) {
        Bits = b.Bits;
    }

    TReferenceSet(i64 begin, i64 end) {
        Bits = 0;
        Add(begin, end);
    }

    void Add(const TReferenceSet &b) {
        Bits |= b.Bits;
    }

    void Add(i64 begin, i64 end) {
        Y_ABORT_UNLESS(begin >= 0 && begin < 8);
        Y_ABORT_UNLESS(end >= 0 && end <= 8);
        for (i64 i = begin; i < end; ++i) {
            Bits |= (1 << i);
        }
    }

    void Clear() {
        Bits = 0;
    }

    bool IsEmpty() const {
        return Bits == 0;
    }

    bool IsSubsetOf(const TReferenceSet &b) const {
        return ((b.Bits & Bits) == Bits);
    }

    void Subtract(const TReferenceSet &b) {
        Bits = (Bits & ~b.Bits);
    }

    void Subtract(i64 begin, i64 end) {
        Y_ABORT_UNLESS(begin >= 0 && begin < 8);
        Y_ABORT_UNLESS(end >= 0 && end <= 8);
        for (i64 i = begin; i < end; ++i) {
            Bits &= ~(1ull << i);
        }
    }

    template <class TIntervals>
    TIntervals ToIntervalSet() const {
        TIntervals set;
        i64 prev = Max<i64>();
        for (i64 i = 0; i <= 8; ++i) {
            if (Bits & (1 << i)) {
                if (prev == Max<i64>()) {
                    prev = i;
                }
            } else {
                if (prev != Max<i64>()) {
                    set.Add(prev, i);
                    prev = Max<i64>();
                }
            }
        }
        return set;
    }

    TString ToString() const {
        TStringStream str;
        str << "{";
        bool isFirst = true;
        i64 prev = Max<i64>();
        for (i64 i = 0; i <= 8; ++i) {
            if (Bits & (1 << i)) {
                if (prev == Max<i64>()) {
                    prev = i;
                    if (!isFirst) {
                        str << " U ";
                    } else {
                        isFirst = false;
                    }
                }
            } else {
                if (prev != Max<i64>()) {
                    str << "[" << prev << ", " << i << ")";
                    prev = Max<i64>();
                }
            }
        }
        str << "}";
        return str.Str();
    }

    void Verify() const {
        Y_ABORT_UNLESS((Bits & 0x7f) == Bits);
    }

    size_t Size() const {
        size_t size = 0;
        bool isInterval = false;
        for (i64 i = 0; i <= 8; ++i) {
            if (Bits & (1 << i)) {
                if (!isInterval) {
                    isInterval = true;
                    size++;
                }
            } else {
                isInterval = false;
            }
        }
        return size;
    }

    friend bool operator ==(const TReferenceSet& x, const TReferenceSet& y) {
        return x.Bits == y.Bits;
    }

};

template <class TIntervals>
TIntervals MakeIntervalSet(ui64 n, ui32 len) {
    TIntervals res;
    for (ui32 i = 0; i < len; ) {
        if (n >> i & 1) {
            const ui32 begin = i;
            while (i < len && n >> i & 1) {
                ++i;
            }
            res.Add(begin, i);
        } else {
            ++i;
        }
    }
    return res;
}

#define MY_UNIT_TEST(N)      \
    template <class TIntervals, const char* TestName>       \
    struct TTestCase##N : public TCurrentTestCase {         \
        TTestCase##N() {                                    \
            Name_ = TestName;                               \
            ForceFork_ = false;                             \
        }                                                   \
        static THolder<NUnitTest::TBaseTestCase> Create() { \
            return ::MakeHolder<TTestCase##N>();            \
        }                                                   \
        void Execute_(NUnitTest::TTestContext&) override;   \
    };                                                      \
    struct TTestRegistration##N {                           \
        TTestRegistration##N() {                            \
            static const char NameMap[] = "IntervalMap" #N; \
            static const char NameVec[] = "IntervalVec" #N; \
            static const char NameSet[] = "IntervalSet" #N; \
            TCurrentTest::AddTest(TTestCase##N<TIntervalMap<i32>, NameMap>::Create); \
            TCurrentTest::AddTest(TTestCase##N<TIntervalVec<i32>, NameVec>::Create); \
            TCurrentTest::AddTest(TTestCase##N<TIntervalSet<i32, 2>, NameSet>::Create); \
        }                                                   \
    };                                                      \
    static TTestRegistration##N testRegistration##N;        \
    template <class TIntervals, const char* type_name>      \
    void TTestCase##N<TIntervals, type_name>::Execute_(NUnitTest::TTestContext& ut_context Y_DECLARE_UNUSED)

Y_UNIT_TEST_SUITE(TIntervalSetTest) {
    MY_UNIT_TEST(TestEmpty) {
        TIntervals a;
        UNIT_ASSERT_EQUAL(a.IsEmpty(), true);
        a.Add(0, 1);
        UNIT_ASSERT_EQUAL(a.IsEmpty(), false);
        a.Subtract(0, 1);
        UNIT_ASSERT_EQUAL(a.IsEmpty(), true);
    }

    MY_UNIT_TEST(TestSpecificAdd) {
        TIntervals a;
        a.Add(55, 56);
        a.Add(50, 80);
        TIntervals b(50, 80);
        UNIT_ASSERT_EQUAL_C(b.IsSubsetOf(a), true, "a# " << a.ToString() << " b# " << b.ToString());
        UNIT_ASSERT_EQUAL_C(a.IsSubsetOf(b), true, "a# " << a.ToString() << " b# " << b.ToString());
        b.Subtract(a);
        UNIT_ASSERT_EQUAL_C(b.IsEmpty(), true, "a# " << a.ToString() << " b# " << b.ToString());
    }

    MY_UNIT_TEST(TestAdd) {
        TIntervals a(0, 10);
        a.Add(20, 22);
        a.Add(44, 80);
        a.Add(90, 100);
        TVector<TIntervals> sets;
        sets.emplace_back(0, 10);
        sets.emplace_back(20, 22);
        sets.emplace_back(44, 50);
        sets.emplace_back(46, 49);
        sets.emplace_back(46, 56);
        sets.emplace_back(55, 56);
        sets.emplace_back(50, 80);
        sets.emplace_back(90, 100);

        TVector<ui32> counters;
        counters.resize(sets.size() * 2 / 3);
        for (ui32 i = 0; i < counters.size(); ++i) {
            counters[i] = 0;
        }

        while (counters.back() < sets.size()) {
            ui64 added = 0;
            //Cerr << "set" << Endl;
            TIntervals b;
            for (ui32 i = 0; i < counters.size(); ++i) {
                added |= (1ull << counters[i]);
                b.Add(sets[counters[i]]);
            }
            for (ui32 i = 0; i < sets.size(); ++i) {
                if (!(added & (1ull << i))) {
                    b.Add(sets[i]);
                }
            }

            UNIT_ASSERT_EQUAL_C(b.IsSubsetOf(a), true, "a# " << a.ToString() << " b# " << b.ToString());
            UNIT_ASSERT_EQUAL_C(a.IsSubsetOf(b), true, "a# " << a.ToString() << " b# " << b.ToString());
            b.Subtract(a);
            UNIT_ASSERT_EQUAL_C(b.IsEmpty(), true, "a# " << a.ToString() << " b# " << b.ToString());

            for (ui32 i = 0; i < counters.size(); ++i) {
                counters[i]++;
                if (counters[i] >= sets.size()) {
                    if (i == counters.size() - 1) {
                        break;
                    }
                    for (ui32 n = 0; n <= i; ++n) {
                        counters[n] = 0;
                    }
                } else {
                    break;
                }
            }
        }
    }

    MY_UNIT_TEST(TestAddSubtract) {
        TIntervals a;
        a.Add(4, 5);   // [4, 5)
        {
            TIntervals b(4, 5);
            UNIT_ASSERT_EQUAL(b.IsSubsetOf(a), true);
            UNIT_ASSERT_EQUAL(a.IsSubsetOf(b), true);
            b.Subtract(a);
            UNIT_ASSERT_EQUAL(b.IsEmpty(), true);
        }
        a.Add(1, 2);   // [1, 2) U [4, 5)
        {
            TIntervals b(1, 2);
            UNIT_ASSERT_EQUAL_C(b.IsSubsetOf(a), true, "a# " << a.ToString() << " b# " << b.ToString());
            UNIT_ASSERT_EQUAL_C(a.IsSubsetOf(b), false, "a# " << a.ToString() << " b# " << b.ToString());
            b.Subtract(a);
            UNIT_ASSERT_EQUAL(b.IsEmpty(), true);
        }
        {
            TIntervals b(4, 5);
            UNIT_ASSERT_EQUAL(b.IsSubsetOf(a), true);
            UNIT_ASSERT_EQUAL(a.IsSubsetOf(b), false);
            b.Subtract(a);
            UNIT_ASSERT_EQUAL(b.IsEmpty(), true);
        }
        a.Add(2, 4);   // [1, 5)
        {
            TIntervals b(1, 5);
            UNIT_ASSERT_EQUAL_C(b.IsSubsetOf(a), true, "a# " << a.ToString() << " b# " << b.ToString());
            UNIT_ASSERT_EQUAL_C(a.IsSubsetOf(b), true, "a# " << a.ToString() << " b# " << b.ToString());
            b.Subtract(a);
            UNIT_ASSERT_EQUAL(b.IsEmpty(), true);
        }
        a.Add(0, 1);   // [0, 5)
        {
            TIntervals b(0, 5);
            UNIT_ASSERT_EQUAL(b.IsSubsetOf(a), true);
            UNIT_ASSERT_EQUAL(a.IsSubsetOf(b), true);
            b.Subtract(a);
            UNIT_ASSERT_EQUAL(b.IsEmpty(), true);
        }
        a.Add(5, 6);   // [0, 6)
        {
            TIntervals b(0, 6);
            UNIT_ASSERT_EQUAL_C(b.IsSubsetOf(a), true, "a# " << a.ToString() << " b# " << b.ToString());
            UNIT_ASSERT_EQUAL(a.IsSubsetOf(b), true);
            b.Subtract(a);
            UNIT_ASSERT_EQUAL(b.IsEmpty(), true);
        }
        a.Add(-1, 1);  // [-1, 6)
        {
            TIntervals b(-1, 6);
            UNIT_ASSERT_EQUAL(b.IsSubsetOf(a), true);
            UNIT_ASSERT_EQUAL(a.IsSubsetOf(b), true);
            b.Subtract(a);
            UNIT_ASSERT_EQUAL(b.IsEmpty(), true);
        }
        a.Add(5, 7);   // [-1, 7)
        {
            TIntervals b(-1, 7);
            UNIT_ASSERT_EQUAL_C(b.IsSubsetOf(a), true, "a# " << a.ToString() << " b# " << b.ToString());
            UNIT_ASSERT_EQUAL_C(a.IsSubsetOf(b), true, "a# " << a.ToString() << " b# " << b.ToString());
            b.Subtract(a);
            UNIT_ASSERT_EQUAL(b.IsEmpty(), true);
        }
        a.Add(-1, 7);  // [-1, 7)
        {
            TIntervals b(-1, 7);
            UNIT_ASSERT_EQUAL(b.IsSubsetOf(a), true);
            UNIT_ASSERT_EQUAL(a.IsSubsetOf(b), true);
            b.Subtract(a);
            UNIT_ASSERT_EQUAL(b.IsEmpty(), true);
        }
        a.Add(-1, 1);  // [-1, 7)
        {
            TIntervals b(-1, 7);
            UNIT_ASSERT_EQUAL(b.IsSubsetOf(a), true);
            UNIT_ASSERT_EQUAL(a.IsSubsetOf(b), true);
            b.Subtract(a);
            UNIT_ASSERT_EQUAL(b.IsEmpty(), true);
        }
        a.Add(1, 7);   // [-1, 7)
        {
            TIntervals b(-1, 7);
            UNIT_ASSERT_EQUAL(b.IsSubsetOf(a), true);
            UNIT_ASSERT_EQUAL(a.IsSubsetOf(b), true);
            b.Subtract(a);
            UNIT_ASSERT_EQUAL(b.IsEmpty(), true);
        }
        a.Subtract(-3, -2); // [-1, 7)
        {
            TIntervals b(-1, 7);
            UNIT_ASSERT_EQUAL(b.IsSubsetOf(a), true);
            UNIT_ASSERT_EQUAL(a.IsSubsetOf(b), true);
            b.Subtract(a);
            UNIT_ASSERT_EQUAL(b.IsEmpty(), true);
        }
        a.Subtract(-3, -1); // [-1, 7)
        {
            TIntervals b(-1, 7);
            UNIT_ASSERT_EQUAL(b.IsSubsetOf(a), true);
            UNIT_ASSERT_EQUAL(a.IsSubsetOf(b), true);
            b.Subtract(a);
            UNIT_ASSERT_EQUAL(b.IsEmpty(), true);
        }
        a.Subtract(-3, 0);  // [0, 7)
        {
            TIntervals b(0, 7);
            UNIT_ASSERT_EQUAL(b.IsSubsetOf(a), true);
            UNIT_ASSERT_EQUAL(a.IsSubsetOf(b), true);
            b.Subtract(a);
            UNIT_ASSERT_EQUAL(b.IsEmpty(), true);
        }
        a.Subtract(8, 9);   // [0, 7)
        {
            TIntervals b(0, 7);
            UNIT_ASSERT_EQUAL(b.IsSubsetOf(a), true);
            UNIT_ASSERT_EQUAL(a.IsSubsetOf(b), true);
            b.Subtract(a);
            UNIT_ASSERT_EQUAL(b.IsEmpty(), true);
        }
        a.Subtract(7, 9);   // [0, 7)
        {
            TIntervals b(0, 7);
            UNIT_ASSERT_EQUAL(b.IsSubsetOf(a), true);
            UNIT_ASSERT_EQUAL(a.IsSubsetOf(b), true);
            b.Subtract(a);
            UNIT_ASSERT_EQUAL(b.IsEmpty(), true);
        }
        a.Subtract(6, 9);   // [0, 6)
        {
            TIntervals b(0, 6);
            UNIT_ASSERT_EQUAL(b.IsSubsetOf(a), true);
            UNIT_ASSERT_EQUAL(a.IsSubsetOf(b), true);
            b.Subtract(a);
            UNIT_ASSERT_EQUAL(b.IsEmpty(), true);
        }
        a.Subtract(0, 1);   // [1, 6)
        {
            TIntervals b(1, 6);
            UNIT_ASSERT_EQUAL(b.IsSubsetOf(a), true);
            UNIT_ASSERT_EQUAL(a.IsSubsetOf(b), true);
            b.Subtract(a);
            UNIT_ASSERT_EQUAL(b.IsEmpty(), true);
        }
        a.Subtract(5, 6);   // [1, 5)
        {
            TIntervals b(1, 5);
            UNIT_ASSERT_EQUAL(b.IsSubsetOf(a), true);
            UNIT_ASSERT_EQUAL(a.IsSubsetOf(b), true);
            b.Subtract(a);
            UNIT_ASSERT_EQUAL(b.IsEmpty(), true);
        }
        a.Subtract(2, 3);   // [1, 2) U [3, 5)
        {
            TIntervals b(1, 2);
            UNIT_ASSERT_EQUAL(b.IsSubsetOf(a), true);
            UNIT_ASSERT_EQUAL(a.IsSubsetOf(b), false);
            b.Subtract(a);
            UNIT_ASSERT_EQUAL(b.IsEmpty(), true);
        }
        {
            TIntervals b(3, 5);
            UNIT_ASSERT_EQUAL_C(b.IsSubsetOf(a), true, "a# " << a.ToString() << " b# " << b.ToString());
            UNIT_ASSERT_EQUAL_C(a.IsSubsetOf(b), false, "a# " << a.ToString() << " b# " << b.ToString());
            b.Subtract(a);
            UNIT_ASSERT_EQUAL(b.IsEmpty(), true);
        }
        a.Subtract(1, 5);   // {}
        UNIT_ASSERT_EQUAL(a.IsEmpty(), true);
    }

    MY_UNIT_TEST(TestSubtract) {
        TIntervals a(0, 100);  // [0, 100)
        a.Subtract(10, 20);      // [0, 10) U [20, 100)
        {
            TIntervals b(0, 10);
            b.Add(20, 100);
            UNIT_ASSERT_EQUAL_C(b.IsSubsetOf(a), true, "a# " << a.ToString() << " b# " << b.ToString());
            UNIT_ASSERT_EQUAL_C(a.IsSubsetOf(b), true, "a# " << a.ToString() << " b# " << b.ToString());
        }
        a.Subtract(80, 90);      // [0, 10) U [20, 80) U [90, 100)
        {
            TIntervals b(0, 10);
            b.Add(20, 80);
            b.Add(90, 100);
            UNIT_ASSERT_EQUAL_C(b.IsSubsetOf(a), true, "a# " << a.ToString() << " b# " << b.ToString());
            UNIT_ASSERT_EQUAL_C(a.IsSubsetOf(b), true, "a# " << a.ToString() << " b# " << b.ToString());
        }
        a.Subtract(30, 40);      // [0, 10) U [20, 30) U [40, 80) U [90, 100)
        {
            TIntervals b(0, 10);
            b.Add(20, 30);
            b.Add(40, 80);
            b.Add(90, 100);
            UNIT_ASSERT_EQUAL_C(b.IsSubsetOf(a), true, "a# " << a.ToString() << " b# " << b.ToString());
            UNIT_ASSERT_EQUAL_C(a.IsSubsetOf(b), true, "a# " << a.ToString() << " b# " << b.ToString());
        }
        a.Subtract(29, 41);      // [0, 10) U [20, 29) U [41, 80) U [90, 100)
        {
            TIntervals b(0, 10);
            b.Add(20, 29);
            b.Add(41, 80);
            b.Add(90, 100);
            UNIT_ASSERT_EQUAL_C(b.IsSubsetOf(a), true, "a# " << a.ToString() << " b# " << b.ToString());
            UNIT_ASSERT_EQUAL_C(a.IsSubsetOf(b), true, "a# " << a.ToString() << " b# " << b.ToString());
        }
        a.Subtract(28, 41);      // [0, 10) U [20, 28) U [41, 80) U [90, 100)
        {
            TIntervals b(0, 10);
            b.Add(20, 28);
            b.Add(41, 80);
            b.Add(90, 100);
            UNIT_ASSERT_EQUAL_C(b.IsSubsetOf(a), true, "a# " << a.ToString() << " b# " << b.ToString());
            UNIT_ASSERT_EQUAL_C(a.IsSubsetOf(b), true, "a# " << a.ToString() << " b# " << b.ToString());
        }
        a.Subtract(27, 40);      // [0, 10) U [20, 27) U [41, 80) U [90, 100)
        {
            TIntervals b(0, 10);
            b.Add(20, 27);
            b.Add(41, 80);
            b.Add(90, 100);
            UNIT_ASSERT_EQUAL_C(b.IsSubsetOf(a), true, "a# " << a.ToString() << " b# " << b.ToString());
            UNIT_ASSERT_EQUAL_C(a.IsSubsetOf(b), true, "a# " << a.ToString() << " b# " << b.ToString());
        }
        a.Subtract(27, 42);      // [0, 10) U [20, 27) U [42, 80) U [90, 100)
        {
            TIntervals b(0, 10);
            b.Add(20, 27);
            b.Add(42, 80);
            b.Add(90, 100);
            UNIT_ASSERT_EQUAL_C(b.IsSubsetOf(a), true, "a# " << a.ToString() << " b# " << b.ToString());
            UNIT_ASSERT_EQUAL_C(a.IsSubsetOf(b), true, "a# " << a.ToString() << " b# " << b.ToString());
        }
        a.Subtract(28, 43);      // [0, 10) U [20, 27) U [43, 80) U [90, 100)
        {
            TIntervals b(0, 10);
            b.Add(20, 27);
            b.Add(43, 80);
            b.Add(90, 100);
            UNIT_ASSERT_EQUAL_C(b.IsSubsetOf(a), true, "a# " << a.ToString() << " b# " << b.ToString());
            UNIT_ASSERT_EQUAL_C(a.IsSubsetOf(b), true, "a# " << a.ToString() << " b# " << b.ToString());
        }
        a.Subtract(23, 24);      // [0, 10) U [20, 23] U [24, 27) U [43, 80) U [90, 100)
        {
            TIntervals b(0, 10);
            b.Add(20, 23);
            b.Add(24, 27);
            b.Add(43, 80);
            b.Add(90, 100);
            UNIT_ASSERT_EQUAL_C(b.IsSubsetOf(a), true, "a# " << a.ToString() << " b# " << b.ToString());
            UNIT_ASSERT_EQUAL_C(a.IsSubsetOf(b), true, "a# " << a.ToString() << " b# " << b.ToString());
        }
        a.Subtract(22, 44);      // [0, 10) U [20, 22] U [44, 80) U [90, 100)
        {
            TIntervals b(0, 10);
            b.Add(20, 22);
            b.Add(44, 80);
            b.Add(90, 100);
            UNIT_ASSERT_EQUAL_C(b.IsSubsetOf(a), true, "a# " << a.ToString() << " b# " << b.ToString());
            UNIT_ASSERT_EQUAL_C(a.IsSubsetOf(b), true, "a# " << a.ToString() << " b# " << b.ToString());
        }
        a.Subtract(22, 44);      // [0, 10) U [20, 22] U [44, 80) U [90, 100)
        {
            TIntervals b(0, 10);
            b.Add(20, 22);
            b.Add(44, 80);
            b.Add(90, 100);
            UNIT_ASSERT_EQUAL_C(b.IsSubsetOf(a), true, "a# " << a.ToString() << " b# " << b.ToString());
            UNIT_ASSERT_EQUAL_C(a.IsSubsetOf(b), true, "a# " << a.ToString() << " b# " << b.ToString());
        }

    }

    MY_UNIT_TEST(TestSubtractAgainstReference) {
        for (ui64 aBits = 0; aBits < 128; ++aBits) {
            for (ui64 bBits = 0; bBits < 128; ++bBits) {
                TReferenceSet aRef(aBits);
                TReferenceSet bRef(bBits);
                TIntervals a = aRef.ToIntervalSet<TIntervals>();
                TIntervals b = bRef.ToIntervalSet<TIntervals>();
                aRef.Subtract(bRef);
                a.Subtract(b);
                TIntervals aa = aRef.ToIntervalSet<TIntervals>();
                if (!(a == aa)) {
                    TReferenceSet ia(aBits);
                    TReferenceSet ib(bBits);
                    UNIT_ASSERT_EQUAL_C(a == aa, true, "a# " << a.ToString() << " aa# " << aa.ToString()
                            << " aBits# " << aBits << " bBits# " << bBits << " ia# " << ia.ToString()
                            << " ib# " << ib.ToString());
                }
            }
        }
    }

    MY_UNIT_TEST(TestAddAgainstReference) {
        for (ui64 aBits = 0; aBits < 128; ++aBits) {
            for (ui64 bBits = 0; bBits < 128; ++bBits) {
                TReferenceSet aRef(aBits);
                TReferenceSet bRef(bBits);
                TIntervals a = aRef.ToIntervalSet<TIntervals>();
                TIntervals b = bRef.ToIntervalSet<TIntervals>();
                aRef.Add(bRef);
                a.Add(b);
                TIntervals aa = aRef.ToIntervalSet<TIntervals>();
                if (!(a == aa)) {
                    TReferenceSet ia(aBits);
                    TReferenceSet ib(bBits);
                    UNIT_ASSERT_EQUAL_C(a == aa, true, "a# " << a.ToString() << " aa# " << aa.ToString()
                            << " aBits# " << aBits << " bBits# " << bBits << " ia# " << ia.ToString()
                            << " ib# " << ib.ToString());
                }
            }
        }
    }

    MY_UNIT_TEST(TestIsSubsetOfAgainstReference) {
        for (ui64 aBits = 0; aBits < 128; ++aBits) {
            for (ui64 bBits = 0; bBits < 128; ++bBits) {
                TReferenceSet aRef(aBits);
                TReferenceSet bRef(bBits);
                TIntervals a = aRef.ToIntervalSet<TIntervals>();
                TIntervals b = bRef.ToIntervalSet<TIntervals>();
                bool isSubsetRef = aRef.IsSubsetOf(bRef);
                bool isSubset = a.IsSubsetOf(b);
                if (isSubset == isSubsetRef) {
                    UNIT_ASSERT_EQUAL_C(isSubset == isSubsetRef, true, "isSubsetRef# " << isSubsetRef << " isSubset# " << isSubset
                            << " aBits# " << aBits << " bBits# " << bBits << " aRef# " << aRef.ToString()
                            << " bRef# " << bRef.ToString());
                }
            }
        }
    }

    MY_UNIT_TEST(TestToStringAgainstReference) {
        for (ui64 aBits = 0; aBits < 128; ++aBits) {
            TReferenceSet aRef(aBits);
            TIntervals a = aRef.ToIntervalSet<TIntervals>();
            TString strRef = aRef.ToString();
            TString str = a.ToString();
            if (strRef == str) {
                UNIT_ASSERT_EQUAL_C(strRef == str, true, "str# " << str << " strRef# " << strRef
                        << " aBits# " << aBits);
            }
        }
    }

    MY_UNIT_TEST(Union) {
        ui32 len = 8;
        ui64 n = 1 << len;
        for (ui64 i = 0; i < n; ++i) {
            for (ui64 j = 0; j < n; ++j) {
                const TIntervals a = MakeIntervalSet<TIntervals>(i, len);
                const TIntervals b = MakeIntervalSet<TIntervals>(j, len);
                const TIntervals res = a | b;
                const TIntervals reference = MakeIntervalSet<TIntervals>(i | j, len);
                UNIT_ASSERT_EQUAL_C(res, reference, "a# " << a.ToString() << " b# " << b.ToString() << " res# "
                    << res.ToString() << " reference# " << reference.ToString());
            }
        }
    }

    MY_UNIT_TEST(UnionInplace) {
        ui32 len = 8;
        ui64 n = 1 << len;
        for (ui64 i = 0; i < n; ++i) {
            for (ui64 j = 0; j < n; ++j) {
                const TIntervals a = MakeIntervalSet<TIntervals>(i, len);
                const TIntervals b = MakeIntervalSet<TIntervals>(j, len);
                TIntervals res = a;
                res |= b;
                const TIntervals reference = MakeIntervalSet<TIntervals>(i | j, len);
                UNIT_ASSERT_EQUAL_C(res, reference, "a# " << a.ToString() << " b# " << b.ToString() << " res# "
                    << res.ToString() << " reference# " << reference.ToString());
            }
        }
    }

    MY_UNIT_TEST(UnionInplaceSelf) {
        ui32 len = 8;
        ui64 n = 1 << len;
        for (ui64 i = 0; i < n; ++i) {
            TIntervals res = MakeIntervalSet<TIntervals>(i, len);
            TIntervals *other = &res;
            res |= *other;
            const TIntervals reference = MakeIntervalSet<TIntervals>(i, len);
            UNIT_ASSERT_EQUAL(res, reference);
        }
    }

    MY_UNIT_TEST(Intersection) {
        ui32 len = 8;
        ui64 n = 1 << len;
        for (ui64 i = 0; i < n; ++i) {
            for (ui64 j = 0; j < n; ++j) {
                const TIntervals a = MakeIntervalSet<TIntervals>(i, len);
                const TIntervals b = MakeIntervalSet<TIntervals>(j, len);
                const TIntervals res = a & b;
                const TIntervals reference = MakeIntervalSet<TIntervals>(i & j, len);
                UNIT_ASSERT_EQUAL_C(res, reference, "a# " << a.ToString() << " b# " << b.ToString() << " res# "
                    << res.ToString() << " reference# " << reference.ToString());
            }
        }
    }

    MY_UNIT_TEST(IntersectionInplace) {
        ui32 len = 8;
        ui64 n = 1 << len;
        for (ui64 i = 0; i < n; ++i) {
            for (ui64 j = 0; j < n; ++j) {
                const TIntervals a = MakeIntervalSet<TIntervals>(i, len);
                const TIntervals b = MakeIntervalSet<TIntervals>(j, len);
                TIntervals res = a;
                res &= b;
                const TIntervals reference = MakeIntervalSet<TIntervals>(i & j, len);
                UNIT_ASSERT_EQUAL_C(res, reference, "a# " << a.ToString() << " b# " << b.ToString() << " res# "
                    << res.ToString() << " reference# " << reference.ToString());
            }
        }
    }

    MY_UNIT_TEST(IntersectionInplaceSelf) {
        ui32 len = 8;
        ui64 n = 1 << len;
        for (ui64 i = 0; i < n; ++i) {
            TIntervals res = MakeIntervalSet<TIntervals>(i, len);
            TIntervals *other = &res;
            res &= *other;
            const TIntervals reference = MakeIntervalSet<TIntervals>(i, len);
            UNIT_ASSERT_EQUAL(res, reference);
        }
    }

    MY_UNIT_TEST(Difference) {
        ui32 len = 8;
        ui64 n = 1 << len;
        for (ui64 i = 0; i < n; ++i) {
            for (ui64 j = 0; j < n; ++j) {
                const TIntervals a = MakeIntervalSet<TIntervals>(i, len);
                const TIntervals b = MakeIntervalSet<TIntervals>(j, len);
                const TIntervals res = a - b;
                const TIntervals reference = MakeIntervalSet<TIntervals>(i & ~j, len);
                UNIT_ASSERT_EQUAL_C(res, reference, "a# " << a.ToString() << " b# " << b.ToString() << " res# "
                    << res.ToString() << " reference# " << reference.ToString());
            }
        }
    }

    MY_UNIT_TEST(DifferenceInplaceSelf) {
        ui32 len = 8;
        ui64 n = 1 << len;
        for (ui64 i = 0; i < n; ++i) {
            TIntervals res = MakeIntervalSet<TIntervals>(i, len);
            TIntervals *other = &res;
            res -= *other;
            UNIT_ASSERT(!res);
        }
    }

    Y_UNIT_TEST(IntervalSetTestIterator) {
        using TIntervals = TIntervalSet<i32>;
        for (ui64 aBits = 0; aBits < 128; ++aBits) {
            TReferenceSet aRef(aBits);
            TIntervals a = aRef.ToIntervalSet<TIntervals>();
            TString strRef = aRef.ToString();

            TStringStream str;
            str << "{";
            for (auto it = a.begin(); it != a.end(); ++it) {
                if (it != a.begin()) {
                    str << " U ";
                }
                auto [begin, end] = *it;
                str << "[" << begin << ", " << end << ")";
            }
            str << "}";

            if (strRef == str.Str()) {
                UNIT_ASSERT_EQUAL_C(strRef == str.Str(), true, "str# " << str.Str() << " strRef# " << strRef
                        << " aBits# " << aBits);
            }
        }
    }
}

} // NKikimr
