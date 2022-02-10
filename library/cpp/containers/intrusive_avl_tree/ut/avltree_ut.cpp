#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/containers/intrusive_avl_tree/avltree.h>

class TAvlTreeTest: public TTestBase {
    UNIT_TEST_SUITE(TAvlTreeTest);
    UNIT_TEST(TestLowerBound);
    UNIT_TEST(TestIterator);
    UNIT_TEST_SUITE_END();

private:
    void TestLowerBound();
    void TestIterator();

    class TIt;
    struct TItCompare {
        static inline bool Compare(const TIt& l, const TIt& r) noexcept;
    };

    class TIt: public TAvlTreeItem<TIt, TItCompare> {
    public:
        TIt(int val = 0)
            : Val(val)
        {
        }

        int Val;
    };

    using TIts = TAvlTree<TIt, TItCompare>;
};

inline bool TAvlTreeTest::TItCompare::Compare(const TIt& l, const TIt& r) noexcept {
    return l.Val < r.Val;
}

UNIT_TEST_SUITE_REGISTRATION(TAvlTreeTest);

void TAvlTreeTest::TestLowerBound() {
    TIts its;
    TIt it1(5);
    TIt it2(2);
    TIt it3(10);
    TIt it4(879);
    TIt it5(1);
    TIt it6(52);
    TIt it7(4);
    TIt it8(5);
    its.Insert(&it1);
    its.Insert(&it2);
    its.Insert(&it3);
    its.Insert(&it4);
    its.Insert(&it5);
    its.Insert(&it6);
    its.Insert(&it7);
    its.Insert(&it8);

    TIt it_zero(0);
    TIt it_large(1000);
    UNIT_ASSERT_EQUAL(its.LowerBound(&it3), &it3);
    UNIT_ASSERT_EQUAL(its.LowerBound(&it_zero), &it5);
    UNIT_ASSERT_EQUAL(its.LowerBound(&it_large), nullptr);
}

void TAvlTreeTest::TestIterator() {
    TIts its;
    TIt it1(1);
    TIt it2(2);
    TIt it3(3);
    TIt it4(4);
    TIt it5(5);
    TIt it6(6);
    TIt it7(7);

    its.Insert(&it3);
    its.Insert(&it1);
    its.Insert(&it7);
    its.Insert(&it5);
    its.Insert(&it4);
    its.Insert(&it6);
    its.Insert(&it2);

    TVector<int> res;
    for (const TIt& i : its) {
        res.push_back(i.Val);
    }

    TVector<int> expected{1, 2, 3, 4, 5, 6, 7};
    UNIT_ASSERT_EQUAL(res, expected);

    res.clear();
    for (TIt& i : its) {
        res.push_back(i.Val);
    }
    UNIT_ASSERT_EQUAL(res, expected);

    res.clear();
    const TIts* constIts = &its;
    for (TIts::const_iterator i = constIts->begin(); i != constIts->end(); ++i) {
        res.push_back(i->Val);
    }
    UNIT_ASSERT_EQUAL(res, expected);
}
