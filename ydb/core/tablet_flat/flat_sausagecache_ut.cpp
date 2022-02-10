#include "flat_sausagecache.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

Y_UNIT_TEST_SUITE(TPrivatePageCacheTest) {
    using TPage = TPrivatePageCache::TPage;

    Y_UNIT_TEST(PageJoinSingle) {
        TPage a(123, 1, nullptr);
        TPage b(234, 2, nullptr);
        TPage* l = TPage::Join(&a, &b);

        UNIT_ASSERT(l == &a);

        UNIT_ASSERT(a.Next()->Node() == &b);
        UNIT_ASSERT(b.Next()->Node() == &a);

        UNIT_ASSERT(b.Prev()->Node() == &a);
        UNIT_ASSERT(a.Prev()->Node() == &b);
    }

    Y_UNIT_TEST(PageJoinMultipleChains) {
        TPage a(123, 1, nullptr);
        TPage b(234, 2, nullptr);
        TPage c(345, 3, nullptr);
        TPage d(456, 4, nullptr);
        a.LinkBefore(&b);
        c.LinkBefore(&d);
        TPage* l = TPage::Join(&a, &c);

        UNIT_ASSERT(l == &a);

        UNIT_ASSERT(a.Next()->Node() == &b);
        UNIT_ASSERT(b.Next()->Node() == &c);
        UNIT_ASSERT(c.Next()->Node() == &d);
        UNIT_ASSERT(d.Next()->Node() == &a);

        UNIT_ASSERT(d.Prev()->Node() == &c);
        UNIT_ASSERT(c.Prev()->Node() == &b);
        UNIT_ASSERT(b.Prev()->Node() == &a);
        UNIT_ASSERT(a.Prev()->Node() == &d);
    }
}

}
}
