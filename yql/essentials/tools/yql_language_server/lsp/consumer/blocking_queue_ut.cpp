#include "blocking_queue.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NLsp;

namespace NSpace {

template <typename T, template <typename> typename U>
struct TInt {
    int V;
};

} // namespace NSpace

Y_UNIT_TEST_SUITE(BlockingQueueConsumerTests) {

Y_UNIT_TEST(Example) {
    auto q = std::make_shared<TBlockingQueue<NSpace::TInt<int, TVector>>>(2);
    auto c = Consumer(q);

    c->Receive({1});
    UNIT_ASSERT_VALUES_EQUAL(q->Pop()->V, 1);

    c->Receive({2});
    c->Receive({3});
    UNIT_ASSERT_VALUES_EQUAL(q->Pop()->V, 2);
    UNIT_ASSERT_VALUES_EQUAL(q->Pop()->V, 3);

    c->Stop();
    UNIT_ASSERT_EXCEPTION_CONTAINS(c->Receive({4}), yexception, "queue rejected");
    UNIT_ASSERT_EXCEPTION_CONTAINS(c->Receive({5}), yexception, "queue rejected");
}

} // Y_UNIT_TEST_SUITE(BlockingQueueConsumerTests)
