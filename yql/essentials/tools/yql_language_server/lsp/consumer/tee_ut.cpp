#include "tee.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NLsp;

Y_UNIT_TEST_SUITE(TeeConsumerTests) {

class TVectorConsumer final: public IConsumer<int> {
public:
    void Receive(int value) override {
        Values_.push_back(value);
    }

    void Stop() override {
    }

    const TVector<int>& Values() const {
        return Values_;
    }

private:
    TVector<int> Values_;
};

Y_UNIT_TEST(Example) {
    auto a = MakeIntrusive<TVectorConsumer>();
    auto b = MakeIntrusive<TVectorConsumer>();
    auto t = Tee<int>([&](auto x) { a->Receive(x); }, b);

    a->Receive(1);
    b->Receive(2);
    t->Receive(3);

    UNIT_ASSERT_VALUES_EQUAL(a->Values(), (TVector<int>{1, 3}));
    UNIT_ASSERT_VALUES_EQUAL(b->Values(), (TVector<int>{2, 3}));
}

} // Y_UNIT_TEST_SUITE(TeeConsumerTests)
