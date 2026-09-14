#include "map.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NLsp;

Y_UNIT_TEST_SUITE(MapConsumerTests) {

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
    auto c = MakeIntrusive<TVectorConsumer>();
    auto m = Map<float, int>([](auto x) { return static_cast<int>(x); }, c);

    m->Receive(1.0);
    m->Receive(1.5);
    m->Receive(2);
    m->Receive(2.5);

    UNIT_ASSERT_VALUES_EQUAL(c->Values(), (TVector<int>{1, 1, 2, 2}));
}

} // Y_UNIT_TEST_SUITE(MapConsumerTests)
