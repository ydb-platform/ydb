#include <ydb/core/ymq/actor/infly.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSQS {

Y_UNIT_TEST_SUITE(InflyTest) {
    Y_UNIT_TEST(AddMessage) {
        TIntrusivePtr<TInflyMessages> infly = MakeIntrusive<TInflyMessages>();
        UNIT_ASSERT_VALUES_EQUAL(infly->GetInflyCount(TInstant::Seconds(42)), 0);
        infly->Add(MakeHolder<TInflyMessage>(1ull, 0ull, TInstant::Seconds(42), 0));
        UNIT_ASSERT_VALUES_EQUAL(infly->GetCapacity(), 1);
        UNIT_ASSERT_VALUES_EQUAL(infly->GetInflyCount(TInstant::Seconds(42)), 1);
        UNIT_ASSERT_VALUES_EQUAL(infly->GetInflyCount(TInstant::Seconds(43)), 0);

        infly->Add(MakeHolder<TInflyMessage>(2ull, 0ull, TInstant::Seconds(12), 0));
        UNIT_ASSERT_VALUES_EQUAL(infly->GetCapacity(), 2);
        UNIT_ASSERT_VALUES_EQUAL(infly->GetInflyCount(TInstant::Seconds(1)), 2);
    }

    Y_UNIT_TEST(DeleteMessage) {
        TIntrusivePtr<TInflyMessages> infly = MakeIntrusive<TInflyMessages>();
        infly->Add(MakeHolder<TInflyMessage>(1ull, 0ull, TInstant::Seconds(42), 0));
        infly->Add(MakeHolder<TInflyMessage>(2ull, 0ull, TInstant::Seconds(12), 0));
        UNIT_ASSERT_VALUES_EQUAL(infly->GetCapacity(), 2);
        UNIT_ASSERT(!infly->Delete(5));
        UNIT_ASSERT_VALUES_EQUAL(infly->GetCapacity(), 2);
        UNIT_ASSERT(infly->Delete(2));
        UNIT_ASSERT_VALUES_EQUAL(infly->GetCapacity(), 1);
        UNIT_ASSERT(!infly->Delete(2));
        UNIT_ASSERT_VALUES_EQUAL(infly->GetCapacity(), 1);
        UNIT_ASSERT(infly->Delete(1));
        UNIT_ASSERT_VALUES_EQUAL(infly->GetCapacity(), 0);
    }

    Y_UNIT_TEST(ChangeMesageVisibility) {
        TIntrusivePtr<TInflyMessages> infly = MakeIntrusive<TInflyMessages>();
        infly->Add(MakeHolder<TInflyMessage>(1ull, 0ull, TInstant::Seconds(42), 0));
        infly->Add(MakeHolder<TInflyMessage>(2ull, 0ull, TInstant::Seconds(12), 0));
        infly->Add(MakeHolder<TInflyMessage>(5ull, 0ull, TInstant::Seconds(150), 0));
        UNIT_ASSERT_VALUES_EQUAL(infly->GetCapacity(), 3);
        UNIT_ASSERT_VALUES_EQUAL(infly->GetInflyCount(TInstant::Seconds(50)), 1);

        {
            TInflyMessages::TChangeVisibilityCandidates changeVisibilityCandidates(infly);
            UNIT_ASSERT(!changeVisibilityCandidates.Add(3));
            UNIT_ASSERT_VALUES_EQUAL(infly->GetInflyCount(TInstant::Seconds(50)), 1);
            UNIT_ASSERT_VALUES_EQUAL(infly->GetCapacity(), 3);

            UNIT_ASSERT(changeVisibilityCandidates.Add(2));
            UNIT_ASSERT_VALUES_EQUAL(infly->GetCapacity(), 3);

            UNIT_ASSERT_VALUES_EQUAL(infly->GetInflyCount(TInstant::Seconds(50)), 1);
            UNIT_ASSERT_VALUES_EQUAL(infly->GetInflyCount(TInstant::Seconds(40)), 2);

            changeVisibilityCandidates.SetVisibilityDeadline(2, TInstant::Seconds(100));
            UNIT_ASSERT_VALUES_EQUAL(infly->GetCapacity(), 3);

            UNIT_ASSERT_VALUES_EQUAL(infly->GetInflyCount(TInstant::Seconds(50)), 2);
            UNIT_ASSERT_VALUES_EQUAL(infly->GetInflyCount(TInstant::Seconds(40)), 3);

            UNIT_ASSERT(changeVisibilityCandidates.Add(5));
            UNIT_ASSERT(changeVisibilityCandidates.Delete(5));
            UNIT_ASSERT_VALUES_EQUAL(infly->GetCapacity(), 2);

            // test offsets that doesn't exist
            UNIT_ASSERT(!changeVisibilityCandidates.Has(42));
            changeVisibilityCandidates.SetVisibilityDeadline(42, TInstant::Seconds(50));
            UNIT_ASSERT(!changeVisibilityCandidates.Delete(42));
        }
        {
            TInflyMessages::TChangeVisibilityCandidates changeVisibilityCandidates(infly);
            // test empty candidates
            UNIT_ASSERT(!changeVisibilityCandidates.Has(42));
            UNIT_ASSERT(!changeVisibilityCandidates.Delete(10));
            changeVisibilityCandidates.SetVisibilityDeadline(100500, TInstant::Seconds(100500));
        }
        UNIT_ASSERT_VALUES_EQUAL(infly->GetInflyCount(TInstant::Seconds(50)), 1);
        UNIT_ASSERT_VALUES_EQUAL(infly->GetInflyCount(TInstant::Seconds(40)), 2);
        UNIT_ASSERT_VALUES_EQUAL(infly->GetCapacity(), 2);
    }

    Y_UNIT_TEST(ReceiveMessages) {
        TIntrusivePtr<TInflyMessages> infly = MakeIntrusive<TInflyMessages>();
        infly->Add(MakeHolder<TInflyMessage>(1ull, 0ull, TInstant::Seconds(1), 0));
        infly->Add(MakeHolder<TInflyMessage>(2ull, 0ull, TInstant::Seconds(2), 0));
        infly->Add(MakeHolder<TInflyMessage>(3ull, 0ull, TInstant::Seconds(3), 0));
        infly->Add(MakeHolder<TInflyMessage>(4ull, 0ull, TInstant::Seconds(4), 0));
        infly->Add(MakeHolder<TInflyMessage>(5ull, 0ull, TInstant::Seconds(5), 0));
        UNIT_ASSERT_VALUES_EQUAL(infly->GetCapacity(), 5);
        UNIT_ASSERT_VALUES_EQUAL(infly->GetInflyCount(TInstant::Seconds(50)), 0);
        {
            auto messages = infly->Receive(10, TInstant::Seconds(5));
            UNIT_ASSERT(messages);
            UNIT_ASSERT_VALUES_EQUAL(infly->GetCapacity(), 5);
            UNIT_ASSERT_VALUES_EQUAL(infly->GetInflyCount(TInstant::Seconds(0)), 1);
            UNIT_ASSERT(!infly->Receive(10, TInstant::Seconds(5)));
            UNIT_ASSERT(!messages.Has(5));
            UNIT_ASSERT(messages.Has(1));
            UNIT_ASSERT(messages.Has(2));
            UNIT_ASSERT(messages.Has(3));
            UNIT_ASSERT(messages.Has(4));
            auto i = messages.Begin();
            ++i;
            UNIT_ASSERT(i != messages.End());
            i->Message().SetVisibilityDeadline(TInstant::Seconds(100));

            ++i;
            UNIT_ASSERT(i != messages.End());
            i->Message().SetVisibilityDeadline(TInstant::Seconds(100));

            ++i;
            UNIT_ASSERT(i != messages.End());
            i->Message().SetVisibilityDeadline(TInstant::Seconds(100));

            ++i;
            UNIT_ASSERT(i == messages.End());
            UNIT_ASSERT_VALUES_EQUAL(infly->GetInflyCount(TInstant::Seconds(50)), 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(infly->GetInflyCount(TInstant::Seconds(50)), 3);
        UNIT_ASSERT_VALUES_EQUAL(infly->GetCapacity(), 5);
    }

    Y_UNIT_TEST(DeleteReceivedMessage) {
        TIntrusivePtr<TInflyMessages> infly = MakeIntrusive<TInflyMessages>();
        infly->Add(MakeHolder<TInflyMessage>(1ull, 0ull, TInstant::Seconds(1), 0));
        infly->Add(MakeHolder<TInflyMessage>(2ull, 0ull, TInstant::Seconds(2), 0));
        infly->Add(MakeHolder<TInflyMessage>(3ull, 0ull, TInstant::Seconds(3), 0));
        UNIT_ASSERT_VALUES_EQUAL(infly->GetCapacity(), 3);
        UNIT_ASSERT_VALUES_EQUAL(infly->GetInflyCount(TInstant::Seconds(2)), 2);
        {
            auto messages = infly->Receive(10, TInstant::Seconds(2));
            UNIT_ASSERT(messages);
            UNIT_ASSERT_VALUES_EQUAL(infly->GetCapacity(), 3);
            UNIT_ASSERT_VALUES_EQUAL(infly->GetInflyCount(TInstant::Seconds(2)), 2);
            auto i = messages.Begin();
            UNIT_ASSERT(i != messages.End());
            UNIT_ASSERT(messages.Delete(1));
            UNIT_ASSERT(!messages.Delete(1));
            UNIT_ASSERT_VALUES_EQUAL(infly->GetCapacity(), 2);
        }
        UNIT_ASSERT_VALUES_EQUAL(infly->GetCapacity(), 2);
    }
}

} // namespace NKikimr::NSQS
