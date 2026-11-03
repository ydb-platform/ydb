#include <ydb/public/sdk/cpp/src/client/topic/impl/producer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <memory>

using namespace NYdb;
using namespace NYdb::NTopic;

namespace NYdb::inline Dev::NTopic {

    struct TProducerRetryPolicyTestHelper {
        static TProducer::TProducerRetryPolicy::TPtr Create(IRetryPolicy::TPtr userPolicy) {
            return std::make_shared<TProducer::TProducerRetryPolicy>(std::move(userPolicy));
        }
    };

} // namespace NYdb::inline Dev::NTopic

namespace {

    class TScriptedRetryPolicy: public NTopic::IRetryPolicy {
    public:
        mutable int StateCount = 0;
        mutable int CallCount = 0;
        mutable EStatus LastStatus = EStatus::STATUS_UNDEFINED;

        struct TState: public NTopic::IRetryPolicy::IRetryState {
            explicit TState(TScriptedRetryPolicy* policy)
                : Policy(policy)
            {
            }

            TMaybe<TDuration> GetNextRetryDelay(EStatus status) override {
                ++Policy->CallCount;
                Policy->LastStatus = status;
                if (AttemptsLeft == 0) {
                    return Nothing();
                }
                --AttemptsLeft;
                return TDuration::MilliSeconds(15);
            }

            TScriptedRetryPolicy* Policy;
            int AttemptsLeft = 1;
        };

        NTopic::IRetryPolicy::IRetryState::TPtr CreateRetryState() const override {
            ++StateCount;
            return std::make_unique<TState>(const_cast<TScriptedRetryPolicy*>(this));
        }
    };

} // namespace

Y_UNIT_TEST_SUITE(ProducerRetryPolicy) {

    Y_UNIT_TEST(OverloadedDoesNotConsultUserPolicy) {
        auto user = std::make_shared<TScriptedRetryPolicy>();
        auto policy = TProducerRetryPolicyTestHelper::Create(user);
        auto state = policy->CreateRetryState();

        const auto delay = state->GetNextRetryDelay(EStatus::OVERLOADED);

        UNIT_ASSERT(!delay);
        UNIT_ASSERT_VALUES_EQUAL(user->StateCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(user->CallCount, 0);
    }

    Y_UNIT_TEST(DelegatesOtherStatusesAndReusesState) {
        auto user = std::make_shared<TScriptedRetryPolicy>();
        auto policy = TProducerRetryPolicyTestHelper::Create(user);
        auto state = policy->CreateRetryState();

        const auto first = state->GetNextRetryDelay(EStatus::UNAVAILABLE);
        UNIT_ASSERT(first);
        UNIT_ASSERT_VALUES_EQUAL(first->MilliSeconds(), 15);
        UNIT_ASSERT_VALUES_EQUAL(user->StateCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(user->CallCount, 1);
        UNIT_ASSERT(user->LastStatus == EStatus::UNAVAILABLE);

        // OVERLOADED is handled by the wrapper and must not consume a user attempt.
        UNIT_ASSERT(!state->GetNextRetryDelay(EStatus::OVERLOADED));
        UNIT_ASSERT_VALUES_EQUAL(user->CallCount, 1);

        const auto second = state->GetNextRetryDelay(EStatus::UNAVAILABLE);
        UNIT_ASSERT(!second);
        UNIT_ASSERT_VALUES_EQUAL(user->StateCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(user->CallCount, 2);
    }

    Y_UNIT_TEST(RetryStatesAreIndependent) {
        auto user = std::make_shared<TScriptedRetryPolicy>();
        auto policy = TProducerRetryPolicyTestHelper::Create(user);
        auto first = policy->CreateRetryState();
        auto second = policy->CreateRetryState();

        UNIT_ASSERT(first->GetNextRetryDelay(EStatus::UNAVAILABLE));
        UNIT_ASSERT(!first->GetNextRetryDelay(EStatus::UNAVAILABLE));
        UNIT_ASSERT(second->GetNextRetryDelay(EStatus::TRANSPORT_UNAVAILABLE));
        UNIT_ASSERT_VALUES_EQUAL(user->StateCount, 2);
        UNIT_ASSERT(user->LastStatus == EStatus::TRANSPORT_UNAVAILABLE);
    }

    Y_UNIT_TEST(MissingUserPolicyUsesDefaultAndDoesNotNeedProducer) {
        // The wrapper must stay usable with no TProducer: write sessions call it
        // after the producer has already been destroyed.
        auto policy = TProducerRetryPolicyTestHelper::Create(nullptr);
        auto state = policy->CreateRetryState();

        const auto retry = state->GetNextRetryDelay(EStatus::UNAVAILABLE);
        UNIT_ASSERT(retry);
        UNIT_ASSERT(*retry > TDuration::Zero());

        auto fatal = policy->CreateRetryState();
        UNIT_ASSERT(!fatal->GetNextRetryDelay(EStatus::SCHEME_ERROR));
        UNIT_ASSERT(!fatal->GetNextRetryDelay(EStatus::OVERLOADED));
    }

} // Y_UNIT_TEST_SUITE(ProducerRetryPolicy)
