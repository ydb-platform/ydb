#include "counters.h"
#include <library/cpp/testing/unittest/registar.h>
#include <util/system/event.h>
#include <util/system/thread.h>

using namespace NMonitoring;

Y_UNIT_TEST_SUITE(TDynamicCountersContentionTest) {

    Y_UNIT_TEST(EnsureNonlocking) {
        TDynamicCounterPtr counters = MakeIntrusive<TDynamicCounters>();

        class TConsumer : public ICountableConsumer {
            TAutoEvent Ev;
            TAutoEvent Response;
            TDynamicCounterPtr Counters;
            TThread Thread;

        public:
            TConsumer(TDynamicCounterPtr counters)
                : Counters(counters)
                , Thread(std::bind(&TConsumer::ThreadFunc, this))
            {
                Thread.Start();
            }

            ~TConsumer() override {
                Thread.Join();
            }

            void OnCounter(const TString& /*labelName*/, const TString& /*labelValue*/, const TCounterForPtr* /*counter*/) override {
                Ev.Signal();
                Response.Wait();
            }

            void OnHistogram(const TString& /*labelName*/, const TString& /*labelValue*/, IHistogramSnapshotPtr /*snapshot*/, bool /*derivative*/) override {
            }

            void OnGroupBegin(const TString& /*labelName*/, const TString& /*labelValue*/, const TDynamicCounters* /*group*/) override {
            }

            void OnGroupEnd(const TString& /*labelName*/, const TString& /*labelValue*/, const TDynamicCounters* /*group*/) override {
            }

        private:
            void ThreadFunc() {
                // acts like a coroutine
                Ev.Wait();
                auto ctr = Counters->GetSubgroup("label", "value")->GetCounter("name");
                Y_ABORT_UNLESS(*ctr == 42);
                Response.Signal();
            }
        };

        auto ctr = counters->GetSubgroup("label", "value")->GetCounter("name");
        *ctr = 42;
        TConsumer consumer(counters);
        counters->Accept({}, {}, consumer);
    }

}
