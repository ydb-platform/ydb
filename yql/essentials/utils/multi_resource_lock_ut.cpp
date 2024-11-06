#include "multi_resource_lock.h"
#include <util/generic/xrange.h>
#include <library/cpp/threading/future/async.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NYql {
using namespace NThreading;

Y_UNIT_TEST_SUITE(TMultiResourceLock) {
    Y_UNIT_TEST(ManyResources) {
        TMultiResourceLock multiLock;
        const int workersCount = 3;
        TVector<TVector<int>> workersData;
        workersData.resize(workersCount);

        TAdaptiveThreadPool queue;
        queue.Start(0);

        TVector<NThreading::TFuture<void>> workers;
        workers.reserve(workersCount);
        TManualEvent startEvent;

        for (int i = 0; i < workersCount; ++i) {
            TString resourceId = ToString(i);
            TVector<int>& data = workersData.at(i);
            NThreading::TFuture<void> f = NThreading::Async([&, resourceId]() {
                startEvent.Wait();

                for (int j = 0; j < 1000; ++j) {
                    const auto& l = multiLock.Acquire(resourceId);
                    Y_UNUSED(l);
                    data.push_back(j);
                }
            }, queue);

            workers.push_back(std::move(f));
        }

        startEvent.Signal();

        NThreading::TFuture<void> all = NThreading::WaitExceptionOrAll(workers);
        all.GetValueSync();
        queue.Stop();

        // analyze workersData:
        auto range0_999 = xrange(0, 1000);
        for (auto& w : workersData) {
            UNIT_ASSERT_VALUES_EQUAL(w.size(), 1000);
            UNIT_ASSERT(std::equal(range0_999.begin(), range0_999.end(), w.begin()));
        }
    }
}

}
