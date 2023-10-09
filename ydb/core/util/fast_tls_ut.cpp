#include "fast_tls.h"

#include "ut_common.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/vector.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TFastTlsTest) {

    struct TMyValue {
        static size_t Dtors;

        size_t Value = 0;

        ~TMyValue() {
            ++Dtors;
        }
    };

    size_t TMyValue::Dtors = 0;

    Y_UNIT_TEST(IterationAfterThreadDeath) {
        size_t expectedDtors = 0u;

        for (size_t i = 0; i < 4; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(TMyValue::Dtors, expectedDtors);

            TFastThreadLocal<TMyValue> value;
            TVector<THolder<TWorkerThread>> workers(4);

            size_t base = i * 1000000u;
            size_t next = base + 1;
            for (auto& worker : workers) {
                size_t current = next++;
                worker = TWorkerThread::Spawn([current, &value] {
                    Y_ABORT_UNLESS(value->Value == 0);

                    value->Value = current;
                });
            }

            for (auto& worker : workers) {
                worker->Join();
            }

            TVector<size_t> seen;
            for (auto it = value.Iterator(); it.IsValid(); it.Next()) {
                seen.push_back(it->Value);
            }

            std::sort(seen.begin(), seen.end());

            UNIT_ASSERT_VALUES_EQUAL(seen.size(), 4u);
            UNIT_ASSERT_VALUES_EQUAL(seen[0], base + 1u);
            UNIT_ASSERT_VALUES_EQUAL(seen[1], base + 2u);
            UNIT_ASSERT_VALUES_EQUAL(seen[2], base + 3u);
            UNIT_ASSERT_VALUES_EQUAL(seen[3], base + 4u);

            UNIT_ASSERT_VALUES_EQUAL(TMyValue::Dtors, expectedDtors);

            expectedDtors += 4;
        }

        UNIT_ASSERT_VALUES_EQUAL(TMyValue::Dtors, expectedDtors);
    }

    Y_UNIT_TEST(ManyThreadLocals) {
        for (size_t i = 0; i < 4; ++i) {
            TFastThreadLocal<size_t> values[4096];
            for (size_t i = 0; i < 4096; ++i) {
                Y_ABORT_UNLESS(values[i].Get() == 0);
                values[i] = i + 1;
            }
            for (size_t i = 0; i < 4096; ++i) {
                Y_ABORT_UNLESS(values[i].Get() == i + 1);
            }
        }
    }

    Y_UNIT_TEST(ManyConcurrentKeys) {
        static constexpr size_t Concurrency = 8;
        static constexpr size_t Iterations = 100000;

        TVector<THolder<TWorkerThread>> workers(Concurrency);
        for (auto& worker : workers) {
            worker = TWorkerThread::Spawn([]() {
                for (size_t i = 0; i < Iterations; ++i) {
                    TFastThreadLocal<int> local;
                    local = i;
                }
            });
        }
    }

}

}
