#include "postpone_time_predictor.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/timer_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/maybe.h>
#include <util/thread/factory.h>

namespace NYdb::NBS {

namespace {

////////////////////////////////////////////////////////////////////////////////

void RunPredictorJobs(
    TVector<THolder<IThreadFactory::IThread>>& workers,
    IPostponeTimePredictorPtr predictor,
    std::shared_ptr<TTestTimer> timer,
    TDuration delayWindow)
{
    const auto p = delayWindow.Seconds();
    workers.push_back(SystemThreadFactory()->Run(
        [predictor, timer, start = 1, end = p / 3]()
        {
            for (size_t i = start; i <= end; ++i) {
                predictor->Register(TDuration::MilliSeconds(i));
                timer->AdvanceTime(TDuration::MilliSeconds(350));
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
                timer->AdvanceTime(TDuration::MilliSeconds(600));
            }
        }));
    workers.push_back(SystemThreadFactory()->Run(
        [predictor, timer, start = p / 3 + 1, end = p / 3 * 2]()
        {
            for (size_t i = start; i <= end; ++i) {
                predictor->Register(TDuration::MilliSeconds(i));
                timer->AdvanceTime(TDuration::MilliSeconds(500));
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
                timer->AdvanceTime(TDuration::MilliSeconds(450));
            }
        }));
    workers.push_back(SystemThreadFactory()->Run(
        [predictor, timer, start = p / 3 * 2 + 1, end = p]()
        {
            for (size_t i = start; i <= end; ++i) {
                predictor->Register(TDuration::MilliSeconds(i));
                timer->AdvanceTime(TDuration::MilliSeconds(200));
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
                timer->AdvanceTime(TDuration::MilliSeconds(750));
            }
        }));
    for (auto& worker: workers) {
        worker->Join();
    }
    workers.clear();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPostponeTimePredictor)
{
    Y_UNIT_TEST(ShouldReturnZeroWhenEmpty)
    {
        auto timer = std::make_shared<TTestTimer>();
        auto predictor = CreatePostponeTimePredictor(
            timer,
            TDuration::Seconds(1),
            0.5,
            TMaybe<TDuration>());

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            predictor->GetPossiblePostponeDuration());
    }

    Y_UNIT_TEST(ShouldReturnZeroWhenEmptyConcurrently)
    {
        auto timer = std::make_shared<TTestTimer>();
        auto predictor = CreatePostponeTimePredictor(
            timer,
            TDuration::Seconds(1),
            0.5,
            TMaybe<TDuration>());

        TVector<THolder<IThreadFactory::IThread>> workers;
        workers.reserve(3);

        workers.push_back(SystemThreadFactory()->Run(
            [predictor]()
            {
                UNIT_ASSERT_VALUES_EQUAL(
                    TDuration::Zero(),
                    predictor->GetPossiblePostponeDuration());
            }));
        workers.push_back(SystemThreadFactory()->Run(
            [predictor]()
            {
                UNIT_ASSERT_VALUES_EQUAL(
                    TDuration::Zero(),
                    predictor->GetPossiblePostponeDuration());
            }));
        workers.push_back(SystemThreadFactory()->Run(
            [predictor]()
            {
                UNIT_ASSERT_VALUES_EQUAL(
                    TDuration::Zero(),
                    predictor->GetPossiblePostponeDuration());
            }));

        for (auto& worker: workers) {
            worker->Join();
        }
    }

    Y_UNIT_TEST(ShouldReturnMeanWhenReachPercentage)
    {
        auto timer = std::make_shared<TTestTimer>();
        auto predictor = CreatePostponeTimePredictor(
            timer,
            TDuration::Seconds(1),
            0.5,
            TMaybe<TDuration>());

        predictor->Register(TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            predictor->GetPossiblePostponeDuration());

        predictor->Register(TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            predictor->GetPossiblePostponeDuration());

        predictor->Register(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            predictor->GetPossiblePostponeDuration());

        predictor->Register(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(1'500),
            predictor->GetPossiblePostponeDuration());
    }

    Y_UNIT_TEST(ShouldReturnMeanWhenReachPercentageConcurrently)
    {
        auto timer = std::make_shared<TTestTimer>();
        auto predictor = CreatePostponeTimePredictor(
            timer,
            TDuration::Seconds(1),
            0.5,
            TMaybe<TDuration>());

        TVector<THolder<IThreadFactory::IThread>> workers;
        workers.reserve(3);

        workers.push_back(SystemThreadFactory()->Run(
            [predictor]()
            {
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
                predictor->Register(TDuration::Zero());
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
            }));
        workers.push_back(SystemThreadFactory()->Run(
            [predictor]()
            {
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
                predictor->Register(TDuration::Seconds(1));
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
            }));
        workers.push_back(SystemThreadFactory()->Run(
            [predictor]()
            {
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
                predictor->Register(TDuration::Seconds(2));
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
            }));

        for (auto& worker: workers) {
            worker->Join();
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(1'500),
            predictor->GetPossiblePostponeDuration());
    }

    Y_UNIT_TEST(ShouldGetDelayUpperBoundIfDefined)
    {
        auto timer = std::make_shared<TTestTimer>();
        auto predictor = CreatePostponeTimePredictor(
            timer,
            TDuration::Seconds(1),
            0.5,
            TMaybe<TDuration>(TDuration::Seconds(1)));

        predictor->Register(TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            predictor->GetPossiblePostponeDuration());

        predictor->Register(TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            predictor->GetPossiblePostponeDuration());

        predictor->Register(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            predictor->GetPossiblePostponeDuration());

        predictor->Register(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1),   // Instead of 1.5s
            predictor->GetPossiblePostponeDuration());
    }

    Y_UNIT_TEST(ShouldGetDelayUpperBoundIfDefinedConcurrently)
    {
        auto timer = std::make_shared<TTestTimer>();
        auto predictor = CreatePostponeTimePredictor(
            timer,
            TDuration::Seconds(1),
            0.5,
            TMaybe<TDuration>(TDuration::Seconds(1)));

        TVector<THolder<IThreadFactory::IThread>> workers;
        workers.reserve(3);

        workers.push_back(SystemThreadFactory()->Run(
            [predictor]()
            {
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
                predictor->Register(TDuration::Zero());
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
            }));
        workers.push_back(SystemThreadFactory()->Run(
            [predictor]()
            {
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
                predictor->Register(TDuration::Seconds(1));
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
            }));
        workers.push_back(SystemThreadFactory()->Run(
            [predictor]()
            {
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
                predictor->Register(TDuration::Seconds(2));
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
            }));

        for (auto& worker: workers) {
            worker->Join();
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1),   // Insted of 1.5s
            predictor->GetPossiblePostponeDuration());
    }

    Y_UNIT_TEST(ShouldRemoveStaleAfterTimeout)
    {
        auto timer = std::make_shared<TTestTimer>();

        auto predictor = CreatePostponeTimePredictor(
            timer,
            TDuration::Seconds(10),
            0.5,
            TMaybe<TDuration>());

        predictor->Register(TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            predictor->GetPossiblePostponeDuration());

        timer->AdvanceTime(TDuration::MilliSeconds(1'500));

        predictor->Register(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1),
            predictor->GetPossiblePostponeDuration());

        timer->AdvanceTime(TDuration::MilliSeconds(1'000));

        predictor->Register(TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            predictor->GetPossiblePostponeDuration());

        timer->AdvanceTime(TDuration::MilliSeconds(2'500));

        predictor->Register(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(1'500),
            predictor->GetPossiblePostponeDuration());

        timer->AdvanceTime(TDuration::MilliSeconds(5'999));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(1'500),
            predictor->GetPossiblePostponeDuration());

        timer->AdvanceTime(TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(2'000),
            predictor->GetPossiblePostponeDuration());

        timer->AdvanceTime(TDuration::MilliSeconds(4'000));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),   // Zero because of seconds accuracy
            predictor->GetPossiblePostponeDuration());
    }

    Y_UNIT_TEST(ShouldRemoveStaleAfterTimeoutConcurrently)
    {
        auto timer = std::make_shared<TTestTimer>();
        auto predictor = CreatePostponeTimePredictor(
            timer,
            TDuration::Seconds(10),
            0.5,
            TMaybe<TDuration>());

        TVector<THolder<IThreadFactory::IThread>> workers;
        workers.reserve(3);

        workers.push_back(SystemThreadFactory()->Run(
            [predictor]()
            {
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
                predictor->Register(TDuration::Zero());
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
            }));
        workers.push_back(SystemThreadFactory()->Run(
            [predictor]()
            {
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
                predictor->Register(TDuration::Seconds(1));
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
            }));

        for (auto& worker: workers) {
            worker->Join();
        }
        workers.clear();

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1),
            predictor->GetPossiblePostponeDuration());

        workers.push_back(SystemThreadFactory()->Run(
            [predictor, timer]()
            {
                timer->AdvanceTime(TDuration::MilliSeconds(500));
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
                timer->AdvanceTime(TDuration::MilliSeconds(1'000));
                predictor->Register(TDuration::Zero());
                timer->AdvanceTime(TDuration::MilliSeconds(250));
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
                timer->AdvanceTime(TDuration::MilliSeconds(750));
            }));
        workers.push_back(SystemThreadFactory()->Run(
            [predictor, timer]()
            {
                timer->AdvanceTime(TDuration::MilliSeconds(250));
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
                timer->AdvanceTime(TDuration::MilliSeconds(1'000));
                predictor->Register(TDuration::Seconds(2));
                timer->AdvanceTime(TDuration::MilliSeconds(750));
                Y_UNUSED(predictor->GetPossiblePostponeDuration());
                timer->AdvanceTime(TDuration::MilliSeconds(500));
            }));

        for (auto& worker: workers) {
            worker->Join();
        }
        workers.clear();

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(1'500),
            predictor->GetPossiblePostponeDuration());

        timer->AdvanceTime(TDuration::MilliSeconds(5'000));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(2'000),
            predictor->GetPossiblePostponeDuration());

        timer->AdvanceTime(TDuration::MilliSeconds(8'000));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            predictor->GetPossiblePostponeDuration());
    }

    Y_UNIT_TEST(ShouldCorrectlyWorkOnRingBufferOverflow)
    {
        const auto delayWindow = TDuration::Seconds(5);

        auto timer = std::make_shared<TTestTimer>();
        auto predictor = CreatePostponeTimePredictor(
            timer,
            delayWindow,
            0.5,
            TMaybe<TDuration>());

        for (size_t i = 1; i <= delayWindow.Seconds(); ++i) {
            predictor->Register(TDuration::MilliSeconds(i));
            UNIT_ASSERT_VALUES_EQUAL(
                TDuration::MilliSeconds(i * (i + 1) / 2) / i,
                predictor->GetPossiblePostponeDuration());
            timer->AdvanceTime(TDuration::Seconds(1));
        }

        timer->AdvanceTime(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            predictor->GetPossiblePostponeDuration());

        for (size_t i = 1; i <= delayWindow.Seconds(); ++i) {
            predictor->Register(TDuration::MilliSeconds(i));
            UNIT_ASSERT_VALUES_EQUAL(
                TDuration::MilliSeconds(i * (i + 1) / 2) / i,
                predictor->GetPossiblePostponeDuration());
            timer->AdvanceTime(TDuration::Seconds(1));
        }

        timer->AdvanceTime(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MicroSeconds(4'500),
            predictor->GetPossiblePostponeDuration());
        timer->AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(5),
            predictor->GetPossiblePostponeDuration());
        timer->AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            predictor->GetPossiblePostponeDuration());

        for (size_t i = 1; i <= delayWindow.Seconds(); ++i) {
            predictor->Register(TDuration::MilliSeconds(i));
            UNIT_ASSERT_VALUES_EQUAL(
                TDuration::MilliSeconds(i * (i + 1) / 2) / i,
                predictor->GetPossiblePostponeDuration());
            timer->AdvanceTime(TDuration::Seconds(1));
        }

        timer->AdvanceTime(TDuration::Seconds(1));
        predictor->Register(TDuration::MilliSeconds(12));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(6),
            predictor->GetPossiblePostponeDuration());
        timer->AdvanceTime(TDuration::Seconds(3));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(12),
            predictor->GetPossiblePostponeDuration());
        timer->AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(12),
            predictor->GetPossiblePostponeDuration());
        timer->AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            predictor->GetPossiblePostponeDuration());
    }

    Y_UNIT_TEST(ShouldCorrectlyWorkOnRingBufferOverflowConcurrently)
    {
        constexpr auto p = 6;
        constexpr auto delayWindow = TDuration::Seconds(p);

        auto timer = std::make_shared<TTestTimer>();
        auto predictor = CreatePostponeTimePredictor(
            timer,
            delayWindow,
            0.5,
            TMaybe<TDuration>());

        TVector<THolder<IThreadFactory::IThread>> workers;
        workers.reserve(3);

        RunPredictorJobs(workers, predictor, timer, delayWindow);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(p * (p + 1) / 2) / p,
            predictor->GetPossiblePostponeDuration());

        timer->AdvanceTime(TDuration::MilliSeconds(delayWindow.Seconds() * 50));

        RunPredictorJobs(workers, predictor, timer, delayWindow);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(p * (p + 1) / 2) / p,
            predictor->GetPossiblePostponeDuration());
    }
}

}   // namespace NYdb::NBS
