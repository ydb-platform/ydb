#include <ydb/library/actors/interconnect/interconnect_tcp_session.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>

using namespace NActors;

namespace {

constexpr ui32 MaxPassToSwitch = 4;
constexpr ui32 MaxConfidence = 7;
constexpr ui32 DirectSendUntilRetrain = 64;

struct TPredictorCounters {
    ui32 Direct = 0;
    ui32 Train = 0;
};

} // namespace

Y_UNIT_TEST_SUITE(DirectSendPredictor) {
    Y_UNIT_TEST(SwitchesToDirectAfterConsecutiveSuccessfulTraining) {
        TDirectSendPredictor predictor;
        TPredictorCounters counters;

        auto direct = [&] {
            ++counters.Direct;
        };
        auto train = [&] {
            ++counters.Train;
            predictor.Train(true);
        };

        for (ui32 i = 0; i < MaxPassToSwitch; ++i) {
            predictor.Predict(direct, train);
            UNIT_ASSERT_VALUES_EQUAL(counters.Direct, 0);
            UNIT_ASSERT_VALUES_EQUAL(counters.Train, i + 1);
        }

        for (ui32 i = 0; i < DirectSendUntilRetrain; ++i) {
            predictor.Predict(direct, train);
            UNIT_ASSERT_VALUES_EQUAL(counters.Direct, i + 1);
            UNIT_ASSERT_VALUES_EQUAL(counters.Train, MaxPassToSwitch);
        }

        predictor.Predict(direct, train);
        UNIT_ASSERT_VALUES_EQUAL(counters.Direct, DirectSendUntilRetrain);
        UNIT_ASSERT_VALUES_EQUAL(counters.Train, MaxPassToSwitch + 1);

        predictor.Predict(direct, train);
        UNIT_ASSERT_VALUES_EQUAL(counters.Direct, DirectSendUntilRetrain + 1);
        UNIT_ASSERT_VALUES_EQUAL(counters.Train, MaxPassToSwitch + 1);
    }

    Y_UNIT_TEST(FailedTrainingDecrementsConfidence) {
        TDirectSendPredictor predictor;
        TPredictorCounters counters;
        bool trainResult = true;

        auto direct = [&] {
            ++counters.Direct;
        };
        auto train = [&] {
            ++counters.Train;
            predictor.Train(trainResult);
        };

        for (ui32 i = 0; i < MaxPassToSwitch - 1; ++i) {
            predictor.Predict(direct, train);
        }

        trainResult = false;
        predictor.Predict(direct, train);
        UNIT_ASSERT_VALUES_EQUAL(counters.Direct, 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.Train, MaxPassToSwitch);

        trainResult = true;
        for (ui32 i = 0; i < 2; ++i) {
            predictor.Predict(direct, train);
            UNIT_ASSERT_VALUES_EQUAL(counters.Direct, 0);
        }

        predictor.Predict(direct, train);
        UNIT_ASSERT_VALUES_EQUAL(counters.Direct, 1);
        UNIT_ASSERT_VALUES_EQUAL(counters.Train, MaxPassToSwitch + 2);
    }

    Y_UNIT_TEST(ForwardsArgumentsToSelectedCallback) {
        TDirectSendPredictor predictor;
        TPredictorCounters counters;
        ui32 directSum = 0;
        TString directTrace;
        ui32 trainSum = 0;

        auto direct = [&](ui32 value, TStringBuf marker) {
            ++counters.Direct;
            directSum += value;
            directTrace += marker;
        };
        auto train = [&](bool ok, ui32 value) {
            ++counters.Train;
            trainSum += value;
            predictor.Train(ok);
        };

        for (ui32 i = 0; i < MaxPassToSwitch; ++i) {
            predictor.Predict(
                direct,
                std::forward_as_tuple(10u, TStringBuf("direct")),
                train,
                std::forward_as_tuple(true, i + 1));
        }

        UNIT_ASSERT_VALUES_EQUAL(counters.Direct, 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.Train, MaxPassToSwitch);
        UNIT_ASSERT_VALUES_EQUAL(trainSum, 10);

        predictor.Predict(
            direct,
            std::forward_as_tuple(7u, TStringBuf("D")),
            train,
            std::forward_as_tuple(true, 100u));

        UNIT_ASSERT_VALUES_EQUAL(counters.Direct, 1);
        UNIT_ASSERT_VALUES_EQUAL(counters.Train, MaxPassToSwitch);
        UNIT_ASSERT_VALUES_EQUAL(directSum, 7);
        UNIT_ASSERT_VALUES_EQUAL(directTrace, "D");
        UNIT_ASSERT_VALUES_EQUAL(trainSum, 10);
    }

    Y_UNIT_TEST(TrainingMayArriveAfterCallback) {
        TDirectSendPredictor predictor;
        TPredictorCounters counters;
        bool trainingRequested = false;

        auto direct = [&] {
            ++counters.Direct;
        };
        auto train = [&] {
            ++counters.Train;
            trainingRequested = true;
        };

        for (ui32 i = 0; i < MaxPassToSwitch; ++i) {
            predictor.Predict(direct, train);
            UNIT_ASSERT_VALUES_EQUAL(counters.Direct, 0);
            UNIT_ASSERT_VALUES_EQUAL(counters.Train, i + 1);
            UNIT_ASSERT(trainingRequested);
            trainingRequested = false;
            predictor.Train(true);
        }

        predictor.Predict(direct, train);
        UNIT_ASSERT_VALUES_EQUAL(counters.Direct, 1);
        UNIT_ASSERT_VALUES_EQUAL(counters.Train, MaxPassToSwitch);
    }

    Y_UNIT_TEST(MissingTrainingReportKeepsTrainingPath) {
        TDirectSendPredictor predictor;
        TPredictorCounters counters;

        auto direct = [&] {
            ++counters.Direct;
        };
        auto train = [&] {
            ++counters.Train;
        };

        for (ui32 i = 0; i < MaxPassToSwitch + 1; ++i) {
            predictor.Predict(direct, train);
            UNIT_ASSERT_VALUES_EQUAL(counters.Direct, 0);
            UNIT_ASSERT_VALUES_EQUAL(counters.Train, i + 1);
        }
    }

    Y_UNIT_TEST(SingleMissAtHighConfidenceKeepsHysteresis) {
        TDirectSendPredictor predictor;
        TPredictorCounters counters;
        bool trainResult = false;

        auto direct = [&] {
            ++counters.Direct;
        };
        auto train = [&] {
            ++counters.Train;
            predictor.Train(trainResult);
        };

        for (ui32 i = 0; i < MaxConfidence; ++i) {
            predictor.Train(true);
        }

        for (ui32 i = 0; i < DirectSendUntilRetrain; ++i) {
            predictor.Predict(direct, train);
        }

        predictor.Predict(direct, train);
        UNIT_ASSERT_VALUES_EQUAL(counters.Direct, DirectSendUntilRetrain);
        UNIT_ASSERT_VALUES_EQUAL(counters.Train, 1);

        trainResult = true;
        predictor.Predict(direct, train);
        UNIT_ASSERT_VALUES_EQUAL(counters.Direct, DirectSendUntilRetrain);
        UNIT_ASSERT_VALUES_EQUAL(counters.Train, 2);

        predictor.Predict(direct, train);
        UNIT_ASSERT_VALUES_EQUAL(counters.Direct, DirectSendUntilRetrain + 1);
        UNIT_ASSERT_VALUES_EQUAL(counters.Train, 2);
    }

    Y_UNIT_TEST(PropagatesCallbackExceptions) {
        TDirectSendPredictor predictor;

        auto direct = [] {
            ythrow yexception() << "direct callback";
        };
        auto train = [] {
            ythrow yexception() << "train callback";
        };

        UNIT_ASSERT_EXCEPTION_CONTAINS(predictor.Predict(direct, train), yexception, "train callback");

        auto successfulTrain = [&] {
            predictor.Train(true);
        };
        for (ui32 i = 0; i < MaxPassToSwitch; ++i) {
            predictor.Predict([] {}, successfulTrain);
        }

        UNIT_ASSERT_EXCEPTION_CONTAINS(predictor.Predict(direct, train), yexception, "direct callback");
    }
}
