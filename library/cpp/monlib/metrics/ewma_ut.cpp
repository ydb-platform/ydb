#include "ewma.h"
#include "metric.h"

#include <library/cpp/testing/unittest/registar.h>


using namespace NMonitoring;

const auto EPS = 1e-6;
void ElapseMinute(IExpMovingAverage& ewma) {
    for (auto i = 0; i < 12; ++i) {
        ewma.Tick();
    }
}

Y_UNIT_TEST_SUITE(TEwmaTest) {
    Y_UNIT_TEST(OneMinute) {
        TGauge gauge;

        auto ewma = OneMinuteEwma(&gauge);
        ewma->Update(3);
        ewma->Tick();

        TVector<double> expectedValues {
            0.6,
            0.22072766,
            0.08120117,
            0.02987224,
            0.01098938,
            0.00404277,
            0.00148725,
            0.00054713,
            0.00020128,
            0.00007405,
            0.00002724,
            0.00001002,
            0.00000369,
            0.00000136,
            0.00000050,
            0.00000018,
        };

        for (auto expectedValue : expectedValues) {
            UNIT_ASSERT_DOUBLES_EQUAL(ewma->Rate(), expectedValue, EPS);
            ElapseMinute(*ewma);
        }
    }

    Y_UNIT_TEST(FiveMinutes) {
        TGauge gauge;

        auto ewma = FiveMinuteEwma(&gauge);
        ewma->Update(3);
        ewma->Tick();

        TVector<double> expectedValues {
            0.6,
            0.49123845,
            0.40219203,
            0.32928698,
            0.26959738,
            0.22072766,
            0.18071653,
            0.14795818,
            0.12113791,
            0.09917933,
            0.08120117,
            0.06648190,
            0.05443077,
            0.04456415,
            0.03648604,
            0.02987224,
        };

        for (auto expectedValue : expectedValues) {
            UNIT_ASSERT_DOUBLES_EQUAL(ewma->Rate(), expectedValue, EPS);
            ElapseMinute(*ewma);
        }
    }

    Y_UNIT_TEST(FiveteenMinutes) {
        TGauge gauge;

        auto ewma = FiveteenMinuteEwma(&gauge);
        ewma->Update(3);
        ewma->Tick();

        TVector<double> expectedValues {
            0.6,
            0.56130419,
            0.52510399,
            0.49123845,
            0.45955700,
            0.42991879,
            0.40219203,
            0.37625345,
            0.35198773,
            0.32928698,
            0.30805027,
            0.28818318,
            0.26959738,
            0.25221023,
            0.23594443,
            0.22072766,
        };

        for (auto expectedValue : expectedValues) {
            UNIT_ASSERT_DOUBLES_EQUAL(ewma->Rate(), expectedValue, EPS);
            ElapseMinute(*ewma);
        }
    }
}
