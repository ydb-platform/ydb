#pragma once

#include <util/generic/ymath.h>
#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>

namespace NGreedyDict {
    enum EEntryScore {
        ES_COUNT,
        ES_LEN_COUNT,
        ES_SIMPLE,
        ES_LEN_SIMPLE,
        ES_SOLAR
    };

    enum EEntryStatTest {
        EST_NONE = 0,
        EST_SIMPLE_NORM = 2
    };

    inline float ModelP(ui32 countA, ui32 countB, ui32 total) {
        return float(countA) * countB / total / total;
    }

    // P (ab | dependent)
    inline float SimpleTest(float modelp, ui32 countAB, ui32 total) {
        float realp = float(countAB) / total;
        return modelp >= realp ? 0 : (realp - modelp);
    }

    inline float SolarTest(float modelp, ui32 countAB, ui32 total) {
        float realp = float(countAB) / total;
        return modelp >= realp ? 0 : (modelp + realp * (log(realp / modelp) - 1));
    }

    // P (ab | dependent) / P (ab)
    inline float SimpleTestNorm(float modelp, ui32 countAB, ui32 total) {
        float realp = float(countAB) / total;
        return modelp >= realp ? 0 : (realp - modelp) / realp;
    }

    inline float StatTest(EEntryStatTest test, float modelp, ui32 countAB, ui32 total) {
        if (!total) {
            return 0;
        }
        switch (test) {
            case EST_NONE:
                return 1;
            case EST_SIMPLE_NORM:
                return SimpleTestNorm(modelp, countAB, total);
        }
        Y_ABORT("no way!");
        return 0;
    }

    inline float Score(EEntryScore score, ui32 len, float modelp, ui32 count, ui32 total) {
        if (!total) {
            return 0;
        }
        ui32 m = 1;
        switch (score) {
            case ES_LEN_COUNT:
                m = len;
                [[fallthrough]];
            case ES_COUNT:
                return m * count;
            case ES_LEN_SIMPLE:
                m = len;
                [[fallthrough]];
            case ES_SIMPLE:
                return m * SimpleTest(modelp, count, total);
            case ES_SOLAR:
                return SolarTest(modelp, count, total);
        }
        Y_ABORT("no way!");
        return 0;
    }

}
