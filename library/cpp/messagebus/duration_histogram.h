#pragma once

#include <util/datetime/base.h>
#include <util/generic/bitops.h>
#include <util/generic/string.h>

#include <array>

struct TDurationHistogram {
    static const unsigned Buckets = 20;
    std::array<ui64, Buckets> Times;

    static const unsigned SecondBoundary = 11;

    TDurationHistogram() {
        Times.fill(0);
    }

    static unsigned BucketFor(TDuration d) {
        ui64 units = d.MicroSeconds() * (1 << SecondBoundary) / 1000000;
        if (units == 0) {
            return 0;
        }
        unsigned bucket = GetValueBitCount(units) - 1;
        if (bucket >= Buckets) {
            bucket = Buckets - 1;
        }
        return bucket;
    }

    void AddTime(TDuration d) {
        Times[BucketFor(d)] += 1;
    }

    TDurationHistogram& operator+=(const TDurationHistogram& that) {
        for (unsigned i = 0; i < Times.size(); ++i) {
            Times[i] += that.Times[i];
        }
        return *this;
    }

    static TString LabelBefore(unsigned i);

    TString PrintToString() const;
};
