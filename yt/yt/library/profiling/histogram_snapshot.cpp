#include "histogram_snapshot.h"

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

THistogramSnapshot MergeHistograms(const THistogramSnapshot& first, const THistogramSnapshot& second)
{
    THistogramSnapshot result;

    size_t i = 0, j = 0;
    auto pushFirst = [&] {
        result.Bounds.push_back(first.Bounds[i]);
        result.Values.push_back(first.Values[i]);
        ++i;
    };
    auto pushSecond = [&] {
        result.Bounds.push_back(second.Bounds[j]);
        result.Values.push_back(second.Values[j]);
        ++j;
    };

    while (true) {
        if (i == first.Bounds.size() || j == second.Bounds.size()) {
            break;
        }

        if (first.Bounds[i] < second.Bounds[j]) {
            pushFirst();
        } else if (first.Bounds[i] > second.Bounds[j]) {
            pushSecond();
        } else {
            result.Bounds.push_back(second.Bounds[j]);
            result.Values.push_back(first.Values[i] + second.Values[j]);

            ++i;
            ++j;
        }
    }

    while (i != first.Bounds.size()) {
        pushFirst();
    }

    while (j != second.Bounds.size()) {
        pushSecond();
    }

    int infBucket = 0;
    if (first.Values.size() != first.Bounds.size()) {
        infBucket += first.Values.back();
    }
    if (second.Values.size() != second.Bounds.size()) {
        infBucket += second.Values.back();
    }
    if (infBucket != 0) {
        result.Values.push_back(infBucket);
    }

    return result;
}

THistogramSnapshot& THistogramSnapshot::operator += (const THistogramSnapshot& other)
{
    if (Bounds.empty()) {
        Bounds = other.Bounds;
        Values = other.Values;
    } else if (other.Bounds.empty()) {
        // Do nothing
    } else if (Bounds == other.Bounds) {
        if (Values.size() < other.Values.size()) {
            Values.push_back(0);
            YT_VERIFY(Values.size() == other.Values.size());
        }

        for (size_t i = 0; i < other.Values.size(); ++i) {
            Values[i] += other.Values[i];
        }
    } else {
        *this = MergeHistograms(*this, other);
    }

    return *this;
}

bool THistogramSnapshot::IsEmpty() const
{
    bool empty = true;
    for (auto value : Values) {
        if (value != 0) {
            empty = false;
        }
    }
    return empty;
}

bool THistogramSnapshot::operator == (const THistogramSnapshot& other) const
{
    if (IsEmpty() && other.IsEmpty()) {
        return true;
    }

    return Values == other.Values && Bounds == other.Bounds;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
