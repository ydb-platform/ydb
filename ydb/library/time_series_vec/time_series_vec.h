#pragma once

#include <util/datetime/base.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>
#include <util/system/types.h>

namespace NKikimr {

template <class T, ui64 IntervalUs = 10'000> class TTimeSeriesMap;
template <class T, ui64 IntervalUs = 10'000> class TTimeSeriesVec;

// Unbounded and sparse variant of TTimeSeriesVec.
template <class T, ui64 IntervalUs>
class TTimeSeriesMap {
    THashMap<ui64, double> Values; // id -> value; unsorted
public:
    void Clear() {
        Values.clear();
    }

    TInstant Align(TInstant instant) const {
        return TInstant::MicroSeconds(instant.MicroSeconds() / IntervalUs * IntervalUs);
    }

    // Amount of stored intervals
    size_t Size() const {
        return Values.size();
    }

    static constexpr TDuration Interval() noexcept {
        return TDuration::MicroSeconds(IntervalUs);
    }

    void Add(TInstant instant, T value) {
        Values[instant.MicroSeconds() / IntervalUs] += value;
    }

    friend class TTimeSeriesVec<T>;
};

// Time series storage and aggregator with compile-time defined discretization
// Values added up and aligned according to grid with `IntervalUs` steps
// Fixed-size circular buffer is used as storage
// Only last consecutive `Size()` intervals are stored
template <class T, ui64 IntervalUs>
class TTimeSeriesVec {
    TVector<T> Values; // circular buffer of intervals
    ui64 Id = 0; // id (instant/interval) of the last value in buffer
public:
    // Make empty time series
    explicit TTimeSeriesVec(size_t size) {
        Values.resize(size, T());
        Id = size - 1;
    }

    // Make a subseries holding values within given time range [begin; end)
    TTimeSeriesVec(const TTimeSeriesVec& source, TInstant begin, TInstant end) {
        ui64 beginId = Max(source.BeginId(), begin.MicroSeconds() / IntervalUs);
        ui64 endId = Min(source.EndId(), end.MicroSeconds() / IntervalUs);
        if (beginId < endId) {
            Values.reserve(endId - beginId);
            for (ui64 id = beginId; id != endId; id++) {
                Values.emplace_back(source.Values[id % source.Size()]);
            }
            Id = endId - 1;
        } else {
            Values.resize(1, T());
            Id = 0;
        }
    }

    void Clear() {
        for (T& value : Values) {
            value = T();
        }
    }

    TInstant Align(TInstant instant) const {
        return TInstant::MicroSeconds(instant.MicroSeconds() / IntervalUs * IntervalUs);
    }

    // Max amount of intervals
    size_t Size() const {
        return Values.size();
    }

    static constexpr TDuration Interval() noexcept {
        return TDuration::MicroSeconds(IntervalUs);
    }

    // Instant of start of the first available interval (the oldest one)
    TInstant Begin() const {
        return TInstant::MicroSeconds(BeginId() * IntervalUs);
    }

    // Instant of interval after the last available one (the newest one)
    TInstant End() const {
        return TInstant::MicroSeconds(EndId() * IntervalUs);
    }

    // Instant of the next interval
    TInstant Next(TInstant instant) const {
        return TInstant::MicroSeconds(instant.MicroSeconds() + IntervalUs);
    }

    // Get value of interval containing instant
    T Get(TInstant instant) const {
        ui64 id = instant.MicroSeconds() / IntervalUs;
        return Values[id % Size()];
    }

    size_t Add(const TTimeSeriesVec& o, TInstant beginLimit = TInstant::Max()) {
        ui64 idx = o.BeginId() % o.Size();
        size_t result = Add(o.Begin(), &o.Values[0] + idx, o.Size() - idx, beginLimit);
        if (idx > 0) {
            result += Add(o.Begin() + o.Interval() * (o.Size() - idx), &o.Values[0], idx, beginLimit);
        }
        return result;
    }

    // Add values from array `values` of size `size` with custom `interval` (optimized for IntervalUs)
    // History overwrite is allowed up to `beginLimit` (excluded)
    // Returns number of added (or skipped) values
    size_t Add(TDuration interval, TInstant start, const T* values, size_t size, TInstant beginLimit = TInstant::Max()) {
        if (interval.MicroSeconds() == IntervalUs) {
            return Add(start, values, size, beginLimit);
        } else {
            size_t result = 0;
            while (size-- && Add(start, *values++, beginLimit)) {
                result++;
                start += interval;
            }
            return result;
        }
    }

    // Add single value at given instant
    // History overwrite is allowed up to `beginLimit` (excluded)
    // Returns true iff addition was successful or skipped (too old)
    bool Add(TInstant instant, T value, TInstant beginLimit = TInstant::Max()) {
        ui64 id = instant.MicroSeconds() / IntervalUs;
        return Add(id, value, beginLimit);
    }

    size_t Add(const TTimeSeriesMap<T>& tsm, TInstant beginLimit = TInstant::Max()) {
        size_t result = 0;
        for (auto [id, value] : tsm.Values) {
            if (Add(id, value, beginLimit)) {
                result++;
            }
        }
        return result;
    }

    // All values less than minBoundary but bigger than beginLimit will be inserted after minBoundary
    size_t AddShifted(
        const TTimeSeriesMap<T>& tsm,
        TInstant minBoundary,
        TInstant beginLimit = TInstant::Max())
    {
        size_t result = 0;
        for (auto [id, value] : tsm.Values) {
            if (AddShifted(id, value, minBoundary, beginLimit)) {
                result++;
            }
        }
        return result;
    }

private:
    ui64 BeginId() const {
        return Id + 1 - Size();
    }

    ui64 EndId() const {
        return Id + 1;
    }

    // Optimized Add() for values with interval equal to IntervalUs
    size_t Add(TInstant start, const T* values, size_t size, TInstant beginLimit = TInstant::Max()) {
        ui64 id1 = start.MicroSeconds() / IntervalUs;
        ui64 id2 = id1 + size;
        if (beginLimit != TInstant::Max()) { // enforce limit
            id2 = Min(id2, beginLimit.MicroSeconds() / IntervalUs + Size() - 1);
        }
        Propagate(id2 - 1);
        ui64 skip = 0;
        if (BeginId() > id1) {
            skip = BeginId() - id1;
        }
        values += skip;
        size -= skip;
        size_t added = 0;
        for (ui64 id = id1 + skip; id < id2; id++, added++) {
            Values[id % Size()] += *values++;
        }
        return added;
    }

    // All values less than minBoundary but bigger than beginLimit will be inserted after minBoundary
    bool AddShifted(
        ui64 id,
        T value,
        TInstant minBoundary,
        TInstant beginLimit = TInstant::Max())
    {
        if (beginLimit != TInstant::Max()) {
            ui64 limitId = beginLimit.MicroSeconds() / IntervalUs + Size() - 1;
            if (id > limitId) {
                Propagate(limitId); // actually do propagation even on failure to be consistent
                return false; // limit exceeded
            }
        }
        ui64 minId = minBoundary.MicroSeconds() / IntervalUs + 1;
        if (id < minId) {
            id = minId;
        }
        Propagate(id);
        if (id < BeginId()) {
            return true; // skip value from past, successfully
        }
        Values[id % Size()] += value;
        return true; // added successfully
    }

    bool Add(ui64 id, T value, TInstant beginLimit = TInstant::Max()) {
        if (beginLimit != TInstant::Max()) {
            ui64 limitId = beginLimit.MicroSeconds() / IntervalUs + Size() - 1;
            if (id > limitId) {
                Propagate(limitId); // actually do propagation even on failure to be consistent
                return false; // limit exceeded
            }
        }
        Propagate(id);
        if (id < BeginId()) {
            return true; // skip value from past, successfully
        }
        Values[id % Size()] += value;
        return true; // added successfully
    }

    void Propagate(ui64 id) {
        if (id <= Id) {
            return;
        }
        if (id - Id >= Size()) {
            Clear();
            Id = id;
        } else {
            while (Id < id) {
                Values[++Id % Size()] = T();
            }
        }
    }
};

}
