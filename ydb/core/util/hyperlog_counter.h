#pragma once
#include "defs.h"
#include <library/cpp/random_provider/random_provider.h>

namespace NKikimr {

///@see APPROXIMATE COUNTING: A DETAILED ANALYSIS (http://algo.inria.fr/flajolet/Publications/Flajolet85c.pdf)
class THyperLogCounter {
public:
    THyperLogCounter();
    explicit THyperLogCounter(ui8 value);
    ui8 GetValue() const;
    ui64 GetEstimatedCounter() const;
    // returns true if value has been changed
    bool Increment(IRandomProvider& randomProvider);
    // returns true if value has been changed
    bool Add(ui64 addend, IRandomProvider& randomProvider);
    // returns true if value has been changed
    bool Merge(const THyperLogCounter& other, IRandomProvider& randomProvider);

private:
    ui8 Value; // actually only 6 bits are used
};

class THybridLogCounter {
public:
    static const ui8 IsHyperLog = 0x80;
    static const ui8 DataMask = 0x7f;

    THybridLogCounter();
    explicit THybridLogCounter(ui8 value);
    ui8 GetValue() const;
    ui64 GetEstimatedCounter() const;
    // returns true if value has been changed
    bool Increment(IRandomProvider& randomProvider);
    // returns true if value has been changed
    bool Add(ui64 addend, IRandomProvider& randomProvider);
    // returns true if value has been changed
    bool Merge(const THybridLogCounter& other, IRandomProvider& randomProvider);

private:
    ui8 Value;
};

} // NKikimr
