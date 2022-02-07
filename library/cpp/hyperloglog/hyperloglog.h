#pragma once

#include <util/system/types.h>
#include <util/stream/input.h>
#include <util/generic/array_ref.h>

#include <vector>

class IOutputStream;

class THyperLogLogBase {
protected:
    explicit THyperLogLogBase(unsigned precision);

public:
    static const constexpr unsigned PRECISION_MIN = 4;

    static const constexpr unsigned PRECISION_MAX = 18;

    void Update(ui64 hash);

    void Merge(const THyperLogLogBase& rh);

    ui64 Estimate() const;

    void Save(IOutputStream& out) const;

protected:
    unsigned Precision;

    TArrayRef<ui8> RegistersRef;
};

template <typename Alloc>
class THyperLogLogWithAlloc : public THyperLogLogBase {
private:
    explicit THyperLogLogWithAlloc(unsigned precision)
        : THyperLogLogBase(precision) {
        Registers.resize(1u << precision);
        RegistersRef = MakeArrayRef(Registers);
    }

public:
    THyperLogLogWithAlloc(THyperLogLogWithAlloc&&) = default;

    THyperLogLogWithAlloc& operator=(THyperLogLogWithAlloc&&) = default;

    static THyperLogLogWithAlloc Create(unsigned precision) {
        return THyperLogLogWithAlloc(precision);
    }

    static THyperLogLogWithAlloc Load(IInputStream& in) {
        char precision = {};
        Y_ENSURE(in.ReadChar(precision));
        auto res = Create(precision);
        in.LoadOrFail(res.Registers.data(), res.Registers.size() * sizeof(res.Registers.front()));
        return res;
    }

private:
    std::vector<ui8, Alloc> Registers;
};

using THyperLogLog = THyperLogLogWithAlloc<std::allocator<ui8>>;
