#pragma once

#include "util/generic/noncopyable.h"
#include "util/generic/vector.h"
#include "util/system/types.h"

#include <memory>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

class TAffinity
{
private:
    TVector<ui8> Cores;

public:
    static TAffinity Current();

    TAffinity();
    TAffinity(TVector<ui8> cores);

    void Set() const;
    bool Empty() const;

    const TVector<ui8>& GetCores() const;
};

////////////////////////////////////////////////////////////////////////////////

class TAffinityGuard
    : private TMoveOnly
{
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TAffinityGuard(const TAffinity& affinity);
    ~TAffinityGuard();

    void Release();
};

}   // namespace NYdb::NBS
