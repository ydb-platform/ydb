#pragma once

#include "public.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TFls
{
public:
    TFls() = default;
    ~TFls();

    TFls(const TFls&) = delete;
    TFls& operator=(const TFls&) = delete;

    using TCookie = void*;

    TCookie Get(int index) const;
    void Set(int index, TCookie cookie);

private:
    std::vector<TCookie> Slots_;
};

TFls* GetCurrentFls();
TFls* SwapCurrentFls(TFls* newFls);

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TFlsSlot
{
public:
    TFlsSlot();

    const T& operator*() const;
    T& operator*();

    const T* operator->() const;
    T* operator->();

    T* GetOrCreate() const;
    const T* Get(const TFls& fls) const;

    bool IsInitialized() const;

private:
    T* Create() const;

    const int Index_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#define FLS_INL_H_
#include "fls-inl.h"
#undef FLS_INL_H_
