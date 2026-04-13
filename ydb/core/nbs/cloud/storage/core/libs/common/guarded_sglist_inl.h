#pragma once

#if !defined(BLOCKSTORE_INCLUDE_GUARDED_SGLIST_INL)
#error "this file should not be included directly"
#endif

#include "block_buffer.h"

#include <util/generic/buffer.h>
#include <util/generic/string.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TGuardedBuffer<T>::TImpl
    : public TAtomicRefCount<TImpl>
    , private TMoveOnly
{
private:
    TGuardedSgList GuardedObject;
    T Buffer;

public:
    explicit TImpl(T buffer)
        : Buffer(std::move(buffer))
    {}

    ~TImpl()
    {
        GuardedObject.Close();
    }

    TGuardedSgList CreateGuardedSgList(TSgList sglist) const
    {
        return GuardedObject.Create(std::move(sglist));
    }

    const T& GetBuffer() const
    {
        return Buffer;
    }

    T ExtractBuffer()
    {
        GuardedObject.Close();
        return std::move(Buffer);
    }
};

////////////////////////////////////////////////////////////////////////////////

inline TSgList CreateSgList(const TString& s)
{
    if (s.empty()) {
        return {{}};
    }
    return {{s.data(), s.length()}};
}

inline TSgList CreateSgList(const TBuffer& buffer)
{
    if (buffer.Empty()) {
        return {{}};
    }
    return {{buffer.Data(), buffer.Size()}};
}

inline TSgList CreateSgList(const TBlockBuffer& blockBuffer)
{
    return blockBuffer.GetBlocks();
}

template <typename T>
TGuardedBuffer<T>::TGuardedBuffer(T buffer)
    : Impl(MakeIntrusive<TImpl>(std::move(buffer)))
{}

template <typename T>
const T& TGuardedBuffer<T>::Get() const
{
    Y_ABORT_UNLESS(Impl);
    return Impl->GetBuffer();
}

template <typename T>
T TGuardedBuffer<T>::Extract()
{
    Y_ABORT_UNLESS(Impl);
    return Impl->ExtractBuffer();
}

template <typename T>
TGuardedSgList TGuardedBuffer<T>::GetGuardedSgList() const
{
    Y_ABORT_UNLESS(Impl);
    return Impl->CreateGuardedSgList(CreateSgList(Impl->GetBuffer()));
}

template <typename T>
TGuardedSgList TGuardedBuffer<T>::CreateGuardedSgList(TSgList sglist) const
{
    Y_ABORT_UNLESS(Impl);
    return Impl->CreateGuardedSgList(std::move(sglist));
}

}   // namespace NYdb::NBS
