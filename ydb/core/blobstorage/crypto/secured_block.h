#pragma once

#include <util/generic/bitops.h>
#include <util/system/types.h>
#include <util/system/yassert.h>
#include <util/system/compiler.h>
#include <util/generic/utility.h>
#include <util/generic/strbuf.h>
#include <util/generic/cast.h>

#ifdef _win_
#include <windows.h>
#endif

template <class T1, class T2>
inline T2 ModPowerOf2(const T1& a, const T2& b)
{
    Y_ASSERT(IsPowerOf2(b));
    return T2(a) & (b-1);
}

inline bool IsAlignedOn(const void* p, unsigned int alignment)
{
    if (alignment==1 || IsPowerOf2(alignment))
        return ModPowerOf2((size_t)p, alignment) == 0;

    return (size_t)p % alignment == 0;
}

ui8* AlignedAllocate(size_t size);
void AlignedDeallocate(ui8* p);
ui8* UnalignedAllocate(size_t size);
void UnalignedDeallocate(ui8* p);

inline void SecureWipeBuffer(ui8* buf, size_t size)
{
#ifdef _win_
    ::SecureZeroMemory(buf, size);
#else
    ::memset(buf, 0, size);

  /* As best as we can tell, this is sufficient to break any optimisations that
     might try to eliminate "superfluous" memsets. If there's an easy way to
     detect memset_s, it would be better to use that. */
  __asm__ __volatile__("" : : "r"(buf) : "memory");
#endif
}

inline void SecureWipeBuffer(char* buf, size_t size)
{
    SecureWipeBuffer(reinterpret_cast<ui8*>(buf), size);
}

class TAlignedAllocator
{
public:
    ui8* Allocate(size_t nbytes) {
        if (nbytes == 0)
            return nullptr;

        if (nbytes >= 16)
            return AlignedAllocate(nbytes);

        return UnalignedAllocate(nbytes);
    }

    void Deallocate(ui8* ptr, size_t nbytes) {
        SecureWipeBuffer(ptr, nbytes);

        if (nbytes >= 16)
            return AlignedDeallocate(ptr);

        UnalignedDeallocate(ptr);
    }

    ui8* Reallocate(ui8* ptr, size_t oldSize, size_t newSize, bool preserve) {
        if (oldSize == newSize)
            return ptr;

        if (preserve) {
            ui8* newPtr = Allocate(newSize);
            memcpy(newPtr, ptr, Min(oldSize, newSize));
            Deallocate(ptr, oldSize);
            return newPtr;
        }

        Deallocate(ptr, oldSize);
        return Allocate(newSize);
    }
};

template <size_t nbytes, size_t alignment = 8>
class TFixedAllocator
{
public:
    ui8* Allocate(size_t n) {
        Y_UNUSED(n);

        // TODO: use static assert
        Y_ASSERT(IsAlignedOn(Array_, alignment));
        return GetAlignedArray();
    }

    void Deallocate(ui8* ptr, size_t n) {
        Y_UNUSED(n);
        Y_UNUSED(ptr);

        ui8 *p = GetAlignedArray();
        SecureWipeBuffer(p, nbytes);
    }

    ui8* Reallocate(ui8* ptr, size_t oldSize, size_t newSize, bool preserve) {
        Y_UNUSED(preserve);

        if (oldSize > newSize)
            SecureWipeBuffer(ptr + newSize, oldSize - newSize);
        return ptr;
    }

private:
    ui8* GetAlignedArray() {
        return (ui8*)(Array_ + (0 - (size_t)Array_) % 16);
    }

    alignas(alignment) ui8 Array_[nbytes + alignment];
};


template <typename Allocator>
class TBaseBlock
{
public:
    typedef ui8 value_type;
    typedef ui8* iterator;
    typedef const ui8* const_iterator;
    typedef size_t size_type;

public:
    explicit TBaseBlock(size_t size = 0)
        : Size_(size)
    {
        Ptr_ = Allocator_.Allocate(size);
        Clean();
    }

    TBaseBlock(const TBaseBlock<Allocator>& other)
        : TBaseBlock(other.Size_)
    {
        memcpy(Ptr_, other.Ptr_, Size_);
    }

    TBaseBlock(const ui8* ptr, size_t size)
        : TBaseBlock(size)
    {
        if (ptr == nullptr)
            memset(Ptr_, 0, size);
        else
            memcpy(Ptr_, ptr, size);
    }

    ~TBaseBlock() {
        Allocator_.Deallocate(Ptr_, Size_);
    }

    operator const void*() const { return Ptr_; }
    operator void*() { return Ptr_; }

    operator const ui8*() const { return Ptr_; }
    operator ui8*() { return Ptr_; }

    iterator begin() { return Ptr_; }
    const_iterator begin() const { return Ptr_; }

    iterator end() { return Ptr_ + Size_; }
    const_iterator end() const { return Ptr_ + Size_; }

    ui8* Data() { return Ptr_; }
    const ui8* Data() const { return Ptr_; }

    size_t Size() const { return Size_; }
    bool Empty() const { return Size_ == 0; }

    void Assign(const ui8* ptr, size_t size) {
        New(size);
        memcpy(Ptr_, ptr, size);
    }

    void Assign(const TBaseBlock<Allocator>& other) {
        if (this != &other) {
            New(other.Size_);
            memcpy(Ptr_, other.Ptr_, other.Size_);
        }
    }

    TBaseBlock<Allocator>& operator=(const TBaseBlock<Allocator>& other) {
        Assign(other);
        return *this;
    }

    const TBaseBlock<Allocator> operator+(const TBaseBlock<Allocator>& other) {
        TBaseBlock<Allocator> result(Size_ + other.Size_);
        memcpy(result.Ptr_, Ptr_, Size_);
        memcpy(result.Ptr_ + Size_, other.Ptr_, other.Size_);
        return result;
    }

    bool operator==(const TBaseBlock<Allocator>& other) const {
        bool sameSize = (Size_ == other.Size_);
        return sameSize && !memcmp(Ptr_, other.Ptr_, Size_);
    }

    bool operator!=(const TBaseBlock<Allocator>& other) const {
        return !operator==(other);
    }

    void New(size_t newSize) {
        Ptr_ = Allocator_.Reallocate(Ptr_, Size_, newSize, false);
        Size_ = newSize;
    }

    void Clean() {
        memset(Ptr_, 0, Size_);
    }

    void CleanNew(size_t newSize) {
        New(newSize);
        memset(Ptr_, 0, Size_);
    }

    void Grow(size_t newSize) {
        if (newSize > Size_) {
            Ptr_ = Allocator_.Reallocate(Ptr_, Size_, newSize, true);
            Size_ = newSize;
        }
    }

    void CleanGrow(size_t newSize) {
        if (newSize > Size_) {
            Ptr_ = Allocator_.Reallocate(Ptr_, Size_, newSize, true);
            memset(Ptr_ + Size_, 0, newSize - Size_);
            Size_ = newSize;
        }
    }

    void Resize(size_t newSize) {
        Ptr_ = Allocator_.Reallocate(Ptr_, Size_, newSize, true);
        Size_ = newSize;
    }

    void Swap(TBaseBlock<Allocator>& other) {
        DoSwap(Allocator_, other.Allocator_);
        DoSwap(Size_, other.Size_);
        DoSwap(Ptr_, other.Ptr_);
    }

    TStringBuf AsStringBuf() const {
        return { reinterpret_cast<const char*>(Data()), Size() };
    }

private:
    Allocator Allocator_;
    size_t Size_;
    ui8* Ptr_;
};


template <typename A = TAlignedAllocator>
class TSecuredBlock : public TBaseBlock<A>
{
public:
    explicit TSecuredBlock(size_t size = 0)
        : TBaseBlock<A>(size) {}

    TSecuredBlock(const ui8* data, size_t size)
        : TBaseBlock<A>(data, size) {}

    TSecuredBlock(const char* data, size_t size)
        : TBaseBlock<A>(reinterpret_cast<const ui8*>(data), size) {}

    template<typename U>
    void Append(const TBaseBlock<U>& other) {
        size_t oldSize = this->Size();
        this->Grow(oldSize + other.Size());
        memcpy(this->Data() + oldSize, other.Data(), other.Size());
    }

    void Append(const char* data, size_t size) {
        size_t oldSize = this->Size();
        this->Grow(oldSize + size);
        memcpy(this->Data() + oldSize, data, size);
    }
};


template<size_t nbytes, typename A = TFixedAllocator<nbytes>>
class TFixedSecuredBlock : public TBaseBlock<A>
{
public:
    TFixedSecuredBlock()
        : TBaseBlock<A>(nbytes) {}

    explicit TFixedSecuredBlock(const ui8* data)
        : TBaseBlock<A>(data, nbytes) {}

    explicit TFixedSecuredBlock(const char* data)
        : TBaseBlock<A>(reinterpret_cast<const ui8*>(data), nbytes) {}
};
