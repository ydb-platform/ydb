#pragma once
#include <util/system/compiler.h>
#include <util/system/types.h>
#include <util/generic/yexception.h>
#include <vector>

namespace NKikimr {
namespace NMiniKQL {

//TODO: only POD key & payloads are now supported
template <typename TKey, typename TEqual, typename THash, typename TAllocator, typename TDeriv>
class TRobinHoodHashBase {
protected:
    using TPSLStorage = i32;

    using TVec = std::vector<char, TAllocator>;

    explicit TRobinHoodHashBase(ui64 initialCapacity = 1u << 8)
        : Capacity(initialCapacity)
    {
        Y_ENSURE((Capacity & (Capacity - 1)) == 0);
    }

public:
    // returns iterator
    Y_FORCE_INLINE char* Insert(TKey key, bool& isNew) {
        auto ret = InsertImpl(key, isNew, Capacity, Data);
        Size += isNew ? 1 : 0;
        return ret;
    }

    // should be called after Insert if isNew is true
    Y_FORCE_INLINE void CheckGrow() {
        if (Size * 2 >= Capacity) {
            Grow();
        }
    }

    ui64 GetSize() const {
        return Size;
    }

    const char* Begin() const {
        return Data.data();
    }

    const char* End() const {
        return Data.data() + Data.size();
    }

    char* Begin() {
        return Data.data();
    }

    char* End() {
        return Data.data() + Data.size();
    }

    void Advance(char*& ptr) {
        ptr += AsDeriv().GetCellSize();
    }

    void Advance(const char*& ptr) {
        ptr += AsDeriv().GetCellSize();
    }

    static const TPSLStorage& GetPSL(const char* ptr) {
        return *(const TPSLStorage*)ptr;
    }

    static const TKey& GetKey(const char* ptr) {
        return *(const TKey*)(ptr + sizeof(TPSLStorage));
    }

    const void* GetPayload(const char* ptr) {
        return AsDeriv().GetPayloadImpl(ptr);
    }

    static TPSLStorage& GetPSL(char* ptr) {
        return *(TPSLStorage*)ptr;
    }

    static TKey& GetKey(char* ptr) {
        return *(TKey*)(ptr + sizeof(TPSLStorage));
    }

    void* GetPayload(char* ptr) {
        return AsDeriv().GetPayloadImpl(ptr);
    }

private:
    Y_FORCE_INLINE char* InsertImpl(TKey key, bool& isNew, ui64 capacity, TVec& data) {
        isNew = false;
        ui64 bucket = THash()(key) & (capacity - 1);
        char* ptr = data.data() + AsDeriv().GetCellSize() * bucket;
        TPSLStorage distance = 0;
        char* returnPtr;
        for (;;) {
            if (GetPSL(ptr) < 0) {
                isNew = true;
                GetPSL(ptr) = distance;
                GetKey(ptr) = key;
                return ptr;
            }

            if (TEqual()(GetKey(ptr), key)) {
                return ptr;
            }

            if (distance > GetPSL(ptr)) {
                // swap keys & state
                returnPtr = ptr;
                std::swap(distance, GetPSL(ptr));
                std::swap(key, GetKey(ptr));
                AsDeriv().SavePayload(GetPayload(ptr));
                isNew = true;

                ++distance;
                AdvancePointer(ptr, data);
                break;
            }

            ++distance;
            AdvancePointer(ptr, data);
        }

        for (;;) {
            if (GetPSL(ptr) < 0) {
                GetPSL(ptr) = distance;
                GetKey(ptr) = key;
                AsDeriv().RestorePayload(GetPayload(ptr));
                return returnPtr; // for original key
            }

            if (distance > GetPSL(ptr)) {
                // swap keys & state
                std::swap(distance, GetPSL(ptr));
                std::swap(key, GetKey(ptr));
                AsDeriv().SwapPayload(GetPayload(ptr));
            }

            ++distance;
            AdvancePointer(ptr, data);
        }
    }

    void Grow() {
        TVec newData;
        auto newCapacity = Capacity * 2;
        Allocate(newCapacity, newData);
        for (auto iter = Begin(); iter != End(); Advance(iter)) {
            if (GetPSL(iter) < 0) {
                continue;
            }

            bool isNew;
            auto newIter = InsertImpl(GetKey(iter), isNew, newCapacity, newData);
            Y_ASSERT(isNew);
            AsDeriv().CopyPayload(GetPayload(newIter), GetPayload(iter));
        }

        Data.swap(newData);
        Capacity = newCapacity;
    }

    void AdvancePointer(char*& ptr, TVec& data) const {
        ptr += AsDeriv().GetCellSize();
        ptr = (ptr == data.data() + data.size()) ? data.data() : ptr;
    }

protected:
    void Init() {
        Allocate(Capacity, Data);
    }

private:
    void Allocate(ui64 capacity, TVec& data) const {
        data.resize(AsDeriv().GetCellSize() * capacity);
        char* ptr = data.data();
        for (ui64 i = 0; i < capacity; ++i) {
            GetPSL(ptr) = -1;
            ptr += AsDeriv().GetCellSize();
        }
    }

    const TDeriv& AsDeriv() const {
        return static_cast<const TDeriv&>(*this);
    }

    TDeriv& AsDeriv() {
        return static_cast<TDeriv&>(*this);
    }

private:
    ui64 Size = 0;
    ui64 Capacity;
    TVec Data;
};

template <typename TKey, typename TEqual = std::equal_to<TKey>, typename THash = std::hash<TKey>, typename TAllocator = std::allocator<char>>
class TRobinHoodHashMap : public TRobinHoodHashBase<TKey, TEqual, THash, TAllocator, TRobinHoodHashMap<TKey, TEqual, THash, TAllocator>> {
public:
    using TSelf = TRobinHoodHashMap<TKey, TEqual, THash, TAllocator>;
    using TBase = TRobinHoodHashBase<TKey, TEqual, THash, TAllocator, TSelf>;

    explicit TRobinHoodHashMap(ui32 payloadSize, ui64 initialCapacity = 1u << 8)
        : TBase(initialCapacity)
        , CellSize(sizeof(typename TBase::TPSLStorage) + sizeof(TKey) + payloadSize)
        , PayloadSize(payloadSize)
    {
        TmpPayload.resize(PayloadSize);
        TmpPayload2.resize(PayloadSize);
        TBase::Init();
    }

    ui32 GetCellSize() const {
        return CellSize;
    }

    void* GetPayloadImpl(char* ptr) {
        return ptr + sizeof(typename TBase::TPSLStorage) + sizeof(TKey);
    }

    const void* GetPayloadImpl(const char* ptr) {
        return ptr + sizeof(typename TBase::TPSLStorage) + sizeof(TKey);
    }

    void CopyPayload(void* dst, const void* src) {
        memcpy(dst, src, PayloadSize);
    }

    void SavePayload(const void* p) {
        memcpy(TmpPayload.data(), p, PayloadSize);
    }

    void RestorePayload(void* p) {
        memcpy(p, TmpPayload.data(), PayloadSize);
    }

    void SwapPayload(void* p) {
        memcpy(TmpPayload2.data(), p, PayloadSize);
        memcpy(p, TmpPayload.data(), PayloadSize);
        TmpPayload2.swap(TmpPayload);
    }

private:
    const ui32 CellSize;
    const ui32 PayloadSize;
    typename TBase::TVec TmpPayload, TmpPayload2;
};

template <typename TKey, typename TEqual = std::equal_to<TKey>, typename THash = std::hash<TKey>, typename TAllocator = std::allocator<char>>
class TRobinHoodHashSet : public TRobinHoodHashBase<TKey, TEqual, THash, TAllocator, TRobinHoodHashSet<TKey, TEqual, THash, TAllocator>> {
public:
    using TSelf = TRobinHoodHashSet<TKey, TEqual, THash, TAllocator>;
    using TBase = TRobinHoodHashBase<TKey, TEqual, THash, TAllocator, TSelf>;

    explicit TRobinHoodHashSet(ui64 initialCapacity = 1u << 8)
        : TBase(initialCapacity)
    {
        TBase::Init();
    }

    ui32 GetCellSize() const {
        return sizeof(typename TBase::TPSLStorage) + sizeof(TKey);
    }

    void* GetPayloadImpl(char* ptr) {
        Y_UNUSED(ptr);
        return nullptr;
    }

    const void* GetPayloadImpl(const char* ptr) {
        Y_UNUSED(ptr);
        return nullptr;
    }

    void CopyPayload(void* dst, const void* src) {
        Y_UNUSED(dst);
        Y_UNUSED(src);
    }

    void SavePayload(const void* p) {
        Y_UNUSED(p);
    }

    void RestorePayload(void* p) {
        Y_UNUSED(p);
    }

    void SwapPayload(void* p) {
        Y_UNUSED(p);
    }
};

}
}
