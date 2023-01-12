#pragma once
#include <util/system/compiler.h>
#include <util/system/types.h>
#include <util/generic/yexception.h>
#include <vector>

#include <util/digest/city.h>
#include <util/generic/scope.h>

namespace NKikimr {
namespace NMiniKQL {

//TODO: only POD key & payloads are now supported
template <typename TKey, typename TEqual, typename THash, typename TAllocator, typename TDeriv>
class TRobinHoodHashBase {
protected:
    THash HashLocal;
    TEqual EqualLocal;
    using TPSLStorage = i32;

    explicit TRobinHoodHashBase(const ui64 initialCapacity, THash hash, TEqual equal)
        : HashLocal(std::move(hash))
        , EqualLocal(std::move(equal))
        , Capacity(initialCapacity)
        , Allocator()
        , SelfHash(GetSelfHash(this))
    {
        Y_ENSURE((Capacity & (Capacity - 1)) == 0);
    }

    ~TRobinHoodHashBase() {
        if (Data) {
            Allocator.deallocate(Data, DataEnd - Data);
        }
    }

    TRobinHoodHashBase(const TRobinHoodHashBase&) = delete;
    TRobinHoodHashBase(TRobinHoodHashBase&&) = delete;
    void operator=(const TRobinHoodHashBase&) = delete;
    void operator=(TRobinHoodHashBase&&) = delete;

public:
    // returns iterator
    Y_FORCE_INLINE char* Insert(TKey key, bool& isNew) {
        auto ret = InsertImpl(key, isNew, Capacity, Data, DataEnd);
        Size += isNew ? 1 : 0;
        return ret;
    }

    // should be called after Insert if isNew is true
    Y_FORCE_INLINE void CheckGrow() {
        if (Size * 2 >= Capacity) {
            Grow();
        }
    }

    void Clear() {
        char* ptr = Data;
        for (ui64 i = 0; i < Capacity; ++i) {
            GetPSL(ptr) = -1;
            ptr += AsDeriv().GetCellSize();
        }
        Size = 0;
    }

    bool Empty() const {
        return !Size;
    }

    ui64 GetSize() const {
        return Size;
    }

    const char* Begin() const {
        return Data;
    }

    const char* End() const {
        return DataEnd;
    }

    char* Begin() {
        return Data;
    }

    char* End() {
        return DataEnd;
    }

    void Advance(char*& ptr) {
        ptr += AsDeriv().GetCellSize();
    }

    void Advance(const char*& ptr) {
        ptr += AsDeriv().GetCellSize();
    }

    bool IsValid(const char* ptr) {
        return GetPSL(ptr) >= 0;
    }

    static const TPSLStorage& GetPSL(const char* ptr) {
        return *(const TPSLStorage*)ptr;
    }

    static const TKey& GetKey(const char* ptr) {
        return *(const TKey*)(ptr + sizeof(TPSLStorage));
    }

    static TKey& GetKey(char* ptr) {
        return *(TKey*)(ptr + sizeof(TPSLStorage));
    }

    const void* GetPayload(const char* ptr) {
        return AsDeriv().GetPayloadImpl(ptr);
    }

    static TPSLStorage& GetPSL(char* ptr) {
        return *(TPSLStorage*)ptr;
    }

    void* GetMutablePayload(char* ptr) {
        return AsDeriv().GetPayloadImpl(ptr);
    }

private:
    Y_FORCE_INLINE char* InsertImpl(TKey key, bool& isNew, ui64 capacity, char* data, char* dataEnd) {
        isNew = false;
        ui64 bucket = (SelfHash ^ HashLocal(key)) & (capacity - 1);
        char* ptr = data + AsDeriv().GetCellSize() * bucket;
        TPSLStorage distance = 0;
        char* returnPtr;
        typename TDeriv::TPayloadStore tmpPayload;
        for (;;) {
            if (GetPSL(ptr) < 0) {
                isNew = true;
                GetPSL(ptr) = distance;
                GetKey(ptr) = key;
                return ptr;
            }

            if (EqualLocal(GetKey(ptr), key)) {
                return ptr;
            }

            if (distance > GetPSL(ptr)) {
                // swap keys & state
                returnPtr = ptr;
                std::swap(distance, GetPSL(ptr));
                std::swap(key, GetKey(ptr));
                AsDeriv().SavePayload(GetPayload(ptr), tmpPayload);
                isNew = true;

                ++distance;
                AdvancePointer(ptr, data, dataEnd);
                break;
            }

            ++distance;
            AdvancePointer(ptr, data, dataEnd);
        }

        for (;;) {
            if (GetPSL(ptr) < 0) {
                GetPSL(ptr) = distance;
                GetKey(ptr) = key;
                AsDeriv().RestorePayload(GetMutablePayload(ptr), tmpPayload);
                return returnPtr; // for original key
            }

            if (distance > GetPSL(ptr)) {
                // swap keys & state
                std::swap(distance, GetPSL(ptr));
                std::swap(key, GetKey(ptr));
                AsDeriv().SwapPayload(GetMutablePayload(ptr), tmpPayload);
            }

            ++distance;
            AdvancePointer(ptr, data, dataEnd);
        }
    }

    void Grow() {
        auto newCapacity = Capacity * 2;
        char *newData, *newDataEnd;
        Allocate(newCapacity, newData, newDataEnd);
        Y_DEFER {
            Allocator.deallocate(newData, newDataEnd - newData);
        };

        for (auto iter = Begin(); iter != End(); Advance(iter)) {
            if (GetPSL(iter) < 0) {
                continue;
            }

            bool isNew;
            auto newIter = InsertImpl(GetKey(iter), isNew, newCapacity, newData, newDataEnd);
            Y_ASSERT(isNew);
            AsDeriv().CopyPayload(GetMutablePayload(newIter), GetPayload(iter));
        }

        Capacity = newCapacity;
        std::swap(Data, newData);
        std::swap(DataEnd, newDataEnd);
    }

    void AdvancePointer(char*& ptr, char* begin, char* end) const {
        ptr += AsDeriv().GetCellSize();
        ptr = (ptr == end) ? begin : ptr;
    }

    static ui64 GetSelfHash(void* self) {
        char buf[sizeof(void*)];
        *(void**)buf = self;
        return CityHash64(buf, sizeof(buf));
    }

protected:
    void Init() {
        Allocate(Capacity, Data, DataEnd);
    }

private:
    void Allocate(ui64 capacity, char*& data, char*& dataEnd) {
        ui64 bytes = capacity * AsDeriv().GetCellSize();
        data = Allocator.allocate(bytes);
        dataEnd = data + bytes;
        char* ptr = data;
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
    TAllocator Allocator;
    const ui64 SelfHash;
    char* Data = nullptr;
    char* DataEnd = nullptr;
};

template <typename TKey, typename TEqual = std::equal_to<TKey>, typename THash = std::hash<TKey>, typename TAllocator = std::allocator<char>>
class TRobinHoodHashMap : public TRobinHoodHashBase<TKey, TEqual, THash, TAllocator, TRobinHoodHashMap<TKey, TEqual, THash, TAllocator>> {
public:
    using TSelf = TRobinHoodHashMap<TKey, TEqual, THash, TAllocator>;
    using TBase = TRobinHoodHashBase<TKey, TEqual, THash, TAllocator, TSelf>;
    using TPayloadStore = int;

    explicit TRobinHoodHashMap(ui32 payloadSize, ui64 initialCapacity = 1u << 8)
        : TBase(initialCapacity, THash(), TEqual())
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

    void SavePayload(const void* p, int& store) {
        Y_UNUSED(store);
        memcpy(TmpPayload.data(), p, PayloadSize);
    }

    void RestorePayload(void* p, const int& store) {
        Y_UNUSED(store);
        memcpy(p, TmpPayload.data(), PayloadSize);
    }

    void SwapPayload(void* p, int& store) {
        Y_UNUSED(store);
        memcpy(TmpPayload2.data(), p, PayloadSize);
        memcpy(p, TmpPayload.data(), PayloadSize);
        TmpPayload2.swap(TmpPayload);
    }

private:
    const ui32 CellSize;
    const ui32 PayloadSize;
    using TVec = std::vector<char, TAllocator>;
    TVec TmpPayload, TmpPayload2;
};

template <typename TKey, typename TPayload, typename TEqual = std::equal_to<TKey>, typename THash = std::hash<TKey>, typename TAllocator = std::allocator<char>>
class TRobinHoodHashFixedMap : public TRobinHoodHashBase<TKey, TEqual, THash, TAllocator, TRobinHoodHashFixedMap<TKey, TPayload, TEqual, THash, TAllocator>> {
public:
    using TSelf = TRobinHoodHashFixedMap<TKey, TPayload, TEqual, THash, TAllocator>;
    using TBase = TRobinHoodHashBase<TKey, TEqual, THash, TAllocator, TSelf>;
    using TPayloadStore = TPayload;

    explicit TRobinHoodHashFixedMap(ui64 initialCapacity = 1u << 8)
        : TBase(initialCapacity, THash(), TEqual())
    {
        TBase::Init();
    }

    ui32 GetCellSize() const {
        return sizeof(typename TBase::TPSLStorage) + sizeof(TKey) + sizeof(TPayload);
    }

    void* GetPayloadImpl(char* ptr) {
        return ptr + sizeof(typename TBase::TPSLStorage) + sizeof(TKey);
    }

    const void* GetPayloadImpl(const char* ptr) {
        return ptr + sizeof(typename TBase::TPSLStorage) + sizeof(TKey);
    }

    void CopyPayload(void* dst, const void* src) {
        *(TPayload*)dst = *(const TPayload*)src;
    }

    void SavePayload(const void* p, TPayload& store) {
        store = *(const TPayload*)p;
    }

    void RestorePayload(void* p, const TPayload& store) {
        *(TPayload*)p = store;
    }

    void SwapPayload(void* p, TPayload& store) {
        std::swap(*(TPayload*)p, store);
    }
};

template <typename TKey, typename TEqual = std::equal_to<TKey>, typename THash = std::hash<TKey>, typename TAllocator = std::allocator<char>>
class TRobinHoodHashSet : public TRobinHoodHashBase<TKey, TEqual, THash, TAllocator, TRobinHoodHashSet<TKey, TEqual, THash, TAllocator>> {
public:
    using TSelf = TRobinHoodHashSet<TKey, TEqual, THash, TAllocator>;
    using TBase = TRobinHoodHashBase<TKey, TEqual, THash, TAllocator, TSelf>;
    using TPayloadStore = int;

    explicit TRobinHoodHashSet(ui64 initialCapacity, THash hash, TEqual equal)
        : TBase(initialCapacity, hash, equal) {
        TBase::Init();
    }

    explicit TRobinHoodHashSet(ui64 initialCapacity = 1u << 8)
        : TBase(initialCapacity, THash(), TEqual()) {
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

    void SavePayload(const void* p, int& store) {
        Y_UNUSED(p);
        Y_UNUSED(store);
    }

    void RestorePayload(void* p, const int& store) {
        Y_UNUSED(p);
        Y_UNUSED(store);
    }

    void SwapPayload(void* p, int& store) {
        Y_UNUSED(p);
        Y_UNUSED(store);
    }
};

}
}
