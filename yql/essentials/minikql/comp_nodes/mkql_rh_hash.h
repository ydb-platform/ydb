#pragma once
#include <util/system/compiler.h>
#include <util/system/types.h>
#include <util/generic/bitops.h>
#include <util/generic/yexception.h>
#include <vector>
#include <span>

#include <yql/essentials/minikql/mkql_rh_hash_utils.h>
#include <yql/essentials/utils/prefetch.h>

#include <util/digest/city.h>
#include <util/generic/scope.h>

namespace NKikimr {
namespace NMiniKQL {

template <class TKey>
struct TRobinHoodDefaultSettings {
    static constexpr bool CacheHash = !std::is_arithmetic<TKey>::value;
};

template <typename TKey>
struct TRobinHoodBatchRequestItem {
    // input
    alignas(TKey) char KeyStorage[sizeof(TKey)];

    const TKey& GetKey() const {
        return *reinterpret_cast<const TKey*>(KeyStorage);
    }

    void ConstructKey(const TKey& key) {
        new (KeyStorage) TKey(key);
    }

    // intermediate data
    ui64 Hash;
    char* InitialIterator;
};

constexpr ui32 PrefetchBatchSize = 64;

//TODO: only POD key & payloads are now supported
template <typename TKey, typename TEqual, typename THash, typename TAllocator, typename TDeriv, bool CacheHash>
class TRobinHoodHashBase {
public:
    using iterator = char*;
    using const_iterator = const char*;
protected:
    THash HashLocal_;
    TEqual EqualLocal_;
    template <bool CacheHashForPSL>
    struct TPSLStorageImpl;

    template <>
    struct TPSLStorageImpl<true> {
        i32 Distance = -1;
        ui64 Hash = 0;
        TPSLStorageImpl() = default;
        TPSLStorageImpl(const ui64 hash)
            : Distance(0)
            , Hash(hash) {

        }
    };

    template <>
    struct TPSLStorageImpl<false> {
        i32 Distance = -1;
        TPSLStorageImpl() = default;
        TPSLStorageImpl(const ui64 /*hash*/)
            : Distance(0) {

        }
    };

    using TPSLStorage = TPSLStorageImpl<CacheHash>;

    explicit TRobinHoodHashBase(const ui64 initialCapacity, THash hash, TEqual equal)
        : HashLocal_(std::move(hash))
        , EqualLocal_(std::move(equal))
        , Capacity_(initialCapacity)
        , CapacityShift_(64 - MostSignificantBit(initialCapacity))
        , Allocator_()
        , SelfHash_(GetSelfHash(this))
    {
        Y_ENSURE((Capacity_ & (Capacity_ - 1)) == 0);
    }

    ~TRobinHoodHashBase() {
        if (Data_) {
            Allocator_.deallocate(Data_, DataEnd_ - Data_);
        }
    }

    TRobinHoodHashBase(const TRobinHoodHashBase&) = delete;
    TRobinHoodHashBase(TRobinHoodHashBase&&) = delete;
    void operator=(const TRobinHoodHashBase&) = delete;
    void operator=(TRobinHoodHashBase&&) = delete;

public:
    // returns iterator
    Y_FORCE_INLINE char* Insert(TKey key, bool& isNew) {
        auto hash = HashLocal_(key);
        auto ptr = MakeIterator(hash, Data_, CapacityShift_);
        auto ret = InsertImpl(key, hash, isNew, Data_, DataEnd_, ptr);
        Size_ += isNew ? 1 : 0;
        return ret;
    }

    // should be called after Insert if isNew is true
    Y_FORCE_INLINE void CheckGrow() {
        if (RHHashTableNeedsGrow(Size_, Capacity_)) {
            Grow();
        }
    }

    // returns iterator or nullptr if key is not present
    Y_FORCE_INLINE char* Lookup(TKey key) {
        auto hash = HashLocal_(key);
        auto ptr = MakeIterator(hash, Data_, CapacityShift_);
        auto ret = LookupImpl(key, hash, Data_, DataEnd_, ptr);
        return ret;
    }

    template <typename TSink>
    Y_NO_INLINE void BatchInsert(std::span<TRobinHoodBatchRequestItem<TKey>> batchRequest, TSink&& sink) {
        while (RHHashTableNeedsGrow(Size_ + batchRequest.size(), Capacity_)) {
            Grow();
        }

        for (size_t i = 0; i < batchRequest.size(); ++i) {
            auto& r = batchRequest[i];
            r.Hash = HashLocal_(r.GetKey());
            r.InitialIterator = MakeIterator(r.Hash, Data_, CapacityShift_);
            NYql::PrefetchForWrite(r.InitialIterator);
        }

        for (size_t i = 0; i < batchRequest.size(); ++i) {
            auto& r = batchRequest[i];
            bool isNew;
            auto iter = InsertImpl(r.GetKey(), r.Hash, isNew, Data_, DataEnd_, r.InitialIterator);
            Size_ += isNew ? 1 : 0;
            sink(i, iter, isNew);
        }
    }

    template <typename TSink>
    Y_NO_INLINE void BatchLookup(std::span<TRobinHoodBatchRequestItem<TKey>> batchRequest, TSink&& sink) {
        for (size_t i = 0; i < batchRequest.size(); ++i) {
            auto& r = batchRequest[i];
            r.Hash = HashLocal_(r.GetKey());
            r.InitialIterator = MakeIterator(r.Hash, Data_, CapacityShift_);
            NYql::PrefetchForRead(r.InitialIterator);
        }

        for (size_t i = 0; i < batchRequest.size(); ++i) {
            auto& r = batchRequest[i];
            auto iter = LookupImpl(r.GetKey(), r.Hash, Data_, DataEnd_, r.InitialIterator);
            sink(i, iter);
        }
    }

    ui64 GetCapacity() const {
        return Capacity_;
    }

    void Clear() {
        char* ptr = Data_;
        for (ui64 i = 0; i < Capacity_; ++i) {
            GetPSL(ptr).Distance = -1;
            ptr += AsDeriv().GetCellSize();
        }
        Size_ = 0;
    }

    bool Empty() const {
        return !Size_;
    }

    ui64 GetSize() const {
        return Size_;
    }

    const char* Begin() const {
        return Data_;
    }

    const char* End() const {
        return DataEnd_;
    }

    char* Begin() {
        return Data_;
    }

    char* End() {
        return DataEnd_;
    }

    void Advance(char*& ptr) const {
        ptr += AsDeriv().GetCellSize();
    }

    void Advance(const char*& ptr) const {
        ptr += AsDeriv().GetCellSize();
    }

    bool IsValid(const char* ptr) {
        return GetPSL(ptr).Distance >= 0;
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
    struct TInternalBatchRequestItem : TRobinHoodBatchRequestItem<TKey> {
        char* OriginalIterator;
    };

    Y_FORCE_INLINE char* MakeIterator(const ui64 hash, char* data, ui64 capacityShift) {
        // https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/
        ui64 bucket = ((SelfHash_ ^ hash) * 11400714819323198485llu) >> capacityShift;
        char* ptr = data + AsDeriv().GetCellSize() * bucket;
        return ptr;
    }

    Y_FORCE_INLINE char* InsertImpl(TKey key, const ui64 hash, bool& isNew, char* data, char* dataEnd, char* ptr) {
        isNew = false;
        TPSLStorage psl(hash);
        char* returnPtr;
        typename TDeriv::TPayloadStore tmpPayload;
        for (;;) {
            auto& pslPtr = GetPSL(ptr);
            if (pslPtr.Distance < 0) {
                isNew = true;
                pslPtr = psl;
                GetKey(ptr) = key;
                return ptr;
            }

            if constexpr (CacheHash) {
                if (pslPtr.Hash == psl.Hash && EqualLocal_(GetKey(ptr), key)) {
                    return ptr;
                }
            } else {
                if (EqualLocal_(GetKey(ptr), key)) {
                    return ptr;
                }
            }

            if (psl.Distance > pslPtr.Distance) {
                // swap keys & state
                returnPtr = ptr;
                std::swap(psl, pslPtr);
                std::swap(key, GetKey(ptr));
                AsDeriv().SavePayload(GetPayload(ptr), tmpPayload);
                isNew = true;

                ++psl.Distance;
                AdvancePointer(ptr, data, dataEnd);
                break;
            }

            ++psl.Distance;
            AdvancePointer(ptr, data, dataEnd);
        }

        for (;;) {
            auto& pslPtr = GetPSL(ptr);
            if (pslPtr.Distance < 0) {
                pslPtr = psl;
                GetKey(ptr) = key;
                AsDeriv().RestorePayload(GetMutablePayload(ptr), tmpPayload);
                return returnPtr; // for original key
            }

            if (psl.Distance > pslPtr.Distance) {
                // swap keys & state
                std::swap(psl, pslPtr);
                std::swap(key, GetKey(ptr));
                AsDeriv().SwapPayload(GetMutablePayload(ptr), tmpPayload);
            }

            ++psl.Distance;
            AdvancePointer(ptr, data, dataEnd);
        }
    }

    Y_FORCE_INLINE char* LookupImpl(TKey key, const ui64 hash, char* data, char* dataEnd, char* ptr) {
        i32 currDistance = 0;
        for (;;) {
            auto& pslPtr = GetPSL(ptr);
            if (pslPtr.Distance < 0 || currDistance > pslPtr.Distance) {
                return nullptr;
            }

            if constexpr (CacheHash) {
                if (pslPtr.Hash == hash && EqualLocal_(GetKey(ptr), key)) {
                    return ptr;
                }
            } else {
                if (EqualLocal_(GetKey(ptr), key)) {
                    return ptr;
                }
            }

            ++currDistance;
            AdvancePointer(ptr, data, dataEnd);
        }
    }

    Y_NO_INLINE void Grow() {
        auto newCapacity = Capacity_ * CalculateRHHashTableGrowFactor(Capacity_);
        auto newCapacityShift = 64 - MostSignificantBit(newCapacity);
        char *newData, *newDataEnd;
        Allocate(newCapacity, newData, newDataEnd);
        Y_DEFER {
            Allocator_.deallocate(newData, newDataEnd - newData);
        };

        std::array<TInternalBatchRequestItem, PrefetchBatchSize> batch;
        ui32 batchLen = 0;
        for (auto iter = Begin(); iter != End(); Advance(iter)) {
            if (GetPSL(iter).Distance < 0) {
                continue;
            }

            if (batchLen == batch.size()) {
                CopyBatch({batch.data(), batchLen}, newData, newDataEnd);
                batchLen = 0;
            }

            auto& r = batch[batchLen++];
            r.ConstructKey(GetKey(iter));
            r.OriginalIterator = iter;

            if constexpr (CacheHash) {
                r.Hash = GetPSL(iter).Hash;
            } else {
                r.Hash = HashLocal_(r.GetKey());
            }

            r.InitialIterator = MakeIterator(r.Hash, newData, newCapacityShift);
            NYql::PrefetchForWrite(r.InitialIterator);
        }

        CopyBatch({batch.data(), batchLen}, newData, newDataEnd);

        Capacity_ = newCapacity;
        CapacityShift_ = newCapacityShift;
        std::swap(Data_, newData);
        std::swap(DataEnd_, newDataEnd);
    }

    Y_NO_INLINE void CopyBatch(std::span<TInternalBatchRequestItem> batch, char* newData, char* newDataEnd) {
        for (auto& r : batch) {
            bool isNew;
            auto iter = InsertImpl(r.GetKey(), r.Hash, isNew, newData, newDataEnd, r.InitialIterator);
            Y_ASSERT(isNew);
            AsDeriv().CopyPayload(GetMutablePayload(iter), GetPayload(r.OriginalIterator));
        }
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
        Allocate(Capacity_, Data_, DataEnd_);
    }

private:
    void Allocate(ui64 capacity, char*& data, char*& dataEnd) {
        ui64 bytes = capacity * AsDeriv().GetCellSize();
        data = Allocator_.allocate(bytes);
        dataEnd = data + bytes;
        char* ptr = data;
        for (ui64 i = 0; i < capacity; ++i) {
            GetPSL(ptr).Distance = -1;
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
    ui64 Size_ = 0;
    ui64 Capacity_;
    ui64 CapacityShift_;
    TAllocator Allocator_;
    const ui64 SelfHash_;
    char* Data_ = nullptr;
    char* DataEnd_ = nullptr;
};

template <typename TKey, typename TEqual = std::equal_to<TKey>, typename THash = std::hash<TKey>, typename TAllocator = std::allocator<char>, typename TSettings = TRobinHoodDefaultSettings<TKey>>
class TRobinHoodHashMap : public TRobinHoodHashBase<TKey, TEqual, THash, TAllocator, TRobinHoodHashMap<TKey, TEqual, THash, TAllocator, TSettings>, TSettings::CacheHash> {
public:
    using TSelf = TRobinHoodHashMap<TKey, TEqual, THash, TAllocator, TSettings>;
    using TBase = TRobinHoodHashBase<TKey, TEqual, THash, TAllocator, TSelf, TSettings::CacheHash>;
    using TPayloadStore = int;

    explicit TRobinHoodHashMap(ui32 payloadSize, ui64 initialCapacity = 1u << 8)
        : TBase(initialCapacity, THash(), TEqual())
        , CellSize_(sizeof(typename TBase::TPSLStorage) + sizeof(TKey) + payloadSize)
        , PayloadSize_(payloadSize)
    {
        TmpPayload_.resize(PayloadSize_);
        TmpPayload2_.resize(PayloadSize_);
        TBase::Init();
    }

    explicit TRobinHoodHashMap(ui32 payloadSize, const THash& hash, const TEqual& equal, ui64 initialCapacity = 1u << 8)
        : TBase(initialCapacity, hash, equal)
        , CellSize_(sizeof(typename TBase::TPSLStorage) + sizeof(TKey) + payloadSize)
        , PayloadSize_(payloadSize)
    {
        TmpPayload_.resize(PayloadSize_);
        TmpPayload2_.resize(PayloadSize_);
        TBase::Init();
    }

    ui32 GetCellSize() const {
        return CellSize_;
    }

    void* GetPayloadImpl(char* ptr) {
        return ptr + sizeof(typename TBase::TPSLStorage) + sizeof(TKey);
    }

    const void* GetPayloadImpl(const char* ptr) {
        return ptr + sizeof(typename TBase::TPSLStorage) + sizeof(TKey);
    }

    void CopyPayload(void* dst, const void* src) {
        memcpy(dst, src, PayloadSize_);
    }

    void SavePayload(const void* p, int& store) {
        Y_UNUSED(store);
        memcpy(TmpPayload_.data(), p, PayloadSize_);
    }

    void RestorePayload(void* p, const int& store) {
        Y_UNUSED(store);
        memcpy(p, TmpPayload_.data(), PayloadSize_);
    }

    void SwapPayload(void* p, int& store) {
        Y_UNUSED(store);
        memcpy(TmpPayload2_.data(), p, PayloadSize_);
        memcpy(p, TmpPayload_.data(), PayloadSize_);
        TmpPayload2_.swap(TmpPayload_);
    }

private:
    const ui32 CellSize_;
    const ui32 PayloadSize_;
    using TVec = std::vector<char, TAllocator>;
    TVec TmpPayload_, TmpPayload2_;
};

template <typename TKey, typename TPayload, typename TEqual = std::equal_to<TKey>, typename THash = std::hash<TKey>, typename TAllocator = std::allocator<char>, typename TSettings = TRobinHoodDefaultSettings<TKey>>
class TRobinHoodHashFixedMap : public TRobinHoodHashBase<TKey, TEqual, THash, TAllocator, TRobinHoodHashFixedMap<TKey, TPayload, TEqual, THash, TAllocator, TSettings>, TSettings::CacheHash> {
public:
    using TSelf = TRobinHoodHashFixedMap<TKey, TPayload, TEqual, THash, TAllocator, TSettings>;
    using TBase = TRobinHoodHashBase<TKey, TEqual, THash, TAllocator, TSelf, TSettings::CacheHash>;
    using TPayloadStore = TPayload;

    explicit TRobinHoodHashFixedMap(ui64 initialCapacity = 1u << 8)
        : TBase(initialCapacity, THash(), TEqual())
    {
        TBase::Init();
    }

    explicit TRobinHoodHashFixedMap(const THash& hash, const TEqual& equal, ui64 initialCapacity = 1u << 8)
        : TBase(initialCapacity, hash, equal)
    {
        TBase::Init();
    }

    static constexpr ui32 GetCellSize() {
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

template <typename TKey, typename TEqual = std::equal_to<TKey>, typename THash = std::hash<TKey>, typename TAllocator = std::allocator<char>, typename TSettings = TRobinHoodDefaultSettings<TKey>>
class TRobinHoodHashSet : public TRobinHoodHashBase<TKey, TEqual, THash, TAllocator, TRobinHoodHashSet<TKey, TEqual, THash, TAllocator, TSettings>, TSettings::CacheHash> {
public:
    using TSelf = TRobinHoodHashSet<TKey, TEqual, THash, TAllocator, TSettings>;
    using TBase = TRobinHoodHashBase<TKey, TEqual, THash, TAllocator, TSelf, TSettings::CacheHash>;
    using TPayloadStore = int;

    explicit TRobinHoodHashSet(THash hash, TEqual equal, ui64 initialCapacity = 1u << 8)
        : TBase(initialCapacity, hash, equal) {
        TBase::Init();
    }

    explicit TRobinHoodHashSet(ui64 initialCapacity = 1u << 8)
        : TBase(initialCapacity, THash(), TEqual()) {
        TBase::Init();
    }

    static constexpr ui32 GetCellSize() {
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
