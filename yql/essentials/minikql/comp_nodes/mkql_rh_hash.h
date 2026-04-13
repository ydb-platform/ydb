#pragma once

#include <util/system/unaligned_mem.h>
#include <util/system/compiler.h>
#include <util/system/types.h>
#include <util/generic/bitops.h>
#include <util/generic/yexception.h>
#include <vector>
#include <span>

#include <yql/essentials/minikql/mkql_rh_hash_utils.h>
#include <yql/essentials/utils/is_pod.h>
#include <yql/essentials/utils/prefetch.h>

#include <util/digest/city.h>
#include <util/generic/scope.h>

#define MKQL_RH_HASH_MOVE_API_TO_NEW_VERSION

namespace NKikimr::NMiniKQL {

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

// TODO: only POD key & payloads are now supported
template <typename TKey, typename TEqual, typename THash, typename TAllocator, typename TDeriv, bool CacheHash>
class TRobinHoodHashBase {
public:
    static_assert(sizeof(TKey) <= sizeof(void*) * 4, "Key must be small enough for passing by value inside unaligned api.");
    static_assert(NYql::IsPod<TKey>, "Expected POD value type");

    using iterator = char*;
    using const_iterator = const char*;

protected:
    THash HashLocal_;
    TEqual EqualLocal_;
    template <bool CacheHashForPSL>
    struct TPSLStorageImpl;

    template <>
    struct TPSLStorageImpl<true> {
        i32 Distance;
        ui64 Hash;
        TPSLStorageImpl()
            : Distance(-1)
            , Hash(0)
        {
        }
        explicit TPSLStorageImpl(const ui64 hash)
            : Distance(0)
            , Hash(hash)
        {
        }
    };

public:
    template <>
    struct TPSLStorageImpl<false> {
        i32 Distance;
        TPSLStorageImpl()
            : Distance(-1)
        {
        }
        explicit TPSLStorageImpl(const ui64 /*hash*/)
            : Distance(0)
        {
        }
    };

    using TPSLStorage = TPSLStorageImpl<CacheHash>;

protected:
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
            WriteUnaligned<i32>(&static_cast<TPSLStorage*>(GetPslPtr(ptr))->Distance, -1);
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

    bool IsValid(const char* ptr) const {
        return ReadUnaligned<i32>(&static_cast<const TPSLStorage*>(GetPslPtr(ptr))->Distance) >= 0;
    }

    // WARNING: Returns unaligned pointer! Use ReadUnaligned/WriteUnaligned for access
    static const void* GetPslPtr(const char* ptr) {
        return ptr;
    }

    // WARNING: Returns unaligned pointer! Use ReadUnaligned/WriteUnaligned for access
    static const void* GetKeyPtr(const char* ptr) {
        return ptr + sizeof(TPSLStorage);
    }

    // WARNING: Returns unaligned pointer! Use ReadUnaligned/WriteUnaligned for access
    static void* GetKeyPtr(char* ptr) {
        return ptr + sizeof(TPSLStorage);
    }

    static TKey GetKeyValue(const char* ptr) {
        return ReadUnaligned<TKey>(GetKeyPtr(ptr));
    }

    // WARNING: Returns unaligned pointer! Use ReadUnaligned/WriteUnaligned for access
    const void* GetPayloadPtr(const char* ptr) const {
        return AsDeriv().GetPayloadImpl(ptr);
    }

    // WARNING: Returns unaligned pointer! Use ReadUnaligned/WriteUnaligned for access
    static void* GetPslPtr(char* ptr) {
        return ptr;
    }

    // WARNING: Returns unaligned pointer! Use ReadUnaligned/WriteUnaligned for access
    void* GetMutablePayloadPtr(char* ptr) {
        return AsDeriv().GetPayloadImpl(ptr);
    }

private:
    struct TInternalBatchRequestItem: TRobinHoodBatchRequestItem<TKey> {
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
            auto* pslPtr = GetPslPtr(ptr);
            TPSLStorage pslData = ReadUnaligned<TPSLStorage>(pslPtr);
            i32 pslDistance = pslData.Distance;
            if (pslDistance < 0) {
                isNew = true;
                WriteUnaligned<TPSLStorage>(pslPtr, psl);
                SetKeyValue(ptr, key);
                return ptr;
            }

            if constexpr (CacheHash) {
                ui64 pslHash = pslData.Hash;
                if (pslHash == psl.Hash && EqualLocal_(GetKeyValue(ptr), key)) {
                    return ptr;
                }
            } else {
                if (EqualLocal_(GetKeyValue(ptr), key)) {
                    return ptr;
                }
            }

            if (psl.Distance > pslDistance) {
                returnPtr = ptr;

                // swap keys & states
                TKey ptrKey = GetKeyValue(ptr);
                WriteUnaligned<TPSLStorage>(pslPtr, psl);
                SetKeyValue(ptr, key);
                psl = pslData;
                key = ptrKey;

                AsDeriv().SavePayload(GetPayloadPtr(ptr), tmpPayload);
                isNew = true;

                ++psl.Distance;
                AdvancePointer(ptr, data, dataEnd);
                break;
            }

            ++psl.Distance;
            AdvancePointer(ptr, data, dataEnd);
        }

        for (;;) {
            auto pslPtr = GetPslPtr(ptr);
            TPSLStorage pslData = ReadUnaligned<TPSLStorage>(pslPtr);
            i32 pslDistance = pslData.Distance;
            if (pslDistance < 0) {
                WriteUnaligned<TPSLStorage>(pslPtr, psl);
                SetKeyValue(ptr, key);
                AsDeriv().RestorePayload(GetMutablePayloadPtr(ptr), tmpPayload);
                return returnPtr; // for original key
            }

            if (psl.Distance > pslDistance) {
                // swap keys & state
                TKey ptrKey = GetKeyValue(ptr);
                WriteUnaligned<TPSLStorage>(pslPtr, psl);
                SetKeyValue(ptr, key);
                psl = pslData;
                key = ptrKey;
                AsDeriv().SwapPayload(GetMutablePayloadPtr(ptr), tmpPayload);
            }

            ++psl.Distance;
            AdvancePointer(ptr, data, dataEnd);
        }
    }

    static void SetKeyValue(char* ptr, const TKey& key) {
        return WriteUnaligned<TKey>(GetKeyPtr(ptr), key);
    }

    Y_FORCE_INLINE char* LookupImpl(TKey key, const ui64 hash, char* data, char* dataEnd, char* ptr) {
        i32 currDistance = 0;
        for (;;) {
            TPSLStorage pslData = ReadUnaligned<TPSLStorage>(GetPslPtr(ptr));
            i32 pslDistance = pslData.Distance;
            if (pslDistance < 0 || currDistance > pslDistance) {
                return nullptr;
            }

            if constexpr (CacheHash) {
                if (pslData.HashlHash == hash && EqualLocal_(GetKeyValue(ptr), key)) {
                    return ptr;
                }
            } else {
                if (EqualLocal_(GetKeyValue(ptr), key)) {
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
            TPSLStorage pslData = ReadUnaligned<TPSLStorage>(GetPslPtr(iter));
            if (pslData.Distance < 0) {
                continue;
            }

            if (batchLen == batch.size()) {
                CopyBatch({batch.data(), batchLen}, newData, newDataEnd);
                batchLen = 0;
            }

            auto& r = batch[batchLen++];
            r.ConstructKey(GetKeyValue(iter));
            r.OriginalIterator = iter;

            if constexpr (CacheHash) {
                r.Hash = pslData.Hash;
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
            AsDeriv().CopyPayload(GetMutablePayloadPtr(iter), GetPayloadPtr(r.OriginalIterator));
        }
    }

    void AdvancePointer(char*& ptr, char* begin, char* end) const {
        ptr += AsDeriv().GetCellSize();
        ptr = (ptr == end) ? begin : ptr;
    }

    static ui64 GetSelfHash(void* self) {
        char buf[sizeof(void*)];
        WriteUnaligned<void*>(buf, self);
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
            WriteUnaligned<i32>(&static_cast<TPSLStorage*>(GetPslPtr(ptr))->Distance, -1);
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
class TRobinHoodHashMap: public TRobinHoodHashBase<TKey, TEqual, THash, TAllocator, TRobinHoodHashMap<TKey, TEqual, THash, TAllocator, TSettings>, TSettings::CacheHash> {
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

    // WARNING: Returns unaligned pointer! Use ReadUnaligned/WriteUnaligned for access
    void* GetPayloadImpl(char* ptr) {
        return ptr + sizeof(typename TBase::TPSLStorage) + sizeof(TKey);
    }

    // WARNING: Returns unaligned pointer! Use ReadUnaligned/WriteUnaligned for access
    const void* GetPayloadImpl(const char* ptr) const {
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
class TRobinHoodHashFixedMap: public TRobinHoodHashBase<TKey, TEqual, THash, TAllocator, TRobinHoodHashFixedMap<TKey, TPayload, TEqual, THash, TAllocator, TSettings>, TSettings::CacheHash> {
public:
    using TSelf = TRobinHoodHashFixedMap<TKey, TPayload, TEqual, THash, TAllocator, TSettings>;
    using TBase = TRobinHoodHashBase<TKey, TEqual, THash, TAllocator, TSelf, TSettings::CacheHash>;
    using TPayloadStore = TPayload;

    static_assert(NYql::IsPod<TPayload>, "Expected POD value type");

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

    // WARNING: Returns unaligned pointer! Use ReadUnaligned/WriteUnaligned for access
    void* GetPayloadImpl(char* ptr) {
        return ptr + sizeof(typename TBase::TPSLStorage) + sizeof(TKey);
    }

    // WARNING: Returns unaligned pointer! Use ReadUnaligned/WriteUnaligned for access
    const void* GetPayloadImpl(const char* ptr) const {
        return ptr + sizeof(typename TBase::TPSLStorage) + sizeof(TKey);
    }

    void CopyPayload(void* dst, const void* src) {
        WriteUnaligned<TPayload>(dst, ReadUnaligned<TPayload>(src));
    }

    void SavePayload(const void* p, TPayload& store) {
        store = ReadUnaligned<TPayload>(p);
    }

    void RestorePayload(void* p, const TPayload& store) {
        WriteUnaligned<TPayload>(p, store);
    }

    void SwapPayload(void* p, TPayload& store) {
        TPayload temp = ReadUnaligned<TPayload>(p);
        WriteUnaligned<TPayload>(p, store);
        store = temp;
    }
};

template <typename TKey, typename TEqual = std::equal_to<TKey>, typename THash = std::hash<TKey>, typename TAllocator = std::allocator<char>, typename TSettings = TRobinHoodDefaultSettings<TKey>>
class TRobinHoodHashSet: public TRobinHoodHashBase<TKey, TEqual, THash, TAllocator, TRobinHoodHashSet<TKey, TEqual, THash, TAllocator, TSettings>, TSettings::CacheHash> {
public:
    using TSelf = TRobinHoodHashSet<TKey, TEqual, THash, TAllocator, TSettings>;
    using TBase = TRobinHoodHashBase<TKey, TEqual, THash, TAllocator, TSelf, TSettings::CacheHash>;
    using TPayloadStore = int;

    explicit TRobinHoodHashSet(THash hash, TEqual equal, ui64 initialCapacity = 1u << 8)
        : TBase(initialCapacity, hash, equal)
    {
        TBase::Init();
    }

    explicit TRobinHoodHashSet(ui64 initialCapacity = 1u << 8)
        : TBase(initialCapacity, THash(), TEqual())
    {
        TBase::Init();
    }

    static constexpr ui32 GetCellSize() {
        return sizeof(typename TBase::TPSLStorage) + sizeof(TKey);
    }

    void* GetPayloadImpl(char* ptr) {
        Y_UNUSED(ptr);
        return nullptr;
    }

    const void* GetPayloadImpl(const char* ptr) const {
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

} // namespace NKikimr::NMiniKQL
