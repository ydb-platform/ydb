#pragma once

#include <util/system/unaligned_mem.h>
#include <util/system/compiler.h>
#include <util/system/types.h>
#include <util/generic/bitops.h>
#include <util/generic/yexception.h>
#include <span>

#include <yql/essentials/minikql/mkql_rh_hash_utils.h>
#include <yql/essentials/utils/is_pod.h>
#include <yql/essentials/utils/prefetch.h>

#include <util/digest/city.h>
#include <util/generic/scope.h>

namespace NKikimr {
namespace NMiniKQL {

template <typename TKey>
struct TDqRobinHoodBatchRequestItem {
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


// TODO: only POD key & payloads are now supported
template <typename TKey, typename TEqual, typename THash, typename TAllocator>
class TDqRobinHoodHashSet {
public:
    static constexpr const ui32 PrefetchBatchSize = 64;

    static_assert(sizeof(TKey) <= sizeof(void*) * 4, "Key must be small enough for passing by value inside unaligned api.");
    static_assert(NYql::IsPod<TKey>, "Expected POD value type");

    using iterator = char*;
    using const_iterator = const char*;

protected:
    THash HashLocal_;
    TEqual EqualLocal_;
    
public:
    struct TPSLStorage {
        i32 Distance;
        ui32 Hash;
        TPSLStorage()
            : Distance(-1)
            , Hash(0)
        {
        }
        explicit TPSLStorage(const ui32 hash)
            : Distance(0)
            , Hash(hash)
        {
        }
    };

    explicit TDqRobinHoodHashSet(THash hash, TEqual equal, const ui64 initialCapacity = 1u << 8)
        : HashLocal_(std::move(hash))
        , EqualLocal_(std::move(equal))
        , Capacity_(initialCapacity)
        , CapacityShift_(32 - MostSignificantBit(initialCapacity))
        , Allocator_()
        , SelfHash_(GetSelfHash(this))
    {
        Y_ENSURE((Capacity_ & (Capacity_ - 1)) == 0);
        Init();
    }

    ~TDqRobinHoodHashSet() {
        if (Data_) {
            Allocator_.deallocate(Data_, DataEnd_ - Data_);
        }
    }

    TDqRobinHoodHashSet(const TDqRobinHoodHashSet&) = delete;
    TDqRobinHoodHashSet(TDqRobinHoodHashSet&&) = delete;
    void operator=(const TDqRobinHoodHashSet&) = delete;
    void operator=(TDqRobinHoodHashSet&&) = delete;

    // returns iterator
    Y_FORCE_INLINE char* Insert(TKey key, const ui64 hash, bool& isNew) {        
        auto shortHash = static_cast<ui32>(hash);
        auto ptr = MakeIterator(shortHash, Data_, CapacityShift_);
        auto ret = InsertImpl(key, shortHash, isNew, Data_, DataEnd_, ptr);
        Size_ += isNew ? 1 : 0;
        return ret;
    }

    // returns iterator
    Y_FORCE_INLINE char* Insert(TKey key, bool& isNew) {
        return Insert(key, HashLocal_(key), isNew);
    }
    
    // should be called after Insert if isNew is true
    Y_FORCE_INLINE void CheckGrow() {
        if (RHHashTableNeedsGrow(Size_, Capacity_)) {
            Grow();
        }
    }

    ui64 GetCapacity() const {
        return Capacity_;
    }

    void Clear() {
        char* ptr = Data_;
        for (ui64 i = 0; i < Capacity_; ++i) {
            WriteUnaligned<i32>(&static_cast<TPSLStorage*>(GetPslPtr(ptr))->Distance, -1);
            ptr += GetCellSize();
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
        ptr += GetCellSize();
    }

    void Advance(const char*& ptr) const {
        ptr += GetCellSize();
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
    static void* GetPslPtr(char* ptr) {
        return ptr;
    }

    static constexpr ui32 GetCellSize() {
        return sizeof(TPSLStorage) + sizeof(TKey);
    }

private:
    struct TInternalBatchRequestItem: TDqRobinHoodBatchRequestItem<TKey> {
        char* OriginalIterator;
    };

    Y_FORCE_INLINE char* MakeIterator(const ui32 hash, char* data, ui32 capacityShift) {
        // https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/
        ui32 bucket = static_cast<ui32>(((SelfHash_ ^ static_cast<ui64>(hash)) * 11400714819323198485ull)) >> capacityShift;
        char* ptr = data + GetCellSize() * bucket;
        return ptr;
    }

    Y_FORCE_INLINE char* InsertImpl(TKey key, const ui32 hash, bool& isNew, char* data, char* dataEnd, char* ptr) {
        isNew = false;
        TPSLStorage psl(hash);
        char* returnPtr;
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

            ui64 pslHash = pslData.Hash;
            if (pslHash == psl.Hash && EqualLocal_(GetKeyValue(ptr), key)) {
                return ptr;
            }

            if (psl.Distance > pslDistance) {
                returnPtr = ptr;

                // swap keys & states
                TKey ptrKey = GetKeyValue(ptr);
                WriteUnaligned<TPSLStorage>(pslPtr, psl);
                SetKeyValue(ptr, key);
                psl = pslData;
                key = ptrKey;

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
                return returnPtr; // for original key
            }

            if (psl.Distance > pslDistance) {
                // swap keys & state
                TKey ptrKey = GetKeyValue(ptr);
                WriteUnaligned<TPSLStorage>(pslPtr, psl);
                SetKeyValue(ptr, key);
                psl = pslData;
                key = ptrKey;
            }

            ++psl.Distance;
            AdvancePointer(ptr, data, dataEnd);
        }
    }

    static void SetKeyValue(char* ptr, const TKey& key) {
        return WriteUnaligned<TKey>(GetKeyPtr(ptr), key);
    }

    Y_NO_INLINE void Grow() {
        auto newCapacity = Capacity_ * CalculateRHHashTableGrowFactor(Capacity_);
        ui32 newCapacityShift = 32 - MostSignificantBit(newCapacity);
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

            r.Hash = pslData.Hash;

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
            InsertImpl(r.GetKey(), r.Hash, isNew, newData, newDataEnd, r.InitialIterator);
            Y_ASSERT(isNew);
        }
    }

    void AdvancePointer(char*& ptr, char* begin, char* end) const {
        ptr += GetCellSize();
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
        ui64 bytes = capacity * GetCellSize();
        data = Allocator_.allocate(bytes);
        dataEnd = data + bytes;
        char* ptr = data;
        for (ui64 i = 0; i < capacity; ++i) {
            WriteUnaligned<i32>(&static_cast<TPSLStorage*>(GetPslPtr(ptr))->Distance, -1);
            ptr += GetCellSize();
        }
    }

private:
    ui64 Size_ = 0;
    ui64 Capacity_;
    ui32 CapacityShift_;
    TAllocator Allocator_;
    const ui64 SelfHash_;
    char* Data_ = nullptr;
    char* DataEnd_ = nullptr;
};


} // namespace NMiniKQL
} // namespace NKikimr
