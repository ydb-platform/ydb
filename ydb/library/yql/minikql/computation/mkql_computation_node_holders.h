#pragma once

#include <ydb/library/yql/utils/hash.h>

#include "mkql_computation_node_impl.h"
#include "mkql_computation_node_list.h"

#include <ydb/library/yql/minikql/aligned_page_pool.h>
#include <ydb/library/yql/minikql/compact_hash.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/datum.h>

#include <util/generic/maybe.h>
#include <util/memory/pool.h>

#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <optional>
#include <vector>

namespace NKikimr {
namespace NMiniKQL {

class TMemoryUsageInfo;

const ui32 CodegenArraysFallbackLimit = 1000u;

using TKeyTypes = std::vector<std::pair<NUdf::EDataSlot, bool>>;
using TUnboxedValueVector = std::vector<NUdf::TUnboxedValue, TMKQLAllocator<NUdf::TUnboxedValue>>;
using TTemporaryUnboxedValueVector = std::vector<NUdf::TUnboxedValue, TMKQLAllocator<NUdf::TUnboxedValue, EMemorySubPool::Temporary>>;
using TUnboxedValueDeque = std::deque<NUdf::TUnboxedValue, TMKQLAllocator<NUdf::TUnboxedValue>>;
using TKeyPayloadPair = std::pair<NUdf::TUnboxedValue, NUdf::TUnboxedValue>;
using TKeyPayloadPairVector = std::vector<TKeyPayloadPair, TMKQLAllocator<TKeyPayloadPair>>;

class TUnboxedValueBatch {
    // TUnboxedValueBatch represents column values for RowCount rows
    // If wide encoding is used and each row contains Width columns, Values consists of Width * RowCount items:
    //         first Width elements correspond to first row,
    //         second Width elements - to second row, etc
    // For narrow encoding, each row is represented as a single item (a struct) - so Width is equal to 1
private:
    using TBottomType = TUnboxedValueVector;
    using TTopType = std::deque<TBottomType>;

public:
    using value_type = NUdf::TUnboxedValue;

    explicit TUnboxedValueBatch(const TType* rowType = nullptr)
        : Width_((rowType && rowType->IsMulti()) ? static_cast<const TMultiType*>(rowType)->GetElementsCount() : 1u)
        , IsWide_(rowType && rowType->IsMulti())
        , PageSize_(GetPageSize(Width_))
    {
    }

    TUnboxedValueBatch(const TUnboxedValueBatch& other) = default;
    TUnboxedValueBatch& operator=(const TUnboxedValueBatch& other) = default;

    TUnboxedValueBatch(TUnboxedValueBatch&& other)
        : Width_(other.Width_)
        , IsWide_(other.IsWide_)
        , PageSize_(other.PageSize_)
        , Values_(std::move(other.Values_))
        , RowOffset_(other.RowOffset_)
        , RowCount_(other.RowCount_)
    {
        other.clear();
    }

    inline void clear() {
        Values_.clear();
        RowOffset_ = RowCount_ = 0;
    }

    inline bool empty() const {
        return RowCount_ == 0;
    }

    inline void swap(TUnboxedValueBatch& other) {
        std::swap(Width_, other.Width_);
        std::swap(PageSize_, other.PageSize_);
        std::swap(Values_, other.Values_);
        std::swap(RowOffset_, other.RowOffset_);
        std::swap(RowCount_, other.RowCount_);
    }

    template<typename... TArgs>
    void emplace_back(TArgs&&... args) {
        MKQL_ENSURE(!IsWide(), "emplace_back() should not be used for wide batch");
        if (Values_.empty() || Values_.back().size() == Values_.back().capacity()) {
            Values_.emplace_back();
            Values_.back().reserve(PageSize_);
        }
        Values_.back().emplace_back(std::forward<TArgs>(args)...);
        RowCount_++;
    }

    inline void push_back(const value_type& row) {
        emplace_back(row);
    }

    inline void push_back(value_type&& row) {
        emplace_back(std::move(row));
    }

    template<typename TFunc>
    auto ForEachRow(const TFunc& cb) const {
        MKQL_ENSURE(!IsWide(), "ForEachRowWide() should be used instead");
        return DoForEachRow<const NUdf::TUnboxedValue, const TUnboxedValueBatch>(this,
            [&cb](const NUdf::TUnboxedValue* values, ui32 width) {
                Y_DEBUG_ABORT_UNLESS(width == 1);
                Y_DEBUG_ABORT_UNLESS(values);
                return cb(*values);
            });
    }

    template<typename TFunc>
    auto ForEachRow(const TFunc& cb) {
        MKQL_ENSURE(!IsWide(), "ForEachRowWide() should be used instead");
        return DoForEachRow<NUdf::TUnboxedValue, TUnboxedValueBatch>(this,
            [&cb](NUdf::TUnboxedValue* values, ui32 width) {
                Y_DEBUG_ABORT_UNLESS(width == 1);
                Y_DEBUG_ABORT_UNLESS(values);
                return cb(*values);
            });
    }

    template<typename TFunc>
    auto ForEachRowWide(const TFunc& cb) const {
        MKQL_ENSURE(IsWide(), "ForEachRow() should be used instead");
        return DoForEachRow<const NUdf::TUnboxedValue, const TUnboxedValueBatch>(this, cb);
    }

    template<typename TFunc>
    auto ForEachRowWide(const TFunc& cb) {
        MKQL_ENSURE(IsWide(), "ForEachRow() should be used instead");
        return DoForEachRow<NUdf::TUnboxedValue, TUnboxedValueBatch>(this, cb);
    }

    inline TMaybe<ui32> Width() const {
        return IsWide_ ? Width_ : TMaybe<ui32>{};
    }

    inline bool IsWide() const {
        return IsWide_;
    }

    inline ui64 RowCount() const {
        return RowCount_;
    }

    const value_type* Head() const {
        MKQL_ENSURE(RowCount_, "Head() on empty batch");
        return Width_ ? &Values_.front()[RowOffset_ * Width_] : nullptr;
    }

    value_type* Head() {
        MKQL_ENSURE(RowCount_, "Head() on empty batch");
        return Width_ ? &Values_.front()[RowOffset_ * Width_] : nullptr;
    }

    inline void Pop(size_t rowCount = 1) {
        MKQL_ENSURE(rowCount <= RowCount_, "Invalid arg");
        ui64 newStartOffset = (RowOffset_ + rowCount) * Width_;
        while (newStartOffset >= PageSize_) {
            MKQL_ENSURE_S(!Values_.empty());
            Values_.pop_front();
            newStartOffset -= PageSize_;
        }

        RowOffset_ = Width_ ? newStartOffset / Width_ : 0;
        RowCount_ -= rowCount;
    }

    template<typename TFunc>
    void PushRow(const TFunc& producer) {
        ReserveNextRow();
        for (ui32 i = 0; i < Width_; ++i) {
            Values_.back().emplace_back(producer(i));
        }
        ++RowCount_;
    }

    void PushRow(NUdf::TUnboxedValue* values, ui32 width) {
        Y_DEBUG_ABORT_UNLESS(width == Width_);
        ReserveNextRow();
        for (ui32 i = 0; i < Width_; ++i) {
            Values_.back().emplace_back(std::move(values[i]));
        }
        ++RowCount_;
    }

private:
    static const size_t DesiredPageSize = 1024;
    static inline size_t GetPageSize(size_t width) {
        if (!width) {
            return DesiredPageSize;
        }
        size_t pageSize = DesiredPageSize + width - 1;
        return pageSize - pageSize % width;
    }

    inline void ReserveNextRow() {
        bool full = Width_ && (Values_.empty() || Values_.back().size() == PageSize_);
        if (full) {
            Values_.emplace_back();
            Values_.back().reserve(PageSize_);
        }
    }

    template<typename TValue, typename TParent, typename TFunc>
    static auto DoForEachRow(TParent* parent, const TFunc& cb) {
        using TReturn = typename std::result_of<TFunc(TValue*, ui32)>::type;

        auto currTop = parent->Values_.begin();

        Y_DEBUG_ABORT_UNLESS(parent->PageSize_ > parent->RowOffset_);
        Y_DEBUG_ABORT_UNLESS(parent->Width_ == 0 || (parent->PageSize_ - parent->RowOffset_) % parent->Width_ == 0);

        size_t valuesOnPage = parent->PageSize_ - parent->RowOffset_;

        TValue* values = (parent->Width_ && parent->RowCount_) ? currTop->data() + parent->RowOffset_ : nullptr;
        for (size_t i = 0; i < parent->RowCount_; ++i) {
            if constexpr (std::is_same_v<TReturn, bool>) {
                if (!cb(values, parent->Width_)) {
                    return false;
                }
            } else {
                static_assert(std::is_same_v<TReturn, void>, "Callback should either return bool or void");
                cb(values, parent->Width_);
            }
            values += parent->Width_;
            valuesOnPage -= parent->Width_;
            if (!valuesOnPage) {
                valuesOnPage = parent->PageSize_;
                ++currTop;
                values = currTop->data();
            }
        }

        if constexpr (std::is_same_v<TReturn, bool>) {
            return true;
        }
    }

    ui32 Width_;
    bool IsWide_;
    size_t PageSize_;

    TTopType Values_;
    ui64 RowOffset_ = 0;
    ui64 RowCount_ = 0;
};

inline int CompareValues(NUdf::EDataSlot type, bool asc, bool isOptional, const NUdf::TUnboxedValuePod& lhs, const NUdf::TUnboxedValuePod& rhs) {
    int cmp;
    if (isOptional) {
        if (!lhs && !rhs) {
            cmp = 0;
        }
        else if (!lhs) {
            cmp = -1;
        }
        else if (!rhs) {
            cmp = 1;
        }
        else {
            cmp = NUdf::CompareValues(type, lhs, rhs);
        }
    }
    else {
        cmp = NUdf::CompareValues(type, lhs, rhs);
    }

    if (!asc) {
        cmp = -cmp;
    }

    return cmp;
}

inline int CompareValues(const NUdf::TUnboxedValuePod* left, const NUdf::TUnboxedValuePod* right, const TKeyTypes& types, const bool* directions) {
    for (ui32 i = 0; i < types.size(); ++i) {
        if (const auto cmp = CompareValues(types[i].first, directions[i], types[i].second, left[i], right[i])) {
            return cmp;
        }
    }

    return 0;
}

inline int CompareKeys(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right, const TKeyTypes& types, bool isTuple) {
    if (isTuple) {
        if (left && right)
            for (ui32 i = 0; i < types.size(); ++i) {
                if (const auto cmp = CompareValues(types[i].first, true, types[i].second, left.GetElement(i), right.GetElement(i))) {
                    return cmp;
                }
            }
        else if (!left && right)
            return -1;
        else if (left && !right)
            return 1;

        return 0;
    }
    else {
        return CompareValues(types.front().first, true, types.front().second, left, right);
    }
}

struct TKeyPayloadPairLess {
    TKeyPayloadPairLess(const TKeyTypes& types, bool isTuple, const NUdf::ICompare* compare)
        : Types(&types)
        , IsTuple(isTuple)
        , Compare(compare)
    {}

    bool operator()(const TKeyPayloadPair& left, const TKeyPayloadPair& right) const {
        if (Compare) {
            return Compare->Less(left.first, right.first);
        }

        return CompareKeys(left.first, right.first, *Types, IsTuple) < 0;
    }

    const TKeyTypes* Types;
    bool IsTuple;
    const NUdf::ICompare* Compare;
};

struct TKeyPayloadPairEqual {
    TKeyPayloadPairEqual(const TKeyTypes& types, bool isTuple, const NUdf::IEquate* equate)
        : Types(&types)
        , IsTuple(isTuple)
        , Equate(equate)
    {}

    bool operator()(const TKeyPayloadPair& left, const TKeyPayloadPair& right) const {
        if (Equate) {
            return Equate->Equals(left.first, right.first);
        }

        return CompareKeys(left.first, right.first, *Types, IsTuple) == 0;
    }

    const TKeyTypes* Types;
    bool IsTuple;
    const NUdf::IEquate* Equate;
};

struct TValueEqual {
    TValueEqual(const TKeyTypes& types, bool isTuple, const NUdf::IEquate* equate)
        : Types(&types)
        , IsTuple(isTuple)
        , Equate(equate)
    {}

    bool operator()(const NUdf::TUnboxedValue& left, const NUdf::TUnboxedValue& right) const {
        if (Equate) {
            return Equate->Equals(left, right);
        }

        return CompareKeys(left, right, *Types, IsTuple) == 0;
    }

    const TKeyTypes* Types;
    bool IsTuple;
    const NUdf::IEquate* Equate;
};

struct TValueLess {
    TValueLess(const TKeyTypes& types, bool isTuple, const NUdf::ICompare* compare)
        : Types(&types)
        , IsTuple(isTuple)
        , Compare(compare)
    {}

    bool operator()(const NUdf::TUnboxedValue& left, const NUdf::TUnboxedValue& right) const {
        if (Compare) {
            return Compare->Less(left, right);
        }

        return CompareKeys(left, right, *Types, IsTuple) < 0;
    }

    const TKeyTypes* Types;
    bool IsTuple;
    const NUdf::ICompare* Compare;
};

constexpr NUdf::THashType HashOfNull = ~0ULL;

struct TValueHasher {
    TValueHasher(const TKeyTypes& types, bool isTuple, const NUdf::IHash* hash)
        : Types(&types)
        , IsTuple(isTuple)
        , Hash(hash)
    {}

    NUdf::THashType operator()(const NUdf::TUnboxedValuePod& value) const {
        if (Hash) {
            return Hash->Hash(value);
        }

        if (!value)
            return HashOfNull;

        if (IsTuple) {
            NUdf::THashType hash = 0ULL;
            if (auto elements = value.GetElements())
                for (const auto& type : (*Types)) {
                    if (const auto v = *elements++)
                        hash = CombineHashes(hash, NUdf::GetValueHash(type.first, v));
                    else
                        hash = CombineHashes(hash, HashOfNull);
                }
            else
                for (auto i = 0U; i < Types->size(); ++i) {
                    if (const auto v = value.GetElement(i))
                        hash = CombineHashes(hash, NUdf::GetValueHash((*Types)[i].first, v));
                    else
                        hash = CombineHashes(hash, HashOfNull);
                }
            return hash;
        }
        return NUdf::GetValueHash((*Types).front().first, value);
    }

    const TKeyTypes* Types;
    bool IsTuple;
    const NUdf::IHash* Hash;
};



template<typename T>
struct TFloatHash : private std::hash<T> {
    std::size_t operator()(T value) const {
        return std::isnan(value) ? ~0ULL : std::hash<T>::operator()(value);
    }
};

template<typename T>
struct TFloatEquals {
    bool operator()(T l, T r) const {
        return std::isunordered(l, r) ? std::isnan(l) == std::isnan(r) : l == r;
    }
};

template <typename T>
using TMyHash = std::conditional_t<std::is_floating_point<T>::value, TFloatHash<T>, std::hash<T>>;

template <typename T>
using TMyEquals = std::conditional_t<std::is_floating_point<T>::value, TFloatEquals<T>, std::equal_to<T>>;

constexpr float COMPACT_HASH_MAX_LOAD_FACTOR = 1.2f;

using TValuesDictHashMap = std::unordered_map<
        NUdf::TUnboxedValue, NUdf::TUnboxedValue,
        NYql::TVaryingHash<NUdf::TUnboxedValue, TValueHasher>, TValueEqual,
        TMKQLAllocator<std::pair<const NUdf::TUnboxedValue, NUdf::TUnboxedValue>>>;

using TValuesDictHashSet = std::unordered_set<
    NUdf::TUnboxedValue, NYql::TVaryingHash<NUdf::TUnboxedValue, TValueHasher>, TValueEqual,
    TMKQLAllocator<NUdf::TUnboxedValue>>;

template <typename T>
using TValuesDictHashSingleFixedSet = std::unordered_set<T, NYql::TVaryingHash<T, TMyHash<T>>, TMyEquals<T>, TMKQLAllocator<T>>;

template <typename T>
using TValuesDictHashSingleFixedCompactSet = NCHash::TCompactHashSet<T, TMyHash<T>, TMyEquals<T>>;

/*
 * All *Small* functions expect the 'value' content to be formed by the TValuePacker class,
 * which embeds the encoded data length at the beginning of the buffer
 */
inline bool IsSmallValueEmbedded(ui64 value) {
    return (value & 1) != 0;
}

inline TStringBuf GetSmallValue(const ui64& value) {
    if (!IsSmallValueEmbedded(value)) {
        // pointer
        const char* ptr = (const char*)value;
        ui32 length = *(const ui32*)ptr;
        return TStringBuf(ptr, length + 4);
    } else {
        // embedded
        ui32 length = (value & 0x0f) >> 1;
        return TStringBuf(((const char*)&value), length + 1);
    }
}

inline ui64 AddSmallValue(TPagedArena& pool, const TStringBuf& value) {
    if (value.size() <= 8) {
        ui64 ret = 0;
        memcpy((ui8*)&ret, value.data(), value.size());
        Y_DEBUG_ABORT_UNLESS(IsSmallValueEmbedded(ret));
        return ret;
    }
    else {
        auto ptr = pool.Alloc(value.size());
        memcpy((ui8*)ptr, value.data(), value.size());
        return (ui64)ptr;
    }
}

inline ui64 AsSmallValue(const TStringBuf& value) {
    if (value.size() <= 8) {
        ui64 ret = 0;
        memcpy((ui8*)&ret, value.data(), value.size());
        return ret;
    }
    else {
        return (ui64)value.data();
    }
}

struct TSmallValueEqual {
    bool operator()(ui64 lhs, ui64 rhs) const {
        return IsSmallValueEmbedded(lhs) ? lhs == rhs : GetSmallValue(lhs) == GetSmallValue(rhs);
    }
};

struct TSmallValueHash {
    ui64 operator()(ui64 value) const {
        return THash<TStringBuf>()(GetSmallValue(value));
    }
};

using TValuesDictHashCompactSet = NCHash::TCompactHashSet<ui64, TSmallValueHash, TSmallValueEqual>;
using TValuesDictHashCompactMap = NCHash::TCompactHash<ui64, ui64, TSmallValueHash, TSmallValueEqual>;
using TValuesDictHashCompactMultiMap = NCHash::TCompactMultiHash<ui64, ui64, TSmallValueHash, TSmallValueEqual>;

template <typename T>
using TValuesDictHashSingleFixedCompactMap = NCHash::TCompactHash<T, ui64, TMyHash<T>, TMyEquals<T>>;
template <typename T>
using TValuesDictHashSingleFixedCompactMultiMap = NCHash::TCompactMultiHash<T, ui64, TMyHash<T>, TMyEquals<T>>;

template <typename T>
using TValuesDictHashSingleFixedMap = std::unordered_map<T, NUdf::TUnboxedValue, NYql::TVaryingHash<T, TMyHash<T>>, TMyEquals<T>,
    TMKQLAllocator<std::pair<const T, NUdf::TUnboxedValue>>>;

using THashedDictFiller = std::function<void(TValuesDictHashMap&)>;
using THashedSetFiller = std::function<void(TValuesDictHashSet&)>;
using TSortedDictFiller = std::function<void(TKeyPayloadPairVector&)>;
using TSortedSetFiller = std::function<void(TUnboxedValueVector&)>;

enum class EDictSortMode {
    RequiresSorting,
    SortedUniqueAscending,
    SortedUniqueDescening
};

class TTypeHolder: public TComputationValue<TTypeHolder> {
public:
    TTypeHolder(TMemoryUsageInfo* memInfo, TType* type)
        : TComputationValue(memInfo)
        , Type(type)
    {}

    NUdf::TStringRef GetResourceTag() const override {
        return NUdf::TStringRef::Of("TypeHolder");
    }

    void* GetResource() override {
        return Type;
    }

private:
    TType* const Type;
};

class TArrowBlock: public TComputationValue<TArrowBlock> {
public:
    explicit TArrowBlock(TMemoryUsageInfo* memInfo, arrow::Datum&& datum)
        : TComputationValue(memInfo)
        , Datum_(std::move(datum))
    {
    }

    inline static TArrowBlock& From(const NUdf::TUnboxedValue& value) {
        return *static_cast<TArrowBlock*>(value.AsBoxed().Get());
    }

    inline arrow::Datum& GetDatum() {
        return Datum_;
    }

    NUdf::TStringRef GetResourceTag() const override {
        return NUdf::TStringRef::Of("ArrowBlock");
    }

    void* GetResource() override {
        return &Datum_;
    }

private:
    arrow::Datum Datum_;
};

template <class IFace>
class TTypeOperationsRegistry {
    using TValuePtr = typename IFace::TPtr;
public:
    IFace* FindOrEmplace(const TType& type) {
        auto it = Registry.find(type);
        if (it == Registry.end()) {
            TTypeBase tb(type);
            TValuePtr ptr;
            if constexpr (std::is_same_v<IFace, NUdf::IHash>) {
                ptr = MakeHashImpl(&type);
            } else if constexpr (std::is_same_v<IFace, NUdf::IEquate>) {
                ptr = MakeEquateImpl(&type);
            } else if constexpr (std::is_same_v<IFace, NUdf::ICompare>) {
                ptr = MakeCompareImpl(&type);
            } else {
                static_assert(TDependentFalse<IFace>, "unexpected type");
            }
            auto p = std::make_pair((const TTypeBase)type, ptr);
            it = Registry.insert(p).first;
        }
        return it->second.Get();
    }

private:
    THashMap<TTypeBase, TValuePtr, THasherTType, TEqualTType> Registry;
};

class TDirectArrayHolderInplace : public TComputationValue<TDirectArrayHolderInplace> {
public:
    void* operator new(size_t sz) = delete;
    void* operator new[](size_t sz) = delete;
    void operator delete(void *mem, std::size_t sz) {
        const auto pSize = static_cast<void*>(static_cast<ui8*>(mem) + sizeof(TComputationValue<TDirectArrayHolderInplace>));
        FreeWithSize(mem, sz + *static_cast<ui64*>(pSize) * sizeof(NUdf::TUnboxedValue));
    }

    void operator delete[](void *mem, std::size_t sz) = delete;

    TDirectArrayHolderInplace(TMemoryUsageInfo* memInfo, ui64 size)
        : TComputationValue(memInfo)
        , Size(size)
    {
        MKQL_ENSURE(Size > 0U, "Can't create empty array holder.");
        MKQL_MEM_TAKE(GetMemInfo(), GetPtr(), Size * sizeof(NUdf::TUnboxedValue));
        std::memset(GetPtr(), 0, Size * sizeof(NUdf::TUnboxedValue));
    }

    ~TDirectArrayHolderInplace() {
        for (ui64 i = 0U; i < Size; ++i) {
            (GetPtr() + i)->~TUnboxedValue();
        }
        MKQL_MEM_RETURN(GetMemInfo(), GetPtr(), Size * sizeof(NUdf::TUnboxedValue));
    }

    ui64 GetSize() const {
        return Size;
    }

    NUdf::TUnboxedValue* GetPtr() const {
        return (NUdf::TUnboxedValue*)(this + 1);
    }

private:
    class TIterator : public TTemporaryComputationValue<TIterator> {
    public:
        TIterator(const TDirectArrayHolderInplace* parent)
            : TTemporaryComputationValue(parent->GetMemInfo()), Parent(const_cast<TDirectArrayHolderInplace*>(parent))
        {}

    private:
        bool Skip() final {
            return ++Current < Parent->GetSize();
        }

        bool Next(NUdf::TUnboxedValue& value) final {
            if (!Skip())
                return false;
            value = Parent->GetPtr()[Current];
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) final {
            if (!Next(payload))
                return false;
            key = NUdf::TUnboxedValuePod(Current);
            return true;
        }

        const NUdf::TRefCountedPtr<TDirectArrayHolderInplace> Parent;
        ui64 Current = Max<ui64>();
    };

    class TKeysIterator : public TTemporaryComputationValue<TKeysIterator> {
    public:
        TKeysIterator(const TDirectArrayHolderInplace& parent)
            : TTemporaryComputationValue(parent.GetMemInfo()), Size(parent.GetSize())
        {}
    private:
        bool Skip() final {
            return ++Current < Size;
        }

        bool Next(NUdf::TUnboxedValue& key) final {
            if (!Skip())
                return false;
            key = NUdf::TUnboxedValuePod(Current);
            return true;
        }

        const ui64 Size;
        ui64 Current = Max<ui64>();
    };

    bool HasListItems() const final {
        return true;
    }

    bool HasDictItems() const final {
        return true;
    }

    bool HasFastListLength() const final {
        return true;
    }

    ui64 GetListLength() const final {
        return Size;
    }

    ui64 GetDictLength() const final {
        return Size;
    }

    ui64 GetEstimatedListLength() const final {
        return Size;
    }

    NUdf::TUnboxedValue GetListIterator() const final {
        return NUdf::TUnboxedValuePod(new TIterator(this));
    }

    NUdf::TUnboxedValue GetDictIterator() const final {
        return NUdf::TUnboxedValuePod(new TIterator(this));
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const final {
        return NUdf::TUnboxedValuePod(new TIterator(this));
    }

    NUdf::TUnboxedValue GetKeysIterator() const final {
        return NUdf::TUnboxedValuePod(new TKeysIterator(*this));
    }

    NUdf::IBoxedValuePtr ReverseListImpl(const NUdf::IValueBuilder& builder) const final {
        if (1U >= Size)
            return const_cast<TDirectArrayHolderInplace*>(this);

        NUdf::TUnboxedValue* items = nullptr;
        auto result = builder.NewArray(Size, items);
        std::reverse_copy(GetPtr(), GetPtr() + Size, items);
        return result.Release().AsBoxed();
    }

    NUdf::IBoxedValuePtr SkipListImpl(const NUdf::IValueBuilder& builder, ui64 count) const final {
        if (!count)
            return const_cast<TDirectArrayHolderInplace*>(this);

        if (count >= Size)
            return builder.NewEmptyList().Release().AsBoxed();

        const auto newSize = Size - count;
        NUdf::TUnboxedValue* items = nullptr;
        auto result = builder.NewArray(newSize, items);
        std::copy_n(GetPtr() + count, newSize, items);
        return result.Release().AsBoxed();
    }

    NUdf::IBoxedValuePtr TakeListImpl(const NUdf::IValueBuilder& builder, ui64 count) const final {
        if (!count)
            return builder.NewEmptyList().Release().AsBoxed();

        if (count >= Size)
            return const_cast<TDirectArrayHolderInplace*>(this);

        const auto newSize = count;
        NUdf::TUnboxedValue* items = nullptr;
        auto result = builder.NewArray(newSize, items);
        std::copy_n(GetPtr(), newSize, items);
        return result.Release().AsBoxed();
    }

    NUdf::IBoxedValuePtr ToIndexDictImpl(const NUdf::IValueBuilder&) const final {
        return const_cast<TDirectArrayHolderInplace*>(this);
    }

    bool Contains(const NUdf::TUnboxedValuePod& key) const final {
        return key.Get<ui64>() < Size;
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const final {
        const auto index = key.Get<ui64>();
        return index < Size ? GetPtr()[index].MakeOptional() : NUdf::TUnboxedValuePod();
    }

    NUdf::TUnboxedValue GetElement(ui32 index) const final {
        Y_DEBUG_ABORT_UNLESS(index < Size);
        return GetPtr()[index];
    }

    const NUdf::TUnboxedValue* GetElements() const final {
        return GetPtr();
    }

    bool IsSortedDict() const override {
        return true;
    }

    const ui64 Size;
};

//////////////////////////////////////////////////////////////////////////////
// THolderFactory
//////////////////////////////////////////////////////////////////////////////
class THolderFactory: private TNonCopyable
{
public:
    THolderFactory(
        TAllocState& allocState,
        TMemoryUsageInfo& memInfo,
        const IFunctionRegistry* functionRegistry = nullptr);

    ~THolderFactory();

    template <typename T, typename... TArgs>
    NUdf::TUnboxedValuePod Create(TArgs&&... args) const {
        return NUdf::TUnboxedValuePod(AllocateOn<T>(CurrentAllocState, &MemInfo, std::forward<TArgs>(args)...));
    }

    NUdf::TUnboxedValuePod CreateTypeHolder(TType* type) const;

    NUdf::TUnboxedValuePod CreateDirectListHolder(TDefaultListRepresentation&& items) const;

    NUdf::TUnboxedValuePod CreateDirectArrayHolder(ui64 size, NUdf::TUnboxedValue*& itemsPtr) const;

    NUdf::TUnboxedValuePod CreateArrowBlock(arrow::Datum&& datum) const;

    NUdf::TUnboxedValuePod VectorAsArray(TUnboxedValueVector& values) const;

    NUdf::TUnboxedValuePod VectorAsVectorHolder(TUnboxedValueVector&& list) const;
    NUdf::TUnboxedValuePod NewVectorHolder() const;
    NUdf::TUnboxedValuePod NewTemporaryVectorHolder() const;

    const NUdf::IHash* GetHash(const TType& type, bool useIHash) const;
    const NUdf::IEquate* GetEquate(const TType& type, bool useIHash) const;
    const NUdf::ICompare* GetCompare(const TType& type, bool useIHash) const;

    template <class TForwardIterator>
    NUdf::TUnboxedValuePod RangeAsArray(TForwardIterator first, TForwardIterator last) const {
        auto count = std::distance(first, last);
        if (count == 0)
            return GetEmptyContainerLazy();

        NUdf::TUnboxedValue* itemsPtr = nullptr;
        auto tuple = CreateDirectArrayHolder(count, itemsPtr);
        while (first != last) {
            *itemsPtr++ = std::move(*first);
            ++first;
        }

        return tuple;
    }

    NUdf::TUnboxedValuePod CreateDirectSortedSetHolder(
            TSortedSetFiller filler,
            const TKeyTypes& types,
            bool isTuple,
            EDictSortMode mode,
            bool eagerFill,
            TType* encodedType,
            const NUdf::ICompare* compare,
            const NUdf::IEquate* equate) const;

    NUdf::TUnboxedValuePod CreateDirectSortedDictHolder(
            TSortedDictFiller filler,
            const TKeyTypes& types,
            bool isTuple,
            EDictSortMode mode,
            bool eagerFill,
            TType* encodedType,
            const NUdf::ICompare* compare,
            const NUdf::IEquate* equate) const;

    NUdf::TUnboxedValuePod CreateDirectHashedDictHolder(
            THashedDictFiller filler,
            const TKeyTypes& types,
            bool isTuple,
            bool eagerFill,
            TType* encodedType,
            const NUdf::IHash* hash,
            const NUdf::IEquate* equate) const;

    NUdf::TUnboxedValuePod CreateDirectHashedSetHolder(
        THashedSetFiller filler,
        const TKeyTypes& types,
        bool isTuple,
        bool eagerFill,
        TType* encodedType,
        const NUdf::IHash* hash,
        const NUdf::IEquate* equate) const;

    template <typename T, bool OptionalKey>
    NUdf::TUnboxedValuePod CreateDirectHashedSingleFixedSetHolder(TValuesDictHashSingleFixedSet<T>&& set, bool hasNull) const;

    template <typename T, bool OptionalKey>
    NUdf::TUnboxedValuePod CreateDirectHashedSingleFixedCompactSetHolder(TValuesDictHashSingleFixedCompactSet<T>&& set, bool hasNull) const;

    template <typename T, bool OptionalKey>
    NUdf::TUnboxedValuePod CreateDirectHashedSingleFixedMapHolder(TValuesDictHashSingleFixedMap<T>&& map, std::optional<NUdf::TUnboxedValue>&& nullPayload) const;

    NUdf::TUnboxedValuePod CreateDirectHashedCompactSetHolder(
        TValuesDictHashCompactSet&& set, TPagedArena&& pool, TType* keyType,
        TComputationContext* ctx) const;

    NUdf::TUnboxedValuePod CreateDirectHashedCompactMapHolder(
        TValuesDictHashCompactMap&& map, TPagedArena&& pool, TType* keyType, TType* payloadType,
        TComputationContext* ctx) const;

    NUdf::TUnboxedValuePod CreateDirectHashedCompactMultiMapHolder(
        TValuesDictHashCompactMultiMap&& map, TPagedArena&& pool, TType* keyType, TType* payloadType,
        TComputationContext* ctx) const;

    template <typename T, bool OptionalKey>
    NUdf::TUnboxedValuePod CreateDirectHashedSingleFixedCompactMapHolder(
        TValuesDictHashSingleFixedCompactMap<T>&& map, std::optional<ui64>&& nullPayload, TPagedArena&& pool, TType* payloadType,
        TComputationContext* ctx) const;

    template <typename T, bool OptionalKey>
    NUdf::TUnboxedValuePod CreateDirectHashedSingleFixedCompactMultiMapHolder(
        TValuesDictHashSingleFixedCompactMultiMap<T>&& map, std::vector<ui64>&& nullPayloads, TPagedArena&& pool, TType* payloadType,
        TComputationContext* ctx) const;

    NUdf::IDictValueBuilder::TPtr NewDict(
            const NUdf::TType* dictType,
            ui32 flags) const;

    NUdf::TUnboxedValuePod Cloned(const NUdf::TUnboxedValuePod& it) const;
    NUdf::TUnboxedValuePod Reversed(const NUdf::TUnboxedValuePod& it) const;

    NUdf::TUnboxedValuePod CreateLimitedList(
            NUdf::IBoxedValuePtr&& parent,
            TMaybe<ui64> skip, TMaybe<ui64> take,
            TMaybe<ui64> knownLength) const;

    NUdf::TUnboxedValuePod ReverseList(const NUdf::IValueBuilder* builder, const NUdf::TUnboxedValuePod list) const;
    NUdf::TUnboxedValuePod SkipList(const NUdf::IValueBuilder* builder, const NUdf::TUnboxedValuePod list, ui64 count) const;
    NUdf::TUnboxedValuePod TakeList(const NUdf::IValueBuilder* builder, const NUdf::TUnboxedValuePod list, ui64 count) const;
    NUdf::TUnboxedValuePod ToIndexDict(const NUdf::IValueBuilder* builder, const NUdf::TUnboxedValuePod list) const;

    template<bool IsStream>
    NUdf::TUnboxedValuePod Collect(NUdf::TUnboxedValuePod list) const;

    NUdf::TUnboxedValuePod LazyList(NUdf::TUnboxedValuePod list) const;

    NUdf::TUnboxedValuePod Append(NUdf::TUnboxedValuePod list, NUdf::TUnboxedValuePod last) const;
    NUdf::TUnboxedValuePod Prepend(NUdf::TUnboxedValuePod first, NUdf::TUnboxedValuePod list) const;

    NUdf::TUnboxedValuePod CreateVariantHolder(NUdf::TUnboxedValuePod item, ui32 index) const;
    NUdf::TUnboxedValuePod CreateBoxedVariantHolder(NUdf::TUnboxedValuePod item, ui32 index) const;
    NUdf::TUnboxedValuePod CreateIteratorOverList(NUdf::TUnboxedValuePod list) const;
    NUdf::TUnboxedValuePod CreateForwardList(NUdf::TUnboxedValuePod stream) const;

    NUdf::TUnboxedValuePod CloneArray(const NUdf::TUnboxedValuePod list, NUdf::TUnboxedValue*& itemsPtr) const;

    TMemoryUsageInfo& GetMemInfo() const {
        return MemInfo;
    }

    NUdf::TUnboxedValuePod GetEmptyContainerLazy() const;

    void CleanupModulesOnTerminate() const {
        if (FunctionRegistry) {
            FunctionRegistry->CleanupModulesOnTerminate();
        }
    }

    TAlignedPagePool& GetPagePool() const {
        return *CurrentAllocState;
    }

    ui64 GetMemoryUsed() const {
        return CurrentAllocState->GetUsed();
    }

    const IFunctionRegistry* GetFunctionRegistry() const {
        return FunctionRegistry;
    }

    template<bool FromStreams>
    NUdf::TUnboxedValuePod ExtendList(NUdf::TUnboxedValue* data, ui64 size) const;
    NUdf::TUnboxedValuePod ExtendStream(NUdf::TUnboxedValue* data, ui64 size) const;

private:
    TAllocState* const CurrentAllocState;
    TMemoryUsageInfo& MemInfo;
    const IFunctionRegistry* const FunctionRegistry;
    mutable TMaybe<NUdf::TUnboxedValue> EmptyContainer;

    mutable TTypeOperationsRegistry<NUdf::IHash> HashRegistry;
    mutable TTypeOperationsRegistry<NUdf::IEquate> EquateRegistry;
    mutable TTypeOperationsRegistry<NUdf::ICompare> CompareRegistry;
};

constexpr const ui32 STEP_FOR_RSS_CHECK = 100U;

// Returns true if current usage delta exceeds the memory limit
// The function automatically adjusts memory limit taking into account RSS delta between calls
template<bool TrackRss>
inline bool TComputationContext::CheckAdjustedMemLimit(ui64 memLimit, ui64 initMemUsage) {
    if (!memLimit) {
        return false;
    }

    if (TrackRss && (RssCounter++ % STEP_FOR_RSS_CHECK == 0)) {
        UpdateUsageAdjustor(memLimit);
    }
    const auto currentMemUsage = HolderFactory.GetMemoryUsed();
    return currentMemUsage * UsageAdjustor >= initMemUsage + memLimit;
}

void GetDictionaryKeyTypes(const TType* keyType, TKeyTypes& types, bool& isTuple, bool& encoded, bool& useIHash, bool expandTuple = true);

template<bool SupportEqual, bool SupportHash, bool SupportLess>
class TKeyTypeContanerHelper {
public:
    TKeyTypeContanerHelper() = default;
    TKeyTypeContanerHelper(const TType* type) {
        bool encoded;
        bool useIHash;
        GetDictionaryKeyTypes(type, KeyTypes, IsTuple, encoded, useIHash);
        if (useIHash || encoded) {
            if constexpr(SupportEqual) {
                Equate = MakeEquateImpl(type);
            }
            if constexpr(SupportHash) {
                Hash = MakeHashImpl(type);
            }
            if constexpr(SupportLess) {
                Compare = MakeCompareImpl(type);
            }
        }
    }
public: //unavailable getters may be eliminated at compile time, but it'd make code much less readable
    TValueEqual GetValueEqual() const{
        Y_ABORT_UNLESS(SupportEqual);
        return TValueEqual(KeyTypes, IsTuple, Equate.Get());
    }
    TValueHasher GetValueHash() const{
        Y_ABORT_UNLESS(SupportHash);
        return TValueHasher(KeyTypes, IsTuple, Hash.Get());
    }
    TValueLess GetValueLess() const{
        Y_ABORT_UNLESS(SupportLess);
        return TValueLess(KeyTypes, IsTuple , Compare.Get());
    }
private:
    TKeyTypes KeyTypes;
    bool IsTuple = false;
    
    //unsused pointers may be eliminated at compile time, but it'd make code much less readable
    NUdf::IEquate::TPtr Equate;
    NUdf::IHash::TPtr Hash;
    NUdf::ICompare::TPtr Compare;
};

class TPlainContainerCache {
public:
    TPlainContainerCache();

    TPlainContainerCache(const TPlainContainerCache&) = delete;
    TPlainContainerCache& operator=(const TPlainContainerCache&) = delete;

    void Clear();

    NUdf::TUnboxedValuePod NewArray(const THolderFactory& factory, ui64 size, NUdf::TUnboxedValue*& items);

private:
    std::array<NUdf::TUnboxedValue, 2> Cached;
    std::array<NUdf::TUnboxedValue*, 2> CachedItems;
    ui8 CacheIndex = 0U;
};

template<class TObject>
class TMutableObjectOverBoxedValue {
public:
    TMutableObjectOverBoxedValue(TComputationMutables& mutables)
        : ObjectIndex(mutables.CurValueIndex++)
    {}

    template <typename... Args>
    TObject& RefMutableObject(TComputationContext& ctx, Args&&... args) const {
        auto& unboxed = ctx.MutableValues[ObjectIndex];
        if (!unboxed.HasValue()) {
            unboxed = ctx.HolderFactory.Create<TObject>(std::forward<Args>(args)...);
        }
        auto boxed = unboxed.AsBoxed();
        return *static_cast<TObject*>(boxed.Get());
    }
private:
    const ui32 ObjectIndex;
};

} // namespace NMiniKQL
} // namespace NKikimr
