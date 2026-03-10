#include "mkql_block_agg.h"
#include "mkql_block_agg_factory.h"
#include "mkql_rh_hash.h"

#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_builder.h>

#include <yql/essentials/minikql/arrow/arrow_defs.h>
#include <yql/essentials/minikql/arrow/arrow_util.h>
#include <yql/essentials/minikql/arrow/mkql_bit_utils.h>

#include <yql/essentials/utils/prefetch.h>

#include <arrow/scalar.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/chunked_array.h>

// #define USE_STD_UNORDERED

namespace NKikimr {
namespace NMiniKQL {

namespace {

constexpr bool InlineAggState = false;

#ifdef USE_STD_UNORDERED
template <typename TKey, typename TEqual = std::equal_to<TKey>, typename THash = std::hash<TKey>, typename TAllocator = std::allocator<char>, typename TSettings = void>
class TDynamicHashMapImpl {
public:
    using TMapType = std::unordered_map<TKey, std::vector<char>, THash, TEqual>;
    using const_iterator = typename TMapType::const_iterator;
    using iterator = typename TMapType::iterator;

    TDynamicHashMapImpl(size_t stateSize, const THash& hasher, const TEqual& equal)
        : StateSize_(stateSize)
        , Map_(0, hasher, equal)
    {
    }

    ui64 GetSize() const {
        return Map_.size();
    }

    const_iterator Begin() const {
        return Map_.begin();
    }

    const_iterator End() const {
        return Map_.end();
    }

    bool IsValid(const_iterator iter) const {
        return true;
    }

    void Advance(const_iterator& iter) const {
        ++iter;
    }

    iterator Insert(const TKey& key, bool& isNew) {
        auto res = Map_.emplace(key, std::vector<char>());
        isNew = res.second;
        if (isNew) {
            res.first->second.resize(StateSize_);
        }

        return res.first;
    }

    template <typename TSink>
    void BatchInsert(std::span<TRobinHoodBatchRequestItem<TKey>> batchRequest, TSink&& sink) {
        for (size_t index = 0; index < batchRequest.size(); ++index) {
            bool isNew;
            auto iter = Insert(batchRequest[index].GetKey(), isNew);
            sink(index, iter, isNew);
        }
    }

    const TKey& GetKey(const_iterator it) const {
        return it->first;
    }

    char* GetMutablePayloadPtr(iterator it) const {
        return it->second.data();
    }

    const char* GetPayloadPtr(const_iterator it) const {
        return it->second.data();
    }

    void CheckGrow() {
    }

private:
    const size_t StateSize_;
    TMapType Map_;
};

template <typename TKey, typename TPayload, typename TEqual = std::equal_to<TKey>, typename THash = std::hash<TKey>, typename TAllocator = std::allocator<char>, typename TSettings = void>
class TFixedHashMapImpl {
public:
    using TMapType = std::unordered_map<TKey, TPayload, THash, TEqual>;
    using const_iterator = typename TMapType::const_iterator;
    using iterator = typename TMapType::iterator;

    TFixedHashMapImpl(const THash& hasher, const TEqual& equal)
        : Map_(0, hasher, equal)
    {
    }

    ui64 GetSize() const {
        return Map_.size();
    }

    const_iterator Begin() const {
        return Map_.begin();
    }

    const_iterator End() const {
        return Map_.end();
    }

    bool IsValid(const_iterator iter) const {
        return true;
    }

    void Advance(const_iterator& iter) const {
        ++iter;
    }

    iterator Insert(const TKey& key, bool& isNew) {
        auto res = Map_.emplace(key, TPayload());
        isNew = res.second;
        return res.first;
    }

    template <typename TSink>
    void BatchInsert(std::span<TRobinHoodBatchRequestItem<TKey>> batchRequest, TSink&& sink) {
        for (size_t index = 0; index < batchRequest.size(); ++index) {
            bool isNew;
            auto iter = Insert(batchRequest[index].GetKey(), isNew);
            sink(index, iter, isNew);
        }
    }

    const TKey& GetKey(const_iterator it) const {
        return it->first;
    }

    char* GetMutablePayloadPtr(iterator it) const {
        return (char*)&it->second;
    }

    const char* GetPayloadPtr(const_iterator it) const {
        return (const char*)&it->second;
    }

    void CheckGrow() {
    }

private:
    TMapType Map_;
};

template <typename TKey, typename TEqual = std::equal_to<TKey>, typename THash = std::hash<TKey>, typename TAllocator = std::allocator<char>, typename TSettings = void>
class THashSetImpl {
public:
    using TSetType = std::unordered_set<TKey, THash, TEqual>;
    using const_iterator = typename TSetType::const_iterator;
    using iterator = typename TSetType::iterator;

    THashSetImpl(const THash& hasher, const TEqual& equal)
        : Set_(0, hasher, equal)
    {
    }

    ui64 GetSize() const {
        return Set_.size();
    }

    const_iterator Begin() const {
        return Set_.begin();
    }

    const_iterator End() const {
        return Set_.end();
    }

    bool IsValid(const_iterator iter) const {
        return true;
    }

    void Advance(const_iterator& iter) const {
        ++iter;
    }

    iterator Insert(const TKey& key, bool& isNew) {
        auto res = Set_.emplace(key);
        isNew = res.second;
        return res.first;
    }

    template <typename TSink>
    void BatchInsert(std::span<TRobinHoodBatchRequestItem<TKey>> batchRequest, TSink&& sink) {
        for (size_t index = 0; index < batchRequest.size(); ++index) {
            bool isNew;
            auto iter = Insert(batchRequest[index].GetKey(), isNew);
            sink(index, iter, isNew);
        }
    }

    void CheckGrow() {
    }

    const TKey& GetKey(const_iterator it) const {
        return *it;
    }

    char* GetMutablePayloadPtr(iterator it) const {
        Y_UNUSED(it);
        return nullptr;
    }

    const char* GetPayloadPtr(const_iterator it) const {
        Y_UNUSED(it);
        return nullptr;
    }

private:
    TSetType Set_;
};

#else
    #define TDynamicHashMapImpl TRobinHoodHashMap
    #define TFixedHashMapImpl TRobinHoodHashFixedMap
    #define THashSetImpl TRobinHoodHashSet
#endif

using TState8 = ui64;
static_assert(sizeof(TState8) == 8);

struct TState16 {
    ui64 FirstHalf;
    ui64 SecondHalf;
};
static_assert(sizeof(TState16) == 16);

using TStateArena = void*;
static_assert(sizeof(TStateArena) == sizeof(void*));

struct TExternalFixedSizeKey {
    const char* Data;
};

struct TKey16 {
    ui64 Lo;
    ui64 Hi;
};

class TSSOKey {
public:
    static constexpr size_t SSO_Length = 15;
    static_assert(SSO_Length < 128); // should fit into 7 bits

private:
    struct TExternal {
        ui64 Length_;
        const char* Ptr_;
    };

    struct TInplace {
        ui8 SmallLength_;
        char Buffer_[SSO_Length];
    };

public:
    TSSOKey() = default;

    TSSOKey(const TSSOKey& other) = default;
    TSSOKey& operator=(const TSSOKey& other) = default;

    static bool CanBeInplace(TStringBuf data) {
        return data.Size() + 1 <= sizeof(TSSOKey);
    }

    static TSSOKey Inplace(TStringBuf data) {
        Y_ASSERT(CanBeInplace(data));
        TSSOKey ret(1 | (data.Size() << 1), 0);
        memcpy(ret.U.I.Buffer_, data.Data(), data.Size());
        return ret;
    }

    static TSSOKey External(TStringBuf data) {
        return TSSOKey(data.Size() << 1, data.Data());
    }

    bool IsInplace() const {
        return U.I.SmallLength_ & 1;
    }

    TStringBuf AsView() const {
        if (IsInplace()) {
            // inplace
            return TStringBuf(U.I.Buffer_, U.I.SmallLength_ >> 1);
        } else {
            // external
            return TStringBuf(U.E.Ptr_, U.E.Length_ >> 1);
        }
    }

    void UpdateExternalPointer(const char* ptr) {
        Y_ASSERT(!IsInplace());
        U.E.Ptr_ = ptr;
    }

private:
    TSSOKey(ui64 length, const char* ptr) {
        U.E.Length_ = length;
        U.E.Ptr_ = ptr;
    }

private:
    union {
        TExternal E;
        TInplace I;
        char A[SSO_Length + 1];
    } U;
};

static_assert(sizeof(TSSOKey) == TSSOKey::SSO_Length + 1);

} // namespace
} // namespace NMiniKQL
} // namespace NKikimr

namespace std {
template <>
struct hash<NKikimr::NMiniKQL::TKey16> {
    using argument_type = NKikimr::NMiniKQL::TKey16;
    using result_type = size_t;
    inline result_type operator()(argument_type const& s) const noexcept {
        auto hasher = std::hash<ui64>();
        return hasher(s.Hi) * 31 + hasher(s.Lo);
    }
};

template <>
struct equal_to<NKikimr::NMiniKQL::TKey16> {
    using argument_type = NKikimr::NMiniKQL::TKey16;
    bool operator()(argument_type x, argument_type y) const {
        return x.Hi == y.Hi && x.Lo == y.Lo;
    }
};

template <>
struct hash<NKikimr::NMiniKQL::TSSOKey> {
    using argument_type = NKikimr::NMiniKQL::TSSOKey;
    using result_type = size_t;
    inline result_type operator()(argument_type const& s) const noexcept {
        return std::hash<std::string_view>()(s.AsView());
    }
};

template <>
struct equal_to<NKikimr::NMiniKQL::TSSOKey> {
    using argument_type = NKikimr::NMiniKQL::TSSOKey;
    bool operator()(argument_type x, argument_type y) const {
        return x.AsView() == y.AsView();
    }
    bool operator()(argument_type x, TStringBuf y) const {
        return x.AsView() == y;
    }
    using is_transparent = void;
};

template <>
struct hash<NKikimr::NMiniKQL::TExternalFixedSizeKey> {
    using argument_type = NKikimr::NMiniKQL::TExternalFixedSizeKey;
    using result_type = size_t;
    hash(ui32 length)
        : Length(length)
    {
    }

    inline result_type operator()(argument_type const& s) const noexcept {
        return std::hash<std::string_view>()(std::string_view(s.Data, Length));
    }

    const ui32 Length;
};

template <>
struct equal_to<NKikimr::NMiniKQL::TExternalFixedSizeKey> {
    using argument_type = NKikimr::NMiniKQL::TExternalFixedSizeKey;
    equal_to(ui32 length)
        : Length(length)
    {
    }

    bool operator()(argument_type x, argument_type y) const {
        return memcmp(x.Data, y.Data, Length) == 0;
    }
    bool operator()(argument_type x, TStringBuf y) const {
        Y_ASSERT(y.Size() <= Length);
        return memcmp(x.Data, y.Data(), Length) == 0;
    }
    using is_transparent = void;

    const ui32 Length;
};
} // namespace std

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <typename T>
struct TAggParams {
    std::unique_ptr<IPreparedBlockAggregator<T>> Prepared_;
    ui32 Column_ = 0;
    TType* StateType_ = nullptr;
    TType* ReturnType_ = nullptr;
    ui32 Hint_ = 0;
};

struct TKeyParams {
    ui32 Index;
    TType* Type;
};

size_t GetBitmapPopCount(const std::shared_ptr<arrow::ArrayData>& arr) {
    size_t len = (size_t)arr->length;
    MKQL_ENSURE(arr->GetNullCount() == 0, "Bitmap block should not have nulls");
    const ui8* src = arr->GetValues<ui8>(1);
    return GetSparseBitmapPopCount(src, len);
}

size_t CalcMaxBlockLenForOutput(TType* out) {
    const auto wideComponents = GetWideComponents(out);
    MKQL_ENSURE(wideComponents.size() > 0, "Expecting at least one output column");

    size_t maxBlockItemSize = 0;
    for (ui32 i = 0; i < wideComponents.size() - 1; ++i) {
        auto type = AS_TYPE(TBlockType, wideComponents[i]);
        MKQL_ENSURE(type->GetShape() != TBlockType::EShape::Scalar, "Expecting block type");
        maxBlockItemSize = std::max(maxBlockItemSize, CalcMaxBlockItemSize(type->GetItemType()));
    }

    return CalcBlockLen(maxBlockItemSize);
}

struct TBlockCombineAllState: public TComputationValue<TBlockCombineAllState> {
    NUdf::TUnboxedValue* Pointer_ = nullptr;
    bool IsFinished_ = false;
    bool HasValues_ = false;
    TUnboxedValueVector Values_;
    std::vector<std::unique_ptr<IBlockAggregatorCombineAll>> Aggs_;
    std::vector<char> AggStates_;
    const std::optional<ui32> FilterColumn_;
    const size_t Width_;

    TBlockCombineAllState(TMemoryUsageInfo* memInfo, size_t width, std::optional<ui32> filterColumn, const std::vector<TAggParams<IBlockAggregatorCombineAll>>& params, TComputationContext& ctx)
        : TComputationValue(memInfo)
        , Values_(std::max(width, params.size()))
        , FilterColumn_(filterColumn)
        , Width_(width)
    {
        Pointer_ = Values_.data();

        ui32 totalStateSize = 0;
        for (const auto& p : params) {
            Aggs_.emplace_back(p.Prepared_->Make(ctx));
            MKQL_ENSURE(Aggs_.back()->StateSize == p.Prepared_->StateSize, "State size mismatch");
            totalStateSize += Aggs_.back()->StateSize;
        }

        AggStates_.resize(totalStateSize);
        char* ptr = AggStates_.data();
        for (const auto& agg : Aggs_) {
            agg->InitState(ptr);
            ptr += agg->StateSize;
        }
    }

    void ProcessInput() {
        const ui64 batchLength = TArrowBlock::From(Values_[Width_ - 1U]).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
        if (!batchLength) {
            return;
        }

        std::optional<ui64> filtered;
        if (FilterColumn_) {
            const auto filterDatum = TArrowBlock::From(Values_[*FilterColumn_]).GetDatum();
            if (filterDatum.is_scalar()) {
                if (!filterDatum.scalar_as<arrow::UInt8Scalar>().value) {
                    return;
                }
            } else {
                const ui64 popCount = GetBitmapPopCount(filterDatum.array());
                if (popCount == 0) {
                    return;
                }

                if (popCount < batchLength) {
                    filtered = popCount;
                }
            }
        }

        HasValues_ = true;
        char* ptr = AggStates_.data();
        for (size_t i = 0; i < Aggs_.size(); ++i) {
            Aggs_[i]->AddMany(ptr, Values_.data(), batchLength, filtered);
            ptr += Aggs_[i]->StateSize;
        }
    }

    bool MakeOutput() {
        IsFinished_ = true;
        if (!HasValues_) {
            return false;
        }

        char* ptr = AggStates_.data();
        for (size_t i = 0; i < Aggs_.size(); ++i) {
            Values_[i] = Aggs_[i]->FinishOne(ptr);
            Aggs_[i]->DestroyState(ptr);
            ptr += Aggs_[i]->StateSize;
        }
        return true;
    }

    NUdf::TUnboxedValuePod Get(size_t index) const {
        return Values_[index];
    }
};

class TBlockCombineAllWrapper: public TMutableComputationNode<TBlockCombineAllWrapper> {
    using TBaseComputation = TMutableComputationNode<TBlockCombineAllWrapper>;

    using TState = TBlockCombineAllState;

public:
    TBlockCombineAllWrapper(TComputationMutables& mutables,
                            IComputationNode* stream,
                            std::optional<ui32> filterColumn,
                            size_t width,
                            std::vector<TAggParams<IBlockAggregatorCombineAll>>&& aggsParams)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Stream_(stream)
        , FilterColumn_(filterColumn)
        , Width_(width)
        , AggsParams_(std::move(aggsParams))
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(width))
    {
        MKQL_ENSURE(Width_ > 0, "Missing block length column");
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto state = ctx.HolderFactory.Create<TState>(Width_, FilterColumn_, AggsParams_, ctx);
        return ctx.HolderFactory.Create<TStreamValue>(std::move(state), std::move(Stream_->GetValue(ctx)));
    }

private:
    class TStreamValue: public TComputationValue<TStreamValue> {
        using TBase = TComputationValue<TStreamValue>;

    public:
        TStreamValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& state, NUdf::TUnboxedValue&& stream)
            : TBase(memInfo)
            , State_(state)
            , Stream_(stream)
        {
        }

    private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) {
            TState& state = *static_cast<TState*>(State_.AsBoxed().Get());
            auto* inputFields = state.Values_.data();
            const size_t inputWidth = state.Width_;

            if (state.IsFinished_) {
                return NUdf::EFetchStatus::Finish;
            }

            while (true) {
                switch (Stream_.WideFetch(inputFields, inputWidth)) {
                    case NUdf::EFetchStatus::Yield:
                        return NUdf::EFetchStatus::Yield;
                    case NUdf::EFetchStatus::Ok:
                        state.ProcessInput();
                        continue;
                    case NUdf::EFetchStatus::Finish:
                        break;
                }
                if (state.MakeOutput()) {
                    for (size_t i = 0; i < width; ++i) {
                        output[i] = state.Get(i);
                    }
                    return NUdf::EFetchStatus::Ok;
                }
                return NUdf::EFetchStatus::Finish;
            }
        }

    private:
        NUdf::TUnboxedValue State_;
        NUdf::TUnboxedValue Stream_;
    };

private:
    void RegisterDependencies() const final {
        DependsOn(Stream_);
    }

private:
    IComputationNode* const Stream_;
    const std::optional<ui32> FilterColumn_;
    const size_t Width_;
    const std::vector<TAggParams<IBlockAggregatorCombineAll>> AggsParams_;
    const size_t WideFieldsIndex_;
};

template <typename T>
T MakeKey(TStringBuf s, ui32 keyLength) {
    Y_UNUSED(keyLength);
    Y_ASSERT(s.Size() <= sizeof(T));
    return ReadUnaligned<T>(s.Data());
}

template <>
TSSOKey MakeKey(TStringBuf s, ui32 keyLength) {
    Y_UNUSED(keyLength);
    if (TSSOKey::CanBeInplace(s)) {
        return TSSOKey::Inplace(s);
    } else {
        return TSSOKey::External(s);
    }
}

template <>
TExternalFixedSizeKey MakeKey(TStringBuf s, ui32 keyLength) {
    Y_ASSERT(s.Size() == keyLength);
    return {s.Data()};
}

template <typename T>
void MoveKeyToArena(void* keyPtr, TPagedArena& arena, ui32 keyLength) = delete;

template <>
void MoveKeyToArena<TSSOKey>(void* keyPtr, TPagedArena& arena, ui32 keyLength) {
    Y_UNUSED(keyLength);
    TSSOKey key = ReadUnaligned<TSSOKey>(keyPtr);
    if (key.IsInplace()) {
        return;
    }

    auto view = key.AsView();
    auto arenaPtr = (char*)arena.Alloc(view.Size());
    memcpy(arenaPtr, view.Data(), view.Size());
    key.UpdateExternalPointer(arenaPtr);
    WriteUnaligned<TSSOKey>(keyPtr, key);
}

template <>
void MoveKeyToArena<TExternalFixedSizeKey>(void* keyPtr, TPagedArena& arena, ui32 keyLength) {
    TExternalFixedSizeKey key = ReadUnaligned<TExternalFixedSizeKey>(keyPtr);
    auto ptr = (char*)arena.Alloc(keyLength);
    memcpy(ptr, key.Data, keyLength);
    key.Data = ptr;
    WriteUnaligned<TExternalFixedSizeKey>(keyPtr, key);
}

template <typename T>
TStringBuf GetKeyView(const T& key, ui32 keyLength) {
    Y_UNUSED(keyLength);
    return TStringBuf((const char*)&key, sizeof(T));
}

template <>
TStringBuf GetKeyView(const TSSOKey& key, ui32 keyLength) {
    Y_UNUSED(keyLength);
    return key.AsView();
}

template <>
TStringBuf GetKeyView(const TExternalFixedSizeKey& key, ui32 keyLength) {
    return TStringBuf(key.Data, keyLength);
}

template <typename T>
std::equal_to<T> MakeEqual(ui32 keyLength) {
    Y_UNUSED(keyLength);
    return std::equal_to<T>();
}

template <>
std::equal_to<TExternalFixedSizeKey> MakeEqual(ui32 keyLength) {
    return std::equal_to<TExternalFixedSizeKey>(keyLength);
}

template <typename T>
std::hash<T> MakeHash(ui32 keyLength) {
    Y_UNUSED(keyLength);
    return std::hash<T>();
}

template <>
std::hash<TExternalFixedSizeKey> MakeHash(ui32 keyLength) {
    return std::hash<TExternalFixedSizeKey>(keyLength);
}

template <typename TKey, typename TAggregator, typename TFixedAggState, bool UseSet, bool UseFilter, bool Finalize, bool Many, typename TDerived>
struct THashedWrapperBaseState: public TBlockState {
private:
    static constexpr bool UseArena = !InlineAggState && std::is_same<TFixedAggState, TStateArena>::value;

public:
    bool WritingOutput_ = false;
    bool IsFinished_ = false;

    const std::optional<ui32> FilterColumn_;
    const std::vector<TKeyParams> Keys_;
    const std::vector<TAggParams<TAggregator>>& AggsParams_;
    const ui32 KeyLength_;
    const ui32 StreamIndex_;
    const std::vector<std::vector<ui32>> Streams_;
    const size_t MaxBlockLen_;
    const size_t Width_;
    const size_t OutputWidth_;

    template <typename TKeyType>
    struct THashSettings {
        static constexpr bool CacheHash = std::is_same_v<TKeyType, TSSOKey>;
    };
    using TDynMapImpl = TDynamicHashMapImpl<TKey, std::equal_to<TKey>, std::hash<TKey>, TMKQLAllocator<char>, THashSettings<TKey>>;
    using TSetImpl = THashSetImpl<TKey, std::equal_to<TKey>, std::hash<TKey>, TMKQLAllocator<char>, THashSettings<TKey>>;
    using TFixedMapImpl = TFixedHashMapImpl<TKey, TFixedAggState, std::equal_to<TKey>, std::hash<TKey>, TMKQLAllocator<char>, THashSettings<TKey>>;

    ui64 BatchNum_ = 0;
    TUnboxedValueVector Values_;
    std::vector<std::unique_ptr<TAggregator>> Aggs_;
    std::vector<ui32> AggStateOffsets_;
    TUnboxedValueVector UnwrappedValues_;
    std::vector<std::unique_ptr<IBlockReader>> Readers_;
    std::vector<std::unique_ptr<IArrayBuilder>> Builders_;
    std::vector<std::unique_ptr<IAggColumnBuilder>> AggBuilders_;
    bool HasValues_ = false;
    ui32 TotalStateSize_ = 0;
    size_t OutputBlockSize_ = 0;
    std::unique_ptr<TDynMapImpl> HashMap_;
    typename TDynMapImpl::const_iterator HashMapIt_;
    std::unique_ptr<TSetImpl> HashSet_;
    typename TSetImpl::const_iterator HashSetIt_;
    std::unique_ptr<TFixedMapImpl> HashFixedMap_;
    typename TFixedMapImpl::const_iterator HashFixedMapIt_;
    TPagedArena Arena_;

    THashedWrapperBaseState(TMemoryUsageInfo* memInfo, ui32 keyLength, ui32 streamIndex, size_t width, size_t outputWidth, std::optional<ui32> filterColumn, const std::vector<TAggParams<TAggregator>>& params,
                            const std::vector<std::vector<ui32>>& streams, const std::vector<TKeyParams>& keys, size_t maxBlockLen, TComputationContext& ctx)
        : TBlockState(memInfo, outputWidth)
        , FilterColumn_(filterColumn)
        , Keys_(keys)
        , AggsParams_(params)
        , KeyLength_(keyLength)
        , StreamIndex_(streamIndex)
        , Streams_(streams)
        , MaxBlockLen_(maxBlockLen)
        , Width_(width)
        , OutputWidth_(outputWidth)
        , Values_(width)
        , UnwrappedValues_(width)
        , Readers_(keys.size())
        , Builders_(keys.size())
        , Arena_(TlsAllocState)
    {
        Pointer = Values_.data();
        for (size_t i = 0; i < Keys_.size(); ++i) {
            auto itemType = AS_TYPE(TBlockType, Keys_[i].Type)->GetItemType();
            Readers_[i] = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
            Builders_[i] = NYql::NUdf::MakeArrayBuilder(TTypeInfoHelper(), itemType, ctx.ArrowMemoryPool, MaxBlockLen_, &ctx.Builder->GetPgBuilder());
        }

        if constexpr (Many) {
            TotalStateSize_ += Streams_.size();
        }

        for (const auto& p : AggsParams_) {
            Aggs_.emplace_back(p.Prepared_->Make(ctx));
            MKQL_ENSURE(Aggs_.back()->StateSize == p.Prepared_->StateSize, "State size mismatch");
            AggStateOffsets_.emplace_back(TotalStateSize_);
            TotalStateSize_ += Aggs_.back()->StateSize;
        }

        auto equal = MakeEqual<TKey>(KeyLength_);
        auto hasher = MakeHash<TKey>(KeyLength_);
        if constexpr (UseSet) {
            MKQL_ENSURE(params.empty(), "Only keys are supported");
            HashSet_ = std::make_unique<THashSetImpl<TKey, std::equal_to<TKey>, std::hash<TKey>, TMKQLAllocator<char>, THashSettings<TKey>>>(hasher, equal);
        } else {
            if (!InlineAggState) {
                HashFixedMap_ = std::make_unique<TFixedHashMapImpl<TKey, TFixedAggState, std::equal_to<TKey>, std::hash<TKey>, TMKQLAllocator<char>, THashSettings<TKey>>>(hasher, equal);
            } else {
                HashMap_ = std::make_unique<TDynamicHashMapImpl<TKey, std::equal_to<TKey>, std::hash<TKey>, TMKQLAllocator<char>, THashSettings<TKey>>>(TotalStateSize_, hasher, equal);
            }
        }
    }

    void ProcessInput(const THolderFactory& holderFactory) {
        ++BatchNum_;
        const auto batchLength = TArrowBlock::From(Values_.back()).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
        if (!batchLength) {
            return;
        }

        const ui8* filterBitmap = nullptr;
        if constexpr (UseFilter) {
            auto filterDatum = TArrowBlock::From(Values_[*FilterColumn_]).GetDatum();
            if (filterDatum.is_scalar()) {
                if (!filterDatum.template scalar_as<arrow::UInt8Scalar>().value) {
                    return;
                }
            } else {
                const auto& arr = filterDatum.array();
                filterBitmap = arr->template GetValues<ui8>(1);
                ui64 popCount = GetBitmapPopCount(arr);
                if (popCount == 0) {
                    return;
                }
            }
        }

        const ui32* streamIndexData = nullptr;
        TMaybe<ui32> streamIndexScalar;
        if constexpr (Many) {
            auto streamIndexDatum = TArrowBlock::From(Values_[StreamIndex_]).GetDatum();
            if (streamIndexDatum.is_scalar()) {
                streamIndexScalar = streamIndexDatum.template scalar_as<arrow::UInt32Scalar>().value;
            } else {
                MKQL_ENSURE(streamIndexDatum.is_array(), "Expected array");
                streamIndexData = streamIndexDatum.array()->template GetValues<ui32>(1);
            }
            UnwrappedValues_ = Values_;
            for (const auto& p : AggsParams_) {
                const auto& columnDatum = TArrowBlock::From(UnwrappedValues_[p.Column_]).GetDatum();
                MKQL_ENSURE(columnDatum.is_array(), "Expected array");
                UnwrappedValues_[p.Column_] = holderFactory.CreateArrowBlock(Unwrap(*columnDatum.array(), p.StateType_));
            }
        }

        HasValues_ = true;
        std::vector<arrow::Datum> keysDatum;
        keysDatum.reserve(Keys_.size());
        for (ui32 i = 0; i < Keys_.size(); ++i) {
            keysDatum.emplace_back(TArrowBlock::From(Values_[Keys_[i].Index]).GetDatum());
        }

        std::array<TOutputBuffer, PrefetchBatchSize> out;
        if constexpr (!std::is_same<TKey, TSSOKey>::value && !std::is_same<TKey, TExternalFixedSizeKey>::value) {
            for (ui32 i = 0; i < PrefetchBatchSize; ++i) {
                out[i].Resize(sizeof(TKey));
            }
        }

        std::array<TRobinHoodBatchRequestItem<TKey>, PrefetchBatchSize> insertBatch;
        std::array<ui64, PrefetchBatchSize> insertBatchRows;
        std::array<char*, PrefetchBatchSize> insertBatchPayloads;
        std::array<bool, PrefetchBatchSize> insertBatchIsNew;
        ui32 insertBatchLen = 0;

        const auto processInsertBatch = [&]() {
            for (ui32 i = 0; i < insertBatchLen; ++i) {
                auto& r = insertBatch[i];
                TStringBuf str = out[i].Finish();
                TKey key = MakeKey<TKey>(str, KeyLength_);
                r.ConstructKey(key);
            }

            if constexpr (UseSet) {
                HashSet_->BatchInsert({insertBatch.data(), insertBatchLen}, [&](size_t index, typename THashedWrapperBaseState::TSetImpl::iterator iter, bool isNew) {
                    Y_UNUSED(index);
                    if (isNew) {
                        if constexpr (std::is_same<TKey, TSSOKey>::value || std::is_same<TKey, TExternalFixedSizeKey>::value) {
                            MoveKeyToArena<TKey>(HashSet_->GetKeyPtr(iter), Arena_, KeyLength_);
                        }
                    }
                });
            } else {
                using THashTable = std::conditional_t<InlineAggState, typename THashedWrapperBaseState::TDynMapImpl, typename THashedWrapperBaseState::TFixedMapImpl>;
                THashTable* hash;
                if constexpr (!InlineAggState) {
                    hash = HashFixedMap_.get();
                } else {
                    hash = HashMap_.get();
                }

                hash->BatchInsert({insertBatch.data(), insertBatchLen}, [&](size_t index, typename THashTable::iterator iter, bool isNew) {
                    if (isNew) {
                        if constexpr (std::is_same<TKey, TSSOKey>::value || std::is_same<TKey, TExternalFixedSizeKey>::value) {
                            MoveKeyToArena<TKey>(hash->GetKeyPtr(iter), Arena_, KeyLength_);
                        }
                    }

                    if constexpr (UseArena) {
                        // prefetch payloads only
                        auto* payload = hash->GetMutablePayloadPtr(iter);
                        char* ptr;
                        if (isNew) {
                            ptr = (char*)Arena_.Alloc(TotalStateSize_);
                            WriteUnaligned<char*>(payload, ptr);
                        } else {
                            ptr = ReadUnaligned<char*>(payload);
                        }

                        insertBatchIsNew[index] = isNew;
                        insertBatchPayloads[index] = ptr;
                        NYql::PrefetchForWrite(ptr);
                    } else {
                        // process insert
                        auto* payload = hash->GetMutablePayloadPtr(iter);
                        auto row = insertBatchRows[index];
                        ui32 streamIndex = 0;
                        if constexpr (Many) {
                            streamIndex = streamIndexScalar ? *streamIndexScalar : streamIndexData[row];
                        }

                        Insert(row, static_cast<char*>(payload), isNew, streamIndex);
                    }
                });

                if constexpr (UseArena) {
                    for (ui32 i = 0; i < insertBatchLen; ++i) {
                        auto row = insertBatchRows[i];
                        ui32 streamIndex = 0;
                        if constexpr (Many) {
                            streamIndex = streamIndexScalar ? *streamIndexScalar : streamIndexData[row];
                        }

                        bool isNew = insertBatchIsNew[i];
                        char* payload = insertBatchPayloads[i];
                        Insert(row, payload, isNew, streamIndex);
                    }
                }
            }
        };

        for (ui64 row = 0; row < batchLength; ++row) {
            if constexpr (UseFilter) {
                if (filterBitmap && !filterBitmap[row]) {
                    continue;
                }
            }

            // encode key
            auto& buf = out[insertBatchLen];
            buf.Rewind();
            if constexpr (!std::is_same<TKey, TSSOKey>::value && !std::is_same<TKey, TExternalFixedSizeKey>::value) {
                WriteUnaligned<TKey>(buf.Data(), TKey{});
            }

            for (ui32 i = 0; i < keysDatum.size(); ++i) {
                if (keysDatum[i].is_scalar()) {
                    // TODO: more efficient code when grouping by scalar
                    Readers_[i]->SaveScalarItem(*keysDatum[i].scalar(), buf);
                } else {
                    Readers_[i]->SaveItem(*keysDatum[i].array(), row, buf);
                }
            }

            insertBatchRows[insertBatchLen] = row;
            ++insertBatchLen;
            if (insertBatchLen == PrefetchBatchSize) {
                processInsertBatch();
                insertBatchLen = 0;
            }
        }

        processInsertBatch();
    }

    bool Finish() {
        if (!HasValues_) {
            IsFinished_ = true;
            return false;
        }

        WritingOutput_ = true;
        OutputBlockSize_ = 0;
        PrepareAggBuilders();

        if constexpr (UseSet) {
            HashSetIt_ = HashSet_->Begin();
        } else {
            if constexpr (!InlineAggState) {
                HashFixedMapIt_ = HashFixedMap_->Begin();
            } else {
                HashMapIt_ = HashMap_->Begin();
            }
        }
        return true;
    }

    bool FillOutput(const THolderFactory& holderFactory) {
        bool exit = false;
        while (WritingOutput_) {
            if constexpr (UseSet) {
                for (; !exit && HashSetIt_ != HashSet_->End(); HashSet_->Advance(HashSetIt_)) {
                    if (!HashSet_->IsValid(HashSetIt_)) {
                        continue;
                    }

                    if (OutputBlockSize_ == MaxBlockLen_) {
                        Flush(false, holderFactory);
                        // return EFetchResult::One;
                        exit = true;
                        break;
                    }

                    TKey key = HashSet_->GetKeyValue(HashSetIt_);
                    TInputBuffer in(GetKeyView<TKey>(key, KeyLength_));
                    for (auto& kb : Builders_) {
                        kb->Add(in);
                    }
                    ++OutputBlockSize_;
                }
                break;
            } else {
                const bool done = InlineAggState ? Iterate(*HashMap_, HashMapIt_) : Iterate(*HashFixedMap_, HashFixedMapIt_);
                if (done) {
                    break;
                }
                Flush(false, holderFactory);
                exit = true;
                break;
            }
        }

        if (!exit) {
            IsFinished_ = true;
            WritingOutput_ = false;
            if (!OutputBlockSize_) {
                return false;
            }
            Flush(true, holderFactory);
        }

        FillArrays();
        return true;
    }

private:
    void PrepareAggBuilders() {
        if constexpr (!UseSet) {
            AggBuilders_.clear();
            AggBuilders_.reserve(Aggs_.size());
            for (const auto& a : Aggs_) {
                if constexpr (Finalize) {
                    AggBuilders_.emplace_back(a->MakeResultBuilder(MaxBlockLen_));
                } else {
                    AggBuilders_.emplace_back(a->MakeStateBuilder(MaxBlockLen_));
                }
            }
        }
    }

    void Flush(bool final, const THolderFactory& holderFactory) {
        if (!OutputBlockSize_) {
            return;
        }

        for (size_t i = 0; i < Builders_.size(); ++i) {
            Values[i] = holderFactory.CreateArrowBlock(Builders_[i]->Build(final));
        }

        if constexpr (!UseSet) {
            for (size_t i = 0; i < Aggs_.size(); ++i) {
                Values[Builders_.size() + i] = AggBuilders_[i]->Build();
            }
            if (!final) {
                PrepareAggBuilders();
            }
        }

        Values.back() = holderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(OutputBlockSize_)));
        OutputBlockSize_ = 0;
    }

    void Insert(ui64 row, char* payload, bool isNew, ui32 currentStreamIndex) const {
        char* ptr = payload;

        if (isNew) {
            if constexpr (Many) {
                static_assert(Finalize);
                MKQL_ENSURE(currentStreamIndex < Streams_.size(), "Invalid stream index");
                memset(ptr, 0, Streams_.size());
                ptr[currentStreamIndex] = 1;

                for (auto i : Streams_[currentStreamIndex]) {
                    Aggs_[i]->LoadState(ptr + AggStateOffsets_[i], BatchNum_, UnwrappedValues_.data(), row);
                }
            } else {
                for (size_t i = 0; i < Aggs_.size(); ++i) {
                    if constexpr (Finalize) {
                        Aggs_[i]->LoadState(ptr, BatchNum_, Values_.data(), row);
                    } else {
                        Aggs_[i]->InitKey(ptr, BatchNum_, Values_.data(), row);
                    }

                    ptr += Aggs_[i]->StateSize;
                }
            }
        } else {
            if constexpr (Many) {
                static_assert(Finalize);
                MKQL_ENSURE(currentStreamIndex < Streams_.size(), "Invalid stream index");

                bool isNewStream = !ptr[currentStreamIndex];
                ptr[currentStreamIndex] = 1;

                for (auto i : Streams_[currentStreamIndex]) {
                    if (isNewStream) {
                        Aggs_[i]->LoadState(ptr + AggStateOffsets_[i], BatchNum_, UnwrappedValues_.data(), row);
                    } else {
                        Aggs_[i]->UpdateState(ptr + AggStateOffsets_[i], BatchNum_, UnwrappedValues_.data(), row);
                    }
                }
            } else {
                for (size_t i = 0; i < Aggs_.size(); ++i) {
                    if constexpr (Finalize) {
                        Aggs_[i]->UpdateState(ptr, BatchNum_, Values_.data(), row);
                    } else {
                        Aggs_[i]->UpdateKey(ptr, BatchNum_, Values_.data(), row);
                    }

                    ptr += Aggs_[i]->StateSize;
                }
            }
        }
    }

    template <typename THash>
    bool Iterate(THash& hash, typename THash::const_iterator& iter) {
        MKQL_ENSURE(WritingOutput_, "Supposed to be called at the end");
        std::array<typename THash::const_iterator, PrefetchBatchSize> iters;
        ui32 itersLen = 0;
        auto iterateBatch = [&]() {
            for (ui32 i = 0; i < itersLen; ++i) {
                auto iter = iters[i];
                TKey key = hash.GetKeyValue(iter);
                auto payload = hash.GetPayloadPtr(iter);
                char* ptr;
                if constexpr (UseArena) {
                    ptr = ReadUnaligned<char*>(payload);
                } else {
                    ptr = (char*)payload;
                }

                TInputBuffer in(GetKeyView<TKey>(key, KeyLength_));
                for (auto& kb : Builders_) {
                    kb->Add(in);
                }

                if constexpr (Many) {
                    for (ui32 i = 0; i < Streams_.size(); ++i) {
                        MKQL_ENSURE(ptr[i], "Missing partial aggregation state for stream #" << i);
                    }

                    ptr += Streams_.size();
                }

                for (size_t i = 0; i < Aggs_.size(); ++i) {
                    AggBuilders_[i]->Add(ptr);
                    Aggs_[i]->DestroyState(ptr);

                    ptr += Aggs_[i]->StateSize;
                }
            }
        };

        for (; iter != hash.End(); hash.Advance(iter)) {
            if (!hash.IsValid(iter)) {
                continue;
            }

            if (OutputBlockSize_ == MaxBlockLen_) {
                iterateBatch();
                return false;
            }

            if (itersLen == iters.size()) {
                iterateBatch();
                itersLen = 0;
            }

            iters[itersLen] = iter;
            ++itersLen;
            ++OutputBlockSize_;
            if constexpr (UseArena) {
                auto payload = hash.GetPayloadPtr(iter);
                auto ptr = ReadUnaligned<char*>(payload);
                NYql::PrefetchForWrite(ptr);
            }

            if constexpr (std::is_same<TKey, TSSOKey>::value) {
                TKey key = hash.GetKeyValue(iter);
                if (!key.IsInplace()) {
                    NYql::PrefetchForRead(key.AsView().Data());
                }
            } else if constexpr (std::is_same<TKey, TExternalFixedSizeKey>::value) {
                TKey key = hash.GetKeyValue(iter);
                NYql::PrefetchForRead(key.Data);
            }
        }

        iterateBatch();
        return true;
    }
};

template <typename TKey, typename TAggregator, typename TFixedAggState, bool UseSet, bool UseFilter, bool Finalize, bool Many, typename TDerived>
class THashedWrapperBase: public TMutableComputationNode<TDerived> {
    using TComputationBase = TMutableComputationNode<TDerived>;

    using TState = THashedWrapperBaseState<TKey, TAggregator, TFixedAggState, UseSet, UseFilter, Finalize, Many, TDerived>;

public:
    THashedWrapperBase(TComputationMutables& mutables,
                       IComputationNode* stream,
                       std::optional<ui32> filterColumn,
                       size_t width,
                       const std::vector<TKeyParams>& keys,
                       size_t maxBlockLen,
                       ui32 keyLength,
                       std::vector<TAggParams<TAggregator>>&& aggsParams,
                       ui32 streamIndex,
                       std::vector<std::vector<ui32>>&& streams)
        : TComputationBase(mutables, EValueRepresentation::Boxed)
        , Stream_(stream)
        , FilterColumn_(filterColumn)
        , Width_(width)
        , OutputWidth_(keys.size() + aggsParams.size() + 1)
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(width))
        , Keys_(keys)
        , MaxBlockLen_(maxBlockLen)
        , AggsParams_(std::move(aggsParams))
        , KeyLength_(keyLength)
        , StreamIndex_(streamIndex)
        , Streams_(std::move(streams))
    {
        MKQL_ENSURE(Width_ > 0, "Missing block length column");
        if constexpr (UseFilter) {
            MKQL_ENSURE(filterColumn, "Missing filter column");
            MKQL_ENSURE(!Finalize, "Filter isn't compatible with Finalize");
        } else {
            MKQL_ENSURE(!filterColumn, "Unexpected filter column");
        }
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto state = ctx.HolderFactory.Create<TState>(KeyLength_, StreamIndex_, Width_, OutputWidth_, FilterColumn_, AggsParams_, Streams_, Keys_, MaxBlockLen_, ctx);
        return ctx.HolderFactory.Create<TStreamValue>(ctx.HolderFactory, std::move(state), std::move(Stream_->GetValue(ctx)));
    }

private:
    class TStreamValue: public TComputationValue<TStreamValue> {
        using TBase = TComputationValue<TStreamValue>;

    public:
        TStreamValue(TMemoryUsageInfo* memInfo, const THolderFactory& holderFactory,
                     NUdf::TUnboxedValue&& state, NUdf::TUnboxedValue&& stream)
            : TBase(memInfo)
            , State_(state)
            , Stream_(stream)
            , HolderFactory_(holderFactory)
        {
        }

    private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) {
            TState& state = *static_cast<TState*>(State_.AsBoxed().Get());
            auto* inputFields = state.Values_.data();
            const size_t inputWidth = state.Width_;
            const size_t outputWidth = state.OutputWidth_;
            MKQL_ENSURE(outputWidth == width, "The given width doesn't equal to the result type size");

            if (!state.Count) {
                if (state.IsFinished_) {
                    return NUdf::EFetchStatus::Finish;
                }

                while (!state.WritingOutput_) {
                    switch (Stream_.WideFetch(inputFields, inputWidth)) {
                        case NUdf::EFetchStatus::Yield:
                            return NUdf::EFetchStatus::Yield;
                        case NUdf::EFetchStatus::Ok:
                            state.ProcessInput(HolderFactory_);
                            continue;
                        case NUdf::EFetchStatus::Finish:
                            break;
                    }

                    if (state.Finish()) {
                        break;
                    } else {
                        return NUdf::EFetchStatus::Finish;
                    }
                }

                if (!state.FillOutput(HolderFactory_)) {
                    return NUdf::EFetchStatus::Finish;
                }
            }

            const auto sliceSize = state.Slice();
            for (size_t i = 0; i < outputWidth; ++i) {
                output[i] = state.Get(sliceSize, HolderFactory_, i);
            }
            return NUdf::EFetchStatus::Ok;
        }

    private:
        NUdf::TUnboxedValue State_;
        NUdf::TUnboxedValue Stream_;
        const THolderFactory& HolderFactory_;
    };

private:
    void RegisterDependencies() const final {
        this->DependsOn(Stream_);
    }

    IComputationNode* const Stream_;
    const std::optional<ui32> FilterColumn_;
    const size_t Width_;
    const size_t OutputWidth_;
    const size_t WideFieldsIndex_;
    const std::vector<TKeyParams> Keys_;
    const size_t MaxBlockLen_;
    const std::vector<TAggParams<TAggregator>> AggsParams_;
    const ui32 KeyLength_;
    const ui32 StreamIndex_;
    const std::vector<std::vector<ui32>> Streams_;
};

template <typename TKey, typename TFixedAggState, bool UseSet, bool UseFilter>
class TBlockCombineHashedWrapper
    : public THashedWrapperBase<TKey, IBlockAggregatorCombineKeys, TFixedAggState, UseSet, UseFilter, false, false, TBlockCombineHashedWrapper<TKey, TFixedAggState, UseSet, UseFilter>> {
public:
    using TSelf = TBlockCombineHashedWrapper<TKey, TFixedAggState, UseSet, UseFilter>;
    using TBase = THashedWrapperBase<TKey, IBlockAggregatorCombineKeys, TFixedAggState, UseSet, UseFilter, false, false, TSelf>;

    TBlockCombineHashedWrapper(TComputationMutables& mutables,
                               IComputationNode* stream,
                               std::optional<ui32> filterColumn,
                               size_t width,
                               const std::vector<TKeyParams>& keys,
                               size_t maxBlockLen,
                               ui32 keyLength,
                               std::vector<TAggParams<IBlockAggregatorCombineKeys>>&& aggsParams)
        : TBase(mutables, stream, filterColumn, width, keys, maxBlockLen, keyLength, std::move(aggsParams), 0, {})
    {
    }
};

template <typename TKey, typename TFixedAggState, bool UseSet>
class TBlockMergeFinalizeHashedWrapper
    : public THashedWrapperBase<TKey, IBlockAggregatorFinalizeKeys, TFixedAggState, UseSet, false, true, false, TBlockMergeFinalizeHashedWrapper<TKey, TFixedAggState, UseSet>> {
public:
    using TSelf = TBlockMergeFinalizeHashedWrapper<TKey, TFixedAggState, UseSet>;
    using TBase = THashedWrapperBase<TKey, IBlockAggregatorFinalizeKeys, TFixedAggState, UseSet, false, true, false, TSelf>;

    TBlockMergeFinalizeHashedWrapper(TComputationMutables& mutables,
                                     IComputationNode* stream,
                                     size_t width,
                                     const std::vector<TKeyParams>& keys,
                                     size_t maxBlockLen,
                                     ui32 keyLength,
                                     std::vector<TAggParams<IBlockAggregatorFinalizeKeys>>&& aggsParams)
        : TBase(mutables, stream, {}, width, keys, maxBlockLen, keyLength, std::move(aggsParams), 0, {})
    {
    }
};

template <typename TKey, typename TFixedAggState>
class TBlockMergeManyFinalizeHashedWrapper
    : public THashedWrapperBase<TKey, IBlockAggregatorFinalizeKeys, TFixedAggState, false, false, true, true, TBlockMergeManyFinalizeHashedWrapper<TKey, TFixedAggState>> {
public:
    using TSelf = TBlockMergeManyFinalizeHashedWrapper<TKey, TFixedAggState>;
    using TBase = THashedWrapperBase<TKey, IBlockAggregatorFinalizeKeys, TFixedAggState, false, false, true, true, TSelf>;

    TBlockMergeManyFinalizeHashedWrapper(TComputationMutables& mutables,
                                         IComputationNode* stream,
                                         size_t width,
                                         const std::vector<TKeyParams>& keys,
                                         size_t maxBlockLen,
                                         ui32 keyLength,
                                         std::vector<TAggParams<IBlockAggregatorFinalizeKeys>>&& aggsParams,
                                         ui32 streamIndex, std::vector<std::vector<ui32>>&& streams)
        : TBase(mutables, stream, {}, width, keys, maxBlockLen, keyLength, std::move(aggsParams), streamIndex, std::move(streams))
    {
    }
};

template <typename TAggregator>
std::unique_ptr<IPreparedBlockAggregator<TAggregator>> PrepareBlockAggregator(const IBlockAggregatorFactory& factory,
                                                                              TTupleType* tupleType,
                                                                              std::optional<ui32> filterColumn,
                                                                              const std::vector<ui32>& argsColumns,
                                                                              const TTypeEnvironment& env,
                                                                              TType* returnType,
                                                                              ui32 hint);

template <>
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareBlockAggregator<IBlockAggregatorCombineAll>(const IBlockAggregatorFactory& factory,
                                                                                                                         TTupleType* tupleType,
                                                                                                                         std::optional<ui32> filterColumn,
                                                                                                                         const std::vector<ui32>& argsColumns,
                                                                                                                         const TTypeEnvironment& env,
                                                                                                                         TType* returnType,
                                                                                                                         ui32 hint) {
    Y_UNUSED(hint);
    MKQL_ENSURE(!returnType, "Unexpected return type");
    return factory.PrepareCombineAll(tupleType, filterColumn, argsColumns, env);
}

template <>
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareBlockAggregator<IBlockAggregatorCombineKeys>(const IBlockAggregatorFactory& factory,
                                                                                                                           TTupleType* tupleType,
                                                                                                                           std::optional<ui32> filterColumn,
                                                                                                                           const std::vector<ui32>& argsColumns,
                                                                                                                           const TTypeEnvironment& env,
                                                                                                                           TType* returnType,
                                                                                                                           ui32 hint) {
    Y_UNUSED(hint);
    MKQL_ENSURE(!filterColumn, "Unexpected filter column");
    MKQL_ENSURE(!returnType, "Unexpected return type");
    return factory.PrepareCombineKeys(tupleType, argsColumns, env);
}

template <>
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareBlockAggregator<IBlockAggregatorFinalizeKeys>(const IBlockAggregatorFactory& factory,
                                                                                                                             TTupleType* tupleType,
                                                                                                                             std::optional<ui32> filterColumn,
                                                                                                                             const std::vector<ui32>& argsColumns,
                                                                                                                             const TTypeEnvironment& env,
                                                                                                                             TType* returnType,
                                                                                                                             ui32 hint) {
    MKQL_ENSURE(!filterColumn, "Unexpected filter column");
    MKQL_ENSURE(returnType, "Missing return type");
    return factory.PrepareFinalizeKeys(tupleType, argsColumns, env, returnType, hint);
}

template <typename TAggregator>
ui32 FillAggParams(TTupleLiteral* aggsVal, TTupleType* tupleType, std::optional<ui32> filterColumn, std::vector<TAggParams<TAggregator>>& aggsParams,
                   const TTypeEnvironment& env, bool overState, bool many, TArrayRef<TType* const> returnTypes, ui32 keysCount) {
    TTupleType* unwrappedTupleType = tupleType;
    if (many) {
        std::vector<TType*> unwrappedTypes(tupleType->GetElementsCount());
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            unwrappedTypes[i] = tupleType->GetElementType(i);
        }

        for (ui32 i = 0; i < aggsVal->GetValuesCount(); ++i) {
            auto aggVal = AS_VALUE(TTupleLiteral, aggsVal->GetValue(i));
            MKQL_ENSURE(aggVal->GetValuesCount() == 2, "Expected only one column");
            auto index = AS_VALUE(TDataLiteral, aggVal->GetValue(1))->AsValue().Get<ui32>();
            MKQL_ENSURE(index < unwrappedTypes.size(), "Bad state column index");
            auto blockType = AS_TYPE(TBlockType, unwrappedTypes[index]);
            MKQL_ENSURE(blockType->GetShape() == TBlockType::EShape::Many, "State must be a block");
            bool isOptional;
            auto unpacked = UnpackOptional(blockType->GetItemType(), isOptional);
            MKQL_ENSURE(isOptional, "State must be optional");
            unwrappedTypes[index] = TBlockType::Create(unpacked, TBlockType::EShape::Many, env);
        }

        unwrappedTupleType = TTupleType::Create(unwrappedTypes.size(), unwrappedTypes.data(), env);
    }

    ui32 totalStateSize = 0;
    for (ui32 i = 0; i < aggsVal->GetValuesCount(); ++i) {
        auto aggVal = AS_VALUE(TTupleLiteral, aggsVal->GetValue(i));
        auto name = AS_VALUE(TDataLiteral, aggVal->GetValue(0))->AsValue().AsStringRef();

        std::vector<ui32> argColumns;
        for (ui32 j = 1; j < aggVal->GetValuesCount(); ++j) {
            argColumns.push_back(AS_VALUE(TDataLiteral, aggVal->GetValue(j))->AsValue().Get<ui32>());
        }

        TAggParams<TAggregator> p;
        if (overState) {
            MKQL_ENSURE(argColumns.size() == 1, "Expected exactly one column");
            p.Column_ = argColumns[0];
            p.StateType_ = AS_TYPE(TBlockType, tupleType->GetElementType(p.Column_))->GetItemType();
            p.ReturnType_ = returnTypes[i + keysCount];
            TStringBuf left, right;
            if (TStringBuf(name).TrySplit('#', left, right)) {
                p.Hint_ = FromString<ui32>(right);
            }
        }

        p.Prepared_ = PrepareBlockAggregator<TAggregator>(GetBlockAggregatorFactory(name), unwrappedTupleType, filterColumn, argColumns, env, p.ReturnType_, p.Hint_);

        totalStateSize += p.Prepared_->StateSize;
        aggsParams.emplace_back(std::move(p));
    }

    return totalStateSize;
}

template <bool UseSet, bool UseFilter, typename TKey>
IComputationNode* MakeBlockCombineHashedWrapper(
    ui32 keyLength,
    ui32 totalStateSize,
    TComputationMutables& mutables,
    IComputationNode* stream,
    std::optional<ui32> filterColumn,
    size_t width,
    const std::vector<TKeyParams>& keys,
    size_t maxBlockLen,
    std::vector<TAggParams<IBlockAggregatorCombineKeys>>&& aggsParams) {
    if (totalStateSize <= sizeof(TState8)) {
        return new TBlockCombineHashedWrapper<TKey, TState8, UseSet, UseFilter>(mutables, stream, filterColumn, width, keys, maxBlockLen, keyLength, std::move(aggsParams));
    }

    if (totalStateSize <= sizeof(TState16)) {
        return new TBlockCombineHashedWrapper<TKey, TState16, UseSet, UseFilter>(mutables, stream, filterColumn, width, keys, maxBlockLen, keyLength, std::move(aggsParams));
    }

    return new TBlockCombineHashedWrapper<TKey, TStateArena, UseSet, UseFilter>(mutables, stream, filterColumn, width, keys, maxBlockLen, keyLength, std::move(aggsParams));
}

template <bool UseSet, bool UseFilter>
IComputationNode* MakeBlockCombineHashedWrapper(
    TMaybe<ui32> totalKeysSize,
    bool isFixed,
    ui32 totalStateSize,
    TComputationMutables& mutables,
    IComputationNode* stream,
    std::optional<ui32> filterColumn,
    size_t width,
    const std::vector<TKeyParams>& keys,
    size_t maxBlockLen,
    std::vector<TAggParams<IBlockAggregatorCombineKeys>>&& aggsParams) {
    if (totalKeysSize && *totalKeysSize <= sizeof(ui32)) {
        return MakeBlockCombineHashedWrapper<UseSet, UseFilter, ui32>(*totalKeysSize, totalStateSize, mutables, stream, filterColumn, width, keys, maxBlockLen, std::move(aggsParams));
    }

    if (totalKeysSize && *totalKeysSize <= sizeof(ui64)) {
        return MakeBlockCombineHashedWrapper<UseSet, UseFilter, ui64>(*totalKeysSize, totalStateSize, mutables, stream, filterColumn, width, keys, maxBlockLen, std::move(aggsParams));
    }

    if (totalKeysSize && *totalKeysSize <= sizeof(TKey16)) {
        return MakeBlockCombineHashedWrapper<UseSet, UseFilter, TKey16>(*totalKeysSize, totalStateSize, mutables, stream, filterColumn, width, keys, maxBlockLen, std::move(aggsParams));
    }

    if (totalKeysSize && isFixed) {
        return MakeBlockCombineHashedWrapper<UseSet, UseFilter, TExternalFixedSizeKey>(*totalKeysSize, totalStateSize, mutables, stream, filterColumn, width, keys, maxBlockLen, std::move(aggsParams));
    }

    return MakeBlockCombineHashedWrapper<UseSet, UseFilter, TSSOKey>(Max<ui32>(), totalStateSize, mutables, stream, filterColumn, width, keys, maxBlockLen, std::move(aggsParams));
}

template <typename TKey, bool UseSet>
IComputationNode* MakeBlockMergeFinalizeHashedWrapper(
    ui32 keyLength,
    ui32 totalStateSize,
    TComputationMutables& mutables,
    IComputationNode* stream,
    size_t width,
    const std::vector<TKeyParams>& keys,
    size_t maxBlockLen,
    std::vector<TAggParams<IBlockAggregatorFinalizeKeys>>&& aggsParams) {
    if (totalStateSize <= sizeof(TState8)) {
        return new TBlockMergeFinalizeHashedWrapper<TKey, TState8, UseSet>(mutables, stream, width, keys, maxBlockLen, keyLength, std::move(aggsParams));
    }

    if (totalStateSize <= sizeof(TState16)) {
        return new TBlockMergeFinalizeHashedWrapper<TKey, TState16, UseSet>(mutables, stream, width, keys, maxBlockLen, keyLength, std::move(aggsParams));
    }

    return new TBlockMergeFinalizeHashedWrapper<TKey, TStateArena, UseSet>(mutables, stream, width, keys, maxBlockLen, keyLength, std::move(aggsParams));
}

template <bool UseSet>
IComputationNode* MakeBlockMergeFinalizeHashedWrapper(
    TMaybe<ui32> totalKeysSize,
    bool isFixed,
    ui32 totalStateSize,
    TComputationMutables& mutables,
    IComputationNode* stream,
    size_t width,
    const std::vector<TKeyParams>& keys,
    size_t maxBlockLen,
    std::vector<TAggParams<IBlockAggregatorFinalizeKeys>>&& aggsParams) {
    if (totalKeysSize && *totalKeysSize <= sizeof(ui32)) {
        return MakeBlockMergeFinalizeHashedWrapper<ui32, UseSet>(*totalKeysSize, totalStateSize, mutables, stream, width, keys, maxBlockLen, std::move(aggsParams));
    }

    if (totalKeysSize && *totalKeysSize <= sizeof(ui64)) {
        return MakeBlockMergeFinalizeHashedWrapper<ui64, UseSet>(*totalKeysSize, totalStateSize, mutables, stream, width, keys, maxBlockLen, std::move(aggsParams));
    }

    if (totalKeysSize && *totalKeysSize <= sizeof(TKey16)) {
        return MakeBlockMergeFinalizeHashedWrapper<TKey16, UseSet>(*totalKeysSize, totalStateSize, mutables, stream, width, keys, maxBlockLen, std::move(aggsParams));
    }

    if (totalKeysSize && isFixed) {
        return MakeBlockMergeFinalizeHashedWrapper<TExternalFixedSizeKey, UseSet>(*totalKeysSize, totalStateSize, mutables, stream, width, keys, maxBlockLen, std::move(aggsParams));
    }

    return MakeBlockMergeFinalizeHashedWrapper<TSSOKey, UseSet>(Max<ui32>(), totalStateSize, mutables, stream, width, keys, maxBlockLen, std::move(aggsParams));
}

template <typename TKey>
IComputationNode* MakeBlockMergeManyFinalizeHashedWrapper(
    ui32 keyLength,
    ui32 totalStateSize,
    TComputationMutables& mutables,
    IComputationNode* stream,
    size_t width,
    const std::vector<TKeyParams>& keys,
    size_t maxBlockLen,
    std::vector<TAggParams<IBlockAggregatorFinalizeKeys>>&& aggsParams,
    ui32 streamIndex,
    std::vector<std::vector<ui32>>&& streams) {
    if (totalStateSize <= sizeof(TState8)) {
        return new TBlockMergeManyFinalizeHashedWrapper<TKey, TState8>(mutables, stream, width, keys, maxBlockLen, keyLength, std::move(aggsParams), streamIndex, std::move(streams));
    }

    if (totalStateSize <= sizeof(TState16)) {
        return new TBlockMergeManyFinalizeHashedWrapper<TKey, TState16>(mutables, stream, width, keys, maxBlockLen, keyLength, std::move(aggsParams), streamIndex, std::move(streams));
    }

    return new TBlockMergeManyFinalizeHashedWrapper<TKey, TStateArena>(mutables, stream, width, keys, maxBlockLen, keyLength, std::move(aggsParams), streamIndex, std::move(streams));
}

IComputationNode* MakeBlockMergeManyFinalizeHashedWrapper(
    TMaybe<ui32> totalKeysSize,
    bool isFixed,
    ui32 totalStateSize,
    TComputationMutables& mutables,
    IComputationNode* stream,
    size_t width,
    const std::vector<TKeyParams>& keys,
    size_t maxBlockLen,
    std::vector<TAggParams<IBlockAggregatorFinalizeKeys>>&& aggsParams,
    ui32 streamIndex,
    std::vector<std::vector<ui32>>&& streams) {
    if (totalKeysSize && *totalKeysSize <= sizeof(ui32)) {
        return MakeBlockMergeManyFinalizeHashedWrapper<ui32>(*totalKeysSize, totalStateSize, mutables, stream, width, keys, maxBlockLen, std::move(aggsParams), streamIndex, std::move(streams));
    }

    if (totalKeysSize && *totalKeysSize <= sizeof(ui64)) {
        return MakeBlockMergeManyFinalizeHashedWrapper<ui64>(*totalKeysSize, totalStateSize, mutables, stream, width, keys, maxBlockLen, std::move(aggsParams), streamIndex, std::move(streams));
    }

    if (totalKeysSize && *totalKeysSize <= sizeof(TKey16)) {
        return MakeBlockMergeManyFinalizeHashedWrapper<TKey16>(*totalKeysSize, totalStateSize, mutables, stream, width, keys, maxBlockLen, std::move(aggsParams), streamIndex, std::move(streams));
    }

    if (totalKeysSize && isFixed) {
        return MakeBlockMergeManyFinalizeHashedWrapper<TExternalFixedSizeKey>(*totalKeysSize, totalStateSize, mutables, stream, width, keys, maxBlockLen, std::move(aggsParams), streamIndex, std::move(streams));
    }

    return MakeBlockMergeManyFinalizeHashedWrapper<TSSOKey>(Max<ui32>(), totalStateSize, mutables, stream, width, keys, maxBlockLen, std::move(aggsParams), streamIndex, std::move(streams));
}

void PrepareKeys(const std::vector<TKeyParams>& keys, TMaybe<ui32>& totalKeysSize, bool& isFixed) {
    NYql::NUdf::TBlockItemSerializeProps props;
    for (auto& param : keys) {
        auto type = AS_TYPE(TBlockType, param.Type);
        UpdateBlockItemSerializeProps(TTypeInfoHelper(), type->GetItemType(), props);
    }

    isFixed = props.IsFixed;
    totalKeysSize = props.MaxSize;
}

void FillAggStreams(TRuntimeNode streamsNode, std::vector<std::vector<ui32>>& streams) {
    auto streamsVal = AS_VALUE(TTupleLiteral, streamsNode);
    for (ui32 i = 0; i < streamsVal->GetValuesCount(); ++i) {
        streams.emplace_back();
        auto& stream = streams.back();
        auto streamVal = AS_VALUE(TTupleLiteral, streamsVal->GetValue(i));
        for (ui32 j = 0; j < streamVal->GetValuesCount(); ++j) {
            ui32 index = AS_VALUE(TDataLiteral, streamVal->GetValue(j))->AsValue().Get<ui32>();
            stream.emplace_back(index);
        }
    }
}

} // namespace

IComputationNode* WrapBlockCombineAll(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 args");
    MKQL_ENSURE(callable.GetInput(0).GetStaticType()->IsStream(), "input must be stream");
    MKQL_ENSURE(callable.GetType()->GetReturnType()->IsStream(), "output must be stream");

    const auto wideComponents = GetWideComponents(callable.GetInput(0).GetStaticType());
    const auto tupleType = TTupleType::Create(wideComponents.size(), wideComponents.data(), ctx.Env);
    const auto returnWideComponents = GetWideComponents(callable.GetType()->GetReturnType());

    const auto wideStream = LocateNode(ctx.NodeLocator, callable, 0);

    auto filterColumnVal = AS_VALUE(TOptionalLiteral, callable.GetInput(1));
    std::optional<ui32> filterColumn;
    if (filterColumnVal->HasItem()) {
        filterColumn = AS_VALUE(TDataLiteral, filterColumnVal->GetItem())->AsValue().Get<ui32>();
    }

    auto aggsVal = AS_VALUE(TTupleLiteral, callable.GetInput(2));
    std::vector<TAggParams<IBlockAggregatorCombineAll>> aggsParams;
    FillAggParams<IBlockAggregatorCombineAll>(aggsVal, tupleType, filterColumn, aggsParams, ctx.Env, false, false, returnWideComponents, 0);

    return new TBlockCombineAllWrapper(ctx.Mutables, wideStream, filterColumn, tupleType->GetElementsCount(), std::move(aggsParams));
}

IComputationNode* WrapBlockCombineHashed(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4, "Expected 4 args");
    MKQL_ENSURE(callable.GetInput(0).GetStaticType()->IsStream(), "input must be stream");
    MKQL_ENSURE(callable.GetType()->GetReturnType()->IsStream(), "output must be stream");

    const auto wideComponents = GetWideComponents(callable.GetInput(0).GetStaticType());
    const auto tupleType = TTupleType::Create(wideComponents.size(), wideComponents.data(), ctx.Env);
    const auto returnWideComponents = GetWideComponents(callable.GetType()->GetReturnType());

    const auto wideStream = LocateNode(ctx.NodeLocator, callable, 0);

    auto filterColumnVal = AS_VALUE(TOptionalLiteral, callable.GetInput(1));
    std::optional<ui32> filterColumn;
    if (filterColumnVal->HasItem()) {
        filterColumn = AS_VALUE(TDataLiteral, filterColumnVal->GetItem())->AsValue().Get<ui32>();
    }

    auto keysVal = AS_VALUE(TTupleLiteral, callable.GetInput(2));
    std::vector<TKeyParams> keys;
    for (ui32 i = 0; i < keysVal->GetValuesCount(); ++i) {
        ui32 index = AS_VALUE(TDataLiteral, keysVal->GetValue(i))->AsValue().Get<ui32>();
        keys.emplace_back(TKeyParams{index, tupleType->GetElementType(index)});
    }

    auto aggsVal = AS_VALUE(TTupleLiteral, callable.GetInput(3));
    std::vector<TAggParams<IBlockAggregatorCombineKeys>> aggsParams;
    ui32 totalStateSize = FillAggParams<IBlockAggregatorCombineKeys>(aggsVal, tupleType, {}, aggsParams, ctx.Env, false, false, returnWideComponents, keys.size());

    TMaybe<ui32> totalKeysSize;
    bool isFixed = false;
    PrepareKeys(keys, totalKeysSize, isFixed);

    const size_t maxBlockLen = CalcMaxBlockLenForOutput(callable.GetType()->GetReturnType());
    if (filterColumn) {
        if (aggsParams.empty()) {
            return MakeBlockCombineHashedWrapper<true, true>(totalKeysSize, isFixed, totalStateSize, ctx.Mutables, wideStream, filterColumn, tupleType->GetElementsCount(), keys, maxBlockLen, std::move(aggsParams));
        } else {
            return MakeBlockCombineHashedWrapper<false, true>(totalKeysSize, isFixed, totalStateSize, ctx.Mutables, wideStream, filterColumn, tupleType->GetElementsCount(), keys, maxBlockLen, std::move(aggsParams));
        }
    } else {
        if (aggsParams.empty()) {
            return MakeBlockCombineHashedWrapper<true, false>(totalKeysSize, isFixed, totalStateSize, ctx.Mutables, wideStream, filterColumn, tupleType->GetElementsCount(), keys, maxBlockLen, std::move(aggsParams));
        } else {
            return MakeBlockCombineHashedWrapper<false, false>(totalKeysSize, isFixed, totalStateSize, ctx.Mutables, wideStream, filterColumn, tupleType->GetElementsCount(), keys, maxBlockLen, std::move(aggsParams));
        }
    }
}

IComputationNode* WrapBlockMergeFinalizeHashed(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 args");
    MKQL_ENSURE(callable.GetInput(0).GetStaticType()->IsStream(), "input must be stream");
    MKQL_ENSURE(callable.GetType()->GetReturnType()->IsStream(), "output must be stream");

    const auto wideComponents = GetWideComponents(callable.GetInput(0).GetStaticType());
    const auto tupleType = TTupleType::Create(wideComponents.size(), wideComponents.data(), ctx.Env);
    const auto returnWideComponents = GetWideComponents(callable.GetType()->GetReturnType());

    const auto wideStream = LocateNode(ctx.NodeLocator, callable, 0);

    auto keysVal = AS_VALUE(TTupleLiteral, callable.GetInput(1));
    std::vector<TKeyParams> keys;
    for (ui32 i = 0; i < keysVal->GetValuesCount(); ++i) {
        ui32 index = AS_VALUE(TDataLiteral, keysVal->GetValue(i))->AsValue().Get<ui32>();
        keys.emplace_back(TKeyParams{index, tupleType->GetElementType(index)});
    }

    auto aggsVal = AS_VALUE(TTupleLiteral, callable.GetInput(2));
    std::vector<TAggParams<IBlockAggregatorFinalizeKeys>> aggsParams;
    ui32 totalStateSize = FillAggParams<IBlockAggregatorFinalizeKeys>(aggsVal, tupleType, {}, aggsParams, ctx.Env, true, false, returnWideComponents, keys.size());

    TMaybe<ui32> totalKeysSize;
    bool isFixed = false;
    PrepareKeys(keys, totalKeysSize, isFixed);

    const size_t maxBlockLen = CalcMaxBlockLenForOutput(callable.GetType()->GetReturnType());
    if (aggsParams.empty()) {
        return MakeBlockMergeFinalizeHashedWrapper<true>(totalKeysSize, isFixed, totalStateSize, ctx.Mutables, wideStream, tupleType->GetElementsCount(), keys, maxBlockLen, std::move(aggsParams));
    } else {
        return MakeBlockMergeFinalizeHashedWrapper<false>(totalKeysSize, isFixed, totalStateSize, ctx.Mutables, wideStream, tupleType->GetElementsCount(), keys, maxBlockLen, std::move(aggsParams));
    }
}

IComputationNode* WrapBlockMergeManyFinalizeHashed(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 5, "Expected 5 args");
    MKQL_ENSURE(callable.GetInput(0).GetStaticType()->IsStream(), "input must be stream");
    MKQL_ENSURE(callable.GetType()->GetReturnType()->IsStream(), "output must be stream");

    const auto wideComponents = GetWideComponents(callable.GetInput(0).GetStaticType());
    const auto tupleType = TTupleType::Create(wideComponents.size(), wideComponents.data(), ctx.Env);
    const auto returnWideComponents = GetWideComponents(callable.GetType()->GetReturnType());

    const auto wideStream = LocateNode(ctx.NodeLocator, callable, 0);

    auto keysVal = AS_VALUE(TTupleLiteral, callable.GetInput(1));
    std::vector<TKeyParams> keys;
    for (ui32 i = 0; i < keysVal->GetValuesCount(); ++i) {
        ui32 index = AS_VALUE(TDataLiteral, keysVal->GetValue(i))->AsValue().Get<ui32>();
        keys.emplace_back(TKeyParams{index, tupleType->GetElementType(index)});
    }

    const auto aggsVal = AS_VALUE(TTupleLiteral, callable.GetInput(2));
    std::vector<TAggParams<IBlockAggregatorFinalizeKeys>> aggsParams;
    ui32 totalStateSize = FillAggParams<IBlockAggregatorFinalizeKeys>(aggsVal, tupleType, {}, aggsParams, ctx.Env, true, true, returnWideComponents, keys.size());

    TMaybe<ui32> totalKeysSize;
    bool isFixed = false;
    PrepareKeys(keys, totalKeysSize, isFixed);

    const ui32 streamIndex = AS_VALUE(TDataLiteral, callable.GetInput(3))->AsValue().Get<ui32>();
    std::vector<std::vector<ui32>> streams;
    FillAggStreams(callable.GetInput(4), streams);
    totalStateSize += streams.size();

    const size_t maxBlockLen = CalcMaxBlockLenForOutput(callable.GetType()->GetReturnType());
    if (aggsParams.empty()) {
        return MakeBlockMergeFinalizeHashedWrapper<true>(totalKeysSize, isFixed, totalStateSize, ctx.Mutables, wideStream, tupleType->GetElementsCount(),
                                                         keys, maxBlockLen, std::move(aggsParams));
    } else {
        return MakeBlockMergeManyFinalizeHashedWrapper(totalKeysSize, isFixed, totalStateSize, ctx.Mutables, wideStream, tupleType->GetElementsCount(),
                                                       keys, maxBlockLen, std::move(aggsParams), streamIndex, std::move(streams));
    }
}

} // namespace NMiniKQL
} // namespace NKikimr
