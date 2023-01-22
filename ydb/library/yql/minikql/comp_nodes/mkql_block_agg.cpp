#include "mkql_block_agg.h"
#include "mkql_block_agg_factory.h"
#include "mkql_block_builder.h"
#include "mkql_block_impl.h"
#include "mkql_rh_hash.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/minikql/arrow/mkql_bit_utils.h>

#include <arrow/scalar.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/chunked_array.h>

//#define USE_STD_UNORDERED

namespace NKikimr {
namespace NMiniKQL {

namespace {

constexpr bool InlineAggState = false;

#ifdef USE_STD_UNORDERED
template <typename TKey, typename TEqual = std::equal_to<TKey>, typename THash = std::hash<TKey>, typename TAllocator = std::allocator<char>>
class TDynamicHashMapImpl {
    using TMapType = std::unordered_map<TKey, std::vector<char>, THash, TEqual>;
    using const_iterator = typename TMapType::const_iterator;
    using iterator = typename TMapType::iterator;
public:
    TDynamicHashMapImpl(size_t stateSize)
        : StateSize_(stateSize)
    {}

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

    const TKey& GetKey(const_iterator it) const {
        return it->first;
    }

    char* GetMutablePayload(iterator it) const {
        return it->second.data();
    }

    const char* GetPayload(const_iterator it) const {
        return it->second.data();
    }

    void CheckGrow() {
    }

private:
    const size_t StateSize_;
    TMapType Map_;
};

template <typename TKey, typename TPayload, typename TEqual = std::equal_to<TKey>, typename THash = std::hash<TKey>, typename TAllocator = std::allocator<char>>
class TFixedHashMapImpl {
    using TMapType = std::unordered_map<TKey, TPayload, THash, TEqual>;
    using const_iterator = typename TMapType::const_iterator;
    using iterator = typename TMapType::iterator;
public:
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

    const TKey& GetKey(const_iterator it) const {
        return it->first;
    }

    char* GetMutablePayload(iterator it) const {
        return (char*)&it->second;
    }

    const char* GetPayload(const_iterator it) const {
        return (const char*)&it->second;
    }

    void CheckGrow() {
    }

private:
    TMapType Map_;
};

template <typename TKey, typename TEqual = std::equal_to<TKey>, typename THash = std::hash<TKey>, typename TAllocator = std::allocator<char>>
class THashSetImpl {
    using TSetType = std::unordered_set<TKey, THash, TEqual>;
    using const_iterator = typename TSetType::const_iterator;
    using iterator = typename TSetType::iterator;
public:
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

    void CheckGrow() {
    }

    const TKey& GetKey(const_iterator it) const {
        return *it;
    }

    char* GetMutablePayload(iterator it) const {
        Y_UNUSED(it);
        return nullptr;
    }

    const char* GetPayload(const_iterator it) const {
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

using TState16 = std::pair<ui64, ui64>;
static_assert(sizeof(TState16) == 16);

using TStateArena = void*;
static_assert(sizeof(TStateArena) == sizeof(void*));

struct TExternalFixedSizeKey {
    mutable const char* Data;
};

struct TKey16 {
    ui64 Lo;
    ui64 Hi;
};

class TSSOKey {
private:
    static constexpr size_t SSO_Length = 15;
    static_assert(SSO_Length < 128); // should fit into 7 bits

    struct TExternal {
        ui64 Length_;
        const char* Ptr_;
    };

    struct TInplace {
        ui8 SmallLength_;
        char Buffer_[SSO_Length];
    };

public:
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

    void UpdateExternalPointer(const char *ptr) const {
        Y_ASSERT(!IsInplace());
        const_cast<TExternal&>(U.E).Ptr_ = ptr;
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
    } U;
};

}
}
}

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
        {}

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
        {}

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
}

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <typename T>
struct TAggParams {
    std::unique_ptr<IPreparedBlockAggregator<T>> Prepared_;
    ui32 Column_ = 0;
    TType* StateType_ = nullptr;
};

struct TKeyParams {
    ui32 Index;
    TType* Type;
};

class TInputBuffer {
public:
    TInputBuffer(TStringBuf buf)
        : Buf_(buf)
    {}

    char PopChar() {
        Ensure(1);
        char c = Buf_.Data()[Pos_];
        ++Pos_;
        return c;
    }

    template <typename T>
    T PopNumber() {
        Ensure(sizeof(T));
        T t = *(const T*)(Buf_.Data() + Pos_);
        Pos_ += sizeof(T);
        return t;
    }

    std::string_view PopString() {
        ui32 size = PopNumber<ui32>();
        Ensure(size);
        std::string_view result(Buf_.Data() + Pos_, size);
        Pos_ += size;
        return result;
    }

private:
    void Ensure(size_t delta) {
        MKQL_ENSURE(Pos_ + delta <= Buf_.Size(), "Unexpected end of buffer");
    }

private:
    size_t Pos_ = 0;
    TStringBuf Buf_;
};

class TOutputBuffer {
public:
    void PushChar(char c) {
        Ensure(1);
        Vec_[Pos_] = c;
        ++Pos_;
    }

    template <typename T>
    void PushNumber(T t) {
        Ensure(sizeof(T));
        *(T*)&Vec_[Pos_] = t;
        Pos_ += sizeof(T);
    }

    void PushString(std::string_view data) {
        Ensure(sizeof(ui32) + data.size());
        *(ui32*)&Vec_[Pos_] = data.size();
        Pos_ += sizeof(ui32);
        std::memcpy(Vec_.data() + Pos_, data.data(), data.size());
        Pos_ += data.size();
    }

    // fill with zeros
    void Resize(size_t size) {
        Pos_ = 0;
        Vec_.clear();
        Vec_.resize(size);
    }

    void Rewind() {
        Pos_ = 0;
    }

    TStringBuf Finish() const {
        return TStringBuf(Vec_.data(), Vec_.data() + Pos_);
    }

private:
    void Ensure(size_t delta) {
        if (Pos_ + delta > Vec_.size()) {
            if (Pos_ + delta > Vec_.capacity()) {
                Vec_.reserve(Max(2 * Vec_.capacity(), Pos_ + delta));
            }
            // TODO: replace TVector - resize() performs unneeded zeroing here
            Vec_.resize(Pos_ + delta);
        }
    }

private:
    size_t Pos_ = 0;
    TVector<char> Vec_;
};

class IKeyColumnBuilder {
public:
    virtual ~IKeyColumnBuilder() = default;

    // decode part of buffer and advances position
    virtual void Add(TInputBuffer& in) = 0;

    virtual NUdf::TUnboxedValue Build() = 0;
};

class IKeySerializer {
public:
    virtual ~IKeySerializer() = default;

    // handle scalar or array item
    virtual void Serialize(const arrow::Datum& value, ui64 index, TOutputBuffer& out) const = 0;

    virtual std::unique_ptr<IKeyColumnBuilder> MakeBuilder(ui64 size, TComputationContext& ctx) const = 0;
};

template <typename T, typename TBuilder, bool IsOptional>
class TFixedSizeKeyColumnBuilder : public IKeyColumnBuilder  {
public:
    TFixedSizeKeyColumnBuilder(ui64 size, const std::shared_ptr<arrow::DataType>& dataType, TComputationContext& ctx)
        : Builder_(dataType, &ctx.ArrowMemoryPool)
        , Ctx_(ctx)
    {
        ARROW_OK(this->Builder_.Reserve(size));
    }

    void Add(TInputBuffer& in) final {
        if constexpr (IsOptional) {
            if (in.PopChar()) {
                auto x = in.PopNumber<T>();
                this->Builder_.UnsafeAppend(x);
            } else {
                this->Builder_.UnsafeAppendNull();
            }
        } else {
            auto x = in.PopNumber<T>();
            this->Builder_.UnsafeAppend(x);
        }
    }

    NUdf::TUnboxedValue Build() final {
        std::shared_ptr<arrow::ArrayData> result;
        ARROW_OK(this->Builder_.FinishInternal(&result));
        return Ctx_.HolderFactory.CreateArrowBlock(result);
    }

private:
    TBuilder Builder_;
    TComputationContext& Ctx_;
};

template <typename T, bool IsOptional>
class TStringKeyColumnBuilder : public IKeyColumnBuilder  {
public:
    using TArrowType = typename TPrimitiveDataType<T>::TResult;
    using TOffset = typename TArrowType::offset_type;

    TStringKeyColumnBuilder(ui64 size, TComputationContext& ctx)
        : Ctx_(ctx)
        , MaxLen_(size)
    {
        Reserve();
    }

    void Add(TInputBuffer& in) final {
        if constexpr (IsOptional) {
            if (!in.PopChar()) {
                NullBuilder_->UnsafeAppend(0);
                AppendCurrentOffset();
                return;
            }
        }

        std::string_view str = in.PopString();

        size_t currentLen = DataBuilder_->Length();
        // empty string can always be appended
        if (!str.empty() && currentLen + str.size() > MaxBlockSizeInBytes) {
            if (currentLen) {
                FlushChunk(false);
            }
            if (str.size() > MaxBlockSizeInBytes) {
                DataBuilder_->Reserve(str.size());
            }
        }

        AppendCurrentOffset();
        DataBuilder_->UnsafeAppend((const ui8*)str.data(), str.size());
        if constexpr (IsOptional) {
            NullBuilder_->UnsafeAppend(1);
        }
    }

    NUdf::TUnboxedValue Build() final {
        FlushChunk(true);
        arrow::ArrayVector chunks;
        for (auto& data : Chunks_) {
            chunks.push_back(arrow::Datum(data).make_array());
        }
        Y_VERIFY(!chunks.empty());

        auto chunked = ARROW_RESULT(arrow::ChunkedArray::Make(std::move(chunks), std::make_shared<TArrowType>()));
        return Ctx_.HolderFactory.CreateArrowBlock(std::move(chunked));
    }

private:
    void Reserve() {
        if constexpr (IsOptional) {
            NullBuilder_ = std::make_unique<TTypedBufferBuilder<ui8>>(&Ctx_.ArrowMemoryPool);
            NullBuilder_->Reserve(MaxLen_ + 1);
        }
        OffsetsBuilder_ = std::make_unique<TTypedBufferBuilder<TOffset>>(&Ctx_.ArrowMemoryPool);
        OffsetsBuilder_->Reserve(MaxLen_ + 1);
        DataBuilder_ = std::make_unique<TTypedBufferBuilder<ui8>>(&Ctx_.ArrowMemoryPool);
        DataBuilder_->Reserve(MaxBlockSizeInBytes);
    }

    void AppendCurrentOffset() {
        OffsetsBuilder_->UnsafeAppend(DataBuilder_->Length());
    }

    void FlushChunk(bool finish) {
        const auto length = OffsetsBuilder_->Length();
        Y_VERIFY(length > 0);

        AppendCurrentOffset();
        std::shared_ptr<arrow::Buffer> nullBitmap;
        if constexpr (IsOptional) {
            nullBitmap = NullBuilder_->Finish();
            nullBitmap = MakeDenseBitmap(nullBitmap->data(), length, &Ctx_.ArrowMemoryPool);
        }
        std::shared_ptr<arrow::Buffer> offsets = OffsetsBuilder_->Finish();
        std::shared_ptr<arrow::Buffer> data = DataBuilder_->Finish();

        auto arrowType = std::make_shared<TArrowType>();
        Chunks_.push_back(arrow::ArrayData::Make(arrowType, length, { nullBitmap, offsets, data }));
        if (!finish) {
            Reserve();
        }
    }

    std::unique_ptr<TTypedBufferBuilder<ui8>> NullBuilder_;
    std::unique_ptr<TTypedBufferBuilder<TOffset>> OffsetsBuilder_;
    std::unique_ptr<TTypedBufferBuilder<ui8>> DataBuilder_;

    std::vector<std::shared_ptr<arrow::ArrayData>> Chunks_;

    TComputationContext& Ctx_;
    const ui64 MaxLen_;
};

template <typename T, typename TScalar, typename TBuilder, bool IsOptional>
class TFixedSizeKeySerializer : public IKeySerializer {
public:
    TFixedSizeKeySerializer(const std::shared_ptr<arrow::DataType>& dataType)
        : DataType_(dataType)
    {}

    virtual void Serialize(const arrow::Datum& value, ui64 index, TOutputBuffer& out) const final {
        T x;
        if (value.is_scalar()) {
            const auto& scalar = value.scalar_as<TScalar>();
            if constexpr (IsOptional) {
                if (scalar.is_valid) {
                    out.PushChar(1);
                    x = scalar.value;
                } else {
                    out.PushChar(0);
                    return;
                }
               
            } else {
                Y_ASSERT(scalar.is_valid);
                x = scalar.value;
            }
        } else {
            const auto& array = *value.array();
            if constexpr (IsOptional) {
                if (array.GetNullCount() == 0 || arrow::BitUtil::GetBit(array.GetValues<uint8_t>(0, 0), index + array.offset)) {
                    out.PushChar(1);
                    x = array.GetValues<T>(1)[index];
                } else {
                    out.PushChar(0);
                    return;
                }
            } else {
                x = array.GetValues<T>(1)[index];
            }
        }

        out.PushNumber<T>(x);
    }

    std::unique_ptr<IKeyColumnBuilder> MakeBuilder(ui64 size, TComputationContext& ctx) const final {
        return std::make_unique<TFixedSizeKeyColumnBuilder<T, TBuilder, IsOptional>>(size, DataType_, ctx);
    }

private:
    const std::shared_ptr<arrow::DataType> DataType_;
};

template <typename T, bool IsOptional>
class TStringKeySerializer : public IKeySerializer {
public:
    using TOffset = typename TPrimitiveDataType<T>::TResult::offset_type;

    TStringKeySerializer() = default;

    virtual void Serialize(const arrow::Datum& value, ui64 index, TOutputBuffer& out) const final {
        std::string_view x;
        if (value.is_scalar()) {
            const auto& scalar = *value.scalar();
            if constexpr (IsOptional) {
                if (!scalar.is_valid) {
                    out.PushChar(0);
                    return;
                }
                out.PushChar(1);
            }
            x = GetStringScalarValue(scalar);
        } else {
            const auto& array = *value.array();
            if constexpr (IsOptional) {
                bool isValid = array.GetNullCount() == 0 ||
                    arrow::BitUtil::GetBit(array.GetValues<uint8_t>(0, 0), index + array.offset);
                if (!isValid) {
                    out.PushChar(0);
                    return;
                }
                out.PushChar(1);
            }
            const TOffset* offsets = array.GetValues<TOffset>(1);
            const char* data = array.GetValues<char>(2, 0);
            x = std::string_view(data + offsets[index], offsets[index + 1] - offsets[index]);
        }

        out.PushString(x);
    }

    std::unique_ptr<IKeyColumnBuilder> MakeBuilder(ui64 size, TComputationContext& ctx) const final {
        return std::make_unique<TStringKeyColumnBuilder<T, IsOptional>>(size, ctx);
    }
};

size_t GetBitmapPopCount(const std::shared_ptr<arrow::ArrayData>& arr) {
    size_t len = (size_t)arr->length;
    MKQL_ENSURE(arr->GetNullCount() == 0, "Bitmap block should not have nulls");
    const ui8* src = arr->GetValues<ui8>(1);
    return GetSparseBitmapPopCount(src, len);
}

class TBlockCombineAllWrapper : public TStatefulWideFlowComputationNode<TBlockCombineAllWrapper> {
public:
    TBlockCombineAllWrapper(TComputationMutables& mutables,
        IComputationWideFlowNode* flow,
        std::optional<ui32> filterColumn,
        size_t width,
        TVector<TAggParams<IBlockAggregatorCombineAll>>&& aggsParams)
        : TStatefulWideFlowComputationNode(mutables, flow, EValueRepresentation::Any)
        , Flow_(flow)
        , FilterColumn_(filterColumn)
        , Width_(width)
        , AggsParams_(std::move(aggsParams))
    {
        MKQL_ENSURE(Width_ > 0, "Missing block length column");
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state,
        TComputationContext& ctx,
        NUdf::TUnboxedValue*const* output) const
    {
        auto& s = GetState(state, ctx);
        if (s.IsFinished_) {
            return EFetchResult::Finish;
        }

        for (;;) {
            auto result = Flow_->FetchValues(ctx, s.ValuePointers_.data());
            if (result == EFetchResult::Yield) {
                return result;
            } else if (result == EFetchResult::One) {
                ui64 batchLength = GetBatchLength(s.Values_.data());
                if (!batchLength) {
                    continue;
                }

                std::optional<ui64> filtered;
                if (FilterColumn_) {
                    auto filterDatum = TArrowBlock::From(s.Values_[*FilterColumn_]).GetDatum();
                    if (filterDatum.is_scalar()) {
                        if (!filterDatum.scalar_as<arrow::UInt8Scalar>().value) {
                            continue;
                        }
                    } else {
                        ui64 popCount = GetBitmapPopCount(filterDatum.array());
                        if (popCount == 0) {
                            continue;
                        }

                        if (popCount < batchLength) {
                            filtered = popCount;
                        }
                    }
                }

                s.HasValues_ = true;
                char* ptr = s.AggStates_.data();
                for (size_t i = 0; i < s.Aggs_.size(); ++i) {
                    if (output[i]) {
                        s.Aggs_[i]->AddMany(ptr, s.Values_.data(), batchLength, filtered);
                    }

                    ptr += s.Aggs_[i]->StateSize;
                }
            } else {
                s.IsFinished_ = true;
                if (!s.HasValues_) {
                    return EFetchResult::Finish;
                }

                char* ptr = s.AggStates_.data();
                for (size_t i = 0; i < s.Aggs_.size(); ++i) {
                    if (auto* out = output[i]; out != nullptr) {
                        *out = s.Aggs_[i]->FinishOne(ptr);
                        s.Aggs_[i]->DestroyState(ptr);
                    }

                    ptr += s.Aggs_[i]->StateSize;
                }

                return EFetchResult::One;
            }
        }

        return EFetchResult::Finish;
    }

private:
    struct TState : public TComputationValue<TState> {
        TVector<NUdf::TUnboxedValue> Values_;
        TVector<NUdf::TUnboxedValue*> ValuePointers_;
        TVector<std::unique_ptr<IBlockAggregatorCombineAll>> Aggs_;
        bool IsFinished_ = false;
        bool HasValues_ = false;
        TVector<char> AggStates_;

        TState(TMemoryUsageInfo* memInfo, size_t width, std::optional<ui32> filterColumn, const TVector<TAggParams<IBlockAggregatorCombineAll>>& params, TComputationContext& ctx)
            : TComputationValue(memInfo)
            , Values_(width)
            , ValuePointers_(width)
        {
            for (size_t i = 0; i < width; ++i) {
                ValuePointers_[i] = &Values_[i];
            }

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
    };

private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue()) {
            state = ctx.HolderFactory.Create<TState>(Width_, FilterColumn_, AggsParams_, ctx);
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

    ui64 GetBatchLength(const NUdf::TUnboxedValue* columns) const {
        return TArrowBlock::From(columns[Width_ - 1]).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
    }

private:
    IComputationWideFlowNode* Flow_;
    std::optional<ui32> FilterColumn_;
    const size_t Width_;
    const TVector<TAggParams<IBlockAggregatorCombineAll>> AggsParams_;
};

template <typename T>
T MakeKey(TStringBuf s, ui32 keyLength) {
    Y_UNUSED(keyLength);
    Y_ASSERT(s.Size() <= sizeof(T));
    return *(const T*)s.Data();
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
    return { s.Data() };
}

void MoveKeyToArena(const TSSOKey& key, TPagedArena& arena, ui32 keyLength) {
    Y_UNUSED(keyLength);
    if (key.IsInplace()) {
        return;
    }

    auto view = key.AsView();
    auto ptr = (char*)arena.Alloc(view.Size());
    memcpy(ptr, view.Data(), view.Size());
    key.UpdateExternalPointer(ptr);
}

void MoveKeyToArena(const TExternalFixedSizeKey& key, TPagedArena& arena, ui32 keyLength) {
    auto ptr = (char*)arena.Alloc(keyLength);
    memcpy(ptr, key.Data, keyLength);
    key.Data = ptr;
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
class THashedWrapperBase : public TStatefulWideFlowBlockComputationNode<TDerived> {
public:
    using TSelf = THashedWrapperBase<TKey, TAggregator, TFixedAggState, UseSet, UseFilter, Finalize, Many, TDerived>;
    using TBase = TStatefulWideFlowBlockComputationNode<TDerived>;

    static constexpr bool UseArena = !InlineAggState && std::is_same<TFixedAggState, TStateArena>::value;

    THashedWrapperBase(TComputationMutables& mutables,
        IComputationWideFlowNode* flow,
        std::optional<ui32> filterColumn,
        size_t width,
        const std::vector<TKeyParams>& keys,
        std::vector<std::unique_ptr<IKeySerializer>>&& keySerializers,
        ui32 keyLength,
        TVector<TAggParams<TAggregator>>&& aggsParams,
        ui32 streamIndex,
        TVector<TVector<ui32>>&& streams)
        : TBase(mutables, flow, keys.size() + aggsParams.size() + 1)
        , Flow_(flow)
        , FilterColumn_(filterColumn)
        , Width_(width)
        , OutputWidth_(keys.size() + aggsParams.size() + 1)
        , Keys_(keys)
        , KeySerializers_(std::move(keySerializers))
        , KeyLength_(keyLength)
        , AggsParams_(std::move(aggsParams))
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

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state,
        TComputationContext& ctx,
        NUdf::TUnboxedValue*const* output) const
    {
        auto& s = GetState(state, ctx);
        if (s.IsFinished_) {
            return EFetchResult::Finish;
        }

        for (;;) {
            auto result = Flow_->FetchValues(ctx, s.ValuePointers_.data());
            if (result == EFetchResult::Yield) {
                return result;
            } else if (result == EFetchResult::One) {
                ui64 batchLength = GetBatchLength(s.Values_.data());
                if (!batchLength) {
                    continue;
                }

                const ui8* filterBitmap = nullptr;
                if constexpr (UseFilter) {
                    auto filterDatum = TArrowBlock::From(s.Values_[*FilterColumn_]).GetDatum();
                    if (filterDatum.is_scalar()) {
                        if (!filterDatum.template scalar_as<arrow::UInt8Scalar>().value) {
                            continue;
                        }
                    } else {
                        const auto& arr = filterDatum.array();
                        filterBitmap = arr->template GetValues<ui8>(1);
                        ui64 popCount = GetBitmapPopCount(arr);
                        if (popCount == 0) {
                            continue;
                        }
                    }
                }

                const ui32* streamIndexData = nullptr;
                if constexpr (Many) {
                    auto streamIndexDatum = TArrowBlock::From(s.Values_[StreamIndex_]).GetDatum();
                    MKQL_ENSURE(streamIndexDatum.is_array(), "Expected array");
                    streamIndexData = streamIndexDatum.array()->template GetValues<ui32>(1);
                    s.UnwrappedValues_ = s.Values_;
                    for (const auto& p : AggsParams_) {
                        const auto& columnDatum = TArrowBlock::From(s.UnwrappedValues_[p.Column_]).GetDatum();
                        MKQL_ENSURE(columnDatum.is_array(), "Expected array");
                        s.UnwrappedValues_[p.Column_] = ctx.HolderFactory.CreateArrowBlock(Unwrap(*columnDatum.array(), p.StateType_));
                    }
                }

                s.HasValues_ = true;
                TVector<arrow::Datum> keysDatum;
                keysDatum.reserve(Keys_.size());
                for (ui32 i = 0; i < Keys_.size(); ++i) {
                    keysDatum.emplace_back(TArrowBlock::From(s.Values_[Keys_[i].Index]).GetDatum());
                }

                TOutputBuffer out;
                out.Resize(sizeof(TKey));
                for (ui64 row = 0; row < batchLength; ++row) {
                    if constexpr (UseFilter) {
                        if (filterBitmap && !filterBitmap[row]) {
                            continue;
                        }
                    }

                    out.Rewind();
                    // encode key
                    for (ui32 i = 0; i < keysDatum.size(); ++i) {
                        KeySerializers_[i]->Serialize(keysDatum[i], row, out);
                    }

                    auto str = out.Finish();
                    TKey key = MakeKey<TKey>(str, KeyLength_);
                    if constexpr (UseSet) {
                        bool isNew;
                        auto iter = s.HashSet_->Insert(key, isNew);
                        if (isNew) {
                            if constexpr (std::is_same<TKey, TSSOKey>::value || std::is_same<TKey, TExternalFixedSizeKey>::value) {
                                MoveKeyToArena(s.HashSet_->GetKey(iter), s.Arena_, KeyLength_);
                            }

                            s.HashSet_->CheckGrow();
                        }
                    } else {
                        if (!InlineAggState) {
                            Insert(*s.HashFixedMap_, key, row, streamIndexData, output, s);
                        } else {
                            Insert(*s.HashMap_, key, row, streamIndexData, output, s);
                        }
                    }
                }
            } else {
                s.IsFinished_ = true;
                if (!s.HasValues_) {
                    return EFetchResult::Finish;
                }

                // export results, TODO: split by batches
                ui64 size;
                if constexpr (UseSet) {
                    size = s.HashSet_->GetSize();
                } else {
                    if (!InlineAggState) {
                        size = s.HashFixedMap_->GetSize();
                    } else {
                        size = s.HashMap_->GetSize();
                    }
                }

                TVector<std::unique_ptr<IKeyColumnBuilder>> keyBuilders;
                for (const auto& ks : KeySerializers_) {
                    keyBuilders.emplace_back(ks->MakeBuilder(size, ctx));
                }

                if constexpr (UseSet) {
                    for (auto iter = s.HashSet_->Begin(); iter != s.HashSet_->End(); s.HashSet_->Advance(iter)) {
                        if (!s.HashSet_->IsValid(iter)) {
                            continue;
                        }

                        const TKey& key = s.HashSet_->GetKey(iter);
                        TInputBuffer in(GetKeyView<TKey>(key, KeyLength_));
                        for (auto& kb : keyBuilders) {
                            kb->Add(in);
                        }
                    }
                } else {
                    TVector<std::unique_ptr<IAggColumnBuilder>> aggBuilders;
                    for (const auto& a : s.Aggs_) {
                        if constexpr (Finalize) {
                            aggBuilders.emplace_back(a->MakeResultBuilder(size));
                        } else {
                            aggBuilders.emplace_back(a->MakeStateBuilder(size));
                        }
                    }

                    if (!InlineAggState) {
                        Iterate(*s.HashFixedMap_, keyBuilders, aggBuilders, output, s);
                    } else {
                        Iterate(*s.HashMap_, keyBuilders, aggBuilders, output, s);
                    }

                    for (size_t i = 0; i < s.Aggs_.size(); ++i) {
                        if (output[Keys_.size() + i]) {
                            *output[Keys_.size() + i] = aggBuilders[i]->Build();
                        }
                    }
                }

                for (ui32 i = 0; i < Keys_.size(); ++i) {
                    if (output[i]) {
                        *output[i] = keyBuilders[i]->Build();
                    }
                }

                MKQL_ENSURE(output[OutputWidth_ - 1], "Block size should not be marked as unused");
                *output[OutputWidth_ - 1] = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(size)));
                return EFetchResult::One;
            }
        }
    }

private:
    struct TState : public TComputationValue<TState> {
        using TBase = TComputationValue<TState>;

        TVector<NUdf::TUnboxedValue> Values_;
        TVector<NUdf::TUnboxedValue*> ValuePointers_;
        TVector<std::unique_ptr<TAggregator>> Aggs_;
        TVector<ui32> AggStateOffsets_;
        TVector<NUdf::TUnboxedValue> UnwrappedValues_;
        bool IsFinished_ = false;
        bool HasValues_ = false;
        ui32 TotalStateSize_ = 0;
        std::unique_ptr<TDynamicHashMapImpl<TKey, std::equal_to<TKey>, std::hash<TKey>, TMKQLAllocator<char>>> HashMap_;
        std::unique_ptr<THashSetImpl<TKey, std::equal_to<TKey>, std::hash<TKey>, TMKQLAllocator<char>>> HashSet_;
        std::unique_ptr<TFixedHashMapImpl<TKey, TFixedAggState, std::equal_to<TKey>, std::hash<TKey>, TMKQLAllocator<char>>> HashFixedMap_;
        TPagedArena Arena_;

        TState(TMemoryUsageInfo* memInfo, ui32 keyLength, size_t width, std::optional<ui32> filterColumn, const TVector<TAggParams<TAggregator>>& params,
            const TVector<TVector<ui32>>& streams, TComputationContext& ctx)
            : TBase(memInfo)
            , Values_(width)
            , ValuePointers_(width)
            , UnwrappedValues_(width)
            , Arena_(TlsAllocState)
        {
            for (size_t i = 0; i < width; ++i) {
                ValuePointers_[i] = &Values_[i];
            }

            if constexpr (Many) {
                TotalStateSize_ += streams.size();
            } 

            for (const auto& p : params) {
                Aggs_.emplace_back(p.Prepared_->Make(ctx));
                MKQL_ENSURE(Aggs_.back()->StateSize == p.Prepared_->StateSize, "State size mismatch");
                AggStateOffsets_.emplace_back(TotalStateSize_);
                TotalStateSize_ += Aggs_.back()->StateSize;
            }

            auto equal = MakeEqual<TKey>(keyLength);
            auto hasher = MakeHash<TKey>(keyLength);
            if constexpr (UseSet) {
                MKQL_ENSURE(params.empty(), "Only keys are supported");
                HashSet_ = std::make_unique<THashSetImpl<TKey, std::equal_to<TKey>, std::hash<TKey>, TMKQLAllocator<char>>>(hasher, equal);
            } else {
                if (!InlineAggState) {
                    HashFixedMap_ = std::make_unique<TFixedHashMapImpl<TKey, TFixedAggState, std::equal_to<TKey>, std::hash<TKey>, TMKQLAllocator<char>>>(hasher, equal);
                } else {
                    HashMap_ = std::make_unique<TDynamicHashMapImpl<TKey, std::equal_to<TKey>, std::hash<TKey>, TMKQLAllocator<char>>>(TotalStateSize_, hasher, equal);
                }
            } 
        }
    };

private:
    void RegisterDependencies() const final {
        this->FlowDependsOn(Flow_);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue()) {
            state = ctx.HolderFactory.Create<TState>(KeyLength_, Width_, FilterColumn_, AggsParams_, Streams_, ctx);
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

    template <typename THash>
    void Insert(THash& hash, const TKey& key, ui64 row, const ui32* streamIndexData, NUdf::TUnboxedValue*const* output, TState& s) const {
        bool isNew;
        auto iter = hash.Insert(key, isNew);
        char* payload = (char*)hash.GetMutablePayload(iter);
        char* ptr;

        if (isNew) {
            if constexpr (UseArena) {
                ptr = (char*)s.Arena_.Alloc(s.TotalStateSize_);
                *(char**)payload = ptr;
            } else {
                ptr = payload;
            }

            if constexpr (Many) {
                static_assert(Finalize);
                ui32 currentStreamIndex = streamIndexData[row];
                MKQL_ENSURE(currentStreamIndex < Streams_.size(), "Invalid stream index");
                memset(ptr, 0, Streams_.size());
                ptr[currentStreamIndex] = 1;

                for (auto i : Streams_[currentStreamIndex]) {
                    if (output[Keys_.size() + i]) {
                        s.Aggs_[i]->LoadState(ptr + s.AggStateOffsets_[i], s.UnwrappedValues_.data(), row);
                    }
                }
            } else {
                for (size_t i = 0; i < s.Aggs_.size(); ++i) {
                    if (output[Keys_.size() + i]) {
                        if constexpr (Finalize) {
                            s.Aggs_[i]->LoadState(ptr, s.Values_.data(), row);
                        } else {
                            s.Aggs_[i]->InitKey(ptr, s.Values_.data(), row);
                        }
                    }

                    ptr += s.Aggs_[i]->StateSize;
                }
            }

            if constexpr (std::is_same<TKey, TSSOKey>::value || std::is_same<TKey, TExternalFixedSizeKey>::value) {
                MoveKeyToArena(hash.GetKey(iter), s.Arena_, KeyLength_);
            }

            hash.CheckGrow();
        } else {
            if constexpr (UseArena) {
                ptr = *(char**)payload;
            } else {
                ptr = payload;
            }

            if constexpr (Many) {
                static_assert(Finalize);
                ui32 currentStreamIndex = streamIndexData[row];
                MKQL_ENSURE(currentStreamIndex < Streams_.size(), "Invalid stream index");

                bool isNewStream = !ptr[currentStreamIndex];
                ptr[currentStreamIndex] = 1;

                for (auto i : Streams_[currentStreamIndex]) {
                    if (output[Keys_.size() + i]) {
                        if (isNewStream) {
                            s.Aggs_[i]->LoadState(ptr + s.AggStateOffsets_[i], s.UnwrappedValues_.data(), row);
                        } else {
                            s.Aggs_[i]->UpdateState(ptr + s.AggStateOffsets_[i], s.UnwrappedValues_.data(), row);
                        }
                    }
                }
            } else {
                for (size_t i = 0; i < s.Aggs_.size(); ++i) {
                    if (output[Keys_.size() + i]) {
                        if constexpr (Finalize) {
                            s.Aggs_[i]->UpdateState(ptr, s.Values_.data(), row);
                        } else {
                            s.Aggs_[i]->UpdateKey(ptr, s.Values_.data(), row);
                        }
                    }

                    ptr += s.Aggs_[i]->StateSize;
                }
            }
        }
    }

    template <typename THash>
    void Iterate(THash& hash, const TVector<std::unique_ptr<IKeyColumnBuilder>>& keyBuilders,
        const TVector<std::unique_ptr<IAggColumnBuilder>>& aggBuilders,
        NUdf::TUnboxedValue*const* output, TState& s) const {
        MKQL_ENSURE(s.IsFinished_, "Supposed to be called at the end");
        for (auto iter = hash.Begin(); iter != hash.End(); hash.Advance(iter)) {
            if (!hash.IsValid(iter)) {
                continue;
            }

            const TKey& key = hash.GetKey(iter);
            auto payload = (char*)hash.GetMutablePayload(iter);
            char* ptr;
            if constexpr (UseArena) {
                ptr = *(char**)payload;
            } else {
                ptr = payload;
            }

            TInputBuffer in(GetKeyView<TKey>(key, KeyLength_));
            for (auto& kb : keyBuilders) {
                kb->Add(in);
            }

            if constexpr (Many) {
                for (ui32 i = 0; i < Streams_.size(); ++i) {
                    MKQL_ENSURE(ptr[i], "Missing partial aggregation state");
                }

                ptr += Streams_.size();
            }

            for (size_t i = 0; i < s.Aggs_.size(); ++i) {
                if (output[Keys_.size() + i]) {
                    aggBuilders[i]->Add(ptr);
                    s.Aggs_[i]->DestroyState(ptr);
                }

                ptr += s.Aggs_[i]->StateSize;
            }
        }
    }

    ui64 GetBatchLength(const NUdf::TUnboxedValue* columns) const {
        return TArrowBlock::From(columns[Width_ - 1]).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
    }

private:
    IComputationWideFlowNode* Flow_;
    std::optional<ui32> FilterColumn_;
    const size_t Width_;
    const size_t OutputWidth_;
    const std::vector<TKeyParams> Keys_;
    const TVector<TAggParams<TAggregator>> AggsParams_;
    std::vector<std::unique_ptr<IKeySerializer>> KeySerializers_;
    const ui32 KeyLength_;
    const ui32 StreamIndex_;
    const TVector<TVector<ui32>> Streams_;
};

template <typename TKey, typename TFixedAggState, bool UseSet, bool UseFilter>
class TBlockCombineHashedWrapper : public THashedWrapperBase<TKey, IBlockAggregatorCombineKeys, TFixedAggState, UseSet, UseFilter, false, false, TBlockCombineHashedWrapper<TKey, TFixedAggState, UseSet, UseFilter>> {
public:
    using TSelf = TBlockCombineHashedWrapper<TKey, TFixedAggState, UseSet, UseFilter>;
    using TBase = THashedWrapperBase<TKey, IBlockAggregatorCombineKeys, TFixedAggState, UseSet, UseFilter, false, false, TSelf>;

    TBlockCombineHashedWrapper(TComputationMutables& mutables,
        IComputationWideFlowNode* flow,
        std::optional<ui32> filterColumn,
        size_t width,
        const std::vector<TKeyParams>& keys,
        std::vector<std::unique_ptr<IKeySerializer>>&& keySerializers,
        ui32 keyLength,
        TVector<TAggParams<IBlockAggregatorCombineKeys>>&& aggsParams)
        : TBase(mutables, flow, filterColumn, width, keys, std::move(keySerializers), keyLength, std::move(aggsParams), 0, {})
    {}
};

template <typename TKey, typename TFixedAggState, bool UseSet>
class TBlockMergeFinalizeHashedWrapper : public THashedWrapperBase<TKey, IBlockAggregatorFinalizeKeys, TFixedAggState, UseSet, false, true, false, TBlockMergeFinalizeHashedWrapper<TKey, TFixedAggState, UseSet>> {
public:
    using TSelf = TBlockMergeFinalizeHashedWrapper<TKey, TFixedAggState, UseSet>;
    using TBase = THashedWrapperBase<TKey, IBlockAggregatorFinalizeKeys, TFixedAggState, UseSet, false, true, false, TSelf>;

    TBlockMergeFinalizeHashedWrapper(TComputationMutables& mutables,
        IComputationWideFlowNode* flow,
        size_t width,
        const std::vector<TKeyParams>& keys,
        std::vector<std::unique_ptr<IKeySerializer>>&& keySerializers,
        ui32 keyLength,
        TVector<TAggParams<IBlockAggregatorFinalizeKeys>>&& aggsParams)
        : TBase(mutables, flow, {}, width, keys, std::move(keySerializers), keyLength, std::move(aggsParams), 0, {})
    {}
};

template <typename TKey, typename TFixedAggState>
class TBlockMergeManyFinalizeHashedWrapper : public THashedWrapperBase<TKey, IBlockAggregatorFinalizeKeys, TFixedAggState, false, false, true, true, TBlockMergeManyFinalizeHashedWrapper<TKey, TFixedAggState>> {
public:
    using TSelf = TBlockMergeManyFinalizeHashedWrapper<TKey, TFixedAggState>;
    using TBase = THashedWrapperBase<TKey, IBlockAggregatorFinalizeKeys, TFixedAggState, false, false, true, true, TSelf>;

    TBlockMergeManyFinalizeHashedWrapper(TComputationMutables& mutables,
        IComputationWideFlowNode* flow,
        size_t width,
        const std::vector<TKeyParams>& keys,
        std::vector<std::unique_ptr<IKeySerializer>>&& keySerializers,
        ui32 keyLength,
        TVector<TAggParams<IBlockAggregatorFinalizeKeys>>&& aggsParams,
        ui32 streamIndex, TVector<TVector<ui32>>&& streams)
        : TBase(mutables, flow, {}, width, keys, std::move(keySerializers), keyLength, std::move(aggsParams), streamIndex, std::move(streams))
    {}
};

template <typename TAggregator>
std::unique_ptr<IPreparedBlockAggregator<TAggregator>> PrepareBlockAggregator(const IBlockAggregatorFactory& factory,
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env);

template <>
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareBlockAggregator<IBlockAggregatorCombineAll>(const IBlockAggregatorFactory& factory,
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) {
    return factory.PrepareCombineAll(tupleType, filterColumn, argsColumns, env);
}

template <>
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareBlockAggregator<IBlockAggregatorCombineKeys>(const IBlockAggregatorFactory& factory,
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) {
    return factory.PrepareCombineKeys(tupleType, filterColumn, argsColumns, env);
}

template <>
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareBlockAggregator<IBlockAggregatorFinalizeKeys>(const IBlockAggregatorFactory& factory,
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) {
    MKQL_ENSURE(!filterColumn, "Unexpected filter column");
    return factory.PrepareFinalizeKeys(tupleType, argsColumns, env);
}

template <typename TAggregator>
ui32 FillAggParams(TTupleLiteral* aggsVal, TTupleType* tupleType, std::optional<ui32> filterColumn, TVector<TAggParams<TAggregator>>& aggsParams,
    const TTypeEnvironment& env, bool overState, bool many) {
    TTupleType* unwrappedTupleType = tupleType;
    if (many) {
        TVector<TType*> unwrappedTypes(tupleType->GetElementsCount());
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
        p.Prepared_ = PrepareBlockAggregator<TAggregator>(GetBlockAggregatorFactory(name), unwrappedTupleType, filterColumn, argColumns, env);
        if (overState) {
            MKQL_ENSURE(argColumns.size() == 1, "Expected exactly one column");
            p.Column_ = argColumns[0];
            p.StateType_ = AS_TYPE(TBlockType, tupleType->GetElementType(p.Column_))->GetItemType();
        }

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
    IComputationWideFlowNode* flow,
    std::optional<ui32> filterColumn,
    size_t width,
    const std::vector<TKeyParams>& keys,
    std::vector<std::unique_ptr<IKeySerializer>>&& keySerializers,
    TVector<TAggParams<IBlockAggregatorCombineKeys>>&& aggsParams) {
    if (totalStateSize <= sizeof(TState8)) {
        return new TBlockCombineHashedWrapper<TKey, TState8, UseSet, UseFilter>(mutables, flow, filterColumn, width, keys, std::move(keySerializers), keyLength, std::move(aggsParams));
    }

    if (totalStateSize <= sizeof(TState16)) {
        return new TBlockCombineHashedWrapper<TKey, TState16, UseSet, UseFilter>(mutables, flow, filterColumn, width, keys, std::move(keySerializers), keyLength, std::move(aggsParams));
    }

    return new TBlockCombineHashedWrapper<TKey, TStateArena, UseSet, UseFilter>(mutables, flow, filterColumn, width, keys, std::move(keySerializers), keyLength, std::move(aggsParams));
}

template <bool UseSet, bool UseFilter>
IComputationNode* MakeBlockCombineHashedWrapper(
    TMaybe<ui32> totalKeysSize,
    bool isFixed,
    ui32 totalStateSize,
    TComputationMutables& mutables,
    IComputationWideFlowNode* flow,
    std::optional<ui32> filterColumn,
    size_t width,
    const std::vector<TKeyParams>& keys,
    std::vector<std::unique_ptr<IKeySerializer>>&& keySerializers,
    TVector<TAggParams<IBlockAggregatorCombineKeys>>&& aggsParams) {
    if (totalKeysSize && *totalKeysSize <= sizeof(ui32)) {
        return MakeBlockCombineHashedWrapper<UseSet, UseFilter, ui32>(*totalKeysSize, totalStateSize, mutables, flow, filterColumn, width, keys, std::move(keySerializers), std::move(aggsParams));
    }

    if (totalKeysSize && *totalKeysSize <= sizeof(ui64)) {
        return MakeBlockCombineHashedWrapper<UseSet, UseFilter, ui64>(*totalKeysSize, totalStateSize, mutables, flow, filterColumn, width, keys, std::move(keySerializers), std::move(aggsParams));
    }

    if (totalKeysSize && *totalKeysSize <= sizeof(TKey16)) {
        return MakeBlockCombineHashedWrapper<UseSet, UseFilter, TKey16>(*totalKeysSize, totalStateSize, mutables, flow, filterColumn, width, keys, std::move(keySerializers), std::move(aggsParams));
    }

    if (totalKeysSize && isFixed) {
        return MakeBlockCombineHashedWrapper<UseSet, UseFilter, TExternalFixedSizeKey>(*totalKeysSize, totalStateSize, mutables, flow, filterColumn, width, keys, std::move(keySerializers), std::move(aggsParams));
    }

    return MakeBlockCombineHashedWrapper<UseSet, UseFilter, TSSOKey>(Max<ui32>(), totalStateSize, mutables, flow, filterColumn, width, keys, std::move(keySerializers), std::move(aggsParams));
}

template <typename TKey, bool UseSet>
IComputationNode* MakeBlockMergeFinalizeHashedWrapper(
    ui32 keyLength,
    ui32 totalStateSize,
    TComputationMutables& mutables,
    IComputationWideFlowNode* flow,
    size_t width,
    const std::vector<TKeyParams>& keys,
    std::vector<std::unique_ptr<IKeySerializer>>&& keySerializers,
    TVector<TAggParams<IBlockAggregatorFinalizeKeys>>&& aggsParams) {

    if (totalStateSize <= sizeof(TState8)) {
        return new TBlockMergeFinalizeHashedWrapper<TKey, TState8, UseSet>(mutables, flow, width, keys, std::move(keySerializers), keyLength, std::move(aggsParams));
    }

    if (totalStateSize <= sizeof(TState16)) {
        return new TBlockMergeFinalizeHashedWrapper<TKey, TState16, UseSet>(mutables, flow, width, keys, std::move(keySerializers), keyLength, std::move(aggsParams));
    }

    return new TBlockMergeFinalizeHashedWrapper<TKey, TStateArena, UseSet>(mutables, flow, width, keys, std::move(keySerializers), keyLength, std::move(aggsParams));
}

template <bool UseSet>
IComputationNode* MakeBlockMergeFinalizeHashedWrapper(
    TMaybe<ui32> totalKeysSize,
    bool isFixed,
    ui32 totalStateSize,
    TComputationMutables& mutables,
    IComputationWideFlowNode* flow,
    size_t width,
    const std::vector<TKeyParams>& keys,
    std::vector<std::unique_ptr<IKeySerializer>>&& keySerializers,
    TVector<TAggParams<IBlockAggregatorFinalizeKeys>>&& aggsParams) {
    if (totalKeysSize && *totalKeysSize <= sizeof(ui32)) {
        return MakeBlockMergeFinalizeHashedWrapper<ui32, UseSet>(*totalKeysSize, totalStateSize, mutables, flow, width, keys, std::move(keySerializers), std::move(aggsParams));
    }

    if (totalKeysSize && *totalKeysSize <= sizeof(ui64)) {
        return MakeBlockMergeFinalizeHashedWrapper<ui64, UseSet>(*totalKeysSize, totalStateSize, mutables, flow, width, keys, std::move(keySerializers), std::move(aggsParams));
    }

    if (totalKeysSize && *totalKeysSize <= sizeof(TKey16)) {
        return MakeBlockMergeFinalizeHashedWrapper<TKey16, UseSet>(*totalKeysSize, totalStateSize, mutables, flow, width, keys, std::move(keySerializers), std::move(aggsParams));
    }

    if (totalKeysSize && isFixed) {
        return MakeBlockMergeFinalizeHashedWrapper<TExternalFixedSizeKey, UseSet>(*totalKeysSize, totalStateSize, mutables, flow, width, keys, std::move(keySerializers), std::move(aggsParams));
    }

    return MakeBlockMergeFinalizeHashedWrapper<TSSOKey, UseSet>(Max<ui32>(), totalStateSize, mutables, flow, width, keys, std::move(keySerializers), std::move(aggsParams));
}

template <typename TKey>
IComputationNode* MakeBlockMergeManyFinalizeHashedWrapper(
    ui32 keyLength,
    ui32 totalStateSize,
    TComputationMutables& mutables,
    IComputationWideFlowNode* flow,
    size_t width,
    const std::vector<TKeyParams>& keys,
    std::vector<std::unique_ptr<IKeySerializer>>&& keySerializers,
    TVector<TAggParams<IBlockAggregatorFinalizeKeys>>&& aggsParams,
    ui32 streamIndex,
    TVector<TVector<ui32>>&& streams) {

    if (totalStateSize <= sizeof(TState8)) {
        return new TBlockMergeManyFinalizeHashedWrapper<TKey, TState8>(mutables, flow, width, keys, std::move(keySerializers), keyLength, std::move(aggsParams), streamIndex, std::move(streams));
    }

    if (totalStateSize <= sizeof(TState16)) {
        return new TBlockMergeManyFinalizeHashedWrapper<TKey, TState16>(mutables, flow, width, keys, std::move(keySerializers), keyLength, std::move(aggsParams), streamIndex, std::move(streams));
    }

    return new TBlockMergeManyFinalizeHashedWrapper<TKey, TStateArena>(mutables, flow, width, keys, std::move(keySerializers), keyLength, std::move(aggsParams), streamIndex, std::move(streams));
}

IComputationNode* MakeBlockMergeManyFinalizeHashedWrapper(
    TMaybe<ui32> totalKeysSize,
    bool isFixed,
    ui32 totalStateSize,
    TComputationMutables& mutables,
    IComputationWideFlowNode* flow,
    size_t width,
    const std::vector<TKeyParams>& keys,
    std::vector<std::unique_ptr<IKeySerializer>>&& keySerializers,
    TVector<TAggParams<IBlockAggregatorFinalizeKeys>>&& aggsParams,
    ui32 streamIndex,
    TVector<TVector<ui32>>&& streams) {
    if (totalKeysSize && *totalKeysSize <= sizeof(ui32)) {
        return MakeBlockMergeManyFinalizeHashedWrapper<ui32>(*totalKeysSize, totalStateSize, mutables, flow, width, keys, std::move(keySerializers), std::move(aggsParams), streamIndex, std::move(streams));
    }

    if (totalKeysSize && *totalKeysSize <= sizeof(ui64)) {
        return MakeBlockMergeManyFinalizeHashedWrapper<ui64>(*totalKeysSize, totalStateSize, mutables, flow, width, keys, std::move(keySerializers), std::move(aggsParams), streamIndex, std::move(streams));
    }

    if (totalKeysSize && *totalKeysSize <= sizeof(TKey16)) {
        return MakeBlockMergeManyFinalizeHashedWrapper<TKey16>(*totalKeysSize, totalStateSize, mutables, flow, width, keys, std::move(keySerializers), std::move(aggsParams), streamIndex, std::move(streams));
    }

    if (totalKeysSize && isFixed) {
        return MakeBlockMergeManyFinalizeHashedWrapper<TExternalFixedSizeKey>(*totalKeysSize, totalStateSize, mutables, flow, width, keys, std::move(keySerializers), std::move(aggsParams), streamIndex, std::move(streams));
    }

    return MakeBlockMergeManyFinalizeHashedWrapper<TSSOKey>(Max<ui32>(), totalStateSize, mutables, flow, width, keys, std::move(keySerializers), std::move(aggsParams), streamIndex, std::move(streams));
}

void PrepareKeys(const std::vector<TKeyParams>& keys, TMaybe<ui32>& totalKeysSize, bool& isFixed, std::vector<std::unique_ptr<IKeySerializer>>& keySerializers) {
    isFixed = true;
    totalKeysSize = 0;
    keySerializers.clear();
    for (const auto& k : keys) {
        auto itemType = AS_TYPE(TBlockType, k.Type)->GetItemType();
        bool isOptional;
        auto dataType = UnpackOptionalData(itemType, isOptional);
        if (isOptional && totalKeysSize) {
            *totalKeysSize += 1;
            isFixed = false;
        }

        switch (*dataType->GetDataSlot()) {
        case NUdf::EDataSlot::Int8:
            if (totalKeysSize) {
                *totalKeysSize += 1;
            }
            if (isOptional) {
                keySerializers.emplace_back(std::make_unique<TFixedSizeKeySerializer<i8, arrow::Int8Scalar, arrow::Int8Builder, true>>(arrow::int8()));
            } else {
                keySerializers.emplace_back(std::make_unique<TFixedSizeKeySerializer<i8, arrow::Int8Scalar, arrow::Int8Builder, false>>(arrow::int8()));
            }

            break;
        case NUdf::EDataSlot::Bool:
        case NUdf::EDataSlot::Uint8:
            if (totalKeysSize) {
                *totalKeysSize += 1;
            }
            if (isOptional) {
                keySerializers.emplace_back(std::make_unique<TFixedSizeKeySerializer<ui8, arrow::UInt8Scalar, arrow::UInt8Builder, true>>(arrow::uint8()));
            } else {
                keySerializers.emplace_back(std::make_unique<TFixedSizeKeySerializer<ui8, arrow::UInt8Scalar, arrow::UInt8Builder, false>>(arrow::uint8()));
            }

            break;
        case NUdf::EDataSlot::Int16:
            if (totalKeysSize) {
                *totalKeysSize += 2;
            }
            if (isOptional) {
                keySerializers.emplace_back(std::make_unique<TFixedSizeKeySerializer<i16, arrow::Int16Scalar, arrow::Int16Builder, true>>(arrow::int16()));
            } else {
                keySerializers.emplace_back(std::make_unique<TFixedSizeKeySerializer<i16, arrow::Int16Scalar, arrow::Int16Builder, false>>(arrow::int16()));
            }

            break;
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Date:
            if (totalKeysSize) {
                *totalKeysSize += 2;
            }
            if (isOptional) {
                keySerializers.emplace_back(std::make_unique<TFixedSizeKeySerializer<ui16, arrow::UInt16Scalar, arrow::UInt16Builder, true>>(arrow::uint16()));
            } else {
                keySerializers.emplace_back(std::make_unique<TFixedSizeKeySerializer<ui16, arrow::UInt16Scalar, arrow::UInt16Builder, false>>(arrow::uint16()));
            }

            break;
        case NUdf::EDataSlot::Int32:
            if (totalKeysSize) {
                *totalKeysSize += 4;
            }
            if (isOptional) {
                keySerializers.emplace_back(std::make_unique<TFixedSizeKeySerializer<i32, arrow::Int32Scalar, arrow::Int32Builder, true>>(arrow::int32()));
            } else {
                keySerializers.emplace_back(std::make_unique<TFixedSizeKeySerializer<i32, arrow::Int32Scalar, arrow::Int32Builder, false>>(arrow::int32()));
            }

            break;
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Datetime:
            if (totalKeysSize) {
                *totalKeysSize += 4;
            }
            if (isOptional) {
                keySerializers.emplace_back(std::make_unique<TFixedSizeKeySerializer<ui32, arrow::UInt32Scalar, arrow::UInt32Builder, true>>(arrow::uint32()));
            } else {
                keySerializers.emplace_back(std::make_unique<TFixedSizeKeySerializer<ui32, arrow::UInt32Scalar, arrow::UInt32Builder, false>>(arrow::uint32()));
            }

            break;
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
            if (totalKeysSize) {
                *totalKeysSize += 8;
            }
            if (isOptional) {
                keySerializers.emplace_back(std::make_unique<TFixedSizeKeySerializer<i64, arrow::Int64Scalar, arrow::Int64Builder, true>>(arrow::int64()));
            } else {
                keySerializers.emplace_back(std::make_unique<TFixedSizeKeySerializer<i64, arrow::Int64Scalar, arrow::Int64Builder, false>>(arrow::int64()));
            }

            break;
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
            if (totalKeysSize) {
                *totalKeysSize += 8;
            }
            if (isOptional) {
                keySerializers.emplace_back(std::make_unique<TFixedSizeKeySerializer<ui64, arrow::UInt64Scalar, arrow::UInt64Builder, true>>(arrow::uint64()));
            } else {
                keySerializers.emplace_back(std::make_unique<TFixedSizeKeySerializer<ui64, arrow::UInt64Scalar, arrow::UInt64Builder, false>>(arrow::uint64()));
            }

            break;
        case NUdf::EDataSlot::String:
            totalKeysSize = {};
            if (isOptional) {
                keySerializers.emplace_back(std::make_unique<TStringKeySerializer<char*, true>>());
            } else {
                keySerializers.emplace_back(std::make_unique<TStringKeySerializer<char*, false>>());
            }

            break;
        case NUdf::EDataSlot::Utf8:
            totalKeysSize = {};
            if (isOptional) {
                keySerializers.emplace_back(std::make_unique<TStringKeySerializer<NYql::NUdf::TUtf8, true>>());
            } else {
                keySerializers.emplace_back(std::make_unique<TStringKeySerializer<NYql::NUdf::TUtf8, false>>());
            }

            break;
        default:
            throw yexception() << "Unsupported key type";
        }
    }
}

void FillAggStreams(TRuntimeNode streamsNode, TVector<TVector<ui32>>& streams) {
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

}

IComputationNode* WrapBlockCombineAll(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 args");
    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto tupleType = AS_TYPE(TTupleType, flowType->GetItemType());

    auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    auto filterColumnVal = AS_VALUE(TOptionalLiteral, callable.GetInput(1));
    std::optional<ui32> filterColumn;
    if (filterColumnVal->HasItem()) {
        filterColumn = AS_VALUE(TDataLiteral, filterColumnVal->GetItem())->AsValue().Get<ui32>();
    }

    auto aggsVal = AS_VALUE(TTupleLiteral, callable.GetInput(2));
    TVector<TAggParams<IBlockAggregatorCombineAll>> aggsParams;
    ui32 totalStateSize = FillAggParams<IBlockAggregatorCombineAll>(aggsVal, tupleType, filterColumn, aggsParams, ctx.Env, false, false);
    return new TBlockCombineAllWrapper(ctx.Mutables, wideFlow, filterColumn, tupleType->GetElementsCount(), std::move(aggsParams));
}

IComputationNode* WrapBlockCombineHashed(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4, "Expected 4 args");
    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto tupleType = AS_TYPE(TTupleType, flowType->GetItemType());

    auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    auto filterColumnVal = AS_VALUE(TOptionalLiteral, callable.GetInput(1));
    std::optional<ui32> filterColumn;
    if (filterColumnVal->HasItem()) {
        filterColumn = AS_VALUE(TDataLiteral, filterColumnVal->GetItem())->AsValue().Get<ui32>();
    }

    auto keysVal = AS_VALUE(TTupleLiteral, callable.GetInput(2));
    std::vector<TKeyParams> keys;
    for (ui32 i = 0; i < keysVal->GetValuesCount(); ++i) {
        ui32 index = AS_VALUE(TDataLiteral, keysVal->GetValue(i))->AsValue().Get<ui32>();
        keys.emplace_back(TKeyParams{ index, tupleType->GetElementType(index) });
    }

    auto aggsVal = AS_VALUE(TTupleLiteral, callable.GetInput(3));
    TVector<TAggParams<IBlockAggregatorCombineKeys>> aggsParams;
    ui32 totalStateSize = FillAggParams<IBlockAggregatorCombineKeys>(aggsVal, tupleType, filterColumn, aggsParams, ctx.Env, false, false);

    TMaybe<ui32> totalKeysSize;
    bool isFixed = false;
    std::vector<std::unique_ptr<IKeySerializer>> keySerializers;
    PrepareKeys(keys, totalKeysSize, isFixed, keySerializers);

    if (filterColumn) {
        if (aggsParams.size() == 0) {
            return MakeBlockCombineHashedWrapper<true, true>(totalKeysSize, isFixed, totalStateSize, ctx.Mutables, wideFlow, filterColumn, tupleType->GetElementsCount(), keys, std::move(keySerializers), std::move(aggsParams));
        } else {
            return MakeBlockCombineHashedWrapper<false, true>(totalKeysSize, isFixed, totalStateSize, ctx.Mutables, wideFlow, filterColumn, tupleType->GetElementsCount(), keys, std::move(keySerializers), std::move(aggsParams));
        }
    } else {
        if (aggsParams.size() == 0) {
            return MakeBlockCombineHashedWrapper<true, false>(totalKeysSize, isFixed, totalStateSize, ctx.Mutables, wideFlow, filterColumn, tupleType->GetElementsCount(), keys, std::move(keySerializers), std::move(aggsParams));
        } else {
            return MakeBlockCombineHashedWrapper<false, false>(totalKeysSize, isFixed, totalStateSize, ctx.Mutables, wideFlow, filterColumn, tupleType->GetElementsCount(), keys, std::move(keySerializers), std::move(aggsParams));
        }
    }
}

IComputationNode* WrapBlockMergeFinalizeHashed(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 args");
    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto tupleType = AS_TYPE(TTupleType, flowType->GetItemType());

    auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    auto keysVal = AS_VALUE(TTupleLiteral, callable.GetInput(1));
    std::vector<TKeyParams> keys;
    for (ui32 i = 0; i < keysVal->GetValuesCount(); ++i) {
        ui32 index = AS_VALUE(TDataLiteral, keysVal->GetValue(i))->AsValue().Get<ui32>();
        keys.emplace_back(TKeyParams{ index, tupleType->GetElementType(index) });
    }

    auto aggsVal = AS_VALUE(TTupleLiteral, callable.GetInput(2));
    TVector<TAggParams<IBlockAggregatorFinalizeKeys>> aggsParams;
    ui32 totalStateSize = FillAggParams<IBlockAggregatorFinalizeKeys>(aggsVal, tupleType, {}, aggsParams, ctx.Env, true, false);

    TMaybe<ui32> totalKeysSize;
    bool isFixed = false;
    std::vector<std::unique_ptr<IKeySerializer>> keySerializers;
    PrepareKeys(keys, totalKeysSize, isFixed, keySerializers);

    if (aggsParams.size() == 0) {
        return MakeBlockMergeFinalizeHashedWrapper<true>(totalKeysSize, isFixed, totalStateSize, ctx.Mutables, wideFlow, tupleType->GetElementsCount(), keys, std::move(keySerializers), std::move(aggsParams));
    } else {
        return MakeBlockMergeFinalizeHashedWrapper<false>(totalKeysSize, isFixed, totalStateSize, ctx.Mutables, wideFlow, tupleType->GetElementsCount(), keys, std::move(keySerializers), std::move(aggsParams));
    }
}

IComputationNode* WrapBlockMergeManyFinalizeHashed(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 5, "Expected 5 args");
    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto tupleType = AS_TYPE(TTupleType, flowType->GetItemType());

    auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    auto keysVal = AS_VALUE(TTupleLiteral, callable.GetInput(1));
    std::vector<TKeyParams> keys;
    for (ui32 i = 0; i < keysVal->GetValuesCount(); ++i) {
        ui32 index = AS_VALUE(TDataLiteral, keysVal->GetValue(i))->AsValue().Get<ui32>();
        keys.emplace_back(TKeyParams{ index, tupleType->GetElementType(index) });
    }

    auto aggsVal = AS_VALUE(TTupleLiteral, callable.GetInput(2));
    TVector<TAggParams<IBlockAggregatorFinalizeKeys>> aggsParams;
    ui32 totalStateSize = FillAggParams<IBlockAggregatorFinalizeKeys>(aggsVal, tupleType, {}, aggsParams, ctx.Env, true, true);

    TMaybe<ui32> totalKeysSize;
    bool isFixed = false;
    std::vector<std::unique_ptr<IKeySerializer>> keySerializers;
    PrepareKeys(keys, totalKeysSize, isFixed, keySerializers);

    ui32 streamIndex = AS_VALUE(TDataLiteral, callable.GetInput(3))->AsValue().Get<ui32>();
    TVector<TVector<ui32>> streams;
    FillAggStreams(callable.GetInput(4), streams);
    totalStateSize += streams.size();

    if (aggsParams.size() == 0) {
        return MakeBlockMergeFinalizeHashedWrapper<true>(totalKeysSize, isFixed, totalStateSize, ctx.Mutables, wideFlow, tupleType->GetElementsCount(),
            keys, std::move(keySerializers), std::move(aggsParams));
    } else {
        return MakeBlockMergeManyFinalizeHashedWrapper(totalKeysSize, isFixed, totalStateSize, ctx.Mutables, wideFlow, tupleType->GetElementsCount(),
            keys, std::move(keySerializers), std::move(aggsParams), streamIndex, std::move(streams));
    }
}

}
}
