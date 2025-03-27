#include "mkql_block_transport.h"
#include "mkql_block_builder.h"

#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/public/udf/arrow/dispatch_traits.h>
#include <yql/essentials/public/udf/arrow/memory_pool.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NKikimr::NMiniKQL {

namespace {

using NYql::TChunkedBuffer;

TChunkedBuffer MakeChunkedBufferAndUntrack(const std::shared_ptr<const arrow::Buffer>& owner, const char* data, size_t size) {
    MKQLArrowUntrack(owner->data(), owner->capacity());
    return TChunkedBuffer(TStringBuf{data, size}, owner);
}

class TOwnedArrowBuffer : public arrow::Buffer {
public:
    TOwnedArrowBuffer(TStringBuf span, const std::shared_ptr<const void>& owner)
        : arrow::Buffer(reinterpret_cast<const uint8_t*>(span.data()), span.size())
        , Owner_(owner)
    {
    }
private:
    const std::shared_ptr<const void> Owner_;
};

std::shared_ptr<arrow::Buffer> MakeEmptyBuffer() {
    return std::make_shared<arrow::Buffer>(nullptr, 0);
}

bool HasArrrowAlignment(const void* buf) {
    return AlignUp(buf, NYql::NUdf::ArrowMemoryAlignment) == buf;
}

std::shared_ptr<arrow::Buffer> MakeZeroBuffer(size_t byteLen) {
    using namespace NYql::NUdf;
    if (!byteLen) {
        return MakeEmptyBuffer();
    }

    constexpr size_t NullWordCount = (MaxBlockSizeInBytes + sizeof(ui64) - 1) / sizeof(ui64);
    constexpr size_t ExtraAlignWords = (ArrowMemoryAlignment > sizeof(ui64)) ? (ArrowMemoryAlignment / sizeof(ui64) - 1) : 0;
    static const ui64 nulls[NullWordCount + ExtraAlignWords] = { 0 };

    // round all buffer length to 64 bytes
    size_t capacity = AlignUp(byteLen, size_t(64));
    if (capacity <= NullWordCount * sizeof(ui64)) {
        return std::make_shared<arrow::Buffer>(AlignUp(reinterpret_cast<const ui8*>(nulls), ArrowMemoryAlignment), byteLen);
    }

    auto result = AllocateResizableBuffer(byteLen, GetYqlMemoryPool());
    ARROW_OK(result->Resize(byteLen));
    std::memset(result->mutable_data(), 0, byteLen);
    return result;
}

std::shared_ptr<arrow::Buffer> MakeZeroBitmap(size_t bitCount) {
    // align up 8 byte boundary
    size_t byteCount = AlignUp(bitCount, size_t(64)) >> 3;
    return MakeZeroBuffer(byteCount);
}

bool NeedStoreBitmap(const arrow::ArrayData& data) {
    auto nullCount = data.GetNullCount();
    return nullCount != 0 && nullCount != data.length;
}

void StoreNullsSizes(const arrow::ArrayData& data, const IBlockSerializer::TMetadataSink& metaSink) {
    metaSink(data.GetNullCount());
    if (!NeedStoreBitmap(data)) {
        metaSink(0);
        return;
    }

    const ui64 desiredOffset = data.offset % 8;
    size_t nullBytes = AlignUp((size_t)data.length + desiredOffset, size_t(8)) >> 3;
    metaSink(nullBytes);
}

void LoadNullsSizes(const IBlockDeserializer::TMetadataSource& metaSource, TMaybe<ui64>& nullsCount, TMaybe<ui64>& nullsSize) {
    YQL_ENSURE(!nullsCount.Defined() && !nullsSize.Defined(), "Attempt to load null sizes twice (most likely LoadArray() is not called)");
    nullsCount = metaSource();
    nullsSize = metaSource();
}

void StoreNulls(const arrow::ArrayData& data, TChunkedBuffer& dst) {
    if (!NeedStoreBitmap(data)) {
        return;
    }
    const ui64 desiredOffset = data.offset % 8;
    size_t nullBytes = AlignUp((size_t)data.length + desiredOffset, size_t(8)) >> 3;
    YQL_ENSURE(desiredOffset <= (size_t)data.offset);
    YQL_ENSURE((data.offset - desiredOffset) % 8 == 0);
    const char* nulls = data.GetValues<char>(0, 0) + (data.offset - desiredOffset) / 8;
    dst.Append(MakeChunkedBufferAndUntrack(data.buffers[0], nulls, nullBytes));
}

void LoadBufferSize(const IBlockDeserializer::TMetadataSource& metaSource, TMaybe<ui64>& result) {
    YQL_ENSURE(!result.Defined(), "Attempt to load buffer size twice (most likely LoadArray() is not called)");
    result = metaSource();
}

std::shared_ptr<arrow::Buffer> LoadBuffer(TChunkedBuffer& source, TMaybe<ui64> size) {
    using namespace NYql::NUdf;
    YQL_ENSURE(size.Defined(), "Buffer size is not loaded");
    if (!*size) {
        return MakeEmptyBuffer();
    }

    size_t toAppend = *size;
    const TChunkedBuffer::TChunk& front = source.Front();
    if (front.Buf.size() >= toAppend && HasArrrowAlignment(front.Buf.data())) {
        TStringBuf data = source.Front().Buf;
        data.Trunc(toAppend);
        auto result = std::make_shared<TOwnedArrowBuffer>(data, source.Front().Owner);
        source.Erase(toAppend);
        return result;
    }

    auto result = AllocateResizableBuffer(toAppend, NYql::NUdf::GetYqlMemoryPool());
    ARROW_OK(result->Resize((int64_t)toAppend));
    uint8_t* dst = result->mutable_data();
    while (toAppend) {
        const TChunkedBuffer::TChunk& front = source.Front();
        TStringBuf buf = front.Buf;
        YQL_ENSURE(!buf.empty(), "Premature end of buffer");
        size_t chunk = std::min(toAppend, buf.size());
        std::memcpy(dst, buf.data(), chunk);
        dst += chunk;
        toAppend -= chunk;
        source.Erase(chunk);
    }

    return result;
}

std::shared_ptr<arrow::Buffer> LoadNullsBitmap(TChunkedBuffer& source, TMaybe<ui64> nullCount, TMaybe<ui64> bitmapSize) {
    YQL_ENSURE(nullCount.Defined(), "Bitmap null count is not loaded");
    YQL_ENSURE(bitmapSize.Defined(), "Bitmap size is not loaded");
    if (*nullCount == 0) {
        YQL_ENSURE(!*bitmapSize);
        return {};
    }
    YQL_ENSURE(*bitmapSize);
    return LoadBuffer(source, bitmapSize);
}

class TBlockSerializerBase : public IBlockSerializer {
public:
    explicit TBlockSerializerBase(const TBlockSerializerParams& params)
        : Pool_(params.Pool)
        , MinFillPercentage_(params.MinFillPercentage)
    {
        YQL_ENSURE(!MinFillPercentage_ || *MinFillPercentage_ <= 100);
    }

protected:
    arrow::MemoryPool* Pool_;
    const TMaybe<ui8> MinFillPercentage_;
};

class TBlockDeserializerBase : public IBlockDeserializer {
public:
    TBlockDeserializerBase() = default;

    virtual void SetArrowType(const std::shared_ptr<arrow::DataType>& type) {
        ArrowType_ = type;
    }

    void LoadMetadata(const TMetadataSource& metaSource) final {
        if (IsNullable()) {
            LoadNullsSizes(metaSource, NullsCount_, NullsSize_);
        }
        DoLoadMetadata(metaSource);
    }

    virtual std::shared_ptr<arrow::ArrayData> LoadArray(TChunkedBuffer& src, ui64 blockLen, ui64 offset) final {
        YQL_ENSURE(blockLen > 0, "Should be handled earlier");
        std::shared_ptr<arrow::Buffer> nulls;
        i64 nullsCount = 0;
        if (IsNullable()) {
            YQL_ENSURE(NullsCount_.Defined() && NullsSize_.Defined(), "Nulls metadata should be loaded");
            if (*NullsCount_ != 0) {
                if (*NullsSize_ == 0) {
                    auto result = MakeDefaultValue(blockLen, offset);
                    ResetMetadata();
                    return result;
                }
                nulls = LoadNullsBitmap(src, NullsCount_, NullsSize_);
                nullsCount = *NullsCount_;
            }
        }
        auto result = DoLoadArray(src, nulls, nullsCount, blockLen, offset);
        ResetMetadata();
        return result;
    }

    void ResetMetadata() {
        NullsCount_ = NullsSize_ = {};
        DoResetMetadata();
    }

    std::shared_ptr<arrow::ArrayData> MakeDefaultValue(ui64 blockLen, ui64 offset) const {
        std::shared_ptr<arrow::Buffer> nulls;
        i64 nullsCount = 0;
        if (IsNullable()) {
            nulls = MakeZeroBitmap(blockLen + offset);
            nullsCount = blockLen;
        }
        return DoMakeDefaultValue(nulls, nullsCount, blockLen, offset);
    }

protected:
    virtual void DoLoadMetadata(const TMetadataSource& metaSource) = 0;
    virtual void DoResetMetadata() = 0;
    virtual bool IsNullable() const = 0;
    virtual std::shared_ptr<arrow::ArrayData> DoMakeDefaultValue(const std::shared_ptr<arrow::Buffer>& nulls, i64 nullsCount, ui64 blockLen, ui64 offset) const = 0;
    virtual std::shared_ptr<arrow::ArrayData> DoLoadArray(TChunkedBuffer& src, const std::shared_ptr<arrow::Buffer>& nulls, i64 nullsCount, ui64 blockLen, ui64 offset) = 0;

    std::shared_ptr<arrow::DataType> ArrowType_;
    TMaybe<ui64> NullsCount_;
    TMaybe<ui64> NullsSize_;
};

template<size_t ObjectSize, bool Nullable>
class TFixedSizeBlockSerializer final : public TBlockSerializerBase {
    using TBase = TBlockSerializerBase;

public:
    using TBase::TBase;

    size_t ArrayMetadataCount() const final {
        return Nullable ? 3 : 1;
    }

    void StoreMetadata(const arrow::ArrayData& data, const IBlockSerializer::TMetadataSink& metaSink) const final {
        if constexpr (Nullable) {
            StoreNullsSizes(data, metaSink);
            if (data.GetNullCount() == data.length) {
                metaSink(0);
                return;
            }
        }
        const ui64 desiredOffset = data.offset % 8;
        size_t dataBytes = ((size_t)data.length + desiredOffset) * ObjectSize;
        metaSink(dataBytes);
    }

    void StoreArray(const arrow::ArrayData& data, TChunkedBuffer& dst) const final {
        if constexpr (Nullable) {
            StoreNulls(data, dst);
            if (data.GetNullCount() == data.length) {
                return;
            }
        }

        const ui64 desiredOffset = data.offset % 8;
        const char* buf = reinterpret_cast<const char*>(data.buffers[1]->data()) + (data.offset - desiredOffset) * ObjectSize;
        size_t dataBytes = ((size_t)data.length + desiredOffset) * ObjectSize;
        dst.Append(MakeChunkedBufferAndUntrack(data.buffers[1], buf, dataBytes));
    }
};

template<size_t ObjectSize, bool Nullable>
class TFixedSizeBlockDeserializer final : public TBlockDeserializerBase {
public:
    TFixedSizeBlockDeserializer() = default;
private:
    void DoLoadMetadata(const TMetadataSource& metaSource) final {
        LoadBufferSize(metaSource, DataSize_);
    }

    bool IsNullable() const final {
        return Nullable;
    }

    std::shared_ptr<arrow::ArrayData> DoMakeDefaultValue(const std::shared_ptr<arrow::Buffer>& nulls, i64 nullsCount, ui64 blockLen, ui64 offset) const final {
        auto data = MakeZeroBuffer((blockLen + offset) * ObjectSize);
        return arrow::ArrayData::Make(ArrowType_, blockLen, { nulls, data }, nullsCount, offset);
    }

    std::shared_ptr<arrow::ArrayData> DoLoadArray(TChunkedBuffer& src, const std::shared_ptr<arrow::Buffer>& nulls, i64 nullsCount, ui64 blockLen, ui64 offset) final {
        auto data = LoadBuffer(src, DataSize_);
        return arrow::ArrayData::Make(ArrowType_, blockLen, {nulls, data}, nullsCount, offset);
    }

    void DoResetMetadata() final {
        DataSize_ = {};
    }

    TMaybe<ui64> DataSize_;
};

template<typename TStringType, bool Nullable>
class TStringBlockSerializer final : public TBlockSerializerBase {
    using TBase = TBlockSerializerBase;
    using TOffset = typename TStringType::offset_type;

public:
    using TBase::TBase;

private:
    size_t ArrayMetadataCount() const final {
        return Nullable ? 4 : 2;
    }

    void StoreMetadata(const arrow::ArrayData& data, const IBlockSerializer::TMetadataSink& metaSink) const final {
        if constexpr (Nullable) {
            StoreNullsSizes(data, metaSink);
            if (data.GetNullCount() == data.length) {
                metaSink(0);
                metaSink(0);
                return;
            }
        }

        const ui64 desiredOffset = data.offset % 8;
        size_t offsetsSize = ((size_t)data.length + 1 + desiredOffset) * sizeof(TOffset);
        metaSink(offsetsSize);

        if (ShouldTrimArray(data)) {
            const TOffset* offsetData = data.GetValues<TOffset>(1) - desiredOffset;
            metaSink(offsetData[data.length + desiredOffset] - offsetData[0]);
        } else {
            metaSink(data.buffers[2]->size());
        }
    }

    void StoreArray(const arrow::ArrayData& data, TChunkedBuffer& dst) const final {
        if constexpr (Nullable) {
            StoreNulls(data, dst);
            if (data.GetNullCount() == data.length) {
                return;
            }
        }

        if (ShouldTrimArray(data)) {
            StoreTrimmedArray(data, dst);
            return;
        }

        const ui64 desiredOffset = data.offset % 8;
        const char* offsets = reinterpret_cast<const char*>(data.GetValues<TOffset>(1) - desiredOffset);
        size_t offsetsSize = ((size_t)data.length + 1 + desiredOffset) * sizeof(TOffset);
        dst.Append(MakeChunkedBufferAndUntrack(data.buffers[1], offsets, offsetsSize));

        const char* mainData = reinterpret_cast<const char*>(data.buffers[2]->data());
        size_t mainSize = data.buffers[2]->size();
        dst.Append(MakeChunkedBufferAndUntrack(data.buffers[2], mainData, mainSize));
    }

private:
    bool ShouldTrimArray(const arrow::ArrayData& data) const {
        if (!MinFillPercentage_) {
            return false;
        }

        const TOffset* offsetData = data.GetValues<TOffset>(1);
        return offsetData[data.length] - offsetData[0] <= data.buffers[2]->capacity() * *MinFillPercentage_ / 100;
    }

    void StoreTrimmedArray(const arrow::ArrayData& data, TChunkedBuffer& dst) const {
        const int64_t desiredOffset = data.offset % 8;
        const int64_t offsetsLength = data.length + 1 + desiredOffset;
        const int64_t offsetsSize = sizeof(TOffset) * offsetsLength;

        auto trimmedOffsetBuffer = NUdf::AllocateResizableBuffer(offsetsSize, Pool_);
        ARROW_OK(trimmedOffsetBuffer->Resize(offsetsSize, false));
        TOffset* trimmedOffsetBufferData = reinterpret_cast<TOffset*>(trimmedOffsetBuffer->mutable_data());

        const TOffset* offsetData = data.GetValues<TOffset>(1) - desiredOffset;
        for (int64_t i = 0; i < offsetsLength; ++i) {
            trimmedOffsetBufferData[i] = offsetData[i] - offsetData[0];
        }

        MKQLArrowUntrack(data.buffers[1]->data(), data.buffers[1]->capacity());
        dst.Append(MakeChunkedBufferAndUntrack(std::move(trimmedOffsetBuffer), reinterpret_cast<const char*>(trimmedOffsetBufferData), offsetsSize));

        const char* mainData = reinterpret_cast<const char*>(data.buffers[2]->data() + offsetData[0]);
        dst.Append(MakeChunkedBufferAndUntrack(data.buffers[2], mainData, trimmedOffsetBufferData[offsetsLength - 1]));
    }
};

template<typename TStringType, bool Nullable>
class TStringBlockDeserializer final : public TBlockDeserializerBase {
    using TOffset = typename TStringType::offset_type;
public:
    TStringBlockDeserializer() = default;
private:
    void DoLoadMetadata(const TMetadataSource& metaSource) final {
        LoadBufferSize(metaSource, OffsetsSize_);
        LoadBufferSize(metaSource, DataSize_);
    }

    std::shared_ptr<arrow::ArrayData> DoMakeDefaultValue(const std::shared_ptr<arrow::Buffer>& nulls, i64 nullsCount, ui64 blockLen, ui64 offset) const final {
        auto offsets = MakeZeroBuffer((blockLen + 1 + offset) * sizeof(TOffset));
        auto data = MakeEmptyBuffer();
        return arrow::ArrayData::Make(ArrowType_, blockLen, { nulls, offsets, data }, nullsCount, offset);
    }

    std::shared_ptr<arrow::ArrayData> DoLoadArray(TChunkedBuffer& src, const std::shared_ptr<arrow::Buffer>& nulls, i64 nullsCount, ui64 blockLen, ui64 offset) final {
        auto offsets = LoadBuffer(src, OffsetsSize_);
        auto data = LoadBuffer(src, DataSize_);
        return arrow::ArrayData::Make(ArrowType_, blockLen, {nulls, offsets, data }, nullsCount, offset);
    }

    bool IsNullable() const final {
        return Nullable;
    }

    void DoResetMetadata() final {
        OffsetsSize_ = DataSize_ = {};
    }

    TMaybe<ui64> OffsetsSize_;
    TMaybe<ui64> DataSize_;
};

class TExtOptionalBlockSerializer final : public TBlockSerializerBase {
    using TBase = TBlockSerializerBase;

public:
    TExtOptionalBlockSerializer(std::unique_ptr<IBlockSerializer>&& inner, const TBlockSerializerParams& params)
        : TBase(params)
        , Inner_(std::move(inner))
    {
    }

private:
    size_t ArrayMetadataCount() const final {
        return 2 + Inner_->ArrayMetadataCount();
    }

    void StoreMetadata(const arrow::ArrayData& data, const IBlockSerializer::TMetadataSink& metaSink) const final {
        StoreNullsSizes(data, metaSink);
        if (data.GetNullCount() == data.length) {
            auto innerCount = Inner_->ArrayMetadataCount();
            for (size_t i = 0; i < innerCount; ++i) {
                metaSink(0);
            }
        } else {
            Inner_->StoreMetadata(*data.child_data[0], metaSink);
        }
    }

    void StoreArray(const arrow::ArrayData& data, TChunkedBuffer& dst) const final {
        StoreNulls(data, dst);
        if (data.GetNullCount() != data.length) {
            Inner_->StoreArray(*data.child_data[0], dst);
        }
    }

    const std::unique_ptr<IBlockSerializer> Inner_;
};

class TExtOptionalBlockDeserializer final : public TBlockDeserializerBase {
public:
    explicit TExtOptionalBlockDeserializer(std::unique_ptr<TBlockDeserializerBase>&& inner)
        : Inner_(std::move(inner))
    {
    }
private:
    void DoLoadMetadata(const TMetadataSource& metaSource) final {
        Inner_->LoadMetadata(metaSource);
    }

    std::shared_ptr<arrow::ArrayData> DoMakeDefaultValue(const std::shared_ptr<arrow::Buffer>& nulls, i64 nullsCount, ui64 blockLen, ui64 offset) const final {
        return arrow::ArrayData::Make(ArrowType_, blockLen, {nulls}, { Inner_->MakeDefaultValue(blockLen, offset) }, nullsCount, offset);
    }

    std::shared_ptr<arrow::ArrayData> DoLoadArray(TChunkedBuffer& src, const std::shared_ptr<arrow::Buffer>& nulls, i64 nullsCount, ui64 blockLen, ui64 offset) final {
        return arrow::ArrayData::Make(ArrowType_, blockLen, {nulls}, { Inner_->LoadArray(src, blockLen, offset) }, nullsCount, offset);
    }

    bool IsNullable() const final {
        return true;
    }

    void DoResetMetadata() final {
        Inner_->ResetMetadata();
    }

    void SetArrowType(const std::shared_ptr<arrow::DataType>& type) final {
        ArrowType_ = type;
        YQL_ENSURE(type->fields().size() == 1);
        Inner_->SetArrowType(type->fields().front()->type());
    }

    const std::unique_ptr<TBlockDeserializerBase> Inner_;
};

class TSingularTypeBlockSerializer final : public TBlockSerializerBase {
    using TBase = TBlockSerializerBase;

public:
    using TBase::TBase;

private:
    size_t ArrayMetadataCount() const final {
        return 0;
    }

    void StoreMetadata(const arrow::ArrayData& data, const IBlockSerializer::TMetadataSink& metaSink) const final {
        Y_UNUSED(data, metaSink);
    }

    void StoreArray(const arrow::ArrayData& data, TChunkedBuffer& dst) const final {
        Y_UNUSED(data, dst);
    }
};

class TSingularTypeBlockDeserializer final: public TBlockDeserializerBase {
private:
    void DoLoadMetadata(const TMetadataSource& metaSource) final {
        Y_UNUSED(metaSource);
    }

    std::shared_ptr<arrow::ArrayData> DoMakeDefaultValue(const std::shared_ptr<arrow::Buffer>& nulls, i64 nullsCount, ui64 blockLen, ui64 offset) const final {
        Y_UNUSED(offset);
        Y_ENSURE(nullsCount == 0);
        Y_ENSURE(!nulls || nulls->size() == 0);
        return arrow::NullArray(blockLen).data();
    }

    std::shared_ptr<arrow::ArrayData> DoLoadArray(TChunkedBuffer& src, const std::shared_ptr<arrow::Buffer>& nulls, i64 nullsCount, ui64 blockLen, ui64 offset) final {
        Y_UNUSED(offset, src);
        Y_ENSURE(nullsCount == 0);
        Y_ENSURE(!nulls || nulls->size() == 0);
        return arrow::NullArray(blockLen).data();
    }

    bool IsNullable() const final {
        return false;
    }

    void DoResetMetadata() final {
    }
};

template<bool Nullable, typename TDerived>
class TTupleBlockSerializerBase : public TBlockSerializerBase {
    using TBase = TBlockSerializerBase;

public:
    using TBase::TBase;

private:
    size_t ArrayMetadataCount() const final {
        size_t result = static_cast<const TDerived*>(this)->GetChildrenMetaCount();
        if constexpr (Nullable) {
            result += 2;
        }
        return result;
    }

    void StoreMetadata(const arrow::ArrayData& data, const IBlockSerializer::TMetadataSink& metaSink) const final {
        if constexpr (Nullable) {
            StoreNullsSizes(data, metaSink);
        }
        if (data.GetNullCount() == data.length) {
            auto childCount = static_cast<const TDerived*>(this)->GetChildrenMetaCount();
            for (size_t i = 0; i < childCount; ++i) {
                metaSink(0);
            }
        } else {
            static_cast<const TDerived*>(this)->StoreChildrenMetadata(data.child_data, metaSink);
        }
    }

    void StoreArray(const arrow::ArrayData& data, TChunkedBuffer& dst) const final {
        if constexpr (Nullable) {
            StoreNulls(data, dst);
        }
        if (data.GetNullCount() != data.length) {
            static_cast<const TDerived*>(this)->StoreChildrenArrays(data.child_data, dst);
        }
    }
};

template<bool Nullable>
class TTupleBlockSerializer final : public TTupleBlockSerializerBase<Nullable, TTupleBlockSerializer<Nullable>> {
    using TBase = TTupleBlockSerializerBase<Nullable, TTupleBlockSerializer<Nullable>>;

public:
    TTupleBlockSerializer(TVector<std::unique_ptr<IBlockSerializer>>&& children, const TBlockSerializerParams& params)
        : TBase(params)
        , Children_(std::move(children))
    {}

    size_t GetChildrenMetaCount() const {
        size_t result = 0;
        for (const auto& child : Children_) {
            result += child->ArrayMetadataCount();
        }
        return result;
    }

    void StoreChildrenMetadata(const std::vector<std::shared_ptr<arrow::ArrayData>>& child_data,
        const IBlockSerializer::TMetadataSink& metaSink) const {

        for (size_t i = 0; i < Children_.size(); ++i) {
            Children_[i]->StoreMetadata(*child_data[i], metaSink);
        }
    }

    void StoreChildrenArrays(const std::vector<std::shared_ptr<arrow::ArrayData>>& child_data, TChunkedBuffer& dst) const {
        for (size_t i = 0; i < Children_.size(); ++i) {
            Children_[i]->StoreArray(*child_data[i], dst);
        }
    }

private:
    const TVector<std::unique_ptr<IBlockSerializer>> Children_;
};

template<typename TDate, bool Nullable>
class TTzDateBlockSerializer final : public TTupleBlockSerializerBase<Nullable, TTzDateBlockSerializer<TDate, Nullable>> {
    using TBase = TTupleBlockSerializerBase<Nullable, TTzDateBlockSerializer<TDate, Nullable>>;

public:
    explicit TTzDateBlockSerializer(const TBlockSerializerParams& params)
        : TBase(params)
        , DateSerialiser_(params)
        , TzSerialiser_(params)
    {}

    size_t GetChildrenMetaCount() const {
        return DateSerialiser_.ArrayMetadataCount() + TzSerialiser_.ArrayMetadataCount();
    }

    void StoreChildrenMetadata(const std::vector<std::shared_ptr<arrow::ArrayData>>& child_data,
        const IBlockSerializer::TMetadataSink& metaSink) const {
        DateSerialiser_.StoreMetadata(*child_data[0], metaSink);
        TzSerialiser_.StoreMetadata(*child_data[1], metaSink);
    }

    void StoreChildrenArrays(const std::vector<std::shared_ptr<arrow::ArrayData>>& child_data, TChunkedBuffer& dst) const {
        DateSerialiser_.StoreArray(*child_data[0], dst);
        TzSerialiser_.StoreArray(*child_data[1], dst);
    }

private:
    using TDateLayout = typename NUdf::TDataType<TDate>::TLayout;

    TFixedSizeBlockSerializer<sizeof(TDateLayout), false> DateSerialiser_;
    TFixedSizeBlockSerializer<sizeof(NYql::NUdf::TTimezoneId), false> TzSerialiser_;
};

template<bool Nullable>
class TTupleBlockDeserializer final : public TBlockDeserializerBase {
public:
    explicit TTupleBlockDeserializer(TVector<std::unique_ptr<TBlockDeserializerBase>>&& children)
        : Children_(std::move(children))
    {
    }
private:
    void DoLoadMetadata(const TMetadataSource& metaSource) final {
        for (auto& child : Children_) {
            child->LoadMetadata(metaSource);
        }
    }

    std::shared_ptr<arrow::ArrayData> DoMakeDefaultValue(const std::shared_ptr<arrow::Buffer>& nulls, i64 nullsCount, ui64 blockLen, ui64 offset) const final {
        std::vector<std::shared_ptr<arrow::ArrayData>> childData;
        for (auto& child : Children_) {
            childData.emplace_back(child->MakeDefaultValue(blockLen, offset));
        }
        return arrow::ArrayData::Make(ArrowType_, blockLen, {nulls}, std::move(childData), nullsCount, offset);
    }

    std::shared_ptr<arrow::ArrayData> DoLoadArray(TChunkedBuffer& src, const std::shared_ptr<arrow::Buffer>& nulls, i64 nullsCount, ui64 blockLen, ui64 offset) final {
        std::vector<std::shared_ptr<arrow::ArrayData>> childData;
        for (auto& child : Children_) {
            childData.emplace_back(child->LoadArray(src, blockLen, offset));
        }
        return arrow::ArrayData::Make(ArrowType_, blockLen, {nulls}, std::move(childData), nullsCount, offset);
    }

    void DoResetMetadata() final {
        for (auto& child : Children_) {
            child->ResetMetadata();
        }
    }

    bool IsNullable() const final {
        return Nullable;
    }

    void SetArrowType(const std::shared_ptr<arrow::DataType>& type) final {
        ArrowType_ = type;
        YQL_ENSURE(type->fields().size() == Children_.size());
        for (size_t i = 0; i < Children_.size(); ++i) {
            Children_[i]->SetArrowType(type->field(i)->type());
        }
    }

    const TVector<std::unique_ptr<TBlockDeserializerBase>> Children_;
};

template<typename TDate, bool Nullable>
class TTzDateBlockDeserializer final : public TBlockDeserializerBase {
public:
    TTzDateBlockDeserializer() = default;

private:
    void DoLoadMetadata(const TMetadataSource& metaSource) final {
        DateDeserialiser_.LoadMetadata(metaSource);
        TzDeserialiser_.LoadMetadata(metaSource);
    }

    std::shared_ptr<arrow::ArrayData> DoMakeDefaultValue(const std::shared_ptr<arrow::Buffer>& nulls, i64 nullsCount, ui64 blockLen, ui64 offset) const final {
        std::vector<std::shared_ptr<arrow::ArrayData>> childData;
        childData.emplace_back(DateDeserialiser_.MakeDefaultValue(blockLen, offset));
        childData.emplace_back(TzDeserialiser_.MakeDefaultValue(blockLen, offset));
        return arrow::ArrayData::Make(ArrowType_, blockLen, {nulls}, std::move(childData), nullsCount, offset);
    }

    std::shared_ptr<arrow::ArrayData> DoLoadArray(TChunkedBuffer& src, const std::shared_ptr<arrow::Buffer>& nulls, i64 nullsCount, ui64 blockLen, ui64 offset) final {
        std::vector<std::shared_ptr<arrow::ArrayData>> childData;
        childData.emplace_back(DateDeserialiser_.LoadArray(src, blockLen, offset));
        childData.emplace_back(TzDeserialiser_.LoadArray(src, blockLen, offset));
        return arrow::ArrayData::Make(ArrowType_, blockLen, {nulls}, std::move(childData), nullsCount, offset);
    }

    void DoResetMetadata() final {
        DateDeserialiser_.ResetMetadata();
        TzDeserialiser_.ResetMetadata();
    }

    bool IsNullable() const final {
        return Nullable;
    }

    void SetArrowType(const std::shared_ptr<arrow::DataType>& type) final {
        YQL_ENSURE(type->fields().size() == 2);
        ArrowType_ = type;
        DateDeserialiser_.SetArrowType(type->field(0)->type());
        TzDeserialiser_.SetArrowType(type->field(1)->type());
    }

    using TDateLayout = typename NUdf::TDataType<TDate>::TLayout;

    TFixedSizeBlockDeserializer<sizeof(TDateLayout), false> DateDeserialiser_;
    TFixedSizeBlockDeserializer<sizeof(NYql::NUdf::TTimezoneId), false> TzDeserialiser_;
};

struct TSerializerTraits {
    using TResult = IBlockSerializer;
    template <bool Nullable>
    using TTuple = TTupleBlockSerializer<Nullable>;
    template <typename T, bool Nullable>
    using TFixedSize = TFixedSizeBlockSerializer<sizeof(T), Nullable>;
    template <typename TStringType, bool Nullable, NUdf::EDataSlot TOriginal = NUdf::EDataSlot::String>
    using TStrings = TStringBlockSerializer<TStringType, Nullable>;
    using TExtOptional = TExtOptionalBlockSerializer;
    template<typename TTzDateType, bool Nullable>
    using TTzDate = TTzDateBlockSerializer<TTzDateType, Nullable>;
    using TSingularType = TSingularTypeBlockSerializer;
    constexpr static bool PassType = false;

    static std::unique_ptr<TResult> MakePg(const NUdf::TPgTypeDescription& desc, const NUdf::IPgBuilder* pgBuilder, const TBlockSerializerParams& params) {
        Y_UNUSED(pgBuilder);
        if (desc.PassByValue) {
            return std::make_unique<TFixedSize<ui64, true>>(params);
        }
        return std::make_unique<TStrings<arrow::BinaryType, true>>(params);
    }

    static std::unique_ptr<TResult> MakeResource(bool isOptional, const TBlockSerializerParams& params) {
        Y_UNUSED(isOptional, params);
        ythrow yexception() << "Serializer not implemented for block resources";
    }

    static std::unique_ptr<TResult> MakeSingular(const TBlockSerializerParams& params) {
        return std::make_unique<TSingularType>(params);
    }

    template<typename TTzDateType>
    static std::unique_ptr<TResult> MakeTzDate(bool isOptional, const TBlockSerializerParams& params) {
        if (isOptional) {
            return std::make_unique<TTzDate<TTzDateType, true>>(params);
        } else {
            return std::make_unique<TTzDate<TTzDateType, false>>(params);
        }
    }
};

struct TDeserializerTraits {
    using TResult = TBlockDeserializerBase;
    template <bool Nullable>
    using TTuple = TTupleBlockDeserializer<Nullable>;
    template <typename T, bool Nullable>
    using TFixedSize = TFixedSizeBlockDeserializer<sizeof(T), Nullable>;
    template <typename TStringType, bool Nullable, NUdf::EDataSlot TOriginal = NUdf::EDataSlot::String>
    using TStrings = TStringBlockDeserializer<TStringType, Nullable>;
    using TExtOptional = TExtOptionalBlockDeserializer;
    template<typename TTzDateType, bool Nullable>
    using TTzDate = TTzDateBlockDeserializer<TTzDateType, Nullable>;
    using TSingularType = TSingularTypeBlockDeserializer;

    constexpr static bool PassType = false;

    static std::unique_ptr<TResult> MakePg(const NUdf::TPgTypeDescription& desc, const NUdf::IPgBuilder* pgBuilder) {
        Y_UNUSED(pgBuilder);
        if (desc.PassByValue) {
            return std::make_unique<TFixedSize<ui64, true>>();
        }
        return std::make_unique<TStrings<arrow::BinaryType, true>>();
    }

    static std::unique_ptr<TResult> MakeResource(bool isOptional) {
        Y_UNUSED(isOptional);
        ythrow yexception() << "Deserializer not implemented for block resources";
    }

    static std::unique_ptr<TResult> MakeSingular() {
        return std::make_unique<TSingularType>();
    }

    template<typename TTzDateType>
    static std::unique_ptr<TResult> MakeTzDate(bool isOptional) {
        if (isOptional) {
            return std::make_unique<TTzDate<TTzDateType, true>>();
        }
        else {
            return std::make_unique<TTzDate<TTzDateType, false>>();
        }
    }
};

} // namespace


std::unique_ptr<IBlockSerializer> MakeBlockSerializer(const NYql::NUdf::ITypeInfoHelper& typeInfoHelper, const NYql::NUdf::TType* type, const TBlockSerializerParams& params) {
    return NYql::NUdf::DispatchByArrowTraits<TSerializerTraits>(typeInfoHelper, type, nullptr, params);
}

std::unique_ptr<IBlockDeserializer> MakeBlockDeserializer(const NYql::NUdf::ITypeInfoHelper& typeInfoHelper, const NYql::NUdf::TType* type) {
    std::unique_ptr<TBlockDeserializerBase> result =  NYql::NUdf::DispatchByArrowTraits<TDeserializerTraits>(typeInfoHelper, type, nullptr);
    result->SetArrowType(NYql::NUdf::GetArrowType(typeInfoHelper, type));
    return std::move(result);
}


} // namespace NKikimr::NMiniKQL
