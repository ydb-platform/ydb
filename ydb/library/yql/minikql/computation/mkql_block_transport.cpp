#include "mkql_block_transport.h"
#include "mkql_block_builder.h"

#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/public/udf/arrow/block_reader.h>
#include <ydb/library/yql/utils/rope_over_buffer.h>
#include <ydb/library/yql/utils/yql_panic.h>

namespace NKikimr::NMiniKQL {

namespace {

TRope MakeReadOnlyRopeAndUntrack(const std::shared_ptr<const arrow::Buffer>& owner, const char* data, size_t size) {
    MKQLArrowUntrack(owner->data());
    return NYql::MakeReadOnlyRope(owner, data, size);
}

class TOwnedArrowBuffer : public arrow::Buffer {
public:
    TOwnedArrowBuffer(TContiguousSpan span, const std::shared_ptr<const void>& owner)
        : arrow::Buffer(reinterpret_cast<const uint8_t*>(span.Data()), span.Size())
        , Owner_(owner)
    {
    }
private:
    const std::shared_ptr<const void> Owner_;
};

std::shared_ptr<arrow::Buffer> MakeEmptyBuffer() {
    return std::make_shared<arrow::Buffer>(nullptr, 0);
}

std::shared_ptr<arrow::Buffer> MakeZeroBuffer(size_t byteLen) {
    constexpr size_t NullWordCount = (MaxBlockSizeInBytes + sizeof(ui64) - 1) / sizeof(ui64);
    static ui64 nulls[NullWordCount] = { 0 };
    if (byteLen <= sizeof(nulls)) {
        return std::make_shared<arrow::Buffer>(reinterpret_cast<const ui8*>(nulls), byteLen);
    }

    size_t wordCount = (byteLen + sizeof(ui64) - 1) / sizeof(ui64);
    std::shared_ptr<ui64[]> buf(new ui64[wordCount]);
    std::fill(buf.get(), buf.get() + wordCount, 0);
    return std::make_shared<TOwnedArrowBuffer>(TContiguousSpan{ reinterpret_cast<const char*>(buf.get()), byteLen }, buf);
}

std::shared_ptr<arrow::Buffer> MakeZeroBitmap(size_t bitCount) {
    // align up 8 byte boundary
    size_t byteCount = ((bitCount + 63u) & ~size_t(63u)) >> 3;
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
    size_t nullBytes = (((size_t)data.length + desiredOffset + 7) & ~7ull) >> 3;
    metaSink(nullBytes);
}

void LoadNullsSizes(const IBlockDeserializer::TMetadataSource& metaSource, TMaybe<ui64>& nullsCount, TMaybe<ui64>& nullsSize) {
    YQL_ENSURE(!nullsCount.Defined() && !nullsSize.Defined(), "Attempt to load null sizes twice (most likely LoadArray() is not called)");
    nullsCount = metaSource();
    nullsSize = metaSource();
}

void StoreNulls(const arrow::ArrayData& data, TRope& dst) {
    if (!NeedStoreBitmap(data)) {
        return;
    }
    const ui64 desiredOffset = data.offset % 8;
    size_t nullBytes = (((size_t)data.length + desiredOffset + 7) & ~7ull) >> 3;
    YQL_ENSURE(desiredOffset <= (size_t)data.offset);
    YQL_ENSURE((data.offset - desiredOffset) % 8 == 0);
    const char* nulls = data.GetValues<char>(0, 0) + (data.offset - desiredOffset) / 8;
    dst.Insert(dst.End(), MakeReadOnlyRopeAndUntrack(data.buffers[0], nulls, nullBytes));
}

void LoadBufferSize(const IBlockDeserializer::TMetadataSource& metaSource, TMaybe<ui64>& result) {
    YQL_ENSURE(!result.Defined(), "Attempt to load buffer size twice (most likely LoadArray() is not called)");
    result = metaSource();
}

std::shared_ptr<arrow::Buffer> LoadBuffer(TRope& source, TMaybe<ui64> size) {
    YQL_ENSURE(size.Defined(), "Buffer size is not loaded");
    if (!*size) {
        return std::make_shared<arrow::Buffer>(nullptr, 0);
    }

    YQL_ENSURE(source.size() >= *size, "Premature end of data");
    auto owner = std::make_shared<TRope>(source.Begin(), source.Begin() + *size);
    source.EraseFront(*size);

    owner->Compact();
    return std::make_shared<TOwnedArrowBuffer>(owner->GetContiguousSpan(), owner);
}

std::shared_ptr<arrow::Buffer> LoadNullsBitmap(TRope& source, TMaybe<ui64> nullCount, TMaybe<ui64> bitmapSize) {
    YQL_ENSURE(nullCount.Defined(), "Bitmap null count is not loaded");
    YQL_ENSURE(bitmapSize.Defined(), "Bitmap size is not loaded");
    if (*nullCount == 0) {
        YQL_ENSURE(!*bitmapSize);
        return {};
    }
    YQL_ENSURE(*bitmapSize);
    return LoadBuffer(source, bitmapSize);
}

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

    virtual std::shared_ptr<arrow::ArrayData> LoadArray(TRope& src, ui64 blockLen, ui64 offset) final {
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
    virtual std::shared_ptr<arrow::ArrayData> DoLoadArray(TRope& src, const std::shared_ptr<arrow::Buffer>& nulls, i64 nullsCount, ui64 blockLen, ui64 offset) = 0;

    std::shared_ptr<arrow::DataType> ArrowType_;
    TMaybe<ui64> NullsCount_;
    TMaybe<ui64> NullsSize_;
};

template<size_t ObjectSize, bool Nullable>
class TFixedSizeBlockSerializer final : public IBlockSerializer {
public:
    TFixedSizeBlockSerializer() = default;

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

    void StoreArray(const arrow::ArrayData& data, TRope& dst) const final {
        if constexpr (Nullable) {
            StoreNulls(data, dst);
            if (data.GetNullCount() == data.length) {
                return;
            }
        }

        const ui64 desiredOffset = data.offset % 8;
        const char* buf = reinterpret_cast<const char*>(data.buffers[1]->data()) + (data.offset - desiredOffset) * ObjectSize;
        size_t dataBytes = ((size_t)data.length + desiredOffset) * ObjectSize;
        dst.Insert(dst.End(), MakeReadOnlyRopeAndUntrack(data.buffers[1], buf, dataBytes));
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

    std::shared_ptr<arrow::ArrayData> DoLoadArray(TRope& src, const std::shared_ptr<arrow::Buffer>& nulls, i64 nullsCount, ui64 blockLen, ui64 offset) final {
        auto data = LoadBuffer(src, DataSize_);
        return arrow::ArrayData::Make(ArrowType_, blockLen, {nulls, data}, nullsCount, offset);
    }

    void DoResetMetadata() final {
        DataSize_ = {};
    }

    TMaybe<ui64> DataSize_;
};


template<typename TStringType, bool Nullable>
class TStringBlockSerializer final : public IBlockSerializer {
    using TOffset = typename TStringType::offset_type;
public:
    TStringBlockSerializer() = default;
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
        metaSink(data.buffers[2]->size());
    }

    void StoreArray(const arrow::ArrayData& data, TRope& dst) const final {
        if constexpr (Nullable) {
            StoreNulls(data, dst);
            if (data.GetNullCount() == data.length) {
                return;
            }
        }

        const ui64 desiredOffset = data.offset % 8;
        const char* offsets = reinterpret_cast<const char*>(data.GetValues<TOffset>(1) - desiredOffset);
        size_t offsetsSize = ((size_t)data.length + 1 + desiredOffset) * sizeof(TOffset);
        dst.Insert(dst.End(), MakeReadOnlyRopeAndUntrack(data.buffers[1], offsets, offsetsSize));

        const char* mainData = reinterpret_cast<const char*>(data.buffers[2]->data());
        size_t mainSize = data.buffers[2]->size();
        dst.Insert(dst.End(), MakeReadOnlyRopeAndUntrack(data.buffers[2], mainData, mainSize));
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

    std::shared_ptr<arrow::ArrayData> DoLoadArray(TRope& src, const std::shared_ptr<arrow::Buffer>& nulls, i64 nullsCount, ui64 blockLen, ui64 offset) final {
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

class TExtOptionalBlockSerializer final : public IBlockSerializer {
public:
    explicit TExtOptionalBlockSerializer(std::unique_ptr<IBlockSerializer>&& inner)
        : Inner_(std::move(inner))
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

    void StoreArray(const arrow::ArrayData& data, TRope& dst) const final {
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

    std::shared_ptr<arrow::ArrayData> DoLoadArray(TRope& src, const std::shared_ptr<arrow::Buffer>& nulls, i64 nullsCount, ui64 blockLen, ui64 offset) final {
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

template<bool Nullable, typename TDerived>
class TTupleBlockSerializerBase : public IBlockSerializer {
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

    void StoreArray(const arrow::ArrayData& data, TRope& dst) const final {
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
public:
    TTupleBlockSerializer(TVector<std::unique_ptr<IBlockSerializer>>&& children)
        : Children_(std::move(children))
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

    void StoreChildrenArrays(const std::vector<std::shared_ptr<arrow::ArrayData>>& child_data, TRope& dst) const {
        for (size_t i = 0; i < Children_.size(); ++i) {
            Children_[i]->StoreArray(*child_data[i], dst);
        }
    }

private:
    const TVector<std::unique_ptr<IBlockSerializer>> Children_;
};

template<typename TDate, bool Nullable>
class TTzDateBlockSerializer final : public TTupleBlockSerializerBase<Nullable, TTzDateBlockSerializer<TDate, Nullable>> {
public:
    TTzDateBlockSerializer() = default;

    size_t GetChildrenMetaCount() const {
        return DateSerialiser_.ArrayMetadataCount() + TzSerialiser_.ArrayMetadataCount();
    }

    void StoreChildrenMetadata(const std::vector<std::shared_ptr<arrow::ArrayData>>& child_data, 
        const IBlockSerializer::TMetadataSink& metaSink) const {
        DateSerialiser_.StoreMetadata(*child_data[0], metaSink);
        TzSerialiser_.StoreMetadata(*child_data[1], metaSink);
    }

    void StoreChildrenArrays(const std::vector<std::shared_ptr<arrow::ArrayData>>& child_data, TRope& dst) const {
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

    std::shared_ptr<arrow::ArrayData> DoLoadArray(TRope& src, const std::shared_ptr<arrow::Buffer>& nulls, i64 nullsCount, ui64 blockLen, ui64 offset) final {
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

    std::shared_ptr<arrow::ArrayData> DoLoadArray(TRope& src, const std::shared_ptr<arrow::Buffer>& nulls, i64 nullsCount, ui64 blockLen, ui64 offset) final {
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


    static std::unique_ptr<TResult> MakePg(const NUdf::TPgTypeDescription& desc, const NUdf::IPgBuilder* pgBuilder) {
        Y_UNUSED(pgBuilder);
        if (desc.PassByValue) {
            return std::make_unique<TFixedSize<ui64, true>>();
        }
        return std::make_unique<TStrings<arrow::BinaryType, true>>();
    }

    static std::unique_ptr<TResult> MakeResource(bool isOptional) {
        Y_UNUSED(isOptional);
        ythrow yexception() << "Serializer not implemented for block resources";
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


std::unique_ptr<IBlockSerializer> MakeBlockSerializer(const NYql::NUdf::ITypeInfoHelper& typeInfoHelper, const NYql::NUdf::TType* type) {
    return NYql::NUdf::MakeBlockReaderImpl<TSerializerTraits>(typeInfoHelper, type, nullptr);
}

std::unique_ptr<IBlockDeserializer> MakeBlockDeserializer(const NYql::NUdf::ITypeInfoHelper& typeInfoHelper, const NYql::NUdf::TType* type) {
    std::unique_ptr<TBlockDeserializerBase> result =  NYql::NUdf::MakeBlockReaderImpl<TDeserializerTraits>(typeInfoHelper, type, nullptr);
    result->SetArrowType(NYql::NUdf::GetArrowType(typeInfoHelper, type));
    return std::move(result);
}


} // namespace NKikimr::NMiniKQL
