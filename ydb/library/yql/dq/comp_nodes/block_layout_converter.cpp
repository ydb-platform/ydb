
#include "block_layout_converter.h"

#include <yql/essentials/minikql/arrow/arrow_util.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/public/udf/arrow/defs.h>
#include <yql/essentials/public/udf/arrow/dispatch_traits.h>
#include <yql/essentials/public/udf/arrow/util.h>
#include <yql/essentials/public/udf/udf_type_inspection.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_value_builder.h>
#include <yql/essentials/utils/yql_panic.h>

#include <arrow/array/data.h>
#include <arrow/datum.h>

#include <util/generic/vector.h>

#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/tuple.h>

namespace NKikimr::NMiniKQL {

struct IColumnDataExtractor {
    using TPtr = std::unique_ptr<IColumnDataExtractor>;

    virtual ~IColumnDataExtractor() = default;

    virtual TVector<ui8*> GetColumnsData(std::shared_ptr<arrow::ArrayData> array) = 0;
    virtual TVector<ui8*> GetNullBitmap(std::shared_ptr<arrow::ArrayData> array) = 0;
    virtual ui32 GetElementSize() = 0;
    virtual NPackedTuple::EColumnSizeType GetElementSizeType() = 0;
    virtual std::shared_ptr<arrow::ArrayData> ReserveArray(const TVector<ui64>& bytes, ui32 len, [[maybe_unused]] bool isBitmapNull = false) = 0;
    // Ugly interface, but I dont care
    virtual void AppendInnerExtractors(std::vector<IColumnDataExtractor*>& extractors) = 0;
};

// ------------------------------------------------------------

template <typename TLayout, bool Nullable>
class TFixedSizeColumnDataExtractor : public IColumnDataExtractor {
public:
    TFixedSizeColumnDataExtractor(arrow::MemoryPool* pool, TType* type)
        : Pool_(pool)
        , Type_(type)
    {}

    TVector<ui8*> GetColumnsData(std::shared_ptr<arrow::ArrayData> array) override {
        Y_ENSURE(array->buffers.size() == 2);

        return {array->GetMutableValues<ui8>(1)};
    }

    TVector<ui8*> GetNullBitmap(std::shared_ptr<arrow::ArrayData> array) override {
        Y_ENSURE(array->buffers.size() > 0);

        return {array->GetMutableValues<ui8>(0)};
    }

    ui32 GetElementSize() override {
        return sizeof(TLayout);
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        return NPackedTuple::EColumnSizeType::Fixed;
    }

    std::shared_ptr<arrow::ArrayData> ReserveArray(const TVector<ui64>& bytes, ui32 len, [[maybe_unused]] bool isBitmapNull = false) override {
        Y_ENSURE(bytes.size() == 1);
        auto bytesCount = bytes.front();
        Y_ENSURE(bytesCount == len * GetElementSize());

        std::shared_ptr<arrow::DataType> type;
        auto isConverted = ConvertArrowType(Type_, type);
        Y_ENSURE(isConverted);

        std::shared_ptr<arrow::Buffer> nullBitmap;
        if (!isBitmapNull) {
            nullBitmap = NUdf::AllocateBitmapWithReserve(len, Pool_);
        }
        auto dataBuffer = NUdf::AllocateResizableBuffer(bytesCount, Pool_);

        return arrow::ArrayData::Make(std::move(type), len, {std::move(nullBitmap), std::move(dataBuffer)});
    }

    void AppendInnerExtractors(std::vector<IColumnDataExtractor*>& extractors) override {
        extractors.push_back(this);
    }

protected:
    arrow::MemoryPool* Pool_;
    TType* Type_;
};

template <bool Nullable>
class TResourceColumnDataExtractor : public IColumnDataExtractor {
public:
    TResourceColumnDataExtractor(arrow::MemoryPool* pool, TType* type)
        : Pool_(pool)
        , Type_(type)
    {}

    TVector<ui8*> GetColumnsData(std::shared_ptr<arrow::ArrayData> array) override {
        Y_ENSURE(array->buffers.size() == 2);
        Y_ENSURE(array->child_data.empty());

        return {array->GetMutableValues<ui8>(1)};
    }

    TVector<ui8*> GetNullBitmap(std::shared_ptr<arrow::ArrayData> array) override {
        Y_ENSURE(array->buffers.size() > 0);

        return {array->GetMutableValues<ui8>(0)};
    }

    ui32 GetElementSize() override {
        return sizeof(NUdf::TUnboxedValue);
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        return NPackedTuple::EColumnSizeType::Fixed;
    }

    std::shared_ptr<arrow::ArrayData> ReserveArray(const TVector<ui64>& bytes, ui32 len, [[maybe_unused]] bool isBitmapNull = false) override {
        Y_ENSURE(bytes.size() == 1);
        auto bytesCount = bytes.front();
        Y_ENSURE(bytesCount == len * GetElementSize());

        std::shared_ptr<arrow::DataType> type;
        auto isConverted = ConvertArrowType(Type_, type);
        Y_ENSURE(isConverted);

        std::shared_ptr<arrow::Buffer> nullBitmap;
        if (!isBitmapNull) {
            nullBitmap = NUdf::AllocateBitmapWithReserve(len, Pool_);
        }
        auto dataBuffer = NUdf::AllocateResizableBuffer<NUdf::TResizableManagedBuffer<NUdf::TUnboxedValue>>(bytesCount, Pool_);
        ARROW_OK(dataBuffer->Resize(bytesCount));

        return arrow::ArrayData::Make(std::move(type), len, {std::move(nullBitmap), std::move(dataBuffer)});
    }

    void AppendInnerExtractors(std::vector<IColumnDataExtractor*>& extractors) override {
        extractors.push_back(this);
    }

protected:
    arrow::MemoryPool* Pool_;
    TType* Type_;
};

class TSingularColumnDataExtractor : public IColumnDataExtractor {
public:
    TSingularColumnDataExtractor(arrow::MemoryPool* pool, TType* type) {
        Y_UNUSED(pool, type);
    }

    TVector<ui8*> GetColumnsData(std::shared_ptr<arrow::ArrayData> array) override {
        return {array->GetMutableValues<ui8>(0)}; // nullptr
    }

    TVector<ui8*> GetNullBitmap(std::shared_ptr<arrow::ArrayData> array) override {
        Y_UNUSED(array);
        return {nullptr};
    }

    ui32 GetElementSize() override {
        return 1; // or 0?
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        return NPackedTuple::EColumnSizeType::Fixed;
    }

    std::shared_ptr<arrow::ArrayData> ReserveArray(const TVector<ui64>& bytes, ui32 len, [[maybe_unused]] bool isBitmapNull = false) override {
        Y_UNUSED(bytes);
        return arrow::ArrayData::Make(arrow::null(), len, {nullptr}, len);
    }

    void AppendInnerExtractors(std::vector<IColumnDataExtractor*>& extractors) override {
        extractors.push_back(this);
    }
};

template <typename TStringType, bool Nullable>
class TStringColumnDataExtractor : public IColumnDataExtractor {
    using TOffset = typename TStringType::offset_type;

public:
    TStringColumnDataExtractor(arrow::MemoryPool* pool, TType* type)
        : Pool_(pool)
        , Type_(type)
    {}

    TVector<ui8*> GetColumnsData(std::shared_ptr<arrow::ArrayData> array) override {
        Y_ENSURE(array->buffers.size() == 3);
        Y_ENSURE(array->child_data.empty());

        return {array->GetMutableValues<ui8>(1), array->GetMutableValues<ui8>(2)};
    }

    TVector<ui8*> GetNullBitmap(std::shared_ptr<arrow::ArrayData> array) override {
        Y_ENSURE(array->buffers.size() > 0);

        return {array->GetMutableValues<ui8>(0), nullptr};
    }

    ui32 GetElementSize() override {
        return 16; // for now threshold for variable sized types is 16 bytes
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        return NPackedTuple::EColumnSizeType::Variable;
    }

    std::shared_ptr<arrow::ArrayData> ReserveArray(const TVector<ui64>& bytes, ui32 len, [[maybe_unused]] bool isBitmapNull = false) override {
        Y_ENSURE(bytes.size() == 1);
        auto bytesCount = bytes.front();

        std::shared_ptr<arrow::DataType> type;
        auto isConverted = ConvertArrowType(Type_, type);
        Y_ENSURE(isConverted);

        std::shared_ptr<arrow::Buffer> nullBitmap;
        if (!isBitmapNull) {
            nullBitmap = NUdf::AllocateBitmapWithReserve(len, Pool_);
        }
        auto offsetBuffer = NUdf::AllocateResizableBuffer(sizeof(TOffset) * (len + 1), Pool_);
        // zeroize offsets buffer, or your code will die
        // low-level unpack expects that first offset is set to null
        std::memset(offsetBuffer->mutable_data(), 0, sizeof(TOffset) * (len + 1));
        auto dataBuffer = NUdf::AllocateResizableBuffer(bytesCount, Pool_);

        return arrow::ArrayData::Make(std::move(type), len, {std::move(nullBitmap), std::move(offsetBuffer), std::move(dataBuffer)});
    }

    void AppendInnerExtractors(std::vector<IColumnDataExtractor*>& extractors) override {
        extractors.push_back(this);
    }

protected:
    arrow::MemoryPool* Pool_;
    TType* Type_;
};

template <bool Nullable>
class TTupleColumnDataExtractor : public IColumnDataExtractor {
public:
    TTupleColumnDataExtractor(
        std::vector<IColumnDataExtractor::TPtr> children, arrow::MemoryPool* pool, TType* type
    )
        : Children_(std::move(children))
        , Pool_(pool)
        , Type_(type)
    {}

    TVector<ui8*> GetColumnsData(std::shared_ptr<arrow::ArrayData> array) override {
        Y_ENSURE(array->buffers.size() == 1);

        TVector<ui8*> childrenData;
        Y_ENSURE(array->child_data.size() == Children_.size());

        for (size_t i = 0; i < Children_.size(); i++) {
            auto data = Children_[i]->GetColumnsData(array->child_data[i]);
            childrenData.insert(childrenData.end(), data.begin(), data.end());
        }

        return childrenData;
    }

    TVector<ui8*> GetNullBitmap(std::shared_ptr<arrow::ArrayData> array) override {
        Y_ENSURE(array->buffers.size() == 1);

        TVector<ui8*> childrenData;
        Y_ENSURE(array->child_data.size() == Children_.size());

        for (size_t i = 0; i < Children_.size(); i++) {
            auto data = Children_[i]->GetNullBitmap(array->child_data[i]);
            childrenData.insert(childrenData.end(), data.begin(), data.end());
        }

        return childrenData;
    }

    ui32 GetElementSize() override {
        THROW yexception() << "Do not call GetElementSize on tuples";
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        THROW yexception() << "Do not call GetElementSizeType on tuples";
    }

    std::shared_ptr<arrow::ArrayData> ReserveArray(const TVector<ui64>& bytes, ui32 len, [[maybe_unused]] bool isBitmapNull = false) override {
        std::shared_ptr<arrow::DataType> type;
        auto isConverted = ConvertArrowType(Type_, type);
        Y_ENSURE(isConverted);

        std::shared_ptr<arrow::Buffer> nullBitmap;
        if (!isBitmapNull) {
            nullBitmap = NUdf::AllocateBitmapWithReserve(len, Pool_);
        }
        std::vector<std::shared_ptr<arrow::ArrayData>> reservedChildren;
        for (size_t i = 0; i < Children_.size(); i++) {
            reservedChildren.push_back(Children_[i]->ReserveArray({bytes[i]}, len)); // TODO: handle recursive tuple, only one level of nesting is available now
        }

        return arrow::ArrayData::Make(std::move(type), len, {std::move(nullBitmap)}, std::move(reservedChildren));
    }

    void AppendInnerExtractors(std::vector<IColumnDataExtractor*>& extractors) override {
        for (auto& child: Children_) {
            child->AppendInnerExtractors(extractors);
        }
    }

protected:
    TTupleColumnDataExtractor() = default;

protected:
    std::vector<IColumnDataExtractor::TPtr> Children_;
    arrow::MemoryPool* Pool_;
    TType* Type_;
};

template<typename TDate, bool Nullable>
class TTzDateColumnDataExtractor : public TTupleColumnDataExtractor<Nullable> {
    using TBase = TTupleColumnDataExtractor<Nullable>;
    using TDateLayout = typename NUdf::TDataType<TDate>::TLayout;

public:
    TTzDateColumnDataExtractor(arrow::MemoryPool* pool, TType* type) {
        this->Pool_ = pool;
        this->Type_ = type;
        this->Children_.push_back(std::make_unique<TFixedSizeColumnDataExtractor<TDateLayout, false>>(pool, type));
        this->Children_.push_back(std::make_unique<TFixedSizeColumnDataExtractor<ui16, false>>(pool, type));
    }
};

// TODO: Doesn't supported yet, use nullable parameter in type instead
class TExternalOptionalColumnDataExtractor : public IColumnDataExtractor {
public:
    TExternalOptionalColumnDataExtractor(
        IColumnDataExtractor::TPtr inner, arrow::MemoryPool* pool, TType* type
    )
        : Inner_(std::move(inner))
        , Pool_(pool)
        , Type_(type)
    {}

    TVector<ui8*> GetColumnsData(std::shared_ptr<arrow::ArrayData> array) override {
        Y_ENSURE(array->buffers.size() == 1);
        Y_ENSURE(array->child_data.size() == 1);

        return Inner_->GetColumnsData(array->child_data[0]);
    }

    TVector<ui8*> GetNullBitmap(std::shared_ptr<arrow::ArrayData> array) override {
        Y_ENSURE(array->buffers.size() > 0);

        return Inner_->GetNullBitmap(array);
    }

    ui32 GetElementSize() override {
        return Inner_->GetElementSize();
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        return Inner_->GetElementSizeType();
    }

    std::shared_ptr<arrow::ArrayData> ReserveArray(const TVector<ui64>& bytes, ui32 len, [[maybe_unused]] bool isBitmapNull = false) override {
        std::shared_ptr<arrow::DataType> type;
        auto isConverted = ConvertArrowType(Type_, type);
        Y_ENSURE(isConverted);

        std::shared_ptr<arrow::Buffer> nullBitmap;
        if (!isBitmapNull) {
            nullBitmap = NUdf::AllocateBitmapWithReserve(len, Pool_);
        }
        auto reservedInner = Inner_->ReserveArray(bytes, len);

        return arrow::ArrayData::Make(std::move(type), len, {std::move(nullBitmap)}, {std::move(reservedInner)});
    }

    void AppendInnerExtractors(std::vector<IColumnDataExtractor*>& extractors) override {
        extractors.push_back(this);
    }

private:
    IColumnDataExtractor::TPtr Inner_;
    arrow::MemoryPool* Pool_;
    TType* Type_;
};

// ------------------------------------------------------------

struct TColumnDataExtractorTraits {
    using TResult = IColumnDataExtractor;
    template <bool Nullable>
    using TTuple = TTupleColumnDataExtractor<Nullable>;
    template <typename T, bool Nullable>
    using TFixedSize = TFixedSizeColumnDataExtractor<T, Nullable>;
    template <typename TStringType, bool Nullable, NKikimr::NUdf::EDataSlot>
    using TStrings = TStringColumnDataExtractor<TStringType, Nullable>;
    using TExtOptional = TExternalOptionalColumnDataExtractor;
    template<bool Nullable>
    using TResource = TResourceColumnDataExtractor<Nullable>;
    template<typename TTzDate, bool Nullable>
    using TTzDateReader = TTzDateColumnDataExtractor<TTzDate, Nullable>;
    using TSingular = TSingularColumnDataExtractor;

    constexpr static bool PassType = false;

    static TResult::TPtr MakePg(const NUdf::TPgTypeDescription& desc, const NUdf::IPgBuilder* pgBuilder, arrow::MemoryPool* pool, TType* type) {
        Y_UNUSED(pgBuilder);
        if (desc.PassByValue) {
            return std::make_unique<TFixedSize<ui64, true>>(pool, type);
        } else {
            return std::make_unique<TStrings<arrow::BinaryType, true, NKikimr::NUdf::EDataSlot::String>>(pool, type);
        }
    }

    static TResult::TPtr MakeSingular(arrow::MemoryPool* pool, TType* type) {
        return std::make_unique<TSingular>(pool, type);
    }

    static TResult::TPtr MakeResource(bool isOptional, arrow::MemoryPool* pool, TType* type) {
        if (isOptional) {
            return std::make_unique<TResource<true>>(pool, type);
        } else {
            return std::make_unique<TResource<false>>(pool, type);
        }
    }

    template<typename TTzDate>
    static TResult::TPtr MakeTzDate(bool isOptional, arrow::MemoryPool* pool, TType* type) {
        if (isOptional) {
            return std::make_unique<TTzDateReader<TTzDate, true>>(pool, type);
        } else {
            return std::make_unique<TTzDateReader<TTzDate, false>>(pool, type);
        }
    }
};

// ------------------------------------------------------------

class TBlockLayoutConverter : public IBlockLayoutConverter {
    auto GetColumns_(const TVector<arrow::Datum>& columns) {
        Y_ENSURE(columns.size() == Extractors_.size());

        std::fill(IsBitmapNull_.begin(), IsBitmapNull_.end(), false);
        TVector<const ui8*> columnsData;
        TVector<const ui8*> columnsNullBitmap;

        for (size_t i = 0; i < columns.size(); ++i) {
            const auto& column = columns[i];

            auto data = Extractors_[i]->GetColumnsData(column.array());
            columnsData.insert(columnsData.end(), data.begin(), data.end());

            auto nullBitmap = Extractors_[i]->GetNullBitmap(column.array());
            columnsNullBitmap.insert(columnsNullBitmap.end(), nullBitmap.begin(), nullBitmap.end());
            if (nullBitmap.front() == nullptr) {
                IsBitmapNull_[i] = true;
            }
        }

        return std::pair{std::move(columnsData), std::move(columnsNullBitmap)};
    }

public:
    TBlockLayoutConverter(
        TVector<IColumnDataExtractor::TPtr>&& extractors,
        const TVector<NPackedTuple::EColumnRole>& roles,
        bool rememberNullBitmaps = true // remember bitmaps which are equal to nullptr to not allocate memory for them in unpack
    )
        : Extractors_(std::move(extractors))
        , InnerMapping_(Extractors_.size())
        , RememberNullBitmaps_(rememberNullBitmaps)
        , IsBitmapNull_(Extractors_.size(), false)
    {
        Y_ENSURE(roles.size() == Extractors_.size());

        ui32 colCounter = 0;
        TVector<NPackedTuple::TColumnDesc> columnDescrs;
        for (size_t i = 0; i < Extractors_.size(); ++i) {
            auto& extractor = Extractors_[i];
            auto& mapping = InnerMapping_[i];
            auto prevSize = InnerExtractors_.size();
            extractor->AppendInnerExtractors(InnerExtractors_);

            for (size_t j = 0; j < InnerExtractors_.size() - prevSize; ++j) {
                NPackedTuple::TColumnDesc descr;
                descr.Role = roles[i];
                columnDescrs.push_back(descr);
                mapping.push_back(colCounter);
                colCounter++;
            }
        }

        for (size_t i = 0; i < columnDescrs.size(); ++i) {
            auto& descr = columnDescrs[i];
            descr.DataSize = InnerExtractors_[i]->GetElementSize();
            descr.SizeType = InnerExtractors_[i]->GetElementSizeType();
        }

        TupleLayout_ = NPackedTuple::TTupleLayout::Create(columnDescrs);
    }

    void Pack(const TVector<arrow::Datum>& columns, TPackResult& packed) override {
        auto [columnsData, columnsNullBitmap] = GetColumns_(columns);

        auto& packedTuples = packed.PackedTuples;
        auto& overflow = packed.Overflow;
        auto& nTuples = packed.NTuples;

        auto currentSize = (TupleLayout_->TotalRowSize) * nTuples;
        auto tuplesToPack = columns.front().array()->length;
        nTuples += tuplesToPack;
        auto newSize = (TupleLayout_->TotalRowSize) * nTuples;
        
        Cerr << "=== DETAILED PACK DEBUG ===" << Endl;
        Cerr << "  TotalRowSize: " << static_cast<size_t>(TupleLayout_->TotalRowSize) << Endl;
        Cerr << "  nTuples (before): " << static_cast<size_t>(nTuples - tuplesToPack) << Endl;
        Cerr << "  tuplesToPack: " << static_cast<size_t>(tuplesToPack) << Endl;
        Cerr << "  nTuples (after): " << static_cast<size_t>(nTuples) << Endl;
        Cerr << "  currentSize: " << static_cast<size_t>(currentSize) << " bytes" << Endl;
        Cerr << "  newSize: " << static_cast<size_t>(newSize) << " bytes" << Endl;
        Cerr << "  packedTuples.size() before resize: " << static_cast<size_t>(packedTuples.size()) << Endl;
        Cerr << "  packedTuples.capacity() before resize: " << static_cast<size_t>(packedTuples.capacity()) << Endl;
        Cerr << "  overflow.size(): " << static_cast<size_t>(overflow.size()) << Endl;
        Cerr << "  overflow.capacity(): " << static_cast<size_t>(overflow.capacity()) << Endl;
        Cerr << "  overflow.data(): " << static_cast<void*>(overflow.data()) << Endl;
        
        packedTuples.resize(newSize, 0);
        
        Cerr << "  packedTuples.size() after resize: " << static_cast<size_t>(packedTuples.size()) << Endl;
        Cerr << "  packedTuples.data(): " << static_cast<void*>(packedTuples.data()) << Endl;
        Cerr << "  packedTuples.data() + currentSize: " << static_cast<void*>(packedTuples.data() + currentSize) << Endl;
        Cerr << "  columnsData.size(): " << static_cast<size_t>(columnsData.size()) << Endl;
        Cerr << "  columnsNullBitmap.size(): " << static_cast<size_t>(columnsNullBitmap.size()) << Endl;
        
        // Проверка валидности указателей
        if (packedTuples.data() + currentSize + (TupleLayout_->TotalRowSize * tuplesToPack) > packedTuples.data() + packedTuples.size()) {
            Cerr << "ERROR: Pack destination pointer would exceed buffer bounds!" << Endl;
            Cerr << "  Write end would be at: " << static_cast<void*>(packedTuples.data() + currentSize + (TupleLayout_->TotalRowSize * tuplesToPack)) << Endl;
            Cerr << "  Buffer end is at: " << static_cast<void*>(packedTuples.data() + packedTuples.size()) << Endl;
            throw yexception() << "Pack buffer overflow detected";
        }
        
        Cerr << "  About to call TupleLayout_->Pack()..." << Endl;

        TupleLayout_->Pack(
            columnsData.data(), columnsNullBitmap.data(),
            packedTuples.data() + currentSize, overflow, 0, tuplesToPack);
            
        Cerr << "  TupleLayout_->Pack() completed successfully" << Endl;
        Cerr << "  overflow.size() after Pack: " << static_cast<size_t>(overflow.size()) << Endl;
        Cerr << "  overflow.data() after Pack: " << static_cast<void*>(overflow.data()) << Endl;
        Cerr << "  TupleLayout_ address: " << static_cast<void*>(TupleLayout_.get()) << Endl;
        Cerr << "=== END DETAILED PACK DEBUG ===" << Endl;
    }

    void BucketPack(const TVector<arrow::Datum>& columns, TPaddedPtr<TPackResult> packs, ui32 bucketsLogNum) override {
        auto [columnsData, columnsNullBitmap] = GetColumns_(columns);
        auto tuplesToPack = columns.front().array()->length;

        const auto reses = TPaddedPtr(&packs[0].PackedTuples, packs.Step());
        const auto overflows = TPaddedPtr(&packs[0].Overflow, packs.Step());

        TupleLayout_->BucketPack(
            columnsData.data(), columnsNullBitmap.data(),
            reses, overflows, 0, tuplesToPack, bucketsLogNum);

        for (ui32 bucket = 0; bucket < (1u << bucketsLogNum); ++bucket) {
            auto& pack = packs[bucket];
            pack.NTuples = pack.PackedTuples.size() / TupleLayout_->TotalRowSize;
        }
    }

    void Unpack(const TPackResult& packed, TVector<arrow::Datum>& columns) override {
        columns.resize(Extractors_.size());

        std::vector<ui64, TMKQLAllocator<ui64>> bytesPerColumn;
        TupleLayout_->CalculateColumnSizes(
            packed.PackedTuples.data(), packed.NTuples, bytesPerColumn);

        TVector<ui8*> columnsData;
        TVector<ui8*> columnsNullBitmap;
        for (size_t i = 0; i < columns.size(); ++i) {
            const auto& mapping = InnerMapping_[i];
            TVector<ui64> bytesCount;
            for (auto idx: mapping) {
                bytesCount.push_back(bytesPerColumn[idx]);
            }

            bool isBitmapNull = false;
            if (RememberNullBitmaps_) {
                isBitmapNull = IsBitmapNull_[i];
            }
            columns[i] = Extractors_[i]->ReserveArray(bytesCount, packed.NTuples, isBitmapNull);

            auto data = Extractors_[i]->GetColumnsData(columns[i].array());
            columnsData.insert(columnsData.end(), data.begin(), data.end());

            auto nullBitmap = Extractors_[i]->GetNullBitmap(columns[i].array());
            columnsNullBitmap.insert(columnsNullBitmap.end(), nullBitmap.begin(), nullBitmap.end());
        }

        TupleLayout_->Unpack(
            columnsData.data(), columnsNullBitmap.data(),
            packed.PackedTuples.data(), packed.Overflow, 0, packed.NTuples);
    }

    const NPackedTuple::TTupleLayout* GetTupleLayout() const override {
        return TupleLayout_.get();
    }

private:
    TVector<IColumnDataExtractor::TPtr> Extractors_;
    std::vector<IColumnDataExtractor*> InnerExtractors_;
    TVector<TVector<ui32>> InnerMapping_;
    THolder<NPackedTuple::TTupleLayout> TupleLayout_;
    bool RememberNullBitmaps_;
    TVector<bool> IsBitmapNull_;
};

// ------------------------------------------------------------

IBlockLayoutConverter::TPtr MakeBlockLayoutConverter(
    const NUdf::ITypeInfoHelper& typeInfoHelper, const TVector<TType*>& types,
    const TVector<NPackedTuple::EColumnRole>& roles, arrow::MemoryPool* pool)
{
    TVector<IColumnDataExtractor::TPtr> extractors;

    for (auto type: types) {
        extractors.emplace_back(DispatchByArrowTraits<TColumnDataExtractorTraits>(typeInfoHelper, type, nullptr, pool, type));
    }

    return std::make_unique<TBlockLayoutConverter>(std::move(extractors), roles);
}

} // namespace NKikimr::NMiniKQL

