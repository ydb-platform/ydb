// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#include "factory.h"

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>
#include <util/stream/str.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NKikHouse {
namespace NSerialization {

class IColumn;
using TMutableColumnPtr = TIntrusivePtr<IColumn>;
using TColumnPtr = TIntrusiveConstPtr<IColumn>;

class IDataType;
using TDataTypePtr = TIntrusiveConstPtr<IDataType>;

// Generic data type interface
class IDataType : public TThrRefBase {
public:
    virtual ~IDataType() = default;
    virtual const TString& getName() const = 0;
    virtual TMutableColumnPtr createColumn() const = 0;

    // Converts 'const this' into TIntrusiveConstPtr
    TDataTypePtr getPtr() const {
        return TDataTypePtr(const_cast<IDataType*>(this));
    }
};

// Generic column interface
class IColumn : public TThrRefBase {
public:
    virtual ~IColumn() = default;
    virtual const TDataTypePtr& getType() const = 0;
    virtual void insertData(const char* buf, size_t sz) = 0;
    virtual void insertDefault() = 0;
    virtual size_t rows() const = 0;
    virtual size_t byteSize() const = 0;
    // Does binary serialization
    virtual void serialize(IOutputStream& out) const = 0;
};

struct TTypeAndName {
    TDataTypePtr Type;
    TString Name;
};

using TTypesAndNames = TVector<TTypeAndName>;

// A block of data with several named columns that have the same number of rows
class TBlock {
    TTypesAndNames TypesAndNames;
    TVector<TMutableColumnPtr> Data;
public:
    TBlock() = default;

    TBlock(TTypesAndNames&& typesAndNames)
        : TypesAndNames(std::move(typesAndNames))
    {
        for (const auto& tn : TypesAndNames) {
            Data.push_back(tn.Type->createColumn());
        }
    }

    TBlock cloneEmpty() const {
        return TBlock(TTypesAndNames(TypesAndNames));
    }

    size_t columns() const {
        return TypesAndNames.size();
    }

    size_t rows() const {
        return Data.empty() ? 0 : Data.front()->rows();
    }

    const TDataTypePtr& getType(size_t i) const {
        return TypesAndNames.at(i).Type;
    }

    const TString& getName(size_t i) const {
        return TypesAndNames.at(i).Name;
    }

    TMutableColumnPtr getMutableColumn(size_t i) {
        return Data.at(i);
    }

    TColumnPtr getColumn(size_t i) const {
        return Data.at(i);
    }

    // Check that all columns have the same number of rows
    void checkNumberOfRows() const {
        if (Data.empty())
            return;

        size_t expectedRowCount = Data[0]->rows();
        for (size_t i = 1; i < Data.size(); ++i) {
            if (Data[i]->rows() != expectedRowCount) {
                throw yexception() << "Column '" << getName(i) << "' in the block has different number of rows: "
                    << Data[i]->rows() << " expected: " << expectedRowCount;
            }
        }
    }

    // Writes some metadata before the block contents
    void serializeBlockHeader(IOutputStream& out) const;
};


inline void writeVarUInt(ui64 x, IOutputStream& out) {
    for (size_t i = 0; i < 9; ++i) {
        ui8 byte = x & 0x7F;
        if (x > 0x7F)
            byte |= 0x80;

        out.Write(byte);

        x >>= 7;
        if (!x)
            return;
    }
}

inline void writeStringBinary(const TStringBuf& s, IOutputStream& out) {
    writeVarUInt(s.size(), out);
    out.Write(s);
}

template <typename T>
inline void writePODBinary(const T & x, IOutputStream& out) {
    out.Write(reinterpret_cast<const char *>(&x), sizeof(x));
}


void TBlock::serializeBlockHeader(IOutputStream& out) const {
#define APPLY_FOR_BLOCK_INFO_FIELDS(M) \
    M(bool,     is_overflows,     false,     1) \
    M(i32,      bucket_num,     -1,     2)

#define DECLARE_FIELD(TYPE, NAME, DEFAULT, FIELD_NUM) \
    TYPE NAME = DEFAULT;

    APPLY_FOR_BLOCK_INFO_FIELDS(DECLARE_FIELD)
#undef DECLARE_FIELD

    /// Set of pairs `FIELD_NUM`, value in binary form. Then 0.
#define WRITE_FIELD(TYPE, NAME, DEFAULT, FIELD_NUM) \
    writeVarUInt(FIELD_NUM, out); \
    writePODBinary(NAME, out);

    APPLY_FOR_BLOCK_INFO_FIELDS(WRITE_FIELD)
#undef WRITE_FIELD

    writeVarUInt(0, out);
}


// Writes blocks into an output stream
class TBlockWriter {
    ui32 ClientRevision = 0;
    IOutputStream& Out;
public:
    TBlockWriter(IOutputStream& out, ui32 clientRevision)
        : ClientRevision(clientRevision)
        , Out(out)
    {}

    void Write(const TBlock& block) {
        // Additional information about the block.
        if (ClientRevision > 0)
            block.serializeBlockHeader(Out);

        block.checkNumberOfRows();

        // Dimensions
        size_t columns = block.columns();
        size_t rows = block.rows();

        writeVarUInt(columns, Out);
        writeVarUInt(rows, Out);

        for (size_t i = 0; i < columns; ++i) {
            TString columnName = block.getName(i);
            writeStringBinary(columnName, Out);

            TDataTypePtr type = block.getType(i);
            TString typeName = type->getName();
            writeStringBinary(typeName, Out);

            // Data
            if (rows) {
                // Zero items of data is always represented as zero number of bytes.
                TColumnPtr column = block.getColumn(i);
                writeData(column, Out);
            }
        }
    }

private:
    void writeData(const TColumnPtr & column, IOutputStream& out) {
        column->serialize(out);
    }
};


class TDataTypeBase : public IDataType {
    TString Name;
protected:
    explicit TDataTypeBase(const TString& name)
        : Name(name)
    {}
public:
    const TString& getName() const override {
        return Name;
    }
};


class TColumnBase : public IColumn {
    TDataTypePtr Type;
protected:
    explicit TColumnBase(TDataTypePtr dataType)
        : Type(dataType)
    {}
public:
    const TDataTypePtr& getType() const override {
        return Type;
    }
};


class TNullableColumn : public TColumnBase {
    TVector<char> Nulls;
    TMutableColumnPtr Values;
public:
    TNullableColumn(TDataTypePtr dataType);

    void insertData(const char* buf, size_t sz) override {
        Nulls.push_back(0);
        Values->insertData(buf, sz);
    }

    void insertDefault() override {
        Nulls.push_back(1);
        Values->insertDefault();
    }

    size_t rows() const override {
        return Nulls.size();
    }

    size_t byteSize() const override {
        return Nulls.size() + Values->byteSize();
    }

    void serialize(IOutputStream& out) const override {
        out.Write(Nulls.data(), Nulls.size());
        Values->serialize(out);
    }
};


class TFixedSizeColumn : public TColumnBase {
    TVector<char> Data;
    const size_t ElementSize;
public:
    TFixedSizeColumn(TDataTypePtr dataType, size_t elementSize)
        : TColumnBase(dataType)
        , ElementSize(elementSize)
    {}

    void insertData(const char* buf, size_t sz) override {
        if (sz != ElementSize) {
            throw yexception() << "Data size " << sz << " doesn't match element size " << ElementSize;
        }
        Data.insert(Data.end(), buf, buf + sz);
    }

    void insertDefault() override {
        Data.resize(Data.size() + ElementSize);
    }

    size_t rows() const override {
        return Data.size() / ElementSize;
    }

    size_t byteSize() const override {
        return Data.size();
    }

    void serialize(IOutputStream& out) const override {
        out.Write(Data.data(), Data.size());
    }
};


class TStringColumn : public TColumnBase {
    using TOffset = size_t;
    TVector<char> Data;         // The buffer to store all strings data
    TVector<TOffset> Offsets;   // Offsets in the buffer (i-th offset points to the end of i-th string)
public:
    TStringColumn(TDataTypePtr dataType)
        : TColumnBase(dataType)
    {}

    void insertData(const char* buf, size_t sz) override {
        Data.insert(Data.end(), buf, buf + sz);
        Data.push_back(0); // Always append '\0' at the end
        Offsets.push_back(Data.size());
    }

    void insertDefault() override {
        Data.push_back(0);
        Offsets.push_back(Data.size());
    }

    size_t rows() const override {
        return Offsets.size();
    }

    size_t byteSize() const override {
        return Data.size() + Offsets.size()*sizeof(TOffset);
    }

    void serialize(IOutputStream& out) const override {
        if (rows() == 0)
            return;

        size_t size = Offsets[0] - 1;
        writeVarUInt(size, out);
        out.Write(Data.data(), size);

        for (size_t i = 1; i < Offsets.size(); ++i) {
            size_t size = Offsets[i] - 1 - Offsets[i-1];
            writeVarUInt(size, out);
            out.Write(Data.data() + Offsets[i-1], size);
        }
    }
};


class TNullableDataType : public TDataTypeBase {
    const TDataTypePtr Nested;
public:
    explicit TNullableDataType(const TDataTypePtr& nested)
        : TDataTypeBase("Nullable(" + nested->getName() + ")")
        , Nested(nested)
    {}

    TMutableColumnPtr createColumn() const override {
        return new TNullableColumn(this->getPtr());
    }

    TDataTypePtr getNested() const {
        return Nested;
    }
};


template<class TElement>
class TFixedSizeDataType : public TDataTypeBase {
public:
    explicit TFixedSizeDataType(const TString& name)
        : TDataTypeBase(name)
    {}

    TMutableColumnPtr createColumn() const override {
        return new TFixedSizeColumn(this->getPtr(), sizeof(TElement));
    }
};


class TStringDataType : public TDataTypeBase {
public:
    TStringDataType()
        : TDataTypeBase("String")
    {}

    TMutableColumnPtr createColumn() const override {
        return new TStringColumn(this->getPtr());
    }
};


TNullableColumn::TNullableColumn(TDataTypePtr dataType)
    : TColumnBase(dataType)
    , Nulls()
    , Values()
{
    const TNullableDataType* nullableType = dynamic_cast<const TNullableDataType*>(dataType.Get());
    Values = nullableType->getNested()->createColumn();
}


using namespace NKikimr;

// Returns specific data types by their names
class TDataTypeRegistry : public TThrRefBase {
    THashMap<TString, TDataTypePtr> Types;

private:
    void Register(TDataTypePtr dataType) {
        Types[dataType->getName()] = dataType;
    }

public:
    TDataTypeRegistry() {
        Register(new TStringDataType());

        Register(new TFixedSizeDataType<i8>("Int8"));
        Register(new TFixedSizeDataType<i16>("Int16"));
        Register(new TFixedSizeDataType<i32>("Int32"));
        Register(new TFixedSizeDataType<i64>("Int64"));

        Register(new TFixedSizeDataType<ui8>("UInt8"));
        Register(new TFixedSizeDataType<ui16>("UInt16"));
        Register(new TFixedSizeDataType<ui32>("UInt32"));
        Register(new TFixedSizeDataType<ui64>("UInt64"));

        Register(new TFixedSizeDataType<float>("Float32"));
        Register(new TFixedSizeDataType<double>("Float64"));

        Register(new TFixedSizeDataType<ui16>("Date"));
        Register(new TFixedSizeDataType<ui32>("DateTime"));

        Register(new TFixedSizeDataType<NYql::NDecimal::TInt128>("Decimal(22,9)"));
    }

    TDataTypePtr Get(TStringBuf name) const {
        return Types.at(name);
    }

    TDataTypePtr GetByYdbType(NScheme::TTypeInfo type) const {

    #define CONVERT(ydbType, chType) \
        case NScheme::NTypeIds::ydbType: \
            return Get(#chType);

        switch (type.GetTypeId()) {
        CONVERT(Bool,   UInt8);

        CONVERT(Int8,   Int8);
        CONVERT(Int16,  Int16);
        CONVERT(Int32,  Int32);
        CONVERT(Int64,  Int64);

        CONVERT(Uint8,  UInt8);
        CONVERT(Uint16, UInt16);
        CONVERT(Uint32, UInt32);
        CONVERT(Uint64, UInt64);

        CONVERT(Float,  Float32);
        CONVERT(Double, Float64);

        CONVERT(String, String);
        CONVERT(Utf8,   String);
        CONVERT(Json,   String);
        CONVERT(Yson,   String);

        CONVERT(Date,       Date);
        CONVERT(Datetime,   DateTime);
        CONVERT(Timestamp,  UInt64);
        CONVERT(Interval,   Int64);

        CONVERT(Decimal, Decimal(22,9));

        // Some internal types
        CONVERT(PairUi64Ui64,   String);
        CONVERT(ActorId,        String);
        CONVERT(StepOrderId,    String);

        case NScheme::NTypeIds::Pg:
            // TODO: support pg types
            throw yexception() << "Unsupported pg type";

        default:
            throw yexception() << "Unsupported type: " << type.GetTypeId();
        }
    #undef CONVERT
    }
};

using TDataTypeRegistryPtr = TIntrusiveConstPtr<TDataTypeRegistry>;

void AddNull(const TMutableColumnPtr& column) {
    // Default value is NULL for Nullable column
    column->insertDefault();
}

constexpr NYql::NDecimal::TInt128 Decimal128Min = NYql::NDecimal::GetBounds<38, true, true>().first;
constexpr NYql::NDecimal::TInt128 Decimal128Max = NYql::NDecimal::GetBounds<38, true, true>().second;

void AddDecimal(const TMutableColumnPtr& column, const TCell& cell) {
    struct THalves {
        ui64 lo;
        ui64 hi;
    };

    if (cell.Size() != sizeof(THalves)) {
        AddNull(column);
        return;
    }

    const THalves halves = cell.AsValue<THalves>();
    const NYql::NDecimal::TInt128 val = NYql::NDecimal::FromHalfs(halves.lo, halves.hi);

    // Return MAX Decimal128 instead of +inf and MIN Decimal128 instead of -inf
    if (val == NYql::NDecimal::Inf()) {
        auto infVal = NYql::NDecimal::MakePair(Decimal128Max);
        column->insertData((const char*)&infVal, sizeof(infVal));
        return;
    }

    if (val == -NYql::NDecimal::Inf()) {
        auto minusInfVal = NYql::NDecimal::MakePair(Decimal128Min);
        column->insertData((const char*)&minusInfVal, sizeof(minusInfVal));
        return;
    }

    if (NYql::NDecimal::IsNormal(val)) {
        column->insertData(cell.Data(), cell.Size());
        return;
    } else {
        // Convert all non-numbers to NULLs
        AddNull(column);
        return;
    }
}

size_t AddValue(const TMutableColumnPtr& column, const TCell& cell, NScheme::TTypeInfo type) {
    size_t prevBytes = column->byteSize();
    if (cell.IsNull()) {
        AddNull(column);
    } else {
        auto typeId = type.GetTypeId();
        if (typeId == NScheme::NTypeIds::Pg) {
            // TODO: support pg types
            Y_ABORT_UNLESS(false, "pg types are not supported");
        } else if (typeId == NScheme::NTypeIds::Decimal) {
            AddDecimal(column, cell);
        } else {
            column->insertData(cell.Data(), cell.Size());
        }
    }
    return column->byteSize() - prevBytes;
}

TTypesAndNames MakeColumns(const TDataTypeRegistryPtr& dataTypeRegistry, const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns) {
    TTypesAndNames res;
    for (auto& c : columns) {
        TDataTypePtr dataType = dataTypeRegistry->GetByYdbType(c.second);
        dataType = new TNullableDataType(dataType);
        res.push_back({dataType, c.first});
    }
    return res;
}

} // namespace NSerialization

using namespace NSerialization;

// Saves rows in ClickHouse native format so that they can be sent to CH
// and processed there without further conversions
class TBlockBuilder : public NKikimr::IBlockBuilder {
public:
    explicit TBlockBuilder(TDataTypeRegistryPtr dataTypeRegistry);
    ~TBlockBuilder();
    bool Start(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns, ui64 maxRowsInBlock, ui64 maxBytesInBlock, TString& err) override;
    void AddRow(const NKikimr::TDbTupleRef& key, const NKikimr::TDbTupleRef& value) override;
    TString Finish() override;
    size_t Bytes() const override;

private:
    std::unique_ptr<IBlockBuilder> Clone() const override;

private:
    const TDataTypeRegistryPtr DataTypeRegistry;

    class TImpl;
    TAutoPtr<TImpl> Impl;
};


class TBlockBuilder::TImpl {
    constexpr static ui32 DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD = 54408;
public:
    TImpl(const TDataTypeRegistryPtr& dataTypeRegistry, const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns, ui64 maxRowsInBlock, ui64 maxBytesInBlock)
        : MaxRowsInBlock(maxRowsInBlock)
        , MaxBytesInBlock(maxBytesInBlock)
        , BlockTemplate(MakeColumns(dataTypeRegistry, columns))
        , Out(Buffer)
        , BlockWriter(Out, DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD)
    {
        StartNewBlock();
    }

    void AddRow(const TDbTupleRef& key, const TDbTupleRef& value) {
        Y_UNUSED(key);

        if (CurrentBlockRows >= MaxRowsInBlock || CurrentBlockBytes >= MaxBytesInBlock) {
            FinishCurrentBlock();
        }

        ++CurrentBlockRows;
        for (size_t ci = 0; ci < value.ColumnCount; ++ci) {
            CurrentBlockBytes += AddValue(CurrentBlock.getMutableColumn(ci), value.Columns[ci], value.Types[ci]);
        }
    }

    TString Finish() {
        FinishCurrentBlock();
        Out.Finish();
        return Buffer;
    }

    size_t Bytes() const {
        return CurrentBlockBytes + Buffer.size();
    }

private:
    void FinishCurrentBlock() {
        if (CurrentBlockRows > 0) {
            BlockWriter.Write(CurrentBlock);

            StartNewBlock();
        }
    }

    void StartNewBlock() {
        CurrentBlockRows = 0;
        CurrentBlockBytes = 0;
        CurrentBlock = BlockTemplate.cloneEmpty();
    }

private:
    TDataTypeRegistryPtr DateTypeRegistry;
    const ui64 MaxRowsInBlock;
    const ui64 MaxBytesInBlock;
    TBlock BlockTemplate;
    size_t CurrentBlockRows;
    size_t CurrentBlockBytes;
    TBlock CurrentBlock;
    TString Buffer;
    TStringOutput Out;
    TBlockWriter BlockWriter;
};


TBlockBuilder::TBlockBuilder(TDataTypeRegistryPtr dataTypeRegistry)
    : DataTypeRegistry(dataTypeRegistry)
{}

TBlockBuilder::~TBlockBuilder() {
}

bool TBlockBuilder::Start(const std::vector<std::pair<TString,  NScheme::TTypeInfo>>& columns, ui64 maxRowsInBlock, ui64 maxBytesInBlock, TString& err) {
    try {
        Impl.Reset(new TImpl(DataTypeRegistry, columns, maxRowsInBlock, maxBytesInBlock));
    } catch (std::exception& e) {
        err = e.what();
        return false;
    }
    return true;
}

void TBlockBuilder::AddRow(const TDbTupleRef& key, const TDbTupleRef& value) {
    if (Impl)
        Impl->AddRow(key, value);
}

TString TBlockBuilder::Finish() {
    if (!Impl)
        return TString();

    return Impl->Finish();
}

size_t TBlockBuilder::Bytes() const {
    if (!Impl)
        return 0;

    return Impl->Bytes();
}

std::unique_ptr<IBlockBuilder> TBlockBuilder::Clone() const {
    return std::make_unique<TBlockBuilder>(DataTypeRegistry);
}

////////////////////////////////////////////////////////////////////////////////

void RegisterFormat(NKikimr::TFormatFactory& factory) {
    TDataTypeRegistryPtr dataTypeRegistry(new TDataTypeRegistry);
    factory.RegisterBlockBuilder(std::make_unique<TBlockBuilder>(dataTypeRegistry), "clickhouse_native");
}

} // namespace NKikHouse
