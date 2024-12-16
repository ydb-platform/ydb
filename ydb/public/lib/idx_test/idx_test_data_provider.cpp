#include "idx_test.h"
#include "idx_test_common.h"
#include "idx_test_data_provider.h"

#include <library/cpp/string_utils/base64/base64.h>

#include <util/string/builder.h>

using namespace NYdb;
using namespace NYdb::NTable;

namespace NIdxTest {

NYdb::TValue CreateOptionalValue(const TColumn& column, const TRandomValueProvider& rvp) {
    NYdb::TValueBuilder value;

    const NYdb::TType& type = column.Type;
    NYdb::TTypeParser typeParser(type);
    typeParser.OpenOptional();
    switch (typeParser.GetPrimitive()) {
        case EPrimitiveType::Bool:
            value.OptionalBool(rvp.RandomBool());
            break;
        case EPrimitiveType::Int8:
            value.OptionalInt8(rvp.RandomUi8());
            break;
        case EPrimitiveType::Uint8:
            value.OptionalUint8(rvp.RandomI8());
            break;
        case EPrimitiveType::Int16:
            value.OptionalInt16(rvp.RandomI16());
            break;
        case EPrimitiveType::Uint16:
            value.OptionalUint16(rvp.RandomUi16());
            break;
        case EPrimitiveType::Int32:
            value.OptionalInt32(rvp.RandomI32());
            break;
        case EPrimitiveType::Uint32:
            value.OptionalUint32(rvp.RandomUi32());
            break;
        case EPrimitiveType::Int64:
            value.OptionalInt64(rvp.RandomI64());
            break;
        case EPrimitiveType::Uint64:
            value.OptionalUint64(rvp.RandomUi64());
            break;
        case EPrimitiveType::Float:
            value.OptionalFloat(rvp.RandomFloat());
            break;
        case EPrimitiveType::Double:
            value.OptionalDouble(rvp.RandomDouble());
            break;
        case EPrimitiveType::String:
            value.OptionalString(rvp.RandomString());
            break;
        case EPrimitiveType::Utf8:
            value.OptionalUtf8(Base64Encode(rvp.RandomString()));
            break;
        case EPrimitiveType::Json:
            {
                auto sb = TStringBuilder() << "[\"" << Base64Encode(rvp.RandomString()) << "\"]";
                value.OptionalJson(TString(sb));
            }
            break;
        case EPrimitiveType::Uuid:
            value.OptionalUuid(TUuidValue(rvp.RandomUi64(), rvp.RandomUi64()));
            break;
        default:
                Y_ABORT_UNLESS(false, "unimplemented");
    }
    return value.Build();
}

NYdb::TValue CreateValue(const TColumn& column, const TRandomValueProvider& rvp) {
    NYdb::TValueBuilder value;

    const NYdb::TType& type = column.Type;
    NYdb::TTypeParser typeParser(type);
    typeParser.OpenOptional();
    switch (typeParser.GetPrimitive()) {
        case EPrimitiveType::Bool:
            value.Bool(rvp.RandomBool());
            break;
        case EPrimitiveType::Int8:
            value.Int8(rvp.RandomUi8());
            break;
        case EPrimitiveType::Uint8:
            value.Uint8(rvp.RandomI8());
            break;
        case EPrimitiveType::Int16:
            value.Int16(rvp.RandomI16());
            break;
        case EPrimitiveType::Uint16:
            value.Uint16(rvp.RandomUi16());
            break;
        case EPrimitiveType::Int32:
            value.Int32(rvp.RandomI32());
            break;
        case EPrimitiveType::Uint32:
            value.Uint32(rvp.RandomUi32());
            break;
        case EPrimitiveType::Int64:
            value.Int64(rvp.RandomI64());
            break;
        case EPrimitiveType::Uint64:
            value.Uint64(rvp.RandomUi64());
            break;
        case EPrimitiveType::Float:
            value.Float(rvp.RandomFloat());
            break;
        case EPrimitiveType::Double:
            value.Double(rvp.RandomDouble());
            break;
        case EPrimitiveType::String:
            value.String(rvp.RandomString());
            break;
        case EPrimitiveType::Utf8:
            value.Utf8(Base64Encode(rvp.RandomString()));
            break;
        case EPrimitiveType::Json:
            {
                auto sb = TStringBuilder() << "[\"" << Base64Encode(rvp.RandomString()) << "\"]";
                value.Json(TString(sb));
            }
            break;
        default:
                Y_ABORT_UNLESS(false, "unimplemented");
    }
    return value.Build();
}

NYdb::TValue CreateRow(const TVector<TColumn>& columns, const TRandomValueProvider& rvp) {
    NYdb::TValueBuilder value;
    value.BeginStruct();
    for (const NYdb::TColumn& col : columns) {
        const NYdb::TType& type = col.Type;
        NYdb::TTypeParser typeParser(type);
        typeParser.OpenOptional();
        switch (typeParser.GetPrimitive()) {
            case EPrimitiveType::Bool:
                value.AddMember(col.Name).Bool(rvp.RandomBool());
                break;
            case EPrimitiveType::Int8:
                value.AddMember(col.Name).Int8(rvp.RandomUi8());
                break;
            case EPrimitiveType::Uint8:
                value.AddMember(col.Name).Uint8(rvp.RandomI8());
                break;
            case EPrimitiveType::Int16:
                value.AddMember(col.Name).Int16(rvp.RandomI16());
                break;
            case EPrimitiveType::Uint16:
                value.AddMember(col.Name).Uint16(rvp.RandomUi16());
                break;
            case EPrimitiveType::Int32:
                value.AddMember(col.Name).Int32(rvp.RandomI32());
                break;
            case EPrimitiveType::Uint32:
                value.AddMember(col.Name).Uint32(rvp.RandomUi32());
                break;
            case EPrimitiveType::Int64:
                value.AddMember(col.Name).Int64(rvp.RandomI64());
                break;
            case EPrimitiveType::Uint64:
                value.AddMember(col.Name).Uint64(rvp.RandomUi64());
                break;
            case EPrimitiveType::Float:
                value.AddMember(col.Name).Float(rvp.RandomFloat());
                break;
            case EPrimitiveType::Double:
                value.AddMember(col.Name).Double(rvp.RandomDouble());
                break;
            case EPrimitiveType::String:
                value.AddMember(col.Name).String(rvp.RandomString());
                break;
            case EPrimitiveType::Utf8:
                value.AddMember(col.Name).Utf8(Base64Encode(rvp.RandomString()));
                break;
            case EPrimitiveType::Json:
                {
                    auto sb = TStringBuilder() << "[\"" << Base64Encode(rvp.RandomString()) << "\"]";
                    value.AddMember(col.Name).Json(TString(sb));
                }
            break;

            default:
                Y_ABORT_UNLESS(false, "unimplemented");
        }
    }
    value.EndStruct();
    return value.Build();
}

NYdb::TParams CreateParamsAsItems(const TVector<TValue>& values, const TVector<TString>& paramNames) {
    TParamsBuilder paramsBuilder;
    if (values.size() != paramNames.size()) {
        ythrow yexception() << "values and params size missmatch";
    }

    for (size_t i = 0; i < values.size(); i++) {
        paramsBuilder.AddParam(paramNames[i], values[i]);
    }

    return paramsBuilder.Build();
}

NYdb::TParams CreateParamsAsList(const TVector<NYdb::TValue>& batch, const TString& paramName) {
    NYdb::TParamsBuilder paramsBuilder;
    AddParamsAsList(paramsBuilder, batch, paramName);
    return paramsBuilder.Build();
}

void AddParamsAsList(NYdb::TParamsBuilder& paramsBuilder, const TVector<NYdb::TValue>& batch, const TString& paramName) {
    TValueBuilder builder;
    builder.BeginList();

    for (const NYdb::TValue& item : batch) {
        builder.AddListItem(item);
    }
    builder.EndList();

    paramsBuilder.AddParam(paramName, builder.Build());
}

class TDataProvider
    : public IDataProvider
    , public TRandomValueProvider {
public:
    TDataProvider(ui32 rowsCount, ui32 shardsCount, TTableDescription tableDesc)
        : RowsCount_(rowsCount)
        , TableDesc_(tableDesc)
        , BatchSz_((rowsCount > 10) ? 10 : rowsCount)
    {
        Positions_.resize(shardsCount);
    }

    NYdb::NTable::TTableDescription GetTableSchema() override {
        return TableDesc_;
    }

    NYdb::TType GetRowType() override {
        return CreateRow().GetType();
    }

    TMaybe<TVector<NYdb::TValue>> GetBatch(ui32 streamId) override {
        auto& pos = Positions_[streamId];
        if (pos >= RowsCount_)
            return {};

        pos += BatchSz_;
        return CreateBatch();
    }
private:
    TVector<NYdb::TValue> CreateBatch() {
        TVector<NYdb::TValue> batch;
        size_t batchSz = BatchSz_;
        while (batchSz--) {
            batch.push_back(CreateRow());
        }
        return batch;
    }

    NYdb::TValue CreateRow() {
        return ::NIdxTest::CreateRow(TableDesc_.GetColumns(), *this);
    }

    const ui32 RowsCount_;
    const TTableDescription TableDesc_;
    const ui32 BatchSz_;

    TVector<size_t> Positions_;
};

TRandomValueProvider::TRandomValueProvider(ui8 ranges, ui8 limit)
    : RangesBits_(ranges)
    , LimitMask_((limit == 0) ? Max<ui64>() : (1ull << limit) - 1)
{}

IDataProvider::TPtr CreateDataProvider(ui32 rowsCount, ui32 shardsCount, TTableDescription tableDesc) {
    return std::make_unique<TDataProvider>(rowsCount, shardsCount, tableDesc);
}

} // namespace NIdxTest
