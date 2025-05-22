#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/public/udf/udf_type_printer.h>
#include <yql/essentials/utils/utf8.h>
#include <yql/essentials/minikql/dom/json.h>

#include <Poco/Util/Application.h>

#include "base/common/JSON.h"

#include "src/Columns/ColumnAggregateFunction.h"
#include "src/Columns/ColumnArray.h"
#include "src/Columns/ColumnConst.h"
#include "src/Columns/ColumnNullable.h"
#include "src/Columns/ColumnTuple.h"
#include "src/Columns/ColumnsNumber.h"
#include "src/Columns/ColumnString.h"

#include "src/DataStreams/NativeBlockInputStream.h"

#include "src/DataTypes/DataTypeEnum.h"
#include "src/DataTypes/DataTypesNumber.h"
#include "src/DataTypes/DataTypeDate.h"
#include "src/DataTypes/DataTypeDateTime64.h"
#include "src/DataTypes/DataTypeFactory.h"
#include "src/DataTypes/DataTypeArray.h"
#include "src/DataTypes/DataTypeNothing.h"
#include "src/DataTypes/DataTypeTuple.h"
#include "src/DataTypes/DataTypeNullable.h"
#include "src/DataTypes/DataTypeString.h"
#include "src/DataTypes/DataTypeUUID.h"

#include "src/Core/Block.h"
#include "src/Core/ColumnsWithTypeAndName.h"

#include "src/Formats/FormatFactory.h"
#include "src/Formats/registerFormats.h"
#include "src/Processors/Formats/InputStreamFromInputFormat.h"
#include "src/Processors/Formats/OutputStreamToOutputFormat.h"

#include <util/generic/size_literals.h>
#include <util/generic/yexception.h>
#include <util/string/split.h>

using namespace NYql;
using namespace NUdf;

namespace {
struct TPgConvertInfo {
    ui32 TypeId = 0;
    TStringRef TypeName;
    i32 TypeLen = 0;
    const TType* SourceDataType = nullptr;
    std::optional<EDataSlot> SourceLogicalSlot; // empty means that there is not corresponding type for pg type, e.g. point
};

struct TColumnMeta {
    std::optional<TPgConvertInfo> PgConvertInfo;
    std::optional<TString> Aggregation;
    std::optional<EDataSlot> Slot; // empty if null type
    bool IsOptional = false;
    bool IsEmptyList = false;
    bool IsList = false;
    bool IsTuple = false;
    ui8 Precision = 0, Scale = 0; // filled for Decimal
    std::vector<TColumnMeta> Items; // filled if IsTuple or IsList
    std::vector<TString> Enum;
    std::unordered_map<i16, std::pair<TString, ui32>> EnumVar;
};

std::pair<std::optional<EDataSlot>, TType*> InferSlotFromPgType(IFunctionTypeInfoBuilder& typeInfoBuilder, const TStringRef typeName) {
    if (typeName == TStringRef("bool")) {
        return { EDataSlot::Bool, typeInfoBuilder.SimpleType<bool>() };
    }
    if (typeName == TStringRef("int4")) {
        return { EDataSlot::Int32, typeInfoBuilder.SimpleType<i32>() };
    }
    if (typeName == TStringRef("int8")) {
        return { EDataSlot::Int64, typeInfoBuilder.SimpleType<i64>() };
    }
    if (typeName == TStringRef("float4")) {
        return { EDataSlot::Float, typeInfoBuilder.SimpleType<float>() };
    }
    if (typeName == TStringRef("float8")) {
        return { EDataSlot::Double, typeInfoBuilder.SimpleType<double>() };
    }
    if (typeName == TStringRef("text")) {
        return { EDataSlot::Utf8, typeInfoBuilder.SimpleType<TUtf8>() };
    }
    if (IsIn({ TStringRef("cstring"), TStringRef("varchar"), TStringRef("bytea") }, typeName)) {
        return { EDataSlot::String, nullptr };
    }
    return {};
}

TPgConvertInfo GetPgConvertInfo(IFunctionTypeInfoBuilder& typeInfoBuilder, const ITypeInfoHelper& typeHelper, const TType* type) {
    TPgConvertInfo info;
    info.TypeId = TPgTypeInspector(typeHelper, type).GetTypeId();
    auto* pgDescription = typeHelper.FindPgTypeDescription(info.TypeId);
    Y_ENSURE(pgDescription);
    info.TypeLen = pgDescription->Typelen;
    info.TypeName = pgDescription->Name;

    std::tie(info.SourceLogicalSlot, info.SourceDataType) = InferSlotFromPgType(typeInfoBuilder, pgDescription->Name);
    if (info.SourceDataType) {
        info.SourceDataType = typeInfoBuilder.Optional()->Item(info.SourceDataType).Build();
    }
    return info;
}

template<bool MaybeOptional = true>
bool GetDataType(IFunctionTypeInfoBuilder& typeInfoBuilder, const ITypeInfoHelper& typeHelper, const TType* type, TColumnMeta& meta) {
    switch (typeHelper.GetTypeKind(type)) {
    case ETypeKind::Tuple: {
        meta.IsTuple = true;
        const TTupleTypeInspector tupleType(typeHelper, type);
        meta.Items.resize(tupleType.GetElementsCount());
        for (auto i = 0U; i < meta.Items.size(); ++i)
            if (!GetDataType(typeInfoBuilder, typeHelper, tupleType.GetElementType(i), meta.Items[i]))
                return false;
        return true;
    }
    case ETypeKind::List:
        meta.IsList = true;
        type = TListTypeInspector(typeHelper, type).GetItemType();
        meta.Items.resize(1U);
        return GetDataType(typeInfoBuilder, typeHelper, type, meta.Items.front());
    case ETypeKind::Optional:
        if constexpr (MaybeOptional) {
            meta.IsOptional = true;
            type = TOptionalTypeInspector(typeHelper, type).GetItemType();
            return GetDataType<false>(typeInfoBuilder, typeHelper, type, meta);
        }
        else
            break;
    case ETypeKind::Data: {
        const TDataAndDecimalTypeInspector dataType(typeHelper, type);
        meta.Slot = GetDataSlot(dataType.GetTypeId());
        meta.Precision = dataType.GetPrecision();
        meta.Scale = dataType.GetScale();
        return true;
    }
    case ETypeKind::Pg: {
        meta.PgConvertInfo = GetPgConvertInfo(typeInfoBuilder, typeHelper, type);
        return true;
    }
    default:
        break;
    }
    return false;
}

NDB::DataTypePtr PgMetaToClickHouse(const TPgConvertInfo& info) {
    if (info.TypeName == TStringRef("bool")) {
        return std::make_shared<NDB::DataTypeUInt8>();
    }

    if (info.TypeName == TStringRef("int4")) {
        return std::make_shared<NDB::DataTypeInt32>();
    }

    if (info.TypeName == TStringRef("int8")) {
        return std::make_shared<NDB::DataTypeInt64>();
    }

    if (info.TypeName == TStringRef("float4")) {
        return std::make_shared<NDB::DataTypeFloat32>();
    }

    if (info.TypeName == TStringRef("float8")) {
        return std::make_shared<NDB::DataTypeFloat64>();
    }

    if (info.TypeName == TStringRef("timestamp")) {
        return std::make_shared<NDB::DataTypeDateTime64>(6, "UTC");
    }

    return std::make_shared<NDB::DataTypeString>();
}

NDB::DataTypePtr PgMetaToNullableClickHouse(const TPgConvertInfo& info) {
    return makeNullable(PgMetaToClickHouse(info));
}

NDB::DataTypePtr MetaToClickHouse(const TColumnMeta& meta) {
    if (meta.PgConvertInfo) {
        return PgMetaToNullableClickHouse(*meta.PgConvertInfo);
    }

    if (!meta.Enum.empty()) {
        if (meta.Enum.size() < 256) {
            NDB::DataTypeEnum8::Values values;
            if (!meta.EnumVar.size()) {
                for (ui32 i = 0; i < meta.Enum.size(); ++i) {
                    std::string name(meta.Enum[i]);
                    values.emplace_back(std::make_pair(name, i8(Min<i8>() + i)));
                }
            }
            else {
                for (const auto& x : meta.EnumVar) {
                    std::string name(x.second.first);
                    values.emplace_back(std::make_pair(name, i8(x.first)));
                }
            }

            return std::make_shared<NDB::DataTypeEnum8>(values);
        }
        else {
            NDB::DataTypeEnum16::Values values;
            if (!meta.EnumVar.size()) {
                for (ui32 i = 0; i < meta.Enum.size(); ++i) {
                    std::string name(meta.Enum[i]);
                    values.emplace_back(std::make_pair(name, i16(Min<i16>() + i)));
                }
            }
            else {
                for (const auto& x : meta.EnumVar) {
                    std::string name(x.second.first);
                    values.emplace_back(std::make_pair(name, x.first));
                }
            }

            return std::make_shared<NDB::DataTypeEnum16>(values);
        }
    }

    if (meta.Aggregation) {
        return NDB::DataTypeFactory::instance().get(*meta.Aggregation);
    }

    if (meta.IsEmptyList) {
        return std::make_shared<NDB::DataTypeArray>(std::make_shared<NDB::DataTypeNothing>());
    }

    if (meta.IsList) {
        return std::make_shared<NDB::DataTypeArray>(MetaToClickHouse(meta.Items.front()));
    }

    if (meta.IsTuple) {
        NDB::DataTypes elems;
        for (const auto& e : meta.Items) {
            elems.push_back(MetaToClickHouse(e));
        }

        return std::make_shared<NDB::DataTypeTuple>(elems);
    }

    NDB::DataTypePtr ret;
    if (!meta.Slot) {
        ret = makeNullable(std::make_shared<NDB::DataTypeNothing>());
    }
    else {
        switch (*meta.Slot) {
        case EDataSlot::Int8:
            ret = std::make_shared<NDB::DataTypeInt8>();
            break;
        case EDataSlot::Bool:
        case EDataSlot::Uint8:
            ret = std::make_shared<NDB::DataTypeUInt8>();
            break;
        case EDataSlot::Int16:
            ret = std::make_shared<NDB::DataTypeInt16>();
            break;
        case EDataSlot::Uint16:
            ret = std::make_shared<NDB::DataTypeUInt16>();
            break;
        case EDataSlot::Int32:
            ret = std::make_shared<NDB::DataTypeInt32>();
            break;
        case EDataSlot::Uint32:
            ret = std::make_shared<NDB::DataTypeUInt32>();
            break;
        case EDataSlot::Int64:
            ret = std::make_shared<NDB::DataTypeInt64>();
            break;
        case EDataSlot::Uint64:
            ret = std::make_shared<NDB::DataTypeUInt64>();
            break;
        case EDataSlot::Float:
            ret = std::make_shared<NDB::DataTypeFloat32>();
            break;
        case EDataSlot::Double:
            ret = std::make_shared<NDB::DataTypeFloat64>();
            break;
        case EDataSlot::String:
        case EDataSlot::Utf8:
        case EDataSlot::Json:
            ret = std::make_shared<NDB::DataTypeString>();
            break;
        case EDataSlot::Date:
        case EDataSlot::TzDate:
            ret = std::make_shared<NDB::DataTypeDate>();
            break;
        case EDataSlot::Datetime:
        case EDataSlot::TzDatetime:
            ret = std::make_shared<NDB::DataTypeDateTime>("UTC");
            break;
        case EDataSlot::Timestamp:
        case EDataSlot::TzTimestamp:
            ret = std::make_shared<NDB::DataTypeDateTime64>(6, "UTC");
            break;
        case EDataSlot::Uuid:
            ret = std::make_shared<NDB::DataTypeUUID>();
            break;
        default:
            ythrow yexception() << "Unsupported argument type: " << GetDataTypeInfo(*meta.Slot).Name;
        }
    }

    ret = meta.IsOptional ? makeNullable(ret) : ret;
    return ret;
}

template<bool Forward>
void PermuteUuid(const char* src, char* dst) {
    static constexpr ui32 Pairs[16] = { 4, 5, 6, 7, 2, 3, 0, 1, 15, 14, 13, 12, 11, 10, 9, 8 };
    static constexpr ui32 InvPairs[16] = { 6, 7, 4, 5, 0, 1, 2, 3, 15, 14, 13, 12, 11, 10, 9, 8 };
    for (ui32 i = 0; i < 16; ++i) {
        dst[Forward ? Pairs[i] : InvPairs[i]] = src[i];
    }
}

TUnboxedValuePod ConvertOutputValueForPgType(const NDB::IColumn* col, const TPgConvertInfo& meta,
    const IValueBuilder* valueBuilder, ssize_t externalIndex)
{
    Y_ENSURE(col->getDataType() == NDB::TypeIndex::Nullable);
    NDB::StringRef ref;
    if (col->isNullAt(externalIndex)) {
        return {};
    }

    auto& pgBuilder = valueBuilder->GetPgBuilder();
    ref = static_cast<const NDB::ColumnNullable*>(col)->getNestedColumn().getDataAt(externalIndex);
    if (!meta.SourceLogicalSlot) {
        // just parse pg value
        TStringValue parseError("");
        if (ref.size > std::numeric_limits<ui32>::max()) {
            ythrow yexception() << "Too huge payload to parse pg value: " << ref.size;
        }
        auto ret = pgBuilder.ValueFromText(meta.TypeId, { ref.data, static_cast<ui32>(ref.size) }, parseError);
        if (!ret) {
            ythrow yexception() << "Failed to parse value of pg type " << meta.TypeName << ", details: " << TStringBuf(parseError.Data(), parseError.Size());
        }
        return ret.Release();
    }

    switch (*meta.SourceLogicalSlot) {
    case EDataSlot::Bool:
        [[fallthrough]];
    case EDataSlot::Int32:
        [[fallthrough]];
    case EDataSlot::Int64:
        [[fallthrough]];
    case EDataSlot::Float:
        [[fallthrough]];
    case EDataSlot::Double: {
        TUnboxedValuePod ret = TUnboxedValuePod::Zero();
        Y_ENSURE(ref.size <= 8);
        memcpy(&ret, ref.data, ref.size);
        return pgBuilder.ConvertToPg(ret, meta.SourceDataType, meta.TypeId).Release();
    }
    case EDataSlot::Utf8:
        if (!IsUtf8(std::string_view(ref))) {
            ythrow yexception() << "Bad Utf8.";
        }
        [[fallthrough]];
    case EDataSlot::String:
        if (ref.size > std::numeric_limits<ui32>::max()) {
            ythrow yexception() << "Too huge payload to build pg string: " << ref.size;
        }

        return pgBuilder.NewString(meta.TypeLen, meta.TypeId, { ref.data,  static_cast<ui32>(ref.size) }).Release();
    default:
        ythrow yexception() << "Unexpected slot " << *meta.SourceLogicalSlot << " for pg type " << meta.TypeName;
    }
}

TUnboxedValuePod ConvertOutputValue(const NDB::IColumn* col, const TColumnMeta& meta, ui32 tzId,
    const IValueBuilder* valueBuilder, ssize_t externalIndex = 0) {
    if (meta.PgConvertInfo) {
        return ConvertOutputValueForPgType(col, *meta.PgConvertInfo, valueBuilder, externalIndex);
    }

    if (!meta.Enum.empty()) {
        auto ref = col->getDataAt(externalIndex);
        i16 value;
        if (col->getDataType() == NDB::TypeIndex::Int8) {
            value = *(const i8*)ref.data;
        }
        else if (col->getDataType() == NDB::TypeIndex::Int16) {
            value = *(const i16*)ref.data;
        }
        else {
            ythrow yexception() << "Unsupported column type: " << col->getName();
        }

        Y_ENSURE(meta.EnumVar.size() == meta.Enum.size());
        const auto x = meta.EnumVar.find(value);
        if (x == meta.EnumVar.cend()) {
            ythrow yexception() << "Missing enum value: " << value;
        }

        const ui32 index = x->second.second;
        return valueBuilder->NewVariant(index, TUnboxedValue::Void()).Release();
    }

    if (meta.Aggregation) {
        auto field = (*col)[externalIndex];
        const auto& state = field.get<NDB::AggregateFunctionStateData &>();
        if (state.data.size() > std::numeric_limits<ui32>::max()) {
            ythrow yexception() << "Too huge aggregation state to put it into string: " << state.data.size();
        }

        return valueBuilder->NewString({ state.data.data(), static_cast<ui32>(state.data.size()) }).Release();
    }

    if (meta.IsEmptyList) {
        return valueBuilder->NewEmptyList().Release();
    }

    const NDB::IColumn* data = col;
    if (meta.IsList) {
        const NDB::ColumnArray& res = static_cast<const NDB::ColumnArray&>(*col);
        const NDB::IColumn::Offsets& offsets = res.getOffsets();
        data = &res.getData();
        ui64 start = offsets[externalIndex - 1];
        ui64 limit = offsets[externalIndex] - start;
        TUnboxedValue* items = nullptr;
        TUnboxedValue ret = valueBuilder->NewArray(limit, items);
        for (ui64 index = start; index < start + limit; ++index) {
            TUnboxedValue* current = items + index - start;
            *current = ConvertOutputValue(data, meta.Items.front(), tzId, valueBuilder, index);
        }

        return ret.Release();
    }

    if (meta.IsTuple) {
        auto count = meta.Items.size();
        TUnboxedValue* items = nullptr;
        TUnboxedValue ret = valueBuilder->NewArray(count, items);
        const NDB::ColumnTuple& res = static_cast<const NDB::ColumnTuple&>(*col);
        for (ui32 i = 0; i < count; ++i) {
            items[i] = ConvertOutputValue(&res.getColumn(i), meta.Items[i], tzId, valueBuilder, externalIndex);
        }

        return ret.Release();
    }

    if (!meta.Slot) {
        return {};
    }

    NDB::StringRef ref;
    if (meta.IsOptional) {
        if (data->isNullAt(externalIndex)) {
            return {};
        }

        ref = static_cast<const NDB::ColumnNullable*>(data)->getNestedColumn().getDataAt(externalIndex);
    }
    else {
        ref = data->getDataAt(externalIndex);
    }

    if (const auto slot = *meta.Slot; slot == EDataSlot::String) {
        if (ref.size > std::numeric_limits<ui32>::max()) {
            ythrow yexception() << "Too huge payload to build string: " << ref.size;
        }

        return valueBuilder->NewString({ ref.data, static_cast<ui32>(ref.size) }).Release();
    }
    else if (slot == EDataSlot::Utf8) {
        if (!IsUtf8(std::string_view(ref))) {
            ythrow yexception() << "Bad Utf8.";
        }
        if (ref.size > std::numeric_limits<ui32>::max()) {
            ythrow yexception() << "Too huge payload to build Utf8 string: " << ref.size;
        }

        return valueBuilder->NewString({ ref.data, static_cast<ui32>(ref.size) }).Release();
    }
    else if (slot == EDataSlot::Json) {
        if (!NDom::IsValidJson(std::string_view(ref))) {
            ythrow yexception() << "Bad Json.";
        }
        if (ref.size > std::numeric_limits<ui32>::max()) {
            ythrow yexception() << "Too huge payload to build Json string: " << ref.size;
        }

        return valueBuilder->NewString({ ref.data, static_cast<ui32>(ref.size) }).Release();
    }
    else if (slot == EDataSlot::Uuid) {
        char uuid[16];
        if (ref.size != sizeof(uuid)) {
            ythrow yexception() << "Unsupported Uuid of size " << ref.size;
        }
        PermuteUuid<false>(ref.data, uuid);
        return valueBuilder->NewString({ uuid, sizeof(uuid) }).Release();
    }
    else if (slot == EDataSlot::Decimal) {
        if (NDecimal::TInt128 decimal; ref.size == sizeof(decimal)) {
            std::memcpy(&decimal, ref.data, ref.size);
            return TUnboxedValuePod(decimal);
        }
        else
            ythrow yexception() << "Unsupported decimal of size " << ref.size;
    }
    else {
        auto size = GetDataTypeInfo(*meta.Slot).FixedSize;
        TUnboxedValuePod ret = TUnboxedValuePod::Zero();
        Y_ENSURE(ref.size <= 8);
        Y_ENSURE(ref.size == size);
        memcpy(&ret, ref.data, ref.size);
        if (tzId) {
            if (*meta.Slot == EDataSlot::TzDatetime) {
                ret.SetTimezoneId(tzId);
            }
            else if (*meta.Slot == EDataSlot::TzDate) {
                auto& builder = valueBuilder->GetDateBuilder();
                ui32 year, month, day;
                if (const auto date = ret.Get<ui16>(); !builder.SplitDate(date, year, month, day)) {
                    ythrow yexception() << "Error in SplitDate(" << date << ").";
                }

                ui32 datetime;
                if (!builder.MakeDatetime(year, month, day, 23u, 59u, 0u, datetime, tzId)) {
                    ythrow yexception() << "Error in MakeDatetime(" << year << ',' << month << ',' << day << ',' << tzId << ").";
                }

                ret = TUnboxedValuePod(ui16(datetime / 86400u));
                ret.SetTimezoneId(tzId);
            }
        }

        return ret;
    }
}

void ConvertInputValue(const TUnboxedValuePod& value, NDB::IColumn* col, const TColumnMeta& meta) {
    if (meta.IsOptional && !value)
        return col->insert(NDB::Field());

    if (meta.IsEmptyList) {
        col->insertDefault();
        return;
    }

    if (meta.IsList) {
        auto& res = static_cast<NDB::ColumnArray&>(*col);
        auto& offsets = res.getOffsets();
        const auto data = &res.getData();
        ui64 count = 0ULL;
        if (const auto elements = value.GetElements()) {
            for (const auto length = value.GetListLength(); count < length; ++count)
                ConvertInputValue(elements[count], data, meta.Items.front());
        } else {
            const auto iterator = value.GetListIterator();
            for (TUnboxedValue current; iterator.Next(current); ++count) {
                ConvertInputValue(current, data, meta.Items.front());
            }
        }

        const auto prev = offsets.back();
        offsets.emplace_back(prev + count);
        return;
    }

    if (meta.IsTuple) {
        auto& res = static_cast<NDB::ColumnTuple&>(*col);
        for (ui32 i = 0U; i < meta.Items.size(); ++i) {
            const auto current = value.GetElement(i);
            ConvertInputValue(current, &res.getColumn(i), meta.Items[i]);
        }

        return;
    }

    if (!meta.Slot) {
        col->insertDefault();
        return;
    }

    switch (*meta.Slot) {
        case EDataSlot::Utf8:
        case EDataSlot::Json:
        case EDataSlot::String:
            return col->insert(NDB::Field(value.AsStringRef()));
        case EDataSlot::Bool:
            return col->insert(NDB::Field(value.Get<bool>()));
        case EDataSlot::Float:
            return col->insert(NDB::Field(value.Get<float>()));
        case EDataSlot::Double:
            return col->insert(NDB::Field(value.Get<double>()));
        case EDataSlot::Uint8:
            return col->insert(NDB::Field(value.Get<ui8>()));
        case EDataSlot::Uint16:
        case EDataSlot::Date:
        case EDataSlot::TzDate:
            return col->insert(NDB::Field(value.Get<ui16>()));
        case EDataSlot::Uint32:
        case EDataSlot::Datetime:
        case EDataSlot::TzDatetime:
            return col->insert(NDB::Field(value.Get<ui32>()));
        case EDataSlot::Uint64:
        case EDataSlot::Timestamp:
        case EDataSlot::TzTimestamp:
            return col->insert(NDB::Field(value.Get<ui64>()));
        case EDataSlot::Int8:
            return col->insert(NDB::Field(value.Get<i8>()));
        case EDataSlot::Int16:
            return col->insert(NDB::Field(value.Get<i16>()));
        case EDataSlot::Int32:
            return col->insert(NDB::Field(value.Get<i32>()));
        case EDataSlot::Int64:
            return col->insert(NDB::Field(value.Get<i64>()));
        case EDataSlot::Uuid: {
            NDB::UUID uuid;
            PermuteUuid<true>(value.AsStringRef().Data(), reinterpret_cast<char*>(&uuid));
            return col->insert(NDB::Field(uuid));
        }
        default:
            UdfTerminate("TODO: Unsupported field type.");
    }
}

class TParseFromYdb : public TBoxedValue {
public:
    class TStreamValue : public TBoxedValue {
    public:
        TStreamValue(const IValueBuilder* valueBuilder, const TUnboxedValue& stream,
            const std::vector<TColumnMeta>& outMeta,
            const TSourcePosition& pos,
            ui32 tzId)
            : ValueBuilder(valueBuilder)
            , Stream(stream)
            , OutMeta(outMeta)
            , Pos(pos)
            , TzId(tzId)
            , Cache(OutMeta.size())
        {}

        EFetchStatus Fetch(TUnboxedValue& result) final try {
            for (;;) {
                if (!BlockStream) {
                    if (const auto status = Stream.Fetch(Input); EFetchStatus::Ok != status)
                        return status;

                    const std::string_view buffer = Input.AsStringRef();
                    Buffer = std::make_unique<NDB::ReadBufferFromMemory>(buffer.data(), buffer.size());
                    BlockStream = std::make_unique<NDB::NativeBlockInputStream>(*Buffer, DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD);
                }

                if (CurrentRow >= CurrentBlock.rows()) {
                    CurrentRow = 0;
                    if (CurrentBlock = BlockStream->read(); !CurrentBlock) {
                        BlockStream.reset();
                        Buffer.reset();
                        continue;
                    }
                }

                TUnboxedValue* items = nullptr;
                result = Cache.NewArray(*ValueBuilder, items);
                for (ui32 i = 0; i < OutMeta.size(); ++i) {
                    *items++ = ConvertOutputValue(CurrentBlock.getByPosition(i).column.get(), OutMeta[i], TzId, ValueBuilder, CurrentRow);
                }

                ++CurrentRow;
                return EFetchStatus::Ok;
            }
        }
        catch (const Poco::Exception& e) {
            UdfTerminate((TStringBuilder() << ValueBuilder->WithCalleePosition(Pos) << " " << e.displayText()).data());
        }
        catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << ValueBuilder->WithCalleePosition(Pos) << " " << e.what()).data());
        }
    private:
        const IValueBuilder* ValueBuilder;
        const TUnboxedValue Stream;
        const std::vector<TColumnMeta> OutMeta; // in struct order
        const TSourcePosition Pos;
        const ui32 TzId;

        TPlainArrayCache Cache;

        TUnboxedValue Input;
        std::unique_ptr<NDB::ReadBuffer> Buffer;
        std::unique_ptr<NDB::IBlockInputStream> BlockStream;
        NDB::Block CurrentBlock;
        size_t CurrentRow = 0;
    };

    TParseFromYdb(const TSourcePosition& pos, std::vector<TColumnMeta>&& metaForColumns)
        : Pos(pos), OutMeta(std::move(metaForColumns))
    {}

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final try {
        ui32 tzId = 0U;
        if (const auto& tz = args[1U]) {
            if (!valueBuilder->GetDateBuilder().FindTimezoneId(tz.AsStringRef(), tzId)) {
                tzId = 0U;
            }
        }

        return TUnboxedValuePod(new TStreamValue(valueBuilder, *args, OutMeta, Pos, tzId));
    }
    catch (const Poco::Exception& e) {
        UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos) << " " << e.displayText()).data());
    }
    catch (const std::exception& e) {
        UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos) << " " << e.what()).data());
    }
private:
    const TSourcePosition Pos;
    const std::vector<TColumnMeta> OutMeta; // in struct order
};

class TStreamValue : public TBoxedValue {
public:
    TStreamValue(const std::string& type, const NDB::FormatSettings& settings, const IValueBuilder* valueBuilder, const TUnboxedValue& stream,
    const std::vector<TColumnMeta>& outMeta, const NDB::ColumnsWithTypeAndName& columns, ui32 tupleSize, const TSourcePosition& pos, ui32 tzId)
        : ValueBuilder(valueBuilder)
        , Stream(stream)
        , OutMeta(outMeta)
        , Columns(columns)
        , TupleSize(tupleSize)
        , Pos(pos)
        , TzId(tzId)
        , Cache(OutMeta.size())
        , TupleCache(tupleSize)
        , Type(type)
        , Settings(settings)
    {}
private:
    EFetchStatus Fetch(TUnboxedValue& result) final try {
        for (;;) {
            if (!BlockStream) {
                if (const auto status = Stream.Fetch(Input); EFetchStatus::Ok != status)
                    return status;

                InputElement = TupleSize ? Input.GetElement(0) : Input;
                const std::string_view buffer = InputElement.AsStringRef();
                Buffer = std::make_unique<NDB::ReadBufferFromMemory>(buffer.data(), buffer.size());
                BlockStream = std::make_unique<NDB::InputStreamFromInputFormat>(NDB::FormatFactory::instance().getInputFormat(Type, *Buffer, NDB::Block(Columns), nullptr, buffer.size(),  Settings));
            }

            if (CurrentRow >= CurrentBlock.rows()) {
                CurrentRow = 0;
                if (CurrentBlock = BlockStream->read(); !CurrentBlock) {
                    BlockStream.reset();
                    Buffer.reset();
                    continue;
                }
            }

            TUnboxedValue* items = nullptr;
            result = Cache.NewArray(*ValueBuilder, items);
            for (ui32 i = 0; i < OutMeta.size(); ++i) {
                *items++ = ConvertOutputValue(CurrentBlock.getByPosition(i).column.get(), OutMeta[i], TzId, ValueBuilder, CurrentRow);
            }

            if (TupleSize) {
                TUnboxedValue* tupleItems = nullptr;
                auto tuple = TupleCache.NewArray(*ValueBuilder, tupleItems);
                *tupleItems++ = result;
                for (ui32 i = 1; i < TupleSize; ++i) {
                    *tupleItems++ = Input.GetElement(i);
                }
                result = tuple;
            }

            ++CurrentRow;
            return EFetchStatus::Ok;
        }
    }
    catch (const Poco::Exception& e) {
        UdfTerminate((TStringBuilder() << ValueBuilder->WithCalleePosition(Pos) << " " << e.displayText()).data());
    }
    catch (const std::exception& e) {
        UdfTerminate((TStringBuilder() << ValueBuilder->WithCalleePosition(Pos) << " " << e.what()).data());
    }

    const IValueBuilder* ValueBuilder;
    const TUnboxedValue Stream;
    const std::vector<TColumnMeta> OutMeta;
    const NDB::ColumnsWithTypeAndName Columns;
    const ui32 TupleSize;
    const TSourcePosition Pos;
    const ui32 TzId;

    TPlainArrayCache Cache;
    TPlainArrayCache TupleCache;

    TUnboxedValue Input;
    TUnboxedValue InputElement;
    const TString Type;
    const NDB::FormatSettings Settings;

    std::unique_ptr<NDB::ReadBuffer> Buffer;
    std::unique_ptr<NDB::IBlockInputStream> BlockStream;
    NDB::Block CurrentBlock;
    size_t CurrentRow = 0U;
};

static NDB::FormatSettings::DateTimeFormat ToDateTimeFormat(const std::string& formatName) {
    static std::map<std::string, NDB::FormatSettings::DateTimeFormat> formats{
        {"POSIX", NDB::FormatSettings::DateTimeFormat::POSIX},
        {"ISO", NDB::FormatSettings::DateTimeFormat::ISO}
    };
    if (auto it = formats.find(formatName); it != formats.end()) {
        return it->second;
    }
    return NDB::FormatSettings::DateTimeFormat::Unspecified;
}

static NDB::FormatSettings::TimestampFormat ToTimestampFormat(const std::string& formatName) {
    static std::map<std::string, NDB::FormatSettings::TimestampFormat> formats{
        {"POSIX", NDB::FormatSettings::TimestampFormat::POSIX},
        {"ISO", NDB::FormatSettings::TimestampFormat::ISO},
        {"UNIX_TIME_MILLISECONDS", NDB::FormatSettings::TimestampFormat::UnixTimeMilliseconds},
        {"UNIX_TIME_SECONDS", NDB::FormatSettings::TimestampFormat::UnixTimeSeconds},
        {"UNIX_TIME_MICROSECONDS", NDB::FormatSettings::TimestampFormat::UnixTimeMicroSeconds}
    };
    if (auto it = formats.find(formatName); it != formats.end()) {
        return it->second;
    }
    return NDB::FormatSettings::TimestampFormat::Unspecified;
}

NDB::FormatSettings GetFormatSettings(const std::string_view& view) {
    NDB::FormatSettings settings;
    settings.skip_unknown_fields = true;
    settings.with_names_use_header = true;
    if (!view.empty()) {
        const std::string str(view);
        const JSON json(str);
#define SUPPORTED_FLAGS(xx) \
        xx(skip_unknown_fields) \
        xx(import_nested_json) \
        xx(with_names_use_header) \
        xx(null_as_default) \

#define SET_FLAG(flag) \
        if (json.has(#flag)) \
            settings.flag = json[#flag].get<bool>();

        SUPPORTED_FLAGS(SET_FLAG)

#undef SET_FLAG
#undef SUPPORTED_FLAGS
        if (json.has("csvdelimiter")) {
            auto delimiter = json["csvdelimiter"].getString();
            if (delimiter.size() != 1) {
                throw yexception() << "CSV delimiter should contain only one symbol. Specified delimiter '" << delimiter << "' is not allowed";
            }
            settings.csv.delimiter = delimiter[0];
        }

        if (json.has("data.datetime.formatname")) {
            auto formatName = json["data.datetime.formatname"].getString();
            settings.date_time_format_name = ToDateTimeFormat(formatName);
        }

        if (json.has("data.datetime.format")) {
            auto format = json["data.datetime.format"].getString();
            settings.date_time_format = format;
        }

        if (json.has("data.timestamp.formatname")) {
            auto formatName = json["data.timestamp.formatname"].getString();
            settings.timestamp_format_name = ToTimestampFormat(formatName);
        }

        if (json.has("data.timestamp.format")) {
            auto format = json["data.timestamp.format"].getString();
            settings.timestamp_format = format;
        }

        if (json.has("data.date.format")) {
            auto format = json["data.date.format"].getString();
            settings.date_format = format;
        }
    }
    return settings;
}

class TParseFormat : public TBoxedValue {
public:
    TParseFormat(const std::string_view& type, const std::string_view& settings,
                 const TSourcePosition& pos, std::vector<TColumnMeta>&& outMeta,
                 NDB::ColumnsWithTypeAndName&& columns, ui32 tupleSize)
        : Type(type)
        , Settings(GetFormatSettings(settings))
        , Pos(pos)
        , OutMeta(std::move(outMeta))
        , Columns(std::move(columns))
        , TupleSize(tupleSize)
    {}

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final try {
        ui32 tzId = 0U;
        if (const auto& tz = args[1U]) {
            if (!valueBuilder->GetDateBuilder().FindTimezoneId(tz.AsStringRef(), tzId)) {
                tzId = 0U;
            }
        }

        return TUnboxedValuePod(new TStreamValue(Type, Settings, valueBuilder, *args, OutMeta, Columns, TupleSize, Pos, tzId));
    }
    catch (const Poco::Exception& e) {
        UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos) << " " << e.displayText()).data());
    }
    catch (const std::exception& e) {
        UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos) << " " << e.what()).data());
    }
private:
    const std::string Type;
    const NDB::FormatSettings Settings;
    const TSourcePosition Pos;
    const std::vector<TColumnMeta> OutMeta;
    const NDB::ColumnsWithTypeAndName Columns;
    const ui32 TupleSize;
};

class TParseBlocks : public TBoxedValue {
    class TStreamValue : public TBoxedValue {
    public:
        TStreamValue(const IValueBuilder* valueBuilder, const TUnboxedValue& stream, const std::vector<TColumnMeta> outMeta, ui32 tupleSize, const TSourcePosition& pos, ui32 tzId)
            : ValueBuilder(valueBuilder)
            , Stream(stream)
            , OutMeta(outMeta)
            , TupleSize(tupleSize)
            , Pos(pos)
            , TzId(tzId)
            , Cache(OutMeta.size())
            , TupleCache(tupleSize)
        {}
    private:
        EFetchStatus Fetch(TUnboxedValue& result) final try {
            while (true) {
                if (!Input) {
                    if (const auto status = Stream.Fetch(Input); EFetchStatus::Ok != status)
                        return status;
                }

                const auto block = static_cast<NDB::Block*>(TupleSize ? Input.GetElement(0).GetResource() :
                                                                        Input.GetResource());

                if (CurrentRow >= block->rows()) {
                    CurrentRow = 0;
                    Input = NUdf::TUnboxedValuePod();
                    continue;
                }

                TUnboxedValue* items = nullptr;
                result = Cache.NewArray(*ValueBuilder, items);
                for (ui32 i = 0; i < OutMeta.size(); ++i) {
                    *items++ = ConvertOutputValue(block->getByPosition(i).column.get(), OutMeta[i], TzId, ValueBuilder, CurrentRow);
                }

                if (TupleSize) {
                    TUnboxedValue* tupleItems = nullptr;
                    auto tuple = TupleCache.NewArray(*ValueBuilder, tupleItems);
                    *tupleItems++ = result;
                    for (ui32 i = 1; i < TupleSize; ++i) {
                        *tupleItems++ = Input.GetElement(i);
                    }
                    result = tuple;
                }

                ++CurrentRow;
                return EFetchStatus::Ok;
            }
        }
        catch (const Poco::Exception& e) {
            UdfTerminate((TStringBuilder() << ValueBuilder->WithCalleePosition(Pos) << " " << e.displayText()).data());
        }
        catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << ValueBuilder->WithCalleePosition(Pos) << " " << e.what()).data());
        }

        const IValueBuilder* ValueBuilder;
        const TUnboxedValue Stream;
        const std::vector<TColumnMeta> OutMeta;
        const ui32 TupleSize;
        const TSourcePosition Pos;
        const ui32 TzId;

        TPlainArrayCache Cache;
        TPlainArrayCache TupleCache;

        TUnboxedValue Input;
        size_t CurrentRow = 0U;
    };

public:
    TParseBlocks(const TSourcePosition& pos, std::vector<TColumnMeta>&& outMeta, ui32 tupleSize)
        : Pos(pos), OutMeta(std::move(outMeta)), TupleSize(tupleSize)
    {}

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final try {
        ui32 tzId = 0U;
        if (const auto& tz = args[1U]) {
            if (!valueBuilder->GetDateBuilder().FindTimezoneId(tz.AsStringRef(), tzId)) {
                tzId = 0U;
            }
        }

        return TUnboxedValuePod(new TStreamValue(valueBuilder, *args, OutMeta, TupleSize, Pos, tzId));
    }
    catch (const Poco::Exception& e) {
        UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos) << " " << e.displayText()).data());
    }
    catch (const std::exception& e) {
        UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos) << " " << e.what()).data());
    }
private:
    const TSourcePosition Pos;
    const std::vector<TColumnMeta> OutMeta;
    const ui32 TupleSize;
};

class TSerializeFormat : public TBoxedValue {
    class TStreamValue : public TBoxedValue {
    private:
        using TPartitionByKey = std::vector<TUnboxedValue, TStdAllocatorForUdf<TUnboxedValue>>;

        struct TPartitionMapStuff {
            explicit TPartitionMapStuff(size_t size) : Size(size) {}

            bool operator()(const TPartitionByKey& left, const TPartitionByKey& right) const {
                for (auto i = 0U; i < Size; ++i)
                    if (left[i].AsStringRef() != right[i].AsStringRef())
                        return false;

                return true;
            }

            size_t operator()(const TPartitionByKey& value) const {
                size_t hash = 0ULL;
                for (auto i = 0U; i < Size; ++i)
                    hash = CombineHashes(hash, std::hash<std::string_view>()(value[i].AsStringRef()));
                return hash;
            }

            const size_t Size;
        };

        struct TPartitionByPayload {
            explicit TPartitionByPayload(NDB::Block&& block)
                : Block(std::move(block))
            {}

            NDB::Block Block;
            std::unique_ptr<NDB::IBlockOutputStream> OutputStream;
            size_t CurrentFileSize = 0; // Sum of size of blocks that were written to output with this key before closing footer
            bool hasDataInBlock = false; // We wrote something to block at least once.
        };

        using TPartitionsMap = std::unordered_map<TPartitionByKey, TPartitionByPayload, TPartitionMapStuff, TPartitionMapStuff, TStdAllocatorForUdf<std::pair<const TPartitionByKey, TPartitionByPayload>>>;
    public:
        TStreamValue(const IValueBuilder* valueBuilder, const TUnboxedValue& stream, const std::string& type, const NDB::FormatSettings& settings, const std::vector<ui32>& keysIndexes, const std::vector<ui32>& payloadsIndexes, size_t blockSizeLimit, size_t keysCountLimit, size_t totalSizeLimit, size_t fileSizeLimit, const std::vector<TColumnMeta>& inMeta, const NDB::ColumnsWithTypeAndName& columns, const TSourcePosition& pos)
            : ValueBuilder(valueBuilder)
            , Stream(stream)
            , InMeta(inMeta)
            , KeysIndexes(keysIndexes)
            , PayloadsIndexes(payloadsIndexes)
            , BlockSizeLimit(blockSizeLimit)
            , KeysCountLimit(keysCountLimit)
            , TotalSizeLimit(totalSizeLimit)
            , FileSizeLimit(fileSizeLimit)
            , Pos(pos)
            , HeaderBlock(columns)
            , Buffer(std::make_unique<NDB::WriteBufferFromOwnString>())
            , Settings(settings)
            , Type(type)
            , PartitionsMap(0ULL, TPartitionMapStuff(KeysIndexes.size()), TPartitionMapStuff(KeysIndexes.size()))
            , Cache(KeysIndexes.size() + 1U)
        {}
    private:
        bool FlushKey(const TPartitionsMap::iterator it, TUnboxedValue& result, bool finishFile = false) {
            const size_t currentBlockSize = it->second.Block.bytes();
            TotalSize -= currentBlockSize;
            const bool hasDataInFile = it->second.hasDataInBlock || it->second.OutputStream;
            if (!hasDataInFile) {
                return false;
            }
            if (!it->second.OutputStream) {
                it->second.OutputStream = std::make_unique<NDB::OutputStreamToOutputFormat>(NDB::FormatFactory::instance().getOutputFormat(Type, *Buffer, HeaderBlock, nullptr, {}, Settings));
                it->second.OutputStream->writePrefix();
            }
            if (it->second.hasDataInBlock) {
                it->second.OutputStream->write(it->second.Block);
            }
            it->second.CurrentFileSize += Buffer->stringRef().size;
            if (it->second.CurrentFileSize >= FileSizeLimit) { // Finish current file.
                finishFile = true;
            }

            if (finishFile && hasDataInFile) {
                it->second.OutputStream->writeSuffix(); // Finish current file with optional footer.
            }
            it->second.OutputStream->flush();

            if (finishFile) {
                it->second.OutputStream.reset();
                it->second.CurrentFileSize = 0;
            }
            it->second.Block = HeaderBlock.cloneEmpty();
            it->second.hasDataInBlock = false;

            std::string& resultString = Buffer->str();
            bool hasResult = resultString.size() > 0;
            if (hasResult) {
                if (KeysIndexes.empty())
                    result = ValueBuilder->NewString(resultString);
                else {
                    TUnboxedValue* tupleItems = nullptr;
                    result = Cache.NewArray(*ValueBuilder, tupleItems);
                    *tupleItems++ = ValueBuilder->NewString(resultString);
                    std::copy(it->first.cbegin(), it->first.cend(), tupleItems);
                }
            }

            if (finishFile) {
                FinishCurrentFileFlag = true; // The corresponding S3 write actor will finish current file and begin a new one.
                if (!KeysIndexes.empty()) {
                    TUnboxedValue* tupleItems = nullptr;
                    FinishCurrentFileValue = Cache.NewArray(*ValueBuilder, tupleItems);
                    *tupleItems++ = TUnboxedValue(); // Empty optional as finishing mark of current file.
                    std::copy(it->first.cbegin(), it->first.cend(), tupleItems);
                }
                if (!hasResult) {
                    FinishCurrentFileFlag = false;
                    result = FinishCurrentFileValue;
                    hasResult = true;
                }
            }

            Buffer->restart();
            return hasResult;
        }

        EFetchStatus Fetch(TUnboxedValue& result) final try {
            if (FinishCurrentFileFlag) {
                FinishCurrentFileFlag = false;
                result = FinishCurrentFileValue;
                return EFetchStatus::Ok;
            }

            if (IsFinished && PartitionsMap.empty())
                return EFetchStatus::Finish;

            for (TUnboxedValue row; !IsFinished;) {
                switch (const auto status = Stream.Fetch(row)) {
                    case EFetchStatus::Yield:
                        return EFetchStatus::Yield;
                    case EFetchStatus::Ok: {
                        TPartitionByKey keys(KeysIndexes.size());
                        for (auto i = 0U; i < keys.size(); ++i)
                            keys[i] = row.GetElement(KeysIndexes[i]);

                        const auto [partIt, insertedNew] = PartitionsMap.emplace(std::move(keys), HeaderBlock.cloneEmpty());

                        if (insertedNew && PartitionsMap.size() > KeysCountLimit) {
                            UdfTerminate((TStringBuilder() << ValueBuilder->WithCalleePosition(Pos) << " Too many unique keys: " << PartitionsMap.size()).data());
                        }

                        bool flush = !insertedNew && partIt->second.Block.bytes() >= BlockSizeLimit;
                        if (flush) {
                            Y_ABORT_UNLESS(FlushKey(partIt, result));
                        }

                        if (!flush && TotalSizeLimit && TotalSizeLimit <= TotalSize) {
                            const auto top = std::max_element(PartitionsMap.begin(), PartitionsMap.end(),
                                [](const TPartitionsMap::value_type& l, const TPartitionsMap::value_type& r){ return l.second.Block.bytes() < r.second.Block.bytes(); });
                            if (FlushKey(top, result)) {
                                flush = true;
                            }
                        }

                        TotalSize -= partIt->second.Block.bytes();
                        auto columns = partIt->second.Block.mutateColumns();
                        partIt->second.hasDataInBlock = true;
                        for (auto i = 0U; i < columns.size(); ++i) {
                            const auto index = PayloadsIndexes[i];
                            ConvertInputValue(row.GetElement(index), columns[i].get(), InMeta[index]);
                        }
                        partIt->second.Block.setColumns(std::move(columns));
                        TotalSize += partIt->second.Block.bytes();

                        if (flush)
                            return EFetchStatus::Ok;
                        else
                            continue;
                    }
                    case EFetchStatus::Finish:
                        IsFinished = true;
                        break;
                }
            }

            if (PartitionsMap.empty() && !FinishCurrentFileFlag)
                return EFetchStatus::Finish;

            while (!PartitionsMap.empty()) {
                const bool hasResult = FlushKey(PartitionsMap.begin(), result, true);
                PartitionsMap.erase(PartitionsMap.cbegin());
                if (hasResult) {
                    return EFetchStatus::Ok;
                }
            }
            return EFetchStatus::Finish;
        }
        catch (const Poco::Exception& e) {
            UdfTerminate((TStringBuilder() << ValueBuilder->WithCalleePosition(Pos) << " " << e.displayText()).data());
        }
        catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << ValueBuilder->WithCalleePosition(Pos) << " " << e.what()).data());
        }

        const IValueBuilder* ValueBuilder;
        const TUnboxedValue Stream;
        const std::vector<TColumnMeta> InMeta;
        const std::vector<ui32> KeysIndexes;
        const std::vector<ui32> PayloadsIndexes;
        const size_t BlockSizeLimit;
        const size_t KeysCountLimit;
        const size_t TotalSizeLimit;
        const size_t FileSizeLimit;
        const TSourcePosition Pos;
        TUnboxedValue FinishCurrentFileValue;
        bool FinishCurrentFileFlag = false;

        const NDB::Block HeaderBlock;
        const std::unique_ptr<NDB::WriteBufferFromOwnString> Buffer;
        const NDB::FormatSettings Settings;
        const std::string Type;

        TPartitionsMap PartitionsMap;

        TPlainArrayCache Cache;

        bool IsFinished = false;
        size_t TotalSize = 0ULL;
    };
public:
    TSerializeFormat(const std::string_view& type, const std::string_view& settings, std::vector<ui32>&& keysIndexes, std::vector<ui32>&& payloadsIndexes, size_t blockSizeLimit, size_t keysCountLimit, size_t totalSizeLimit, size_t fileSizeLimit, const TSourcePosition& pos, std::vector<TColumnMeta>&& inMeta, NDB::ColumnsWithTypeAndName&& columns)
        : Type(type), Settings(GetFormatSettings(settings)), KeysIndexes(std::move(keysIndexes)), PayloadsIndexes(std::move(payloadsIndexes)), BlockSizeLimit(blockSizeLimit), KeysCountLimit(keysCountLimit), TotalSizeLimit(totalSizeLimit), FileSizeLimit(fileSizeLimit), Pos(pos), InMeta(std::move(inMeta)), Columns(std::move(columns))
    {}

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final try {
        return TUnboxedValuePod(new TStreamValue(valueBuilder, *args, Type, Settings, KeysIndexes, PayloadsIndexes, BlockSizeLimit, KeysCountLimit, TotalSizeLimit, FileSizeLimit, InMeta, Columns, Pos));
    }
    catch (const Poco::Exception& e) {
        UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos) << " " << e.displayText()).data());
    }
    catch (const std::exception& e) {
        UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos) << " " << e.what()).data());
    }
private:
    const std::string Type;
    const NDB::FormatSettings Settings;
    const std::vector<ui32> KeysIndexes;
    const std::vector<ui32> PayloadsIndexes;
    const size_t BlockSizeLimit;
    const size_t KeysCountLimit;
    const size_t TotalSizeLimit;
    const size_t FileSizeLimit;
    const TSourcePosition Pos;

    const std::vector<TColumnMeta> InMeta;
    const NDB::ColumnsWithTypeAndName Columns;
};

struct TCHInitializer {
    using TWeakPtr = std::weak_ptr<TCHInitializer>;
    using TPtr = std::shared_ptr<TCHInitializer>;

    TCHInitializer()
    {
        NDB::registerFormats();
    }
};

template <typename T>
TString MakeEnumImpl(const T& values) {
    TStringBuilder str;
    str << "Enum<";
    bool first = true;
    for (const auto& value : values) {
        if (!first) {
            str << ',';
        }
        else {
            first = false;
        }

        str << "'" << value.first << "'";
    }

    str << '>';
    return str;
}

std::optional<TString> MakeYqlType(NDB::DataTypePtr type, bool validTz) {
    if (type->getTypeId() == NDB::TypeIndex::Enum8) {
        const NDB::DataTypeEnum8* enum8 = NDB::checkAndGetDataType<NDB::DataTypeEnum8>(type.get());
        return MakeEnumImpl(enum8->getValues());
    }

    if (type->getTypeId() == NDB::TypeIndex::Enum16) {
        const NDB::DataTypeEnum16* enum16 = NDB::checkAndGetDataType<NDB::DataTypeEnum16>(type.get());
        return MakeEnumImpl(enum16->getValues());
    }

    if (type->getTypeId() == NDB::TypeIndex::AggregateFunction) {
        return "Tagged<String,'" + TString(type->getName()) + "'>";
    }

    if (type->getTypeId() == NDB::TypeIndex::Array) {
        const NDB::DataTypeArray* array = NDB::checkAndGetDataType<NDB::DataTypeArray>(type.get());
        type = array->getNestedType();
        if (type->getTypeId() == NDB::TypeIndex::Nothing) {
            return "EmptyList";
        }

        auto inner = MakeYqlType(type, validTz);
        if (!inner) {
            return std::nullopt;
        }

        return "List<" + *inner + '>';
    }

    if (type->getTypeId() == NDB::TypeIndex::Tuple) {
        const NDB::DataTypeTuple* tuple = NDB::checkAndGetDataType<NDB::DataTypeTuple>(type.get());
        const auto& elems = tuple->getElements();
        TStringBuilder str;
        str << "Tuple<";
        bool first = true;
        for (const auto& e : elems) {
            auto inner = MakeYqlType(e, validTz);
            if (!inner) {
                return std::nullopt;
            }

            if (!first) {
                str << ',';
            } else {
                first = false;
            }

            str << *inner;
        }

        str << '>';
        return str;
    }

    if (type->isNullable()) {
       type = removeNullable(type);
       if (type->getTypeId() == NDB::TypeIndex::Nothing) {
          return "Null";
       }

       auto inner = MakeYqlType(type, validTz);
       if (!inner) {
           return std::nullopt;
       }

       return "Optional<" + *inner + '>';
   }

   if (type->getTypeId() == NDB::TypeIndex::UInt8) return "Uint8";
   else if (type->getTypeId() == NDB::TypeIndex::Int8) return "Int8";
   else if (type->getTypeId() == NDB::TypeIndex::UInt16) return "Uint16";
   else if (type->getTypeId() == NDB::TypeIndex::Int16) return "Int16";
   else if (type->getTypeId() == NDB::TypeIndex::UInt32) return "Uint32";
   else if (type->getTypeId() == NDB::TypeIndex::Int32) return "Int32";
   else if (type->getTypeId() == NDB::TypeIndex::UInt64) return "Uint64";
   else if (type->getTypeId() == NDB::TypeIndex::Int64) return "Int64";
   else if (type->getTypeId() == NDB::TypeIndex::Float32) return "Float";
   else if (type->getTypeId() == NDB::TypeIndex::Float64) return "Double";
   else if (type->getTypeId() == NDB::TypeIndex::String) return "String";
   else if (type->getTypeId() == NDB::TypeIndex::FixedString) return "String";
   else if (validTz && type->getTypeId() == NDB::TypeIndex::Date) return "TzDate";
   else if (validTz && type->getTypeId() == NDB::TypeIndex::DateTime) return "TzDatetime";
   else if (type->getTypeId() == NDB::TypeIndex::UUID) return "Uuid";
   else return std::nullopt;
}

SIMPLE_UDF(TToYqlType, TOptional<TUtf8>(TUtf8, TUtf8)) {
    const auto ref = args[0].AsStringRef();
    const auto tzRef = args[1].AsStringRef();
    ui32 tzId;
    const bool validTz = valueBuilder->GetDateBuilder().FindTimezoneId(tzRef, tzId);
    const NDB::String typeStr(ref.Data(), ref.Data() + ref.Size());
    const auto type = NDB::DataTypeFactory::instance().get(typeStr);
    if (const auto ret = MakeYqlType(type, validTz)) {
        return valueBuilder->NewString(*ret);
    }
    return TUnboxedValue();
}

}

class TClickHouseClientModule : public IUdfModule
{
public:
    TClickHouseClientModule() = default;

    static const TStringRef& Name() {
        static constexpr auto name = TStringRef::Of("ClickHouseClient");
        return name;
    }

    void CleanupOnTerminate() const final {}

    void GetAllFunctions(IFunctionsSink& sink) const final {
        sink.Add(TStringRef::Of("ToYqlType"));
        sink.Add(TStringRef::Of("ParseFormat"))->SetTypeAwareness();
        sink.Add(TStringRef::Of("ParseBlocks"))->SetTypeAwareness();
        sink.Add(TStringRef::Of("ParseFromYdb"))->SetTypeAwareness();
        sink.Add(TStringRef::Of("SerializeFormat"))->SetTypeAwareness();
    }

    void BuildFunctionTypeInfo(
                        const TStringRef& name,
                        TType* userType,
                        const TStringRef& typeConfig,
                        ui32 flags,
                        IFunctionTypeInfoBuilder& builder) const final try {
        LazyInitContext();
        auto argBuilder = builder.Args();
        if (name == "ToYqlType") {
            argBuilder->Add<TUtf8>();
            argBuilder->Add<TUtf8>();
            argBuilder->Done();
            builder.Returns<TOptional<TUtf8>>();
            if (!(flags & TFlags::TypesOnly)) {
                builder.Implementation(new TToYqlType(builder));
            }

            return;
        }

        const auto typeHelper = builder.TypeInfoHelper();
        if (name == "ParseBlocks") {
            const auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
            if (!userTypeInspector || userTypeInspector.GetElementsCount() < 3) {
                return builder.SetError("Invalid user type.");
            }

            const auto argsTypeTuple = userTypeInspector.GetElementType(0);
            const auto argsTypeInspector = TTupleTypeInspector(*typeHelper, argsTypeTuple);
            if (!argsTypeInspector) {
                return builder.SetError("Invalid user type - expected tuple.");
            }

            if (const auto argsCount = argsTypeInspector.GetElementsCount(); argsCount < 1 || argsCount > 2) {
                ::TStringBuilder sb;
                sb << "Invalid user type - expected one or two arguments, got: " << argsCount;
                return builder.SetError(sb);
            }

            ui32 tupleSize = 0;
            TType* inputItemType = builder.Resource("ClickHouseClient.Block");

            const auto resultType = userTypeInspector.GetElementType(2);
            auto structType = TStructTypeInspector(*typeHelper, resultType);
            if (!structType) {
                if (const auto tupleType = TTupleTypeInspector(*typeHelper, resultType)) {
                    tupleSize = tupleType.GetElementsCount();
                    if (tupleSize > 0) {
                        structType = TStructTypeInspector(*typeHelper, tupleType.GetElementType(0));
                        auto tupleBuilder = builder.Tuple(tupleSize);
                        tupleBuilder->Add(inputItemType);
                        for (ui32 i = 1; i < tupleSize; ++i) {
                            tupleBuilder->Add(tupleType.GetElementType(i));
                        }
                        inputItemType = tupleBuilder->Build();
                    }
                }
            }

            builder.UserType(userType);
            builder.Args()->Add(builder.Stream()->Item(inputItemType)).Add<TOptional<TUtf8>>().Done();
            builder.OptionalArgs(1U);
            builder.Returns(builder.Stream()->Item(resultType));

            if (structType) {
                std::vector<TColumnMeta> outMeta(structType.GetMembersCount());
                for (ui32 i = 0U; i < structType.GetMembersCount(); ++i) {
                    if (auto& meta = outMeta[i]; !GetDataType(builder, *typeHelper, structType.GetMemberType(i), meta)) {
                        ::TStringBuilder sb;
                        sb << "Incompatible column '" << structType.GetMemberName(i) << "' type: ";
                        TTypePrinter(*typeHelper, structType.GetMemberType(i)).Out(sb.Out);
                        return builder.SetError(sb);
                    }
                }

                if (!(flags & TFlags::TypesOnly)) {
                    builder.Implementation(new TParseBlocks(builder.GetSourcePosition(), std::move(outMeta), tupleSize));
                }
                return;
            } else {
                ::TStringBuilder sb;
                sb << "Incompatible row type: ";
                TTypePrinter(*typeHelper, resultType).Out(sb.Out);
                return builder.SetError(sb);
            }
        } else if (name == "ParseFormat") {
            const auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
            if (!userTypeInspector || userTypeInspector.GetElementsCount() < 3) {
                return builder.SetError("Invalid user type.");
            }

            const auto argsTypeTuple = userTypeInspector.GetElementType(0);
            const auto argsTypeInspector = TTupleTypeInspector(*typeHelper, argsTypeTuple);
            if (!argsTypeInspector) {
                return builder.SetError("Invalid user type - expected tuple.");
            }

            if (const auto argsCount = argsTypeInspector.GetElementsCount(); argsCount < 1 || argsCount > 2) {
                ::TStringBuilder sb;
                sb << "Invalid user type - expected one or two arguments, got: " << argsCount;
                return builder.SetError(sb);
            }

            ui32 tupleSize = 0;
            TType* inputItemType = builder.Primitive(GetDataTypeInfo(EDataSlot::String).TypeId);

            const auto resultType = userTypeInspector.GetElementType(2);
            auto structType = TStructTypeInspector(*typeHelper, resultType);
            if (!structType) {
                if (const auto tupleType = TTupleTypeInspector(*typeHelper, resultType)) {
                    tupleSize = tupleType.GetElementsCount();
                    if (tupleSize > 0) {
                        structType = TStructTypeInspector(*typeHelper, tupleType.GetElementType(0));
                        auto tupleBuilder = builder.Tuple(tupleSize);
                        tupleBuilder->Add(inputItemType);
                        for (ui32 i = 1; i < tupleSize; ++i) {
                            tupleBuilder->Add(tupleType.GetElementType(i));
                        }
                        inputItemType = tupleBuilder->Build();
                    }
                }
            }

            builder.UserType(userType);
            builder.Args()->Add(builder.Stream()->Item(inputItemType)).Add<TOptional<TUtf8>>().Done();
            builder.OptionalArgs(1U);
            builder.Returns(builder.Stream()->Item(resultType));

            if (structType) {
                std::vector<TColumnMeta> outMeta(structType.GetMembersCount());
                NDB::ColumnsWithTypeAndName columns(structType.GetMembersCount());
                for (ui32 i = 0U; i < structType.GetMembersCount(); ++i) {
                    if (auto& meta = outMeta[i]; GetDataType(builder, *typeHelper, structType.GetMemberType(i), meta)) {
                        auto& colsumn = columns[i];
                        colsumn.type = MetaToClickHouse(meta);
                        colsumn.name = structType.GetMemberName(i);
                    } else {
                        ::TStringBuilder sb;
                        sb << "Incompatible column '" << structType.GetMemberName(i) << "' type: ";
                        TTypePrinter(*typeHelper, structType.GetMemberType(i)).Out(sb.Out);
                        return builder.SetError(sb);
                    }
                }

                if (!(flags & TFlags::TypesOnly)) {
                    const std::string_view& typeCfg = typeConfig;
                    const auto jsonFrom = typeCfg.find('{');
                    builder.Implementation(new TParseFormat(typeCfg.substr(0U, jsonFrom),
                                                            std::string_view::npos == jsonFrom ? "" : typeCfg.substr(jsonFrom),
                                                            builder.GetSourcePosition(),
                                                            std::move(outMeta),
                                                            std::move(columns),
                                                            tupleSize));
                }
                return;
            } else {
                ::TStringBuilder sb;
                sb << "Incompatible row type: ";
                TTypePrinter(*typeHelper, resultType).Out(sb.Out);
                return builder.SetError(sb);
            }
        } else if (name == "SerializeFormat") {
            const auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
            if (!userTypeInspector || userTypeInspector.GetElementsCount() < 1) {
                return builder.SetError("Invalid user type.");
            }

            const auto argsTypeTuple = userTypeInspector.GetElementType(0);
            const auto argsTypeInspector = TTupleTypeInspector(*typeHelper, argsTypeTuple);
            if (!argsTypeInspector) {
                return builder.SetError("Invalid user type - expected tuple.");
            }

            if (const auto argsCount = argsTypeInspector.GetElementsCount(); argsCount != 1) {
                ::TStringBuilder sb;
                sb << "Invalid user type - expected one or two arguments, got: " << argsCount;
                return builder.SetError(sb);
            }

            const auto inputType = argsTypeInspector.GetElementType(0);

            const std::string_view& typeCfg = typeConfig;
            const auto jsonFrom = typeCfg.find('{');
            std::vector<std::string> keys;
            const auto& tail = std::string_view::npos == jsonFrom ? "" : typeCfg.substr(jsonFrom);
            size_t blockSizeLimit = 1_MB;
            size_t keysCountLimit = 4096;
            size_t totalSizeLimit = 10_MB;
            size_t fileSizeLimit = 50_MB;
            if (!tail.empty()) {
                const std::string str(tail);
                const JSON json(str);
                if (json.has("keys")) {
                    const auto& list = json["keys"];
                    keys.reserve(list.size());
                    std::transform(list.begin(), list.end(), std::back_inserter(keys), std::mem_fun_ref(&JSON::getString));
                }
                if (json.has("block_size_limit")) {
                    blockSizeLimit = FromString(TStringBuf(json["block_size_limit"].getString()));
                }
                if (json.has("keys_count_limit")) {
                    keysCountLimit = FromString(TStringBuf(json["keys_count_limit"].getString()));
                }
                if (json.has("total_size_limit")) {
                    totalSizeLimit = FromString(TStringBuf(json["total_size_limit"].getString()));
                }
                if (json.has("file_size_limit")) {
                    fileSizeLimit = FromString(TStringBuf(json["file_size_limit"].getString()));
                }
            }
            blockSizeLimit = Min(blockSizeLimit, fileSizeLimit);

            builder.UserType(userType);
            builder.Args()->Add(inputType).Done();
            if (keys.empty())
                builder.Returns(builder.Stream()->Item(builder.Optional()->Item<const char*>()));
            else {
                const auto tuple = builder.Tuple();
                tuple->Add(builder.Optional()->Item<const char*>());
                for (auto k =0U; k < keys.size(); ++k)
                    tuple->Add<TUtf8>();
                builder.Returns(builder.Stream()->Item(tuple->Build()));
            }

            if (const auto structType = TStructTypeInspector(*typeHelper, TStreamTypeInspector(*typeHelper, inputType).GetItemType())) {
                std::vector<TColumnMeta> inMeta(structType.GetMembersCount());
                std::vector<ui32> keyIndexes(keys.size()), payloadIndexes(structType.GetMembersCount() - keys.size());
                NDB::ColumnsWithTypeAndName columns;
                columns.reserve(payloadIndexes.size());
                for (ui32 i = 0U; i < structType.GetMembersCount(); ++i) {
                    if (auto& meta = inMeta[i]; GetDataType(builder, *typeHelper, structType.GetMemberType(i), meta)) {
                        const std::string_view name = structType.GetMemberName(i);
                        bool payload = true;
                        for (ui32 k = 0U; k < keys.size(); ++k) {
                            if (keys[k] == name) {
                                keyIndexes[k] = i;
                                payload = false;
                            }
                        }

                        if (payload) {
                            payloadIndexes[columns.size()] = i;
                            columns.emplace_back(MetaToClickHouse(meta), std::string(name));
                        }
                    } else {
                        ::TStringBuilder sb;
                        sb << "Incompatible column '" << structType.GetMemberName(i) << "' type: ";
                        TTypePrinter(*typeHelper, structType.GetMemberType(i)).Out(sb.Out);
                        return builder.SetError(sb);
                    }
                }

                if (!(flags & TFlags::TypesOnly)) {
                    builder.Implementation(new TSerializeFormat(typeCfg.substr(0U, jsonFrom), tail, std::move(keyIndexes), std::move(payloadIndexes), blockSizeLimit, keysCountLimit, totalSizeLimit, fileSizeLimit, builder.GetSourcePosition(), std::move(inMeta), std::move(columns)));
                }
                return;
            } else {
                ::TStringBuilder sb;
                sb << "Incompatible row type: ";
                TTypePrinter(*typeHelper, inputType).Out(sb.Out);
                return builder.SetError(sb);
            }
        } else if (name == "ParseFromYdb") {
            builder.UserType(userType);
            builder.Args()->Add(builder.Stream()->Item<char*>()).Add<TOptional<TUtf8>>().Done();
            builder.OptionalArgs(1U);
            builder.Returns(builder.Stream()->Item(userType));

            if (const auto structType = TStructTypeInspector(*typeHelper, userType)) {
                std::vector<TColumnMeta> columns(structType.GetMembersCount());
                for (ui32 i = 0U; i < structType.GetMembersCount(); ++i) {
                    if (const auto dataType = TDataAndDecimalTypeInspector(*typeHelper, TOptionalTypeInspector(*typeHelper, structType.GetMemberType(i)).GetItemType())) {
                        auto& meta = columns[i];
                        meta.Slot = GetDataSlot(dataType.GetTypeId());
                        meta.IsOptional = true;
                        meta.Precision = dataType.GetPrecision();
                        meta.Scale = dataType.GetScale();
                    } else {
                        ::TStringBuilder sb;
                        sb << "Incompatible column '" << structType.GetMemberName(i) << "' type: ";
                        TTypePrinter(*typeHelper, structType.GetMemberType(i)).Out(sb.Out);
                        return builder.SetError(sb);
                    }
                }

                if (!(flags & TFlags::TypesOnly)) {
                    builder.Implementation(new TParseFromYdb(builder.GetSourcePosition(), std::move(columns)));
                }
                return;
        } else {
                ::TStringBuilder sb;
                sb << "Incompatible row type: ";
                TTypePrinter(*typeHelper, userType).Out(sb.Out);
                return builder.SetError(sb);
            }
        }
    }
    catch (const Poco::Exception& e) {
        builder.SetError(e.displayText());
    }
    catch (const std::exception& e) {
        builder.SetError(TStringBuf(e.what()));
    }
private:
    void LazyInitContext() const {
        const std::unique_lock lock(CtxMutex);
        if (!Ctx) {
            if (auto ctx = StaticCtx.lock()) {
                Ctx = std::move(ctx);
            } else {
                StaticCtx = Ctx = std::make_shared<TCHInitializer>();
            }
        }
    }

    static std::mutex CtxMutex;
    static TCHInitializer::TWeakPtr StaticCtx;
    mutable TCHInitializer::TPtr Ctx;
};

std::mutex TClickHouseClientModule::CtxMutex;
TCHInitializer::TWeakPtr TClickHouseClientModule::StaticCtx;

REGISTER_MODULES(TClickHouseClientModule);
