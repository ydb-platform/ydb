#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/udf_type_printer.h>
#include <ydb/library/yql/utils/utf8.h>
#include <ydb/library/yql/minikql/dom/json.h>

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
#include "src/Formats/InputStreamFromInputFormat.h"
#include "src/Formats/registerFormats.h"

#include <util/generic/yexception.h>
#include <util/string/split.h>

using namespace NYql;
using namespace NUdf;

namespace {

struct TColumnMeta {
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

template<bool MaybeOptional = true>
bool GetDataType(const ITypeInfoHelper& typeHelper, const TType* type, TColumnMeta& meta) {
    switch (typeHelper.GetTypeKind(type)) {
    case ETypeKind::Tuple: {
        meta.IsTuple = true;
        const TTupleTypeInspector tupleType(typeHelper, type);
        meta.Items.resize(tupleType.GetElementsCount());
        for (auto i = 0U; i < meta.Items.size(); ++i)
            if (!GetDataType(typeHelper, tupleType.GetElementType(i), meta.Items[i]))
                return false;
        return true;
    }
    case ETypeKind::List:
        meta.IsList = true;
        type = TListTypeInspector(typeHelper, type).GetItemType();
        meta.Items.resize(1U);
        return GetDataType(typeHelper, type, meta.Items.front());
    case ETypeKind::Optional:
        if constexpr (MaybeOptional) {
            meta.IsOptional = true;
            type = TOptionalTypeInspector(typeHelper, type).GetItemType();
            return GetDataType<false>(typeHelper, type, meta);
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
    default:
        break;
    }
    return false;
}

NDB::DataTypePtr MetaToClickHouse(const TColumnMeta& meta) {
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
            ret = std::make_shared<NDB::DataTypeDateTime>();
            break;
        case EDataSlot::Uuid:
            ret = std::make_shared<NDB::DataTypeUUID>();
            break;
        default:
            throw yexception() << "Unsupported argument type: " << GetDataTypeInfo(*meta.Slot).Name;
        }
    }

    ret = meta.IsOptional ? makeNullable(ret) : ret;
    return ret;
}

void PermuteUuid(const char* src, char* dst, bool forward) {
    static ui32 Pairs[16] = { 4, 5, 6, 7, 2, 3, 0, 1, 15, 14, 13, 12, 11, 10, 9, 8 };
    static ui32 InvPairs[16] = { 6, 7, 4, 5, 0, 1, 2, 3, 15, 14, 13, 12, 11, 10, 9, 8 };
    for (ui32 i = 0; i < 16; ++i) {
        dst[forward ? Pairs[i] : InvPairs[i]] = src[i];
    }
}

TUnboxedValuePod ConvertOutputValue(const NDB::IColumn* col, const TColumnMeta& meta, ui32 tzId,
    const IValueBuilder* valueBuilder, ssize_t externalIndex = 0) {
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
            throw yexception() << "Unsupported column type: " << col->getName();
        }

        Y_ENSURE(meta.EnumVar.size() == meta.Enum.size());
        const auto x = meta.EnumVar.find(value);
        if (x == meta.EnumVar.cend()) {
            throw yexception() << "Missing enum value: " << value;
        }

        const ui32 index = x->second.second;
        return valueBuilder->NewVariant(index, TUnboxedValue::Void()).Release();
    }

    if (meta.Aggregation) {
        auto field = (*col)[externalIndex];
        const auto& state = field.get<NDB::AggregateFunctionStateData &>();
        return valueBuilder->NewString({ state.data.data(), (ui32)state.data.size() }).Release();
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
        return valueBuilder->NewString({ ref.data, (ui32)ref.size }).Release();
    }
    else if (slot == EDataSlot::Utf8) {
        if (!IsUtf8(std::string_view(ref))) {
            ythrow yexception() << "Bad Utf8.";
        }
        return valueBuilder->NewString({ ref.data, (ui32)ref.size }).Release();
    }
    else if (slot == EDataSlot::Json) {
        if (!NDom::IsValidJson(std::string_view(ref))) {
            ythrow yexception() << "Bad Json.";
        }
        return valueBuilder->NewString({ ref.data, (ui32)ref.size }).Release();
    }
    else if (slot == EDataSlot::Uuid) {
        char uuid[16];
        PermuteUuid(ref.data, uuid, false);
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
        memcpy(&ret, ref.data, size);
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
    const std::vector<TColumnMeta> outMeta, const NDB::ColumnsWithTypeAndName& columns, const TSourcePosition& pos, ui32 tzId)
        : ValueBuilder(valueBuilder)
        , Stream(stream)
        , OutMeta(outMeta)
        , Columns(columns)
        , Pos(pos)
        , TzId(tzId)
        , Cache(OutMeta.size())
        , Type(type)
        , Settings(settings)
    {}
private:
    EFetchStatus Fetch(TUnboxedValue& result) final try {
        for (;;) {
            if (!BlockStream) {
                if (const auto status = Stream.Fetch(Input); EFetchStatus::Ok != status)
                    return status;

                const std::string_view buffer = Input.AsStringRef();
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
    const TSourcePosition Pos;
    const ui32 TzId;

    TPlainArrayCache Cache;

    TUnboxedValue Input;
    const TString Type;
    const NDB::FormatSettings Settings;

    std::unique_ptr<NDB::ReadBuffer> Buffer;
    std::unique_ptr<NDB::IBlockInputStream> BlockStream;
    NDB::Block CurrentBlock;
    size_t CurrentRow = 0U;
};

class TParseFormat : public TBoxedValue {
public:
    TParseFormat(const std::string_view& type, const std::string_view& settings, const TSourcePosition& pos, std::vector<TColumnMeta>&& outMeta, NDB::ColumnsWithTypeAndName&& columns)
        : Type(type), Settings(GetFormatSettings(settings)), Pos(pos), OutMeta(std::move(outMeta)), Columns(std::move(columns))
    {}

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final try {
        ui32 tzId = 0U;
        if (const auto& tz = args[1U]) {
            if (!valueBuilder->GetDateBuilder().FindTimezoneId(tz.AsStringRef(), tzId)) {
                tzId = 0U;
            }
        }

        return TUnboxedValuePod(new TStreamValue(Type, Settings, valueBuilder, *args, OutMeta, Columns, Pos, tzId));
    }
    catch (const Poco::Exception& e) {
        UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos) << " " << e.displayText()).data());
    }
    catch (const std::exception& e) {
        UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos) << " " << e.what()).data());
    }
private:
    static NDB::FormatSettings GetFormatSettings(const std::string_view& view) {
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
        }
        return settings;
    }

    const std::string Type;
    const NDB::FormatSettings Settings;
    const TSourcePosition Pos;
    const std::vector<TColumnMeta> OutMeta;
    const NDB::ColumnsWithTypeAndName Columns;
};

class TParseBlocks : public TBoxedValue {
    class TStreamValue : public TBoxedValue {
    public:
        TStreamValue(const IValueBuilder* valueBuilder, const TUnboxedValue& stream, const std::vector<TColumnMeta> outMeta, const TSourcePosition& pos, ui32 tzId)
            : ValueBuilder(valueBuilder)
            , Stream(stream)
            , OutMeta(outMeta)
            , Pos(pos)
            , TzId(tzId)
            , Cache(OutMeta.size())
        {}
    private:
        EFetchStatus Fetch(TUnboxedValue& result) final try {
            while (true) {
                if (!Input) {
                    if (const auto status = Stream.Fetch(Input); EFetchStatus::Ok != status)
                        return status;
                }

                const auto block = static_cast<NDB::Block*>(Input.GetResource());

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
        const TSourcePosition Pos;
        const ui32 TzId;

        TPlainArrayCache Cache;

        TUnboxedValue Input;
        size_t CurrentRow = 0U;
    };

public:
    TParseBlocks(const TSourcePosition& pos, std::vector<TColumnMeta>&& outMeta)
        : Pos(pos), OutMeta(std::move(outMeta))
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
    const std::vector<TColumnMeta> OutMeta;
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
        static const auto name = TStringRef::Of("ClickHouseClient");
        return name;
    }

    void CleanupOnTerminate() const final {}

    void GetAllFunctions(IFunctionsSink& sink) const final {
        sink.Add(TStringRef::Of("ToYqlType"));
        sink.Add(TStringRef::Of("ParseFormat"))->SetTypeAwareness();
        sink.Add(TStringRef::Of("ParseBlocks"))->SetTypeAwareness();
        sink.Add(TStringRef::Of("ParseFromYdb"))->SetTypeAwareness();
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

            const auto resultType = userTypeInspector.GetElementType(2);

            builder.UserType(userType);
            builder.Args()->Add(builder.Stream()->Item(builder.Resource("ClickHouseClient.Block"))).Add<TOptional<TUtf8>>().Done();
            builder.OptionalArgs(1U);
            builder.Returns(builder.Stream()->Item(resultType));

            if (const auto structType = TStructTypeInspector(*typeHelper, resultType)) {
                std::vector<TColumnMeta> outMeta(structType.GetMembersCount());
                for (ui32 i = 0U; i < structType.GetMembersCount(); ++i) {
                    if (auto& meta = outMeta[i]; !GetDataType(*typeHelper, structType.GetMemberType(i), meta)) {
                        ::TStringBuilder sb;
                        sb << "Incompatible column '" << structType.GetMemberName(i) << "' type: ";
                        TTypePrinter(*typeHelper, structType.GetMemberType(i)).Out(sb.Out);
                        return builder.SetError(sb);
                    }
                }

                if (!(flags & TFlags::TypesOnly)) {
                    builder.Implementation(new TParseBlocks(builder.GetSourcePosition(), std::move(outMeta)));
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

            const auto resultType = userTypeInspector.GetElementType(2);

            builder.UserType(userType);
            builder.Args()->Add(builder.Stream()->Item<char*>()).Add<TOptional<TUtf8>>().Done();
            builder.OptionalArgs(1U);
            builder.Returns(builder.Stream()->Item(resultType));

            if (const auto structType = TStructTypeInspector(*typeHelper, resultType)) {
                std::vector<TColumnMeta> outMeta(structType.GetMembersCount());
                NDB::ColumnsWithTypeAndName columns(structType.GetMembersCount());
                for (ui32 i = 0U; i < structType.GetMembersCount(); ++i) {
                    if (auto& meta = outMeta[i]; GetDataType(*typeHelper, structType.GetMemberType(i), meta)) {
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
                    builder.Implementation(new TParseFormat(typeCfg.substr(0U, jsonFrom), std::string_view::npos == jsonFrom ? "" : typeCfg.substr(jsonFrom),  builder.GetSourcePosition(), std::move(outMeta), std::move(columns)));
                }
                return;
            } else {
                ::TStringBuilder sb;
                sb << "Incompatible row type: ";
                TTypePrinter(*typeHelper, resultType).Out(sb.Out);
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
