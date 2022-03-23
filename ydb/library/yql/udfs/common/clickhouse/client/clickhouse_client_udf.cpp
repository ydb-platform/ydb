#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/udf_type_printer.h>
#include <ydb/library/yql/utils/utf8.h>

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

DB::DataTypePtr MetaToClickHouse(const TColumnMeta& meta) {
    if (!meta.Enum.empty()) {
        if (meta.Enum.size() < 256) {
            DB::DataTypeEnum8::Values values;
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

            return std::make_shared<DB::DataTypeEnum8>(values);
        }
        else {
            DB::DataTypeEnum16::Values values;
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

            return std::make_shared<DB::DataTypeEnum16>(values);
        }
    }

    if (meta.Aggregation) {
        return DB::DataTypeFactory::instance().get(*meta.Aggregation);
    }

    if (meta.IsEmptyList) {
        return std::make_shared<DB::DataTypeArray>(std::make_shared<DB::DataTypeNothing>());
    }

    if (meta.IsList) {
        return std::make_shared<DB::DataTypeArray>(MetaToClickHouse(meta.Items.front()));
    }

    if (meta.IsTuple) {
        DB::DataTypes elems;
        for (const auto& e : meta.Items) {
            elems.push_back(MetaToClickHouse(e));
        }

        return std::make_shared<DB::DataTypeTuple>(elems);
    }

    DB::DataTypePtr ret;
    if (!meta.Slot) {
        ret = makeNullable(std::make_shared<DB::DataTypeNothing>());
    }
    else {
        switch (*meta.Slot) {
        case EDataSlot::Int8:
            ret = std::make_shared<DB::DataTypeInt8>();
            break;
        case EDataSlot::Bool:
        case EDataSlot::Uint8:
            ret = std::make_shared<DB::DataTypeUInt8>();
            break;
        case EDataSlot::Int16:
            ret = std::make_shared<DB::DataTypeInt16>();
            break;
        case EDataSlot::Uint16:
            ret = std::make_shared<DB::DataTypeUInt16>();
            break;
        case EDataSlot::Int32:
            ret = std::make_shared<DB::DataTypeInt32>();
            break;
        case EDataSlot::Uint32:
            ret = std::make_shared<DB::DataTypeUInt32>();
            break;
        case EDataSlot::Int64:
            ret = std::make_shared<DB::DataTypeInt64>();
            break;
        case EDataSlot::Uint64:
            ret = std::make_shared<DB::DataTypeUInt64>();
            break;
        case EDataSlot::Float:
            ret = std::make_shared<DB::DataTypeFloat32>();
            break;
        case EDataSlot::Double:
            ret = std::make_shared<DB::DataTypeFloat64>();
            break;
        case EDataSlot::String:
            ret = std::make_shared<DB::DataTypeString>();
            break;
        case EDataSlot::Date:
        case EDataSlot::TzDate:
            ret = std::make_shared<DB::DataTypeDate>();
            break;
        case EDataSlot::Datetime:
        case EDataSlot::TzDatetime:
            ret = std::make_shared<DB::DataTypeDateTime>();
            break;
        case EDataSlot::Uuid:
            ret = std::make_shared<DB::DataTypeUUID>();
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

TUnboxedValuePod ConvertOutputValue(const DB::IColumn* col, const TColumnMeta& meta, ui32 tzId,
    const IValueBuilder* valueBuilder, ssize_t externalIndex = 0) {
    if (!meta.Enum.empty()) {
        auto ref = col->getDataAt(externalIndex);
        i16 value;
        if (col->getDataType() == DB::TypeIndex::Int8) {
            value = *(const i8*)ref.data;
        }
        else if (col->getDataType() == DB::TypeIndex::Int16) {
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
        const auto& state = field.get<DB::AggregateFunctionStateData &>();
        return valueBuilder->NewString({ state.data.data(), (ui32)state.data.size() }).Release();
    }

    if (meta.IsEmptyList) {
        return valueBuilder->NewEmptyList().Release();
    }

    const DB::IColumn* data = col;
    if (meta.IsList) {
        const DB::ColumnArray& res = static_cast<const DB::ColumnArray&>(*col);
        const DB::IColumn::Offsets& offsets = res.getOffsets();
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
        const DB::ColumnTuple& res = static_cast<const DB::ColumnTuple&>(*col);
        for (ui32 i = 0; i < count; ++i) {
            items[i] = ConvertOutputValue(&res.getColumn(i), meta.Items[i], tzId, valueBuilder, externalIndex);
        }

        return ret.Release();
    }

    if (!meta.Slot) {
        return {};
    }

    StringRef ref;
    if (meta.IsOptional) {
        if (data->isNullAt(externalIndex)) {
            return {};
        }

        ref = static_cast<const DB::ColumnNullable*>(data)->getNestedColumn().getDataAt(externalIndex);
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
                    Buffer = std::make_unique<DB::ReadBufferFromMemory>(buffer.data(), buffer.size());
                    BlockStream = std::make_unique<DB::NativeBlockInputStream>(*Buffer, DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD);
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
        std::unique_ptr<DB::ReadBuffer> Buffer;
        std::unique_ptr<DB::IBlockInputStream> BlockStream;
        DB::Block CurrentBlock;
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

template<bool WithYields>
class TStreamValue : public TBoxedValue {
    class TReadBufferFromStream : public DB::ReadBuffer {
    public:
        TReadBufferFromStream(TUnboxedValue stream)
            : DB::ReadBuffer(nullptr, 0ULL), Stream(std::move(stream))
        {}
    private:
        bool nextImpl() final {
            switch (Stream.Fetch(Value)) {
                case EFetchStatus::Finish:
                    return false;
                case EFetchStatus::Yield:
                    throw yexception() << "Enexpected yield.";
                case EFetchStatus::Ok:
                    break;
            }
            const std::string_view view(Value.AsStringRef());
            working_buffer = DB::BufferBase::Buffer(const_cast<char*>(view.cbegin()), const_cast<char*>(view.cend()));
            return true;
        }

        const TUnboxedValue Stream;
        TUnboxedValue Value;
    };
public:
    TStreamValue(const std::string& type, const DB::FormatSettings& settings, const IValueBuilder* valueBuilder, const TUnboxedValue& stream,
        const std::vector<TColumnMeta> outMeta, const DB::ColumnsWithTypeAndName& columns, const TSourcePosition& pos, ui32 tzId);
private:
    EFetchStatus Fetch(TUnboxedValue& result) final;

    const IValueBuilder* ValueBuilder;
    const TUnboxedValue Stream;
    const std::vector<TColumnMeta> OutMeta;
    const DB::ColumnsWithTypeAndName Columns;
    const TSourcePosition Pos;
    const ui32 TzId;

    TPlainArrayCache Cache;

    TUnboxedValue Input;
    const TString Type;
    const DB::FormatSettings Settings;

    std::unique_ptr<DB::ReadBuffer> Buffer;
    std::unique_ptr<DB::IBlockInputStream> BlockStream;
    DB::Block CurrentBlock;
    size_t CurrentRow = 0U;
};

template<>
TStreamValue<true>::TStreamValue(const std::string& type, const DB::FormatSettings& settings, const IValueBuilder* valueBuilder, const TUnboxedValue& stream,
    const std::vector<TColumnMeta> outMeta, const DB::ColumnsWithTypeAndName& columns, const TSourcePosition& pos, ui32 tzId)
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

template<>
TStreamValue<false>::TStreamValue(const std::string& type, const DB::FormatSettings& settings, const IValueBuilder* valueBuilder, const TUnboxedValue& stream,
    const std::vector<TColumnMeta> outMeta, const DB::ColumnsWithTypeAndName& columns, const TSourcePosition& pos, ui32 tzId)
    : ValueBuilder(valueBuilder)
    , Stream(stream)
    , OutMeta(outMeta)
    , Columns(columns)
    , Pos(pos)
    , TzId(tzId)
    , Cache(OutMeta.size())
    , Type(type)
    , Settings(settings)
    , Buffer(std::make_unique<TReadBufferFromStream>(std::move(stream)))
    , BlockStream(std::make_unique<DB::InputStreamFromInputFormat>(DB::FormatFactory::instance().getInputFormat(Type, *Buffer, DB::Block(Columns), nullptr, 1024 * 1024,  Settings)))
    , CurrentBlock(BlockStream->read())
{}

template<>
EFetchStatus TStreamValue<true>::Fetch(TUnboxedValue& result) try {
    for (;;) {
        if (!BlockStream) {
            if (const auto status = Stream.Fetch(Input); EFetchStatus::Ok != status)
                return status;

            const std::string_view buffer = Input.AsStringRef();
            Buffer = std::make_unique<DB::ReadBufferFromMemory>(buffer.data(), buffer.size());
            BlockStream = std::make_unique<DB::InputStreamFromInputFormat>(DB::FormatFactory::instance().getInputFormat(Type, *Buffer, DB::Block(Columns), nullptr, buffer.size(),  Settings));
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

template<>
EFetchStatus TStreamValue<false>::Fetch(TUnboxedValue& result) try {
    if (CurrentRow >= CurrentBlock.rows()) {
        CurrentRow = 0;
        CurrentBlock = BlockStream->read();

        if (!CurrentBlock)
            return EFetchStatus::Finish;
    }

    TUnboxedValue* items = nullptr;
    result = Cache.NewArray(*ValueBuilder, items);
    for (ui32 i = 0; i < OutMeta.size(); ++i) {
        *items++ = ConvertOutputValue(CurrentBlock.getByPosition(i).column.get(), OutMeta[i], TzId, ValueBuilder, CurrentRow);
    }

    ++CurrentRow;
    return EFetchStatus::Ok;
}
catch (const Poco::Exception& e) {
    UdfTerminate((TStringBuilder() << ValueBuilder->WithCalleePosition(Pos) << " " << e.displayText()).data());
}
catch (const std::exception& e) {
    UdfTerminate((TStringBuilder() << ValueBuilder->WithCalleePosition(Pos) << " " << e.what()).data());
}

template<bool WithYields>
class TParseFormat : public TBoxedValue {
public:
    TParseFormat(const std::string_view& type, const std::string_view& settings, const TSourcePosition& pos, std::vector<TColumnMeta>&& outMeta, DB::ColumnsWithTypeAndName&& columns)
        : Type(type), Settings(GetFormatSettings(settings)), Pos(pos), OutMeta(std::move(outMeta)), Columns(std::move(columns))
    {}

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final try {
        ui32 tzId = 0U;
        if (const auto& tz = args[1U]) {
            if (!valueBuilder->GetDateBuilder().FindTimezoneId(tz.AsStringRef(), tzId)) {
                tzId = 0U;
            }
        }

        return TUnboxedValuePod(new TStreamValue<WithYields>(Type, Settings, valueBuilder, *args, OutMeta, Columns, Pos, tzId));
    }
    catch (const Poco::Exception& e) {
        UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos) << " " << e.displayText()).data());
    }
    catch (const std::exception& e) {
        UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos) << " " << e.what()).data());
    }
private:
    static DB::FormatSettings GetFormatSettings(const std::string_view& view) {
        DB::FormatSettings settings;
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
    const DB::FormatSettings Settings;
    const TSourcePosition Pos;
    const std::vector<TColumnMeta> OutMeta;
    const DB::ColumnsWithTypeAndName Columns;
};

struct TCHInitializer {
    using TWeakPtr = std::weak_ptr<TCHInitializer>;
    using TPtr = std::shared_ptr<TCHInitializer>;

    TCHInitializer()
    {
        DB::registerFormats();
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

std::optional<TString> MakeYqlType(DB::DataTypePtr type, bool validTz) {
    if (type->getTypeId() == DB::TypeIndex::Enum8) {
        const DB::DataTypeEnum8* enum8 = DB::checkAndGetDataType<DB::DataTypeEnum8>(type.get());
        return MakeEnumImpl(enum8->getValues());
    }

    if (type->getTypeId() == DB::TypeIndex::Enum16) {
        const DB::DataTypeEnum16* enum16 = DB::checkAndGetDataType<DB::DataTypeEnum16>(type.get());
        return MakeEnumImpl(enum16->getValues());
    }

    if (type->getTypeId() == DB::TypeIndex::AggregateFunction) {
        return "Tagged<String,'" + TString(type->getName()) + "'>";
    }

    if (type->getTypeId() == DB::TypeIndex::Array) {
        const DB::DataTypeArray* array = DB::checkAndGetDataType<DB::DataTypeArray>(type.get());
        type = array->getNestedType();
        if (type->getTypeId() == DB::TypeIndex::Nothing) {
            return "EmptyList";
        }

        auto inner = MakeYqlType(type, validTz);
        if (!inner) {
            return std::nullopt;
        }

        return "List<" + *inner + '>';
    }

    if (type->getTypeId() == DB::TypeIndex::Tuple) {
        const DB::DataTypeTuple* tuple = DB::checkAndGetDataType<DB::DataTypeTuple>(type.get());
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
       if (type->getTypeId() == DB::TypeIndex::Nothing) {
          return "Null";
       }

       auto inner = MakeYqlType(type, validTz);
       if (!inner) {
           return std::nullopt;
       }

       return "Optional<" + *inner + '>';
   }

   if (type->getTypeId() == DB::TypeIndex::UInt8) return "Uint8";
   else if (type->getTypeId() == DB::TypeIndex::Int8) return "Int8";
   else if (type->getTypeId() == DB::TypeIndex::UInt16) return "Uint16";
   else if (type->getTypeId() == DB::TypeIndex::Int16) return "Int16";
   else if (type->getTypeId() == DB::TypeIndex::UInt32) return "Uint32";
   else if (type->getTypeId() == DB::TypeIndex::Int32) return "Int32";
   else if (type->getTypeId() == DB::TypeIndex::UInt64) return "Uint64";
   else if (type->getTypeId() == DB::TypeIndex::Int64) return "Int64";
   else if (type->getTypeId() == DB::TypeIndex::Float32) return "Float";
   else if (type->getTypeId() == DB::TypeIndex::Float64) return "Double";
   else if (type->getTypeId() == DB::TypeIndex::String) return "String";
   else if (type->getTypeId() == DB::TypeIndex::FixedString) return "String";
   else if (validTz && type->getTypeId() == DB::TypeIndex::Date) return "TzDate";
   else if (validTz && type->getTypeId() == DB::TypeIndex::DateTime) return "TzDatetime";
   else if (type->getTypeId() == DB::TypeIndex::UUID) return "Uuid";
   else return std::nullopt;
}

SIMPLE_UDF(TToYqlType, TOptional<TUtf8>(TUtf8, TUtf8)) {
    const auto ref = args[0].AsStringRef();
    const auto tzRef = args[1].AsStringRef();
    ui32 tzId;
    const bool validTz = valueBuilder->GetDateBuilder().FindTimezoneId(tzRef, tzId);
    const DB::String typeStr(ref.Data(), ref.Data() + ref.Size());
    const auto type = DB::DataTypeFactory::instance().get(typeStr);
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
        sink.Add(TStringRef::Of("ParseSource"))->SetTypeAwareness();
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
        if (name == "ParseFormat" || name == "ParseSource") {
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
                DB::ColumnsWithTypeAndName columns(structType.GetMembersCount());
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
                    if (name == "ParseFormat")
                        builder.Implementation(new TParseFormat<true>(typeCfg.substr(0U, jsonFrom), std::string_view::npos == jsonFrom ? "" : typeCfg.substr(jsonFrom),  builder.GetSourcePosition(), std::move(outMeta), std::move(columns)));
                    else
                        builder.Implementation(new TParseFormat<false>(typeCfg.substr(0U, jsonFrom), std::string_view::npos == jsonFrom ? "" : typeCfg.substr(jsonFrom),  builder.GetSourcePosition(), std::move(outMeta), std::move(columns)));
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
