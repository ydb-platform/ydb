#include "udf_data_type.h"

#include <array>
#include <optional>

namespace NYql {
namespace NUdf {
namespace {
ui8 GetDecimalWidth(EDataTypeFeatures features, size_t size) {
    if (features & NUdf::IntegralType) {
        switch (size) {
            case 1U: return 3U;
            case 2U: return 5U;
            case 4U: return 10U;
            case 8U: return 20U;
        }
    }
    return 0U;
}

#define OK {ECastOptions::Complete}
#define MF {ECastOptions::MayFail}
#define LD {ECastOptions::MayLoseData}
#define FL {ECastOptions::MayFail | ECastOptions::MayLoseData}
#define NO {ECastOptions::Impossible}
#define UN {ECastOptions::Impossible | ECastOptions::Undefined}

static const std::array<std::array<std::optional<TCastResultOptions>, DataSlotCount>, DataSlotCount> CastResultsTable = {{
    // Bool, Int8 ----integrals---- Uint64   Floats, Strings, YJsons, Uuid, DateTimes, Interval, TzDateTimes, Decimal, DyNumber, JsonDocument
    {{ OK,  OK, OK, OK, OK, OK, OK, OK, OK,  OK, OK,  OK, OK,  NO, NO,  NO,  NO, NO, NO,  NO,  NO, NO, NO,  NO, NO, NO,  NO, NO, NO,  NO,  NO, NO, NO }}, // Bool

    {{ LD,  OK, MF, OK, MF, OK, MF, OK, MF,  OK, OK,  OK, OK,  NO, NO,  NO,  MF, MF, MF,  OK,  MF, MF, MF,  UN, NO, NO,  OK, OK, OK,  OK,  NO, NO, NO }}, // Int8
    {{ LD,  MF, OK, OK, OK, OK, OK, OK, OK,  OK, OK,  OK, OK,  NO, NO,  NO,  OK, OK, OK,  OK,  OK, OK, OK,  UN, NO, NO,  OK, OK, OK,  OK,  NO, NO, NO }}, // Uint8
    {{ LD,  MF, MF, OK, MF, OK, MF, OK, MF,  OK, OK,  OK, OK,  NO, NO,  NO,  MF, MF, MF,  OK,  MF, MF, MF,  UN, NO, NO,  OK, OK, OK,  OK,  NO, NO, NO }}, // Int16
    {{ LD,  MF, MF, MF, OK, OK, OK, OK, OK,  OK, OK,  OK, OK,  NO, NO,  NO,  MF, OK, OK,  OK,  MF, OK, OK,  UN, NO, NO,  OK, OK, OK,  OK,  NO, NO, NO }}, // Uint16
    {{ LD,  MF, MF, MF, MF, OK, MF, OK, MF,  OK, LD,  OK, OK,  NO, NO,  NO,  MF, MF, MF,  OK,  MF, MF, MF,  UN, NO, NO,  MF, OK, OK,  OK,  NO, NO, NO }}, // Int32
    {{ LD,  MF, MF, MF, MF, MF, OK, OK, OK,  OK, LD,  OK, OK,  NO, NO,  NO,  MF, MF, OK,  OK,  MF, MF, OK,  UN, NO, NO,  MF, OK, OK,  OK,  NO, NO, NO }}, // Uint32
    {{ LD,  MF, MF, MF, MF, MF, MF, OK, MF,  LD, LD,  OK, OK,  NO, NO,  NO,  MF, MF, MF,  MF,  MF, MF, MF,  UN, NO, NO,  MF, MF, MF,  MF,  NO, NO, NO }}, // Int64
    {{ LD,  MF, MF, MF, MF, MF, MF, MF, OK,  LD, LD,  OK, OK,  NO, NO,  NO,  MF, MF, MF,  MF,  MF, MF, MF,  UN, NO, NO,  MF, MF, MF,  MF,  NO, NO, NO }}, // Uint64

    {{ FL,  FL, FL, FL, FL, FL, FL, FL, FL,  OK, LD,  OK, OK,  NO, NO,  NO,  NO, NO, NO,  NO,  NO, NO, NO,  NO, NO, NO,  NO, NO, NO,  NO,  NO, NO, NO }}, // Double
    {{ FL,  FL, FL, FL, FL, FL, FL, FL, FL,  OK, OK,  OK, OK,  NO, NO,  NO,  NO, NO, NO,  NO,  NO, NO, NO,  NO, NO, NO,  NO, NO, NO,  NO,  NO, NO, NO }}, // Float

    {{ MF,  MF, MF, MF, MF, MF, MF, MF, MF,  FL, FL,  OK, MF,  MF, MF,  MF,  MF, MF, MF,  MF,  MF, MF, MF,  FL, FL, MF,  MF, MF, MF,  MF,  MF, MF, MF }}, // String
    {{ MF,  MF, MF, MF, MF, MF, MF, MF, MF,  FL, FL,  OK, OK,  MF, MF,  MF,  MF, MF, MF,  MF,  MF, MF, MF,  FL, FL, MF,  MF, MF, MF,  MF,  MF, MF, MF }}, // Utf8

    {{ NO,  NO, NO, NO, NO, NO, NO, NO, NO,  NO, NO,  OK, NO,  OK, NO,  NO,  NO, NO, NO,  NO,  NO, NO, NO,  NO, NO, NO,  NO, NO, NO,  NO,  NO, NO, NO }}, // Yson
    {{ NO,  NO, NO, NO, NO, NO, NO, NO, NO,  NO, NO,  OK, OK,  NO, OK,  NO,  NO, NO, NO,  NO,  NO, NO, NO,  NO, NO, OK,  NO, NO, NO,  NO,  NO, NO, NO }}, // Json

    {{ NO,  NO, NO, NO, NO, NO, NO, NO, NO,  NO, NO,  OK, OK,  NO, NO,  OK,  NO, NO, NO,  NO,  NO, NO, NO,  NO, NO, NO,  NO, NO, NO,  NO,  NO, NO, NO }}, // Uuid

    {{ NO,  MF, MF, MF, OK, OK, OK, OK, OK,  OK, OK,  OK, OK,  NO, NO,  NO,  OK, OK, OK,  NO,  OK, OK, OK,  NO, NO, NO,  OK, OK, OK,  NO,  NO, NO, NO }}, // Date
    {{ NO,  MF, MF, MF, MF, MF, OK, OK, OK,  OK, LD,  OK, OK,  NO, NO,  NO,  LD, OK, OK,  NO,  LD, OK, OK,  NO, NO, NO,  LD, OK, OK,  NO,  NO, NO, NO }}, // Datetime
    {{ NO,  MF, MF, MF, MF, MF, MF, OK, OK,  LD, LD,  OK, OK,  NO, NO,  NO,  LD, LD, OK,  NO,  LD, LD, OK,  NO, NO, NO,  LD, LD, OK,  NO,  NO, NO, NO }}, // Timestamp

    {{ NO,  MF, MF, MF, MF, MF, MF, OK, MF,  LD, LD,  OK, OK,  NO, NO,  NO,  NO, NO, NO,  OK,  NO, NO, NO,  NO, NO, NO,  NO, NO, NO,  OK,  NO, NO, NO }}, // Interval

    {{ NO,  MF, MF, MF, OK, OK, OK, OK, OK,  OK, OK,  OK, OK,  NO, NO,  NO,  OK, OK, OK,  NO,  OK, OK, OK,  NO, NO, NO,  NO, NO, NO,  NO,  OK, OK, OK }}, // TzDate
    {{ NO,  MF, MF, MF, MF, MF, OK, OK, OK,  OK, LD,  OK, OK,  NO, NO,  NO,  LD, OK, OK,  NO,  LD, OK, OK,  NO, NO, NO,  NO, NO, NO,  NO,  LD, OK, OK }}, // TzDatetime
    {{ NO,  MF, MF, MF, MF, MF, MF, OK, OK,  LD, LD,  OK, OK,  NO, NO,  NO,  LD, LD, OK,  NO,  LD, LD, OK,  NO, NO, NO,  NO, NO, NO,  NO,  LD, LD, OK }}, // TzTimestamp

    {{ NO,  UN, UN, UN, UN, UN, UN, UN, UN,  LD, LD,  OK, OK,  NO, NO,  NO,  NO, NO, NO,  NO,  NO, NO, NO,  UN, NO, NO,  NO, NO, NO,  NO,  NO, NO, NO }}, // Decimal
    {{ NO,  NO, NO, NO, NO, NO, NO, NO, NO,  NO, NO,  OK, OK,  NO, NO,  NO,  NO, NO, NO,  NO,  NO, NO, NO,  NO, OK, NO,  NO, NO, NO,  NO,  NO, NO, NO }}, // DyNumber
    {{ NO,  NO, NO, NO, NO, NO, NO, NO, NO,  NO, NO,  OK, OK,  NO, OK,  NO,  NO, NO, NO,  NO,  NO, NO, NO,  NO, NO, OK,  NO, NO, NO,  NO,  NO, NO, NO }}, // JsonDocument

    {{ NO,  MF, MF, MF, MF, OK, MF, OK, MF,  LD, OK,  OK, OK,  NO, NO,  NO,  MF, MF, MF,  NO,  NO, NO, NO,  NO, NO, NO,  OK, OK, OK,  NO,  NO, NO, NO }}, // Date32
    {{ NO,  MF, MF, MF, MF, MF, MF, OK, MF,  LD, LD,  OK, OK,  NO, NO,  NO,  FL, MF, MF,  NO,  NO, NO, NO,  NO, NO, NO,  LD, OK, OK,  NO,  NO, NO, NO }}, // Datetime64
    {{ NO,  MF, MF, MF, MF, MF, MF, OK, MF,  LD, LD,  OK, OK,  NO, NO,  NO,  FL, FL, MF,  NO,  NO, NO, NO,  NO, NO, NO,  LD, LD, OK,  NO,  NO, NO, NO }}, // Timestamp64

    {{ NO,  MF, MF, MF, MF, MF, MF, OK, MF,  LD, LD,  OK, OK,  NO, NO,  NO,  NO, NO, NO,  MF,  NO, NO, NO,  NO, NO, NO,  NO, NO, NO,  OK,  NO, NO, NO }}, // Interval64

    {{ NO,  NO, NO, NO, NO, NO, NO, NO, NO,  NO, NO,  OK, OK,  NO, NO,  NO,  MF, MF, MF,  NO,  MF, MF, MF,  NO, NO, NO,  OK, OK, OK,  NO,  OK, OK, OK }}, // TzDate32
    {{ NO,  NO, NO, NO, NO, NO, NO, NO, NO,  NO, NO,  OK, OK,  NO, NO,  NO,  MF, MF, MF,  NO,  MF, MF, MF,  NO, NO, NO,  LD, OK, OK,  NO,  LD, OK, OK }}, // TzDatetime64
    {{ NO,  NO, NO, NO, NO, NO, NO, NO, NO,  NO, NO,  OK, OK,  NO, NO,  NO,  MF, MF, MF,  NO,  MF, MF, MF,  NO, NO, NO,  LD, LD, OK,  NO,  LD, LD, OK }}, // TzTimestamp64
}};

}

TMaybe<TCastResultOptions> GetCastResult(EDataSlot source, EDataSlot target) {
    if (const auto& r = CastResultsTable[static_cast<size_t>(source)][static_cast<size_t>(target)]) {
        return *r;
    }
    return Nothing();
}

#define UDF_TYPE_ID_CHECK(xName, xId, xUnused2, xUnused3, xUnused4, xUnused5) \
    case xId: return EDataSlot::xName;

#define UDF_TYPE_PARSE(xName, xUnused1, xUnused2, xUnused3, xUnused4, xUnused5) \
    if (TStringBuf(#xName) == str) {                        \
        return EDataSlot::xName;                           \
    }

#define UDF_TYPE_INFO(xName, xId, xType, xFeatures, xLayoutType, xParamsCount) \
    { TStringBuf(#xName), xId, static_cast<EDataTypeFeatures>(xFeatures), \
        TPlainDataType<xType>::Result || TTzDataType<xType>::Result ? sizeof(xLayoutType) : 0, xParamsCount, GetDecimalWidth(static_cast<EDataTypeFeatures>(xFeatures), sizeof(xLayoutType)) },

TMaybe<EDataSlot> FindDataSlot(TDataTypeId id) {
    switch (id) {
        UDF_TYPE_ID_MAP(UDF_TYPE_ID_CHECK);
    }

    return {};
}

EDataSlot GetDataSlot(TDataTypeId id) {
    switch (id) {
        UDF_TYPE_ID_MAP(UDF_TYPE_ID_CHECK);
    }

    ythrow yexception() << "Invalid data type id: " << id;
}

TMaybe<EDataSlot> FindDataSlot(TStringBuf str) {
    UDF_TYPE_ID_MAP(UDF_TYPE_PARSE);
    return {};
}

EDataSlot GetDataSlot(TStringBuf str) {
    UDF_TYPE_ID_MAP(UDF_TYPE_PARSE);
    ythrow yexception() << "Invalid data type: " << str;
}

const TDataTypeInfo DataTypeInfos[DataSlotCount] = {
    UDF_TYPE_ID_MAP(UDF_TYPE_INFO)
};

bool IsComparable(EDataSlot left, EDataSlot right) {
    const auto lf = GetDataTypeInfo(left).Features;
    const auto rf = GetDataTypeInfo(right).Features;

    if (!(lf & rf & CanCompare)) {
        return false;
    }

    if (left == right) {
        return true;
    }

    if (lf & rf & NumericType) {
        return true;
    }

    if (lf & rf & StringType) {
        return true;
    }

    if (lf & (DateType | TzDateType) && rf & (DateType | TzDateType)) {
        return true;
    }

    if (lf & rf & TimeIntervalType) {
        return true;
    }

    if (lf & (IntegralType | DecimalType) && rf & (IntegralType | DecimalType)) {
        return true;
    }

    return false;
}

} // namespace NUdf
} // namespace NYql
