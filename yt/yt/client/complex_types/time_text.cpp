#include "time_text.h"

namespace NYT::NComplexTypes {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static void ValidateLength(TStringBuf data, ESimpleLogicalValueType valueType)
{
    size_t minLength;
    size_t maxLength;
    switch (valueType) {
        case ESimpleLogicalValueType::Date:
            minLength = maxLength = DateLength;
            break;
        case ESimpleLogicalValueType::Datetime:
            minLength = maxLength = DateTimeLength;
            break;
        case ESimpleLogicalValueType::Timestamp:
            minLength = DateTimeLength;
            maxLength = TimestampLength;
            break;
        default:
            YT_ABORT();
    }
    if (minLength > data.size() || data.size() > maxLength) {
        THROW_ERROR_EXCEPTION(
            "Invalid date string length. Expected: [%v..%v], got: %v",
            minLength,
            maxLength,
            data.size());
    }
}

ui64 BinaryTimeFromText(TStringBuf data, ESimpleLogicalValueType valueType)
{
    // ISO 8601 allows omitting dates' "suffix". E.g 2021-09, 2021-09-30, 2021-09-30T23:59 are valid ISO 8601 dates.
    // So we have to validate date's length to make sure we respect ValueType_.
    ValidateLength(data, valueType);
    TInstant instant;
    try {
        instant = TInstant::ParseIso8601(data);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Could not parse date %Qv", data)
            << ex;
    }
    switch (valueType) {
        case ESimpleLogicalValueType::Date:
            return instant.Days();
        case ESimpleLogicalValueType::Datetime:
            return instant.Seconds();
        case ESimpleLogicalValueType::Timestamp:
            return instant.MicroSeconds();
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
