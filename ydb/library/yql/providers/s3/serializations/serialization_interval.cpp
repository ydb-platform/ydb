#include "serialization_interval.h"

#include <ydb/library/yql/udfs/common/clickhouse/client/src/IO/ReadHelpers.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/IO/WriteHelpers.h>

#include <util/generic/serialized_enum.h>

namespace NYql::NSerialization {

using namespace NDB;
namespace {

Int64 ToAvgMicroSeconds(TSerializationInterval::EUnit unit)
{
    switch (unit)
    {
        case TSerializationInterval::EUnit::MICROSECONDS: return 1LL;
        case TSerializationInterval::EUnit::MILLISECONDS: return 1000LL;
        case TSerializationInterval::EUnit::SECONDS: return 1000000LL;
        case TSerializationInterval::EUnit::MINUTES: return 60000000LL;
        case TSerializationInterval::EUnit::HOURS: return 3600000000LL;
        case TSerializationInterval::EUnit::DAYS: return 86400000000LL;
        case TSerializationInterval::EUnit::WEEKS: return 604800000000LL;
    }
}

}

TSerializationInterval::EUnit TSerializationInterval::ToUnit(const TString& unit) {
    const auto names = GetEnumNames<EUnit>();
    for (const auto& name: names) {
        if (name.second == unit) {
            return name.first;
        }
    }
    return EUnit::MICROSECONDS;
}

TSerializationInterval::TSerializationInterval(EUnit unit)
    : Multiplier(ToAvgMicroSeconds(unit))
{}

void TSerializationInterval::serializeText(const IColumn & column, size_t rowNum, WriteBuffer & ostr, const FormatSettings &) const
{
    writeText(assert_cast<const ColumnVector<Int64> &>(column).getData()[rowNum] / Multiplier , ostr);
}

void TSerializationInterval::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & formatSettings) const
{
    Base::deserializeText(column, istr, formatSettings);
    assert_cast<ColumnVector<Int64> &>(column).getData().back() *= Multiplier;
}

void TSerializationInterval::serializeTextJSON(const IColumn & column, size_t rowNum, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto x = assert_cast<const ColumnVector<Int64> &>(column).getData()[rowNum] / Multiplier;
    writeJSONNumber(x, ostr, settings);
}

void TSerializationInterval::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & formatSettings) const
{
    Base::deserializeTextJSON(column, istr, formatSettings);
    assert_cast<ColumnVector<Int64> &>(column).getData().back() *= Multiplier;
}

void TSerializationInterval::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & formatSettings) const
{
    Base::deserializeTextCSV(column, istr, formatSettings);
    assert_cast<ColumnVector<Int64> &>(column).getData().back() *= Multiplier;
}

void TSerializationInterval::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    /// ColumnVector<T>::ValueType is a narrower type. For example, UInt8, when the Field type is UInt64
    typename ColumnVector<Int64>::ValueType x = get<FieldType>(field) / Multiplier;
    writeBinary(x, ostr);
}

void TSerializationInterval::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    Base::deserializeBinary(field, istr);
    field.get<Int64>() *= Multiplier;
}

void TSerializationInterval::serializeBinary(const IColumn & column, size_t rowNum, WriteBuffer & ostr) const
{
    writeBinary(assert_cast<const ColumnVector<Int64> &>(column).getData()[rowNum] / Multiplier, ostr);
}

void TSerializationInterval::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    Base::deserializeBinary(column, istr);
    assert_cast<ColumnVector<Int64> &>(column).getData().back() *= Multiplier;
}

void TSerializationInterval::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const typename ColumnVector<NDB::Int64>::Container & x = typeid_cast<const ColumnVector<NDB::Int64> &>(column).getData();
    size_t size = x.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    for (size_t i = offset; i < limit; i++) {
        serializeBinary(column, i, ostr);
    }
}

void TSerializationInterval::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avgValueSizeHint) const
{
    Base::deserializeBinaryBulk(column, istr, limit, avgValueSizeHint);
    ColumnVector<Int64>::Container & x = typeid_cast<ColumnVector<Int64> &>(column).getData();
    for (size_t i = x.size() - limit; i < x.size(); i++) {
        assert_cast<ColumnVector<Int64> &>(column).getData()[i] *= Multiplier;
    }
}

NDB::DataTypePtr GetInterval(TSerializationInterval::EUnit unit) {
    auto customDescr = std::make_unique<NDB::DataTypeCustomDesc>(std::make_unique<NDB::DataTypeCustomFixedName>("IntervalSecond"), std::make_shared<TSerializationInterval>(unit));
    return NDB::DataTypeFactory::instance().getCustom(std::move(customDescr));
}

}
