#include "serialization_interval.h"

#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeFactory.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeInterval.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/Serializations/SerializationNumber.h>

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

class TSerializationIntervalImpl : public SerializationNumber<Int64> {
public:
    using Base = NDB::SerializationNumber<Int64>;

public:
    TSerializationIntervalImpl(TSerializationInterval::EUnit unit)
      : Multiplier(ToAvgMicroSeconds(unit))
    {
    }
    
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override {
        writeText(assert_cast<const ColumnVector<Int64> &>(column).getData()[row_num] / Multiplier , ostr);
    }

    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override {
        Base::deserializeText(column, istr, settings);
        assert_cast<ColumnVector<Int64> &>(column).getData().back() *= Multiplier;
    }

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override {
        auto x = assert_cast<const ColumnVector<Int64> &>(column).getData()[row_num] / Multiplier;
        writeJSONNumber(x, ostr, settings);
    }

    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override {
        Base::deserializeTextJSON(column, istr, settings);
        assert_cast<ColumnVector<Int64> &>(column).getData().back() *= Multiplier;
    }

    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override {
        Base::deserializeTextCSV(column, istr, settings);
        assert_cast<ColumnVector<Int64> &>(column).getData().back() *= Multiplier;
    }

    /** Format is platform-dependent. */
    void serializeBinary(const Field & field, WriteBuffer & ostr) const override {
        /// ColumnVector<T>::ValueType is a narrower type. For example, UInt8, when the Field type is UInt64
        typename ColumnVector<Int64>::ValueType x = get<FieldType>(field) / Multiplier;
        writeBinary(x, ostr);
    }

    void deserializeBinary(Field & field, ReadBuffer & istr) const override {
        Base::deserializeBinary(field, istr);
        field.get<Int64>() *= Multiplier;
    }

    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override {
        writeBinary(assert_cast<const ColumnVector<Int64> &>(column).getData()[row_num] / Multiplier, ostr);
    }

    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override {
        Base::deserializeBinary(column, istr);
        assert_cast<ColumnVector<Int64> &>(column).getData().back() *= Multiplier;
    }

    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override {
        const typename ColumnVector<NDB::Int64>::Container & x = typeid_cast<const ColumnVector<NDB::Int64> &>(column).getData();
        size_t size = x.size();

        if (limit == 0 || offset + limit > size) {
            limit = size - offset;
        }

        for (size_t i = offset; i < limit; i++) {
            serializeBinary(column, i, ostr);
        }
    }

    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override {
        Base::deserializeBinaryBulk(column, istr, limit, avg_value_size_hint);
        ColumnVector<Int64>::Container & x = typeid_cast<ColumnVector<Int64> &>(column).getData();
        for (size_t i = x.size() - limit; i < x.size(); i++) {
            assert_cast<ColumnVector<Int64> &>(column).getData()[i] *= Multiplier;
        }
    }

private:
    const Int64 Multiplier;
};

NDB::DataTypePtr GetInterval(TSerializationInterval::EUnit unit) {
    auto customDescr = std::make_unique<NDB::DataTypeCustomDesc>(std::make_unique<NDB::DataTypeCustomFixedName>("IntervalSecond"), std::make_shared<TSerializationIntervalImpl>(unit));
    return NDB::DataTypeFactory::instance().getCustom(std::move(customDescr));
}

}
