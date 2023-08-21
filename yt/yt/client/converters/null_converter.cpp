#include "null_converter.h"

#include <yt/yt/client/table_client/logical_type.h>

namespace NYT::NConverters {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TNullColumnWriterConverter
    : public IColumnConverter
{
public:
    explicit TNullColumnWriterConverter()
    {}

    TConvertedColumn Convert(TRange<NTableClient::TUnversionedRow> rows) override
    {
        auto rowCount = rows.size();

        auto column = std::make_shared<TBatchColumn>();

        column->Type = SimpleLogicalType(ESimpleLogicalValueType::Null);
        column->ValueCount = rowCount;

        TOwningColumn owner = {
            std::move(column),
            /*NullBitmap*/ std::nullopt,
            /*ValueBuffer*/ std::nullopt,
            /*stringBuffer*/ std::nullopt
        };

        return {{owner}, owner.Column.get()};
    }

};

////////////////////////////////////////////////////////////////////////////////

IColumnConverterPtr CreateNullConverter()
{
    return std::make_unique<TNullColumnWriterConverter>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConverters
