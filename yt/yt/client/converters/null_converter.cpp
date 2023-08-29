#include "null_converter.h"

#include <yt/yt/client/table_client/logical_type.h>

namespace NYT::NConverters {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TNullColumnWriterConverter
    : public IColumnConverter
{
public:
    explicit TNullColumnWriterConverter(int columnIndex)
        : ColumnIndex_(columnIndex)
    {}

    TConvertedColumn Convert(const std::vector<TUnversionedRowValues>& rowsValues) override
    {
        auto rowCount = rowsValues.size();

        auto column = std::make_shared<TBatchColumn>();

        column->Id = ColumnIndex_;
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

private:
    const int ColumnIndex_;

};

////////////////////////////////////////////////////////////////////////////////

IColumnConverterPtr CreateNullConverter(int columnIndex)
{
    return std::make_unique<TNullColumnWriterConverter>(columnIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConverters
