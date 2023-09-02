#include "null_column_converter.h"

#include <yt/yt/client/table_client/logical_type.h>

namespace NYT::NColumnConverters {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TNullColumnWriterConverter
    : public IColumnConverter
{
public:
    TNullColumnWriterConverter(int columnIndex)
        : ColumnIndex_(columnIndex)
    { }

    TConvertedColumn Convert(const std::vector<TUnversionedRowValues>& rowsValues) override
    {
        auto rowCount = rowsValues.size();

        auto column = std::make_shared<TBatchColumn>();

        column->Id = ColumnIndex_;
        column->Type = SimpleLogicalType(ESimpleLogicalValueType::Null);
        column->ValueCount = rowCount;

        TOwningColumn owner = {
            .Column = std::move(column),
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

} // namespace NYT::NColumnConverters
