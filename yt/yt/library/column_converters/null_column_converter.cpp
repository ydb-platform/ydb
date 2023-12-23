#include "null_column_converter.h"

#include <yt/yt/client/table_client/logical_type.h>

namespace NYT::NColumnConverters {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TNullColumnWriterConverter
    : public IColumnConverter
{
public:
    TNullColumnWriterConverter(int columnId)
        : ColumnId_(columnId)
    { }

    TConvertedColumn Convert(TRange<TUnversionedRowValues> rowsValues) override
    {
        auto rowCount = rowsValues.size();

        auto column = std::make_shared<TBatchColumn>();

        column->Id = ColumnId_;
        column->Type = SimpleLogicalType(ESimpleLogicalValueType::Null);
        column->ValueCount = rowCount;

        TOwningColumn owner = {
            .Column = std::move(column),
        };

        return {{owner}, owner.Column.get()};
    }

private:
    const int ColumnId_;
};

////////////////////////////////////////////////////////////////////////////////

IColumnConverterPtr CreateNullConverter(int columnId)
{
    return std::make_unique<TNullColumnWriterConverter>(columnId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnConverters
