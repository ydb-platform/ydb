#include "yson_map_to_unversioned_value.h"

namespace NYT::NFormats {

using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TYsonMapToUnversionedValueConverter::TYsonMapToUnversionedValueConverter(
    const NComplexTypes::TYsonConverterConfig& config,
    IValueConsumer* consumer)
    : Consumer_(consumer)
    , AllowUnknownColumns_(consumer->GetAllowUnknownColumns())
    , NameTable_(consumer->GetNameTable())
    , ColumnConsumer_(config, this)
{ }

void TYsonMapToUnversionedValueConverter::Reset()
{
    YT_VERIFY(!InsideValue_);
}

void TYsonMapToUnversionedValueConverter::OnStringScalar(TStringBuf value)
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnStringScalar(value);
    } else {
        THROW_ERROR_EXCEPTION("YSON map expected");
    }
}

void TYsonMapToUnversionedValueConverter::OnInt64Scalar(i64 value)
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnInt64Scalar(value);
    } else {
        THROW_ERROR_EXCEPTION("YSON map expected");
    }
}

void TYsonMapToUnversionedValueConverter::OnUint64Scalar(ui64 value)
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnUint64Scalar(value);
    } else {
        THROW_ERROR_EXCEPTION("YSON map expected");
    }
}

void TYsonMapToUnversionedValueConverter::OnDoubleScalar(double value)
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnDoubleScalar(value);
    } else {
        THROW_ERROR_EXCEPTION("YSON map expected");
    }
}

void TYsonMapToUnversionedValueConverter::OnBooleanScalar(bool value)
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnBooleanScalar(value);
    } else {
        THROW_ERROR_EXCEPTION("YSON map expected");
    }
}

void TYsonMapToUnversionedValueConverter::OnEntity()
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnEntity();
    } else {
        THROW_ERROR_EXCEPTION("YSON map expected");
    }
}

void TYsonMapToUnversionedValueConverter::OnBeginList()
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnBeginList();
    } else {
        THROW_ERROR_EXCEPTION("YSON map expected");
    }
}

void TYsonMapToUnversionedValueConverter::OnListItem()
{
    YT_VERIFY(InsideValue_); // Should crash on BeginList()
    ColumnConsumer_.OnListItem();
}

void TYsonMapToUnversionedValueConverter::OnBeginMap()
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnBeginMap();
    }
}

void TYsonMapToUnversionedValueConverter::OnKeyedItem(TStringBuf name)
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnKeyedItem(name);
    } else {
        InsideValue_ = true;
        if (AllowUnknownColumns_) {
            ColumnConsumer_.SetColumnIndex(NameTable_->GetIdOrRegisterName(name));
        } else {
            auto id = NameTable_->FindId(name);
            if (!id) {
                THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::SchemaViolation, "No column %Qv in table schema",
                    name);
            }
            ColumnConsumer_.SetColumnIndex(*id);
        }
    }
}

void TYsonMapToUnversionedValueConverter::OnEndMap()
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnEndMap();
    }
}

void TYsonMapToUnversionedValueConverter::OnBeginAttributes()
{
    if (Y_LIKELY(InsideValue_)) {
        ColumnConsumer_.OnBeginAttributes();
    } else {
        THROW_ERROR_EXCEPTION("YSON map without attributes expected");
    }
}

void TYsonMapToUnversionedValueConverter::OnEndList()
{
    YT_VERIFY(InsideValue_); // Should throw on BeginList().
    ColumnConsumer_.OnEndList();
}

void TYsonMapToUnversionedValueConverter::OnEndAttributes()
{
    YT_VERIFY(InsideValue_); // Should throw on BeginAttributes()
    ColumnConsumer_.OnEndAttributes();
}

const TNameTablePtr& TYsonMapToUnversionedValueConverter::GetNameTable() const
{
    return Consumer_->GetNameTable();
}

bool TYsonMapToUnversionedValueConverter::GetAllowUnknownColumns() const
{
    YT_ABORT();
}

void TYsonMapToUnversionedValueConverter::OnBeginRow()
{
    YT_ABORT();
}

void TYsonMapToUnversionedValueConverter::OnValue(const TUnversionedValue& value)
{
    InsideValue_ = false;
    Consumer_->OnValue(value);
}

void TYsonMapToUnversionedValueConverter::OnEndRow()
{
    YT_ABORT();
}

const NTableClient::TTableSchemaPtr& TYsonMapToUnversionedValueConverter::GetSchema() const
{
    return Consumer_->GetSchema();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
