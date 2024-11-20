#pragma once

#include <yt/yt/client/formats/public.h>

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/table_consumer.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <yt/yt/core/yson/consumer.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

class TYsonMapToUnversionedValueConverter
    : public NYson::TYsonConsumerBase
    , private NTableClient::IValueConsumer
{
public:
    TYsonMapToUnversionedValueConverter(
        const NComplexTypes::TYsonConverterConfig& config,
        NTableClient::IValueConsumer* valueConsumer);

    void Reset();
    void OnStringScalar(TStringBuf value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;
    void OnEntity() override;
    void OnBeginList() override;
    void OnListItem() override;
    void OnBeginMap() override;
    void OnKeyedItem(TStringBuf name) override;
    void OnEndMap() override;
    void OnBeginAttributes() override;
    void OnEndList() override;
    void OnEndAttributes() override;

private:
    const NTableClient::TNameTablePtr& GetNameTable() const override;
    bool GetAllowUnknownColumns() const override;
    void OnBeginRow() override;
    void OnValue(const NTableClient::TUnversionedValue& value) override;
    void OnEndRow() override;
    const NTableClient::TTableSchemaPtr& GetSchema() const override;

private:
    NTableClient::IValueConsumer* const Consumer_;
    const bool AllowUnknownColumns_;
    const NTableClient::TNameTablePtr NameTable_;

    NTableClient::TYsonToUnversionedValueConverter ColumnConsumer_;
    bool InsideValue_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
