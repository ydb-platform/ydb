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
    virtual void OnStringScalar(TStringBuf value) override;
    virtual void OnInt64Scalar(i64 value) override;
    virtual void OnUint64Scalar(ui64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnBooleanScalar(bool value) override;
    virtual void OnEntity() override;
    virtual void OnBeginList() override;
    virtual void OnListItem() override;
    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(TStringBuf name) override;
    virtual void OnEndMap() override;
    virtual void OnBeginAttributes() override;
    virtual void OnEndList() override;
    virtual void OnEndAttributes() override;

private:
    virtual const NTableClient::TNameTablePtr& GetNameTable() const override;
    virtual bool GetAllowUnknownColumns() const override;
    virtual void OnBeginRow() override;
    virtual void OnValue(const NTableClient::TUnversionedValue& value) override;
    virtual void OnEndRow() override;
    virtual const NTableClient::TTableSchemaPtr& GetSchema() const override;

private:
    NTableClient::IValueConsumer* const Consumer_;
    const bool AllowUnknownColumns_;
    const NTableClient::TNameTablePtr NameTable_;

    NTableClient::TYsonToUnversionedValueConverter ColumnConsumer_;
    bool InsideValue_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
