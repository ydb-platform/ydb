#pragma once

#include <library/cpp/testing/gtest_extensions/gtest_extensions.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/value_consumer.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TMockValueConsumer
    : public TValueConsumerBase
{
public:
    TMockValueConsumer(
        TNameTablePtr nameTable,
        bool allowUnknownColumns,
        TTableSchemaPtr schema = New<TTableSchema>(),
        TTypeConversionConfigPtr typeConversionConfig = New<TTypeConversionConfig>())
        : TValueConsumerBase(std::move(schema), std::move(typeConversionConfig))
        , NameTable_(nameTable)
        , AllowUnknowsColumns_(allowUnknownColumns)
    {
        InitializeIdToTypeMapping();
    }

    MOCK_METHOD(void, OnBeginRow, (), (override));
    MOCK_METHOD(void, OnMyValue, (const TUnversionedValue& value), (override));
    MOCK_METHOD(void, OnEndRow, (), (override));

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    bool GetAllowUnknownColumns() const override
    {
        return AllowUnknowsColumns_;
    }

private:
    TNameTablePtr NameTable_;
    bool AllowUnknowsColumns_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
