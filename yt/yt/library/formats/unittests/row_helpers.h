#pragma once

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <vector>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCollectingValueConsumer
    : public NTableClient::IValueConsumer
{
public:
    explicit TCollectingValueConsumer(NTableClient::TTableSchemaPtr schema = New<NTableClient::TTableSchema>())
        : Schema_(std::move(schema))
    { }

    explicit TCollectingValueConsumer(NTableClient::TNameTablePtr nameTable, NTableClient::TTableSchemaPtr schema = New<NTableClient::TTableSchema>())
        : Schema_(std::move(schema))
        , NameTable_(std::move(nameTable))
    { }

    const NTableClient::TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    const NTableClient::TTableSchemaPtr& GetSchema() const override
    {
        return Schema_;
    }

    bool GetAllowUnknownColumns() const override
    {
        return true;
    }

    void OnBeginRow() override
    { }

    void OnValue(const NTableClient::TUnversionedValue& value) override
    {
        Builder_.AddValue(value);
    }

    void OnEndRow() override
    {
        RowList_.emplace_back(Builder_.FinishRow());
    }

    NTableClient::TUnversionedRow GetRow(size_t rowIndex)
    {
        return RowList_.at(rowIndex);
    }

    std::optional<NTableClient::TUnversionedValue> FindRowValue(size_t rowIndex, TStringBuf columnName) const
    {
        NTableClient::TUnversionedRow row = RowList_.at(rowIndex);
        auto id = GetNameTable()->GetIdOrThrow(columnName);

        for (const auto& value : row) {
            if (value.Id == id) {
                return value;
            }
        }
        return std::nullopt;
    }

    NTableClient::TUnversionedValue GetRowValue(size_t rowIndex, TStringBuf columnName) const
    {
        auto row = FindRowValue(rowIndex, columnName);
        if (!row) {
            THROW_ERROR_EXCEPTION("Cannot find column %Qv", columnName);
        }
        return *row;
    }

    size_t Size() const
    {
        return RowList_.size();
    }

    const std::vector<NTableClient::TUnversionedOwningRow>& GetRowList() const {
        return RowList_;
    }

private:
    const NTableClient::TTableSchemaPtr Schema_;
    const NTableClient::TNameTablePtr NameTable_ = New<NTableClient::TNameTable>();
    NTableClient::TUnversionedOwningRowBuilder Builder_;
    std::vector<NTableClient::TUnversionedOwningRow> RowList_;
};

////////////////////////////////////////////////////////////////////////////////

i64 GetInt64(const NTableClient::TUnversionedValue& value);
ui64 GetUint64(const NTableClient::TUnversionedValue& value);
double GetDouble(const NTableClient::TUnversionedValue& value);
bool GetBoolean(const NTableClient::TUnversionedValue& value);
TString GetString(const NTableClient::TUnversionedValue& value);
NYTree::INodePtr GetAny(const NTableClient::TUnversionedValue& value);
NYTree::INodePtr GetComposite(const NTableClient::TUnversionedValue& value);
bool IsNull(const NTableClient::TUnversionedValue& value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
