#pragma once

#include <library/cpp/testing/gtest_extensions/gtest_extensions.h>

#include <yt/yt/client/api/table_reader.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

class TMockTableReader
    : public ITableReader
{
public:
    TMockTableReader(const NTableClient::TTableSchemaPtr& schema);

    MOCK_METHOD(i64, GetStartRowIndex, (), (const, override));

    MOCK_METHOD(i64, GetTotalRowCount, (), (const, override));

    MOCK_METHOD(NChunkClient::NProto::TDataStatistics, GetDataStatistics, (), (const, override));

    MOCK_METHOD(TFuture<void>, GetReadyEvent, (), (override));

    MOCK_METHOD(NTableClient::IUnversionedRowBatchPtr, Read, (const NTableClient::TRowBatchReadOptions& options), (override));

    MOCK_METHOD(const std::vector<TString>&, GetOmittedInaccessibleColumns, (), (const, override));

    const NTableClient::TNameTablePtr& GetNameTable() const override;

    const NTableClient::TTableSchemaPtr& GetTableSchema() const override;

private:
    NTableClient::TTableSchemaPtr Schema_;
    NTableClient::TNameTablePtr NameTable_;
};

DEFINE_REFCOUNTED_TYPE(TMockTableReader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
