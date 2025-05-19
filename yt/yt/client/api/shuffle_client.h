#pragma once

#include "client_common.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TShuffleHandle
    : public NYTree::TYsonStruct
{
    NObjectClient::TTransactionId TransactionId;
    std::string CoordinatorAddress;
    std::string Account;
    std::string Medium;
    int PartitionCount;
    int ReplicationFactor;

    REGISTER_YSON_STRUCT(TShuffleHandle);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TShuffleHandle)

void FormatValue(TStringBuilderBase* builder, const TShuffleHandlePtr& shuffleHandle, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

struct TStartShuffleOptions
    : public TTimeoutOptions
{
    std::optional<std::string> Medium;
    std::optional<int> ReplicationFactor;
};

struct TShuffleReaderOptions
{
    NTableClient::TTableReaderConfigPtr Config;
};

struct TShuffleWriterOptions
{
    NTableClient::TTableWriterConfigPtr Config;
    bool OverwriteExistingWriterData = false;
};

////////////////////////////////////////////////////////////////////////////////

struct IShuffleClient
{
    virtual ~IShuffleClient() = default;

    virtual TFuture<TShuffleHandlePtr> StartShuffle(
        const std::string& account,
        int partitionCount,
        NObjectClient::TTransactionId parentTransactionId,
        const TStartShuffleOptions& options) = 0;

    virtual TFuture<IRowBatchReaderPtr> CreateShuffleReader(
        const TShuffleHandlePtr& shuffleHandle,
        int partitionIndex,
        std::optional<std::pair<int, int>> writerIndexRange = {},
        const TShuffleReaderOptions& options = {}) = 0;

    virtual TFuture<IRowBatchWriterPtr> CreateShuffleWriter(
        const TShuffleHandlePtr& shuffleHandle,
        const std::string& partitionColumn,
        std::optional<int> writerIndex = {},
        const TShuffleWriterOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
