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
        const NTableClient::TTableReaderConfigPtr& config = New<NTableClient::TTableReaderConfig>()) = 0;

    virtual TFuture<IRowBatchWriterPtr> CreateShuffleWriter(
        const TShuffleHandlePtr& shuffleHandle,
        const std::string& partitionColumn,
        const NTableClient::TTableWriterConfigPtr& config = New<NTableClient::TTableWriterConfig>()) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
