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
    int PartitionCount;

    REGISTER_YSON_STRUCT(TShuffleHandle);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TShuffleHandle)

////////////////////////////////////////////////////////////////////////////////

struct TStartShuffleOptions
    : public TTimeoutOptions
{ };

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
