#pragma once

#include "client_common.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TShuffleHandle
    : public NYTree::TYsonStruct
{
    TGuid TransactionId;
    TString CoordinatorAddress;
    TString Account;
    int PartitionCount;

    REGISTER_YSON_STRUCT(TShuffleHandle);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TShuffleHandle)

////////////////////////////////////////////////////////////////////////////////

struct TStartShuffleOptions
    : public TTimeoutOptions
{ };

struct TFinishShuffleOptions
    : public TTimeoutOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct IShuffleClient
{
    virtual ~IShuffleClient() = default;

    virtual TFuture<TShuffleHandlePtr> StartShuffle(
        const TString& account,
        int partitionCount,
        const TStartShuffleOptions& options) = 0;

    virtual TFuture<void> FinishShuffle(
        const TShuffleHandlePtr& shuffleHandle,
        const TFinishShuffleOptions& options) = 0;

    virtual TFuture<IRowBatchReaderPtr> CreateShuffleReader(
        const TShuffleHandlePtr& shuffleHandle,
        int partitionIndex,
        const NTableClient::TTableReaderConfigPtr& config = New<NTableClient::TTableReaderConfig>()) = 0;

    virtual TFuture<IRowBatchWriterPtr> CreateShuffleWriter(
        const TShuffleHandlePtr& shuffleHandle,
        const TString& partitionColumn,
        const NTableClient::TTableWriterConfigPtr& config = New<NTableClient::TTableWriterConfig>()) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
