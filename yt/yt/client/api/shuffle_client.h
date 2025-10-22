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

YT_DEFINE_STRONG_TYPEDEF(TSignedShuffleHandlePtr, NSignature::TSignaturePtr);

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
    using TIndexRange = std::pair<int, int>;

    virtual ~IShuffleClient() = default;

    virtual TFuture<TSignedShuffleHandlePtr> StartShuffle(
        const std::string& account,
        int partitionCount,
        NObjectClient::TTransactionId parentTransactionId,
        const TStartShuffleOptions& options) = 0;

    virtual TFuture<IRowBatchReaderPtr> CreateShuffleReader(
        const TSignedShuffleHandlePtr& shuffleHandle,
        int partitionIndex,
        std::optional<TIndexRange> writerIndexRange = {},
        const TShuffleReaderOptions& options = {}) = 0;

    virtual TFuture<IRowBatchWriterPtr> CreateShuffleWriter(
        const TSignedShuffleHandlePtr& shuffleHandle,
        const std::string& partitionColumn,
        std::optional<int> writerIndex = {},
        const TShuffleWriterOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
