#pragma once

#include "table_client.h"

#include <yt/yt/client/table_client/config.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TDistributedWriteSessionStartOptions
    : public TTransactionalOptions
{ };

struct TDistributedWriteSessionFinishOptions
    : public TTransactionalOptions
{ };

struct TParticipantTableWriterOptions
    : public TTableWriterOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct IDistributedTableClientBase
{
    virtual ~IDistributedTableClientBase() = default;

    virtual TFuture<TDistributedWriteSessionPtr> StartDistributedWriteSession(
        const NYPath::TRichYPath& path,
        const TDistributedWriteSessionStartOptions& options = {}) = 0;

    virtual TFuture<void> FinishDistributedWriteSession(
        TDistributedWriteSessionPtr session,
        const TDistributedWriteSessionFinishOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IDistributedTableClient
{
    virtual ~IDistributedTableClient() = default;

    virtual TFuture<ITableWriterPtr> CreateParticipantTableWriter(
        const TDistributedWriteCookiePtr& cookie,
        const TParticipantTableWriterOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
