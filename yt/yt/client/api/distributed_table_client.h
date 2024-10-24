#pragma once

#include "table_client.h"

#include <yt/yt/client/table_client/config.h>

#include <library/cpp/yt/misc/non_null_ptr.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TDistributedWriteSessionStartOptions
    : public TTransactionalOptions
{ };

struct TDistributedWriteSessionFinishOptions
{
    int MaxChildrenPerAttachRequest = 10'000;
};

struct TFragmentTableWriterOptions
    : public TTableWriterOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct IDistributedTableClientBase
{
public:
    virtual ~IDistributedTableClientBase() = default;

    virtual TFuture<TDistributedWriteSessionPtr> StartDistributedWriteSession(
        const NYPath::TRichYPath& path,
        const TDistributedWriteSessionStartOptions& options = {}) = 0;

    virtual TFuture<void> FinishDistributedWriteSession(
        TDistributedWriteSessionPtr session,
        const TDistributedWriteSessionFinishOptions& options = {}) = 0;

    // Helper used to implement FinishDistributedWriteSession efficiently
    // without compromising privacy of session fields.
    // defined in yt/yt/client/api/distributed_table_session.cpp.
    void* GetOpaqueDistributedWriteResults(Y_LIFETIME_BOUND const TDistributedWriteSessionPtr& session);
    // Used in chunk writer for results recording
    void RecordOpaqueWriteResult(const TFragmentWriteCookiePtr& cookie, void* opaqueWriteResult);
};

////////////////////////////////////////////////////////////////////////////////

struct IDistributedTableClient
{
    virtual ~IDistributedTableClient() = default;

    virtual TFuture<ITableWriterPtr> CreateFragmentTableWriter(
        const TFragmentWriteCookiePtr& cookie,
        const TFragmentTableWriterOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
