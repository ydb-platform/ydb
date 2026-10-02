#pragma once

#include "table_client.h"

#include <yt/yt/client/signature/public.h>

#include <yt/yt/client/table_client/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TDistributedWriteSessionWithCookies
    : public NYTree::TYsonStructLite
{
    //! TDistributedWriteSession.
    TSignedDistributedWriteSessionPtr Session;
    //! std::vector<TWriteFragmentCookie>.
    std::vector<TSignedWriteFragmentCookiePtr> Cookies;

    REGISTER_YSON_STRUCT_LITE(TDistributedWriteSessionWithCookies)

    static void Register(TRegistrar registrar);
};

struct TDistributedWriteSessionWithResults
{
    //! TDistributedWriteSession.
    TSignedDistributedWriteSessionPtr Session;
    //! std::vector<TWriteFragmentResult>.
    std::vector<TSignedWriteFragmentResultPtr> Results;
};

////////////////////////////////////////////////////////////////////////////////

struct TDistributedWriteSessionStartOptions
    : public TTransactionalOptions
    , public TTimeoutOptions
{
    int CookieCount = 0;
    //! Timeout for session. Similar to transaction timeout.
    //! This option should not be confused with "Timeout",
    //! which is used to specify the maximum execution time of an API call.
    std::optional<TDuration> SessionTimeout;
};

struct TDistributedWriteSessionPingOptions
    : public TTimeoutOptions
{ };

struct TDistributedWriteSessionFinishOptions
    : public TTimeoutOptions
{ };

struct TTableFragmentWriterOptions
    : public TTableWriterOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct IDistributedTableClientBase
{
    virtual ~IDistributedTableClientBase() = default;

    virtual TFuture<TDistributedWriteSessionWithCookies> StartDistributedWriteSession(
        const NYPath::TRichYPath& path,
        const TDistributedWriteSessionStartOptions& options = {}) = 0;

    virtual TFuture<void> PingDistributedWriteSession(
        TSignedDistributedWriteSessionPtr session,
        const TDistributedWriteSessionPingOptions& options = {}) = 0;

    virtual TFuture<void> FinishDistributedWriteSession(
        const TDistributedWriteSessionWithResults& sessionWithResults,
        const TDistributedWriteSessionFinishOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IDistributedTableClient
{
    virtual ~IDistributedTableClient() = default;

    virtual TFuture<ITableFragmentWriterPtr> CreateTableFragmentWriter(
        const TSignedWriteFragmentCookiePtr& cookie,
        const TTableFragmentWriterOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
