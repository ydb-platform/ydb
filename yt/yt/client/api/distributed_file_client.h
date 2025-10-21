#include "file_client.h"

#include <yt/yt/client/file_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

// Contains the session host data and a set of signed cookies for individual file fragment writes.
//
// #Session: signed session host data.
// #Cookies: signed session cookies per requested participants.
//           Note: not all cookies need to be used.
struct TDistributedWriteFileSessionWithCookies
    : public NYTree::TYsonStructLite
{
    TSignedDistributedWriteFileSessionPtr Session;
    std::vector<TSignedWriteFileFragmentCookiePtr> Cookies;

    REGISTER_YSON_STRUCT_LITE(TDistributedWriteFileSessionWithCookies)

    static void Register(TRegistrar registrar);
};

// TODO(achains): Break this class into two separate parameters
struct TDistributedWriteFileSessionWithResults
{
    TSignedDistributedWriteFileSessionPtr Session;
    std::vector<TSignedWriteFileFragmentResultPtr> Results;
};

////////////////////////////////////////////////////////////////////////////////

struct TDistributedWriteFileSessionStartOptions
    : public TTransactionalOptions
    , public TTimeoutOptions
{
    // Number of participants for individual file fragment writes.
    int CookieCount = 0;
    // Timeout for session. Similar to transaction timeout.
    std::optional<TDuration> Timeout;
};

struct TDistributedWriteFileSessionPingOptions
    : public TTimeoutOptions
{ };

struct TDistributedWriteFileSessionFinishOptions
    : public TTimeoutOptions
{ };

// Options for file fragment writer in distributed write file session.
// Note: MD5 and prerequisite transactions are not yet supported.
struct TFileFragmentWriterOptions
    : public TFileWriterOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct IDistributedFileClientBase
{
    virtual ~IDistributedFileClientBase() = default;

    virtual TFuture<TDistributedWriteFileSessionWithCookies> StartDistributedWriteFileSession(
        const NYPath::TRichYPath& path,
        const TDistributedWriteFileSessionStartOptions& options = {}) = 0;

    virtual TFuture<void> PingDistributedWriteFileSession(
        const TSignedDistributedWriteFileSessionPtr& session,
        const TDistributedWriteFileSessionPingOptions& options = {}) = 0;

    // Finish distributed session with merging all write fragment results.
    //
    // The write happens by merging fragments in the order they appear in #Results.
    //
    // #Session: signed session host data.
    // #Results: signed results of fragment writes.
    virtual TFuture<void> FinishDistributedWriteFileSession(
        const TDistributedWriteFileSessionWithResults& sessionWithResults,
        const TDistributedWriteFileSessionFinishOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IDistributedFileClient
{
    virtual ~IDistributedFileClient() = default;

    // Creates distributed file fragment writer for given cookie.
    //
    // Same cookie may be reused for creating multiple fragment writers.
    // Every new writer instance writes data to its independant root chunk list.
    virtual IFileFragmentWriterPtr CreateFileFragmentWriter(
        const TSignedWriteFileFragmentCookiePtr& cookie,
        const TFileFragmentWriterOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
