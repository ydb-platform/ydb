#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/formats/format.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

//! An instance of driver request.
struct TDriverRequest
{
    TDriverRequest();
    explicit TDriverRequest(TRefCountedPtr holder);

    //! Request identifier to be logged.
    std::variant<ui64, TGuid> Id = static_cast<ui64>(0);

    //! Command name to execute.
    TString CommandName;

    //! Stream used for reading command input.
    //! The stream must stay alive for the duration of #IDriver::Execute.
    NConcurrency::IAsyncZeroCopyInputStreamPtr InputStream;

    //! Stream where the command output is written.
    //! The stream must stay alive for the duration of #IDriver::Execute.
    NConcurrency::IFlushableAsyncOutputStreamPtr OutputStream;

    //! A map containing command parameters.
    NYTree::IMapNodePtr Parameters;

    //! Name of the user issuing the request.
    TString AuthenticatedUser = NSecurityClient::RootUserName;

    //! Provides an additional annotation to differentiate between
    //! various clients that authenticate via the same effective user.
    std::optional<TString> UserTag;

    //! Filled in the context of http proxy.
    std::optional<NNet::TNetworkAddress> UserRemoteAddress;

    //! User token.
    std::optional<TString> UserToken;

    //! TVM service ticket.
    std::optional<TString> ServiceTicket;

    //! Additional logging tags.
    std::optional<TString> LoggingTags;

    //! Provides means to return arbitrary structured data from any command.
    //! Must be filled before writing data to output stream.
    NYson::IYsonConsumer* ResponseParametersConsumer;

    //! Invoked after driver is done producing response parameters and
    //! before first write to output stream.
    std::function<void()> ResponseParametersFinishedCallback;

    void Reset();

private:
    using THolderPtr = TRefCountedPtr;
    THolderPtr Holder_;
};

////////////////////////////////////////////////////////////////////////////////

//! Command meta-descriptor.
/*!
 *  Contains various meta-information describing a given command type.
 */
struct TCommandDescriptor
{
    //! Name of the command.
    TString CommandName;

    //! Type of data expected by the command at #TDriverRequest::InputStream.
    NFormats::EDataType InputType;

    //! Type of data written by the command to #TDriverRequest::OutputStream.
    NFormats::EDataType OutputType;

    //! Whether the command affects the state of the cluster.
    bool Volatile;

    //! Whether the execution of a command is lengthy and/or causes a heavy load.
    bool Heavy;
};

void Serialize(const TCommandDescriptor& descriptor, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

//! An instance of command execution engine.
/*!
 *  Each driver instance maintains a collection of cached connections to
 *  various YT subsystems (e.g. masters, scheduler).
 *
 *  IDriver instances are thread-safe and reentrant.
 */
struct IDriver
    : public virtual TRefCounted
{
    //! Asynchronously executes a given request.
    virtual TFuture<void> Execute(const TDriverRequest& request) = 0;

    //! Returns a descriptor for the command with a given name or
    //! Null if no command with this name is registered.
    virtual std::optional<TCommandDescriptor> FindCommandDescriptor(const TString& commandName) const = 0;

    //! Returns a descriptor for then command with a given name.
    //! Fails if no command with this name is registered.
    TCommandDescriptor GetCommandDescriptor(const TString& commandName) const;

    //! Returns a descriptor for then command with a given name.
    //! Throws if no command with this name is registered.
    TCommandDescriptor GetCommandDescriptorOrThrow(const TString& commandName) const;

    //! Returns the list of descriptors for all supported commands.
    virtual const std::vector<TCommandDescriptor> GetCommandDescriptors() const = 0;

    virtual void ClearMetadataCaches() = 0;

    //! Returns the pool of sticky transactions stored in the driver.
    virtual NApi::IStickyTransactionPoolPtr GetStickyTransactionPool() = 0;

    //! Returns the cache for proxy discovery requests.
    virtual IProxyDiscoveryCachePtr GetProxyDiscoveryCache() = 0;

    //! Returns the underlying connection.
    virtual NApi::IConnectionPtr GetConnection() = 0;

    //! Terminates the underlying connection.
    virtual void Terminate() = 0;
};

DEFINE_REFCOUNTED_TYPE(IDriver)

////////////////////////////////////////////////////////////////////////////////

IDriverPtr CreateDriver(
    NApi::IConnectionPtr connection,
    TDriverConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver

