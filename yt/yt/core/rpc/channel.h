#pragma once

#include "public.h"

#include "protocol_version.h"

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/bus/client.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Encapsulates a portion of streaming data.
struct TStreamingPayload
{
    NCompression::ECodec Codec;
    int SequenceNumber;
    std::vector<TSharedRef> Attachments;
};

//! Encapsulates a progress on reading streaming data.
struct TStreamingFeedback
{
    ssize_t ReadPosition;
};

////////////////////////////////////////////////////////////////////////////////

//! Controls how attachments are being streamed between clients and services.
struct TStreamingParameters
{
    //! Approximate limit for the number of bytes in flight.
    ssize_t WindowSize = 16_MB;
    //! Timeout for reads from attachments input stream.
    std::optional<TDuration> ReadTimeout;
    //! Timeout for writes to attachments output stream.
    std::optional<TDuration> WriteTimeout;
};

////////////////////////////////////////////////////////////////////////////////

//! Controls the lifetime of a request sent via IChannel::Send.
struct IClientRequestControl
    : public virtual TRefCounted
{
    //! Cancels the request.
    /*!
     *  An implementation is free to ignore cancellations.
     */
    virtual void Cancel() = 0;

    //! Sends the streaming request attachments to the service.
    virtual TFuture<void> SendStreamingPayload(const TStreamingPayload& payload) = 0;

    //! Notifies the service of the client's progress in reading the streaming response attachments.
    virtual TFuture<void> SendStreamingFeedback(const TStreamingFeedback& feedback) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClientRequestControl)

////////////////////////////////////////////////////////////////////////////////

struct TSendOptions
{
    std::optional<TDuration> Timeout;
    std::optional<TDuration> AcknowledgementTimeout;
    bool GenerateAttachmentChecksums = true;
    bool RequestHeavy = false;
    EMultiplexingBand MultiplexingBand = EMultiplexingBand::Default;
    int MultiplexingParallelism = 1;
    // For testing purposes only.
    std::optional<TDuration> SendDelay;
};

//! An interface for exchanging request-response pairs.
/*!
 * \note Thread affinity: any.
 */
struct IChannel
    : public virtual TRefCounted
{
    //! Returns a textual representation of the channel's endpoint.
    //! Typically used for logging.
    virtual const TString& GetEndpointDescription() const = 0;

    //! Returns the channel's endpoint attributes.
    //! Typically used for constructing errors.
    virtual const NYTree::IAttributeDictionary& GetEndpointAttributes() const = 0;

    //! Sends a request via the channel.
    /*!
     *  \param request A request to send.
     *  \param responseHandler An object that will handle a response.
     *  \param timeout Request processing timeout.
     *  \returns an object controlling the lifetime of the request;
     *  the latter could be |nullptr| if no control is supported by the implementation in general
     *  or for this particular request.
     */
    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) = 0;

    //! Shuts down the channel.
    /*!
     *  It is safe to call this method multiple times.
     *  After the first call the instance is no longer usable.
     */
    virtual void Terminate(const TError& error) = 0;

    //! Raised whenever the channel is terminated.
    DECLARE_INTERFACE_SIGNAL(void(const TError&), Terminated);

    virtual int GetInflightRequestCount() = 0;

    virtual const IMemoryUsageTrackerPtr& GetChannelMemoryTracker() = 0;
};

DEFINE_REFCOUNTED_TYPE(IChannel)

////////////////////////////////////////////////////////////////////////////////

//! Provides means for parsing addresses and creating channels.
struct IChannelFactory
    : public virtual TRefCounted
{
    virtual IChannelPtr CreateChannel(const TString& address) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChannelFactory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
