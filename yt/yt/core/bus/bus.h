#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/net/public.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

#define ITERATE_BUS_NETWORK_STATISTICS_COUNTER_FIELDS(func) \
    func(InBytes, in_bytes) \
    func(InPackets, in_packets) \
    \
    func(OutBytes, out_bytes) \
    func(OutPackets, out_packets) \
    \
    func(StalledReads, stalled_reads) \
    func(StalledWrites, stalled_writes) \
    \
    func(ReadErrors, read_errors) \
    func(WriteErrors, write_errors) \
    \
    func(Retransmits, retransmits) \
    \
    func(EncoderErrors, encoder_errors) \
    func(DecoderErrors, decoder_errors)

#define ITERATE_BUS_NETWORK_STATISTICS_GAUGE_FIELDS(func) \
    func(PendingOutPackets, pending_out_packets) \
    func(PendingOutBytes, pending_out_bytes) \
    \
    func(ClientConnections, client_connections) \
    func(ServerConnections, server_connections)

#define ITERATE_BUS_NETWORK_STATISTICS_FIELDS(func) \
    ITERATE_BUS_NETWORK_STATISTICS_COUNTER_FIELDS(func) \
    ITERATE_BUS_NETWORK_STATISTICS_GAUGE_FIELDS(func)

struct TBusNetworkStatistics
{
    #define XX(camelCaseField, snakeCaseField) i64 camelCaseField = 0;
    ITERATE_BUS_NETWORK_STATISTICS_FIELDS(XX)
    #undef XX
};

////////////////////////////////////////////////////////////////////////////////

struct TSendOptions
{
    static constexpr int AllParts = std::numeric_limits<int>::max();

    EDeliveryTrackingLevel TrackingLevel = EDeliveryTrackingLevel::None;
    int ChecksummedPartCount = AllParts;
    bool EnableSendCancelation = false;
};

//! A bus, i.e. something capable of transmitting messages.
struct IBus
    : public virtual TRefCounted
{
    //! Returns a textual representation of the bus' endpoint.
    //! Typically used for logging.
    virtual const TString& GetEndpointDescription() const = 0;

    //! Returns the bus' endpoint attributes.
    //! Typically used for constructing errors.
    virtual const NYTree::IAttributeDictionary& GetEndpointAttributes() const = 0;

    //! Returns the bus' endpoint address as it was provided by the configuration (e.g.: non-resolved FQDN).
    //! Empty if it is not supported by the implementation (e.g.: Unix sockets).
    virtual const std::string& GetEndpointAddress() const = 0;

    //! Returns the bus' endpoint network address (e.g. a resolved IP address).
    //! Null if it is not supported by the implementation (e.g. for a client-side bus).
    virtual const NNet::TNetworkAddress& GetEndpointNetworkAddress() const = 0;

    //! Returns the statistics for the whole network this bus is bound to.
    virtual TBusNetworkStatistics GetNetworkStatistics() const = 0;

    //! Returns true if the bus' endpoint belongs to our process.
    virtual bool IsEndpointLocal() const = 0;

    //! Returns true if the bus' communication is encrypted.
    virtual bool IsEncrypted() const = 0;

    //! Returns a future indicating the moment when the bus is actually ready to send messages.
    /*!
     *  Some bus implementations are not immediately ready upon creation. E.g. a client socket
     *  needs to perform a DNS resolve for its underlying address and establish a connection
     *  before messages can actually go through. #IBus::Send can still be invoked before
     *  these background activities are finished but for the sake of diagnostics it is sometimes
     *  beneficial to catch the exact moment the connection becomes ready.
     */
    virtual TFuture<void> GetReadyFuture() const = 0;

    //! Asynchronously sends a message via the bus.
    /*!
     *  \param message A message to send.
     *  \return An asynchronous flag indicating if the delivery (not the processing!) of the message
     *  was successful.
     *
     *  Underlying transport may support delivery cancellation. In that case, when returned future is cancelled,
     *  message is dropped from the send queue.
     *
     *  \note Thread affinity: any
     */
    virtual TFuture<void> Send(TSharedRefArray message, const TSendOptions& options = {}) = 0;

    //! For socket buses, updates the TOS level.
    /*!
     *  \note Thread affinity: any
     */
    virtual void SetTosLevel(TTosLevel tosLevel) = 0;

    //! Terminates the bus.
    /*!
     *  Does not block -- termination typically happens in background.
     *  It is safe to call this method multiple times.
     *  On terminated the instance is no longer usable.

     *  \note Thread affinity: any.
     */
    virtual void Terminate(const TError& error) = 0;

    //! Invoked upon bus termination
    //! (either due to call to #Terminate or other party's failure).
    DECLARE_INTERFACE_SIGNAL(void(const TError&), Terminated);
};

DEFINE_REFCOUNTED_TYPE(IBus)

////////////////////////////////////////////////////////////////////////////////

//! Handles incoming bus messages.
struct IMessageHandler
    : public virtual TRefCounted
{
    //! Raised whenever a new message arrives via the bus.
    /*!
     *  \param message The just arrived message.
     *  \param replyBus A bus that can be used for replying back.
     *
     *  \note
     *  Thread affinity: this method is called from an unspecified thread
     *  and must return ASAP. No context switch or fiber cancelation is possible.
     *
     */
    virtual void HandleMessage(TSharedRefArray message, IBusPtr replyBus) noexcept = 0;
};

DEFINE_REFCOUNTED_TYPE(IMessageHandler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
