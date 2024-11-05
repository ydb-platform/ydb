#pragma once

#include "public.h"
#include "hedging_channel.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

//! A registry maintaining a set of viable peers.
//! Maintains no more than a configured number of channels, keeping the remaining addresses in a backlog.
//! When peers with active channels are unregistered, their places are filled with addresses from the backlog.
/*
 *  Thread affinity: any.
 */
struct IViablePeerRegistry
    : public TRefCounted
{
    //! Returns nullptr if the registry is empty.
    virtual IChannelPtr PickRandomChannel(
        const IClientRequestPtr& request,
        const std::optional<THedgingChannelOptions>& hedgingOptions) const = 0;
    //! Returns nullptr if the registry is empty.
    virtual IChannelPtr PickStickyChannel(const IClientRequestPtr& request) const = 0;
    //! Returns nullptr if this address is not an active peer.
    virtual IChannelPtr GetChannel(const std::string& address) const = 0;

    //! Returns true if a new peer was actually registered and false if it already existed.
    virtual bool RegisterPeer(const std::string& address) = 0;
    //! Returns true if this peer was actually unregistered.
    virtual bool UnregisterPeer(const std::string& address) = 0;
    //! Unregisters stored active peer with the given channel.
    //! Does nothing and returns false if the peer is no longer active
    //! or if the currently stored channel differs from the one given.
    virtual bool UnregisterChannel(const std::string& address, const IChannelPtr& channel) = 0;
    //! Tries to evict a random active peer and replace it with a peer from the backlog.
    //! The evicted peer is stored in the backlog instead.
    //! Returns the address of the rotated peer.
    virtual std::optional<std::string> MaybeRotateRandomPeer() = 0;

    virtual std::vector<IChannelPtr> GetActiveChannels() const = 0;
    virtual void Clear() = 0;

    //! If registry is non-empty, returns void future.
    //! Otherwise, returns an uncancellable future that is set either
    //!   - successfully, if RegisterPeer() is called;
    //!   - to a corresponding error in case of SetError() call, e.g. due to external peer discovery logic error;
    //!   - to a "promise abandoned" error if Clear() is called.
    virtual TFuture<void> GetPeersAvailable() const = 0;

    //! Sets the peer availability promise with the given error.
    virtual void SetError(const TError& error) = 0;
};

DEFINE_REFCOUNTED_TYPE(IViablePeerRegistry)

using TCreateChannelCallback = TCallback<IChannelPtr(const std::string& address)>;

IViablePeerRegistryPtr CreateViablePeerRegistry(
    TViablePeerRegistryConfigPtr config,
    TCreateChannelCallback createChannel,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
