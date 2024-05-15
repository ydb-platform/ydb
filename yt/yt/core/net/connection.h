#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/net/address.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NNet {

////////////////////////////////////////////////////////////////////////////////

struct TConnectionStatistics
{
    TDuration IdleDuration;
    TDuration BusyDuration;
};

////////////////////////////////////////////////////////////////////////////////

struct IConnectionReader
    : public NConcurrency::IAsyncInputStream
{
    virtual TFuture<void> CloseRead() = 0;

    virtual TFuture<void> Abort() = 0;

    virtual int GetHandle() const = 0;

    virtual i64 GetReadByteCount() const = 0;

    virtual void SetReadDeadline(std::optional<TInstant> deadline) = 0;

    virtual TConnectionStatistics GetReadStatistics() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IConnectionReader)

////////////////////////////////////////////////////////////////////////////////

struct IConnectionWriter
    : public NConcurrency::IAsyncOutputStream
{
    virtual TFuture<void> WriteV(const TSharedRefArray& data) = 0;

    virtual TFuture<void> CloseWrite() = 0;

    virtual TFuture<void> Abort() = 0;

    virtual int GetHandle() const = 0;

    virtual i64 GetWriteByteCount() const = 0;

    virtual void SetWriteDeadline(std::optional<TInstant> deadline) = 0;

    virtual TConnectionStatistics GetWriteStatistics() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IConnectionWriter)

////////////////////////////////////////////////////////////////////////////////

struct IConnection
    : public IConnectionReader
    , public IConnectionWriter
{
    virtual const TNetworkAddress& LocalAddress() const = 0;
    virtual const TNetworkAddress& RemoteAddress() const = 0;

    // Returns true if connection is not is failed state and has no
    // active IO operations.
    virtual bool IsIdle() const = 0;

    virtual bool SetNoDelay() = 0;
    virtual bool SetKeepAlive() = 0;

    TFuture<void> Abort() override = 0;

    // SubscribePeerDisconnect is best effort and is not guaranteed to fire.
    virtual void SubscribePeerDisconnect(TCallback<void()> cb) = 0;
};

DEFINE_REFCOUNTED_TYPE(IConnection)

////////////////////////////////////////////////////////////////////////////////

std::pair<IConnectionPtr, IConnectionPtr> CreateConnectionPair(NConcurrency::IPollerPtr poller);

//! File descriptor must be in nonblocking mode.
IConnectionPtr CreateConnectionFromFD(
    TFileDescriptor fd,
    const TNetworkAddress& localAddress,
    const TNetworkAddress& remoteAddress,
    NConcurrency::IPollerPtr poller);

IConnectionReaderPtr CreateInputConnectionFromFD(
    TFileDescriptor fd,
    TString pipePath,
    NConcurrency::IPollerPtr poller,
    const TRefCountedPtr& pipeHolder);

IConnectionReaderPtr CreateInputConnectionFromPath(
    TString pipePath,
    NConcurrency::IPollerPtr poller,
    const TRefCountedPtr& pipeHolder);

IConnectionWriterPtr CreateOutputConnectionFromPath(
    TString pipePath,
    NConcurrency::IPollerPtr poller,
    const TRefCountedPtr& pipeHolder,
    std::optional<int> capacity = {},
    bool useDeliveryFence = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
