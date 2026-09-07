#pragma once

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/thread.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/verify.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/network/sock.h>
#include <util/stream/file.h>
#include <util/stream/format.h>
#include <util/stream/str.h>
#include <util/system/spinlock.h>
#include <util/system/thread.h>

namespace NYdb::NBS::NClient {

using NYdb::NBS::E_GRPC_UNAVAILABLE;
using NYdb::NBS::HasError;
using NYdb::NBS::TErrorResponse;

////////////////////////////////////////////////////////////////////////////////

template <typename TBase, typename TCallContext>
class TUdsSocketClient
    : public TBase
    , public std::enable_shared_from_this<TUdsSocketClient<TBase, TCallContext>>
{
    using TThis = TUdsSocketClient<TBase, TCallContext>;

    enum
    {
        Disconnected = 0,
        Connecting = 1,
        Connected = 2,
    };

    union TState {
        TState(ui64 value)
            : Raw(value)
        {}

        struct
        {
            ui32 InflightCounter;
            ui32 ConnectionState;
        };

        ui64 Raw;
    };

private:
    const TString SocketPath;

    TAtomic State = 0;

public:
    template <typename... TArgs>
    TUdsSocketClient(TString socketPath, TArgs&&... args)
        : TBase(std::forward<TArgs>(args)...)
        , SocketPath(std::move(socketPath))
    {}

    void Connect()
    {
        TState currentState = AtomicGet(State);
        if (currentState.InflightCounter != 0) {
            return;
        }

        if (!SetConnectionState(Connecting, Disconnected)) {
            return;
        }

        auto res = TBase::StartWithUds(SocketPath);

        res = SetConnectionState(res ? Connected : Disconnected, Connecting);

        STORAGE_VERIFY(res, "Socket", SocketPath);
    }

protected:
    template <typename TMethod>
    NThreading::TFuture<typename TMethod::TResponse> ExecuteRequest(
        TCallContext callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        if (!IncInflightCounter()) {
            Connect();
            return NThreading::MakeFuture<typename TMethod::TResponse>(
                TErrorResponse(E_GRPC_UNAVAILABLE, "Broken pipe"));
        }

        auto future = TBase::template ExecuteRequest<TMethod>(
            std::move(callContext),
            std::move(request));

        auto weakPtr = TThis::weak_from_this();
        return future.Apply(
            [weakPtr = std::move(weakPtr)](const auto& f)
            {
                if (auto p = weakPtr.lock()) {
                    p->HandleResponse(f.GetValue());
                }
                return f;
            });
    }

    bool SetConnectionState(ui32 next, ui32 prev)
    {
        while (true) {
            TState currentState = AtomicGet(State);
            if (currentState.ConnectionState != prev) {
                return false;
            }

            TState nextState = currentState;
            nextState.ConnectionState = next;

            if (AtomicCas(&State, nextState.Raw, currentState.Raw)) {
                return true;
            }
        }
    }

    bool IncInflightCounter()
    {
        while (true) {
            TState currentState = AtomicGet(State);
            if (currentState.ConnectionState != Connected) {
                return false;
            }

            TState nextState = currentState;
            ++nextState.InflightCounter;

            if (AtomicCas(&State, nextState.Raw, currentState.Raw)) {
                return true;
            }
        }
    }

    void DecInflightCounter()
    {
        // reduce InflightCounter, don't change ConnectionState
        TState oldState = AtomicGetAndDecrement(State);

        STORAGE_VERIFY(oldState.InflightCounter > 0, "Socket", SocketPath);
    }

private:
    template <typename TResponse>
    void HandleResponse(const TResponse& response)
    {
        if (response.HasError() &&
            response.GetError().GetCode() == E_GRPC_UNAVAILABLE)
        {
            SetConnectionState(Disconnected, Connected);
        }

        DecInflightCounter();
    }
};

}   // namespace NYdb::NBS::NClient
