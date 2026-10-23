#include "endpoint_poller.h"

#include "socket_poller.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/thread.h>

#include <util/folder/path.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/network/nonblock.h>
#include <util/network/pair.h>
#include <util/network/sock.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>

#include <array>

namespace NYdb::NBS::NServer {

using NYdb::NBS::E_INVALID_STATE;
using NYdb::NBS::FACILITY_BLOCKSTORE;
using NYdb::NBS::HasError;
using NYdb::NBS::S_ALREADY;
using NYdb::NBS::SafeExecute;
using NYdb::NBS::SEVERITY_ERROR;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct ICookie: public std::enable_shared_from_this<ICookie>
{
    virtual ~ICookie() = default;
};

using ICookiePtr = std::shared_ptr<ICookie>;

////////////////////////////////////////////////////////////////////////////////

struct TConnection;
using TConnectionPtr = std::shared_ptr<TConnection>;

////////////////////////////////////////////////////////////////////////////////

struct TEndpoint final: public ICookie
{
    const IClientStoragePtr ClientStorage;
    const bool MultiClient;
    const NProto::ERequestSource Source;

    TFsPath SocketPath;
    TLocalStreamSocket Socket;
    TSet<TConnectionPtr> Connections;

    TEndpoint(
        IClientStoragePtr clientStorage,
        bool multiClient,
        NProto::ERequestSource source)
        : ClientStorage(std::move(clientStorage))
        , MultiClient(multiClient)
        , Source(source)
    {}

    NYdb::NBS::NProto::TError
    Open(const TString& socketPath, ui32 backlog, int accessMode)
    {
        auto endPoint = TSockAddrLocal(socketPath.c_str());
        SocketPath = TFsPath(socketPath);
        SocketPath.DeleteIfExists();

        if (auto err = Socket.Bind(&endPoint, DEF_LOCAL_SOCK_MODE)) {
            NYdb::NBS::NProto::TError error;
            error.SetCode(MAKE_BLOCKSTORE_ERROR(err));
            error.SetMessage(
                TStringBuilder()
                << "failed to bind socket " << socketPath.Quote() << ": "
                << LastSystemErrorText(-err));
            return error;
        }

        if (auto err = Socket.Listen(backlog)) {
            NYdb::NBS::NProto::TError error;
            error.SetCode(MAKE_BLOCKSTORE_ERROR(err));
            error.SetMessage(
                TStringBuilder()
                << "failed to listen on socket " << socketPath.Quote() << ": "
                << LastSystemErrorText(-err));
            return error;
        }

        SetNonBlock(Socket);

        if (auto err = Chmod(socketPath.c_str(), accessMode)) {
            NYdb::NBS::NProto::TError error;
            error.SetCode(MAKE_BLOCKSTORE_ERROR(err));
            error.SetMessage(
                TStringBuilder()
                << "failed to chmod socket " << socketPath.Quote() << ": "
                << LastSystemErrorText(-err));
            return error;
        }

        return {};
    }

    void Close(bool deleteSocket)
    {
        Socket.Close();

        if (deleteSocket) {
            SocketPath.DeleteIfExists();
        }
    }
};

using TEndpointPtr = std::shared_ptr<TEndpoint>;

////////////////////////////////////////////////////////////////////////////////

struct TConnection final: public ICookie
{
    TEndpoint* Endpoint;
    TSocketHolder Socket;

    TConnection(TEndpoint* endpoint, TSocketHolder socket)
        : Endpoint(endpoint)
        , Socket(std::move(socket))
    {}

    void DetachFromEndpoint()
    {
        auto cookie = shared_from_this();
        auto connection = std::dynamic_pointer_cast<TConnection>(cookie);

        auto it = Endpoint->Connections.find(connection);
        Y_ABORT_UNLESS(it != Endpoint->Connections.end());
        Endpoint->Connections.erase(it);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TEndpointPoller::TImpl final: public ISimpleThread
{
private:
    TSocketPoller SocketPoller;

    TMutex CookieLock;
    TSet<ICookiePtr> CookieHolders;

    TMutex EndpointLock;
    TMap<TString, TEndpointPtr> Endpoints;

public:
    void Stop()
    {
        with_lock (EndpointLock) {
            for (const auto& item: Endpoints) {
                const auto& endpoint = item.second;
                StopListenEndpointImpl(endpoint, false);
            }
            Endpoints.clear();
        }

        SOCKET sockets[2];
        int ret = SocketPair(sockets);
        Y_ENSURE_EX(ret == 0, TSystemError() << "Can't create socket pair");

        auto sendFinishSocket = MakeHolder<TInetStreamSocket>(sockets[1]);
        auto receiveFinish = MakeAtomicShared<TInetStreamSocket>(sockets[0]);
        SocketPoller.WaitRead(*receiveFinish, nullptr);

        // Just send something to indicate shutdown.
        sendFinishSocket->Send("X", 1);

        Join();
    }

    NYdb::NBS::NProto::TError StartListenEndpoint(
        const TString& unixSocketPath,
        ui32 backlog,
        int accessMode,
        bool multiClient,
        NProto::ERequestSource source,
        IClientStoragePtr clientStorage)
    {
        auto endpoint = std::make_shared<TEndpoint>(
            std::move(clientStorage),
            multiClient,
            source);

        with_lock (EndpointLock) {
            if (Endpoints.find(unixSocketPath) != Endpoints.end()) {
                NYdb::NBS::NProto::TError error;
                error.SetCode(E_INVALID_STATE);
                error.SetMessage(
                    TStringBuilder() << "endpoint " << unixSocketPath.Quote()
                                     << " has already been started");
                return error;
            }

            auto error = SafeExecute<NYdb::NBS::NProto::TError>(
                [&] {
                    return endpoint->Open(unixSocketPath, backlog, accessMode);
                });
            if (HasError(error)) {
                return error;
            }

            auto [it, inserted] = Endpoints.emplace(unixSocketPath, endpoint);
            Y_ABORT_UNLESS(inserted);
        }

        SocketPoller.WaitRead(endpoint->Socket, endpoint.get());
        return {};
    }

    NYdb::NBS::NProto::TError StopListenEndpoint(const TString& unixSocketPath)
    {
        TEndpointPtr endpoint;

        with_lock (EndpointLock) {
            auto it = Endpoints.find(unixSocketPath);
            if (it == Endpoints.end()) {
                NYdb::NBS::NProto::TError error;
                error.SetCode(S_ALREADY);
                error.SetMessage(
                    TStringBuilder() << "endpoint " << unixSocketPath.Quote()
                                     << " has already been stopped");
                return error;
            }

            endpoint = std::move(it->second);
            Endpoints.erase(it);
        }

        StopListenEndpointImpl(endpoint, true);
        return {};
    }

private:
    void StopListenEndpointImpl(const TEndpointPtr& endpoint, bool deleteSocket)
    {
        with_lock (CookieLock) {
            SocketPoller.Unwait(endpoint->Socket);
            CookieHolders.insert(endpoint);

            while (!endpoint->Connections.empty()) {
                RemoveEndpointClient(endpoint->Connections.begin()->get());
            }

            endpoint->Close(deleteSocket);
        }
    }

    void* ThreadProc() override
    {
        ::NYdb::NBS::SetCurrentThreadName("Acceptor");

        static const ui32 PollEventLimit = 20;
        std::array<void*, PollEventLimit> cookies;

        bool running = true;
        while (running) {
            size_t cookieCount =
                SocketPoller.Wait(cookies.data(), cookies.size());

            with_lock (CookieLock) {
                for (size_t i = 0; i < cookieCount; ++i) {
                    auto* cookie = static_cast<ICookie*>(cookies[i]);

                    if (!cookie) {
                        running = false;
                        continue;
                    }

                    auto cookiePtr = cookie->shared_from_this();
                    if (CookieHolders.find(cookiePtr) != CookieHolders.end()) {
                        // event is unwaited, skip it
                        continue;
                    }

                    if (auto* endpoint = dynamic_cast<TEndpoint*>(cookie)) {
                        AcceptEndpointClient(endpoint);
                        continue;
                    }

                    if (auto* connection = dynamic_cast<TConnection*>(cookie)) {
                        RemoveEndpointClient(connection);
                        continue;
                    }
                }

                CookieHolders.clear();
            }
        }
        return nullptr;
    }

    void AcceptEndpointClient(TEndpoint* endpoint)
    {
        auto fd = Accept4(endpoint->Socket, nullptr, nullptr);
        TSocketHolder socket(fd);

        if (!socket.Closed()) {
            auto connection =
                std::make_shared<TConnection>(endpoint, std::move(socket));

            if (!endpoint->MultiClient) {
                while (!endpoint->Connections.empty()) {
                    RemoveEndpointClient(endpoint->Connections.begin()->get());
                }
            }
            endpoint->Connections.insert(connection);

            SocketPoller.WaitClose(connection->Socket, connection.get());

            endpoint->ClientStorage->AddClient(
                connection->Socket,
                endpoint->Source);
        }
    }

    void RemoveEndpointClient(TConnection* connection)
    {
        CookieHolders.insert(connection->shared_from_this());

        connection->Endpoint->ClientStorage->RemoveClient(connection->Socket);

        SocketPoller.Unwait(connection->Socket);

        connection->DetachFromEndpoint();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEndpointPoller::TEndpointPoller()
    : Impl(std::make_unique<TImpl>())
{}

TEndpointPoller::~TEndpointPoller()
{}

void TEndpointPoller::Start()
{
    return Impl->Start();
}

void TEndpointPoller::Stop()
{
    return Impl->Stop();
}

NYdb::NBS::NProto::TError TEndpointPoller::StartListenEndpoint(
    const TString& unixSocketPath,
    ui32 backlog,
    int accessMode,
    bool multiClient,
    NProto::ERequestSource source,
    IClientStoragePtr clientStorage)
{
    return Impl->StartListenEndpoint(
        unixSocketPath,
        backlog,
        accessMode,
        multiClient,
        source,
        std::move(clientStorage));
}

NYdb::NBS::NProto::TError TEndpointPoller::StopListenEndpoint(
    const TString& unixSocketPath)
{
    return Impl->StopListenEndpoint(unixSocketPath);
}

}   // namespace NYdb::NBS::NServer
