#include "simple_server.h"

#include <util/network/pair.h>
#include <util/network/poller.h>
#include <util/network/sock.h>
#include <util/string/builder.h>
#include <util/system/thread.h>
#include <util/thread/pool.h>

TSimpleServer::TSimpleServer(int port, TRequestHandler requestHandler)
    : Port_(port)
{
    auto listenSocket = MakeAtomicShared<TInetStreamSocket>();
    TSockAddrInet addr((TIpHost)INADDR_ANY, Port_);
    SetSockOpt(*listenSocket, SOL_SOCKET, SO_REUSEADDR, 1);
    int ret = listenSocket->Bind(&addr);
    Y_ENSURE_EX(ret == 0, TSystemError() << "Can not bind");

    SOCKET socketPair[2];
    ret = SocketPair(socketPair);
    Y_ENSURE_EX(ret == 0, TSystemError() << "Can not create socket pair");

    ret = listenSocket->Listen(10);
    Y_ENSURE_EX(ret == 0, TSystemError() << "Can not listen socket");

    SendFinishSocket_ = MakeHolder<TInetStreamSocket>(socketPair[1]);

    ThreadPool_ = MakeHolder<TAdaptiveThreadPool>();
    ThreadPool_->Start(1);

    auto receiveFinish = MakeAtomicShared<TInetStreamSocket>(socketPair[0]);
    ListenerThread_ = ThreadPool_->Run([listenSocket, receiveFinish, requestHandler] {
        TSocketPoller socketPoller;
        socketPoller.WaitRead(*receiveFinish, nullptr);
        socketPoller.WaitRead(*listenSocket, (void*)1);

        bool running = true;
        while (running) {
            void* cookies[2];
            size_t cookieCount = socketPoller.WaitI(cookies, 2);
            for (size_t i = 0; i != cookieCount; ++i) {
                if (!cookies[i]) {
                    running = false;
                } else {
                    TSockAddrInet addr;
                    TAtomicSharedPtr<TStreamSocket> socket = MakeAtomicShared<TInetStreamSocket>();
                    int ret = listenSocket->Accept(socket.Get(), &addr);
                    Y_ENSURE_EX(ret == 0, TSystemError() << "Can not accept connection");

                    SystemThreadFactory()->Run(
                        [socket, requestHandler] {
                            TStreamSocketInput input(socket.Get());
                            TStreamSocketOutput output(socket.Get());
                            requestHandler(&input, &output);
                            socket->Close();
                        });
                }
            }
        }
    });
}

TSimpleServer::~TSimpleServer()
{
    try {
        if (ThreadPool_) {
            Stop();
        }
    } catch (...) {
    }
}

void TSimpleServer::Stop()
{
    // Just send something to indicate shutdown.
    SendFinishSocket_->Send("X", 1);
    ListenerThread_->Join();
    ThreadPool_->Stop();
    ThreadPool_.Destroy();
}

int TSimpleServer::GetPort() const
{
    return Port_;
}

TString TSimpleServer::GetAddress() const
{
    return TStringBuilder() << "localhost:" << Port_;
}
