#include "ldap_simple_server.h"
#include <util/network/pair.h>
#include <util/network/poller.h>
#include <util/network/sock.h>
#include <util/string/builder.h>
#include <util/system/thread.h>
#include <util/thread/pool.h>

#include "ldap_mock.h"
#include "ldap_socket_wrapper.h"

namespace LdapMock {

TLdapSimpleServer::TLdapSimpleServer(ui16 port, const TLdapMockResponses& responses)
    : Port(port)
    , Responses(responses)
{
    auto listenSocket = MakeAtomicShared<TInetStreamSocket>();
    TSockAddrInet addr((TIpHost)INADDR_ANY, Port);
    SetSockOpt(*listenSocket, SOL_SOCKET, SO_REUSEADDR, 1);
    int ret = listenSocket->Bind(&addr);
    Y_ENSURE_EX(ret == 0, TSystemError() << "Can not bind");

    SOCKET socketPair[2];
    ret = SocketPair(socketPair);
    Y_ENSURE_EX(ret == 0, TSystemError() << "Can not create socket pair");

    ret = listenSocket->Listen(10);
    Y_ENSURE_EX(ret == 0, TSystemError() << "Can not listen socket");

    SendFinishSocket = MakeHolder<TInetStreamSocket>(socketPair[1]);

    ThreadPool = MakeHolder<TAdaptiveThreadPool>();
    ThreadPool->Start(1);

    auto receiveFinish = MakeAtomicShared<TInetStreamSocket>(socketPair[0]);
    ListenerThread = ThreadPool->Run([listenSocket, receiveFinish, &responses = this->Responses] {
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
                    TAtomicSharedPtr<TLdapSocketWrapper> socket = MakeAtomicShared<TLdapSocketWrapper>(listenSocket);
                    socket->OnAccept();

                    SystemThreadFactory()->Run(
                        [socket, &responses] {
                            LdapRequestHandler(socket, responses);
                            socket->Close();
                        });
                }
            }
        }
    });
}

TLdapSimpleServer::~TLdapSimpleServer()
{
    try {
        if (ThreadPool) {
            Stop();
        }
    } catch (...) {
    }
}

void TLdapSimpleServer::Stop()
{
    // Just send something to indicate shutdown.
    SendFinishSocket->Send("X", 1);
    ListenerThread->Join();
    ThreadPool->Stop();
    ThreadPool.Destroy();
}

int TLdapSimpleServer::GetPort() const
{
    return Port;
}

TString TLdapSimpleServer::GetAddress() const
{
    return TStringBuilder() << "localhost:" << Port;
}

void TLdapSimpleServer::SetBindResponse(const std::pair<TBindRequestInfo, TBindResponseInfo>& response) {
    auto it = std::find_if(Responses.BindResponses.begin(), Responses.BindResponses.end(), [&response](const std::pair<TBindRequestInfo, TBindResponseInfo>& expectedResponse){
        return response.first == expectedResponse.first;
    });

    if (it != Responses.BindResponses.end()) {
        it->second = response.second;
    } else {
        Responses.BindResponses.push_back(response);
    }
}

void TLdapSimpleServer::SetSearchReasponse(const std::pair<TSearchRequestInfo, TSearchResponseInfo>& response) {
    auto it = std::find_if(Responses.SearchResponses.begin(), Responses.SearchResponses.end(), [&response](const std::pair<TSearchRequestInfo, TSearchResponseInfo>& expectedResponse){
        return response.first == expectedResponse.first;
    });

    if (it != Responses.SearchResponses.end()) {
        it->second = response.second;
    } else {
        Responses.SearchResponses.push_back(response);
    }
}

}
