#include <util/system/platform.h>

#if defined(_linux_)

#include <stdio.h>
#include <sys/epoll.h>
#include <ydb/core/protos/test_shard.pb.h>
#include "socket_context.h"

int main(int argc, char *argv[]) {
    if (argc != 2) {
        printf("Usage: %s <port>\n", argv[0]);
        return 1;
    }

    auto server = NInterconnect::TStreamSocket::Make(AF_INET6);
    Y_ABORT_UNLESS(*server != -1, "failed to create server socket");
    SetNonBlock(*server);
    SetSockOpt(*server, SOL_SOCKET, SO_REUSEADDR, 1);
    SetSockOpt(*server, SOL_SOCKET, SO_REUSEPORT, 1);

    int res = server->Bind(NInterconnect::TAddress("::", atoi(argv[1])));
    Y_ABORT_UNLESS(!res, "failed to bind server socket: %s", strerror(-res));
    res = server->Listen(10);
    Y_ABORT_UNLESS(!res, "failed to listen on server socket: %s", strerror(-res));

    int epfd = epoll_create(1);
    if (epfd == -1) {
        Y_ABORT("failed to create epoll: %s", strerror(errno));
    }

    epoll_event e;
    e.events = EPOLLIN;
    e.data.u64 = 0;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, *server, &e) == -1) {
        Y_ABORT("failed to add listening socket to epoll: %s", strerror(errno));
    }

    std::unordered_map<ui64, TSocketContext> Clients;
    ui64 LastClientId = 0;

    TProcessor processor;

    for (;;) {
        static constexpr size_t N = 64;
        epoll_event events[N];
        int res = epoll_wait(epfd, events, N, -1);
        if (res == -1) {
            if (errno == EINTR) {
                continue;
            } else {
                Y_ABORT("epoll_wait failed: %s", strerror(errno));
            }
        }
        for (int i = 0; i < res; ++i) {
            epoll_event& e = events[i];
            if (!e.data.u64) {
                NInterconnect::TAddress addr;
                int fd = server->Accept(addr);
                Y_ABORT_UNLESS(fd >= 0, "failed to accept client socket: %s", strerror(-fd));
                auto client = MakeIntrusive<NInterconnect::TStreamSocket>(fd);
                SetNonBlock(*client);
                SetNoDelay(*client, true);
                const ui64 clientId = ++LastClientId;
                auto&& [it, inserted] = Clients.try_emplace(clientId, clientId, epfd, client, processor);
                Y_ABORT_UNLESS(inserted);
            } else if (auto it = Clients.find(e.data.u64); it != Clients.end()) {
                try {
                    if (e.events & EPOLLIN) {
                        it->second.Read();
                    }
                    if (e.events & EPOLLOUT) {
                        it->second.Write();
                    }
                    it->second.UpdateEpoll();
                } catch (const TExError&) {
                    Clients.erase(it);
                }
            }
        }
    }

    close(epfd);

    return 0;
}

#else

int main(int /*argc*/, char* /*argv*/[]) {
    return 1;
}

#endif
