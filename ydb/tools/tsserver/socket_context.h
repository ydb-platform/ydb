#pragma once

#include "defs.h"
#include "types.h"

using TProcessor = NKikimr::NTestShard::TProcessor;

class TSocketContext {
    const ui64 ClientId;
    const int EpollFd;
    TIntrusivePtr<NInterconnect::TStreamSocket> Socket;
    TProcessor& Processor;

    enum class EReadStep {
        LEN,
        DATA,
    } ReadStep = EReadStep::LEN;
    TString ReadBuffer = TString::Uninitialized(sizeof(ui32));
    char *ReadBegin = ReadBuffer.Detach(), *ReadEnd = ReadBegin + ReadBuffer.size();

    std::deque<TString> OutQ;
    size_t WriteOffset = 0;

    std::optional<ui32> StoredEvents;
    ui32 Events = 0;

public:
    TSocketContext(ui64 clientId, int epollFd, TIntrusivePtr<NInterconnect::TStreamSocket> socket, TProcessor& processor)
        : ClientId(clientId)
        , EpollFd(epollFd)
        , Socket(std::move(socket))
        , Processor(processor)
    {
        printf("[%" PRIu64 "] created\n", ClientId);
        Read();
        Write();
        UpdateEpoll();
    }

    ~TSocketContext() {
        if (StoredEvents) {
            Epoll(EPOLL_CTL_DEL, 0);
        }
    }

    void Epoll(int op, ui32 events) {
        epoll_event e;
        e.events = events;
        e.data.u64 = ClientId;
        if (epoll_ctl(EpollFd, op, *Socket, &e) == -1) {
            Y_ABORT("epoll_ctl failed: %s", strerror(errno));
        }
    }

    void UpdateEpoll() {
        std::optional<int> mode = !StoredEvents ? std::make_optional(EPOLL_CTL_ADD) :
            *StoredEvents != Events ? std::make_optional(EPOLL_CTL_MOD) : std::nullopt;
        if (mode) {
            Epoll(*mode, Events);
        }
        StoredEvents = Events;
    }

    void Read() {
        for (;;) {
            ssize_t n = Socket->Recv(ReadBegin, ReadEnd - ReadBegin);
            if (n > 0) {
                ReadBegin += n;
                if (ReadBegin == ReadEnd) {
                    switch (ReadStep) {
                        case EReadStep::LEN: {
                            ui32 n;
                            Y_ABORT_UNLESS(ReadBuffer.size() == sizeof(n));
                            memcpy(&n, ReadBuffer.data(), ReadBuffer.size()); // strict aliasing
                            Y_ABORT_UNLESS(n <= 64 * 1024 * 1024);
                            ReadBuffer = TString::Uninitialized(n);
                            ReadStep = EReadStep::DATA;
                            break;
                        }

                        case EReadStep::DATA:
                            ProcessReadBuffer();
                            ReadBuffer = TString::Uninitialized(sizeof(ui32));
                            ReadStep = EReadStep::LEN;
                            break;
                    }
                    ReadBegin = ReadBuffer.Detach();
                    ReadEnd = ReadBegin + ReadBuffer.size();
                }
            } else if (-n == EAGAIN || -n == EWOULDBLOCK) {
                Events |= EPOLLIN;
                break;
            } else if (-n == EINTR) {
                continue;
            } else {
                printf("[%" PRIu64 "] failed to receive data: %s\n", ClientId, strerror(-n));
                throw TExError();
            }
        }
    }

    void ProcessReadBuffer() {
        ::NTestShard::TStateServer::TRequest request;
        if (!request.ParseFromString(ReadBuffer)) {
            throw TExError();
        }
        auto send = [this](const auto& proto) {
            TString buffer;
            const bool success = proto.SerializeToString(&buffer);
            Y_ABORT_UNLESS(success);
            Y_ABORT_UNLESS(buffer.size() <= 64 * 1024 * 1024);
            ui32 len = buffer.size();
            TString w = TString::Uninitialized(sizeof(ui32) + len);
            char *p = w.Detach();
            memcpy(p, &len, sizeof(len));
            memcpy(p + sizeof(len), buffer.data(), buffer.size());
            OutQ.push_back(std::move(w));
            Write();
        };
        switch (request.GetCommandCase()) {
            case ::NTestShard::TStateServer::TRequest::kWrite:
                send(Processor.Execute(request.GetWrite()));
                break;

            case ::NTestShard::TStateServer::TRequest::kRead:
                send(Processor.Execute(request.GetRead()));
                break;

            case ::NTestShard::TStateServer::TRequest::kTabletInfo:
            case ::NTestShard::TStateServer::TRequest::COMMAND_NOT_SET:
                printf("[%" PRIu64 "] incorrect request received\n", ClientId);
                throw TExError();
        }
    }

    void Write() {
        while (!OutQ.empty()) {
            auto& buffer = OutQ.front();
            if (WriteOffset == buffer.size()) {
                OutQ.pop_front();
                WriteOffset = 0;
                continue;
            }
            ssize_t n = Socket->Send(buffer.data() + WriteOffset, buffer.size() - WriteOffset);
            if (n > 0) {
                WriteOffset += n;
            } else if (-n == EWOULDBLOCK || -n == EAGAIN) {
                break;
            } else if (-n == EINTR) {
                continue;
            } else {
                printf("[%" PRIu64 "] failed to send data: %s\n", ClientId, strerror(-n));
            }
        }
        if (OutQ.empty()) {
            Events &= ~EPOLLOUT;
        } else {
            Events |= EPOLLOUT;
        }
    }
};
