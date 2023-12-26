#pragma once

#include <library/cpp/testing/unittest/tests_data.h>

#include <util/network/sock.h>
#include <util/network/poller.h>
#include <util/system/thread.h>
#include <util/system/hp_timer.h>
#include <util/generic/list.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/generic/deque.h>
#include <util/random/random.h>

#include <iterator>

class TTrafficInterrupter
   : public ISimpleThread {
    const TString Address;
    const ui16 ForwardPort;
    TInet6StreamSocket ListenSocket;

    struct TConnectionDescriptor;
    struct TDelayedPacket {
        TInet6StreamSocket* ForwardSocket = nullptr;
        TVector<char> Data;
    };
    struct TCompare {
        bool operator()(const std::pair<TInstant, TDelayedPacket>& x, const std::pair<TInstant, TDelayedPacket>& y) const {
            return x.first > y.first;
        };
    };

    struct TDirectedConnection {
        TInet6StreamSocket* Source = nullptr;
        TInet6StreamSocket* Destination = nullptr;
        TList<TConnectionDescriptor>::iterator ListIterator;
        TInstant Timestamp;
        TPriorityQueue<std::pair<TInstant, TDelayedPacket>, TVector<std::pair<TInstant, TDelayedPacket>>, TCompare> DelayedQueue;

        TDirectedConnection(TInet6StreamSocket* source, TInet6StreamSocket* destination)
            : Source(source)
            , Destination(destination)
        {
        }
    };

    struct TConnectionDescriptor {
        std::unique_ptr<TInet6StreamSocket> FirstSocket;
        std::unique_ptr<TInet6StreamSocket> SecondSocket;
        TDirectedConnection ForwardConnection;
        TDirectedConnection BackwardConnection;

        TConnectionDescriptor(std::unique_ptr<TInet6StreamSocket> firstSock,
                              std::unique_ptr<TInet6StreamSocket> secondSock)
            : FirstSocket(std::move(firstSock))
            , SecondSocket(std::move(secondSock))
            , ForwardConnection(FirstSocket.get(), SecondSocket.get())
            , BackwardConnection(SecondSocket.get(), FirstSocket.get())
        {
        }
    };

    template <class It = TList<TConnectionDescriptor>::iterator>
    class TCustomListIteratorCompare {
    public:
        bool operator()(const It& it1, const It& it2) const {
            return (&(*it1) < &(*it2));
        }
    };

    TList<TConnectionDescriptor> Connections;
    TSet<TList<TConnectionDescriptor>::iterator, TCustomListIteratorCompare<>> DroppedConnections;

public:
    TTrafficInterrupter(TString address, ui16 listenPort, ui16 forwardPort, TDuration rejectingTrafficTimeout, double bandwidth, bool disconnect = true)
        : Address(std::move(address))
        , ForwardPort(forwardPort)
        , ListenSocket()
        , RejectingTrafficTimeout(rejectingTrafficTimeout)
        , CurrentRejectingTimeout(rejectingTrafficTimeout)
        , RejectingStateTimer()
        , Bandwidth(bandwidth)
        , Disconnect(disconnect)
        , RejectingTraffic(false)
    {
        SetReuseAddressAndPort(ListenSocket);
        TSockAddrInet6 addr(Address.data(), listenPort);
        Y_ABORT_UNLESS(ListenSocket.Bind(&addr) == 0);
        Y_ABORT_UNLESS(ListenSocket.Listen(5) == 0);

        DelayTraffic = (Bandwidth == 0.0) ? false : true;

        ForwardAddrress.Reset(new TSockAddrInet6(Address.data(), ForwardPort));
        const ui32 BufSize = DelayTraffic ? 4096 : 65536 + 4096;
        Buf.resize(BufSize);
    }

    ~TTrafficInterrupter() {
        Running.store(false, std::memory_order_release);
        this->Join();
    }

private:
    std::atomic<bool> Running = true;
    TVector<char> Buf;
    TSocketPoller SocketPoller;
    THolder<TSockAddrInet6> ForwardAddrress;
    TVector<void*> Events;
    TDuration RejectingTrafficTimeout;
    TDuration CurrentRejectingTimeout;
    TDuration DefaultPollTimeout = TDuration::MilliSeconds(100);
    TDuration DisconnectTimeout = TDuration::MilliSeconds(100);
    THPTimer RejectingStateTimer;
    THPTimer DisconnectTimer;
    double Bandwidth;
    const bool Disconnect;
    bool RejectingTraffic;
    bool DelayTraffic;

    void UpdateRejectingState() {
        if (TDuration::Seconds(std::abs(RejectingStateTimer.Passed())) > CurrentRejectingTimeout) {
            RejectingStateTimer.Reset();
            CurrentRejectingTimeout = (RandomNumber<ui32>(1) ? RejectingTrafficTimeout + TDuration::Seconds(1.0) : RejectingTrafficTimeout - TDuration::Seconds(0.2));
            RejectingTraffic = !RejectingTraffic;
        }
    }

    void RandomlyDisconnect() {
        if (TDuration::Seconds(std::abs(DisconnectTimer.Passed())) > DisconnectTimeout) {
            DisconnectTimer.Reset();
            if (RandomNumber<ui32>(100) > 90) {
                if (!Connections.empty()) {
                    auto it = Connections.begin();
                    std::advance(it, RandomNumber<ui32>(Connections.size()));
                    SocketPoller.Unwait(static_cast<SOCKET>(*it->FirstSocket.get()));
                    SocketPoller.Unwait(static_cast<SOCKET>(*it->SecondSocket.get()));
                    Connections.erase(it);
                }
            }
        }
    }

    void* ThreadProc() override {
        int pollReadyCount = 0;
        SocketPoller.WaitRead(static_cast<SOCKET>(ListenSocket), &ListenSocket);
        Events.resize(10);

        while (Running.load(std::memory_order_acquire)) {
            if (RejectingTrafficTimeout != TDuration::Zero()) {
                UpdateRejectingState();
            }
            if (Disconnect) {
                RandomlyDisconnect();
            }
            if (!RejectingTraffic) {
                TDuration timeout = DefaultPollTimeout;
                auto updateTimout = [&timeout](TDirectedConnection& conn) {
                    if (conn.DelayedQueue) {
                        timeout = Min(timeout, conn.DelayedQueue.top().first - TInstant::Now());
                    }
                };
                for (auto& it : Connections) {
                    updateTimout(it.ForwardConnection);
                    updateTimout(it.BackwardConnection);
                }
                pollReadyCount = SocketPoller.WaitT(Events.data(), Events.size(), timeout);
                if (pollReadyCount > 0) {
                    for (int i = 0; i < pollReadyCount; i++) {
                        HandleSocketPollEvent(Events[i]);
                    }
                    for (auto it : DroppedConnections) {
                        Connections.erase(it);
                    }
                    DroppedConnections.clear();
                }
            }
            if (DelayTraffic) { // process packets from DelayQueues
                auto processDelayedPackages = [](TDirectedConnection& conn) {
                    while (!conn.DelayedQueue.empty()) {
                        auto& frontPackage = conn.DelayedQueue.top();
                        if (TInstant::Now() >= frontPackage.first) {
                            TInet6StreamSocket* sock = frontPackage.second.ForwardSocket;
                            if (sock) {
                                sock->Send(frontPackage.second.Data.data(), frontPackage.second.Data.size());
                            }
                            conn.DelayedQueue.pop();
                        } else {
                            break;
                        }
                    }
                };
                for (auto& it : Connections) {
                    processDelayedPackages(it.ForwardConnection);
                    processDelayedPackages(it.BackwardConnection);
                }
            }
        }
        ListenSocket.Close();
        return nullptr;
    }

    void HandleSocketPollEvent(void* ev) {
        if (ev == static_cast<void*>(&ListenSocket)) {
            TSockAddrInet6 origin;
            Connections.emplace_back(TConnectionDescriptor(std::unique_ptr<TInet6StreamSocket>(new TInet6StreamSocket), std::unique_ptr<TInet6StreamSocket>(new TInet6StreamSocket)));
            int err = ListenSocket.Accept(Connections.back().FirstSocket.get(), &origin);
            if (!err) {
                err = Connections.back().SecondSocket->Connect(ForwardAddrress.Get());
                if (!err) {
                    Connections.back().ForwardConnection.ListIterator = --Connections.end();
                    Connections.back().BackwardConnection.ListIterator = --Connections.end();
                    SocketPoller.WaitRead(static_cast<SOCKET>(*Connections.back().FirstSocket), &Connections.back().ForwardConnection);
                    SocketPoller.WaitRead(static_cast<SOCKET>(*Connections.back().SecondSocket), &Connections.back().BackwardConnection);
                } else {
                    Connections.back().FirstSocket->Close();
                }
            } else {
                Connections.pop_back();
            }
        } else {
            TDirectedConnection* directedConnection = static_cast<TDirectedConnection*>(ev);
            int recvSize = 0;
            do {
                recvSize = directedConnection->Source->Recv(Buf.data(), Buf.size());
            } while (recvSize == -EINTR);

            if (recvSize > 0) {
                if (DelayTraffic) {
                    // put packet into DelayQueue
                    const TDuration baseDelay = TDuration::MicroSeconds(recvSize * 1e6 / Bandwidth);
                    const TInstant now = TInstant::Now();
                    directedConnection->Timestamp = Max(now, directedConnection->Timestamp) + baseDelay;
                    TDelayedPacket pkt;
                    pkt.ForwardSocket = directedConnection->Destination;
                    pkt.Data.resize(recvSize);
                    memcpy(pkt.Data.data(), Buf.data(), recvSize);
                    directedConnection->DelayedQueue.emplace(directedConnection->Timestamp, std::move(pkt));
                } else {
                    directedConnection->Destination->Send(Buf.data(), recvSize);
                }
            } else {
                SocketPoller.Unwait(static_cast<SOCKET>(*directedConnection->Source));
                SocketPoller.Unwait(static_cast<SOCKET>(*directedConnection->Destination));
                DroppedConnections.emplace(directedConnection->ListIterator);
            }
        }
    }
};
