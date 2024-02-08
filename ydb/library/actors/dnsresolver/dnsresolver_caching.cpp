#include "dnsresolver.h"

#include <ydb/library/actors/core/hfunc.h>
#include <util/generic/intrlist.h>

#include <ares.h>

#include <queue>

namespace NActors {
namespace NDnsResolver {

    class TCachingDnsResolver : public TActor<TCachingDnsResolver> {
    public:
        struct TMonCounters {
            NMonitoring::TDynamicCounters::TCounterPtr OutgoingInFlightV4;
            NMonitoring::TDynamicCounters::TCounterPtr OutgoingInFlightV6;
            NMonitoring::TDynamicCounters::TCounterPtr OutgoingInFlightUnspec;
            NMonitoring::TDynamicCounters::TCounterPtr OutgoingErrorsV4;
            NMonitoring::TDynamicCounters::TCounterPtr OutgoingErrorsV6;
            NMonitoring::TDynamicCounters::TCounterPtr OutgoingErrorsUnspec;
            NMonitoring::TDynamicCounters::TCounterPtr OutgoingTotalV4;
            NMonitoring::TDynamicCounters::TCounterPtr OutgoingTotalV6;
            NMonitoring::TDynamicCounters::TCounterPtr OutgoingTotalUnspec;

            NMonitoring::TDynamicCounters::TCounterPtr IncomingInFlight;
            NMonitoring::TDynamicCounters::TCounterPtr IncomingErrors;
            NMonitoring::TDynamicCounters::TCounterPtr IncomingTotal;

            NMonitoring::TDynamicCounters::TCounterPtr CacheSize;
            NMonitoring::TDynamicCounters::TCounterPtr CacheHits;
            NMonitoring::TDynamicCounters::TCounterPtr CacheMisses;

            TMonCounters(const NMonitoring::TDynamicCounterPtr& counters)
                : OutgoingInFlightV4(counters->GetCounter("DnsResolver/Outgoing/InFlight/V4", false))
                , OutgoingInFlightV6(counters->GetCounter("DnsResolver/Outgoing/InFlight/V6", false))
                , OutgoingInFlightUnspec(counters->GetCounter("DnsResolver/Outgoing/InFlight/Unspec", false))
                , OutgoingErrorsV4(counters->GetCounter("DnsResolver/Outgoing/Errors/V4", true))
                , OutgoingErrorsV6(counters->GetCounter("DnsResolver/Outgoing/Errors/V6", true))
                , OutgoingErrorsUnspec(counters->GetCounter("DnsResolver/Outgoing/Errors/Unspec", true))
                , OutgoingTotalV4(counters->GetCounter("DnsResolver/Outgoing/Total/V4", true))
                , OutgoingTotalV6(counters->GetCounter("DnsResolver/Outgoing/Total/V6", true))
                , OutgoingTotalUnspec(counters->GetCounter("DnsResolver/Outgoing/Total/Unspec", true))
                , IncomingInFlight(counters->GetCounter("DnsResolver/Incoming/InFlight", false))
                , IncomingErrors(counters->GetCounter("DnsResolver/Incoming/Errors", true))
                , IncomingTotal(counters->GetCounter("DnsResolver/Incoming/Total", true))
                , CacheSize(counters->GetCounter("DnsResolver/Cache/Size", false))
                , CacheHits(counters->GetCounter("DnsResolver/Cache/Hits", true))
                , CacheMisses(counters->GetCounter("DnsResolver/Cache/Misses", true))
            { }

            const NMonitoring::TDynamicCounters::TCounterPtr& OutgoingInFlightByFamily(int family) const {
                switch (family) {
                    case AF_INET:
                        return OutgoingInFlightV4;
                    case AF_INET6:
                        return OutgoingInFlightV6;
                    case AF_UNSPEC:
                        return OutgoingInFlightUnspec;
                    default:
                        Y_ABORT("Unexpected family %d", family);
                }
            }

            const NMonitoring::TDynamicCounters::TCounterPtr& OutgoingErrorsByFamily(int family) const {
                switch (family) {
                    case AF_INET:
                        return OutgoingErrorsV4;
                    case AF_INET6:
                        return OutgoingErrorsV6;
                    case AF_UNSPEC:
                        return OutgoingErrorsUnspec;
                    default:
                        Y_ABORT("Unexpected family %d", family);
                }
            }

            const NMonitoring::TDynamicCounters::TCounterPtr& OutgoingTotalByFamily(int family) const {
                switch (family) {
                    case AF_INET:
                        return OutgoingTotalV4;
                    case AF_INET6:
                        return OutgoingTotalV6;
                    case AF_UNSPEC:
                        return OutgoingTotalUnspec;
                    default:
                        Y_ABORT("Unexpected family %d", family);
                }
            }
        };

    public:
        TCachingDnsResolver(TActorId upstream, TCachingDnsResolverOptions options)
            : TActor(&TThis::StateWork)
            , Upstream(upstream)
            , Options(std::move(options))
            , MonCounters(Options.MonCounters ? new TMonCounters(Options.MonCounters) : nullptr)
        { }

        static constexpr EActivityType ActorActivityType() {
            return EActivityType::DNS_RESOLVER;
        }

    private:
        STRICT_STFUNC(StateWork, {
            hFunc(TEvents::TEvPoison, Handle);
            hFunc(TEvDns::TEvGetHostByName, Handle);
            hFunc(TEvDns::TEvGetAddr, Handle);
            hFunc(TEvDns::TEvGetHostByNameResult, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
        });

        void Handle(TEvents::TEvPoison::TPtr&) {
            DropPending(ARES_ECANCELLED);
            PassAway();
        }

        void Handle(TEvDns::TEvGetHostByName::TPtr& ev) {
            auto req = MakeHolder<TIncomingRequest>();
            req->Type = EIncomingRequestType::GetHostByName;
            req->Sender = ev->Sender;
            req->Cookie = ev->Cookie;
            req->Name = std::move(ev->Get()->Name);
            req->Family = ev->Get()->Family;
            EnqueueRequest(std::move(req));
        }

        void Handle(TEvDns::TEvGetAddr::TPtr& ev) {
            auto req = MakeHolder<TIncomingRequest>();
            req->Type = EIncomingRequestType::GetAddr;
            req->Sender = ev->Sender;
            req->Cookie = ev->Cookie;
            req->Name = std::move(ev->Get()->Name);
            req->Family = ev->Get()->Family;
            EnqueueRequest(std::move(req));
        }

        void Handle(TEvDns::TEvGetHostByNameResult::TPtr& ev) {
            auto waitingIt = WaitingRequests.find(ev->Cookie);
            Y_ABORT_UNLESS(waitingIt != WaitingRequests.end(), "Unexpected reply, reqId=%" PRIu64, ev->Cookie);
            auto waitingInfo = waitingIt->second;
            WaitingRequests.erase(waitingIt);

            switch (waitingInfo.Family) {
                case AF_UNSPEC:
                case AF_INET6:
                case AF_INET:
                    if (ev->Get()->Status) {
                        ProcessError(waitingInfo.Family, waitingInfo.Position, ev->Get()->Status, std::move(ev->Get()->ErrorText));
                    } else {
                        ProcessAddrs(waitingInfo.Family, waitingInfo.Position, std::move(ev->Get()->AddrsV6), std::move(ev->Get()->AddrsV4));
                    }
                    break;

                default:
                    Y_ABORT("Unexpected request family %d", waitingInfo.Family);
            }
        }

        void Handle(TEvents::TEvUndelivered::TPtr& ev) {
            switch (ev->Get()->SourceType) {
                case TEvDns::TEvGetHostByName::EventType: {
                    auto waitingIt = WaitingRequests.find(ev->Cookie);
                    Y_ABORT_UNLESS(waitingIt != WaitingRequests.end(), "Unexpected TEvUndelivered, reqId=%" PRIu64, ev->Cookie);
                    auto waitingInfo = waitingIt->second;
                    WaitingRequests.erase(waitingIt);

                    switch (waitingInfo.Family) {
                        case AF_UNSPEC:
                        case AF_INET6:
                        case AF_INET:
                            ProcessError(waitingInfo.Family, waitingInfo.Position, ARES_ENOTINITIALIZED, "Caching dns resolver cannot deliver to the underlying resolver");
                            break;

                        default:
                            Y_ABORT("Unexpected request family %d", waitingInfo.Family);
                    }

                    break;
                }

                default:
                    Y_ABORT("Unexpected TEvUndelievered, type=%" PRIu32, ev->Get()->SourceType);
            }
        }

    private:
        enum EIncomingRequestType {
            GetHostByName,
            GetAddr,
        };

        struct TIncomingRequest : public TIntrusiveListItem<TIncomingRequest> {
            EIncomingRequestType Type;
            TActorId Sender;
            ui64 Cookie;
            TString Name;
            int Family;
        };

        using TIncomingRequestList = TIntrusiveListWithAutoDelete<TIncomingRequest, TDelete>;

        void EnqueueRequest(THolder<TIncomingRequest> req) {
            if (MonCounters) {
                ++*MonCounters->IncomingTotal;
            }

            CleanupExpired(TActivationContext::Now());

            switch (req->Family) {
                case AF_UNSPEC:
                    if (Options.AllowIPv6 && Options.AllowIPv4) {
                        EnqueueRequest(AF_UNSPEC, std::move(req));
                        return;
                    }
                    if (Options.AllowIPv6) {
                        EnqueueRequest(AF_INET6, std::move(req));
                        return;
                    }
                    if (Options.AllowIPv4) {
                        EnqueueRequest(AF_INET, std::move(req));
                        return;
                    }
                    break;

                case AF_INET6:
                    if (Options.AllowIPv6) {
                        EnqueueRequest(AF_INET6, std::move(req));
                        return;
                    }
                    break;

                case AF_INET:
                    if (Options.AllowIPv4) {
                        EnqueueRequest(AF_INET, std::move(req));
                        return;
                    }
                    break;
            }

            ReplyWithError(std::move(req), ARES_EBADFAMILY);
        }

        void EnqueueRequest(int family, THolder<TIncomingRequest> req) {
            auto now = TActivationContext::Now();

            auto& fullState = NameToState[req->Name];
            if (MonCounters) {
                *MonCounters->CacheSize = NameToState.size();
            }

            auto& state = fullState.StateByFamily(family);
            EnsureRequest(state, req->Name, family, now);

            if (state.IsHardExpired(now)) {
                Y_ABORT_UNLESS(state.Waiting);
                if (MonCounters) {
                    ++*MonCounters->CacheMisses;
                }
                state.WaitingRequests.PushBack(req.Release());
                return;
            }

            if (MonCounters) {
                ++*MonCounters->CacheHits;
            }

            if (state.Status != 0) {
                ReplyWithError(std::move(req), state.Status, state.ErrorText);
            } else {
                ReplyWithAddrs(std::move(req), state.AddrsIPv6, state.AddrsIPv4);
            }
        }

    private:
        struct TFamilyState {
            TVector<struct in6_addr> AddrsIPv6;
            TVector<struct in_addr> AddrsIPv4;
            TIncomingRequestList WaitingRequests;
            TInstant SoftDeadline;
            TInstant HardDeadline;
            TInstant NextSoftDeadline;
            TInstant NextHardDeadline;
            TString ErrorText;
            int Status = -1; // never requested before
            bool InSoftHeap = false;
            bool InHardHeap = false;
            bool Waiting = false;

            bool Needed() const {
                return InSoftHeap || InHardHeap || Waiting;
            }

            bool ServerReplied() const {
                return ServerReplied(Status);
            }

            bool IsSoftExpired(TInstant now) const {
                return !InSoftHeap || NextSoftDeadline < now;
            }

            bool IsHardExpired(TInstant now) const {
                return !InHardHeap || NextHardDeadline < now;
            }

            static bool ServerReplied(int status) {
                return (
                    status == ARES_SUCCESS ||
                    status == ARES_ENODATA ||
                    status == ARES_ENOTFOUND);
            }
        };

        struct TState {
            TFamilyState StateUnspec;
            TFamilyState StateIPv6;
            TFamilyState StateIPv4;

            bool Needed() const {
                return StateUnspec.Needed() || StateIPv6.Needed() || StateIPv4.Needed();
            }

            const TFamilyState& StateByFamily(int family) const {
                switch (family) {
                    case AF_UNSPEC:
                        return StateUnspec;
                    case AF_INET6:
                        return StateIPv6;
                    case AF_INET:
                        return StateIPv4;
                    default:
                        Y_ABORT("Unsupported family %d", family);
                }
            }

            TFamilyState& StateByFamily(int family) {
                switch (family) {
                    case AF_UNSPEC:
                        return StateUnspec;
                    case AF_INET6:
                        return StateIPv6;
                    case AF_INET:
                        return StateIPv4;
                    default:
                        Y_ABORT("Unsupported family %d", family);
                }
            }
        };

        using TNameToState = THashMap<TString, TState>;

        template<const TFamilyState TState::* StateToFamily,
                 const TInstant TFamilyState::* FamilyToDeadline>
        struct THeapCompare {
            // returns true when b < a
            bool operator()(TNameToState::iterator a, TNameToState::iterator b) const {
                const TState& aState = a->second;
                const TState& bState = b->second;
                const TFamilyState& aFamily = aState.*StateToFamily;
                const TFamilyState& bFamily = bState.*StateToFamily;
                const TInstant& aDeadline = aFamily.*FamilyToDeadline;
                const TInstant& bDeadline = bFamily.*FamilyToDeadline;
                return bDeadline < aDeadline;
            }
        };

        template<const TFamilyState TState::* StateToFamily,
                 const TInstant TFamilyState::* FamilyToDeadline>
        using TStateHeap = std::priority_queue<
                TNameToState::iterator,
                std::vector<TNameToState::iterator>,
                THeapCompare<StateToFamily, FamilyToDeadline>
        >;

        struct TWaitingInfo {
            TNameToState::iterator Position;
            int Family;
        };

    private:
        void EnsureRequest(TFamilyState& state, const TString& name, int family, TInstant now) {
            if (state.Waiting) {
                return; // request is already pending
            }

            if (!state.IsSoftExpired(now) && !state.IsHardExpired(now)) {
                return; // response is not expired yet
            }

            if (MonCounters) {
                ++*MonCounters->OutgoingInFlightByFamily(family);
                ++*MonCounters->OutgoingTotalByFamily(family);
            }

            ui64 reqId = ++LastRequestId;
            auto& req = WaitingRequests[reqId];
            req.Position = NameToState.find(name);
            req.Family = family;
            Y_ABORT_UNLESS(req.Position != NameToState.end());

            Send(Upstream, new TEvDns::TEvGetHostByName(name, family), IEventHandle::FlagTrackDelivery, reqId);
            state.Waiting = true;
        }

        template<TFamilyState TState::* StateToFamily,
                 TInstant TFamilyState::* FamilyToDeadline,
                 TInstant TFamilyState::* FamilyToNextDeadline,
                 bool TFamilyState::* FamilyToFlag,
                 class THeap>
        void PushToHeap(THeap& heap, TNameToState::iterator it, TInstant newDeadline) {
            auto& family = it->second.*StateToFamily;
            TInstant& deadline = family.*FamilyToDeadline;
            TInstant& nextDeadline = family.*FamilyToNextDeadline;
            bool& flag = family.*FamilyToFlag;
            nextDeadline = newDeadline;
            if (!flag) {
                deadline = newDeadline;
                heap.push(it);
                flag = true;
            }
        }

        void PushSoftUnspec(TNameToState::iterator it, TInstant newDeadline) {
            PushToHeap<&TState::StateUnspec, &TFamilyState::SoftDeadline, &TFamilyState::NextSoftDeadline, &TFamilyState::InSoftHeap>(SoftHeapUnspec, it, newDeadline);
        }

        void PushHardUnspec(TNameToState::iterator it, TInstant newDeadline) {
            PushToHeap<&TState::StateUnspec, &TFamilyState::HardDeadline, &TFamilyState::NextHardDeadline, &TFamilyState::InHardHeap>(HardHeapUnspec, it, newDeadline);
        }

        void PushSoftV6(TNameToState::iterator it, TInstant newDeadline) {
            PushToHeap<&TState::StateIPv6, &TFamilyState::SoftDeadline, &TFamilyState::NextSoftDeadline, &TFamilyState::InSoftHeap>(SoftHeapIPv6, it, newDeadline);
        }

        void PushHardV6(TNameToState::iterator it, TInstant newDeadline) {
            PushToHeap<&TState::StateIPv6, &TFamilyState::HardDeadline, &TFamilyState::NextHardDeadline, &TFamilyState::InHardHeap>(HardHeapIPv6, it, newDeadline);
        }

        void PushSoftV4(TNameToState::iterator it, TInstant newDeadline) {
            PushToHeap<&TState::StateIPv4, &TFamilyState::SoftDeadline, &TFamilyState::NextSoftDeadline, &TFamilyState::InSoftHeap>(SoftHeapIPv4, it, newDeadline);
        }

        void PushHardV4(TNameToState::iterator it, TInstant newDeadline) {
            PushToHeap<&TState::StateIPv4, &TFamilyState::HardDeadline, &TFamilyState::NextHardDeadline, &TFamilyState::InHardHeap>(HardHeapIPv4, it, newDeadline);
        }

        void PushSoft(int family, TNameToState::iterator it, TInstant newDeadline) {
            switch (family) {
                case AF_UNSPEC:
                    PushSoftUnspec(it, newDeadline);
                    break;
                case AF_INET6:
                    PushSoftV6(it, newDeadline);
                    break;
                case AF_INET:
                    PushSoftV4(it, newDeadline);
                    break;
                default:
                    Y_ABORT("Unexpected family %d", family);
            }
        }

        void PushHard(int family, TNameToState::iterator it, TInstant newDeadline) {
            switch (family) {
                case AF_UNSPEC:
                    PushHardUnspec(it, newDeadline);
                    break;
                case AF_INET6:
                    PushHardV6(it, newDeadline);
                    break;
                case AF_INET:
                    PushHardV4(it, newDeadline);
                    break;
                default:
                    Y_ABORT("Unexpected family %d", family);
            }
        }

        void ProcessError(int family, TNameToState::iterator it, int status, TString errorText) {
            auto now = TActivationContext::Now();
            if (MonCounters) {
                --*MonCounters->OutgoingInFlightByFamily(family);
                ++*MonCounters->OutgoingErrorsByFamily(family);
            }

            auto& state = it->second.StateByFamily(family);
            Y_ABORT_UNLESS(state.Waiting, "Got error for a state we are not waiting");
            state.Waiting = false;

            // When we have a cached positive reply, don't overwrite it with spurious errors
            const bool serverReplied = TFamilyState::ServerReplied(status);
            if (!serverReplied && state.ServerReplied() && !state.IsHardExpired(now)) {
                PushSoft(family, it, now + Options.SoftNegativeExpireTime);
                if (state.Status == ARES_SUCCESS) {
                    SendAddrs(family, it);
                } else {
                    SendErrors(family, it);
                }
                return;
            }

            state.Status = status;
            state.ErrorText = std::move(errorText);
            PushSoft(family, it, now + Options.SoftNegativeExpireTime);
            if (serverReplied) {
                // Server actually replied, so keep it cached for longer
                PushHard(family, it, now + Options.HardPositiveExpireTime);
            } else {
                PushHard(family, it, now + Options.HardNegativeExpireTime);
            }

            SendErrors(family, it);
        }

        void SendErrors(int family, TNameToState::iterator it) {
            auto& state = it->second.StateByFamily(family);
            while (state.WaitingRequests) {
                THolder<TIncomingRequest> req(state.WaitingRequests.PopFront());
                ReplyWithError(std::move(req), state.Status, state.ErrorText);
            }
        }

        void ProcessAddrs(int family, TNameToState::iterator it, TVector<struct in6_addr> addrs6, TVector<struct in_addr> addrs4) {
            if (Y_UNLIKELY(addrs6.empty() && addrs4.empty())) {
                // Probably unnecessary: we don't want to deal with empty address lists
                return ProcessError(family, it, ARES_ENODATA, ares_strerror(ARES_ENODATA));
            }

            auto now = TActivationContext::Now();
            if (MonCounters) {
                --*MonCounters->OutgoingInFlightByFamily(family);
            }

            auto& state = it->second.StateByFamily(family);
            Y_ABORT_UNLESS(state.Waiting, "Got reply for a state we are not waiting");
            state.Waiting = false;

            state.Status = ARES_SUCCESS;
            state.AddrsIPv6 = std::move(addrs6);
            state.AddrsIPv4 = std::move(addrs4);
            PushSoft(family, it, now + Options.SoftPositiveExpireTime);
            PushHard(family, it, now + Options.HardPositiveExpireTime);

            SendAddrs(family, it);
        }

        void SendAddrs(int family, TNameToState::iterator it) {
            auto& state = it->second.StateByFamily(family);
            while (state.WaitingRequests) {
                THolder<TIncomingRequest> req(state.WaitingRequests.PopFront());
                ReplyWithAddrs(std::move(req), state.AddrsIPv6, state.AddrsIPv4);
            }
        }

    private:
        template<TFamilyState TState::*StateToFamily,
                 TInstant TFamilyState::* FamilyToDeadline,
                 TInstant TFamilyState::* FamilyToNextDeadline,
                 bool TFamilyState::* FamilyToFlag>
        void DoCleanupExpired(TStateHeap<StateToFamily, FamilyToDeadline>& heap, TInstant now) {
            while (!heap.empty()) {
                auto it = heap.top();
                auto& family = it->second.*StateToFamily;
                TInstant& deadline = family.*FamilyToDeadline;
                if (now <= deadline) {
                    break;
                }

                bool& flag = family.*FamilyToFlag;
                Y_ABORT_UNLESS(flag);
                heap.pop();
                flag = false;

                TInstant& nextDeadline = family.*FamilyToNextDeadline;
                if (now < nextDeadline) {
                    deadline = nextDeadline;
                    heap.push(it);
                    flag = true;
                    continue;
                }

                // Remove unnecessary items
                if (!it->second.Needed()) {
                    NameToState.erase(it);
                    if (MonCounters) {
                        *MonCounters->CacheSize = NameToState.size();
                    }
                }
            }
        }

        void CleanupExpired(TInstant now) {
            DoCleanupExpired<&TState::StateUnspec, &TFamilyState::SoftDeadline, &TFamilyState::NextSoftDeadline, &TFamilyState::InSoftHeap>(SoftHeapUnspec, now);
            DoCleanupExpired<&TState::StateUnspec, &TFamilyState::HardDeadline, &TFamilyState::NextHardDeadline, &TFamilyState::InHardHeap>(HardHeapUnspec, now);
            DoCleanupExpired<&TState::StateIPv6, &TFamilyState::SoftDeadline, &TFamilyState::NextSoftDeadline, &TFamilyState::InSoftHeap>(SoftHeapIPv6, now);
            DoCleanupExpired<&TState::StateIPv6, &TFamilyState::HardDeadline, &TFamilyState::NextHardDeadline, &TFamilyState::InHardHeap>(HardHeapIPv6, now);
            DoCleanupExpired<&TState::StateIPv4, &TFamilyState::SoftDeadline, &TFamilyState::NextSoftDeadline, &TFamilyState::InSoftHeap>(SoftHeapIPv4, now);
            DoCleanupExpired<&TState::StateIPv4, &TFamilyState::HardDeadline, &TFamilyState::NextHardDeadline, &TFamilyState::InHardHeap>(HardHeapIPv4, now);
        }

        template<class TEvent>
        void SendError(TActorId replyTo, ui64 cookie, int status, const TString& errorText) {
            auto reply = MakeHolder<TEvent>();
            reply->Status = status;
            reply->ErrorText = errorText;
            this->Send(replyTo, reply.Release(), 0, cookie);
        }

        void ReplyWithError(THolder<TIncomingRequest> req, int status, const TString& errorText) {
            if (MonCounters) {
                ++*MonCounters->IncomingErrors;
            }
            switch (req->Type) {
                case EIncomingRequestType::GetHostByName: {
                    SendError<TEvDns::TEvGetHostByNameResult>(req->Sender, req->Cookie, status, errorText);
                    break;
                }
                case EIncomingRequestType::GetAddr: {
                    SendError<TEvDns::TEvGetAddrResult>(req->Sender, req->Cookie, status, errorText);
                    break;
                }
            }
        }

        void ReplyWithAddrs(THolder<TIncomingRequest> req, const TVector<struct in6_addr>& addrs6, const TVector<struct in_addr>& addrs4) {
            switch (req->Type) {
                case EIncomingRequestType::GetHostByName: {
                    auto reply = MakeHolder<TEvDns::TEvGetHostByNameResult>();
                    reply->AddrsV6 = addrs6;
                    reply->AddrsV4 = addrs4;
                    Send(req->Sender, reply.Release(), 0, req->Cookie);
                    break;
                }
                case EIncomingRequestType::GetAddr: {
                    auto reply = MakeHolder<TEvDns::TEvGetAddrResult>();
                    if (!addrs6.empty()) {
                        reply->Addr = addrs6.front();
                    } else if (!addrs4.empty()) {
                        reply->Addr = addrs4.front();
                    } else {
                        Y_ABORT("Unexpected reply with empty address list");
                    }
                    Send(req->Sender, reply.Release(), 0, req->Cookie);
                    break;
                }
            }
        }

        void ReplyWithError(THolder<TIncomingRequest> req, int status) {
            ReplyWithError(std::move(req), status, ares_strerror(status));
        }

        void DropPending(TIncomingRequestList& list, int status, const TString& errorText) {
            while (list) {
                THolder<TIncomingRequest> req(list.PopFront());
                ReplyWithError(std::move(req), status, errorText);
            }
        }

        void DropPending(int status, const TString& errorText) {
            for (auto& [name, state] : NameToState) {
                DropPending(state.StateUnspec.WaitingRequests, status, errorText);
                DropPending(state.StateIPv6.WaitingRequests, status, errorText);
                DropPending(state.StateIPv4.WaitingRequests, status, errorText);
            }
        }

        void DropPending(int status) {
            DropPending(status, ares_strerror(status));
        }

    private:
        const TActorId Upstream;
        const TCachingDnsResolverOptions Options;
        const THolder<TMonCounters> MonCounters;

        TNameToState NameToState;
        TStateHeap<&TState::StateUnspec, &TFamilyState::SoftDeadline> SoftHeapUnspec;
        TStateHeap<&TState::StateUnspec, &TFamilyState::HardDeadline> HardHeapUnspec;
        TStateHeap<&TState::StateIPv6, &TFamilyState::SoftDeadline> SoftHeapIPv6;
        TStateHeap<&TState::StateIPv6, &TFamilyState::HardDeadline> HardHeapIPv6;
        TStateHeap<&TState::StateIPv4, &TFamilyState::SoftDeadline> SoftHeapIPv4;
        TStateHeap<&TState::StateIPv4, &TFamilyState::HardDeadline> HardHeapIPv4;

        THashMap<ui64, TWaitingInfo> WaitingRequests;
        ui64 LastRequestId = 0;
    };

    IActor* CreateCachingDnsResolver(TActorId upstream, TCachingDnsResolverOptions options) {
        return new TCachingDnsResolver(upstream, std::move(options));
    }

} // namespace NDnsResolver
} // namespace NActors
