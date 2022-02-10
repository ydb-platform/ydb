#include "dnsresolver.h" 
 
#include <library/cpp/actors/core/hfunc.h> 
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
            NMonitoring::TDynamicCounters::TCounterPtr OutgoingErrorsV4; 
            NMonitoring::TDynamicCounters::TCounterPtr OutgoingErrorsV6; 
            NMonitoring::TDynamicCounters::TCounterPtr OutgoingTotalV4; 
            NMonitoring::TDynamicCounters::TCounterPtr OutgoingTotalV6; 
 
            NMonitoring::TDynamicCounters::TCounterPtr IncomingInFlight; 
            NMonitoring::TDynamicCounters::TCounterPtr IncomingErrors; 
            NMonitoring::TDynamicCounters::TCounterPtr IncomingTotal; 
 
            NMonitoring::TDynamicCounters::TCounterPtr CacheSize; 
            NMonitoring::TDynamicCounters::TCounterPtr CacheHits; 
            NMonitoring::TDynamicCounters::TCounterPtr CacheMisses; 
 
            TMonCounters(const NMonitoring::TDynamicCounterPtr& counters) 
                : OutgoingInFlightV4(counters->GetCounter("DnsResolver/Outgoing/InFlight/V4", false)) 
                , OutgoingInFlightV6(counters->GetCounter("DnsResolver/Outgoing/InFlight/V6", false)) 
                , OutgoingErrorsV4(counters->GetCounter("DnsResolver/Outgoing/Errors/V4", true)) 
                , OutgoingErrorsV6(counters->GetCounter("DnsResolver/Outgoing/Errors/V6", true)) 
                , OutgoingTotalV4(counters->GetCounter("DnsResolver/Outgoing/Total/V4", true)) 
                , OutgoingTotalV6(counters->GetCounter("DnsResolver/Outgoing/Total/V6", true)) 
                , IncomingInFlight(counters->GetCounter("DnsResolver/Incoming/InFlight", false)) 
                , IncomingErrors(counters->GetCounter("DnsResolver/Incoming/Errors", true)) 
                , IncomingTotal(counters->GetCounter("DnsResolver/Incoming/Total", true)) 
                , CacheSize(counters->GetCounter("DnsResolver/Cache/Size", false)) 
                , CacheHits(counters->GetCounter("DnsResolver/Cache/Hits", true)) 
                , CacheMisses(counters->GetCounter("DnsResolver/Cache/Misses", true)) 
            { } 
        }; 
 
    public: 
        TCachingDnsResolver(TActorId upstream, TCachingDnsResolverOptions options) 
            : TActor(&TThis::StateWork) 
            , Upstream(upstream) 
            , Options(std::move(options)) 
            , MonCounters(Options.MonCounters ? new TMonCounters(Options.MonCounters) : nullptr) 
        { } 
 
        static constexpr EActivityType ActorActivityType() { 
            return DNS_RESOLVER; 
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
            Y_VERIFY(waitingIt != WaitingRequests.end(), "Unexpected reply, reqId=%" PRIu64, ev->Cookie); 
            auto waitingInfo = waitingIt->second; 
            WaitingRequests.erase(waitingIt); 
 
            switch (waitingInfo.Family) { 
                case AF_INET6: 
                    if (ev->Get()->Status) { 
                        ProcessErrorV6(waitingInfo.Position, ev->Get()->Status, std::move(ev->Get()->ErrorText)); 
                    } else { 
                        ProcessAddrsV6(waitingInfo.Position, std::move(ev->Get()->AddrsV6)); 
                    } 
                    break; 
 
                case AF_INET: 
                    if (ev->Get()->Status) { 
                        ProcessErrorV4(waitingInfo.Position, ev->Get()->Status, std::move(ev->Get()->ErrorText)); 
                    } else { 
                        ProcessAddrsV4(waitingInfo.Position, std::move(ev->Get()->AddrsV4)); 
                    } 
                    break; 
 
                default: 
                    Y_FAIL("Unexpected request family %d", waitingInfo.Family); 
            } 
        } 
 
        void Handle(TEvents::TEvUndelivered::TPtr& ev) { 
            switch (ev->Get()->SourceType) { 
                case TEvDns::TEvGetHostByName::EventType: { 
                    auto waitingIt = WaitingRequests.find(ev->Cookie); 
                    Y_VERIFY(waitingIt != WaitingRequests.end(), "Unexpected TEvUndelivered, reqId=%" PRIu64, ev->Cookie); 
                    auto waitingInfo = waitingIt->second; 
                    WaitingRequests.erase(waitingIt); 
 
                    switch (waitingInfo.Family) { 
                        case AF_INET6: 
                            ProcessErrorV6(waitingInfo.Position, ARES_ENOTINITIALIZED, "Caching dns resolver cannot deliver to the underlying resolver"); 
                            break; 
                        case AF_INET: 
                            ProcessErrorV4(waitingInfo.Position, ARES_ENOTINITIALIZED, "Caching dns resolver cannot deliver to the underlying resolver"); 
                            break; 
                        default: 
                            Y_FAIL("Unexpected request family %d", waitingInfo.Family); 
                    } 
 
                    break; 
                } 
 
                default: 
                    Y_FAIL("Unexpected TEvUndelievered, type=%" PRIu32, ev->Get()->SourceType); 
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
                    if (Options.AllowIPv6) { 
                        EnqueueRequestIPv6(std::move(req)); 
                        return; 
                    } 
                    if (Options.AllowIPv4) { 
                        EnqueueRequestIPv4(std::move(req)); 
                        return; 
                    } 
                    break; 
 
                case AF_INET6: 
                    if (Options.AllowIPv6) { 
                        EnqueueRequestIPv6(std::move(req)); 
                        return; 
                    } 
                    break; 
 
                case AF_INET: 
                    if (Options.AllowIPv4) { 
                        EnqueueRequestIPv4(std::move(req)); 
                        return; 
                    } 
                    break; 
            } 
 
            ReplyWithError(std::move(req), ARES_EBADFAMILY); 
        } 
 
        void EnqueueRequestIPv6(THolder<TIncomingRequest> req) { 
            auto now = TActivationContext::Now(); 
 
            auto& fullState = NameToState[req->Name]; 
            if (MonCounters) { 
                *MonCounters->CacheSize = NameToState.size(); 
            } 
 
            auto& state = fullState.StateIPv6; 
            EnsureRequest(state, req->Name, AF_INET6, now); 
 
            if (state.IsHardExpired(now)) { 
                Y_VERIFY(state.Waiting); 
                if (MonCounters) { 
                    ++*MonCounters->CacheMisses; 
                } 
                // We need to wait for ipv6 reply, schedule ipv4 request in parallel if needed 
                if (Options.AllowIPv4) { 
                    EnsureRequest(fullState.StateIPv4, req->Name, AF_INET, now); 
                } 
                state.WaitingRequests.PushBack(req.Release()); 
                return; 
            } 
 
            // We want to retry AF_UNSPEC with IPv4 in some cases 
            if (req->Family == AF_UNSPEC && Options.AllowIPv4 && state.RetryUnspec()) { 
                EnqueueRequestIPv4(std::move(req)); 
                return; 
            } 
 
            if (MonCounters) { 
                ++*MonCounters->CacheHits; 
            } 
 
            if (state.Status != 0) { 
                ReplyWithError(std::move(req), state.Status, state.ErrorText); 
            } else { 
                ReplyWithAddrs(std::move(req), fullState.AddrsIPv6); 
            } 
        } 
 
        void EnqueueRequestIPv4(THolder<TIncomingRequest> req, bool isCacheMiss = false) { 
            auto now = TActivationContext::Now(); 
 
            auto& fullState = NameToState[req->Name]; 
            if (MonCounters) { 
                *MonCounters->CacheSize = NameToState.size(); 
            } 
 
            auto& state = fullState.StateIPv4; 
            EnsureRequest(state, req->Name, AF_INET, now); 
 
            if (state.IsHardExpired(now)) { 
                Y_VERIFY(state.Waiting); 
                if (MonCounters && !isCacheMiss) { 
                    ++*MonCounters->CacheMisses; 
                } 
                state.WaitingRequests.PushBack(req.Release()); 
                return; 
            } 
 
            if (MonCounters && !isCacheMiss) { 
                ++*MonCounters->CacheHits; 
            } 
 
            if (state.Status != 0) { 
                ReplyWithError(std::move(req), state.Status, state.ErrorText); 
            } else { 
                ReplyWithAddrs(std::move(req), fullState.AddrsIPv4); 
            } 
        } 
 
    private: 
        struct TFamilyState { 
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
 
            bool RetryUnspec() const { 
                return ( 
                    Status == ARES_ENODATA || 
                    Status == ARES_EBADRESP || 
                    Status == ARES_ETIMEOUT); 
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
            TFamilyState StateIPv6; 
            TFamilyState StateIPv4; 
            TVector<struct in6_addr> AddrsIPv6; 
            TVector<struct in_addr> AddrsIPv4; 
 
            bool Needed() const { 
                return StateIPv6.Needed() || StateIPv4.Needed(); 
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
                switch (family) { 
                    case AF_INET6: 
                        ++*MonCounters->OutgoingInFlightV6; 
                        ++*MonCounters->OutgoingTotalV6; 
                        break; 
                    case AF_INET: 
                        ++*MonCounters->OutgoingInFlightV4; 
                        ++*MonCounters->OutgoingTotalV4; 
                        break; 
                } 
            } 
 
            ui64 reqId = ++LastRequestId; 
            auto& req = WaitingRequests[reqId]; 
            req.Position = NameToState.find(name); 
            req.Family = family; 
            Y_VERIFY(req.Position != NameToState.end()); 
 
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
 
        void ProcessErrorV6(TNameToState::iterator it, int status, TString errorText) { 
            auto now = TActivationContext::Now(); 
            if (MonCounters) { 
                --*MonCounters->OutgoingInFlightV6; 
                ++*MonCounters->OutgoingErrorsV6; 
            } 
 
            auto& state = it->second.StateIPv6; 
            Y_VERIFY(state.Waiting, "Got error for a state we are not waiting"); 
            state.Waiting = false; 
 
            // When we have a cached positive reply, don't overwrite it with spurious errors 
            const bool serverReplied = TFamilyState::ServerReplied(status); 
            if (!serverReplied && state.ServerReplied() && !state.IsHardExpired(now)) { 
                PushSoftV6(it, now + Options.SoftNegativeExpireTime); 
                if (state.Status == ARES_SUCCESS) { 
                    SendAddrsV6(it); 
                } else { 
                    SendErrorsV6(it, now); 
                } 
                return; 
            } 
 
            state.Status = status; 
            state.ErrorText = std::move(errorText); 
            PushSoftV6(it, now + Options.SoftNegativeExpireTime); 
            if (serverReplied) { 
                // Server actually replied, so keep it cached for longer 
                PushHardV6(it, now + Options.HardPositiveExpireTime); 
            } else { 
                PushHardV6(it, now + Options.HardNegativeExpireTime); 
            } 
 
            SendErrorsV6(it, now); 
        } 
 
        void SendErrorsV6(TNameToState::iterator it, TInstant now) { 
            bool cleaned = false; 
            auto& state = it->second.StateIPv6; 
            while (state.WaitingRequests) { 
                THolder<TIncomingRequest> req(state.WaitingRequests.PopFront()); 
                if (req->Family == AF_UNSPEC && Options.AllowIPv4 && state.RetryUnspec()) { 
                    if (!cleaned) { 
                        CleanupExpired(now); 
                        cleaned = true; 
                    } 
                    EnqueueRequestIPv4(std::move(req), /* isCacheMiss */ true); 
                } else { 
                    ReplyWithError(std::move(req), state.Status, state.ErrorText); 
                } 
            } 
        } 
 
        void ProcessErrorV4(TNameToState::iterator it, int status, TString errorText) { 
            auto now = TActivationContext::Now(); 
            if (MonCounters) { 
                --*MonCounters->OutgoingInFlightV4; 
                ++*MonCounters->OutgoingErrorsV4; 
            } 
 
            auto& state = it->second.StateIPv4; 
            Y_VERIFY(state.Waiting, "Got error for a state we are not waiting"); 
            state.Waiting = false; 
 
            // When we have a cached positive reply, don't overwrite it with spurious errors 
            const bool serverReplied = TFamilyState::ServerReplied(status); 
            if (!serverReplied && state.ServerReplied() && !state.IsHardExpired(now)) { 
                PushSoftV4(it, now + Options.SoftNegativeExpireTime); 
                if (state.Status == ARES_SUCCESS) { 
                    SendAddrsV4(it); 
                } else { 
                    SendErrorsV4(it); 
                } 
                return; 
            } 
 
            state.Status = status; 
            state.ErrorText = std::move(errorText); 
            PushSoftV4(it, now + Options.SoftNegativeExpireTime); 
            if (serverReplied) { 
                // Server actually replied, so keep it cached for longer 
                PushHardV4(it, now + Options.HardPositiveExpireTime); 
            } else { 
                PushHardV4(it, now + Options.HardNegativeExpireTime); 
            } 
 
            SendErrorsV4(it); 
        } 
 
        void SendErrorsV4(TNameToState::iterator it) { 
            auto& state = it->second.StateIPv4; 
            while (state.WaitingRequests) { 
                THolder<TIncomingRequest> req(state.WaitingRequests.PopFront()); 
                ReplyWithError(std::move(req), state.Status, state.ErrorText); 
            } 
        } 
 
        void ProcessAddrsV6(TNameToState::iterator it, TVector<struct in6_addr> addrs) { 
            if (Y_UNLIKELY(addrs.empty())) { 
                // Probably unnecessary: we don't want to deal with empty address lists 
                return ProcessErrorV6(it, ARES_ENODATA, ares_strerror(ARES_ENODATA)); 
            } 
 
            auto now = TActivationContext::Now(); 
            if (MonCounters) { 
                --*MonCounters->OutgoingInFlightV6; 
            } 
 
            auto& state = it->second.StateIPv6; 
            Y_VERIFY(state.Waiting, "Got reply for a state we are not waiting"); 
            state.Waiting = false; 
 
            state.Status = ARES_SUCCESS; 
            it->second.AddrsIPv6 = std::move(addrs); 
            PushSoftV6(it, now + Options.SoftPositiveExpireTime); 
            PushHardV6(it, now + Options.HardPositiveExpireTime); 
 
            SendAddrsV6(it); 
        } 
 
        void SendAddrsV6(TNameToState::iterator it) { 
            auto& state = it->second.StateIPv6; 
            while (state.WaitingRequests) { 
                THolder<TIncomingRequest> req(state.WaitingRequests.PopFront()); 
                ReplyWithAddrs(std::move(req), it->second.AddrsIPv6); 
            } 
        } 
 
        void ProcessAddrsV4(TNameToState::iterator it, TVector<struct in_addr> addrs) { 
            if (Y_UNLIKELY(addrs.empty())) { 
                // Probably unnecessary: we don't want to deal with empty address lists 
                return ProcessErrorV4(it, ARES_ENODATA, ares_strerror(ARES_ENODATA)); 
            } 
 
            auto now = TActivationContext::Now(); 
            if (MonCounters) { 
                --*MonCounters->OutgoingInFlightV4; 
            } 
 
            auto& state = it->second.StateIPv4; 
            Y_VERIFY(state.Waiting, "Got reply for a state we are not waiting"); 
            state.Waiting = false; 
 
            state.Status = ARES_SUCCESS; 
            it->second.AddrsIPv4 = std::move(addrs); 
            PushSoftV4(it, now + Options.SoftPositiveExpireTime); 
            PushHardV4(it, now + Options.HardPositiveExpireTime); 
 
            SendAddrsV4(it); 
        } 
 
        void SendAddrsV4(TNameToState::iterator it) { 
            auto& state = it->second.StateIPv4; 
            while (state.WaitingRequests) { 
                THolder<TIncomingRequest> req(state.WaitingRequests.PopFront()); 
                ReplyWithAddrs(std::move(req), it->second.AddrsIPv4); 
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
                Y_VERIFY(flag); 
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
 
        void ReplyWithAddrs(THolder<TIncomingRequest> req, const TVector<struct in6_addr>& addrs) { 
            switch (req->Type) { 
                case EIncomingRequestType::GetHostByName: { 
                    auto reply = MakeHolder<TEvDns::TEvGetHostByNameResult>(); 
                    reply->AddrsV6 = addrs; 
                    Send(req->Sender, reply.Release(), 0, req->Cookie); 
                    break; 
                } 
                case EIncomingRequestType::GetAddr: { 
                    Y_VERIFY(!addrs.empty()); 
                    auto reply = MakeHolder<TEvDns::TEvGetAddrResult>(); 
                    reply->Addr = addrs.front(); 
                    Send(req->Sender, reply.Release(), 0, req->Cookie); 
                    break; 
                } 
            } 
        } 
 
        void ReplyWithAddrs(THolder<TIncomingRequest> req, const TVector<struct in_addr>& addrs) { 
            switch (req->Type) { 
                case EIncomingRequestType::GetHostByName: { 
                    auto reply = MakeHolder<TEvDns::TEvGetHostByNameResult>(); 
                    reply->AddrsV4 = addrs; 
                    Send(req->Sender, reply.Release(), 0, req->Cookie); 
                    break; 
                } 
                case EIncomingRequestType::GetAddr: { 
                    Y_VERIFY(!addrs.empty()); 
                    auto reply = MakeHolder<TEvDns::TEvGetAddrResult>(); 
                    reply->Addr = addrs.front(); 
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
