#include "interconnect.h" 
#include "interconnect_address.h" 
#include "events_local.h" 
 
#include <library/cpp/actors/core/actor_bootstrapped.h> 
#include <library/cpp/actors/core/hfunc.h> 
#include <library/cpp/actors/dnsresolver/dnsresolver.h> 
 
namespace NActors { 
 
    using namespace NActors::NDnsResolver; 
 
    class TInterconnectResolveActor : public TActorBootstrapped<TInterconnectResolveActor> { 
    public: 
        TInterconnectResolveActor( 
                const TString& host, ui16 port, ui32 nodeId, const TString& defaultAddress, 
                const TActorId& replyTo, const TActorId& replyFrom, TInstant deadline) 
            : Host(host) 
            , NodeId(nodeId) 
            , Port(port) 
            , DefaultAddress(defaultAddress) 
            , ReplyTo(replyTo) 
            , ReplyFrom(replyFrom) 
            , Deadline(deadline) 
        { } 
 
        TInterconnectResolveActor( 
                const TString& host, ui16 port, 
                const TActorId& replyTo, const TActorId& replyFrom, TInstant deadline) 
            : Host(host) 
            , Port(port) 
            , ReplyTo(replyTo) 
            , ReplyFrom(replyFrom) 
            , Deadline(deadline) 
        { } 
 
        static constexpr EActivityType ActorActivityType() { 
            return NAMESERVICE; 
        } 
 
        void Bootstrap() { 
            TMaybe<TString> errorText; 
            if (auto addr = ExtractDefaultAddr(errorText)) { 
                return SendAddrAndDie(std::move(addr)); 
            } 
 
            if (errorText) { 
                SendErrorAndDie(*errorText); 
            } 
 
            auto now = TActivationContext::Now(); 
            if (Deadline < now) { 
                SendErrorAndDie("Deadline"); 
                return; 
            } 
 
            Send(MakeDnsResolverActorId(), 
                    new TEvDns::TEvGetAddr(Host, AF_UNSPEC), 
                    IEventHandle::FlagTrackDelivery); 
 
            if (Deadline != TInstant::Max()) { 
                Schedule(Deadline, new TEvents::TEvWakeup); 
            } 
 
            Become(&TThis::StateWork); 
        } 
 
        STRICT_STFUNC(StateWork, { 
            sFunc(TEvents::TEvWakeup, HandleTimeout); 
            sFunc(TEvents::TEvUndelivered, HandleUndelivered); 
            hFunc(TEvDns::TEvGetAddrResult, Handle); 
        }); 
 
        void HandleTimeout() { 
            SendErrorAndDie("Deadline"); 
        } 
 
        void HandleUndelivered() { 
            SendErrorAndDie("Dns resolver is unavailable"); 
        } 
 
        void Handle(TEvDns::TEvGetAddrResult::TPtr& ev) { 
            if (auto addr = ExtractAddr(ev->Get())) { 
                return SendAddrAndDie(std::move(addr)); 
            } 
 
            SendErrorAndDie(ev->Get()->ErrorText); 
        } 
 
        void SendAddrAndDie(NAddr::IRemoteAddrPtr addr) { 
            if (NodeId) { 
                auto reply = new TEvLocalNodeInfo; 
                reply->NodeId = *NodeId; 
                reply->Address = std::move(addr); 
                TActivationContext::Send(new IEventHandle(ReplyTo, ReplyFrom, reply)); 
            } else { 
                auto reply = new TEvAddressInfo; 
                reply->Address = std::move(addr); 
                TActivationContext::Send(new IEventHandle(ReplyTo, ReplyFrom, reply)); 
            } 
            PassAway(); 
        } 
 
        void SendErrorAndDie(const TString& errorText) { 
            auto *event = new TEvResolveError; 
            event->Explain = errorText; 
            TActivationContext::Send(new IEventHandle(ReplyTo, ReplyFrom, event)); 
            PassAway(); 
        } 
 
        NAddr::IRemoteAddrPtr ExtractAddr(TEvDns::TEvGetAddrResult* msg) { 
            if (msg->Status == 0) { 
                if (msg->IsV6()) { 
                    struct sockaddr_in6 sin6; 
                    Zero(sin6); 
                    sin6.sin6_family = AF_INET6; 
                    sin6.sin6_addr = msg->GetAddrV6(); 
                    sin6.sin6_port = HostToInet(Port); 
                    return MakeHolder<NAddr::TIPv6Addr>(sin6); 
                } 
 
                if (msg->IsV4()) { 
                    return MakeHolder<NAddr::TIPv4Addr>(TIpAddress(msg->GetAddrV4().s_addr, Port)); 
                } 
 
                Y_FAIL("Unexpected result address family"); 
            } 
 
            return nullptr; 
        } 
 
        NAddr::IRemoteAddrPtr ExtractDefaultAddr(TMaybe<TString>& errorText) { 
            if (DefaultAddress) { 
                NInterconnect::TAddress address(DefaultAddress.data(), Port); 
 
                switch (address.GetFamily()) { 
                case AF_INET: 
                    return MakeHolder<NAddr::TIPv4Addr>(*(sockaddr_in*)address.SockAddr()); 
                case AF_INET6: 
                    return MakeHolder<NAddr::TIPv6Addr>(*(sockaddr_in6*)address.SockAddr()); 
                default: 
                    errorText = "Unsupported default address: " + DefaultAddress; 
                    break; 
                } 
            } 
 
            return nullptr; 
        } 
 
    private: 
        const TString Host; 
        const std::optional<ui32> NodeId; 
        const ui16 Port; 
        const TString DefaultAddress; 
        const TActorId ReplyTo; 
        const TActorId ReplyFrom; 
        const TInstant Deadline; 
    }; 
 
    IActor* CreateResolveActor( 
        const TString& host, ui16 port, ui32 nodeId, const TString& defaultAddress, 
        const TActorId& replyTo, const TActorId& replyFrom, TInstant deadline) 
    { 
        return new TInterconnectResolveActor(host, port, nodeId, defaultAddress, replyTo, replyFrom, deadline); 
    } 
 
    IActor* CreateResolveActor( 
        const TString& host, ui16 port, 
        const TActorId& replyTo, const TActorId& replyFrom, TInstant deadline) 
    { 
        return new TInterconnectResolveActor(host, port, replyTo, replyFrom, deadline); 
    } 
 
} // namespace NActors 
