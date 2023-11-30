#include "dnsresolver.h"

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/builder.h>

#include <ares.h>

using namespace NActors;
using namespace NActors::NDnsResolver;

// FIXME: use a mock resolver
Y_UNIT_TEST_SUITE(CachingDnsResolver) {

    struct TAddrToString {
        TString operator()(const std::monostate&) const {
            return "<nothing>";
        }

        TString operator()(const struct in6_addr& addr) const {
            char dst[INET6_ADDRSTRLEN];
            auto res = ares_inet_ntop(AF_INET6, &addr, dst, INET6_ADDRSTRLEN);
            Y_ABORT_UNLESS(res, "Cannot convert ipv6 address");
            return dst;
        }

        TString operator()(const struct in_addr& addr) const {
            char dst[INET_ADDRSTRLEN];
            auto res = ares_inet_ntop(AF_INET, &addr, dst, INET_ADDRSTRLEN);
            Y_ABORT_UNLESS(res, "Cannot convert ipv4 address");
            return dst;
        }
    };

    TString AddrToString(const std::variant<std::monostate, struct in6_addr, struct in_addr>& v) {
        return std::visit(TAddrToString(), v);
    }

    struct TMockReply {
        static constexpr TDuration DefaultDelay = TDuration::MilliSeconds(1);

        int Status = 0;
        TDuration Delay;
        TVector<struct in6_addr> AddrsV6;
        TVector<struct in_addr> AddrsV4;

        static TMockReply Error(int status, TDuration delay = DefaultDelay) {
            Y_ABORT_UNLESS(status != 0);
            TMockReply reply;
            reply.Status = status;
            reply.Delay = delay;
            return reply;
        }

        static TMockReply Empty(TDuration delay = DefaultDelay) {
            TMockReply reply;
            reply.Delay = delay;
            return reply;
        }

        static TMockReply ManyV6(const TVector<TString>& addrs, TDuration delay = DefaultDelay) {
            TMockReply reply;
            reply.Delay = delay;
            for (const TString& addr : addrs) {
                void* dst = &reply.AddrsV6.emplace_back();
                int status = ares_inet_pton(AF_INET6, addr.c_str(), dst);
                Y_ABORT_UNLESS(status == 1, "Invalid ipv6 address: %s", addr.c_str());
            }
            return reply;
        }

        static TMockReply ManyV4(const TVector<TString>& addrs, TDuration delay = DefaultDelay) {
            TMockReply reply;
            reply.Delay = delay;
            for (const TString& addr : addrs) {
                void* dst = &reply.AddrsV4.emplace_back();
                int status = ares_inet_pton(AF_INET, addr.c_str(), dst);
                Y_ABORT_UNLESS(status == 1, "Invalid ipv4 address: %s", addr.c_str());
            }
            return reply;
        }

        static TMockReply SingleV6(const TString& addr, TDuration delay = DefaultDelay) {
            return ManyV6({ addr }, delay);
        }

        static TMockReply SingleV4(const TString& addr, TDuration delay = DefaultDelay) {
            return ManyV4({ addr }, delay);
        }

        friend TMockReply operator+(const TMockReply& a, const TMockReply& b) {
            Y_ABORT_UNLESS(a.Status == b.Status);
            TMockReply result;
            result.Status = a.Status;
            result.Delay = Max(a.Delay, b.Delay);
            result.AddrsV6.insert(result.AddrsV6.end(), a.AddrsV6.begin(), a.AddrsV6.end());
            result.AddrsV6.insert(result.AddrsV6.end(), b.AddrsV6.begin(), b.AddrsV6.end());
            result.AddrsV4.insert(result.AddrsV4.end(), a.AddrsV4.begin(), a.AddrsV4.end());
            result.AddrsV4.insert(result.AddrsV4.end(), b.AddrsV4.begin(), b.AddrsV4.end());
            return result;
        }
    };

    using TMockDnsCallback = std::function<TMockReply (const TString&, int)>;

    class TMockDnsResolver : public TActor<TMockDnsResolver> {
    public:
        TMockDnsResolver(TMockDnsCallback callback)
            : TActor(&TThis::StateWork)
            , Callback(std::move(callback))
        { }

    private:
        struct TEvPrivate {
            enum EEv {
                EvScheduled = EventSpaceBegin(TEvents::ES_PRIVATE),
            };

            struct TEvScheduled : public TEventLocal<TEvScheduled, EvScheduled> {
                TActorId Sender;
                ui64 Cookie;
                TMockReply Reply;

                TEvScheduled(TActorId sender, ui64 cookie, TMockReply reply)
                    : Sender(sender)
                    , Cookie(cookie)
                    , Reply(std::move(reply))
                { }
            };
        };

    private:
        STRICT_STFUNC(StateWork, {
            hFunc(TEvents::TEvPoison, Handle);
            hFunc(TEvDns::TEvGetHostByName, Handle);
            hFunc(TEvPrivate::TEvScheduled, Handle);
        });

        void Handle(TEvents::TEvPoison::TPtr&) {
            PassAway();
        }

        void Handle(TEvDns::TEvGetHostByName::TPtr& ev) {
            auto reply = Callback(ev->Get()->Name, ev->Get()->Family);
            if (reply.Delay) {
                Schedule(reply.Delay, new TEvPrivate::TEvScheduled(ev->Sender, ev->Cookie, std::move(reply)));
            } else {
                SendReply(ev->Sender, ev->Cookie, std::move(reply));
            }
        }

        void Handle(TEvPrivate::TEvScheduled::TPtr& ev) {
            SendReply(ev->Get()->Sender, ev->Get()->Cookie, std::move(ev->Get()->Reply));
        }

    private:
        void SendReply(const TActorId& sender, ui64 cookie, TMockReply&& reply) {
            auto res = MakeHolder<TEvDns::TEvGetHostByNameResult>();
            res->Status = reply.Status;
            if (res->Status != 0) {
                res->ErrorText = ares_strerror(res->Status);
            } else {
                res->AddrsV6 = std::move(reply.AddrsV6);
                res->AddrsV4 = std::move(reply.AddrsV4);
            }
            Send(sender, res.Release(), 0, cookie);
        }

    private:
        TMockDnsCallback Callback;
    };

    struct TCachingDnsRuntime : public TTestActorRuntimeBase {
        TCachingDnsResolverOptions ResolverOptions;
        TActorId MockResolver;
        TActorId Resolver;
        TActorId Sleeper;
        TString Section_;

        NMonitoring::TDynamicCounters::TCounterPtr InFlightUnspec;
        NMonitoring::TDynamicCounters::TCounterPtr InFlight6;
        NMonitoring::TDynamicCounters::TCounterPtr InFlight4;
        NMonitoring::TDynamicCounters::TCounterPtr TotalUnspec;
        NMonitoring::TDynamicCounters::TCounterPtr Total6;
        NMonitoring::TDynamicCounters::TCounterPtr Total4;
        NMonitoring::TDynamicCounters::TCounterPtr Misses;
        NMonitoring::TDynamicCounters::TCounterPtr Hits;

        THashMap<TString, TMockReply> ReplyV6;
        THashMap<TString, TMockReply> ReplyV4;
        THashMap<TString, TMockReply> ReplyUnspec;

        TCachingDnsRuntime() {
            SetScheduledEventFilter([](auto&&, auto&&, auto&&, auto&&) { return false; });
            ResolverOptions.MonCounters = new NMonitoring::TDynamicCounters;

            ReplyV6["localhost"] = TMockReply::SingleV6("::1");
            ReplyV4["localhost"] = TMockReply::SingleV4("127.0.0.1");
            ReplyV6["yandex.ru"] = TMockReply::SingleV6("2a02:6b8:a::a", TDuration::MilliSeconds(500));
            ReplyV4["yandex.ru"] = TMockReply::SingleV4("77.88.55.77", TDuration::MilliSeconds(250));
            ReplyV6["router.asus.com"] = TMockReply::Error(ARES_ENODATA);
            ReplyV4["router.asus.com"] = TMockReply::SingleV4("192.168.0.1");
            ReplyUnspec["localhost"] = ReplyV6.at("localhost") + ReplyV4.at("localhost");
            ReplyUnspec["yandex.ru"] = ReplyV6.at("yandex.ru") + ReplyV4.at("yandex.ru");
            ReplyUnspec["router.asus.com"] = ReplyV4.at("router.asus.com");
        }

        void Start(TMockDnsCallback callback) {
            MockResolver = Register(new TMockDnsResolver(std::move(callback)));
            EnableScheduleForActor(MockResolver);
            Resolver = Register(CreateCachingDnsResolver(MockResolver, ResolverOptions));
            Sleeper = AllocateEdgeActor();

            InFlightUnspec = ResolverOptions.MonCounters->GetCounter("DnsResolver/Outgoing/InFlight/Unspec", false);
            InFlight6 = ResolverOptions.MonCounters->GetCounter("DnsResolver/Outgoing/InFlight/V6", false);
            InFlight4 = ResolverOptions.MonCounters->GetCounter("DnsResolver/Outgoing/InFlight/V4", false);
            TotalUnspec = ResolverOptions.MonCounters->GetCounter("DnsResolver/Outgoing/Total/Unspec", true);
            Total6 = ResolverOptions.MonCounters->GetCounter("DnsResolver/Outgoing/Total/V6", true);
            Total4 = ResolverOptions.MonCounters->GetCounter("DnsResolver/Outgoing/Total/V4", true);
            Misses = ResolverOptions.MonCounters->GetCounter("DnsResolver/Cache/Misses", true);
            Hits = ResolverOptions.MonCounters->GetCounter("DnsResolver/Cache/Hits", true);
        }

        void Start() {
            Start([this](const TString& name, int family) {
                switch (family) {
                    case AF_UNSPEC: {
                        auto it = ReplyUnspec.find(name);
                        if (it != ReplyUnspec.end()) {
                            return it->second;
                        }
                        break;
                    }
                    case AF_INET6: {
                        auto it = ReplyV6.find(name);
                        if (it != ReplyV6.end()) {
                            return it->second;
                        }
                        break;
                    }
                    case AF_INET: {
                        auto it = ReplyV4.find(name);
                        if (it != ReplyV4.end()) {
                            return it->second;
                        }
                        break;
                    }
                }
                return TMockReply::Error(ARES_ENOTFOUND);
            });
        }

        void Section(const TString& section) {
            Section_ = section;
        }

        void Sleep(TDuration duration) {
            Schedule(new IEventHandle(Sleeper, Sleeper, new TEvents::TEvWakeup), duration);
            GrabEdgeEventRethrow<TEvents::TEvWakeup>(Sleeper);
        }

        void WaitNoInFlight() {
            if (*InFlightUnspec || *InFlight6 || *InFlight4) {
                TDispatchOptions options;
                options.CustomFinalCondition = [&]() {
                    return !*InFlightUnspec && !*InFlight6 && !*InFlight4;
                };
                DispatchEvents(options);
                UNIT_ASSERT_C(!*InFlight6 && !*InFlight4, "Failed to wait for no inflight in " << Section_);
            }
        }

        void SendGetHostByName(const TActorId& sender, const TString& name, int family = AF_UNSPEC) {
            Send(new IEventHandle(Resolver, sender, new TEvDns::TEvGetHostByName(name, family)), 0, true);
        }

        void SendGetAddr(const TActorId& sender, const TString& name, int family = AF_UNSPEC) {
            Send(new IEventHandle(Resolver, sender, new TEvDns::TEvGetAddr(name, family)), 0, true);
        }

        TEvDns::TEvGetHostByNameResult::TPtr WaitGetHostByName(const TActorId& sender) {
            return GrabEdgeEventRethrow<TEvDns::TEvGetHostByNameResult>(sender);
        }

        TEvDns::TEvGetAddrResult::TPtr WaitGetAddr(const TActorId& sender) {
            return GrabEdgeEventRethrow<TEvDns::TEvGetAddrResult>(sender);
        }

        void ExpectInFlightUnspec(i64 count) {
            UNIT_ASSERT_VALUES_EQUAL_C(InFlightUnspec->Val(), count, Section_);
        }

        void ExpectInFlight6(i64 count) {
            UNIT_ASSERT_VALUES_EQUAL_C(InFlight6->Val(), count, Section_);
        }

        void ExpectInFlight4(i64 count) {
            UNIT_ASSERT_VALUES_EQUAL_C(InFlight4->Val(), count, Section_);
        }

        void ExpectTotalUnspec(i64 count) {
            UNIT_ASSERT_VALUES_EQUAL_C(TotalUnspec->Val(), count, Section_);
        }

        void ExpectTotal6(i64 count) {
            UNIT_ASSERT_VALUES_EQUAL_C(Total6->Val(), count, Section_);
        }

        void ExpectTotal4(i64 count) {
            UNIT_ASSERT_VALUES_EQUAL_C(Total4->Val(), count, Section_);
        }

        void ExpectUnspec(i64 total, i64 inflight) {
            UNIT_ASSERT_C(
                TotalUnspec->Val() == total && InFlightUnspec->Val() == inflight,
                Section_ << ": Expect6(" << total << ", " << inflight << ") "
                << " but got (" << TotalUnspec->Val() << ", " << InFlightUnspec->Val() << ")");
        }

        void Expect6(i64 total, i64 inflight) {
            UNIT_ASSERT_C(
                Total6->Val() == total && InFlight6->Val() == inflight,
                Section_ << ": Expect6(" << total << ", " << inflight << ") "
                << " but got (" << Total6->Val() << ", " << InFlight6->Val() << ")");
        }

        void Expect4(i64 total, i64 inflight) {
            UNIT_ASSERT_C(
                Total4->Val() == total && InFlight4->Val() == inflight,
                Section_ << ": Expect4(" << total << ", " << inflight << ") "
                << " got (" << Total4->Val() << ", " << InFlight4->Val() << ")");
        }

        void ExpectMisses(i64 count) {
            UNIT_ASSERT_VALUES_EQUAL_C(Misses->Val(), count, Section_);
        }

        void ExpectHits(i64 count) {
            UNIT_ASSERT_VALUES_EQUAL_C(Hits->Val(), count, Section_);
        }

        void ExpectGetHostByNameError(const TActorId& sender, int status) {
            auto ev = WaitGetHostByName(sender);
            UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, status, Section_ << ": " << ev->Get()->ErrorText);
        }

        void ExpectGetAddrError(const TActorId& sender, int status) {
            auto ev = WaitGetAddr(sender);
            UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, status, Section_ << ": " << ev->Get()->ErrorText);
        }

        void ExpectGetHostByNameSuccess(const TActorId& sender, const TString& expected) {
            auto ev = WaitGetHostByName(sender);
            UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, 0, Section_ << ": " << ev->Get()->ErrorText);
            TStringBuilder result;
            for (const auto& addr : ev->Get()->AddrsV6) {
                if (result) {
                    result << ',';
                }
                result << TAddrToString()(addr);
            }
            for (const auto& addr : ev->Get()->AddrsV4) {
                if (result) {
                    result << ',';
                }
                result << TAddrToString()(addr);
            }
            UNIT_ASSERT_VALUES_EQUAL_C(TString(result), expected, Section_);
        }

        void ExpectGetAddrSuccess(const TActorId& sender, const TString& expected) {
            auto ev = WaitGetAddr(sender);
            UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, 0, Section_ << ": " << ev->Get()->ErrorText);
            TString result = AddrToString(ev->Get()->Addr);
            UNIT_ASSERT_VALUES_EQUAL_C(result, expected, Section_);
        }
    };

    Y_UNIT_TEST(UnusableResolver) {
        TCachingDnsRuntime runtime;
        runtime.Initialize();
        runtime.Start();

        auto sender = runtime.AllocateEdgeActor();

        runtime.SendGetAddr(sender, "yandex.ru", AF_UNSPEC);
        runtime.ExpectGetAddrSuccess(sender, "2a02:6b8:a::a");

        runtime.Send(new IEventHandle(runtime.MockResolver, { }, new TEvents::TEvPoison), 0, true);
        runtime.SendGetAddr(sender, "foo.ru", AF_UNSPEC);
        runtime.ExpectGetAddrError(sender, ARES_ENOTINITIALIZED);
    }

    Y_UNIT_TEST(ResolveCaching) {
        TCachingDnsRuntime runtime;
        runtime.Initialize();
        runtime.Start();

        auto sender = runtime.AllocateEdgeActor();

        // First time resolve, we expect AF_UNSPEC result to be cached
        runtime.Section("First time resolve");
        runtime.SendGetAddr(sender, "yandex.ru", AF_UNSPEC);
        runtime.ExpectGetAddrSuccess(sender, "2a02:6b8:a::a");
        runtime.ExpectUnspec(1, 0);
        runtime.ExpectMisses(1);
        runtime.ExpectHits(0);

        // Second resolve, unspec is a cache hit, ipv6 and ipv4 result in cache misses
        runtime.Section("Second resolve");
        runtime.SendGetAddr(sender, "yandex.ru", AF_UNSPEC);
        runtime.ExpectGetAddrSuccess(sender, "2a02:6b8:a::a");
        runtime.ExpectUnspec(1, 0);
        runtime.ExpectHits(1);

        // Wait until soft expiration and try unspec again
        // Will cause a cache hit, but will start a new request in background
        runtime.Section("Retry both after soft expiration");
        runtime.Sleep(TDuration::Seconds(15));
        runtime.SendGetAddr(sender, "yandex.ru", AF_UNSPEC);
        runtime.ExpectGetAddrSuccess(sender, "2a02:6b8:a::a");
        runtime.ExpectUnspec(2, 1);
        runtime.ExpectMisses(1);
        runtime.ExpectHits(2);
        runtime.WaitNoInFlight();

        // Wait until hard expiration and try unspec again
        // Will cause a cache miss and new resolve requests
        runtime.Section("Retry both after hard expiration");
        runtime.Sleep(TDuration::Hours(2));
        runtime.SendGetAddr(sender, "yandex.ru", AF_UNSPEC);
        runtime.ExpectGetAddrSuccess(sender, "2a02:6b8:a::a");
        runtime.ExpectUnspec(3, 0);
        runtime.ExpectMisses(2);
        runtime.ExpectHits(2);

        // Wait half the hard expiration time, must always result in a cache hit
        runtime.Section("Retry both after half hard expiration");
        for (ui64 i = 1; i <= 4; ++i) {
            runtime.Sleep(TDuration::Hours(1));
            runtime.SendGetAddr(sender, "yandex.ru", AF_UNSPEC);
            runtime.ExpectGetAddrSuccess(sender, "2a02:6b8:a::a");
            runtime.ExpectUnspec(3 + i, 1);
            runtime.ExpectHits(2 + i);
            runtime.WaitNoInFlight();
        }

        // Change unspec result to a timeout, must keep using cached result until hard expiration
        runtime.Section("Dns keeps timing out");
        runtime.ReplyUnspec["yandex.ru"] = TMockReply::Error(ARES_ETIMEOUT);
        for (ui64 i = 1; i <= 4; ++i) {
            runtime.Sleep(TDuration::Seconds(15));
            runtime.SendGetAddr(sender, "yandex.ru", AF_UNSPEC);
            runtime.ExpectGetAddrSuccess(sender, "2a02:6b8:a::a");
            runtime.ExpectUnspec(7 + i, 1);
            runtime.ExpectHits(6 + i);
            runtime.WaitNoInFlight();
        }

        // Change unspec result to v4, must switch to a v4 result eventually
        runtime.Section("Host changes to being ipv4 only");
        runtime.ReplyUnspec["yandex.ru"] = runtime.ReplyV4.at("yandex.ru");
        runtime.Sleep(TDuration::Seconds(2));
        runtime.SendGetAddr(sender, "yandex.ru", AF_UNSPEC);
        runtime.ExpectGetAddrSuccess(sender, "2a02:6b8:a::a");
        runtime.WaitNoInFlight();
        runtime.SendGetAddr(sender, "yandex.ru", AF_UNSPEC);
        runtime.ExpectGetAddrSuccess(sender, "77.88.55.77");
        runtime.ExpectUnspec(12, 0);
        runtime.ExpectMisses(2);

        // Change unspec result to nxdomain, must start returning it
        runtime.Section("Host is removed from dns");
        runtime.ReplyUnspec["yandex.ru"] = TMockReply::Error(ARES_ENOTFOUND);
        runtime.Sleep(TDuration::Seconds(15));
        runtime.SendGetAddr(sender, "yandex.ru", AF_UNSPEC);
        runtime.ExpectGetAddrSuccess(sender, "77.88.55.77");
        runtime.WaitNoInFlight();
        runtime.SendGetAddr(sender, "yandex.ru", AF_UNSPEC);
        runtime.ExpectGetAddrError(sender, ARES_ENOTFOUND);
    }

    Y_UNIT_TEST(ResolveCachingV4) {
        TCachingDnsRuntime runtime;
        runtime.Initialize();
        runtime.Start();

        auto sender = runtime.AllocateEdgeActor();

        runtime.Section("First request");
        runtime.SendGetAddr(sender, "router.asus.com", AF_UNSPEC);
        runtime.ExpectGetAddrSuccess(sender, "192.168.0.1");
        runtime.ExpectMisses(1);

        runtime.Section("Second request");
        runtime.SendGetAddr(sender, "router.asus.com", AF_UNSPEC);
        runtime.ExpectGetAddrSuccess(sender, "192.168.0.1");
        runtime.ExpectHits(1);

        runtime.Section("Dns keeps timing out");
        runtime.ReplyUnspec["router.asus.com"] = TMockReply::Error(ARES_ETIMEOUT);
        for (ui64 i = 1; i <= 4; ++i) {
            runtime.Sleep(TDuration::Seconds(15));
            runtime.SendGetAddr(sender, "router.asus.com", AF_UNSPEC);
            runtime.ExpectGetAddrSuccess(sender, "192.168.0.1");
            runtime.ExpectUnspec(1 + i, 1);
            runtime.ExpectHits(1 + i);
            runtime.WaitNoInFlight();
        }

        runtime.Section("Host is removed from dns");
        runtime.ReplyUnspec["router.asus.com"] = TMockReply::Error(ARES_ENOTFOUND);
        runtime.Sleep(TDuration::Seconds(15));
        runtime.SendGetAddr(sender, "router.asus.com", AF_UNSPEC);
        runtime.ExpectGetAddrSuccess(sender, "192.168.0.1");
        runtime.WaitNoInFlight();
        runtime.SendGetAddr(sender, "router.asus.com", AF_UNSPEC);
        runtime.ExpectGetAddrError(sender, ARES_ENOTFOUND);
    }

    Y_UNIT_TEST(EventualTimeout) {
        TCachingDnsRuntime runtime;
        runtime.Initialize();
        runtime.Start();

        auto sender = runtime.AllocateEdgeActor();

        runtime.ReplyUnspec["notfound.ru"] = TMockReply::Error(ARES_ENOTFOUND);
        runtime.SendGetAddr(sender, "notfound.ru", AF_UNSPEC);
        runtime.ExpectGetAddrError(sender, ARES_ENOTFOUND);

        runtime.ReplyUnspec["notfound.ru"] = TMockReply::Error(ARES_ETIMEOUT);
        runtime.SendGetAddr(sender, "notfound.ru", AF_UNSPEC);
        runtime.ExpectGetAddrError(sender, ARES_ENOTFOUND);
        runtime.WaitNoInFlight();

        bool timeout = false;
        for (ui64 i = 1; i <= 8; ++i) {
            runtime.Sleep(TDuration::Minutes(30));
            runtime.SendGetAddr(sender, "notfound.ru", AF_UNSPEC);
            auto ev = runtime.WaitGetAddr(sender);
            if (ev->Get()->Status == ARES_ETIMEOUT && i > 2) {
                timeout = true;
                break;
            }
            UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, ARES_ENOTFOUND,
                    "Iteration " << i << ": " << ev->Get()->ErrorText);
        }

        UNIT_ASSERT_C(timeout, "DnsResolver did not reply with a timeout");
    }

    Y_UNIT_TEST(MultipleRequestsAndHosts) {
        TCachingDnsRuntime runtime;
        runtime.Initialize();
        runtime.Start();

        auto sender = runtime.AllocateEdgeActor();

        runtime.SendGetHostByName(sender, "router.asus.com", AF_UNSPEC);
        runtime.SendGetAddr(sender, "router.asus.com", AF_UNSPEC);
        runtime.SendGetHostByName(sender, "yandex.ru", AF_UNSPEC);
        runtime.SendGetAddr(sender, "yandex.ru", AF_UNSPEC);
        runtime.ExpectGetHostByNameSuccess(sender, "192.168.0.1");
        runtime.ExpectGetAddrSuccess(sender, "192.168.0.1");
        runtime.ExpectGetHostByNameSuccess(sender, "2a02:6b8:a::a,77.88.55.77");
        runtime.ExpectGetAddrSuccess(sender, "2a02:6b8:a::a");

        runtime.SendGetHostByName(sender, "notfound.ru", AF_UNSPEC);
        runtime.SendGetAddr(sender, "notfound.ru", AF_UNSPEC);
        runtime.ExpectGetHostByNameError(sender, ARES_ENOTFOUND);
        runtime.ExpectGetAddrError(sender, ARES_ENOTFOUND);
    }

    Y_UNIT_TEST(DisabledIPv6) {
        TCachingDnsRuntime runtime;
        runtime.ResolverOptions.AllowIPv6 = false;
        runtime.Initialize();
        runtime.Start();

        auto sender = runtime.AllocateEdgeActor();

        runtime.SendGetHostByName(sender, "yandex.ru", AF_UNSPEC);
        runtime.SendGetAddr(sender, "yandex.ru", AF_UNSPEC);
        runtime.ExpectGetHostByNameSuccess(sender, "77.88.55.77");
        runtime.ExpectGetAddrSuccess(sender, "77.88.55.77");

        runtime.SendGetHostByName(sender, "yandex.ru", AF_INET6);
        runtime.SendGetAddr(sender, "yandex.ru", AF_INET6);
        runtime.ExpectGetHostByNameError(sender, ARES_EBADFAMILY);
        runtime.ExpectGetAddrError(sender, ARES_EBADFAMILY);

        runtime.SendGetHostByName(sender, "yandex.ru", AF_UNSPEC);
        runtime.SendGetAddr(sender, "yandex.ru", AF_UNSPEC);
        runtime.ExpectGetHostByNameSuccess(sender, "77.88.55.77");
        runtime.ExpectGetAddrSuccess(sender, "77.88.55.77");

        runtime.SendGetHostByName(sender, "notfound.ru", AF_UNSPEC);
        runtime.SendGetAddr(sender, "notfound.ru", AF_UNSPEC);
        runtime.ExpectGetHostByNameError(sender, ARES_ENOTFOUND);
        runtime.ExpectGetAddrError(sender, ARES_ENOTFOUND);
    }

    Y_UNIT_TEST(DisabledIPv4) {
        TCachingDnsRuntime runtime;
        runtime.ResolverOptions.AllowIPv4 = false;
        runtime.Initialize();
        runtime.Start();

        auto sender = runtime.AllocateEdgeActor();

        runtime.SendGetHostByName(sender, "router.asus.com", AF_UNSPEC);
        runtime.SendGetAddr(sender, "router.asus.com", AF_UNSPEC);
        runtime.ExpectGetHostByNameError(sender, ARES_ENODATA);
        runtime.ExpectGetAddrError(sender, ARES_ENODATA);

        runtime.SendGetHostByName(sender, "router.asus.com", AF_INET);
        runtime.SendGetAddr(sender, "router.asus.com", AF_INET);
        runtime.ExpectGetHostByNameError(sender, ARES_EBADFAMILY);
        runtime.ExpectGetAddrError(sender, ARES_EBADFAMILY);

        runtime.SendGetHostByName(sender, "router.asus.com", AF_UNSPEC);
        runtime.SendGetAddr(sender, "router.asus.com", AF_UNSPEC);
        runtime.ExpectGetHostByNameError(sender, ARES_ENODATA);
        runtime.ExpectGetAddrError(sender, ARES_ENODATA);

        runtime.SendGetHostByName(sender, "notfound.ru", AF_UNSPEC);
        runtime.SendGetAddr(sender, "notfound.ru", AF_UNSPEC);
        runtime.ExpectGetHostByNameError(sender, ARES_ENOTFOUND);
        runtime.ExpectGetAddrError(sender, ARES_ENOTFOUND);
    }

    Y_UNIT_TEST(PoisonPill) {
        TCachingDnsRuntime runtime;
        runtime.Initialize();
        runtime.Start();

        auto sender = runtime.AllocateEdgeActor();

        runtime.SendGetHostByName(sender, "yandex.ru", AF_UNSPEC);
        runtime.SendGetAddr(sender, "yandex.ru", AF_UNSPEC);
        runtime.Send(new IEventHandle(runtime.Resolver, sender, new TEvents::TEvPoison), 0, true);
        runtime.ExpectGetHostByNameError(sender, ARES_ECANCELLED);
        runtime.ExpectGetAddrError(sender, ARES_ECANCELLED);
    }

}
