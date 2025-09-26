#include "interconnect_mon.h"
#include "interconnect_tcp_proxy.h"

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <openssl/ssl.h>
#include <openssl/pem.h>

namespace NInterconnect {

    using namespace NActors;

    class TInterconnectMonActor : public TActor<TInterconnectMonActor> {
        class TQueryProcessor : public TActorBootstrapped<TQueryProcessor> {
            const TActorId Sender;
            const bool Json;
            TMap<ui32, TInterconnectProxyTCP::TProxyStats> Stats;
            ui32 PendingReplies = 0;

        public:
            static constexpr IActor::EActorActivity ActorActivityType() {
                return EActivityType::INTERCONNECT_MONACTOR;
            }

            TQueryProcessor(const TActorId& sender, bool json)
                : Sender(sender)
                , Json(json)
            {}

            void Bootstrap(const TActorContext& ctx) {
                Become(&TThis::StateFunc, ctx, TDuration::Seconds(5), new TEvents::TEvWakeup);
                Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes);
            }

            void Handle(TEvInterconnect::TEvNodesInfo::TPtr ev, const TActorContext& ctx) {
                TActorSystem* const as = ctx.ExecutorThread.ActorSystem;
                for (const auto& node : ev->Get()->Nodes) {
                    Send(as->InterconnectProxy(node.NodeId), new TInterconnectProxyTCP::TEvQueryStats, IEventHandle::FlagTrackDelivery);
                    ++PendingReplies;
                }
                GenerateResultWhenReady(ctx);
            }

            STRICT_STFUNC(StateFunc,
                HFunc(TEvInterconnect::TEvNodesInfo, Handle)
                HFunc(TInterconnectProxyTCP::TEvStats, Handle)
                CFunc(TEvents::TSystem::Undelivered, HandleUndelivered)
                CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
            )

            void Handle(TInterconnectProxyTCP::TEvStats::TPtr& ev, const TActorContext& ctx) {
                auto *msg = ev->Get();
                Stats.emplace(msg->PeerNodeId, std::move(msg->ProxyStats));
                --PendingReplies;
                GenerateResultWhenReady(ctx);
            }

            void HandleUndelivered(const TActorContext& ctx) {
                --PendingReplies;
                GenerateResultWhenReady(ctx);
            }

            void HandleWakeup(const TActorContext& ctx) {
                PendingReplies = 0;
                GenerateResultWhenReady(ctx);
            }

            void GenerateResultWhenReady(const TActorContext& ctx) {
                if (!PendingReplies) {
                    if (Json) {
                        ctx.Send(Sender, new NMon::TEvHttpInfoRes(GenerateJson(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
                    } else {
                        ctx.Send(Sender, new NMon::TEvHttpInfoRes(GenerateHtml()));
                    }
                    Die(ctx);
                }
            }

            TString GenerateHtml() {
                TStringStream str;
                HTML(str) {
                    TABLE_CLASS("table-sortable table") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "Peer node id"; }
                                TABLEH() { str << "State"; }
                                TABLEH() { str << "Ping"; }
                                TABLEH() { str << "Clock skew"; }
                                TABLEH() { str << "Scope id"; }
                                TABLEH() { str << "Encryption"; }
                                TABLEH() { str << "LastSessionDieTime"; }
                                TABLEH() { str << "TotalOutputQueueSize"; }
                                TABLEH() { str << "Connected"; }
                                TABLEH() { str << "XDC"; }
                                TABLEH() { str << "Host"; }
                                TABLEH() { str << "Port"; }
                                TABLEH() { str << "LastErrorTimestamp"; }
                                TABLEH() { str << "LastErrorKind"; }
                                TABLEH() { str << "LastErrorExplanation"; }
                            }
                        }
                        TABLEBODY() {
                            for (const auto& kv : Stats) {
                                TABLER() {
                                    TABLED() { str << "<a href='" << kv.second.Path << "'>" << kv.first << "</a>"; }
                                    TABLED() { str << kv.second.State; }
                                    TABLED() {
                                        if (kv.second.Ping != TDuration::Zero()) {
                                            str << kv.second.Ping;
                                        }
                                    }
                                    TABLED() {
                                        if (kv.second.ClockSkew < 0) {
                                            str << "-" << TDuration::MicroSeconds(-kv.second.ClockSkew);
                                        } else {
                                            str << "+" << TDuration::MicroSeconds(kv.second.ClockSkew);
                                        }
                                    }
                                    TABLED() { str << ScopeIdToString(kv.second.PeerScopeId); }
                                    TABLED() {
                                        const char *color = kv.second.Encryption != "none" ? "green" : "red";
                                        str << "<font color='" << color << "'>" << kv.second.Encryption << "</font>";
                                    }
                                    TABLED() {
                                        if (kv.second.LastSessionDieTime != TInstant::Zero()) {
                                            str << kv.second.LastSessionDieTime;
                                        }
                                    }
                                    TABLED() { str << kv.second.TotalOutputQueueSize; }
                                    TABLED() { str << (kv.second.Connected ? "yes" : "<strong>no</strong>"); }
                                    TABLED() { str << (kv.second.ExternalDataChannel ? "yes" : "no")
                                        << " (" << (kv.second.XDCFlags & TInterconnectProxyTCP::TProxyStats::XDCFlags::MSG_ZERO_COPY_SEND ? "MSG_ZC_SEND" : "_")  << ")"; }
                                    TABLED() { str << kv.second.Host; }
                                    TABLED() { str << kv.second.Port; }
                                    TABLED() {
                                        str << "<strong>";
                                        if (kv.second.LastErrorTimestamp != TInstant::Zero()) {
                                            str << kv.second.LastErrorTimestamp;
                                        }
                                        str << "</strong>";
                                    }
                                    TABLED() { str << "<strong>" << kv.second.LastErrorKind << "</strong>"; }
                                    TABLED() { str << "<strong>" << kv.second.LastErrorExplanation << "</strong>"; }
                                }
                            }
                        }
                    }
                }
                return str.Str();
            }

            TString GenerateJson() {
                NJson::TJsonValue json;
                for (const auto& [nodeId, info] : Stats) {
                    NJson::TJsonValue item;
                    item["NodeId"] = nodeId;

                    auto id = [](const auto& x) { return x; };
                    auto toString = [](const auto& x) { return x.ToString(); };

#define JSON(NAME, FUN) item[#NAME] = FUN(info.NAME);
                    JSON(Path, id)
                    JSON(State, id)
                    JSON(PeerScopeId, ScopeIdToString)
                    JSON(LastSessionDieTime, toString)
                    JSON(TotalOutputQueueSize, id)
                    JSON(Connected, id)
                    JSON(ExternalDataChannel, id)
                    JSON(Host, id)
                    JSON(Port, id)
                    JSON(LastErrorTimestamp, toString)
                    JSON(LastErrorKind, id)
                    JSON(LastErrorExplanation, id)
                    JSON(Ping, toString)
                    JSON(ClockSkew, id)
                    JSON(Encryption, id)
#undef JSON

                    json[ToString(nodeId)] = item;
                }
                TStringStream str(NMonitoring::HTTPOKJSON);
                NJson::WriteJson(&str, &json);
                return str.Str();
            }
        };

    private:
        TIntrusivePtr<TInterconnectProxyCommon> Common;

    public:
        static constexpr IActor::EActorActivity ActorActivityType() {
            return EActivityType::INTERCONNECT_MONACTOR;
        }

        TInterconnectMonActor(TIntrusivePtr<TInterconnectProxyCommon> common)
            : TActor(&TThis::StateFunc)
            , Common(std::move(common))
        {}

        STRICT_STFUNC(StateFunc,
            HFunc(NMon::TEvHttpInfo, Handle)
        )

        void Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
            const auto& params = ev->Get()->Request.GetParams();
            int certinfo = 0;
            if (TryFromString(params.Get("certinfo"), certinfo) && certinfo) {
                ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(GetCertInfoJson(), ev->Get()->SubRequestId,
                    NMon::TEvHttpInfoRes::Custom));
            } else {
                const bool json = params.Has("fmt") && params.Get("fmt") == "json";
                ctx.Register(new TQueryProcessor(ev->Sender, json));
            }
        }

        TString GetCertInfoJson() const {
            NJson::TJsonValue json(NJson::JSON_MAP);
            if (const TString cert = Common ? Common->Settings.Certificate : TString()) {
                struct TEx : yexception {};
                try {
                    const auto& cert = Common->Settings.Certificate;
                    std::unique_ptr<BIO, void(*)(BIO*)> bio(BIO_new_mem_buf(cert.data(), cert.size()), &BIO_vfree);
                    if (!bio) {
                        throw TEx() << "BIO_new_mem_buf failed";
                    }
                    std::unique_ptr<X509, void(*)(X509*)> x509(PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr),
                        &X509_free);
                    if (!x509) {
                        throw TEx() << "PEM_read_bio_X509 failed";
                    }
                    X509_NAME *name = X509_get_subject_name(x509.get());
                    if (!name) {
                        throw TEx() << "X509_get_subject_name failed";
                    }
                    char buffer[4096];
                    if (char *p = X509_NAME_oneline(name, buffer, sizeof(buffer))) {
                        json["Subject"] = p;
                    }
                    if (int loc = X509_NAME_get_index_by_NID(name, NID_commonName, -1); loc >= 0) {
                        if (X509_NAME_ENTRY *entry = X509_NAME_get_entry(name, loc)) {
                            if (ASN1_STRING *data = X509_NAME_ENTRY_get_data(entry)) {
                                unsigned char *cn;
                                if (const int len = ASN1_STRING_to_UTF8(&cn, data); len >= 0) {
                                    json["CommonName"] = TString(reinterpret_cast<char*>(cn), len);
                                    OPENSSL_free(cn);
                                }
                            }
                        }
                    }
                    auto time = [](const ASN1_TIME *t, const char *name) -> TString {
                        if (t) {
                            struct tm tm;
                            if (ASN1_TIME_to_tm(t, &tm)) {
                                return Strftime("%Y-%m-%dT%H:%M:%S%z", &tm);
                            } else {
                                throw TEx() << "ASN1_TIME_to_tm failed";
                            }
                        } else {
                            throw TEx() << name << " failed";
                        }
                    };
                    json["NotBefore"] = time(X509_get0_notBefore(x509.get()), "X509_get0_notBefore");
                    json["NotAfter"] = time(X509_get0_notAfter(x509.get()), "X509_get0_notAfter");
                } catch (const TEx& ex) {
                    json["Error"] = ex.what();
                }
            }
            TStringStream str(NMonitoring::HTTPOKJSON);
            NJson::WriteJson(&str, &json);
            return str.Str();
        }
    };

    IActor *CreateInterconnectMonActor(TIntrusivePtr<TInterconnectProxyCommon> common) {
        return new TInterconnectMonActor(std::move(common));
    }

} // NInterconnect
