#include "tablet_impl.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/string/escape.h>
#include <util/string/subst.h>

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::THtmlRenderer {
    using TSelf = TKesusTablet;

    TSelf* Self;

    THtmlRenderer(TSelf* self)
        : Self(self)
    {}

    void RenderCount(IOutputStream& out, ui64 count) {
        if (count != Max<ui64>()) {
            out << count;
        } else {
            out << "max";
        }
    }

    void RenderError(IOutputStream& out, const TString& message) {
        HTML(out) {
            TAG(TH3) { out << "ERROR: " << message; }
        }
    }

    void RenderProxyLink(IOutputStream& out, const TActorId& actorId) {
        TCgiParameters params;
        params.InsertEscaped("proxy", TStringBuilder() << actorId);
        out << "<a href=\"app?TabletID=" << Self->TabletID() << "&" << params() << "\">" << actorId << "</a>";
    }

    void RenderProxyList(IOutputStream& out) {
        HTML(out) {
            TAG(TH3) { out << "Proxies"; }

            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "Proxy"; }
                        TABLEH() { out << "Generation"; }
                        TABLEH() { out << "Sessions"; }
                    }
                }
                TABLEBODY() {
                    for (const auto& kv : Self->Proxies) {
                        const auto* proxy = &kv.second;
                        TABLER() {
                            TABLED() { RenderProxyLink(out, proxy->ActorID); }
                            TABLED() { out << proxy->Generation; }
                            TABLED() { out << proxy->AttachedSessions.size(); }
                        }
                    }
                }
            }
        }
    }

    void RenderProxyDetails(IOutputStream& out, const TActorId& actorId) {
        const auto* proxy = Self->Proxies.FindPtr(actorId);
        if (!proxy) {
            RenderError(out, "Proxy not found");
            return;
        }

        HTML(out) {
            TAG(TH2) { out << "Kesus proxy " << actorId; }

            PRE() {
                out << "Generation: " << proxy->Generation << "\n";
            }

            TAG(TH3) { out << "Attached sessions"; }

            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "ID"; }
                    }
                }
                TABLEBODY() {
                    for (const auto& sessionId : proxy->AttachedSessions) {
                        TABLER() {
                            TABLED() { RenderSessionLink(out, sessionId); }
                        }
                    }
                }
            }
        }
    }

    void RenderProxyDetails(IOutputStream& out, const TString& actorIdText) {
        TActorId actorId;
        if (!actorId.Parse(actorIdText.data(), actorIdText.size())) {
            RenderError(out, "Invalid proxy id");
            return;
        }
        RenderProxyDetails(out, actorId);
    }

    void RenderSessionLink(IOutputStream& out, ui64 sessionId) {
        TCgiParameters params;
        params.InsertEscaped("session", TStringBuilder() << sessionId);
        out << "<a href=\"app?TabletID=" << Self->TabletID() << "&" << params() << "\">" << sessionId << "</a>";
    }

    void RenderSessionList(IOutputStream& out) {
        HTML(out) {
            TAG(TH3) { out << "Sessions"; }

            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "ID"; }
                        TABLEH() { out << "Description"; }
                        TABLEH() { out << "Semaphores (owned/waiting)"; }
                        TABLEH() { out << "Owner proxy"; }
                    }
                }
                TABLEBODY() {
                    for (const auto& kv : Self->Sessions) {
                        const auto* session = &kv.second;
                        TABLER() {
                            TABLED() { RenderSessionLink(out, session->Id); }
                            TABLED() { out << session->Description.Quote(); }
                            TABLED() { out << session->OwnedSemaphores.size() << "/" << session->WaitingSemaphores.size(); }
                            TABLED() {
                                if (session->OwnerProxy) {
                                    RenderProxyLink(out, session->OwnerProxy->ActorID);
                                } else {
                                    TDuration timeLeft = session->ScheduledTimeoutDeadline - TActivationContext::Now();
                                    out << "Timeout in " << timeLeft.MilliSeconds() << "ms";
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    void RenderSessionDetails(IOutputStream& out, ui64 sessionId) {
        const auto* session = Self->Sessions.FindPtr(sessionId);
        if (!session) {
            RenderError(out, "Session not found");
            return;
        }

        HTML(out) {
            TAG(TH2) { out << "Kesus session " << sessionId; }

            PRE() {
                out << "Description: " << session->Description.Quote() << "\n";
                if (session->OwnerProxy) {
                    out << "Owner proxy: ";
                    RenderProxyLink(out, session->OwnerProxy->ActorID);
                    out << "\n";
                    out << "Semaphore requests: " << session->SemaphoreWaitCookie.size() << "\n";
                } else {
                    TDuration timeLeft = session->ScheduledTimeoutDeadline - TActivationContext::Now();
                    out << "Timeout in: " << timeLeft.MilliSeconds() << "ms\n";
                }
                out << "Last attach seqno: " << session->LastOwnerSeqNo << "\n";
            }

            TAG(TH3) { out << "Owned semaphores"; }

            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "Name"; }
                        TABLEH() { out << "Count"; }
                        TABLEH() { out << "Data"; }
                    }
                }
                TABLEBODY() {
                    for (const auto& kv : session->OwnedSemaphores) {
                        const auto* owner = &kv.second;
                        const auto* semaphore = Self->Semaphores.FindPtr(kv.first);
                        Y_ABORT_UNLESS(semaphore);
                        TABLER() {
                            TABLED() { RenderSemaphoreLink(out, semaphore->Name); }
                            TABLED() { RenderCount(out, owner->Count); }
                            TABLED() { out << owner->Data.Quote(); }
                        }
                    }
                }
            }

            TAG(TH3) { out << "Waiting semaphores"; }

            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "Name"; }
                        TABLEH() { out << "Count"; }
                        TABLEH() { out << "Data"; }
                        TABLEH() { out << "Timeout"; }
                    }
                }
                TABLEBODY() {
                    for (const auto& kv : session->WaitingSemaphores) {
                        const auto* waiter = &kv.second;
                        const auto* semaphore = Self->Semaphores.FindPtr(kv.first);
                        Y_ABORT_UNLESS(semaphore);
                        TABLER() {
                            TABLED() { RenderSemaphoreLink(out, semaphore->Name); }
                            TABLED() { RenderCount(out, waiter->Count); }
                            TABLED() { out << waiter->Data.Quote(); }
                            TABLED() {
                                TDuration timeLeft = waiter->ScheduledTimeoutDeadline - TActivationContext::Now();
                                out << timeLeft.MilliSeconds() << "ms";
                            }
                        }
                    }
                }
            }
        }
    }

    void RenderSessionDetails(IOutputStream& out, const TString& sessionIdText) {
        ui64 sessionId;
        if (!TryFromString(sessionIdText, sessionId)) {
            RenderError(out, "Invalid session id");
            return;
        }
        RenderSessionDetails(out, sessionId);
    }

    void RenderSemaphoreLink(IOutputStream& out, const TString& name) {
        TCgiParameters params;
        params.InsertEscaped("semaphore", name);
        out << "<a href=\"app?TabletID=" << Self->TabletID() << "&" << params() << "\">" << EscapeC(name) << "</a>";
    }

    void RenderSemaphoreList(IOutputStream& out) {
        HTML(out) {
            TAG(TH3) { out << "Semaphores"; }

            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "Name"; }
                        TABLEH() { out << "Data"; }
                        TABLEH() { out << "Count/Limit"; }
                        TABLEH() { out << "Owners/Waiters"; }
                    }
                }
                TABLEBODY() {
                    for (const auto& kv : Self->Semaphores) {
                        const auto* semaphore = &kv.second;
                        TABLER() {
                            TABLED() { RenderSemaphoreLink(out, semaphore->Name); }
                            TABLED() { out << semaphore->Data.Quote(); }
                            TABLED() {
                                RenderCount(out, semaphore->Count);
                                out << '/';
                                RenderCount(out, semaphore->Limit);
                            }
                            TABLED() { out << semaphore->Owners.size() << '/' << semaphore->Waiters.size(); }
                        }
                    }
                }
            }
        }
    }

    void RenderSemaphoreDetails(IOutputStream& out, const TString& name) {
        const auto* semaphore = Self->SemaphoresByName.Value(name, nullptr);
        if (!semaphore) {
            RenderError(out, "Semaphore not found");
            return;
        }

        HTML(out) {
            TAG(TH2) { out << "Kesus semaphore " << EscapeC(name); }

            PRE() {
                out << "Data: " << semaphore->Data.Quote() << "\n";
                out << "Count: "; RenderCount(out, semaphore->Count); out << "\n";
                out << "Limit: "; RenderCount(out, semaphore->Limit); out << "\n";
                out << "Ephemeral: " << (semaphore->Ephemeral ? "true" : "false") << "\n";
            }

            TAG(TH3) { out << "Owner sessions"; }

            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "Session"; }
                        TABLEH() { out << "Count"; }
                        TABLEH() { out << "Data"; }
                    }
                }
                TABLEBODY() {
                    for (const auto* owner : semaphore->Owners) {
                        TABLER() {
                            TABLED() { RenderSessionLink(out, owner->SessionId); }
                            TABLED() { RenderCount(out, owner->Count); }
                            TABLED() { out << owner->Data.Quote(); }
                        }
                    }
                }
            }

            TAG(TH3) { out << "Waiting sessions"; }

            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "Order"; }
                        TABLEH() { out << "Session"; }
                        TABLEH() { out << "Count"; }
                        TABLEH() { out << "Data"; }
                        TABLEH() { out << "Timeout"; }
                    }
                }
                TABLEBODY() {
                    for (const auto& kv : semaphore->Waiters) {
                        const auto* waiter = kv.second;
                        TABLER() {
                            TABLED() { out << '#' << kv.first; }
                            TABLED() { RenderSessionLink(out, waiter->SessionId); }
                            TABLED() { RenderCount(out, waiter->Count); }
                            TABLED() { out << waiter->Data.Quote(); }
                            TABLED() {
                                TDuration timeLeft = waiter->ScheduledTimeoutDeadline - TActivationContext::Now();
                                out << timeLeft.MilliSeconds() << "ms";
                            }
                        }
                    }
                }
            }
        }
    }

    void RenderQuoterResourceLink(IOutputStream& out, const TQuoterResourceTree* resource) {
        if (resource) {
            TCgiParameters params;
            params.InsertUnescaped("quoter_resource", resource->GetPath());
            out << "<a href=\"app?TabletID=" << Self->TabletID() << "&" << params() << "\">" << resource->GetPath() << "</a>";
        }
    }

    void RenderQuoterResourceTable(IOutputStream& out, std::vector<const TQuoterResourceTree*>& resources) {
        std::sort(resources.begin(), resources.end(),
                  [](const TQuoterResourceTree* res1, const TQuoterResourceTree* res2) {
                      return res1->GetPath() < res2->GetPath();
                  });
        HTML(out) {
            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "Path"; }
                        TABLEH() { out << "Props"; }
                    }
                }
                TABLEBODY() {
                    for (const TQuoterResourceTree* resource : resources) {
                        TABLER() {
                            TABLED() { RenderQuoterResourceLink(out, resource); }
                            TABLED() { out << resource->GetProps(); }
                        }
                    }
                }
            }
        }
    }

    void RenderQuoterResources(IOutputStream& out) {
        HTML(out) {
            TAG(TH3) { out << "Quoter resources"; }

            std::vector<const TQuoterResourceTree*> resources;
            resources.reserve(Self->QuoterResources.GetAllResources().size());
            for (auto&& [path, resource] : Self->QuoterResources.GetAllResources()) {
                resources.push_back(resource);
            }

            RenderQuoterResourceTable(out, resources);
        }
    }

    void RenderQuoterResourceDetails(IOutputStream& out, const TString& path) {
        if (const TQuoterResourceTree* resource = Self->QuoterResources.FindPath(path)) {
            HTML(out) {
                TAG(TH2) { out << "Kesus quoter resource " << EscapeC(path); }

                PRE() {
                    if (resource->GetParent()) {
                        out << "Parent: "; RenderQuoterResourceLink(out, resource->GetParent()); out << "\n";
                    }
                    TString props = "\n";
                    props += resource->GetProps().Utf8DebugString();
                    SubstGlobal(props, "\n", "\n    "); // make indent
                    out << "Props:" << props << "\n";
                    resource->HtmlDebugInfo(out);
                }

                TAG(TH3) { out << "Children resources"; }

                std::vector<const TQuoterResourceTree*> resources(resource->GetChildren().begin(), resource->GetChildren().end());
                RenderQuoterResourceTable(out, resources);

                TAG(TH3) { out << "Sessions"; }
                RenderQuoterResourceSessions(out, resource);
            }
        } else {
            RenderError(out, TStringBuilder() << "Resource with path " << path << " not found");
        }
    }

    void RenderQuoterResourceSessions(IOutputStream& out, const TQuoterResourceTree* resource) {
        HTML(out) {
            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "Client"; }
                        TABLEH() { out << "Version"; }
                        TABLEH() { out << "Active"; }
                        TABLEH() { out << "Sent"; }
                        TABLEH() { out << "ConsumedSinceReplicationEnabled"; }
                        TABLEH() { out << "Requested"; }
                    }
                }
                TABLEBODY() {
                    const auto& clients = resource->GetSessions();
                    for (const auto& [clientId, _] : clients) {
                        const TQuoterSession* session = Self->QuoterResources.FindSession(clientId, resource->GetResourceId());
                        Y_ABORT_UNLESS(session);
                        TABLER() {
                            TABLED() { out << clientId; }
                            TABLED() { out << session->GetClientVersion(); }
                            TABLED() { out << (session->IsActive() ? "true" : "false"); }
                            TABLED() { out << session->GetTotalSent(); }
                            TABLED() { out << session->GetTotalConsumed(); }
                            TABLED() { out << session->GetAmountRequested(); }
                        }
                    }
                }
            }
        }
    }
};

bool TKesusTablet::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx) {
    if (!ev) {
        return true;
    }

    const auto* msg = ev->Get();
    TCgiParameters params(msg->Cgi());

    TStringStream out;
    THtmlRenderer renderer(this);
    HTML(out) {
        if (params.Has("proxy")) {
            renderer.RenderProxyDetails(out, params.Get("proxy"));
        } else if (params.Has("session")) {
            renderer.RenderSessionDetails(out, params.Get("session"));
        } else if (params.Has("semaphore")) {
            renderer.RenderSemaphoreDetails(out, params.Get("semaphore"));
        } else if (params.Has("quoter_resource")) {
            renderer.RenderQuoterResourceDetails(out, params.Get("quoter_resource"));
        } else {
            TAG(TH2) { out << "Kesus " << EscapeC(KesusPath); }
            renderer.RenderProxyList(out);
            renderer.RenderSessionList(out);
            renderer.RenderSemaphoreList(out);
            renderer.RenderQuoterResources(out);
        }
    }

    ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(out.Str()));
    return true;
}

}
}
