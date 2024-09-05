#include "dsproxy_monactor.h"
#include "dsproxy_mon.h"
#include "dsproxy.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/mon/crossref.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/interconnect/interconnect.h>

#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

struct TEvThroughputUpdate : public TEventLocal<TEvThroughputUpdate, TEvBlobStorage::EvThroughputUpdate> {
};

class TMonQueryProcessor : public TActorBootstrapped<TMonQueryProcessor> {
    TIntrusivePtr<TBlobStorageGroupProxyMon> Mon;
    TIntrusivePtr<TGroupQueues> GroupQueues;
    const ui32 GroupId;
    TIntrusivePtr<TBlobStorageGroupInfo> Info;
    NMon::TEvHttpInfo::TPtr Ev;

public:
    TMonQueryProcessor(TIntrusivePtr<TBlobStorageGroupProxyMon> mon, TIntrusivePtr<TGroupQueues> groupQueues,
            ui32 groupId, TIntrusivePtr<TBlobStorageGroupInfo> info, NMon::TEvHttpInfo::TPtr ev)
        : Mon(std::move(mon))
        , GroupQueues(std::move(groupQueues))
        , GroupId(groupId)
        , Info(std::move(info))
        , Ev(ev)
    {}

    void Bootstrap() {
        std::vector<TString> urls;
        if (Info) {
            for (ui32 i = 0; i < Info->GetTotalVDisksNum(); ++i) {
                const TActorId& serviceId = Info->GetActorId(i);
                ui32 nodeId, pdiskId, vslotId;
                std::tie(nodeId, pdiskId, vslotId) = DecomposeVDiskServiceId(serviceId);
                urls.push_back(Sprintf("node/%u/actors/vdisks/vdisk%09" PRIu32 "_%09" PRIu32, nodeId, pdiskId, vslotId));
            }
        }
        const TCgiParameters& cgi = Ev->Get()->Request.GetParams();
        if (cgi.Has("submit_timestats")) {
            Mon->TimeStats.Submit(cgi);
        }
#define UPDATE_WILSON(NAME)                                       \
        if (cgi.Has(#NAME "SamplingRate")) {                      \
            const TString& item = cgi.Get(#NAME "SamplingRate");   \
            ui64 value;                                           \
            if (TryFromString(item, value) && value <= 1000000) { \
                AtomicSet(Mon->NAME ## SamplePPM, value);         \
            }                                                     \
        }
        UPDATE_WILSON(Get)
        UPDATE_WILSON(Put)
        UPDATE_WILSON(Discover)

#define COMPONENT(NAME)                                                          \
        TABLER() {                                                                 \
            TABLED() {                                                             \
                str << #NAME;                                                    \
            }                                                                    \
            TABLED() {                                                             \
                TString value = ToString(Mon->NAME ## SamplePPM);                 \
                str << "<input name=\"" #NAME "SamplingRate\" type=\"number\""   \
                    " value=\"" << value << "\" min=\"0\" max=\"1000000\"/>ppm"; \
            }                                                                    \
        }

        TStringStream str;

        HTML(str) {
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Group content";
                }
                DIV_CLASS("panel-body") {
                    if (Info) {
                        const auto& top = Info->GetTopology();
                        DIV() {
                            str << "GroupID: " << Info->GroupID << "<br/>" << "Generation: " << Info->GroupGeneration;
                        }
                        DIV() {
                            str << "CostModel: ";
                            auto costModel = GroupQueues->CostModel; // acquire owning pointer
                            if (costModel) {
                                str << costModel->ToString();
                            } else {
                                str << "None";
                            }
                        }
                        DIV() TABLE_CLASS("table table-bordered table-condensed") TABLEBODY() {
                            ui32 maxFailDomain = 0;
                            for (const auto& realm : top.FailRealms) {
                                maxFailDomain = Max<ui32>(maxFailDomain, realm.FailDomains.size());
                            }
                            for (ui32 i = 0; i < maxFailDomain; ++i) {
                                TABLER() {
                                    for (const auto& realm : top.FailRealms) {
                                        TABLED() {
                                            if (i < realm.FailDomains.size()) {
                                                const auto& domain = realm.FailDomains[i];
                                                bool first = true;
                                                for (const auto& vdisk : domain.VDisks) {
                                                    if (first) {
                                                        first = false;
                                                    } else {
                                                        str << "<br/>";
                                                    }

                                                    bool ok = true;
                                                    GroupQueues->DisksByOrderNumber[vdisk.OrderNumber]->Queues.ForEachQueue([&](auto& q) {
                                                        if (!q.IsConnected) {
                                                            ok = false;
                                                        }
                                                    });

                                                    const TVDiskID vdiskId = Info->GetVDiskId(vdisk.VDiskIdShort);
                                                    if (const auto& url = urls[vdisk.OrderNumber]) {
                                                        str << "<a ";
                                                        if (!ok) {
                                                            str << "style='color:red' ";
                                                        }
                                                        str << "href=\"" << url << "\">" << vdiskId << "</a>";
                                                    } else {
                                                        str << vdiskId;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        DIV() {
                            TString mode = "<strong>incorrect</strong>";
                            switch (Info->GetEncryptionMode()) {
                                case TBlobStorageGroupInfo::EEM_NONE:
                                    mode = "no encryption";
                                    break;
                                case TBlobStorageGroupInfo::EEM_ENC_V1:
                                    mode = "v1 (at proxy level, no MAC, no CRC)";
                                    break;
                            }
                            str << "EncryptionMode: " << mode;

                            if (Info->GetEncryptionMode() != TBlobStorageGroupInfo::EEM_NONE) {
                                str << "<br/>LifeCyclePhase: " << Info->GetLifeCyclePhase();
                            }
                        }
                    }
                }
            }

            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Queue status";
                }
                DIV_CLASS("panel-body") {
                    DIV() {
                        TABLE_CLASS("table table-condensed") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() { str << "VDisk"; }
                                    TABLEH() { str << "NodeId"; }
                                    TABLEH() { str << "Queue Status"; }
                                }
                            }
                            TABLEBODY() {
                                const auto& fdoms = GroupQueues->FailDomains;
                                for (size_t failDomain = 0, num = 0; failDomain < fdoms.size(); ++failDomain) {
                                    const auto& fdom = fdoms[failDomain];
                                    for (size_t vdisk = 0; vdisk < fdom.VDisks.size(); ++vdisk, ++num) {
                                        TABLER() {
                                            TABLED() { str << Info->GetVDiskId(num); }
                                            TABLED() { str << Info->GetActorId(num).NodeId(); }
                                            TABLED() {
                                                fdom.VDisks[vdisk].Queues.ForEachQueue([&](auto& q) {
                                                    str << (q.IsConnected ? '+' : '0');
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Blob query service";
                }
                DIV_CLASS("panel-body") {
                    FORM_CLASS("form-horizontal") {
                        DIV_CLASS("control-group") {
                            LABEL_CLASS_FOR("control-label", "inputBlob") { str << "Blob ID"; }
                            DIV_CLASS("controls") {
                                str << "<input id=\"inputBlob\" name=\"blob\" type=\"text\"/>";
                            }
                        }
                        str << "<input type=\"hidden\" name=\"groupId\" value=\"" << GroupId << "\">";
                        str << "<input type=\"hidden\" name=\"debugInfo\" value=\"" << 1 << "\">";
                        DIV_CLASS("control-group") {
                            DIV_CLASS("controls") {
                                str << "<button type=\"submit\" formaction=\"/get_blob\" class=\"btn btn-default\">Query</button>";
                            }
                        }
                    }
                }
            }

            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Blob range index query service";
                }
                DIV_CLASS("panel-body") {
                    FORM_CLASS("form-horizontal") {
                        DIV_CLASS("control-group") {
                            LABEL_CLASS_FOR("control-label", "tabletId") { str << "Tablet ID"; }
                            DIV_CLASS("controls") {
                                str << "<input id=\"tabletId\" name=\"tabletId\" type=\"integer\"/>";
                            }
                        }
                        DIV_CLASS("control-group") {
                            LABEL_CLASS_FOR("control-label", "from") { str << "From ID"; }
                            DIV_CLASS("controls") {
                                str << "<input id=\"from\" name=\"from\" type=\"text\"/>";
                            }
                        }
                        DIV_CLASS("control-group") {
                            LABEL_CLASS_FOR("control-label", "to") { str << "To ID"; }
                            DIV_CLASS("controls") {
                                str << "<input id=\"to\" name=\"to\" type=\"text\"/>";
                            }
                        }
                        str << "<input type=\"hidden\" name=\"groupId\" value=\"" << GroupId << "\">";
                        DIV_CLASS("control-group") {
                            DIV_CLASS("controls") {
                                str << "<button type=\"submit\" formaction=\"/blob_range\" class=\"btn btn-default\">Query</button>";
                            }
                        }
                    }
                }
            }

            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Wilson tuning";
                }
                DIV_CLASS("panel-body") {
                    FORM_CLASS("form-vertical") {
                        DIV() {
                            str << "All sampling rates are provided as integer in range [0, 1000000] measured in ppm "
                                " where 0 means disabled sampling for this kind of request";
                        }
                        DIV() {
                            str << "<b>NOTE</b> do not forget to enable WILSON logger at DEBUG level with 100% sampling"
                                " at all nodes of the cluster";
                        }
                        TABLE_CLASS ("table table-condensed") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() {
                                        str << "Query type";
                                    }
                                    TABLEH() {
                                        str << "Sampling rate";
                                    }
                                }
                            }
                            TABLEBODY() {
                                COMPONENT(Put)
                                COMPONENT(Get)
                                COMPONENT(Discover)
                            }
                        }
                        DIV_CLASS("control-group") {
                            DIV_CLASS("controls") {
                                str << "<button type=\"submit\" class=\"btn btn-default\">Commit</button>";
                            }
                        }
                    }
                }
            }

            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Queue latencies, ms";
                }
                DIV_CLASS("panel-body") {
                    if (!GroupQueues) {
                        str << "No data available, GroupQueues is nullptr.";
                    } else {
                        TABLE_CLASS ("table table-condensed") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() { str << "VDisk"; }
                                    TABLEH() { str << "PutTabletLog"; }
                                    TABLEH() { str << "PutAsyncBlob"; }
                                    TABLEH() { str << "PutUserData"; }
                                    TABLEH() { str << "GetAsyncRead"; }
                                    TABLEH() { str << "GetFastRead"; }
                                    TABLEH() { str << "GetDiscover"; }
                                    TABLEH() { str << "GetLowRead"; }
                                }
                            }
                            TABLEBODY() {
                                for (size_t fdIdx = 0, num = 0; fdIdx < GroupQueues->FailDomains.size(); ++fdIdx) {
                                    const TGroupQueues::TFailDomain &failDomain = GroupQueues->FailDomains[fdIdx];
                                    for (size_t vdIdx = 0; vdIdx < failDomain.VDisks.size(); ++vdIdx, ++num) {
                                        const TGroupQueues::TVDisk &vDisk = failDomain.VDisks[vdIdx];
                                        const TGroupQueues::TVDisk::TQueues &q = vDisk.Queues;
                                        TABLER() {
                                            TABLED() { str << Info->GetVDiskId(num); }
#define LATENCY_DATA(NAME) \
            TABLED() { \
                if (q.NAME.FlowRecord) { \
                    str << (q.NAME.FlowRecord->GetPredictedDelayNs() * 0.000001f); \
                } else { \
                    str << "No FlowRecord"; \
                } \
            }
                                            LATENCY_DATA(PutTabletLog);
                                            LATENCY_DATA(PutAsyncBlob);
                                            LATENCY_DATA(PutUserData);
                                            LATENCY_DATA(GetAsyncRead);
                                            LATENCY_DATA(GetFastRead);
                                            LATENCY_DATA(GetDiscover);
                                            LATENCY_DATA(GetLowRead);
#undef LATENCY_DATA
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Mon->TimeStats.Render(str);

        Send(Ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
        PassAway();
    }
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TBlobStorageProxyMonActor
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TBlobStorageGroupProxyMonActor : public TActorBootstrapped<TBlobStorageGroupProxyMonActor> {
    enum {
        EvNodeWhiteboardNotify = EventSpaceBegin(TKikimrEvents::ES_PRIVATE)
    };

    struct TEvNodeWhiteboardNotify : public TEventLocal<TEvNodeWhiteboardNotify, EvNodeWhiteboardNotify>
    {};

    TIntrusivePtr<TBlobStorageGroupProxyMon> Mon;
    const ui32 GroupId;
    TIntrusivePtr<TBlobStorageGroupInfo> Info;
    const TActorId ProxyId;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_GROUP_PROXY_MON;
    }

    TBlobStorageGroupProxyMonActor(TIntrusivePtr<TBlobStorageGroupProxyMon> mon, ui32 groupId,
            TIntrusivePtr<TBlobStorageGroupInfo> info, TActorId proxyId)
        : Mon(std::move(mon))
        , GroupId(groupId)
        , Info(std::move(info))
        , ProxyId(proxyId)
    {}

    ~TBlobStorageGroupProxyMonActor() {
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Actor handlers
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bootstrap state
    void Bootstrap() {
        NActors::TMon* mon = AppData()->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            NMonitoring::TIndexMonPage *proxiesMonPage = actorsMonPage->RegisterIndexPage(
                    "blobstorageproxies", "BlobStorageProxies");

            TString path = Sprintf("blobstorageproxy%09" PRIu32, (ui32)GroupId);
            TString name = Sprintf("BlobStorageProxy%09" PRIu32, (ui32)GroupId);
            mon->RegisterActorPage(TMon::TRegisterActorPageFields{
                .Title = name,
                .RelPath = path,
                .ActorSystem = TlsActivationContext->ExecutorThread.ActorSystem,
                .Index = proxiesMonPage, 
                .PreTag = false, 
                .ActorId = SelfId(),
                .MonServiceName = "dsproxy_mon"
            });
        }

        Become(&TThis::StateOnline);
        Schedule(TDuration::Seconds(5), new TEvents::TEvWakeup());
        Send(SelfId(), new TEvThroughputUpdate);
        Send(SelfId(), new TEvNodeWhiteboardNotify);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // All states
    void HandleWakeup() {
        Schedule(TDuration::Seconds(5), new TEvents::TEvWakeup());
        Mon->Update();
    }

    void HandleThroughputUpdate() {
        Mon->ThroughputUpdate();
        Schedule(TDuration::Seconds(15), new TEvThroughputUpdate);
    }

    void HandleNodeWhiteboardNotify() {
        ui32 groupId, groupGen;
        if (Mon->GetGroupIdGen(&groupId, &groupGen)) {
            auto event = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateUpdate>();
            Mon->SerializeToWhiteboard(event->Record, groupId);
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), event.release());
        }
        Schedule(TDuration::Seconds(15), new TEvNodeWhiteboardNotify);
    }

    void Handle(TEvThroughputAddRequest::TPtr& ev) {
        TEvThroughputAddRequest *msg = ev->Get();
        Mon->CountThroughput(msg->PutHandleClass, msg->Bytes);
    }

    std::deque<NMon::TEvHttpInfo::TPtr> MonQueue;

    void Handle(NMon::TEvHttpInfo::TPtr &ev) {
        if (MonQueue.empty()) {
            Send(ProxyId, new TEvRequestProxySessionsState);
        }
        MonQueue.push_back(ev);
    }

    void Handle(TEvProxySessionsState::TPtr ev) {
        for (auto& monEv : std::exchange(MonQueue, {})) {
            Register(new TMonQueryProcessor(Mon, ev->Get()->GroupQueues, GroupId, Info, monEv));
        }
    }

    void Handle(TEvBlobStorage::TEvConfigureProxy::TPtr ev) {
        Info = std::move(ev->Get()->Info);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Actor state functions
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    STATEFN(StateOnline) {
        switch (ev->GetTypeRewrite()) {
            cFunc(NActors::TEvents::TSystem::Poison, PassAway);
            hFunc(NMon::TEvHttpInfo, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            cFunc(TEvBlobStorage::EvThroughputUpdate, HandleThroughputUpdate);
            cFunc(EvNodeWhiteboardNotify, HandleNodeWhiteboardNotify);
            hFunc(TEvThroughputAddRequest, Handle);
            hFunc(TEvProxySessionsState, Handle);
            hFunc(TEvBlobStorage::TEvConfigureProxy, Handle);
        default:
            break;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Blob Storage Group Proxy Mon Creation
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
IActor* CreateBlobStorageGroupProxyMon(TIntrusivePtr<TBlobStorageGroupProxyMon> mon, ui32 groupId,
        TIntrusivePtr<TBlobStorageGroupInfo> info, TActorId proxyId) {
    return new TBlobStorageGroupProxyMonActor(std::move(mon), groupId, std::move(info), proxyId);
}

} // NKikimr

