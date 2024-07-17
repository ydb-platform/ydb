#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/digest/md5/md5.h>
#include <util/string/vector.h>
#include <ydb/core/tablet_flat/flat_executor_counters.h>
#include <ydb/core/protos/counters_keyvalue.pb.h>
#include "hive_impl.h"
#include "hive_schema.h"
#include "hive_log.h"
#include "monitoring.h"

namespace NKikimr {
namespace NHive {

TLoggedMonTransaction::TLoggedMonTransaction(const NMon::TEvRemoteHttpInfo::TPtr& ev, THive* self) {
    Index = ++self->OperationsLogIndex;

    const auto& query = ev->Get()->ExtendedQuery;
    if (query) {
        NACLib::TUserToken token(query->GetUserToken());
        User = token.GetUserSID();
    }
}

void TLoggedMonTransaction::WriteOperation(NIceDb::TNiceDb& db, const NJson::TJsonValue& op) {
    TStringStream str;
    NJson::WriteJson(&str, &op);
    TInstant timestamp = TActivationContext::Now();
    db.Table<Schema::OperationsLog>().Key(Index).Update<Schema::OperationsLog::User, Schema::OperationsLog::OperationTimestamp, Schema::OperationsLog::Operation>(User, timestamp.MilliSeconds(), str.Str());
}

class TTxMonEvent_DbState : public TTransactionBase<THive> {
public:
    struct TTabletInfo {
        ui32 KnownGeneration;
        ui32 TabletType;
        ui32 LeaderNode;
        ETabletState TabletState;
    };

    struct TNodeInfo {
        TActorId Local;
        ui64 TabletsOn;

        TNodeInfo()
            : TabletsOn(0)
        {}
    };

    const TActorId Source;

    TMap<ui64, TTabletInfo> TabletInfo;
    TMap<ui32, TNodeInfo> NodeInfo;

    TTxMonEvent_DbState(const TActorId &source, TSelf *hive)
        : TBase(hive)
        , Source(source)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_DB_STATE; }

    bool Execute(TTransactionContext &txc, const TActorContext& ctx) override {
        TabletInfo.clear();
        NodeInfo.clear();
        NIceDb::TNiceDb db(txc.DB);

        { // read tablets from DB
            auto rowset = db.Table<Schema::Tablet>().Range().Select();
            if (!rowset.IsReady())
                return false;
            while (rowset.IsValid()) {
                const ui64 tabletId = rowset.GetValue<Schema::Tablet::ID>();
                const ui32 knownGen = rowset.GetValue<Schema::Tablet::KnownGeneration>();
                const ui32 type = rowset.GetValue<Schema::Tablet::TabletType>();
                const ui32 leaderNode = rowset.GetValue<Schema::Tablet::LeaderNode>();
                const ETabletState tabletState = rowset.GetValue<Schema::Tablet::State>();

                TabletInfo[tabletId] = {knownGen, type, leaderNode, tabletState};
                ++NodeInfo[leaderNode].TabletsOn; // leaderNode could be zero, then - counter of tablets w/o leader node
                if (!rowset.Next())
                    return false;
            }
        }

        // read nodes
        {
            auto rowset = db.Table<Schema::Node>().Range().Select();
            if (!rowset.IsReady())
                return false;
            while (rowset.IsValid()) {
                const ui32 nodeId = rowset.GetValue<Schema::Node::ID>();
                const TActorId local = rowset.GetValue<Schema::Node::Local>();

                NodeInfo[nodeId].Local = local;
                if (!rowset.Next())
                    return false;
            }
        }

        // todo: send result back
        TStringStream str;
        RenderHTMLPage(str);
        ctx.Send(Source, new NMon::TEvRemoteHttpInfoRes(str.Str()));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Y_UNUSED(ctx);
    }

    void RenderHTMLPage(IOutputStream &out) {
        HTML(out) {
             UL_CLASS("nav nav-tabs") {
                 LI_CLASS("active") {
                     out << "<a href=\"#known-tablets\" data-toggle=\"tab\">Tablets</a>";
                 }
                 LI() {
                     out << "<a href=\"#per-node-stats\" data-toggle=\"tab\">Nodes</a>";
                 }
             }
             DIV_CLASS("tab-content") {
                 DIV_CLASS_ID("tab-pane fade in active", "known-tablets") {
                     TABLE_SORTABLE_CLASS("table") {
                         TABLEHEAD() {
                              TABLER() {
                                  TABLEH() {out << "Tablet";}
                                  TABLEH() {out << "ID";}
                                  TABLEH() {out << "KnownGeneration";}
                                  TABLEH() {out << "LeaderNode";}
                                  TABLEH() {out << "State";}
                                  TABLEH_CLASS("sorter-false") {}
                              }
                          }
                          TABLEBODY() {
                             ui64 index = 0;
                             for (const auto &tabletPair : TabletInfo) {
                                 const ui64 tabletId = tabletPair.first;
                                 const auto &x = tabletPair.second;
                                 TABLER() {
                                      out << "<td data-text='" << index << "'>" << "<a href=\"../tablets?TabletID="
                                                  << tabletId << "\">"
                                                  << TTabletTypes::TypeToStr((TTabletTypes::EType)x.TabletType)
                                                  << "</a></td>";
                                      TABLED() {out << tabletId;}
                                      TABLED() {out << x.KnownGeneration;}
                                      TABLED_CLASS(x.LeaderNode ? "" : "warning")
                                             {out << x.LeaderNode;}
                                      TABLED() {out << ETabletStateName(x.TabletState);}
                                      TABLED() {out << " <a href=\"../tablets?SsId="
                                                  << tabletId << "\">"
                                                  << "<span class=\"glyphicon glyphicon-tasks\""
                                                  << " title=\"State Storage\"/>"
                                                  << "</a>";}
                                 }
                                 ++index;
                             }
                         }
                     }
                 }
                 DIV_CLASS_ID("tab-pane fade", "per-node-stats") {
                     TABLE_SORTABLE_CLASS("table") {
                          TABLEHEAD() {
                              TABLER() {
                                  TABLEH() {out << "NodeId";}
                                  TABLEH() {out << "Count";}
                              }
                          }
                          TABLEBODY() {
                              for (const auto &nodePair : NodeInfo) {
                                  const ui32 nodeId = nodePair.first;
                                  const auto &x = nodePair.second;
                                  if (nodeId != 0 || x.Local) {
                                      TABLER() {
                                          if (nodeId == 0) {
                                             TABLED_CLASS("danger") {out << "w/o leader node"; }
                                          } else {
                                             TABLED() {out << nodeId; }
                                          }
                                          if (x.Local) {
                                             TABLED() {out << x.TabletsOn;}
                                          } else {
                                             TABLED_CLASS("danger") {out << "down";}
                                          }
                                      }
                                  }
                              }
                         }
                     }
                 }
             }
        }
    }
};

class TTxMonEvent_MemStateTablets : public TTransactionBase<THive> {
public:
    const TActorId Source;
    THolder<NMon::TEvRemoteHttpInfo> Event;
    bool BadOnly = false;
    bool WaitingOnly = false;
    ui64 MaxCount = 0;
    TString Sort;

    TTxMonEvent_MemStateTablets(const TActorId &source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf *hive)
        : TBase(hive)
        , Source(source)
        , Event(ev->Release())
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_MEM_STATE; }

    bool Execute(TTransactionContext &txc, const TActorContext& ctx) override {
        const auto& params(Event->Cgi());
        if (params.contains("bad")) {
            BadOnly = FromStringWithDefault(params.Get("bad"), BadOnly);
        }
        if (params.contains("wait")) {
            WaitingOnly = FromStringWithDefault(params.Get("wait"), WaitingOnly);
        }
        if (params.contains("max")) {
            MaxCount = FromStringWithDefault(params.Get("max"), MaxCount);
        }
        if (params.contains("sort")) {
            Sort = params.Get("sort");
        }
        Y_UNUSED(txc);
        TStringStream str;
        RenderHTMLPage(str);
        ctx.Send(Source, new NMon::TEvRemoteHttpInfoRes(str.Str()));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Y_UNUSED(ctx);
    }

    void RenderHTMLPage(IOutputStream &out) {
        TVector<std::pair<double, TTabletInfo*>> tabletIdIndex;
        std::function<double(const TTabletInfo&)> tabletIndexFunction;
        if (Sort == "weight") {
            tabletIndexFunction = [](const TTabletInfo& tablet) -> double { return -tablet.Weight; };
        } else {
            tabletIndexFunction = [](const TTabletInfo& tablet) -> double { return tablet.GetFullTabletId().first * 100 + tablet.GetFullTabletId().second; };
        }
        if (WaitingOnly) {
            tabletIdIndex.reserve(Self->BootQueue.WaitQueue.size());
            for (const TBootQueue::TBootQueueRecord& rec : Self->BootQueue.WaitQueue) {
                TTabletInfo* tablet = Self->FindTablet(rec.TabletId);
                if (tablet != nullptr) {
                    tabletIdIndex.push_back({tabletIndexFunction(*tablet), tablet});
                }
            }
        } else {
            tabletIdIndex.reserve(Self->Tablets.size());
            for (auto& [tabletId, tablet] : Self->Tablets) {
                tabletIdIndex.push_back({tabletIndexFunction(tablet), &tablet});
                for (auto& follower : tablet.Followers) {
                    tabletIdIndex.emplace_back(tabletIndexFunction(follower), &follower);
                }
            }
        }
        std::sort(tabletIdIndex.begin(), tabletIdIndex.end(), [](const auto& tab1, const auto& tab2) -> bool { return tab1.first < tab2.first; });

        out << "<script>$('.container').css('width', 'auto');</script>";
        out << "<table class='table table-sortable'>";
        out << "<thead>";
        out << "<tr><th>Tablet</th><th>ID</th><th>Generation</th><th>Node</th><th>State</th><th>VolatileState</th><th>LastAlive</th><th>Restarts</th><th>BootState</th><th>Weight</th><th>Resources</th>"
            << "<th>AllowedMetrics</th><th class='sorter-false'></th></tr>";
        out << "</thead>";
        out << "<tbody>";
        ui64 count = 0;
        for (const auto& tabletIdx : tabletIdIndex) {
            TTabletInfo& x = *tabletIdx.second;
            if (BadOnly) {
                if (x.IsAlive()) {
                    continue;
                }
                if (x.IsLeader() && (x.AsLeader().IsLockedToActor() || x.AsLeader().IsExternalBoot())) {
                    continue;
                }
            }
            ++count;
            if (MaxCount != 0 && count > MaxCount) {
                break;
            }
            TFullTabletId tabletId = x.GetFullTabletId();
            x.UpdateWeight();
            out << "<tr>";
            out << "<td data-text='" << TTabletTypes::TypeToStr(x.GetLeader().Type) << "'><a href=\"../tablets?TabletID="
                << tabletId.first << "\">"
                << TTabletTypes::TypeToStr(x.GetLeader().Type)
                << "</a></td>";
            out << "<td data-text='" << count << "'>" << tabletId.first << '.' << tabletId.second << "</td>";
            out << "<td style='text-align:right'>" << x.GetLeader().KnownGeneration << "</td>";
            out << "<td";
            if (x.NodeId == 0) {
                out << " class='warning'";
            }
            out << " style='text-align:right'>" << x.NodeId << "</td>";
            out << "<td>" << ETabletStateName(x.GetLeader().State) << "</td>";
            out << "<td>" << TTabletInfo::EVolatileStateName(x.GetVolatileState()) << "</td>";
            if (x.IsLeader()) {
                TLeaderTabletInfo& m(x.GetLeader());
                out << "<td>" << TInstant::MilliSeconds(m.Statistics.GetLastAliveTimestamp()).ToStringUpToSeconds() << "</td>";
                out << "<td style='text-align:right'>" << m.Statistics.RestartTimestampSize() << "</td>";
            } else {
                out << "<td>-</td>";
                out << "<td>-</td>";
            }
            out << "<td>" << x.BootState << "</td>";
            out << "<td>" << Sprintf("%.9f", x.Weight) << "</td>";
            out << "<td>" << GetResourceValuesText(x) << "</td>";
            out << "<td>" << x.GetTabletAllowedMetricIds() << "</td>";
            out << "<td><a href='../tablets?SsId=" << tabletId.first << "'><span class='glyphicon glyphicon-tasks' title='State Storage'/></a></td>";
            out << "</tr>";
        }
        out << "</tbody></table>";
    }
};

class TTxMonEvent_MemStateNodes : public TTransactionBase<THive> {
public:
    const TActorId Source;
    THolder<NMon::TEvRemoteHttpInfo> Event;
    bool BadOnly = false;
    ui64 MaxCount = 0;

    TTxMonEvent_MemStateNodes(const TActorId &source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf *hive)
        : TBase(hive)
        , Source(source)
        , Event(ev->Release())
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_MEM_STATE; }

    bool Execute(TTransactionContext &txc, const TActorContext& ctx) override {
        Y_UNUSED(txc);
        TStringStream str;
        if (Event->Cgi().Get("format") == "json") {
            RenderJsonPageWithExtraData(str);
            ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes(str.Str()));
        } else {
            RenderHTMLPage(str);
            ctx.Send(Source, new NMon::TEvRemoteHttpInfoRes(str.Str()));
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Y_UNUSED(ctx);
    }

    void RenderHTMLPage(IOutputStream &out) {
        out << "<script>$('.container').css('width', 'auto');</script>";
        out << "<table class='table table-sortable'>";
        out << "<thead>";
        out << "<tr><th>NodeId</th><th>Local</th><th>Domains</th><th>TabletsScheduled</th><th>TabletsRunning</th>"
               "<th>Values</th><th>Total</th><th>Total</th><th>Maximum</th><th>VolatileState</th><th>Location</th><th>LastAlive</th><th>Restarts</th>"
            << "</tr>";
        out << "</thead>";
        out << "<tbody>";
        TVector<std::pair<TNodeId, const TNodeInfo*>> nodeIdIndex;
        for (auto& nodePair : Self->Nodes) {
            nodeIdIndex.emplace_back(nodePair.first, &nodePair.second);
        }
        Sort(nodeIdIndex);
        for (const auto& [nodeId, nodeInfoPtr] : nodeIdIndex) {
            const TNodeInfo& x = *nodeInfoPtr;
            out << "<tr>";
            out << "<td>" << nodeId << "</td>";
            out << "<td>" << x.Local << "</td>";
            out << "<td>" << x.ServicedDomains << "</td>";
            out << "<td>" << x.GetTabletsScheduled() << "</td>";
            out << "<td>" << x.GetTabletsTotal() - x.GetTabletsScheduled() << "</td>";
            out << "<td>" << GetResourceValuesText(x.ResourceValues) << "</td>";
            out << "<td>" << GetResourceValuesText(x.ResourceTotalValues) << "</td>";
            out << "<td>" << x.NodeTotalUsage << "</td>";
            out << "<td>" << GetResourceValuesText(x.ResourceMaximumValues) << "</td>";
            out << "<td>" << TNodeInfo::EVolatileStateName(x.GetVolatileState()) << "</td>";
            out << "<td>" << GetLocationString(x.Location) << "</td>";
            out << "<td>" << TInstant::MilliSeconds(x.Statistics.GetLastAliveTimestamp()).ToStringUpToSeconds() << "</td>";
            out << "<td>" << x.Statistics.RestartTimestampSize() << "</td>";
            out << "</tr>";
        }
        out << "</tbody>";
        out << "</table>";
    }

    void RenderJsonPageWithExtraData(IOutputStream &out) {
        ui64 nodes = 0;
        ui64 aliveNodes = 0;

        for (const auto& pr : Self->Nodes) {
            if (pr.second.IsAlive()) {
                ++aliveNodes;
            }
            if (!pr.second.IsUnknown()) {
                ++nodes;
            }
        }

        NJson::TJsonValue jsonData;

        jsonData["TotalNodes"] = nodes;
        jsonData["AliveNodes"] = aliveNodes;

        TVector<TNodeInfo*> nodeInfos;
        nodeInfos.reserve(Self->Nodes.size());
        for (auto& pr : Self->Nodes) {
            if (!pr.second.IsUnknown()) {
                nodeInfos.push_back(&pr.second);
            }
        }
        TInstant aliveLine = TInstant::Now() - TDuration::Minutes(10);

        NJson::TJsonValue& jsonNodes = jsonData["Nodes"];
        for (TNodeInfo* nodeInfo : nodeInfos) {
            TNodeInfo& node = *nodeInfo;
            TNodeId id = node.Id;

            if (!node.IsAlive() && TInstant::MilliSeconds(node.Statistics.GetLastAliveTimestamp()) < aliveLine) {
                continue;
            }

            NJson::TJsonValue& jsonNode = jsonNodes.AppendValue(NJson::TJsonValue());
            TString host;
            auto it = Self->NodesInfo.find(node.Id);
            if (it != Self->NodesInfo.end()) {
                auto &ni = it->second;
                if (ni.Host.empty()) {
                    host = ni.Address;
                } else {
                    host = ni.Host;
                }
            }

            jsonNode["Id"] = id;
            jsonNode["Host"] = host;

            jsonNode["Domain"] = node.ServicedDomains.empty() ? "" : Self->GetDomainName(node.GetServicedDomain());
            jsonNode["Alive"] = node.IsAlive();
            jsonNode["Down"] = node.Down;
        }
        NJson::WriteJson(&out, &jsonData);
    }
};

class TTxMonEvent_MemStateDomains : public TTransactionBase<THive> {
public:
    const TActorId Source;
    THolder<NMon::TEvRemoteHttpInfo> Event;
    bool BadOnly = false;
    ui64 MaxCount = 0;

    TTxMonEvent_MemStateDomains(const TActorId &source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf *hive)
        : TBase(hive)
        , Source(source)
        , Event(ev->Release())
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_MEM_STATE; }

    bool Execute(TTransactionContext &txc, const TActorContext& ctx) override {
        Y_UNUSED(txc);
        TStringStream str;
        RenderHTMLPage(str);
        ctx.Send(Source, new NMon::TEvRemoteHttpInfoRes(str.Str()));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Y_UNUSED(ctx);
    }

    void RenderHTMLPage(IOutputStream &out) {
        out << "<script>$('.container').css('width', 'auto');</script>";
        out << "<table class='table table-sortable'>";
        out << "<thead>";
        out << "<tr>";
        out << "<th>TenantId</th>";
        out << "<th>Name</th>";
        out << "<th>Hive</th>";
        out << "<th>Status</th>";
        out << "<th>TabletsAliveInTenantDomain</th>";
        out << "<th>TabletsAliveInOtherDomains</th>";
        out << "<th>TabletsTotal</th>";
        out << "</tr>";
        out << "</thead>";
        out << "<tbody>";
        for (const auto& [domainKey, domainInfo] : Self->Domains) {
            out << "<tr>";
            out << "<td>" << domainKey << "</td>";
            out << "<td>" << domainInfo.Path << "</td>";
            if (domainInfo.HiveId) {
                out << "<td><a href='app?TabletID=" << domainInfo.HiveId << "'>" << domainInfo.HiveId << "</a></td>";
                if (domainInfo.HiveId == Self->TabletID()) {
                    out << "<td>itself</td>";
                } else {
                    TLeaderTabletInfo* tablet = Self->FindTablet(domainInfo.HiveId);
                    if (tablet) {
                        out << "<td>" << tablet->StateString() << "</td>";
                    } else {
                        out << "<td>-</td>";
                    }
                }
            } else {
                out << "<td>-</td>";
                out << "<td>-</td>";
            }
            if (domainInfo.TabletsTotal > 0) {
                out << "<td>" << std::round(domainInfo.TabletsAliveInObjectDomain * 100.0 / domainInfo.TabletsTotal) << "%"
                    << " (" << domainInfo.TabletsAliveInObjectDomain << " of " << domainInfo.TabletsTotal << ")" << "</td>";

                const ui64 tabletsAliveInOtherDomains = domainInfo.TabletsAlive - domainInfo.TabletsAliveInObjectDomain;
                out << "<td>" << std::round(tabletsAliveInOtherDomains * 100.0 / domainInfo.TabletsTotal) << "%"
                    << " (" << tabletsAliveInOtherDomains << " of " << domainInfo.TabletsTotal << ")" << "</td>";
            } else {
                out << "<td>-</td>";
                out << "<td>-</td>";
            }
            out << "<td>" << domainInfo.TabletsTotal << "</td>";
            out << "</tr>";
        }
        out << "</tbody>";
        out << "</table>";
    }
};


TString GetDurationString(TDuration duration) {
    int seconds = duration.Seconds();
    if (seconds < 60)
        return Sprintf("%ds", seconds);
    if (seconds < 3600)
        return Sprintf("%d:%02d", seconds / 60, seconds % 60);
    if (seconds < 86400)
        return Sprintf("%d:%02d:%02d", seconds / 3600, (seconds % 3600) / 60, seconds % 60);
    return Sprintf("%dd %02d:%02d:%02d", seconds / 86400, (seconds % 86400) / 3600, (seconds % 3600) / 60, seconds % 60);
}

class TTxMonEvent_Resources : public TTransactionBase<THive> {
public:
    const TActorId Source;
    TAutoPtr<NMon::TEvRemoteHttpInfo> Event;

    TTxMonEvent_Resources(const TActorId &source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf *hive)
        : TBase(hive)
        , Source(source)
        , Event(ev->Release())
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_RESOURCES; }

    bool Execute(TTransactionContext&, const TActorContext& ctx) override {
        TStringStream str;
        RenderHTMLPage(str, ctx);
        ctx.Send(Source, new NMon::TEvRemoteHttpInfoRes(str.Str()));
        return true;
    }

    void Complete(const TActorContext&) override {
    }

    void RenderHTMLPage(IOutputStream& out, const TActorContext&) {
        TVector<ui64> tabletIdIndex;
        tabletIdIndex.reserve(Self->Tablets.size());
        for (const auto &tabletPair : Self->Tablets) {
            TTabletId tabletId = tabletPair.first;
            tabletIdIndex.push_back(tabletId);
        }
        Sort(tabletIdIndex);

        out << "<head></head><body>";

        out << "<table class='table table-sortable'>";
        out << "<thead>";
        out << "<tr>";
        out << "<th>TabletID</th>";
        out << "<th>Counter</th>";
        out << "<th>CPU</th>";
        out << "<th>Memory</th>";
        out << "<th>Network</th>";
        out << "<th>Storage</th>";
        out << "<th>Read</th>";
        out << "<th>Write</th>";
        out << "</tr>";
        out << "</thead>";

        out << "<tbody>";
        for (const auto& pr : Self->Tablets) {
            TTabletId id = pr.first;
            const TTabletInfo& tablet = pr.second;
            ui64 index = EqualRange(tabletIdIndex.begin(), tabletIdIndex.end(), id).first - tabletIdIndex.begin();

            out << "<tr title='" << tablet.GetResourceValues().DebugString() << "'>";
            out << "<td data-text='" << index << "'><a href='../tablets?TabletID=" << id << "'>" << id << "</a></td>";
            out << GetResourceValuesHtml(tablet.GetResourceValues());
            out << "</tr>";
        }
        out <<"</tbody>";
        out << "</table>";

        out << "<script>";

        out << R"(
               var suffixes = ['K', 'M', 'G', 'T', 'P'];

               function condenseValue(value) {
                 var val = value;
                 var suffix = '';
                 var n = 0;
                 while (n < suffixes.length && Math.abs(val) > 1000) {
                   val = val / 1000;
                   suffix = suffixes[n];
                   n++;
                 }
                 return val.toPrecision(4) + suffix;
               }

               function renderGraph(canvas) {
                 var content = canvas.textContent;
                 var values = content.split(',').map(function(item) { return parseInt(item, 10); });
                 var max = Math.max.apply(Math, values);
                 var min = Math.min.apply(Math, values);
                 var valueHeight = max - min + 1;

                 var ctx = canvas.getContext('2d');
                 var graphHeight = canvas.height;
                 var graphWidth = canvas.width;
                 var marginX = 5;
                 var marginY = 12;

                 graphWidth = graphWidth - marginX * 2;
                 graphHeight = graphHeight - marginY * 2;
                 var x = marginX;
                 var y = canvas.height - marginY - (values[0] - min) * graphHeight / valueHeight;

                 ctx.beginPath();
                 ctx.lineWidth = '2';
                 ctx.strokeStyle = 'red';
                 ctx.lineCap = 'round';
                 ctx.lineJoin = 'round';

                 ctx.moveTo(x, y);
                 var len = values.length;
                 for (var i = 1; i < len; i++) {
                   x = marginX + i * graphWidth / (len - 1);
                   y = canvas.height - marginY - (values[i] - min) * graphHeight / valueHeight;
                   ctx.lineTo(x, y);
                 }
                 ctx.stroke();
                 //ctx.beginPath();
                 ctx.lineWidth = '1';
                 ctx.strokeStyle = 'red';
                 ctx.fillStyle = '#FFCCCC';
                 //ctx.moveTo(x, y);
                 ctx.lineTo(marginX + graphWidth, canvas.height - marginY);
                 ctx.lineTo(marginX, canvas.height - marginY);
                 ctx.lineTo(marginX, canvas.height - marginY - (values[0] - min) * graphHeight / valueHeight);
                 ctx.stroke();
                 ctx.fill();
                 ctx.fillStyle = 'black';
                 ctx.font = '10px Arial';
                 var txt;
                 txt = 'min ' + condenseValue(min);
                 /*ctx.shadowOffsetX = 2;
                                ctx.shadowOffsetY = 2;
                                ctx.shadowColor = 'white';*/
                 ctx.textBaseline = 'bottom';
                 ctx.fillText(txt, 0, canvas.height);
                 ctx.textBaseline = 'top';
                 txt = 'max ' + condenseValue(max);
                 ctx.fillText(txt, canvas.width - ctx.measureText(txt).width, 0);
                 /*ctx.beginPath();
                                ctx.strokeStyle = 'black';
                                               ctx.lineWidth = '1';
                                               ctx.strokeRect(0, 0, canvas.width, canvas.height);
                                               ctx.stroke();*/
               }

               $('.resource-graph').each(function() { renderGraph(this); });

               )";

        out << "</script>";

        out << "</body>";
    }
};

class TTxMonEvent_Settings : public TTransactionBase<THive>, public TLoggedMonTransaction {
public:
    const TActorId Source;
    TAutoPtr<NMon::TEvRemoteHttpInfo> Event;
    bool ChangeRequest = false;

    TTxMonEvent_Settings(const TActorId &source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf *hive)
        : TBase(hive)
        , TLoggedMonTransaction(ev, hive)
        , Source(source)
        , Event(ev->Release())
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_SETTINGS; }

    void UpdateConfig(NIceDb::TNiceDb& db, const TString& param, NJson::TJsonValue& jsonLog, TSchemeIds::State compatibilityParam = TSchemeIds::State::DefaultState) {
        const auto& params(Event->Cgi());
        if (params.contains(param)) {
            const TString& value = params.Get(param);
            const google::protobuf::Reflection* reflection = Self->DatabaseConfig.GetReflection();
            const google::protobuf::FieldDescriptor* field = Self->DatabaseConfig.GetDescriptor()->FindFieldByName(param);
            if (reflection != nullptr && field != nullptr) {
                jsonLog[param] = value;
                if (value.empty()) {
                    reflection->ClearField(&Self->DatabaseConfig, field);
                    // compatibility
                    if (compatibilityParam != TSchemeIds::State::DefaultState) {
                        db.Table<Schema::State>().Key(compatibilityParam).Delete();
                    }
                    // compatibility
                } else {
                    switch (field->cpp_type()) {
                    case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
                        {
                            ui64 val = FromStringWithDefault<ui64>(value);
                            reflection->SetUInt64(&Self->DatabaseConfig, field, val);
                            // compatibility
                            if (compatibilityParam != TSchemeIds::State::DefaultState) {
                                db.Table<Schema::State>().Key(compatibilityParam).Update<Schema::State::Value>(val);
                            }
                            // compatibility
                        }
                        break;
                    case google::protobuf::FieldDescriptor::CPPTYPE_ENUM:
                        {
                            ui64 val = FromStringWithDefault<ui64>(value);
                            reflection->SetEnumValue(&Self->DatabaseConfig, field, val);
                            // compatibility
                            if (compatibilityParam != TSchemeIds::State::DefaultState) {
                                db.Table<Schema::State>().Key(compatibilityParam).Update<Schema::State::Value>(val);
                            }
                            // compatibility
                        }
                        break;
                    case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
                        {
                            bool val = FromStringWithDefault<bool>(value);
                            reflection->SetBool(&Self->DatabaseConfig, field, val);
                            // compatibility
                            if (compatibilityParam != TSchemeIds::State::DefaultState) {
                                db.Table<Schema::State>().Key(compatibilityParam).Update<Schema::State::Value>(val ? 1 : 0);
                            }
                            // compatibility
                        }
                        break;
                    case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
                        {
                            double val = FromStringWithDefault<double>(value);
                            reflection->SetDouble(&Self->DatabaseConfig, field, val);
                            // compatibility
                            if (compatibilityParam != TSchemeIds::State::DefaultState) {
                                db.Table<Schema::State>().Key(compatibilityParam).Update<Schema::State::Value>(val * 100);
                            }
                            // compatibility
                        }
                        break;
                    default:
                        break;
                    }
                }
            }
            ChangeRequest = true;
        }
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto& params(Event->Cgi());
        NIceDb::TNiceDb db(txc.DB);

        NJson::TJsonValue jsonOperation;
        auto& configUpdates = jsonOperation["ConfigUpdates"];

        UpdateConfig(db, "MaxTabletsScheduled", configUpdates, TSchemeIds::State::MaxTabletsScheduled);
        UpdateConfig(db, "MaxBootBatchSize", configUpdates, TSchemeIds::State::MaxBootBatchSize);
        UpdateConfig(db, "DrainInflight", configUpdates, TSchemeIds::State::DrainInflight);
        UpdateConfig(db, "MaxResourceCPU", configUpdates, TSchemeIds::State::MaxResourceCPU);
        UpdateConfig(db, "MaxResourceMemory", configUpdates, TSchemeIds::State::MaxResourceMemory);
        UpdateConfig(db, "MaxResourceNetwork", configUpdates, TSchemeIds::State::MaxResourceNetwork);
        UpdateConfig(db, "MaxResourceCounter", configUpdates, TSchemeIds::State::MaxResourceCounter);
        UpdateConfig(db, "MinScatterToBalance", configUpdates, TSchemeIds::State::MinScatterToBalance);
        UpdateConfig(db, "MinCPUScatterToBalance", configUpdates);
        UpdateConfig(db, "MinMemoryScatterToBalance", configUpdates);
        UpdateConfig(db, "MinNetworkScatterToBalance", configUpdates);
        UpdateConfig(db, "MinCounterScatterToBalance", configUpdates);
        UpdateConfig(db, "MaxNodeUsageToKick", configUpdates, TSchemeIds::State::MaxNodeUsageToKick);
        UpdateConfig(db, "NodeUsageRangeToKick", configUpdates);
        UpdateConfig(db, "ResourceChangeReactionPeriod", configUpdates, TSchemeIds::State::ResourceChangeReactionPeriod);
        UpdateConfig(db, "TabletKickCooldownPeriod", configUpdates, TSchemeIds::State::TabletKickCooldownPeriod);
        UpdateConfig(db, "SpreadNeighbours", configUpdates, TSchemeIds::State::SpreadNeighbours);
        UpdateConfig(db, "DefaultUnitIOPS", configUpdates, TSchemeIds::State::DefaultUnitIOPS);
        UpdateConfig(db, "DefaultUnitThroughput", configUpdates, TSchemeIds::State::DefaultUnitThroughput);
        UpdateConfig(db, "DefaultUnitSize", configUpdates, TSchemeIds::State::DefaultUnitSize);
        UpdateConfig(db, "StorageOvercommit", configUpdates, TSchemeIds::State::StorageOvercommit);
        UpdateConfig(db, "StorageBalanceStrategy", configUpdates, TSchemeIds::State::StorageBalanceStrategy);
        UpdateConfig(db, "StorageSelectStrategy", configUpdates, TSchemeIds::State::StorageSelectStrategy);
        UpdateConfig(db, "StorageSafeMode", configUpdates, TSchemeIds::State::StorageSafeMode);
        UpdateConfig(db, "RequestSequenceSize", configUpdates, TSchemeIds::State::RequestSequenceSize);
        UpdateConfig(db, "MinRequestSequenceSize", configUpdates, TSchemeIds::State::MinRequestSequenceSize);
        UpdateConfig(db, "MaxRequestSequenceSize", configUpdates, TSchemeIds::State::MaxRequestSequenceSize);
        UpdateConfig(db, "MetricsWindowSize", configUpdates, TSchemeIds::State::MetricsWindowSize);
        UpdateConfig(db, "ResourceOvercommitment", configUpdates, TSchemeIds::State::ResourceOvercommitment);
        UpdateConfig(db, "NodeBalanceStrategy", configUpdates);
        UpdateConfig(db, "TabletBalanceStrategy", configUpdates);
        UpdateConfig(db, "MinPeriodBetweenBalance", configUpdates);
        UpdateConfig(db, "BalancerInflight", configUpdates);
        UpdateConfig(db, "MaxMovementsOnAutoBalancer", configUpdates);
        UpdateConfig(db, "ContinueAutoBalancer", configUpdates);
        UpdateConfig(db, "MinPeriodBetweenEmergencyBalance", configUpdates);
        UpdateConfig(db, "EmergencyBalancerInflight", configUpdates);
        UpdateConfig(db, "MaxMovementsOnEmergencyBalancer", configUpdates);
        UpdateConfig(db, "ContinueEmergencyBalancer", configUpdates);
        UpdateConfig(db, "MinNodeUsageToBalance", configUpdates);
        UpdateConfig(db, "MinPeriodBetweenReassign", configUpdates);
        UpdateConfig(db, "NodeSelectStrategy", configUpdates);
        UpdateConfig(db, "CheckMoveExpediency", configUpdates);
        UpdateConfig(db, "SpaceUsagePenaltyThreshold", configUpdates);
        UpdateConfig(db, "SpaceUsagePenalty", configUpdates);
        UpdateConfig(db, "WarmUpBootWaitingPeriod", configUpdates);
        UpdateConfig(db, "MaxWarmUpPeriod", configUpdates);
        UpdateConfig(db, "WarmUpEnabled", configUpdates);
        UpdateConfig(db, "ObjectImbalanceToBalance", configUpdates);
        UpdateConfig(db, "ChannelBalanceStrategy", configUpdates);
        UpdateConfig(db, "MaxChannelHistorySize", configUpdates);
        UpdateConfig(db, "StorageInfoRefreshFrequency", configUpdates);
        UpdateConfig(db, "MinStorageScatterToBalance", configUpdates);
        UpdateConfig(db, "MinGroupUsageToBalance", configUpdates);
        UpdateConfig(db, "StorageBalancerInflight", configUpdates);

        if (params.contains("BalancerIgnoreTabletTypes")) {
            auto value = params.Get("BalancerIgnoreTabletTypes");
            TVector<TString> tabletTypeNames = SplitString(value, ";");
            std::vector<TTabletTypes::EType> newTypeList;
            for (const auto& name : tabletTypeNames) {
                TTabletTypes::EType type = TTabletTypes::StrToType(Strip(name));
                if (IsValidTabletType(type)) {
                    newTypeList.emplace_back(type);
                }
            }
            MakeTabletTypeSet(newTypeList);
            if (newTypeList != Self->BalancerIgnoreTabletTypes) {
                // replace DatabaseConfig.BalancerIgnoreTabletTypes inplace
                auto* field = Self->DatabaseConfig.MutableBalancerIgnoreTabletTypes();
                field->Reserve(newTypeList.size());
                field->Clear();
                for (auto i : newTypeList) {
                    field->Add(i);
                }
                ChangeRequest = true;
                configUpdates["BalancerIgnoreTabletTypes"] = value;
                // Self->BalancerIgnoreTabletTypes will be replaced by Self->BuildCurrentConfig()
            }
        }

        if (params.contains("DefaultTabletLimit")) {
            auto value = params.Get("DefaultTabletLimit");
            auto tabletLimits = SplitString(params.Get("DefaultTabletLimit"), ";");
            for (TStringBuf limit : tabletLimits) {
                TStringBuf tabletType = limit.NextTok(':');
                TTabletTypes::EType type = GetTabletTypeByShortName(TString(tabletType));
                auto maxCount = TryFromString<ui64>(limit);
                if (type == TTabletTypes::TypeInvalid || !maxCount) {
                    continue;
                }
                ChangeRequest = true;
                auto* protoLimit = Self->DatabaseConfig.AddDefaultTabletLimit();
                protoLimit->SetType(type);
                protoLimit->SetMaxCount(*maxCount);
            }

            configUpdates["DefaultTabletLimit"] = value;

            // Get rid of duplicates & default values
            google::protobuf::RepeatedPtrField<NKikimrConfig::THiveTabletLimit> cleanTabletLimits;
            auto* dirtyTabletLimits = Self->DatabaseConfig.MutableDefaultTabletLimit();
            std::unordered_set<TTabletTypes::EType> tabletTypes;
            for (auto it = dirtyTabletLimits->rbegin(); it != dirtyTabletLimits->rend(); ++it) {
                auto tabletType = it->GetType();
                if (tabletTypes.contains(tabletType)) {
                    continue;
                }
                tabletTypes.insert(tabletType);
                if (it->GetMaxCount() != TNodeInfo::MAX_TABLET_COUNT_DEFAULT_VALUE) {
                    cleanTabletLimits.Add(std::move(*it));
                }
            }
            cleanTabletLimits.Swap(dirtyTabletLimits);
        }

        if (ChangeRequest) {
            Self->BuildCurrentConfig();
            db.Table<Schema::State>().Key(TSchemeIds::State::DefaultState).Update<Schema::State::Config>(Self->DatabaseConfig);
        }
        if (params.contains("allowedMetrics")) {
            auto& jsonAllowedMetrics = jsonOperation["AllowedMetricsUpdate"];
            TVector<TString> allowedMetrics = SplitString(params.Get("allowedMetrics"), ";");
            for (TStringBuf tabletAllowedMetrics : allowedMetrics) {
                TStringBuf tabletType = tabletAllowedMetrics.NextTok(':');
                TTabletTypes::EType type = GetTabletTypeByShortName(TString(tabletType));
                if (type != TTabletTypes::TypeInvalid) {
                    static const TVector<i64> metricsPos = {
                        NKikimrTabletBase::TMetrics::kCPUFieldNumber,
                        NKikimrTabletBase::TMetrics::kMemoryFieldNumber,
                        NKikimrTabletBase::TMetrics::kNetworkFieldNumber
                    };
                    if (tabletAllowedMetrics.size() == metricsPos.size()) {
                        TVector<i64> metrics = Self->GetTabletTypeAllowedMetricIds(type);
                        bool changed = false;
                        for (ui32 pos = 0; pos < metricsPos.size(); ++pos) {
                            auto id = metricsPos[pos];
                            auto it = std::find(metrics.begin(), metrics.end(), id);
                            if (tabletAllowedMetrics[pos] == '1' && it == metrics.end()) {
                                metrics.emplace_back(id);
                                changed = true;
                            }
                            if (tabletAllowedMetrics[pos] == '0' && it != metrics.end()) {
                                metrics.erase(it);
                                changed = true;
                            }
                        }
                        if (changed) {
                            ChangeRequest = true;
                            NJson::TJsonValue jsonUpdate;
                            jsonUpdate["Type"] = tabletType;
                            const auto* descriptor = NKikimrTabletBase::TMetrics::descriptor();
                            auto& jsonMetrics = jsonUpdate["AllowedMetrics"];
                            for (auto metricNum : metrics) {
                                const auto* field = descriptor->FindFieldByNumber(metricNum);
                                if (field) {
                                    jsonMetrics.AppendValue(field->name());
                                } else {
                                    jsonMetrics.AppendValue(metricNum);
                                }
                            }
                            jsonAllowedMetrics.AppendValue(std::move(jsonUpdate));
                            db.Table<Schema::TabletTypeMetrics>().Key(type).Update<Schema::TabletTypeMetrics::AllowedMetricIDs>(metrics);
                            Self->TabletTypeAllowedMetrics[type] = metrics;
                        }
                    }
                }
            }
        }

        if (ChangeRequest) {
            WriteOperation(db, jsonOperation);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (ChangeRequest) {
            ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes("{\"status\":\"ok\"}"));
        } else {
            TStringStream str;
            RenderHTMLPage(str, ctx);
            ctx.Send(Source, new NMon::TEvRemoteHttpInfoRes(str.Str()));
        }
    }

    void ShowConfig(IOutputStream& out, const TString& param) {
        const google::protobuf::Reflection* reflection = Self->DatabaseConfig.GetReflection();
        const google::protobuf::FieldDescriptor* field = Self->DatabaseConfig.GetDescriptor()->FindFieldByName(param);
        if (reflection != nullptr && field != nullptr) {
            NKikimrConfig::THiveConfig defaultConfig;
            bool localOverrided = reflection->HasField(Self->DatabaseConfig, field);

            out << "<div class='row'>";
            if (localOverrided) {
                out << "<div class='col-sm-3' style='padding-top:12px;text-align:right'><label for='" << param << "'>" << param << ":</label></div>";
            } else {
                out << "<div class='col-sm-3' style='padding-top:12px;text-align:right'><label for='" << param << "' style='font-weight:normal'>" << param << ":</label></div>";
            }

            switch (field->cpp_type()) {
            case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
                {
                    out << "<div class='col-sm-2' style='padding-top:5px'><input id='" << param << "' class='form-control' type='number' step='0.01' style='max-width:170px' value='"
                        << reflection->GetDouble(Self->CurrentConfig, field) << "' onkeydown='edit(this);' onchange='edit(this);'></div>";
                    out << "<div class='col-sm-1'><button type='button' class='btn' style='margin-top:5px' onclick='applyVal(this, \"" << param << "\");' disabled='true'>Apply</button></div>";
                    out << "<div class='col-sm-1'><button type='button' class='btn' style='margin-top:5px' onclick='resetVal(this, \"" << param << "\");' " << (localOverrided ? "" : "disabled='true'") << ">Reset</button></div>";
                    if (reflection->HasField(Self->ClusterConfig, field)) {
                        out << "<div id='CMS" << param << "' class='col-sm-2' style='padding-top:12px'>" << reflection->GetDouble(Self->ClusterConfig, field) << "</div>";
                    } else {
                        out << "<div id='CMS" << param << "' class='col-sm-2' style='padding-top:12px'>-</div>";
                    }
                    out << "<div id='Default" << param << "' class='col-sm-2' style='padding-top:12px'>" << reflection->GetDouble(defaultConfig, field) << "</div>";
                }
                break;
            case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
                {
                    out << "<div class='col-sm-2' style='padding-top:5px'><input id='" << param << "' class='form-control' type='number' style='max-width:170px' value='"
                        << reflection->GetUInt64(Self->CurrentConfig, field) << "' onkeydown='edit(this);' onchange='edit(this);'></div>";
                    out << "<div class='col-sm-1'><button type='button' class='btn' style='margin-top:5px' onclick='applyVal(this, \"" << param << "\");' disabled='true'>Apply</button></div>";
                    out << "<div class='col-sm-1'><button type='button' class='btn' style='margin-top:5px' onclick='resetVal(this, \"" << param << "\");' " << (localOverrided ? "" : "disabled='true'") << ">Reset</button></div>";
                    if (reflection->HasField(Self->ClusterConfig, field)) {
                        out << "<div id='CMS" << param << "' class='col-sm-2' style='padding-top:12px'>" << reflection->GetUInt64(Self->ClusterConfig, field) << "</div>";
                    } else {
                        out << "<div id='CMS" << param << "' class='col-sm-2' style='padding-top:12px'>-</div>";
                    }
                    out << "<div id='Default" << param << "' class='col-sm-2' style='padding-top:12px'>" << reflection->GetUInt64(defaultConfig, field) << "</div>";
                }
                break;
            case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
                {
                    out << "<div class='col-sm-2' style='padding-top:5px'><input id='" << param << "' class='form-control' type='checkbox' style='width:20px;height:20px'"
                        << (reflection->GetBool(Self->CurrentConfig, field) ? " checked" : "") << " onkeydown='edit(this);' onchange='edit(this);'></div>";
                    out << "<div class='col-sm-1'><button type='button' class='btn' style='margin-top:5px' onclick=\"applyVal(this, '" << param << "', $('#" << param << "').is(':checked') ? 1 : 0);\" disabled='true'>Apply</button></div>";
                    out << "<div class='col-sm-1'><button type='button' class='btn' style='margin-top:5px' onclick=\"resetVal(this, '" << param << "'," << (reflection->GetBool(Self->ClusterConfig, field) ? "true" : "false") << ");\" " << (localOverrided ? "" : "disabled='true'") << ">Reset</button></div>";
                    if (reflection->HasField(Self->ClusterConfig, field)) {
                        out << "<div id='CMS" << param << "' class='col-sm-2' style='padding-top:12px'>" << (reflection->GetBool(Self->ClusterConfig, field) ? "true" : "false") << "</div>";
                    } else {
                        out << "<div id='CMS" << param << "' class='col-sm-2' style='padding-top:12px'>-</div>";
                    }
                    out << "<div id='Default" << param << "' class='col-sm-2' style='padding-top:12px'>" << (reflection->GetBool(defaultConfig, field) ? "true" : "false") << "</div>";
                }
                break;
            case google::protobuf::FieldDescriptor::CPPTYPE_ENUM:
                {
                    int enumvalue = reflection->GetEnumValue(Self->CurrentConfig, field);
                    const google::protobuf::EnumDescriptor* enumfield = field->enum_type();
                    out << "<div class='col-sm-2' style='padding-top:5px'><select id='" << param << "' style='max-width:170px;margin-top:7px' onchange='edit(this);'>";
                    TString base = enumfield->FindValueByNumber(0)->name();
                    for (int n = 1; n < enumfield->value_count(); ++n) {
                        const google::protobuf::EnumValueDescriptor* enumdescr = enumfield->FindValueByNumber(n);
                        TString name = enumdescr->name();
                        if (base.size() > name.size()) {
                            base.resize(name.size());
                        }
                        while (base.size() > 0 && base.back() != name[base.size() - 1]) {
                            base.resize(base.size() - 1);
                        }
                    }
                    for (int n = 0; n < enumfield->value_count(); ++n) {
                        const google::protobuf::EnumValueDescriptor* enumdescr = enumfield->FindValueByNumber(n);
                        out << "<option value=" << enumdescr->number() << (enumvalue == enumdescr->number() ? " selected" : "") << ">"
                            << enumdescr->name().substr(base.size())
                            << "</option>";
                    }
                    out << "</select></div>";
                    int defaultenumvalue = reflection->GetEnumValue(Self->ClusterConfig, field);
                    out << "<div class='col-sm-1'><button type='button' class='btn' style='margin-top:5px' onclick='applyVal(this, \"" << param << "\");' disabled='true'>Apply</button></div>";
                    out << "<div class='col-sm-1'><button type='button' class='btn' style='margin-top:5px' onclick='resetVal(this, \"" << param << "\"," << enumfield->FindValueByNumber(defaultenumvalue)->number() << ");' " << (localOverrided ? "" : "disabled='true'") << ">Reset</button></div>";
                    if (reflection->HasField(Self->ClusterConfig, field)) {
                        out << "<div id='CMS" << param << "' class='col-sm-2' style='padding-top:12px'>" << enumfield->FindValueByNumber(defaultenumvalue)->name().substr(base.size()) << "</div>";
                    } else {
                        out << "<div id='CMS" << param << "' class='col-sm-2' style='padding-top:12px'>-</div>";
                    }
                    out << "<div id='Default" << param << "' class='col-sm-2' style='padding-top:12px'>" << enumfield->FindValueByNumber(reflection->GetEnumValue(defaultConfig, field))->name().substr(base.size()) << "</div>";
                }
                break;
            default:
                break;
            }

            out << "</div>";
        }
    }

    void ShowConfigForBalancerIgnoreTabletTypes(IOutputStream& out) {
        // value of protobuf type "repeated field of ETabletTypes::EType"
        // is represented as a single string build from list delimited type names

        auto makeListString = [] (const NKikimrConfig::THiveConfig& config) {
            std::vector<TTabletTypes::EType> types;
            for (auto i : config.GetBalancerIgnoreTabletTypes()) {
                const auto type = TTabletTypes::EType(i);
                if (IsValidTabletType(type)) {
                    types.emplace_back(type);
                }
            }
            MakeTabletTypeSet(types);
            TVector<TString> names;
            for (auto i : types) {
                names.emplace_back(TTabletTypes::TypeToStr(i));
            }
            return JoinStrings(names, ";");
        };

        const TString param("BalancerIgnoreTabletTypes");

        NKikimrConfig::THiveConfig builtinConfig;
        auto builtinDefault = makeListString(builtinConfig);
        auto clusterDefault = makeListString(Self->ClusterConfig);
        auto currentValue = makeListString(Self->CurrentConfig);

        bool localOverrided = (currentValue != clusterDefault);

        out << "<div class='row'>";
        {
            // mark if value is changed locally
            out << "<div class='col-sm-3' style='padding-top:12px;text-align:right'>"
                << "<label for='" << param << "'"
                << (localOverrided ? "" : "' style='font-weight:normal'")
                << ">" << param << ":</label>"
                << "</div>";
            // editable current value
            out << "<div class='col-sm-2' style='padding-top:5px'>"
                << "<input id='" << param << "' style='max-width:170px;margin-top:7px' onkeydown='edit(this);' onchange='edit(this);'"
                << " value='" << currentValue << "'>"
                << "</div>";
            // apply button
            out << "<div class='col-sm-1'><button type='button' class='btn' style='margin-top:5px' onclick='applyVal(this, \"" << param << "\");' disabled='true'>Apply</button></div>";
            // reset button
            out << "<div class='col-sm-1'><button type='button' class='btn' style='margin-top:5px' onclick='resetVal(this, \"" << param << "\");' " << (localOverrided ? "" : "disabled='true'") << ">Reset</button></div>";
            // show cluster default
            out << "<div id='CMS" << param << "' class='col-sm-2' style='padding-top:12px'>"
                << clusterDefault
                << "</div>";
            // show builtin default
            out << "<div id='Default" << param << "' class='col-sm-2' style='padding-top:12px'>"
                << builtinDefault
                << "</div>";
        }
        out << "</div>";
    }


    void RenderHTMLPage(IOutputStream& out, const TActorContext&/* ctx*/) {
        out << "<head></head><body>";
        out << "<script>$('.container > h2').html('Settings');</script>";
        out << "<div class='form-group'>";
        out << "<div class='row' style='margin-bottom:10px;font-weight:bold'><div class='col-sm-3'></div><div class='col-sm-2'>Current</div><div class='col-sm-2'></div><div class='col-sm-2'>CMS</div><div class='col-sm-2'>Default</div></div>";

        ShowConfig(out, "MaxTabletsScheduled");
        ShowConfig(out, "MaxBootBatchSize");
        ShowConfig(out, "DrainInflight");
        ShowConfig(out, "MinScatterToBalance");
        ShowConfig(out, "MinCPUScatterToBalance");
        ShowConfig(out, "MinMemoryScatterToBalance");
        ShowConfig(out, "MinNetworkScatterToBalance");
        ShowConfig(out, "MinCounterScatterToBalance");
        ShowConfig(out, "MinNodeUsageToBalance");
        ShowConfig(out, "MaxNodeUsageToKick");
        ShowConfig(out, "NodeUsageRangeToKick");
        ShowConfig(out, "ResourceChangeReactionPeriod");
        ShowConfig(out, "TabletKickCooldownPeriod");
        ShowConfig(out, "NodeSelectStrategy");
        ShowConfig(out, "SpreadNeighbours");
        ShowConfig(out, "MaxResourceCPU");
        ShowConfig(out, "MaxResourceMemory");
        ShowConfig(out, "MaxResourceNetwork");
        ShowConfig(out, "MaxResourceCounter");
        ShowConfig(out, "DefaultUnitIOPS");
        ShowConfig(out, "DefaultUnitThroughput");
        ShowConfig(out, "DefaultUnitSize");
        ShowConfig(out, "StorageBalanceStrategy");
        ShowConfig(out, "StorageSafeMode");
        ShowConfig(out, "StorageSelectStrategy");
        ShowConfig(out, "MinPeriodBetweenReassign");
        ShowConfig(out, "MetricsWindowSize");
        ShowConfig(out, "ResourceOvercommitment");
        ShowConfig(out, "NodeBalanceStrategy");
        ShowConfig(out, "TabletBalanceStrategy");
        ShowConfig(out, "MinPeriodBetweenBalance");
        ShowConfig(out, "BalancerInflight");
        ShowConfig(out, "MaxMovementsOnAutoBalancer");
        ShowConfig(out, "ContinueAutoBalancer");
        ShowConfig(out, "MinPeriodBetweenEmergencyBalance");
        ShowConfig(out, "EmergencyBalancerInflight");
        ShowConfig(out, "MaxMovementsOnEmergencyBalancer");
        ShowConfig(out, "ContinueEmergencyBalancer");
        ShowConfig(out, "CheckMoveExpediency");
        ShowConfig(out, "SpaceUsagePenaltyThreshold");
        ShowConfig(out, "SpaceUsagePenalty");
        ShowConfig(out, "WarmUpBootWaitingPeriod");
        ShowConfig(out, "MaxWarmUpPeriod");
        ShowConfig(out, "WarmUpEnabled");
        ShowConfig(out, "ObjectImbalanceToBalance");
        ShowConfig(out, "ChannelBalanceStrategy");
        ShowConfig(out, "MaxChannelHistorySize");
        ShowConfig(out, "StorageInfoRefreshFrequency");
        ShowConfig(out, "MinStorageScatterToBalance");
        ShowConfig(out, "MinGroupUsageToBalance");
        ShowConfig(out, "StorageBalancerInflight");
        ShowConfigForBalancerIgnoreTabletTypes(out);

        out << "<div class='row' style='margin-top:40px'>";
        out << "<div class='col-sm-2' style='padding-top:30px;text-align:right'><label for='allowedMetrics'>AllowedMetrics:</label></div>";
        out << "<div class='col-sm-3' style='padding-top:5px'><table>";
        out << "<tr><th style='padding:2px 10px'>Tablet</th><th style='padding:2px 10px'>Cnt</th><th style='padding:2px 10px'>CPU</th><th style='padding:2px 10px'>Mem</th><th style='padding:2px 10px'>Net</th></tr>";
        for (TTabletTypes::EType tabletType : {
             TTabletTypes::DataShard,
             TTabletTypes::Coordinator,
             TTabletTypes::Mediator,
             TTabletTypes::SchemeShard,
             TTabletTypes::Hive,
             TTabletTypes::KeyValue,
             TTabletTypes::PersQueue,
             TTabletTypes::PersQueueReadBalancer,
             TTabletTypes::NodeBroker,
             TTabletTypes::TestShard,
             TTabletTypes::BlobDepot,
             TTabletTypes::ColumnShard}) {
            const TVector<i64>& allowedMetrics = Self->GetTabletTypeAllowedMetricIds(tabletType);
            out << "<tr>"
                   "<td>" << GetTabletTypeShortName(tabletType) << "</td>";
            out << "<td><input id='cpu' class='form-control' type='checkbox' checked='' disabled='' style='width:20px;height:20px;margin:2px auto'</input></td>";
            out << "<td><input id='cpu' class='form-control' type='checkbox'";
            if (Find(allowedMetrics, NKikimrTabletBase::TMetrics::kCPUFieldNumber) != allowedMetrics.end()) {
                out << " checked=''";
            }
            out << " style='width:20px;height:20px;margin:2px auto' onchange='edit(this);'</input></td>";
            out << "<td><input id='mem' class='form-control' type='checkbox'";
            if (Find(allowedMetrics, NKikimrTabletBase::TMetrics::kMemoryFieldNumber) != allowedMetrics.end()) {
                out << " checked=''";
            }
            out << " style='width:20px;height:20px;margin:2px auto' onchange='edit(this);'</input></td>";
            out << "<td><input id='net' class='form-control' type='checkbox'";
            if (Find(allowedMetrics, NKikimrTabletBase::TMetrics::kNetworkFieldNumber) != allowedMetrics.end()) {
                out << " checked=''";
            }
            out << " style='width:20px;height:20px;margin:2px auto' onchange='edit(this);'</input></td>";
            out << "</tr>";
        }
        out << "</table></div>";
        out << "<div class='col-sm-2' style='padding-top:22px'><button type='button' class='btn' style='margin-top:5px' onclick='applyTab(this);' disabled='true'>Apply</button></div>";
        out << "</div>";

        out << "</div>";

        out << R"___(
               <script>
               function edit(button) {
                   $(button).parents('div').next().children('button').prop('disabled', false);
               }

               function applyVal(button, name, val) {
                   var input = $('#' + name);
                   $.ajax({
                       url: document.URL + '&' + name + '=' + (val === undefined ? input.val() : val),
                       success: function() {
                         $(button).prop('disabled', true).removeClass('btn-danger');
                         $(button).parent().next().children().prop('disabled', false);
                         $(button).parent().parent().find('label').removeAttr('style');
                       },
                       error: function() { $(button).addClass('btn-danger'); }
                   });
               }

               function resetVal(button, name, val) {
                   var input = $('#' + name);
                   $.ajax({
                       url: document.URL + '&' + name + '=',
                       success: function() {
                         if (val == undefined) {
                            var v = $('#CMS' + name).text();
                            if (v == '-') {
                                v = $('#Default' + name).text();
                            }
                            input.val(v);
                         } else {
                            if (val === false || val === true) {
                                input.prop('checked', val);
                            } else {
                                input.val(val);
                            }
                         }
                         $(button).prop('disabled', true).removeClass('btn-danger');
                         $(button).parent().parent().find('label').attr('style', 'font-weight:normal');
                       },
                       error: function() { $(button).addClass('btn-danger'); }
                   });
               }

               function applyTab(button, val) {
                   var table = $(button).parent().prev().children('table').first().get(0);
                   var val = '';
                   for (var rowNum = 1; rowNum < table.rows.length; ++rowNum) {
                       var row = table.rows[rowNum];
                       var type = row.cells[0].innerText;
                       var cnt = row.cells[1].firstChild.checked ? 1 : 0;
                       var cpu = row.cells[2].firstChild.checked ? 1 : 0;
                       var mem = row.cells[3].firstChild.checked ? 1 : 0;
                       var net = row.cells[4].firstChild.checked ? 1 : 0;
                       if (val.length != 0)
                           val += ';';
                       val += type + ':' + cpu + mem + net;
                   }
                   var name = 'allowedMetrics';
                   $.ajax({
                       url: document.URL + '&' + name + '=' + val,
                       success: function() { $(button).prop('disabled', true).removeClass('btn-danger'); },
                       error: function() { $(button).addClass('btn-danger'); }
                   });
               }

               </script>
               )___";

        out << "</body>";
    }
};

class TTxMonEvent_TabletAvailability : public TTransactionBase<THive>, public TLoggedMonTransaction {
public:
    const TActorId Source;
    TAutoPtr<NMon::TEvRemoteHttpInfo> Event;
    bool ChangeRequest = false;
    TNodeId NodeId;
    TNodeInfo* Node = nullptr;

    TTxMonEvent_TabletAvailability(const TActorId &source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf *hive)
        : TBase(hive)
        , TLoggedMonTransaction(ev, hive)
        , Source(source)
        , Event(ev->Release())
    {
        NodeId = FromStringWithDefault<TNodeId>(Event->Cgi().Get("node"), 0);
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_TABLET_AVAILABILITY; }

    static TTabletTypes::EType ParseTabletType(const TString& str) {
        auto parsed = FromStringWithDefault<ui32>(str, TTabletTypes::TypeInvalid);
        parsed = std::min<ui32>(parsed, TTabletTypes::TypeInvalid);
        return TTabletTypes::EType(parsed);
    }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        Node = Self->FindNode(NodeId);
        if (Node == nullptr) {
            return true;
        }

        NJson::TJsonValue jsonOperation;
        jsonOperation["NodeId"] = NodeId;

        const auto& cgi = Event->Cgi();
        auto changeType = ParseTabletType(cgi.Get("changetype"));
        auto maxCount = TryFromString<ui64>(cgi.Get("maxcount"));
        auto resetType = ParseTabletType(cgi.Get("resettype"));
        if (changeType != TTabletTypes::TypeInvalid && maxCount) {
            ChangeRequest = true;
            NJson::TJsonValue jsonUpdate;
            jsonUpdate["Type"] = GetTabletTypeShortName(changeType);
            jsonUpdate["MaxCount"] = *maxCount;
            jsonOperation["TabletAvailability"].AppendValue(std::move(jsonUpdate));
            db.Table<Schema::TabletAvailabilityRestrictions>().Key(NodeId, changeType).Update<Schema::TabletAvailabilityRestrictions::MaxCount>(*maxCount);
            Node->TabletAvailabilityRestrictions[changeType] = *maxCount;
            auto it = Node->TabletAvailability.find(changeType);
            if (it != Node->TabletAvailability.end()) {
                it->second.UpdateRestriction(*maxCount);
            }
        }
        if (resetType != TTabletTypes::TypeInvalid) {
            ChangeRequest = true;
            NJson::TJsonValue jsonUpdate;
            jsonUpdate["Type"] = GetTabletTypeShortName(resetType);
            jsonUpdate["MaxCount"] = "[default]";
            jsonOperation["TabletAvailability"].AppendValue(std::move(jsonUpdate));
            Node->TabletAvailabilityRestrictions.erase(resetType);
            auto it = Node->TabletAvailability.find(resetType);
            if (it != Node->TabletAvailability.end()) {
                it->second.RemoveRestriction();
                if (it->second.IsSet) {
                    db.Table<Schema::TabletAvailabilityRestrictions>()
                      .Key(NodeId, resetType)
                      .Update<Schema::TabletAvailabilityRestrictions::MaxCount>(TNodeInfo::MAX_TABLET_COUNT_DEFAULT_VALUE);
                } else {
                    db.Table<Schema::TabletAvailabilityRestrictions>()
                      .Key(NodeId, resetType)
                      .Delete();
                }
            }
        }
        if (ChangeRequest) {
            Self->ObjectDistributions.RemoveNode(*Node);
            Self->ObjectDistributions.AddNode(*Node);
            WriteOperation(db, jsonOperation);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (ChangeRequest) {
            ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes("{\"status\":\"ok\"}"));
        } else {
            ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes("{\"status\":\"error\"}"));
        }
    }

};



class TTxMonEvent_Landing : public TTransactionBase<THive> {
public:
    const TActorId Source;
    TAutoPtr<NMon::TEvRemoteHttpInfo> Event;
    TCgiParameters Cgi;

    TTxMonEvent_Landing(const TActorId &source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf *hive)
        : TBase(hive)
        , Source(source)
        , Event(ev->Release())
        , Cgi(Event->Cgi())
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_LANDING; }

    bool Execute(TTransactionContext &txc, const TActorContext& ctx) override {
        Y_UNUSED(txc);
        TStringStream str;
        RenderHTMLPage(str);
        ctx.Send(Source, new NMon::TEvRemoteHttpInfoRes(str.Str()));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Y_UNUSED(ctx);
    }

    void RenderHTMLPage(IOutputStream &out) {
        ui64 runningTablets = 0;
        ui64 aliveNodes = 0;
        THashMap<TTabletTypes::EType, ui32> tabletTypesToChannels;

        for (const auto& pr : Self->Tablets) {
            if (pr.second.IsRunning()) {
                ++runningTablets;
            }
            if (pr.second.IsLockedToActor()) {
                ++runningTablets;
            }
            for (const auto& sl : pr.second.Followers) {
                if (sl.IsRunning()){
                    ++runningTablets;
                }
            }
            {
                auto it = tabletTypesToChannels.find(pr.second.Type);
                if (it == tabletTypesToChannels.end()) {
                    ui32 channels = pr.second.GetChannelCount();
                    tabletTypesToChannels.emplace(pr.second.Type, channels);
                }
            }
        }
        for (const auto& pr : Self->Nodes) {
            if (pr.second.IsAlive()) {
                ++aliveNodes;
            }
        }

        out << "<head>";
        out << "<style>";
        out << "table.simple-table1 th { text-align: center; }";
        out << "table.simple-table1 td { padding: 1px 3px; }";
        out << "table.simple-table1 td:nth-child(1) { text-align: right; }";
        out << "table.simple-table2 th { text-align: right; }";
        out << "table.simple-table2 tr:nth-child(1) > th:nth-child(2) { text-align: left; }";
        out << "table.simple-table2 tr:nth-child(1) > th:nth-child(3) { text-align: left; }";
        out << "table.simple-table2 tr:nth-child(1) > th:nth-child(4) { text-align: left; }";
        out << "table.simple-table2 td { text-align: right; }";
        out << "table.simple-table2 td:nth-child(2) { text-align: left; }";
        out << "table.simple-table2 td:nth-child(3) { text-align: left; }";
        out << "table.simple-table2 td:nth-child(4) { text-align: left; }";
        out << "table.simple-table3 td { padding: 1px 3px; text-align: right; }";
        out << "table.simple-table3 th { text-align: center; }";
        out << ".table-hover tbody tr:hover > td { background-color: #9dddf2; }";
        out << ".blinking { animation:blinkingText 0.8s infinite; }";
        out << "@keyframes blinkingText { 0% { color: #000; } 49% { color: #000; } 60% { color: transparent; } 99% { color:transparent; } 100% { color: #000; } }";
        out <<  ".box { border: 1px solid grey; border-radius: 5px; padding-left: 2px; padding-right: 2px; display: inline-block; font: 11px Arial; cursor: pointer }";
        out << ".box-disabled { text-decoration: line-through; }";
        out << "</style>";
        out << "</head>";
        out << "<body>";
        out << "<div style='display:flex'><div style='min-width:220px'><table class='simple-table1'>";
        out << "<tr><th colspan='2'>Info</th></tr>";
        TSubDomainKey domainId = Self->GetMySubDomainKey();
        if (domainId) {
            out << "<tr><td><span id='alert-placeholder' class='glyphicon' style='height:14px'></span>" << "Tenant:" << "</td>";
            TDomainInfo* domainInfo = Self->FindDomain(domainId);
            if (domainInfo && domainInfo->Path) {
                out << "<td>" << domainInfo->Path << "</td>";
            } else {
                out << "<td>" << domainId << "</td>";
            }
            out << "<td></tr>";
        }

        out << "<tr><td>" << "Nodes:" << "</td><td id='aliveNodes'>" << aliveNodes << "</td></tr>";
        out << "<tr><td>" << "Tablets:" << "</td><td id='runningTablets'>" << runningTablets << "</td></tr>";
        out << "<tr><td>" << "Boot Queue:" << "</td><td id='bootQueue'>" << Self->BootQueue.BootQueue.size() << "</td></tr>";
        out << "<tr><td>" << "Wait Queue:" << "</td><td id='waitQueue'>" << Self->BootQueue.WaitQueue.size() << "</td></tr>";
        out << "</table></div>";
        out << "<div style='width:180px'><table class='simple-table1'>";
        out << "<tr><th colspan='2'>Totals</th></tr>";
        out << "<tr><td>Counter</td><td id='resourceTotalCounter'></td></tr>";
        out << "<tr><td>CPU</td><td id='resourceTotalCPU'></td></tr>";
        out << "<tr><td>Memory</td><td id='resourceTotalMemory'></td></tr>";
        out << "<tr><td>Network</td><td id='resourceTotalNetwork'></td></tr>";
        out << "</table></div>";
        out << "<div style='width:220px'><table class='simple-table1'>";
        out << "<tr><th colspan='2'>Variance</th></tr>";
        out << "<tr><td>Counter</td><td id='resourceStdDevCounter'></td></tr>";
        out << "<tr><td>CPU</td><td id='resourceStdDevCPU'></td></tr>";
        out << "<tr><td>Memory</td><td id='resourceStdDevMemory'></td></tr>";
        out << "<tr><td>Network</td><td id='resourceStdDevNetwork'></td></tr>";
        out << "</table></div>";
        out << "<div style='min-width:220px'><table class='simple-table1'>";
        out << "<tr><th colspan='2'>Triggers</th></tr>";
        out << "<tr><td>Counter</td><td id='resourceScatterCounter'></td></tr>";
        out << "<tr><td>CPU</td><td id='resourceScatterCPU'></td></tr>";
        out << "<tr><td>Memory</td><td id='resourceScatterMemory'></td></tr>";
        out << "<tr><td>Network</td><td id='resourceScatterNetwork'></td></tr>";
        out << "<tr><td>MaxUsage</td><td id='maxUsage'></td></tr>";
        out << "<tr><td>Imbalance</td><td id='objectImbalance'></td></tr>";
        out << "<tr><td>Storage</td><td id='storageScatter'></td></tr>";
        out << "</table></div>";
        out << "<div style='min-width:220px'><table class='simple-table3'>";
        out << "<tr><th>Balancer</th><th style='min-width:50px'>Runs</th><th style='min-width:50px'>Moves</th>";
        out << "<th style='min-width:80px'>Last run</th><th style='min-width:80px'>Last moves</th><th style='min-width:80px'>Progress</th></tr>";
        for (EBalancerType type : {
            EBalancerType::ScatterCounter,
            EBalancerType::ScatterCPU,
            EBalancerType::ScatterMemory,
            EBalancerType::ScatterNetwork,
            EBalancerType::Emergency,
            EBalancerType::SpreadNeighbours,
            EBalancerType::Scatter,
            EBalancerType::Manual,
            EBalancerType::Storage,
        }) {
            int balancer = static_cast<int>(type);
            out << "<tr id='balancer" << balancer << "'><td>" << EBalancerTypeName(type) << "</td><td></td><td></td><td></td><td></td><td></td></tr>";
        }
        out << "</table></div></div>";

        out << "<table id='node_table' class='table simple-table2 table-hover table-condensed'>";
        out << "<thead><tr>"
               "<th rowspan='2' style='min-width:70px'>Node</th>"
               "<th rowspan='2' style='min-width:280px'>Name</th>"
               "<th rowspan='2' style='min-width:50px'>DC</th>"
               "<th rowspan='2' style='min-width:280px'>Domain</th>"
               "<th rowspan='2' style='min-width:100px'>Uptime</th>"
               "<th rowspan='2'><span class='glyphicon glyphicon-question-sign' title='Unknown state tablets' style='min-width:40px'></span></th>"
               "<th rowspan='2'><span class='glyphicon glyphicon-time' title='Starting tablets' style='min-width:40px'></span></th>"
               "<th rowspan='2'><span class='glyphicon glyphicon-flash' title='Running tablets' style='min-width:40px'></span></th>"
               "<th style='min-width:240px; text-align:center'>Types</th>"
               "<th rowspan='2'>Usage</th>"
               "<th colspan='4' style='text-align:center'>Resources</td>"
               "<th rowspan='2' style='text-align:center'>Active</th>"
               "<th rowspan='2' style='text-align:center'>Freeze</th>"
               "<th rowspan='2' style='text-align:center'>Kick</th>"
               "<th rowspan='2' style='text-align:center'>Drain</th>"
               "<tr>"
               "<th style='text-align:center' id='types'>" << GetTypesHtml(Self->SeenTabletTypes, Self->GetTabletLimit()) << "</th>"
               "<th style='min-width:70px'>cnt</th>"
               "<th style='min-width:100px'>cpu</th>"
               "<th style='min-width:100px'>mem</th>"
               "<th style='min-width:100px'>net</th></tr>"
               "</thead>";
        out << "<tbody>";
        out << "<tr><td colspan=18 style='text-align:center'><button type='button' class='btn btn-info' onclick='updateData();' style='margin-top:30px;margin-bottom:30px'>View Nodes</button></td></tr>";
        out << "</tbody>";
        out << "</table>";

        out << "<div class='row' style='margin-top:100px'>";
        out << "<div class='col-sm-1 col-md-1' style='text-align:center'>";
        out << "<button type='button' class='btn btn-info' onclick='location.href=\"app?TabletID=" << Self->HiveId << "&page=MemStateTablets&bad=1&max=1000\";' style='width:138px'>Bad Tablets</button>";
        out << "</div>";
        out << "<div class='col-sm-1 col-md-1' style='text-align:center'>";
        out << "<button type='button' class='btn btn-info' onclick='location.href=\"app?TabletID=" << Self->HiveId << "&page=MemStateTablets&sort=weight&max=1000\";' style='width:138px'>Heavy Tablets</button>";
        out << "</div>";
        out << "<div class='col-sm-1 col-md-1' style='text-align:center'>";
        out << "<button type='button' class='btn btn-info' onclick='location.href=\"app?TabletID=" << Self->HiveId << "&page=MemStateTablets&wait=1&max=1000\";' style='width:138px'>Waiting Tablets</button>";
        out << "</div>";
        out << "<div class='col-sm-1 col-md-1' style='text-align:center'>";
        out << "<button type='button' class='btn btn-info' onclick='location.href=\"app?TabletID=" << Self->HiveId << "&page=Resources\";' style='width:138px'>Resources</button>";
        out << "</div>";
        out << "<div class='col-sm-1 col-md-1' style='text-align:center'>";
        out << "<button type='button' class='btn btn-info' onclick='location.href=\"app?TabletID=" << Self->HiveId << "&page=MemStateDomains\";' style='width:138px'>Tenants</button>";
        out << "</div>";
        out << "<div class='col-sm-1 col-md-1' style='text-align:center'>";
        out << "<button type='button' class='btn btn-info' data-toggle='modal' data-target='#rebalance' style='width:138px'>Balancer</button>";
        out << "</div>";
        out << "<div class='col-sm-1 col-md-1' style='text-align:center'>";
        out << "<button type='button' class='btn btn-info' onclick='location.href=\"app?TabletID=" << Self->HiveId << "&page=OperationsLog&max=100\";' style='width:138px'>Operations Log</button>";
        out << "</div>";
        out << "</div>";

        out << "<div class='row' style='margin-top:10px'>";
        out << "<div class='col-sm-1 col-md-1' style='text-align:center'>";
        out << "<button type='button' class='btn btn-info' onclick='location.href=\"app?TabletID=" << Self->HiveId << "&page=MemStateNodes\";' style='width:138px'>Nodes</button>";
        out << "</div>";
        out << "<div class='col-sm-1 col-md-1' style='text-align:center'>";
        out << "<button type='button' class='btn btn-info' onclick='location.href=\"app?TabletID=" << Self->HiveId << "&page=Storage\";' style='width:138px'>Storage</button>";
        out << "</div>";
        out << "<div class='col-sm-1 col-md-1' style='text-align:center'>";
        out << "<button type='button' class='btn btn-info' onclick='location.href=\"app?TabletID=" << Self->HiveId << "&page=Groups\";' style='width:138px'>Groups</button>";
        out << "</div>";
        out << "<div class='col-sm-1 col-md-1' style='text-align:center'>";
        out << "<button type='button' class='btn btn-info' onclick='location.href=\"app?TabletID=" << Self->HiveId << "&page=Settings\";' style='width:138px'>Settings</button>";
        out << "</div>";
        out << "<div class='col-sm-1 col-md-1' style='text-align:center'>";
        out << "<button type='button' class='btn btn-info' data-toggle='modal' data-target='#reassign-groups' style='width:138px'>Reassign Groups</button>";
        out << "</div>";
        out << "<div class='col-sm-1 col-md-1' style='text-align:center'>";
        out << "<button type='button' class='btn btn-info' onclick='location.href=\"app?TabletID=" << Self->HiveId << "&page=Subactors\";' style='width:138px'>SubActors</button>";
        out << "</div>";
        out << "</div>";

        out << "<div class='row' style='margin-top:50px'>";

        out << "<div class='form-group'><label for='tablets_group' style='padding-right:10px'>Groups of Tablet:</label>";
        out << "<input type='text' id='tablets_group'><input type='submit' value='open' onclick=\"window.open(document.URL + '&page=Groups&tablet_id=' + this.previousSibling.value);\">";
        out << "</div>";

        out << "<div class='form-group'><label for='groups_tablet' style='padding-right:10px'>Tablets of Group:</label>";
        out << "<input type='text' id='groups_tablet'><input type='submit' value='open' onclick=\"window.open(document.URL + '&page=Groups&group_id=' + this.previousSibling.value);\">";
        out << "</div>";

        out << "</div>";

        out << R"___(
               <div class='modal fade' id='reassign-groups' role='dialog'>
                   <div class='modal-dialog' style='width:60%'>
                       <div class='modal-content'>
                           <div class='modal-header'>
                               <button type='button' class='close' data-dismiss='modal' onclick='cancel();'>&times;</button>
                               <h4 class='modal-title'>Reassign Tablets Groups</h4>
                           </div>
                           <div class='modal-body'>
                               <div class='row'>
                                   <div class='col-md-5'>
                                       <label for='tablet_storage_pool_group'>Storage pool</label>
                                       <div in='tablet_storage_pool_group' class='input-group' style='width:100%'>
                                           <input id='tablet_storage_pool' type='text' min='0' max='255' class='form-control'>
                                       </div>
                                       <label for='tablet_storage_group_group' style='margin-top:20px'>Storage group</label>
                                       <div in='tablet_storage_group_group' class='input-group'>
                                           <input id='tablet_storage_group' type='number' class='form-control'>
                                           <span class='input-group-addon'>group id</span>
                                       </div>
                                   </div>
                                   <div class='col-md-4'>
                                       <label for='tablet_type'>Type</label>
                                       <select id='tablet_type' class='form-control'>
                                           <option selected value>Any</option>
                                       </select>
                                       <label for='tablet_from_channel_group' style='margin-top:20px'>Channels</label>
                                       <div in='tablet_from_channel_group' class='input-group'>
                                           <input id='tablet_from_channel' type='number' value='0' min='0' max='255' class='form-control'>
                                           <span class='input-group-addon'>from</span>
                                           <input id='tablet_to_channel' type='number' value='255' min='0' max='255' class='form-control'>
                                           <span class='input-group-addon'>to</span>
                                       </div>
                                   </div>
                                   <div class='col-md-3'>
                                       <label for='tablet_percent_group'>Percent</label>
                                       <div in='tablet_percent_group' class='input-group'>
                                           <input id='tablet_percent' type='number' value='100' min='1' max='100' class='form-control'>
                                           <span class='input-group-addon'>%</span>
                                       </div>
                                       <label for='tablet_reassign_inflight_group' style='margin-top:20px'>Inflight</label>
                                       <div id='tablet_reassign_inflight_group' class='input-group'>
                                           <input id='tablet_reassign_inflight' type='number' value='1' min='1' max='10' class='form-control'>
                                           <span class='input-group-addon'>1-10</span>
                                       </div>
                                   </div>
                               </div>
                               <div class='row'>
                                   <div class='col-md-12'>
                                       <hr>
                                   </div>
                               </div>
                               <div class='row'>
                                   <div class='col-md-2' style='visibility:hidden'>
                                       <label for='tablets_found_group'>Found</label>
                                       <div id='tablets_found_group'>
                                           <span id='tablets_found'>0</span>
                                       </div>
                                   </div>
                                   <div class='col-md-2' style='visibility:hidden'>
                                       <label for='tablets_processed_group'>Processed</label>
                                       <div id='tablets_processed_group'>
                                           <span id='tablets_processed'></span>
                                       </div>
                                   </div>
                                   <div class='col-md-2' style='visibility:hidden'>
                                       <label for='current_inflight_group'>Inflight</label>
                                       <div id='current_inflight_group'>
                                           <span id='current_inflight'></span>
                                       </div>
                                   </div>
                                   <div class='col-md-2' style='visibility:hidden'>
                                       <label for='time_left_group'>Time left</label>
                                       <div id='time_left_group'>
                                           <span id='time_left'></span>
                                       </div>
                                   </div>
                                   <div class='col-md-2'>
                                   </div>
                                   <div class='col-md-2'>

                                   </div>
                               </div>
                               <div class='row' style='margin-top:20px'>
                                   <div class='col-md-12' style='visibility:hidden'>
                                       <label for='progress_bar_group'>Progress</label>
                                       <div id='progress_bar_group' class='progress' style='height:30px'>
                                           <div id='progress_bar' class='progress-bar' role='progressbar' style='width:0%;font-size:20px;padding-top:5px' aria-valuenow='0' aria-valuemin='0' aria-valuemax='100'></div>
                                       </div>
                                   </div>
                               </div>
                           </div>
                           <div class='modal-footer'>
                               <span id='status_text' style='float:left'></span>
                               <button id='button_query' type='submit' class='btn btn-default' onclick='queryTablets();'>Query</button>
                               <button id='button_reassign' type='submit' class='btn btn-default disabled' onclick='reassignGroups();'>Reassign</button>
                               <button type='button' class='btn btn-default' data-dismiss='modal' onclick='cancel();'>Cancel</button>
                           </div>
                       </div>
                   </div>
               </div>
               )___";

        out << R"___(
               <div class='modal fade' id='rebalance' role='dialog'>
                   <div class='modal-dialog' style='width:60%'>
                       <div class='modal-content'>
                           <div class='modal-header'>
                               <button type='button' class='close' data-dismiss='modal'>&times;</button>
                               <h4 class='modal-title'>Balancer</h4>
                           </div>
                           <div class='modal-body'>
                               <div class='row'>
                                   <div class='col-md-12'>
                                       <h2> Run Balancer</h2>
                                   </div>
                               </div>
                               <div class='row'>
                                   <div class='col-md-2'>
                                       <label for='balancer_max_movements'>Max movements</label>
                                       <div in='balancer_max_movements' class='input-group'>
                                           <input id='balancer_max_movements' type='number' value='1000' class='form-control'>
                                       </div>
                                       <br>
                                   </div>
                               </div>
                               <div class='row'>
                                   <div class='col-md-2'>
                                       <button type='submit' class='btn btn-primary' onclick='rebalanceTablets()' data-dismiss='modal' id='run-balancer'>Run</button>
                                   </div>
                               </div>
                               <div class='row'>
                                   <div class='col-md-12'>
                                       <hr>
                                   </div>
                               </div>
                               <div class='row'>
                                   <div class='col-md-12'>
                                       <h2> Rebalance ALL tablets FROM SCRATCH</h2>
                                   </div>
                               </div>
                               <div class='row'>
                                   <div class='col-md-8'>
                                       <label for='tenant_name'> Please enter the tenant name to confirm you know what you are doing</label>
                                       <div in='tenant_name' class='input-group' style='width:100%'>
                                           <input id='tenant_name' type='text' class='form-control'>
                                       </div>
                                       <br>
                                   </div>
                              </div>
                              <div class='row'>
                                   <div class='col-md-2'>
                                       <button id='button_rebalance' type='submit' class='btn btn-danger' onclick='rebalanceTabletsFromScratch();' data-dismiss='modal'>Run</button>
                                   </div>
                              </div>
                              <div class='row'>
                                   <div class='col-md-12'>
                                       <hr>
                                   </div>
                              </div>
                              <div class='row'>
                                   <div class='col-md-12'>
                                       <h2> Latest tablet moves</h2>
                                   </div>
                              </div>
                              <div class='row'>
                                   <div class='col-md-12'>
                                       <table id='move_history' class='table table-stripped'>
                                       <thead>
                                       <th>Timestamp</th>
                                       <th>Tablet</th>
                                       <th>Node</th>
                                       </thead>
                                       <tbody>
                                       </tbody>
                                       </table>
                                   </div>
                              </div>
                           </div>
                           <div class='modal-footer'>
                               <button type='button' class='btn btn-default' data-dismiss='modal'>Cancel</button>
                           </div>
                       </div>
                   </div>
               </div>
               )___";
        out << "<script>";
        out << "var hiveId = '" << Self->HiveId << "';";
        out << "var tablets = [";
        for (auto itTablet = tabletTypesToChannels.begin(); itTablet != tabletTypesToChannels.end(); ++itTablet) {
            if (itTablet != tabletTypesToChannels.begin()) {
                out << ',';
            }
            out << "{type:" << (ui32)(itTablet->first) << ",name:'" << TTabletTypes::TypeToStr(itTablet->first) << "',channels:" << itTablet->second << "}";
        }
        out << "];";
        out << R"___(

$('.container')
    .toggleClass('container container-fluid')
    .css('padding-left', '1%')
    .css('padding-right', '1%');

function initReassignGroups() {
    var domTabletType = document.getElementById('tablet_type');
    for (var tab = 0; tab < tablets.length; tab++) {
        var opt = document.createElement('option');
        opt.text = tablets[tab].name;
        opt.value = tablets[tab].type;
        domTabletType.add(opt);
    }
}

initReassignGroups();

var tablets_found;
var Nodes = {};
var should_refresh_types = false;

function queryTablets() {
    var storage_pool = $('#tablet_storage_pool').val();
    var storage_group = $('#tablet_storage_group').val();
    var tablet_type = $('#tablet_type').val();
    var channel_from = $('#tablet_from_channel').val();
    var channel_to = $('#tablet_to_channel').val();
    var percent = $('#tablet_percent').val();
    var url = 'app?TabletID=' + hiveId + '&page=FindTablet';
    if (storage_pool) {
        url = url + '&storagePool=' + storage_pool;
    }
    if (storage_group) {
        url = url + '&group=' + storage_group;
    }
    if (tablet_type) {
        url = url + '&type=' + tablet_type;
    }
    if (channel_from) {
        url = url + '&channelFrom=' + channel_from;
    }
    if (channel_to) {
        url = url + '&channelTo=' + channel_to;
    }
    if (percent) {
        url = url + '&percent=' + percent;
    }
    $.ajax({
        url: url,
        success: function(result) {
            tablets_found = result;
            $('#tablets_found_group').parent().css({visibility: 'visible'});
            $('#tablets_found').text(tablets_found.length);
            $('#button_reassign').removeClass('disabled');
        },
        error: function(jqXHR, status) {
            $('#status_text').text(status);
        }
    });
}

var tables_processed;
var current_inflight;

function continueReassign() {
    var max_inflight = $('#tablet_reassign_inflight').val();
    while (tablets_processed < tablets_found.length && current_inflight < max_inflight) {
        var tablet = tablets_found[tablets_processed];
        tablets_processed++;
        current_inflight++;
        $('#current_inflight').text(current_inflight);
        $.ajax({
            url: 'app?TabletID=' + hiveId
                + '&page=ReassignTablet&tablet=' + tablet.tabletId
                + '&channels=' + tablet.channels
                + '&wait=1',
            success: function() {

            },
            error: function(jqXHR, status) {
                $('#status_text').text(status);
            },
            complete: function() {
                $('#tablets_processed').text(tablets_processed);
                var value = Number(tablets_processed * 100 / tablets_found.length).toFixed();
                $('#progress_bar').css('width', value + '%').attr('aria-valuenow', value).text(value + '%');
                current_inflight--;
                continueReassign();
            },
        });
    }
    if (tablets_processed >= tablets_found.length) {
        $('#button_query').removeClass('disabled');
        $('#button_reassign').removeClass('disabled');
    }
}

function cancel() {
    tablets_processed = tablets_found.length;
    $('#tablets_processed_group').parent().css({visibility: 'hidden'});
    $('#current_inflight_group').parent().css({visibility: 'hidden'});
    $('#time_left_group').parent().css({visibility: 'hidden'});
    $('#progress_bar_group').parent().css({visibility: 'hidden'});
}

function reassignGroups() {
    $('#tablets_processed_group').parent().css({visibility: 'visible'});
    $('#current_inflight_group').parent().css({visibility: 'visible'});
    //$('#time_left_group').parent().css({visibility: 'visible'});
    $('#progress_bar_group').parent().css({visibility: 'visible'});
    $('#button_query').addClass('disabled');
    $('#button_reassign').addClass('disabled');
    tablets_processed = 0;
    current_inflight = 0;
    continueReassign();
}

function setDown(element, nodeId, down) {
    if (down && $(element).hasClass('glyphicon-ok')) {
        $(element).removeClass('glyphicon-ok');
        element.inProgress = true;
        $.ajax({url:'app?TabletID=' + hiveId + '&node=' + nodeId + '&page=SetDown&down=1', success: function(){ $(element).addClass('glyphicon-remove'); element.inProgress = false; }});
    } else if (!down && $(element).hasClass('glyphicon-remove')) {
        $(element).removeClass('glyphicon-remove');
        element.inProgress = true;
        $.ajax({url:'app?TabletID=' + hiveId + '&node=' + nodeId + '&page=SetDown&down=0', success: function(){ $(element).addClass('glyphicon-ok'); element.inProgress = false; }});
    }
}

function toggleDown(element, nodeId) {
    setDown(element, nodeId, $(element).hasClass('glyphicon-ok'));
}

function toggleFreeze(element, nodeId) {
    if ($(element).hasClass('glyphicon-play')) {
        $(element).removeClass('glyphicon-play');
        element.inProgress = true;
        $.ajax({url:'app?TabletID=' + hiveId + '&node=' + nodeId + '&page=SetFreeze&freeze=1', success: function(){ $(element).addClass('glyphicon-pause'); element.inProgress = false; }});
    } else if ($(element).hasClass('glyphicon-pause')) {
        $(element).removeClass('glyphicon-pause');
        element.inProgress = true;
        $.ajax({url:'app?TabletID=' + hiveId + '&node=' + nodeId + '&page=SetFreeze&freeze=0', success: function(){ $(element).addClass('glyphicon-play'); element.inProgress = false; }});
    }
}

function kickNode(element, nodeId) {
    $(element).removeClass('glyphicon-transfer');
    $.ajax({url:'app?TabletID=' + hiveId + '&node=' + nodeId + '&page=KickNode', success: function(){ $(element).addClass('glyphicon-transfer'); }});
}

function drainNode(element, nodeId) {
    $(element).removeClass('glyphicon-transfer');
    $.ajax({url:'app?TabletID=' + hiveId + '&node=' + nodeId + '&page=DrainNode', success: function(){ $(element).addClass('blinking'); Nodes[nodeId].Drain = true; }});
}

function rebalanceTablets() {
    $('#balancerProgress').html('o.O');
    var max_movements = $('#balancer_max_movements').val();
    $.ajax({url:'app?TabletID=' + hiveId + '&page=Rebalance&movements=' + max_movements});
}

function rebalanceTabletsFromScratch(element) {
    var tenant_name = $('#tenant_name').val();
    $.ajax({url:'app?TabletID=' + hiveId + '&page=RebalanceFromScratch&tenantName=' + tenant_name});
}

function toggleAlert() {
    $('#alert-placeholder').toggleClass('glyphicon-refresh');
}

function clearAlert() {
    $('#alert-placeholder').removeClass('glyphicon-refresh');
}

function enableType(element, node, type) {
    $(element).css('color', 'gray');
    $.ajax({url:'?TabletID=' + hiveId + '&node=' + node + '&page=TabletAvailability&resettype=' + type});
}

function disableType(element, node, type) {
    $(element).css('color', 'gray');
    $.ajax({url:'?TabletID=' + hiveId + '&node=' + node + '&page=TabletAvailability&maxcount=0&changetype=' + type});
}

function applySetting(button, name, val) {
    $(button).css('color', 'gray');
    if (name == "DefaultTabletLimit") {
        should_refresh_types = true;
    }
    $.ajax({
        url: document.URL + '&page=Settings&' + name + '=' + val,
    });
}

var Empty = true;

function getBalancerString(balancer) {
    return 'runs=' + balancer.TotalRuns + ' moves=' + balancer.TotalMovements;
}

function fillDataShort(result) {
    try {
        if ("TotalTablets" in result) {
            var percent = Math.floor(result.RunningTablets * 100 / result.TotalTablets) + '%';
            var values = result.RunningTablets + ' of ' + result.TotalTablets;
            var warmup = result.WarmUp ? "<span class='glyphicon glyphicon-fire' style='color:red; margin-right:4px'></span>" : "";
            $('#runningTablets').html(warmup + percent + ' (' + values + ')');
            $('#aliveNodes').html(result.AliveNodes);
            $('#bootQueue').html(result.BootQueueSize);
            $('#waitQueue').html(result.WaitQueueSize);
            $('#maxUsage').html(result.MaxUsage);
            $('#objectImbalance').html(result.ObjectImbalance);
            $('#storageScatter').html(result.StorageScatter);
            if (should_refresh_types) {
                $('#types').html(result.Types);
                should_refresh_types = false;
            }

            $('#resourceTotalCounter').html(result.ResourceTotal.Counter);
            $('#resourceTotalCPU').html(result.ResourceTotal.CPU);
            $('#resourceTotalMemory').html(result.ResourceTotal.Memory);
            $('#resourceTotalNetwork').html(result.ResourceTotal.Network);

            $('#resourceStdDevCounter').html(result.ResourceVariance.Counter);
            $('#resourceStdDevCPU').html(result.ResourceVariance.CPU);
            $('#resourceStdDevMemory').html(result.ResourceVariance.Memory);
            $('#resourceStdDevNetwork').html(result.ResourceVariance.Network);

            $('#resourceScatterCounter').html(result.ScatterHtml.Counter);
            $('#resourceScatterCPU').html(result.ScatterHtml.CPU);
            $('#resourceScatterMemory').html(result.ScatterHtml.Memory);
            $('#resourceScatterNetwork').html(result.ScatterHtml.Network);

            for (var b = 0; b < result.Balancers.length; b++) {
                var balancerObj = result.Balancers[b];
                var balancerHtml = $('#balancer' + b)[0];
                balancerHtml.cells[1].innerHTML = balancerObj.TotalRuns;
                balancerHtml.cells[2].innerHTML = balancerObj.TotalMovements;
                if (balancerObj.TotalRuns > 0) {
                    balancerHtml.cells[3].innerHTML = balancerObj.LastRunTimestamp;
                    balancerHtml.cells[4].innerHTML = balancerObj.LastRunMovements;
                } else {
                    balancerHtml.cells[3].innerHTML = '';
                    balancerHtml.cells[4].innerHTML = '';
                }
                if (balancerObj.IsRunningNow && balancerObj.CurrentMaxMovements > 0) {
                    balancerHtml.cells[5].innerHTML = Math.floor(balancerObj.CurrentMovements * 100 / balancerObj.CurrentMaxMovements) + '%';
                } else {
                    balancerHtml.cells[5].innerHTML = '';
                }
            }
        }
        clearAlert();
    }
    catch(err) {
        toggleAlert();
    }
}

function onFreshDataShort(result) {
    fillDataShort(result);
    setTimeout(function(){updateDataShort();}, 500);
}

function onFreshDataLong(result) {
    var nlen;
    try {
        fillDataShort(result);
        if ("Nodes" in result) {
            $('#move_history > tbody > tr').remove();
            for (var i in result.Moves) {
                $(result.Moves[i]).appendTo('#move_history > tbody');
            }
            var old_nodes = {};
            if (Empty) {
                // initialization
                $('#node_table > tbody > tr').remove();
                Empty = false;
            } else {
                for (var id in Nodes) {
                    old_nodes[id] = true;
                }
            }
            var was_append = false;
            nlen = result.Nodes.length;
            for (i = 0; i < nlen; i++) {
                var node = result.Nodes[i];
                var old_node = Nodes[node.Id];
                var nodeElement = $('#node' + node.Id).get(0);
                var nodeElement;
                if (old_node) {
                    nodeElement = old_node.NodeElement;
                } else {
                    nodeElement = $('<tr id="node' + node.Id + '"><td>' + node.Id + '</td>'
                        + '<td></td>'
                        + '<td></td>'
                        + '<td></td>'
                        + '<td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td>'
                        + '<td style="text-align:center"><span title="Toggle node availability" onclick="toggleDown(this,' + node.Id + ')" style="cursor:pointer" class="active-mark glyphicon glyphicon-ok"></span></td>'
                        + '<td style="text-align:center"><span title="Toggle node freeze" onclick="toggleFreeze(this,' + node.Id + ')" style="cursor:pointer" class="glyphicon glyphicon-play"></span></td>'
                        + '<td style="text-align:center"><span title="Kick tablets on this node" onclick="kickNode(this,' + node.Id + ')" style="cursor:pointer" class="glyphicon glyphicon-transfer"></span></td>'
                        + '<td style="text-align:center"><span title="Drain this node" onclick="drainNode(this,' + node.Id + ')" style="cursor:pointer" class="glyphicon glyphicon-log-out"></span></td>'
                        + '</tr>').appendTo('#node_table > tbody').get(0);
                    nodeElement.cells[1].innerHTML = '<a href="' + node.Host + ':8765">' + node.Name + '</a>';
                    nodeElement.cells[2].innerHTML = node.DataCenter;
                    was_append = true;
                }
                delete old_nodes[node.Id];
                if (!old_node || old_node.Alive != node.Alive) {
                    if (node.Alive) {
                        nodeElement.style.color = 'initial';
                    } else {
                        nodeElement.style.color = '#E0E0E0';
                    }
                }
                var element = $(nodeElement.cells[14].children[0]);
                if (!element.hasOwnProperty("inProgress") || !element.inProgress) {
                    if (!old_node || old_node.Down != node.Down) {
                        if (node.Down) {
                            element.removeClass('glyphicon-ok');
                            element.addClass('glyphicon-remove');
                        } else {
                            element.removeClass('glyphicon-remove');
                            element.addClass('glyphicon-ok');
                        }
                    }
                }
                element = $(nodeElement.cells[15].children[0]);
                if (!element.hasOwnProperty("inProgress") || !element.inProgress) {
                    if (!old_node || old_node.Freeze != node.Freeze) {
                        if (node.Freeze) {
                            element.removeClass('glyphicon-play');
                            element.addClass('glyphicon-pause');
                        } else {
                            element.removeClass('glyphicon-pause');
                            element.addClass('glyphicon-play');
                        }
                    }
                }
                element = $(nodeElement.cells[17].children[0]);
                if (!element.hasOwnProperty("inProgress") || !element.inProgress) {
                    if (!old_node || old_node.Drain != node.Drain) {
                        if (node.Drain) {
                            element.addClass('blinking');
                        } else {
                            element.removeClass('blinking');
                        }
                    }
                }
                if (!old_node || old_node.Name != node.Name) {
                    nodeElement.cells[1].innerHTML = '<a href="' + node.Host + ':8765">' + node.Name + '</a>';
                }
                if (!old_node || old_node.DataCenter != node.DataCenter) {
                    nodeElement.cells[2].innerHTML = node.DataCenter;
                }
                if (!old_node || old_node.Domain != node.Domain) {
                    nodeElement.cells[3].innerHTML = node.Domain;
                }
                if (!old_node || old_node.Uptime != node.Uptime) {
                    nodeElement.cells[4].innerHTML = node.Uptime;
                }
                if (!old_node || old_node.Unknown != node.Unknown) {
                    nodeElement.cells[5].innerHTML = node.Unknown;
                }
                if (!old_node || old_node.Starting != node.Starting) {
                    nodeElement.cells[6].innerHTML = node.Starting;
                }
                if (!old_node || old_node.Running != node.Running) {
                    nodeElement.cells[7].innerHTML = node.Running;
                }
                if (!old_node || old_node.Types != node.Types) {
                    nodeElement.cells[8].innerHTML = node.Types;
                }
                if (!old_node || old_node.Usage != node.Usage) {
                    nodeElement.cells[9].innerHTML = node.Usage;
                }
                if (!old_node || old_node.ResourceValues[0] != node.ResourceValues[0]) {
                    nodeElement.cells[10].innerHTML = node.ResourceValues[0];
                }
                if (!old_node || old_node.ResourceValues[1] != node.ResourceValues[1]) {
                    nodeElement.cells[11].innerHTML = node.ResourceValues[1];
                }
                if (!old_node || old_node.ResourceValues[2] != node.ResourceValues[2]) {
                    nodeElement.cells[12].innerHTML = node.ResourceValues[2];
                }
                if (!old_node || old_node.ResourceValues[3] != node.ResourceValues[3]) {
                    nodeElement.cells[13].innerHTML = node.ResourceValues[3];
                }
                node.NodeElement = nodeElement;
                Nodes[node.Id] = node;
            }
            for (var id in old_nodes) {
                $('#node' + id).remove();
                delete Nodes[id];
            }
            if (was_append) {
                $('#node_table > tbody > tr').sort(function(a,b) {
                    if (a.cells[3].innerHTML > b.cells[3].innerHTML)
                        return 1;
                    if (a.cells[3].innerHTML < b.cells[3].innerHTML)
                        return -1;
                    return parseInt(a.cells[0].innerHTML, 10) - parseInt(b.cells[0].innerHTML, 10);
                }).appendTo('#node_table > tbody');
            }
        }
        clearAlert();
    }
    catch(err) {
        toggleAlert();
    }
    setTimeout(function(){updateDataLong();}, 500 + nlen * 10);
}

var switchToLong = false;

function updateDataShort() {
    if (switchToLong) {
        updateDataLong();
        return;
    }
    $.ajax({url:'app?TabletID=' + hiveId + '&page=LandingData',
        success: function(result){ onFreshDataShort(result); },
        error: function(){ toggleAlert(); setTimeout(updateDataShort, 1000); }
    });
}

function updateDataLong() {
    $.ajax({url:'app?TabletID=' + hiveId + '&page=LandingData&nodes=1&moves=1',
        success: function(result){ onFreshDataLong(result); },
        error: function(){ toggleAlert(); setTimeout(updateDataLong, 1000); }
    });
}

function updateData() {
    switchToLong = true;
}

updateDataShort();


            )___";
        out << "</script>";
        out << "</body>";
    }
};

class TTxMonEvent_LandingData : public TTransactionBase<THive> {
public:
    const TActorId Source;
    TAutoPtr<NMon::TEvRemoteHttpInfo> Event;
    TCgiParameters Cgi;

    TTxMonEvent_LandingData(const TActorId &source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf *hive)
        : TBase(hive)
        , Source(source)
        , Event(ev->Release())
        , Cgi(Event->Cgi())
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_LANDING_DATA; }

    bool Execute(TTransactionContext &txc, const TActorContext& ctx) override {
        Y_UNUSED(txc);
        TStringStream str;
        RenderJSONPage(str);
        ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes(str.Str()));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Y_UNUSED(ctx);
    }

    struct TTabletsRunningInfo {
        TNodeId NodeId;
        TTabletTypes::EType TabletType;
        ui64 LeaderCount = 0;
        ui64 FollowerCount = 0;
        ui64 MaxCount = 0;

        TTabletsRunningInfo(TNodeId node, TTabletTypes::EType tabletType) : NodeId(node)
                                                                          , TabletType(tabletType)
        {
        }

        TString ToHTML() const {
            auto totalCount = LeaderCount + FollowerCount;
            TStringBuilder str;
            if (MaxCount > 0) {
                str << "<span class='box' ";
            } else {
                str << "<span class='box box-disabled' ";
            }
            if (totalCount > MaxCount) {
                str << " style='color: red' ";
            }
            str << " onclick='"  << (MaxCount == 0 ? "enableType" : "disableType")
                << "(this," << NodeId << "," << (ui32)TabletType << ")";
            str << "'>";
            str << GetTabletTypeShortName(TabletType);
            str << " ";
            str << LeaderCount;
            if (FollowerCount > 0) {
                str << " (" << FollowerCount << ")";
            }
            str << "</span>";
            return str;
        }
    };

    void RenderJSONPage(IOutputStream &out) {
        ui64 nodes = 0;
        ui64 tablets = 0;
        ui64 runningTablets = 0;
        ui64 aliveNodes = 0;
        THashMap<ui32, TVector<TTabletsRunningInfo>> tabletsByNodeByType;

        for (const auto& pr : Self->Tablets) {
            ++tablets;
            if (pr.second.IsRunning()) {
                ++runningTablets;
            }
            for (const auto& follower : pr.second.Followers) {
                ++tablets;
                if (follower.IsRunning()) {
                    ++runningTablets;
                }
            }
        }

        for (const auto& pr : Self->Nodes) {
            if (pr.second.IsAlive()) {
                ++aliveNodes;
            }
            if (!pr.second.IsUnknown()) {
                ++nodes;
            }
            auto& tabletsByType = tabletsByNodeByType[pr.first];
            tabletsByType.reserve(Self->SeenTabletTypes.size());
            for (auto tabletType : Self->SeenTabletTypes) {
                tabletsByType.emplace_back(pr.first, tabletType);
                auto& current = tabletsByType.back();
                current.MaxCount = pr.second.GetMaxCountForTabletType(tabletType);
                auto tabletsRunningIt = pr.second.TabletsRunningByType.find(tabletType);
                if (tabletsRunningIt == pr.second.TabletsRunningByType.end()) {
                    continue;
                }
                for (const auto* tablet : tabletsRunningIt->second) {
                    if (tablet == nullptr) {
                        continue;
                    }
                    if (tablet->IsLeader()) {
                        ++current.LeaderCount;
                    } else {
                        ++current.FollowerCount;
                    }
                }
            }
        }

        NJson::TJsonValue jsonData;
        THive::THiveStats stats = Self->GetStats();

        jsonData["TotalTablets"] = tablets;
        jsonData["RunningTablets"] = runningTablets;
        jsonData["TotalNodes"] = nodes;
        jsonData["AliveNodes"] = aliveNodes;
        jsonData["ResourceTotal"] = GetResourceValuesJson(Self->TotalRawResourceValues);
        jsonData["ResourceVariance"] = GetResourceValuesJson(Self->GetStDevResourceValues());
        jsonData["BootQueueSize"] = Self->BootQueue.BootQueue.size();
        jsonData["WaitQueueSize"] = Self->BootQueue.WaitQueue.size();
        jsonData["Balancers"] = Self->GetBalancerProgressJson();
        jsonData["MaxUsage"] =  GetValueWithColoredGlyph(stats.MaxUsage, Self->GetMaxNodeUsageToKick()) ;
        auto scatterHtml = convert(stats.ScatterByResource, Self->GetMinScatterToBalance(), GetValueWithColoredGlyph);
        jsonData["ScatterHtml"]["Counter"] = std::get<NMetrics::EResource::Counter>(scatterHtml);
        jsonData["ScatterHtml"]["CPU"] = std::get<NMetrics::EResource::CPU>(scatterHtml);
        jsonData["ScatterHtml"]["Memory"] = std::get<NMetrics::EResource::Memory>(scatterHtml);
        jsonData["ScatterHtml"]["Network"] = std::get<NMetrics::EResource::Network>(scatterHtml);
        jsonData["ObjectImbalance"] = GetValueWithColoredGlyph(Self->ObjectDistributions.GetMaxImbalance(), Self->GetObjectImbalanceToBalance());
        jsonData["StorageScatter"] = GetValueWithColoredGlyph(Self->StorageScatter, Self->GetMinStorageScatterToBalance());
        jsonData["WarmUp"] = Self->WarmUp;
        jsonData["Types"] = GetTypesHtml(Self->SeenTabletTypes, Self->GetTabletLimit());

        if (Cgi.Get("nodes") == "1") {
            TVector<TNodeInfo*> nodeInfos;
            nodeInfos.reserve(Self->Nodes.size());
            for (auto& pr : Self->Nodes) {
                if (!pr.second.IsUnknown()) {
                    nodeInfos.push_back(&pr.second);
                }
            }
            std::sort(nodeInfos.begin(), nodeInfos.end(), [](TNodeInfo* a, TNodeInfo* b) -> bool {
                return std::make_tuple(a->ServicedDomains, a->Id) < std::make_tuple(b->ServicedDomains, b->Id);
            });

            TInstant aliveLine = TInstant::Now() - TDuration::Minutes(10);

            NJson::TJsonValue& jsonNodes = jsonData["Nodes"];
            for (TNodeInfo* nodeInfo : nodeInfos) {
                TNodeInfo& node = *nodeInfo;
                TNodeId id = node.Id;

                if (!node.IsAlive() && TInstant::MilliSeconds(node.Statistics.GetLastAliveTimestamp()) < aliveLine) {
                    continue;
                }

                NJson::TJsonValue& jsonNode = jsonNodes.AppendValue(NJson::TJsonValue());
                TString name = "";
                TString host;
                auto it = Self->NodesInfo.find(node.Id);
                if (it != Self->NodesInfo.end()) {
                    auto &ni = it->second;
                    if (ni.Host.empty()) {
                        name = ni.Address + ":" + ToString(ni.Port);
                        host = ni.Address;
                    } else {
                        name = ni.Host.substr(0, ni.Host.find('.')) + ":" + ToString(ni.Port);
                        host = ni.Host;
                    }
                }

                jsonNode["Id"] = id;
                jsonNode["Host"] = host;
                jsonNode["Name"] = name;
                if (node.LocationAcquired) {
                    jsonNode["DataCenter"] = node.Location.GetDataCenterId();
                }
                jsonNode["Domain"] = node.ServicedDomains.empty() ? "" : Self->GetDomainName(node.GetServicedDomain());
                jsonNode["Alive"] = node.IsAlive();
                jsonNode["Down"] = node.Down;
                jsonNode["Freeze"] = node.Freeze;
                jsonNode["Drain"] = node.IsAlive() ? node.Drain : false;
                jsonNode["Uptime"] = node.IsAlive() ? GetDurationString(node.GetUptime()) : "";
                jsonNode["Unknown"] = node.Tablets[TTabletInfo::EVolatileState::TABLET_VOLATILE_STATE_UNKNOWN].size();
                jsonNode["Starting"] = node.Tablets[TTabletInfo::EVolatileState::TABLET_VOLATILE_STATE_STARTING].size();
                jsonNode["Running"] = node.Tablets[TTabletInfo::EVolatileState::TABLET_VOLATILE_STATE_RUNNING].size();
                {
                    TString types;
                    auto nodeTabletTypes = tabletsByNodeByType.find(node.Id);
                    if (nodeTabletTypes != tabletsByNodeByType.end()) {
                        for (auto it = nodeTabletTypes->second.begin(); it != nodeTabletTypes->second.end(); ++it) {
                            if (!types.empty()) {
                                types += ' ';
                            }
                            types += it->ToHTML();
                        }
                    }
                    jsonNode["Types"] = types;
                }
                double nodeUsage = node.GetNodeUsage();
                jsonNode["Usage"] = GetConditionalRedString(Sprintf("%.3f", nodeUsage), nodeUsage >= 1);
                jsonNode["ResourceValues"] = GetResourceValuesJson(node.ResourceValues, node.ResourceMaximumValues);
                jsonNode["StDevResourceValues"] = GetResourceValuesText(node.GetStDevResourceValues());
            }
        }
        if (Cgi.Get("moves") == "1") {
            NJson::TJsonValue& moves = jsonData["Moves"];
            if (Self->TabletMoveHistory.TotalSize()) {
                for (int i = Self->TabletMoveHistory.TotalSize() - 1; i >= (int)Self->TabletMoveHistory.FirstIndex(); --i) {
                    moves.AppendValue(Self->TabletMoveHistory[i].ToHTML());
                }
            }
        }
        NJson::WriteJson(&out, &jsonData);
    }
};

class TTxMonEvent_SetDown : public TTransactionBase<THive>, public TLoggedMonTransaction {
public:
    const TActorId Source;
    const TNodeId NodeId;
    const bool Down;
    TString Response;

    TTxMonEvent_SetDown(const TActorId& source, TNodeId nodeId, bool down, TSelf* hive, NMon::TEvRemoteHttpInfo::TPtr& ev)
        : TBase(hive)
        , TLoggedMonTransaction(ev, hive)
        , Source(source)
        , NodeId(nodeId)
        , Down(down)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_SET_DOWN; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        TNodeInfo* node = Self->FindNode(NodeId);
        if (node != nullptr) {
            node->SetDown(Down);
            db.Table<Schema::Node>().Key(NodeId).Update(NIceDb::TUpdate<Schema::Node::Down>(Down));
            NJson::TJsonValue jsonOperation;
            jsonOperation["NodeId"] = NodeId;
            jsonOperation["Down"] = Down;
            WriteOperation(db, jsonOperation);
            Response = "{\"NodeId\":" + ToString(NodeId) + ',' + "\"Down\":" + (Down ? "true" : "false") + "}";
        } else {
            Response = "{\"Error\":\"Node " + ToString(NodeId) + " not found\"}";
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxMonEvent_SetDown(" << NodeId << ")::Complete Response=" << Response);
        ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes(Response));
    }
};

class TTxMonEvent_SetFreeze : public TTransactionBase<THive>, public TLoggedMonTransaction {
public:
    const TActorId Source;
    const TNodeId NodeId;
    const bool Freeze;
    TString Response;

    TTxMonEvent_SetFreeze(const TActorId& source, TNodeId nodeId, bool freeze, TSelf* hive, NMon::TEvRemoteHttpInfo::TPtr& ev)
        : TBase(hive)
        , TLoggedMonTransaction(ev, hive)
        , Source(source)
        , NodeId(nodeId)
        , Freeze(freeze)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_SET_FREEZE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        TNodeInfo* node = Self->FindNode(NodeId);
        if (node != nullptr) {
            node->SetFreeze(Freeze);
            db.Table<Schema::Node>().Key(NodeId).Update(NIceDb::TUpdate<Schema::Node::Freeze>(Freeze));
            NJson::TJsonValue jsonOperation;
            jsonOperation["NodeId"] = NodeId;
            jsonOperation["Freeze"] = Freeze;
            WriteOperation(db, jsonOperation);
            Response = "{\"NodeId\":" + ToString(NodeId) + ',' + "\"Freeze\":" + (Freeze ? "true" : "false") + "}";
        } else {
            Response = "{\"Error\":\"Node " + ToString(NodeId) + " not found\"}";
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxMonEvent_SetFreeze(" << NodeId << ")::Complete Response=" << Response);
        ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes(Response));
    }
};

class TTxMonEvent_KickNode : public TTransactionBase<THive> {
public:
    const TActorId Source;
    const TNodeId NodeId;
    TString Response;

    TTxMonEvent_KickNode(const TActorId& source, TNodeId nodeId, TSelf* hive)
        : TBase(hive)
        , Source(source)
        , NodeId(nodeId)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_KICK_NODE; }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        TNodeInfo* node = Self->FindNode(NodeId);
        if (node != nullptr) {
            int kicked = 0;
            for (TTabletInfo* tablet : node->Tablets[TTabletInfo::EVolatileState::TABLET_VOLATILE_STATE_RUNNING]) {
                tablet->Kick();
                ++kicked;
            }
            Response = "{\"TabletsKicked\":" + ToString(kicked) + "}";
        } else {
            Response = "{\"Error\":\"Node " + ToString(NodeId) + " not found\"}";
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxMonEvent_KickNode(" << NodeId << ")::Complete Response=" << Response);
        ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes(Response));
    }
};

class TDrainNodeWaitActor : public TActor<TDrainNodeWaitActor>, public ISubActor {
public:
    TActorId Source;
    THive* Hive;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HIVE_MON_REQUEST;
    }

    TDrainNodeWaitActor(const TActorId& source, THive* hive)
        : TActor(&TDrainNodeWaitActor::StateWork)
        , Source(source)
        , Hive(hive)
    {}

    void PassAway() override {
        Hive->RemoveSubActor(this);
        return IActor::PassAway();
    }

    void Cleanup() override {
        PassAway();
    }

    TSubActorId GetId() const override {
        return SelfId().LocalId();
    }

    void Handle(TEvHive::TEvDrainNodeResult::TPtr& result) {
        Send(Source, new NMon::TEvRemoteJsonInfoRes(
            TStringBuilder() << "{\"status\":\"" << NKikimrProto::EReplyStatus_Name(result->Get()->Record.GetStatus()) << "\","
                             << "\"movements\":" << result->Get()->Record.GetMovements() << "}"));
        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
            hFunc(TEvHive::TEvDrainNodeResult, Handle);
        }
    }
};

class TTxMonEvent_DrainNode : public TTransactionBase<THive> {
public:
    TAutoPtr<NMon::TEvRemoteHttpInfo> Event;
    TActorId Source;
    TNodeId NodeId = 0;
    bool Wait = true;
    TActorId WaitActorId;

    TTxMonEvent_DrainNode(const TActorId& source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf* hive)
        : TBase(hive)
        , Event(ev->Release())
        , Source(source)
    {
        NodeId = FromStringWithDefault<TNodeId>(Event->Cgi().Get("node"), NodeId);
        Wait = FromStringWithDefault(Event->Cgi().Get("wait"), Wait);
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_DRAIN_NODE; }

    bool Execute(TTransactionContext&, const TActorContext& ctx) override {
        TActorId waitActorId;
        TDrainNodeWaitActor* waitActor = nullptr;
        if (Wait) {
            waitActor = new TDrainNodeWaitActor(Source, Self);
            WaitActorId = ctx.RegisterWithSameMailbox(waitActor);
            Self->SubActors.emplace_back(waitActor);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Wait) {
            Self->Execute(Self->CreateSwitchDrainOn(NodeId, {}, WaitActorId));
        } else {
            Self->Execute(Self->CreateSwitchDrainOn(NodeId, {}, {}));
            ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes("{\"status\":\"SCHEDULED\"}"));
        }
    }
};

class TTxMonEvent_Rebalance : public TTransactionBase<THive> {
public:
    const TActorId Source;
    int MaxMovements = 1000;

    TTxMonEvent_Rebalance(const TActorId& source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf* hive)
        : TBase(hive)
        , Source(source)
    {
        MaxMovements = FromStringWithDefault(ev->Get()->Cgi().Get("movements"), MaxMovements);
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_REBALANCE; }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        Self->StartHiveBalancer({
            .Type = EBalancerType::Manual,
            .MaxMovements = MaxMovements
        });
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes("{}"));
    }
};

class TTxMonEvent_StorageRebalance : public TTransactionBase<THive> {
public:
    const TActorId Source;
    TStorageBalancerSettings Settings;

    TTxMonEvent_StorageRebalance(const TActorId& source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf* hive)
        : TBase(hive)
        , Source(source)
    {
        Settings.NumReassigns = FromStringWithDefault(ev->Get()->Cgi().Get("reassigns"), 1000);
        Settings.MaxInFlight = FromStringWithDefault(ev->Get()->Cgi().Get("inflight"), 1);
        Settings.StoragePool = ev->Get()->Cgi().Get("pool");
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_REBALANCE; }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        Self->StartHiveStorageBalancer(Settings);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes("{}"));
    }
};

class TTxMonEvent_RebalanceFromScratch : public TTransactionBase<THive> {
public:
    const TActorId Source;
    TString TenantName;

    TTxMonEvent_RebalanceFromScratch(const TActorId& source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf* hive)
        : TBase(hive)
        , Source(source)
    {
        TenantName = ev->Get()->Cgi().Get("tenantName");
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_REBALANCE_FROM_SCRATCH; }

    bool ValidateTenantName() {
        auto domainId = Self->GetMySubDomainKey();
        auto* domainInfo = Self->FindDomain(domainId);
        if (!domainInfo || !domainInfo->Path) {
            // In the non-normal case of not knowing our domain, allow anything
            return true;
        }
        return TenantName == domainInfo->Path;
    }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        if (!ValidateTenantName()) {
            return true;
        }
        for (const auto& tablet : Self->Tablets) {
            Self->Execute(Self->CreateRestartTablet(tablet.second.GetFullTabletId()));
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes("{}"));
    }
};

class TReassignTabletWaitActor : public TActor<TReassignTabletWaitActor>, public ISubActor {
public:
    TActorId Source;
    ui32 TabletsTotal = std::numeric_limits<ui32>::max();
    ui32 TabletsDone = 0;
    THive* Hive;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HIVE_MON_REQUEST;
    }

    TReassignTabletWaitActor(const TActorId& source, THive* hive)
        : TActor(&TReassignTabletWaitActor::StateWork)
        , Source(source)
        , Hive(hive)
    {}

    void PassAway() override {
        Hive->RemoveSubActor(this);
        return IActor::PassAway();
    }

    void Cleanup() override {
        PassAway();
    }

    TSubActorId GetId() const override {
        return SelfId().LocalId();
    }

    void Handle(TEvPrivate::TEvRestartComplete::TPtr&) {
        ++TabletsDone;
        if (TabletsDone >= TabletsTotal) {
            Send(Source, new NMon::TEvRemoteJsonInfoRes(TStringBuilder() << "{\"total\":" << TabletsDone << "}"));
            PassAway();
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
            hFunc(TEvPrivate::TEvRestartComplete, Handle);
        }
    }
};

class TTxMonEvent_ReassignTablet : public TTransactionBase<THive> {
public:
    TAutoPtr<NMon::TEvRemoteHttpInfo> Event;
    const TActorId Source;
    TTabletId TabletId = 0;
    TTabletTypes::EType TabletType = TTabletTypes::TypeInvalid;
    TVector<ui32> TabletChannels;
    ui32 GroupId = 0;
    TVector<ui32> ForcedGroupIds;
    int TabletPercent = 100;
    TString Error;
    bool Wait = true;

    TTxMonEvent_ReassignTablet(const TActorId& source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf* hive)
        : TBase(hive)
        , Event(ev->Release())
        , Source(source)
    {
        TabletId = FromStringWithDefault<TTabletId>(Event->Cgi().Get("tablet"), TabletId);
        TabletType = (TTabletTypes::EType)FromStringWithDefault<int>(Event->Cgi().Get("type"), TabletType);
        TabletChannels = Scan<ui32>(SplitString(Event->Cgi().Get("channel"), ","));
        TabletPercent = FromStringWithDefault<int>(Event->Cgi().Get("percent"), TabletPercent);
        GroupId = FromStringWithDefault(Event->Cgi().Get("group"), GroupId);
        ForcedGroupIds = Scan<ui32>(SplitString(Event->Cgi().Get("forcedGroup"), ","));
        TabletPercent = std::min(std::abs(TabletPercent), 100);
        Wait = FromStringWithDefault(Event->Cgi().Get("wait"), Wait);
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_REASSIGN_TABLET; }

    TInstant GetMaxTimestamp(const TLeaderTabletInfo* tablet) const {
        TInstant max;
        for (const auto& channel : tablet->TabletStorageInfo->Channels) {
            if (TabletChannels.empty()
                    || std::find(
                        TabletChannels.begin(),
                        TabletChannels.end(),
                        channel.Channel) != TabletChannels.end()) {
                const auto* latest = channel.LatestEntry();
                if (latest != nullptr && latest->Timestamp > max) {
                    max = latest->Timestamp;
                }
            }
        }
        return max;
    }

    bool Execute(TTransactionContext&, const TActorContext& ctx) override {
        if (!ForcedGroupIds.empty() && ForcedGroupIds.size() != TabletChannels.size()) {
            Error = "forcedGroup size should be equal to channel size";
            return true;
        }
        TVector<TLeaderTabletInfo*> tablets;
        if (TabletId != 0) {
            TLeaderTabletInfo* tablet = Self->FindTablet(TabletId);
            if (tablet != nullptr) {
                tablets.push_back(tablet);
            }
        } else if (TabletType != TTabletTypes::TypeInvalid) {
            for (auto& pr : Self->Tablets) {
                if (pr.second.Type == TabletType) {
                    tablets.push_back(&pr.second);
                }
            }
        } else {
            for (auto& pr : Self->Tablets) {
                tablets.push_back(&pr.second);
            }
        }
        if (TabletPercent != 100) {
            std::sort(tablets.begin(), tablets.end(), [this](TLeaderTabletInfo* a, TLeaderTabletInfo* b) -> bool {
                return GetMaxTimestamp(a) < GetMaxTimestamp(b);
            });
            tablets.resize(tablets.size() * TabletPercent / 100);
        }
        TVector<THolder<TEvHive::TEvReassignTablet>> operations;
        TActorId waitActorId;
        TReassignTabletWaitActor* waitActor = nullptr;
        if (Wait) {
            waitActor = new TReassignTabletWaitActor(Source, Self);
            waitActorId = ctx.RegisterWithSameMailbox(waitActor);
            Self->SubActors.emplace_back(waitActor);
        }
        for (TLeaderTabletInfo* tablet : tablets) {
            TVector<ui32> channels;
            TVector<ui32> forcedGroupIds;
            bool skip = false;
            if (GroupId != 0) {
                skip = true;
                for (const auto& channel : tablet->TabletStorageInfo->Channels) {
                    if (TabletChannels.empty() || Find(TabletChannels, channel.Channel) != TabletChannels.end()) {
                        const auto* latest = channel.LatestEntry();
                        if (latest != nullptr && latest->GroupID == GroupId) {
                            skip = false;
                            channels.push_back(channel.Channel);
                            if (!ForcedGroupIds.empty()) {
                                auto itTabletChannel = Find(TabletChannels, channel.Channel);
                                forcedGroupIds.push_back(ForcedGroupIds[std::distance(TabletChannels.begin(), itTabletChannel)]);
                            }
                        }
                    }
                }
            } else {
                channels = TabletChannels;
                forcedGroupIds = ForcedGroupIds;
            }
            if (skip) {
                continue;
            }
            if (Wait) {
                tablet->ActorsToNotifyOnRestart.emplace_back(waitActorId); // volatile settings, will not persist upon restart
            }
            operations.emplace_back(new TEvHive::TEvReassignTablet(tablet->Id, channels, forcedGroupIds));
        }
        if (Wait) {
            waitActor->TabletsTotal = operations.size();
        }
        for (auto& op : operations) {
            ctx.Send(Self->SelfId(), op.Release());
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Error) {
            ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes(TStringBuilder() << "{\"error\":\"" << Error << "\"}"));
        } else {
            if (!Wait) {
                ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes("{}"));
            }
        }
    }
};

class TInitMigrationWaitActor : public TActor<TInitMigrationWaitActor>, public ISubActor {
public:
    TActorId Source;
    THive* Hive;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HIVE_MON_REQUEST;
    }

    TInitMigrationWaitActor(const TActorId& source, THive* hive)
        : TActor(&TInitMigrationWaitActor::StateWork)
        , Source(source)
        , Hive(hive)
    {}

    void PassAway() override {
        Hive->RemoveSubActor(this);
        return IActor::PassAway();
    }

    void Cleanup() override {
        PassAway();
    }

    TSubActorId GetId() const override {
        return SelfId().LocalId();
    }

    void Handle(TEvHive::TEvInitMigrationReply::TPtr& reply) {
        TStringBuilder output;
        NProtobufJson::TProto2JsonConfig config;
        config.SetEnumMode(NProtobufJson::TProto2JsonConfig::EnumName);
        NProtobufJson::Proto2Json(reply->Get()->Record, output, config);
        Send(Source, new NMon::TEvRemoteJsonInfoRes(output));
        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
            hFunc(TEvHive::TEvInitMigrationReply, Handle);
        }
    }
};

class TTxMonEvent_InitMigration : public TTransactionBase<THive> {
public:
    TAutoPtr<NMon::TEvRemoteHttpInfo> Event;
    const TActorId Source;
    bool Wait = true;

    TTxMonEvent_InitMigration(const TActorId& source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf* hive)
        : TBase(hive)
        , Event(ev->Release())
        , Source(source)
    {
        Wait = FromStringWithDefault(Event->Cgi().Get("wait"), Wait);
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_INIT_MIGRATION; }

    bool Execute(TTransactionContext&, const TActorContext& ctx) override {
        TActorId waitActorId;
        TInitMigrationWaitActor* waitActor = nullptr;
        if (Wait) {
            waitActor = new TInitMigrationWaitActor(Source, Self);
            waitActorId = ctx.RegisterWithSameMailbox(waitActor);
            Self->SubActors.emplace_back(waitActor);
        }
        // TODO: pass arguments as post data json
        ctx.Send(new IEventHandle(Self->SelfId(), waitActorId, new TEvHive::TEvInitMigration()));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (!Wait) {
            ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes("{}"));
        }
    }
};

class TQueryMigrationWaitActor : public TActor<TQueryMigrationWaitActor>, public ISubActor {
public:
    TActorId Source;
    THive* Hive;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HIVE_MON_REQUEST;
    }

    TQueryMigrationWaitActor(const TActorId& source, THive* hive)
        : TActor(&TQueryMigrationWaitActor::StateWork)
        , Source(source)
        , Hive(hive)
    {}

    void PassAway() override {
        Hive->RemoveSubActor(this);
        return IActor::PassAway();
    }

    void Cleanup() override {
        PassAway();
    }

    TSubActorId GetId() const override {
        return SelfId().LocalId();
    }

    void Handle(TEvHive::TEvQueryMigrationReply::TPtr& reply) {
        TStringBuilder output;
        NProtobufJson::TProto2JsonConfig config;
        config.SetEnumMode(NProtobufJson::TProto2JsonConfig::EnumName);
        NProtobufJson::Proto2Json(reply->Get()->Record, output, config);
        Send(Source, new NMon::TEvRemoteJsonInfoRes(output));
        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
            hFunc(TEvHive::TEvQueryMigrationReply, Handle);
        }
    }
};

class TTxMonEvent_QueryMigration : public TTransactionBase<THive> {
public:
    TAutoPtr<NMon::TEvRemoteHttpInfo> Event;
    const TActorId Source;

    TTxMonEvent_QueryMigration(const TActorId& source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf* hive)
        : TBase(hive)
        , Event(ev->Release())
        , Source(source)
    {
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_QUERY_MIGRATION; }

    bool Execute(TTransactionContext&, const TActorContext& ctx) override {
        TActorId waitActorId;
        TQueryMigrationWaitActor* waitActor = nullptr;
        waitActor = new TQueryMigrationWaitActor(Source, Self);
        waitActorId = ctx.RegisterWithSameMailbox(waitActor);
        Self->SubActors.emplace_back(waitActor);
        ctx.Send(new IEventHandle(Self->SelfId(), waitActorId, new TEvHive::TEvQueryMigration()));
        return true;
    }

    void Complete(const TActorContext&) override {
    }
};

class TMoveTabletWaitActor : public TActor<TMoveTabletWaitActor>, public ISubActor {
public:
    TActorId Source;
    THive* Hive;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HIVE_MON_REQUEST;
    }

    TMoveTabletWaitActor(const TActorId& source, THive* hive)
        : TActor(&TMoveTabletWaitActor::StateWork)
        , Source(source)
        , Hive(hive)
    {}

    void PassAway() override {
        Hive->RemoveSubActor(this);
        return IActor::PassAway();
    }

    void Cleanup() override {
        PassAway();
    }

    TSubActorId GetId() const override {
        return SelfId().LocalId();
    }

    void Handle(TEvPrivate::TEvRestartComplete::TPtr& result) {
        NJson::TJsonValue response;
        response["status"] = result->Get()->Status;
        Send(Source, new NMon::TEvRemoteJsonInfoRes(NJson::WriteJson(response, false)));
        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
            hFunc(TEvPrivate::TEvRestartComplete, Handle);
        }
    }
};

class TTxMonEvent_MoveTablet : public TTransactionBase<THive> {
public:
    TAutoPtr<NMon::TEvRemoteHttpInfo> Event;
    const TActorId Source;
    TTabletId TabletId = 0;
    TNodeId NodeId = 0;
    bool Wait = true;

    TTxMonEvent_MoveTablet(const TActorId& source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf* hive)
        : TBase(hive)
        , Event(ev->Release())
        , Source(source)
    {
        TabletId = FromStringWithDefault<TTabletId>(Event->Cgi().Get("tablet"), TabletId);
        NodeId = FromStringWithDefault<TNodeId>(Event->Cgi().Get("node"), NodeId);
        Wait = FromStringWithDefault(Event->Cgi().Get("wait"), Wait);
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_MOVE_TABLET; }

    bool Execute(TTransactionContext&, const TActorContext& ctx) override {
        TLeaderTabletInfo* tablet = Self->FindTablet(TabletId);
        if (tablet == nullptr) {
            ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes(TStringBuilder() << "{\"error\":\"Tablet not found\"}"));
            return true;
        }
        TNodeInfo* node = Self->FindNode(NodeId);
        if (node == nullptr) {
            ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes(TStringBuilder() << "{\"error\":\"Node not found\"}"));
            return true;
        }
        if (Wait) {
            TMoveTabletWaitActor* waitActor = new TMoveTabletWaitActor(Source, Self);
            TActorId waitActorId = ctx.RegisterWithSameMailbox(waitActor);
            tablet->ActorsToNotifyOnRestart.emplace_back(waitActorId);
            Self->SubActors.emplace_back(waitActor);
        }
        TInstant now = TActivationContext::Now();
        tablet->MakeBalancerDecision(now);
        Self->Execute(Self->CreateRestartTablet(tablet->GetFullTabletId(), NodeId));
        if (!Wait) {
            ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes("{}"));
        }
        return true;
    }

    void Complete(const TActorContext&) override {}
};

class TStopTabletWaitActor : public TActor<TStopTabletWaitActor>, public ISubActor {
public:
    TActorId Source;
    THive* Hive;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HIVE_MON_REQUEST;
    }

    TStopTabletWaitActor(const TActorId& source, THive* hive)
        : TActor(&TStopTabletWaitActor::StateWork)
        , Source(source)
        , Hive(hive)
    {}

    void PassAway() override {
        Hive->RemoveSubActor(this);
        return IActor::PassAway();
    }

    void Cleanup() override {
        PassAway();
    }

    TSubActorId GetId() const override {
        return SelfId().LocalId();
    }

    void Handle(TEvHive::TEvStopTabletResult::TPtr& result) {
        Send(Source, new NMon::TEvRemoteJsonInfoRes(TStringBuilder() << result->Get()->Record.AsJSON()));
        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
            hFunc(TEvHive::TEvStopTabletResult, Handle);
        }
    }
};

class TTxMonEvent_StopTablet : public TTransactionBase<THive> {
public:
    TAutoPtr<NMon::TEvRemoteHttpInfo> Event;
    const TActorId Source;
    TTabletId TabletId = 0;
    bool Wait = true;

    TTxMonEvent_StopTablet(const TActorId& source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf* hive)
        : TBase(hive)
        , Event(ev->Release())
        , Source(source)
    {
        TabletId = FromStringWithDefault<TTabletId>(Event->Cgi().Get("tablet"), TabletId);
        Wait = FromStringWithDefault(Event->Cgi().Get("wait"), Wait);
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_STOP_TABLET; }

    bool Execute(TTransactionContext&, const TActorContext& ctx) override {
        TLeaderTabletInfo* tablet = Self->FindTablet(TabletId);
        if (tablet != nullptr) {
            TActorId waitActorId;
            TStopTabletWaitActor* waitActor = nullptr;
            if (Wait) {
                waitActor = new TStopTabletWaitActor(Source, Self);
                waitActorId = ctx.RegisterWithSameMailbox(waitActor);
                Self->SubActors.emplace_back(waitActor);
            }
            Self->Execute(Self->CreateStopTablet(TabletId, waitActorId));
            if (!Wait) {
                ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes("{}"));
            }
        } else {
            ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes(TStringBuilder() << "{\"error\":\"Tablet not found\"}"));
        }
        return true;
    }

    void Complete(const TActorContext&) override {}
};

class TResumeTabletWaitActor : public TActor<TResumeTabletWaitActor>, public ISubActor {
public:
    TActorId Source;
    THive* Hive;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HIVE_MON_REQUEST;
    }

    TResumeTabletWaitActor(const TActorId& source, THive* hive)
        : TActor(&TResumeTabletWaitActor::StateWork)
        , Source(source)
        , Hive(hive)
    {}

    void PassAway() override {
        Hive->RemoveSubActor(this);
        return IActor::PassAway();
    }

    void Cleanup() override {
        PassAway();
    }

    TSubActorId GetId() const override {
        return SelfId().LocalId();
    }

    void Handle(TEvHive::TEvResumeTabletResult::TPtr& result) {
        Send(Source, new NMon::TEvRemoteJsonInfoRes(TStringBuilder() << result->Get()->Record.AsJSON()));
        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
            hFunc(TEvHive::TEvResumeTabletResult, Handle);
        }
    }
};


class TTxMonEvent_ResumeTablet : public TTransactionBase<THive> {
public:
    TAutoPtr<NMon::TEvRemoteHttpInfo> Event;
    const TActorId Source;
    TTabletId TabletId = 0;
    bool Wait = true;

    TTxMonEvent_ResumeTablet(const TActorId& source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf* hive)
        : TBase(hive)
        , Event(ev->Release())
        , Source(source)
    {
        TabletId = FromStringWithDefault<TTabletId>(Event->Cgi().Get("tablet"), TabletId);
        Wait = FromStringWithDefault(Event->Cgi().Get("wait"), Wait);
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_STOP_TABLET; }

    bool Execute(TTransactionContext&, const TActorContext& ctx) override {
        TLeaderTabletInfo* tablet = Self->FindTablet(TabletId);
        if (tablet != nullptr) {
            TActorId waitActorId;
            TResumeTabletWaitActor* waitActor = nullptr;
            if (Wait) {
                waitActor = new TResumeTabletWaitActor(Source, Self);
                waitActorId = ctx.RegisterWithSameMailbox(waitActor);
                Self->SubActors.emplace_back(waitActor);
            }
            Self->Execute(Self->CreateResumeTablet(TabletId, waitActorId));
            if (!Wait) {
                ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes("{}"));
            }
        } else {
            ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes(TStringBuilder() << "{\"error\":\"Tablet not found\"}"));
        }
        return true;
    }

    void Complete(const TActorContext&) override {}
};

class TTxMonEvent_FindTablet : public TTransactionBase<THive> {
public:
    THolder<NMon::TEvRemoteHttpInfo> Event;
    const TActorId Source;
    TTabletId TabletId = 0;
    TTabletTypes::EType TabletType = TTabletTypes::TypeInvalid;
    ui32 ChannelFrom = 0;
    ui32 ChannelTo = 255;
    ui32 GroupId = 0;
    TString StoragePool;
    TVector<ui32> ForcedGroupIds;
    int TabletPercent = 100;
    NJson::TJsonValue Result;

    TTxMonEvent_FindTablet(const TActorId& source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf* hive)
        : TBase(hive)
        , Event(ev->Release())
        , Source(source)
    {
        TabletId = FromStringWithDefault<TTabletId>(Event->Cgi().Get("tablet"), TabletId);
        TabletType = (TTabletTypes::EType)FromStringWithDefault<int>(Event->Cgi().Get("type"), TabletType);
        ChannelFrom = FromStringWithDefault<ui32>(Event->Cgi().Get("channelFrom"), ChannelFrom);
        ChannelTo = FromStringWithDefault<ui32>(Event->Cgi().Get("channelTo"), ChannelTo);
        GroupId = FromStringWithDefault(Event->Cgi().Get("group"), GroupId);
        StoragePool = Event->Cgi().Get("storagePool");
        TabletPercent = FromStringWithDefault<int>(Event->Cgi().Get("percent"), TabletPercent);
        TabletPercent = std::min(std::abs(TabletPercent), 100);
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_FIND_TABLET; }

    TInstant GetMaxTimestamp(const TLeaderTabletInfo* tablet) const {
        TInstant max;
        for (const auto& channel : tablet->TabletStorageInfo->Channels) {
            if (channel.Channel >= ChannelFrom && channel.Channel <= ChannelTo) {
                const auto* latest = channel.LatestEntry();
                if (latest != nullptr && latest->Timestamp > max) {
                    max = latest->Timestamp;
                }
            }
        }
        return max;
    }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        TDeque<TLeaderTabletInfo*> tablets;
        if (TabletId != 0) {
            TLeaderTabletInfo* tablet = Self->FindTablet(TabletId);
            if (tablet != nullptr) {
                tablets.push_back(tablet);
            }
        } else if (TabletType != TTabletTypes::TypeInvalid) {
            for (auto& pr : Self->Tablets) {
                if (pr.second.Type == TabletType) {
                    tablets.push_back(&pr.second);
                }
            }
        } else {
            for (auto& pr : Self->Tablets) {
                tablets.push_back(&pr.second);
            }
        }
        Sort(tablets, [this](TLeaderTabletInfo* a, TLeaderTabletInfo* b) -> bool {
            return GetMaxTimestamp(a) < GetMaxTimestamp(b);
        });
        Result.SetType(NJson::EJsonValueType::JSON_ARRAY);
        for (TLeaderTabletInfo* tablet : tablets) {
            TVector<ui32> channels;
            for (const auto& channel : tablet->TabletStorageInfo->Channels) {
                if (channel.Channel < ChannelFrom) {
                    continue;
                }
                if (channel.Channel > ChannelTo) {
                    continue;
                }
                if (StoragePool && channel.StoragePool != StoragePool) {
                    continue;
                }
                if (GroupId) {
                    const auto* latest = channel.LatestEntry();
                    if (latest != nullptr && latest->GroupID != GroupId) {
                        continue;
                    }
                }
                channels.push_back(channel.Channel);
            }
            if (channels) {
                NJson::TJsonValue jsonTablet(NJson::EJsonValueType::JSON_MAP);
                jsonTablet["tabletId"] = ToString(tablet->Id);
                NJson::TJsonValue& jsonChannels(jsonTablet["channels"]);
                for (ui32 channel : channels) {
                    jsonChannels.AppendValue(channel);
                }
                Result.AppendValue(jsonTablet);
            }
        }
        if (TabletPercent != 100) {
            Result.GetArraySafe().resize(Result.GetArraySafe().size() * TabletPercent / 100);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes(NJson::WriteJson(Result, false)));
    }
};

class TTxMonEvent_TabletInfo : public TTransactionBase<THive> {
public:
    THolder<NMon::TEvRemoteHttpInfo> Event;
    const TActorId Source;
    TTabletId TabletId = 0;
    NJson::TJsonValue Result;

    TTxMonEvent_TabletInfo(const TActorId& source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf* hive)
        : TBase(hive)
        , Event(ev->Release())
        , Source(source)
    {
        TabletId = FromStringWithDefault<TTabletId>(Event->Cgi().Get("tablet"), TabletId);
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_TABLET_INFO; }

    static NJson::TJsonValue MakeFrom(const TActorId& actorId) {
        return actorId.ToString();
    }

    static NJson::TJsonValue MakeFrom(const TSubDomainKey& subDomainKey) {
        return TStringBuilder() << subDomainKey;
    }

    static NJson::TJsonValue MakeFrom(const NProtoBuf::Message& proto) {
        NJson::TJsonValue result;
        NProtobufJson::TProto2JsonConfig config;
        config.SetEnumMode(NProtobufJson::TProto2JsonConfig::EnumName);
        try {
            NProtobufJson::Proto2Json(proto, result, config);
        }
        catch (const std::exception& e) {
            result["error"] = e.what();
        }
        return result;
    }

    static NJson::TJsonValue MakeFrom(ui64 item) {
        return item;
    }

    static NJson::TJsonValue MakeFrom(TString item) {
        return item;
    }

    template<typename Type>
    static NJson::TJsonValue MakeFrom(const TVector<Type>& array) {
        NJson::TJsonValue result;
        result.SetType(NJson::JSON_ARRAY);
        for (const auto& item : array) {
            result.AppendValue(MakeFrom(item));
        }
        return result;
    }

    template<typename Type>
    static NJson::TJsonValue MakeFrom(const TList<Type>& list) {
        NJson::TJsonValue result;
        result.SetType(NJson::JSON_ARRAY);
        for (const auto& item : list) {
            result.AppendValue(MakeFrom(item));
        }
        return result;
    }

    template<typename Type>
    static NJson::TJsonValue MakeFrom(const TArrayRef<Type>& arrayRef) {
        NJson::TJsonValue result;
        result.SetType(NJson::JSON_ARRAY);
        for (const auto& item : arrayRef) {
            result.AppendValue(MakeFrom(item));
        }
        return result;
    }

    static NJson::TJsonValue MakeFrom(const TIntrusivePtr<TTabletStorageInfo>& info) {
        NJson::TJsonValue result;
        if (info == nullptr) {
            result.SetType(NJson::JSON_NULL);
            return result;
        }
        result["Version"] = info->Version;
        NJson::TJsonValue& channels = result["Channels"];
        channels.SetType(NJson::JSON_ARRAY);
        for (const TTabletChannelInfo& ch : info->Channels) {
            NJson::TJsonValue& channel = channels.AppendValue({});
            channel["Channel"] = ch.Channel;
            NJson::TJsonValue& history = channel["History"];
            history.SetType(NJson::JSON_ARRAY);
            for (const TTabletChannelInfo::THistoryEntry& hs : ch.History) {
                NJson::TJsonValue& item = history.AppendValue({});
                item["FromGeneration"] = hs.FromGeneration;
                item["GroupID"] = hs.GroupID;
                item["Timestamp"] = hs.Timestamp.ToString();
            }
        }
        return result;
    }

    static NJson::TJsonValue MakeFrom(const NMetrics::TMaximumValueVariableWindowUI64& maximum) {
        NJson::TJsonValue result;
        result["MaximumValue"] = maximum.GetValue();
        result["Data"] = MakeFrom((const NProtoBuf::Message&)maximum);
        return result;
    }

    static NJson::TJsonValue MakeFrom(const NKikimrTabletBase::TMetrics& metrics) {
        return MakeFrom((const NProtoBuf::Message&)metrics);
    }

    static NJson::TJsonValue MakeFrom(const TTabletMetricsAggregates& aggregates) {
        NJson::TJsonValue result;
        result["MaximumCPU"] = MakeFrom(aggregates.MaximumCPU);
        result["MaximumMemory"] = MakeFrom(aggregates.MaximumMemory);
        result["MaximumNetwork"] = MakeFrom(aggregates.MaximumNetwork);
        return result;
    }

    static NJson::TJsonValue MakeFrom(const TTabletCategoryInfo& category) {
        NJson::TJsonValue result;
        result["Id"] = category.Id;
        result["MaxDisconnectTimeout"] = category.MaxDisconnectTimeout;
        result["StickTogetherInDC"] = category.StickTogetherInDC;
        return result;
    }

    static NJson::TJsonValue MakeFrom(const TTabletInfo& tablet) {
        NJson::TJsonValue result;
        result["VolatileState"] = TTabletInfo::EVolatileStateName(tablet.VolatileState);
        result["VolatileStateChangeTime"] = tablet.VolatileStateChangeTime.ToString();
        result["TabletRole"] = TTabletInfo::ETabletRoleName(tablet.TabletRole);
        result["LastBalancerDecisionTime"] = tablet.LastBalancerDecisionTime.ToString();
        result["BalancerPolicy"] = NKikimrHive::EBalancerPolicy_Name(tablet.BalancerPolicy);
        result["NodeId"] = tablet.NodeId;
        result["LastNodeId"] = tablet.LastNodeId;
        result["PreferredNodeId"] = tablet.PreferredNodeId;
        if (tablet.Node != nullptr) {
            result["Node"] = tablet.Node->Id;
        } else {
            result["Node"] = nullptr                                                                                                                                        ;
        }
        result["PostponedStart"] = tablet.PostponedStart.ToString();
        result["Weight"] = tablet.Weight;
        result["BootState"] = tablet.BootState;
        result["ResourceValues"] = MakeFrom(tablet.ResourceValues);
        result["ResourceMetricsAggregates"] = MakeFrom(tablet.ResourceMetricsAggregates);
        result["ActorsToNotify"] = MakeFrom(tablet.ActorsToNotify);
        result["ActorsToNotifyOnRestart"] = MakeFrom(tablet.ActorsToNotifyOnRestart);
        return result;
    }

    static NJson::TJsonValue MakeFrom(const TFollowerGroup& group) {
        NJson::TJsonValue result;
        result["Id"] = TStringBuilder() << group.Id;
        result["AllowLeaderPromotion"] = group.AllowLeaderPromotion;
        result["AllowClientRead"] = group.AllowClientRead;
        result["RequireAllDataCenters"] = group.RequireAllDataCenters;
        result["AllowedNodes"] = MakeFrom(group.NodeFilter.AllowedNodes);
        result["AllowedDataCenters"] = MakeFrom(group.NodeFilter.AllowedDataCenters);
        result["LocalNodeOnly"] = group.LocalNodeOnly;
        result["RequireDifferentNodes"] = group.RequireDifferentNodes;
        result["FollowerCountPerDataCenter"] = group.FollowerCountPerDataCenter;
        return result;
    }

    static NJson::TJsonValue MakeFrom(const TLeaderTabletInfo& tablet) {
        NJson::TJsonValue result;
        result = MakeFrom(static_cast<const TTabletInfo&>(tablet)); // base properties
        result["Id"] = TStringBuilder() << tablet.Id;
        result["State"] = ETabletStateName(tablet.State);
        result["Type"] = TTabletTypes::EType_Name(tablet.Type);
        result["ObjectId"] = TStringBuilder() << tablet.ObjectId;
        result["ObjectDomain"] = TStringBuilder() << tablet.ObjectDomain;
        result["AllowedNodes"] = MakeFrom(tablet.NodeFilter.AllowedNodes);
        result["AllowedDataCenters"] = MakeFrom(tablet.NodeFilter.AllowedDataCenters);
        result["DataCenterPreference"] = MakeFrom(tablet.DataCentersPreference);
        result["TabletStorageInfo"] = MakeFrom(tablet.TabletStorageInfo);
        result["BoundChannels"] = MakeFrom(tablet.BoundChannels);
        result["ChannelProfileNewGroup"] = TString(tablet.ChannelProfileNewGroup.to_string());
        result["ChannelProfileReassignReason"] = NKikimrHive::TEvReassignTablet::EHiveReassignReason_Name(tablet.ChannelProfileReassignReason);
        result["KnownGeneration"] = tablet.KnownGeneration;
        result["BootMode"] = NKikimrHive::ETabletBootMode_Name(tablet.BootMode);
        result["Owner"] = TStringBuilder() << tablet.Owner;
        result["AllowedDomains"] = MakeFrom(tablet.NodeFilter.AllowedDomains);
        result["EffectiveAllowedDomains"] = MakeFrom(tablet.NodeFilter.GetEffectiveAllowedDomains());
        result["StorageInfoSubscribers"] = MakeFrom(tablet.StorageInfoSubscribers);
        result["LockedToActor"] = MakeFrom(tablet.LockedToActor);
        result["LockedReconnectTimeout"] = tablet.LockedReconnectTimeout.ToString();
        result["PendingUnlockSeqNo"] = tablet.PendingUnlockSeqNo;
        result["FollowerGroups"] = MakeFrom(tablet.FollowerGroups);
        result["Followers"] = MakeFrom(tablet.Followers);
        result["Statistics"] = MakeFrom(tablet.Statistics);
        if (tablet.Category) {
            result["Category"] = MakeFrom(*tablet.Category);
        } else {
            result["Category"] = nullptr;
        }
        return result;
    }

    static NJson::TJsonValue MakeFrom(const TFollowerTabletInfo& tablet) {
        NJson::TJsonValue result;
        result = MakeFrom(static_cast<const TTabletInfo&>(tablet)); // base properties
        result["Id"] = TStringBuilder() << tablet.GetFullTabletId();
        result["FollowerGroupId"] = tablet.FollowerGroup.Id;
        return result;
    }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        TLeaderTabletInfo* tablet = Self->FindTablet(TabletId);
        if (tablet != nullptr) {
            Result = MakeFrom(*tablet);
        } else {
            Result["error"] = "Not found";
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes(NJson::WriteJson(Result, false)));
    }
};

class TTxMonEvent_ObjectStats : public TTransactionBase<THive> {
public:
    NJson::TJsonValue Result;
    const TActorId Source;

    TTxMonEvent_ObjectStats(const TActorId& source, TSelf* hive)
        : TBase(hive)
        , Source(source)
    {
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_OBJECT_STATS; }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        std::map<TFullObjectId, std::vector<std::pair<ui64, ui64>>> distributionOfObjects;
        for (const auto& node : Self->Nodes) {
            for (const auto& obj : node.second.TabletsOfObject) {
                distributionOfObjects[obj.first].emplace_back(node.first, obj.second.size());
            }
        }

        Result.SetType(NJson::JSON_ARRAY);
        for (const auto& [key, val] : distributionOfObjects) {
            NJson::TJsonValue listItem;
            listItem["objectId"] = TStringBuilder() << key;
            NJson::TJsonValue& distribution = listItem["distribution"];
            for (const auto& [node, cnt] : val) {
                distribution[TStringBuilder() << node] = cnt;
            }
            Result.AppendValue(listItem);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes(NJson::WriteJson(Result, false)));
    }

};

class TTxMonEvent_ResetTablet : public TTransactionBase<THive> {
    class TResetter : public TActorBootstrapped<TResetter> {
        TIntrusivePtr<TTabletStorageInfo> Info;
        const TActorId Source;
        const ui32 KnownGeneration;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::HIVE_MON_REQUEST;
        }

        TResetter(TIntrusivePtr<TTabletStorageInfo> info, TActorId source, ui32 knownGeneration)
            : Info(std::move(info))
            , Source(source)
            , KnownGeneration(knownGeneration)
        {}

        void Bootstrap(const TActorContext& ctx) {
            Become(&TThis::StateFunc);
            ctx.Register(CreateTabletReqReset(SelfId(), std::move(Info), KnownGeneration));
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvTablet::TEvResetTabletResult, Handle)
        )

        void Handle(TEvTablet::TEvResetTabletResult::TPtr& ev, const TActorContext& ctx) {
            ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes(Sprintf("{\"status\": \"%s\"}",
                NKikimrProto::EReplyStatus_Name(ev->Get()->Status).data())));
        }
    };

public:
    const TActorId Source;
    TTabletId TabletId = 0;
    TString Error;
    TIntrusivePtr<TTabletStorageInfo> Info;
    ui32 KnownGeneration = 0;

    TTxMonEvent_ResetTablet(const TActorId& source, TTabletId tabletId, TSelf* hive)
        : TBase(hive)
        , Source(source)
        , TabletId(tabletId)
    {
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_RESET_TABLET; }

    bool Execute(TTransactionContext&, const TActorContext& /*ctx*/) override {
        if (TabletId) {
            if (TLeaderTabletInfo* tablet = Self->FindTablet(TabletId)) {
                Info = tablet->TabletStorageInfo;
                KnownGeneration = tablet->KnownGeneration;
            } else {
                Error = "tablet not found";
            }
        } else {
            Error = "tablet parameter not set";
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Info) {
            ctx.Register(new TResetter(std::move(Info), Source, KnownGeneration));
        } else if (Error) {
            ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes(TStringBuilder() << "{\"error\":\"" << Error << "\"}"));
        } else {
            Y_ABORT("unexpected state");
        }
    }
};

class TUpdateResourcesActor : public TActorBootstrapped<TUpdateResourcesActor> {
public:
    TActorId Source;
    TActorId Hive;
    NKikimrHive::TTabletMetrics Metrics;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HIVE_MON_REQUEST;
    }

    TUpdateResourcesActor(const TActorId& source, const TActorId& hive, const NKikimrHive::TTabletMetrics& metrics)
        : Source(source)
        , Hive(hive)
        , Metrics(metrics)
    {}

    void HandleTimeout(const TActorContext& ctx) {
        ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes("{\"Error\": \"Timeout\"}"));
        Die(ctx);
    }

    void Handle(TEvLocal::TEvTabletMetricsAck::TPtr, const TActorContext& ctx) {
        ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes("{}"));
        Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvLocal::TEvTabletMetricsAck, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        TAutoPtr<TEvHive::TEvTabletMetrics> event(new TEvHive::TEvTabletMetrics());
        auto& record = event->Record;
        auto& metrics = *record.AddTabletMetrics();
        metrics = Metrics;
        ctx.Send(Hive, event.Release());
        Become(&TUpdateResourcesActor::StateWork, ctx, TDuration::MilliSeconds(30), new TEvents::TEvWakeup());
    }
};

class TCreateTabletActor : public TActorBootstrapped<TCreateTabletActor> {
public:
    TActorId Source;
    TAutoPtr<TEvHive::TEvCreateTablet> Event;
    THive* Hive;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HIVE_MON_REQUEST;
    }

    TCreateTabletActor(const TActorId& source, ui64 owner, ui64 ownerIdx, TTabletTypes::EType type, ui32 channelsProfile, ui32 followers, THive* hive)
        : Source(source)
        , Event(new TEvHive::TEvCreateTablet())
        , Hive(hive)
    {
        Event->Record.SetOwner(owner);
        Event->Record.SetOwnerIdx(ownerIdx);
        Event->Record.SetTabletType(type);
        Event->Record.SetChannelsProfile(channelsProfile);
        Event->Record.SetFollowerCount(followers);
    }

    void HandleTimeout(const TActorContext& ctx) {
        ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes("{\"Error\": \"Timeout\"}"));
        Die(ctx);
    }

    void Handle(TEvHive::TEvCreateTabletReply::TPtr& ptr, const TActorContext& ctx) {
        TStringStream stream;
        stream << ptr->Get()->Record.AsJSON();
        ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes(stream.Str()));
        Die(ctx);
    }

    void Handle(TEvHive::TEvTabletCreationResult::TPtr& ptr, const TActorContext& ctx) {
        TStringStream stream;
        stream << ptr->Get()->Record.AsJSON();
        ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes(stream.Str()));
        Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvHive::TEvCreateTabletReply, Handle);
            HFunc(TEvHive::TEvTabletCreationResult, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        ctx.Send(Hive->SelfId(), Event.Release());
        Become(&TThis::StateWork, ctx, TDuration::Seconds(30), new TEvents::TEvWakeup());
    }
};

class TDeleteTabletActor : public TActorBootstrapped<TDeleteTabletActor> {
private:
    ui64 FAKE_TXID = -1;

public:
    TActorId Source;
    TAutoPtr<TEvHive::TEvDeleteTablet> Event;
    THive* Hive;

    TDeleteTabletActor(const TActorId& source, ui64 owner, ui64 ownerIdx, THive* hive)
        : Source(source)
        , Event(new TEvHive::TEvDeleteTablet())
        , Hive(hive)
    {
        Event->Record.SetShardOwnerId(owner);
        Event->Record.AddShardLocalIdx(ownerIdx);
        Event->Record.SetTxId_Deprecated(FAKE_TXID);
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HIVE_MON_REQUEST;
    }

    TDeleteTabletActor(const TActorId& source, ui64 tabletId, THive* hive)
        : Source(source)
        , Event(new TEvHive::TEvDeleteTablet())
        , Hive(hive)
    {
        ui64 owner = 0;
        ui64 ownerIdx = -1;
        for (auto it = hive->OwnerToTablet.begin(); it != hive->OwnerToTablet.end(); ++it) {
            if (it->second == tabletId) {
                owner = it->first.first;
                ownerIdx = it->first.second;
                break;
            }
        }

        Event->Record.SetShardOwnerId(owner);
        Event->Record.AddShardLocalIdx(ownerIdx);
        Event->Record.SetTxId_Deprecated(FAKE_TXID);
    }

private:
    void HandleTimeout(const TActorContext& ctx) {
        ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes("{\"Error\": \"Timeout\"}"));
        Die(ctx);
    }

    void Handle(TEvHive::TEvDeleteTabletReply::TPtr& ptr, const TActorContext& ctx) {
        TStringStream stream;
        stream << ptr->Get()->Record.AsJSON();
        ctx.Send(Source, new NMon::TEvRemoteJsonInfoRes(stream.Str()));
        Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvHive::TEvDeleteTabletReply, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

public:
    void Bootstrap(const TActorContext& ctx) {
        ctx.Send(Hive->SelfId(), Event.Release());
        Become(&TThis::StateWork, ctx, TDuration::Seconds(30), new TEvents::TEvWakeup());
    }
};

class TTxMonEvent_Groups : public TTransactionBase<THive> {
public:
    const TActorId Source;
    TAutoPtr<NMon::TEvRemoteHttpInfo> Event;

    TTxMonEvent_Groups(const TActorId &source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf *hive)
        : TBase(hive)
        , Source(source)
        , Event(ev->Release())
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_GROUPS; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        TStringStream out;
        const auto& params(Event->Cgi());
        TTabletId filterTabletId = FromStringWithDefault<TTabletId>(params.Get("tablet_id"), (ui64)-1);
        ui64 filterGroupId = FromStringWithDefault<ui64>(params.Get("group_id"), (ui64)-1);

        out << "<head>";
        out << "<script>$('.container').toggleClass('container container-fluid').css('padding-left', '5%') .css('padding-right', '5%');</script>";
        out << "</head><body>";

        out << "<table class='table'>";
        out << "<thead>";
        out << "<tr><th>TabletID</th><th>Channel</th><th>GroupID</th><th>Generation</th><th>Version</th><th>Timestamp</th><th>Current AU</th></tr>";
        out << "</thead>";

        out << "<tbody>";

        NIceDb::TNiceDb db(txc.DB);
        auto tabletChannelGenRowset = db.Table<Schema::TabletChannelGen>().Range().Select();
        if (!tabletChannelGenRowset.IsReady())
            return false;
        while (!tabletChannelGenRowset.EndOfSet()) {
            TTabletId id = tabletChannelGenRowset.GetValue<Schema::TabletChannelGen::Tablet>();
            auto group = tabletChannelGenRowset.GetValue<Schema::TabletChannelGen::Group>();
            bool filterOk = (filterTabletId == id) || (filterGroupId == group) || (filterTabletId == (ui64)-1 && filterGroupId == (ui64)-1);
            if (filterOk) {
                ui32 channel = tabletChannelGenRowset.GetValue<Schema::TabletChannelGen::Channel>();
                out << "<tr>";
                out << "<td><a href='../tablets?TabletID=" << id << "'>" << id << "</a></td>";
                out << "<td>" << channel << "</td>";
                out << "<td>" << group << "</td>";
                out << "<td>" << tabletChannelGenRowset.GetValue<Schema::TabletChannelGen::Generation>() << "</td>";
                if (tabletChannelGenRowset.HaveValue<Schema::TabletChannelGen::Version>()) {
                    out << "<td>" << tabletChannelGenRowset.GetValue<Schema::TabletChannelGen::Version>() << "</td>";
                } else {
                    out << "<td></td>";
                }
                if (tabletChannelGenRowset.HaveValue<Schema::TabletChannelGen::Timestamp>()) {
                    out << "<td>" << TInstant::MilliSeconds(tabletChannelGenRowset.GetValue<Schema::TabletChannelGen::Timestamp>()).ToString() << "</td>";
                } else {
                    out << "<td></td>";
                }
                TString unitSize;
                TTabletInfo* tablet = Self->FindTablet(id);
                if (tablet) {
                    TLeaderTabletInfo& leader = tablet->GetLeader();
                    if (channel < leader.GetChannelCount()) {
                        unitSize = leader.BoundChannels[channel].ShortDebugString();
                    }
                }
                out << "<td>" << unitSize << "</td>";

                out << "</tr>";
            }

            if (!tabletChannelGenRowset.Next())
                return false;
        }
        out <<"</tbody>";
        out << "</table>";

        out << "</body>";
        ctx.Send(Source, new NMon::TEvRemoteHttpInfoRes(out.Str()));
        return true;
    }

    void Complete(const TActorContext&) override {}
};

class TTxMonEvent_NotReady : public TTransactionBase<THive> {
public:
    const TActorId Source;

    TTxMonEvent_NotReady(const TActorId& source, TSelf* hive)
        : TBase(hive)
        , Source(source)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_NOT_READY; }

    bool Execute(TTransactionContext&, const TActorContext& ctx) override {
        ctx.Send(Source, new NMon::TEvRemoteHttpInfoRes("<head></head><body><h1>Hive is not ready yet</h1></body>"));
        return true;
    }

    void Complete(const TActorContext&) override {}
};

class TTxMonEvent_Storage : public TTransactionBase<THive> {
public:
    const TActorId Source;
    THolder<NMon::TEvRemoteHttpInfo> Event;
    bool Kinds = false;

    TTxMonEvent_Storage(const TActorId &source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf *hive)
        : TBase(hive)
        , Source(source)
        , Event(ev->Release())
    {
        Kinds = FromStringWithDefault(Event->Cgi().Get("kinds"), Kinds);
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_STORAGE; }

    bool Execute(TTransactionContext &txc, const TActorContext& ctx) override {
        Y_UNUSED(txc);
        TStringStream str;
        RenderHTMLPage(str);
        ctx.Send(Source, new NMon::TEvRemoteHttpInfoRes(str.Str()));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Y_UNUSED(ctx);
    }

    struct TUnitKind {
        double IOPS;
        ui64 Throughput;
        ui64 Size;

        bool operator ==(const TUnitKind& o) const {
            return IOPS == o.IOPS && Throughput == o.Throughput && Size == o.Size;
        }

        TString ToString() const {
            return TStringBuilder() << "{IOPS:" << Sprintf("%.2f",IOPS) << ",Throughput:" << GetBytesPerSecond(Throughput) << ",Size:" << GetBytes(Size) << "}";
        }
    };

    struct TUnitKindHash {
        size_t operator ()(const TUnitKind& a) const {
            return hash_combiner::hash_val(a.IOPS, a.Throughput, a.Size);
        }
    };

    using TKindMap = THashMap<TUnitKind, ui32, TUnitKindHash>;

    void GetUnitKinds(const TStorageGroupInfo& group, TKindMap& kinds) {
        for (const auto& channel : group.Units) {
            const auto& boundChannels(*channel.ChannelInfo);
            kinds[TUnitKind{boundChannels.GetIOPS(), boundChannels.GetThroughput(), boundChannels.GetSize()}]++;
        }
    }

    void RenderHTMLPage(IOutputStream &out) {
        out << "<script>$('.container').css('width', 'auto');</script>";

        if (Kinds) {
            out << "<p><a href=\"?TabletID=" << Self->HiveId << "&page=Storage&kinds=false\">Turn off kinds grouping</a></p>";
        } else {
            out << "<p><a href=\"?TabletID=" << Self->HiveId << "&page=Storage&kinds=true\">Turn on kinds grouping</a></p>";
        }
        out << "<table class='table table-sortable'>";
        out << "<thead>";
        if (Kinds) {
            out << "<tr><th>Storage Pool</th><th>Group ID</th><th>Binding Kind</th><th>AUs</th><th>Acq. IOPS</th><th>Max. IOPS</th><th>Acq. Size</th><th>Max. Size</th>"
                   "<th>Acq. Thr.</th><th>Max. Thr.</th><th>Overcommit</th><th>Group Usage</th></tr>";
        } else {
            out << "<tr><th>Storage Pool</th><th>Group ID</th><th>AUs</th><th>Acq. IOPS</th><th>Max. IOPS</th><th>Acq. Size</th><th>Max. Size</th>"
                   "<th>Acq. Thr.</th><th>Max. Thr.</th><th>Allocated Size</th><th>Available Size</th><th>Overcommit</th><th>Group Usage</th></tr>";
        }
        out << "</thead>";
        out << "<tbody>";
        for (const auto& prStoragePool : Self->StoragePools) {
            for (const auto& prStorageGroup : prStoragePool.second.Groups) {
                if (Kinds) {
                    TKindMap kindMap;
                    GetUnitKinds(prStorageGroup.second, kindMap);
                    for (const auto& [kind, units] : kindMap) {
                        out << "<tr>";
                        out << "<td>" << prStoragePool.second.Name << "</td>";
                        out << "<td style='text-align:right'>" << prStorageGroup.second.Id << "</td>";
                        out << "<td style='text-align:right'>" << kind.ToString() << "</td>";
                        out << "<td style='text-align:right'>" << units << "</td>";
                        out << "<td style='text-align:right'>" << Sprintf("%.2f", kind.IOPS * units) << "</td>";
                        out << "<td style='text-align:right'>" << Sprintf("%.2f", prStorageGroup.second.MaximumResources.IOPS) << "</td>";
                        out << "<td style='text-align:right'>" << kind.Size * units << "</td>";
                        out << "<td style='text-align:right'>" << prStorageGroup.second.MaximumResources.Size << "</td>";
                        out << "<td style='text-align:right'>" << kind.Throughput * units << "</td>";
                        out << "<td style='text-align:right'>" << prStorageGroup.second.MaximumResources.Throughput << "</td>";
                        out << "<td style='text-align:right'>" << Sprintf("%.2f", prStorageGroup.second.StoragePool.GetOvercommit()) << "</td>";
                        out << "<td style='text-align:right'>" << Sprintf("%.2f", prStorageGroup.second.GetUsage()) << "</td>";
                        out << "</tr>";
                    }
                } else {
                    const TStorageGroupInfo& group = prStorageGroup.second;
                    out << "<tr>";
                    out << "<td>" << prStoragePool.second.Name << "</td>";
                    out << "<td style='text-align:right'>" << group.Id << "</td>";
                    out << "<td style='text-align:right'>" << group.Units.size() << "</td>";
                    out << "<td style='text-align:right'>" << Sprintf("%.2f", group.AcquiredResources.IOPS) << "</td>";
                    out << "<td style='text-align:right'>" << Sprintf("%.2f", group.MaximumResources.IOPS) << "</td>";
                    out << "<td style='text-align:right'>" << group.AcquiredResources.Size << "</td>";
                    out << "<td style='text-align:right'>" << group.MaximumResources.Size << "</td>";
                    out << "<td style='text-align:right'>" << group.AcquiredResources.Throughput << "</td>";
                    out << "<td style='text-align:right'>" << group.MaximumResources.Throughput << "</td>";
                    out << "<td style='text-align:right'>" << group.GroupParameters.GetAllocatedSize() << "</td>";
                    out << "<td style='text-align:right'>" << group.GroupParameters.GetAvailableSize() << "</td>";
                    out << "<td style='text-align:right'>" << Sprintf("%.2f", group.StoragePool.GetOvercommit()) << "</td>";
                    out << "<td style='text-align:right'>" << Sprintf("%.2f", group.GetUsage()) << "</td>";
                    out << "</tr>";
                }
            }
        }
        out << "</tbody>";
        out << "</table>";
        out << "</div></div>";
    }
};

class TTxMonEvent_Subactors : public TTransactionBase<THive> {
public:
    const TActorId Source;
    THolder<NMon::TEvRemoteHttpInfo> Event;
    TMaybe<TSubActorId> SubActorToStop;

    TTxMonEvent_Subactors(const TActorId &source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf *hive)
        : TBase(hive)
        , Source(source)
        , Event(ev->Release())
    {
        SubActorToStop = TryFromString<TSubActorId>(Event->Cgi().Get("stop"));
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_SUBACTORS; }

    bool Execute(TTransactionContext &txc, const TActorContext& ctx) override {
        Y_UNUSED(txc);
        if (SubActorToStop) {
            Self->StopSubActor(*SubActorToStop);
        }
        TStringStream str;
        RenderHTMLPage(str);
        ctx.Send(Source, new NMon::TEvRemoteHttpInfoRes(str.Str()));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Y_UNUSED(ctx);
    }

    void RenderHTMLPage(IOutputStream& out) {
        out << "<head>";
        out << "<style>";
        out << "table.simple-table2 th { text-align: right; }";
        out << "table.simple-table2 tr:nth-child(1) > th:nth-child(2) { text-align: left; }";
        out << "table.simple-table2 tr:nth-child(1) > th:nth-child(3) { text-align: left; }";
        out << "table.simple-table2 tr:nth-child(1) > th:nth-child(4) { text-align: left; }";
        out << "table.simple-table2 td { text-align: right; }";
        out << "table.simple-table2 td:nth-child(2) { text-align: left; }";
        out << "table.simple-table2 td:nth-child(3) { text-align: left; }";
        out << "table.simple-table2 td:nth-child(4) { text-align: left; }";
        out << "</style>";
        out << "</head>";
        out << "<body>";
        out << "<table class='table simple-table2'>";
        out << "<thead>";
        out << "<tr><th>Id</th><th>Description</th><th>Started at</th><th>Stop</th></tr>";
        out << "</thead>";
        out << "<tbody>";
        for (const auto* subActor: Self->SubActors) {
            out << "<tr>";
            out << "<td>" << subActor->GetId() << "</td>";
            out << "<td>" << subActor->GetDescription() << "</td>";
            out << "<td>" << subActor->StartTime << "</td>";
            out << "<td><a href = '?TabletID=" << Self->HiveId << "&page=Subactors&stop=" << subActor->GetId() << "'><span class='glyphicon glyphicon-remove-sign' title='Stop SubActor'></span></a></td>";
            out << "</tr>";
        }
        out << "</tbody>";
        out << "</table>";
        out << "</body>";
    }
};

class TTxMonEvent_OperationsLog : public TTransactionBase<THive> {
public:
    const TActorId Source;
    THolder<NMon::TEvRemoteHttpInfo> Event;
    ui64 MaxCount = 100;

    TTxMonEvent_OperationsLog(const TActorId& source, NMon::TEvRemoteHttpInfo::TPtr& ev, TSelf* hive)
        : TBase(hive)
        , Source(source)
        , Event(ev->Release())
    {
        MaxCount = FromStringWithDefault(Event->Cgi().Get("max"), MaxCount);
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        TStringStream out;
        out << "<head>";
        out << "<style>";
        out << "table.simple-table2 th { text-align: right; }";
        out << "table.simple-table2 tr:nth-child(1) > th:nth-child(2) { text-align: left; }";
        out << "table.simple-table2 tr:nth-child(1) > th:nth-child(3) { text-align: left; }";
        out << "table.simple-table2 tr:nth-child(1) > th:nth-child(4) { text-align: left; }";
        out << "table.simple-table2 td { text-align: right; }";
        out << "table.simple-table2 td:nth-child(2) { text-align: left; }";
        out << "table.simple-table2 td:nth-child(3) { text-align: left; }";
        out << "table.simple-table2 td:nth-child(4) { text-align: left; }";
        out << "</style>";
        out << "</head>";
        out << "<body>";
        out << "<table class='table simple-table2'>";
        out << "<thead>";
        out << "<tr><th>Timestamp</th><th>User</th><th>Description</th></tr>";
        out << "</thead>";
        out << "<tbody>";

        NIceDb::TNiceDb db(txc.DB);
        auto operationsRowset = db.Table<Schema::OperationsLog>().All().Reverse().Select();
        if (!operationsRowset.IsReady()) {
            return false;
        }
        for (ui64 cnt = 0; !operationsRowset.EndOfSet() && cnt < MaxCount; ++cnt) {
            TString user = operationsRowset.GetValue<Schema::OperationsLog::User>();
            out << "<tr>";
            out << "<td>";
            if (operationsRowset.HaveValue<Schema::OperationsLog::OperationTimestamp>()) {
                out << TInstant::MilliSeconds(operationsRowset.GetValue<Schema::OperationsLog::OperationTimestamp>());
            }
            out << "</td>";
            out << "<td>" << (user.empty() ? "anonymous" : user.c_str()) << "</td>";
            out << "<td>";
            out << operationsRowset.GetValue<Schema::OperationsLog::Operation>();
            out << "</td>";
            out << "</tr>";
            if (!operationsRowset.Next()) {
                return false;
            }
        }
        out << "</tbody>";
        out << "</table>";
        out << "</body>";
        ctx.Send(Source, new NMon::TEvRemoteHttpInfoRes(out.Str()));
        return true;
    }

    void Complete(const TActorContext&) override {}
};

bool THive::IsSafeOperation(NMon::TEvRemoteHttpInfo::TPtr& ev, const TActorContext& ctx) {
    NMon::TEvRemoteHttpInfo* httpInfo = ev->Get();
    if (httpInfo->Method != HTTP_METHOD_POST) {
        ctx.Send(ev->Sender, new NMon::TEvRemoteJsonInfoRes("{\"error\":\"only POST method is allowed\"}"));
        return false;
    }
    if (!GetEnableDestroyOperations()) {
        ctx.Send(ev->Sender, new NMon::TEvRemoteJsonInfoRes("{\"error\":\"destroy operations are disabled\"}"));
        return false;
    }
    TCgiParameters cgi(httpInfo->Cgi());
    TStringBuilder keyData;
    keyData << cgi.Get("tablet") << cgi.Get("owner") << cgi.Get("owner_idx");
    if (keyData.Empty()) {
        ctx.Send(ev->Sender, new NMon::TEvRemoteJsonInfoRes("{\"error\":\"tablet, owner or owner_idx parameters not set\"}"));
        return false;
    }
    TString key = MD5::Data(keyData);
    if (key != cgi.Get("key")) {
        ctx.Send(ev->Sender, new NMon::TEvRemoteJsonInfoRes("{\"error\":\"key parameter is incorrect\"}"));
        return false;
    }
    return true;
}

void THive::CreateEvMonitoring(NMon::TEvRemoteHttpInfo::TPtr& ev, const TActorContext& ctx) {
    if (!ReadyForConnections) {
        return Execute(new TTxMonEvent_NotReady(ev->Sender, this), ctx);
    }
    NMon::TEvRemoteHttpInfo* httpInfo = ev->Get();
    TCgiParameters cgi(httpInfo->Cgi());
    TString page = cgi.Has("page") ? cgi.Get("page") : "";
    if (page == "MemStateTablets")
        return Execute(new TTxMonEvent_MemStateTablets(ev->Sender, ev, this), ctx);
    if (page == "MemStateNodes")
        return Execute(new TTxMonEvent_MemStateNodes(ev->Sender, ev, this), ctx);
    if (page == "MemStateDomains")
        return Execute(new TTxMonEvent_MemStateDomains(ev->Sender, ev, this), ctx);
    if (page == "DbState")
        return Execute(new TTxMonEvent_DbState(ev->Sender, this), ctx);
    if (page == "SetDown") {
        TNodeId nodeId = FromStringWithDefault<TNodeId>(cgi.Get("node"), 0);
        return Execute(new TTxMonEvent_SetDown(ev->Sender, nodeId, FromStringWithDefault<i32>(cgi.Get("down"), 0) != 0, this, ev), ctx);
    }
    if (page == "SetFreeze") {
        TNodeId nodeId = FromStringWithDefault<TNodeId>(cgi.Get("node"), 0);
        return Execute(new TTxMonEvent_SetFreeze(ev->Sender, nodeId, FromStringWithDefault<i32>(cgi.Get("freeze"), 0) != 0, this, ev), ctx);
    }
    if (page == "KickNode") {
        TNodeId nodeId = FromStringWithDefault<TNodeId>(cgi.Get("node"), 0);
        return Execute(new TTxMonEvent_KickNode(ev->Sender, nodeId, this), ctx);
    }
    if (page == "DrainNode") {
        return Execute(new TTxMonEvent_DrainNode(ev->Sender, ev, this), ctx);
    }
    if (page == "Rebalance") {
        return Execute(new TTxMonEvent_Rebalance(ev->Sender, ev, this), ctx);
    }
    if (page == "RebalanceFromScratch") {
        return Execute(new TTxMonEvent_RebalanceFromScratch(ev->Sender, ev, this), ctx);
    }
    if (page == "LandingData") {
        return Execute(new TTxMonEvent_LandingData(ev->Sender, ev, this), ctx);
    }
    if (page == "ReassignTablet") {
        return Execute(new TTxMonEvent_ReassignTablet(ev->Sender, ev, this), ctx);
    }
    if (page == "InitMigration") {
        return Execute(new TTxMonEvent_InitMigration(ev->Sender, ev, this), ctx);
    }
    if (page == "QueryMigration") {
        return Execute(new TTxMonEvent_QueryMigration(ev->Sender, ev, this), ctx);
    }
    if (page == "MoveTablet") {
        return Execute(new TTxMonEvent_MoveTablet(ev->Sender, ev, this), ctx);
    }
    if (page == "StopTablet") {
        return Execute(new TTxMonEvent_StopTablet(ev->Sender, ev, this), ctx);
    }
    if (page == "ResumeTablet") {
        return Execute(new TTxMonEvent_ResumeTablet(ev->Sender, ev, this), ctx);
    }
    if (page == "FindTablet") {
        return Execute(new TTxMonEvent_FindTablet(ev->Sender, ev, this), ctx);
    }
    if (page == "TabletInfo") {
        return Execute(new TTxMonEvent_TabletInfo(ev->Sender, ev, this), ctx);
    }
    if (page == "ObjectStats") {
        return Execute(new TTxMonEvent_ObjectStats(ev->Sender, this), ctx);
    }
    if (page == "CreateTablet") {
        ui64 owner = FromStringWithDefault<ui64>(cgi.Get("owner"), 0);
        ui64 ownerIdx = FromStringWithDefault<ui64>(cgi.Get("owner_idx"), 0);
        TTabletTypes::EType type = (TTabletTypes::EType)FromStringWithDefault<ui32>(cgi.Get("type"), 0);
        ui32 channelsProfile = FromStringWithDefault<ui32>(cgi.Get("profile"), 0);
        ui32 followers = FromStringWithDefault<ui32>(cgi.Get("followers"), 0);
        ctx.RegisterWithSameMailbox(new TCreateTabletActor(ev->Sender, owner, ownerIdx, type, channelsProfile, followers, this));
        return;
    }
    if (page == "ResetTablet") {
        if (IsSafeOperation(ev, ctx)) {
            TTabletId tabletId = FromStringWithDefault<TTabletId>(cgi.Get("tablet"), 0);
            return Execute(new TTxMonEvent_ResetTablet(ev->Sender, tabletId, this), ctx);
        }
    }
    if (page == "DeleteTablet") {
        if (IsSafeOperation(ev, ctx)) {
            if (cgi.Has("owner") && cgi.Has("owner_idx")) {
                ui64 owner = FromStringWithDefault<ui64>(cgi.Get("owner"), 0);
                ui64 ownerIdx = FromStringWithDefault<ui64>(cgi.Get("owner_idx"), 0);
                ctx.RegisterWithSameMailbox(new TDeleteTabletActor(ev->Sender, owner, ownerIdx, this));
            } else if (cgi.Has("tablet")) {
                TTabletId tabletId = FromStringWithDefault<TTabletId>(cgi.Get("tablet"), 0);
                ctx.RegisterWithSameMailbox(new TDeleteTabletActor(ev->Sender, tabletId, this));
            } else {
                ctx.Send(ev->Sender, new NMon::TEvRemoteJsonInfoRes("{\"Error\": \"tablet or (owner, owner_idx) params must be specified\"}"));
            }
        }
        return;
    }
    if (page == "Resources") {
        return Execute(new TTxMonEvent_Resources(ev->Sender, ev, this), ctx);
    }
    if (page == "Settings") {
        return Execute(new TTxMonEvent_Settings(ev->Sender, ev, this), ctx);
    }
    if (page == "Groups") {
        return Execute(new TTxMonEvent_Groups(ev->Sender, ev, this), ctx);
    }
    if (page == "UpdateResources") {
        TTabletId tabletId = FromStringWithDefault<TTabletId>(cgi.Get("tablet"), 0);
        TTabletInfo* tablet = FindTablet(tabletId);
        if (tablet != nullptr) {
            NKikimrHive::TTabletMetrics metrics;
            metrics.SetTabletID(tabletId);
            if (cgi.Get("cpu")) {
                metrics.MutableResourceUsage()->SetCPU(FromStringWithDefault<ui64>(cgi.Get("cpu"), 0));
            }
            if (cgi.Get("kv")) {
                metrics.MutableResourceUsage()->SetMemory(FromStringWithDefault<ui64>(cgi.Get("kv"), 0));
            }
            if (cgi.Get("memory")) {
                metrics.MutableResourceUsage()->SetMemory(FromStringWithDefault<ui64>(cgi.Get("memory"), 0));
            }
            if (cgi.Get("network")) {
                metrics.MutableResourceUsage()->SetNetwork(FromStringWithDefault<ui64>(cgi.Get("network"), 0));
            }
            ctx.RegisterWithSameMailbox(new TUpdateResourcesActor(ev->Sender, SelfId(), metrics));
            return;
        }
    }
    if (page == "Storage") {
        return Execute(new TTxMonEvent_Storage(ev->Sender, ev, this), ctx);
    }
    if (page == "StorageRebalance") {
        return Execute(new TTxMonEvent_StorageRebalance(ev->Sender, ev, this), ctx);
    }
    if (page == "Subactors") {
        return Execute(new TTxMonEvent_Subactors(ev->Sender, ev, this), ctx);
    }
    if (page == "TabletAvailability") {
        return Execute(new TTxMonEvent_TabletAvailability(ev->Sender, ev, this), ctx);
    }
    if (page == "OperationsLog") {
        return Execute(new TTxMonEvent_OperationsLog(ev->Sender, ev, this), ctx);
    }
    return Execute(new TTxMonEvent_Landing(ev->Sender, ev, this), ctx);
}

} // NHive
} // NKikimr
