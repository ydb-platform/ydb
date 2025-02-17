#include "read_balancer.h"

#include "read_balancer__balancing.h"

#include "common_app.h"

namespace NKikimr::NPQ {

bool TPersQueueReadBalancer::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx) {
    if (!ev) {
        return true;
    }

    TString str = GenerateStat();
    ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(str));
    return true;
}

TString TPersQueueReadBalancer::GenerateStat() {
    auto& metrics = AggregatedStats.Metrics;

    TStringStream str;
    HTML_APP_PAGE(str, "PersQueueReadBalancer " << TabletID() << " (" << Path << ")") {
        NAVIGATION_BAR() {
            NAVIGATION_TAB("generic", "Generic Info");
            NAVIGATION_TAB("partitions", "Partitions");
            for (auto& [consumerName, _] : Balancer->GetConsumers()) {
                NAVIGATION_TAB("c_" << EncodeAnchor(consumerName), NPersQueue::ConvertOldConsumerName(consumerName));
            }

            NAVIGATION_TAB_CONTENT("generic") {
                LAYOUT_ROW() {
                    LAYOUT_COLUMN() {
                        PROPERTIES("Tablet info") {
                            PROPERTY("Topic", Topic);
                            PROPERTY("Path", Path);
                            PROPERTY("Initialized", Inited ? "yes" : "no");
                            PROPERTY("SchemeShard", "<a href=\"?TabletID=" << SchemeShardId << "\">" << SchemeShardId << "</a>");
                            PROPERTY("PathId", PathId);
                            PROPERTY("Version", Version);
                            PROPERTY("Generation", Generation);
                        }
                    }
                    LAYOUT_COLUMN() {
                        PROPERTIES("Statistics") {
                            PROPERTY("Active pipes", Balancer->GetSessions().size());
                            PROPERTY("Active partitions", NumActiveParts);
                            PROPERTY("Total data size", AggregatedStats.TotalDataSize);
                            PROPERTY("Reserve size", PartitionReserveSize());
                            PROPERTY("Used reserve size", AggregatedStats.TotalUsedReserveSize);
                            PROPERTY("[Total/Max/Avg]WriteSpeedSec", metrics.TotalAvgWriteSpeedPerSec << "/" << metrics.MaxAvgWriteSpeedPerSec << "/" << metrics.TotalAvgWriteSpeedPerSec / NumActiveParts);
                            PROPERTY("[Total/Max/Avg]WriteSpeedMin", metrics.TotalAvgWriteSpeedPerMin << "/" << metrics.MaxAvgWriteSpeedPerMin << "/" << metrics.TotalAvgWriteSpeedPerMin / NumActiveParts);
                            PROPERTY("[Total/Max/Avg]WriteSpeedHour", metrics.TotalAvgWriteSpeedPerHour << "/" << metrics.MaxAvgWriteSpeedPerHour << "/" << metrics.TotalAvgWriteSpeedPerHour / NumActiveParts);
                            PROPERTY("[Total/Max/Avg]WriteSpeedDay", metrics.TotalAvgWriteSpeedPerDay << "/" << metrics.MaxAvgWriteSpeedPerDay << "/" << metrics.TotalAvgWriteSpeedPerDay / NumActiveParts);
                        }
                    }
                }
            }

            NAVIGATION_TAB_CONTENT("partitions") {
                auto partitionAnchor = [&](const ui32 partitionId) {
                    return TStringBuilder() << "P" << partitionId;
                };

                TABLE_CLASS("table") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() { str << "Partition"; }
                            TABLEH() { str << "Status"; }
                            TABLEH() { str << "TabletId"; }
                            TABLEH() { str << "Parents"; }
                            TABLEH() { str << "Children"; }
                            TABLEH() { str << "Size"; }
                        }
                    }
                    TABLEBODY() {
                        for (auto& [partitionId, partitionInfo] : PartitionsInfo) {
                            const auto& stats = AggregatedStats.Stats[partitionId];
                            const auto* node = PartitionGraph.GetPartition(partitionId);
                            TString style = node && node->Children.empty() ? "text-success" : "text-muted";

                            TABLER() {
                                TABLED() {
                                     DIV_CLASS_ID(style, partitionAnchor(partitionId)) {
                                         str << partitionId;
                                    }
                                }
                                TABLED() {
                                    if (node) {
                                        str << (node->Children.empty() ? "Active" : "Inactive");
                                        if (node->IsRoot()) {
                                            str << " (root)";
                                        }
                                    }
                                }
                                TABLED() { HREF(TStringBuilder() << "?TabletID=" << partitionInfo.TabletId) { str << partitionInfo.TabletId; } }
                                TABLED() {
                                    if (node) {
                                        for (auto* parent : node->Parents) {
                                            HREF("#" + partitionAnchor(parent->Id)) { str << parent->Id; }
                                            str << ", ";
                                        }
                                    }
                                }
                                TABLED() {
                                    if (node) {
                                        for (auto* child : node->Children) {
                                            HREF("#" + partitionAnchor(child->Id)) { str << child->Id; }
                                            str << ", ";
                                        }
                                    }
                                }
                                TABLED() { str << stats.DataSize; }
                            }
                        }
                    }
                }
            }

            Balancer->RenderApp(__navigationBar);
        }
    };

    return str.Str();
}

}
