#include "read_balancer__balancing.h"

#include "common_app.h"

#include <library/cpp/monlib/service/pages/templates.h>

#define DEBUG(message)


namespace NKikimr::NPQ::NBalancing {

void TBalancer::RenderApp(NApp::TNavigationBar& __navigationBar) const {
    IOutputStream& __stream = __navigationBar;

    for (auto& [consumerName, consumer] : Consumers) {
        auto consumerAnchor = "c_" + EncodeAnchor(consumerName);

        auto familyAnchor = [&](const size_t familyId) {
            return TStringBuilder() << consumerAnchor << "_F" << familyId;
        };
        auto partitionAnchor = [&](const ui32 partitionId) {
            return TStringBuilder() << consumerAnchor << "_P" << partitionId;
        };

        NAVIGATION_TAB_CONTENT(consumerAnchor) {
            TABLE_CLASS("table") {
                CAPTION() { __stream << "Families"; }
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { __stream << "Id"; }
                        TABLEH() { __stream << "Status"; }
                        TABLEH() { __stream << "Partitions"; }
                        TABLEH() { __stream << "Session"; }
                        TABLEH() { __stream << "Statistics"; }
                    }
                }

                TABLEBODY() {
                    for (auto& [familyId, family] : consumer->Families) {
                        TABLER() {
                            TABLED() {  DIV_CLASS_ID("text-info", familyAnchor(familyId)) { __stream << familyId; } }
                            TABLED() { __stream << family->Status; }
                            TABLED() {
                                for (auto partitionId : family->Partitions) {
                                    HREF("#" + partitionAnchor(partitionId)) { __stream << partitionId; }
                                    __stream << ", ";
                                }
                            }
                            TABLED() { __stream << (family->Session ? family->Session->SessionName : ""); }
                            TABLED() { __stream << "Active " << family->ActivePartitionCount << " / Inactive " << family->InactivePartitionCount << " / Locked " << family->LockedPartitions.size(); }
                        }
                    }
                }
            }

            size_t free = 0;
            size_t finished = 0;
            size_t read = 0;
            size_t ready = 0;

            TABLE_CLASS("table") {
                CAPTION() { __stream << "Partitions"; }
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { __stream << "Id"; }
                        TABLEH() { __stream << "Family"; }
                        TABLEH() { __stream << "Status"; };
                        TABLEH() { __stream << "Parents"; }
                        TABLEH() { __stream << "Description"; }
                        TABLEH() { __stream << "P Generation"; }
                        TABLEH() { __stream << "P Cookie"; }
                    }
                }

                TABLEBODY() {
                    for (auto& [partitionId, partition] : consumer->Partitions) {
                        const auto* family = consumer->FindFamily(partitionId);
                        const auto* node = consumer->GetPartitionGraph().GetPartition(partitionId);
                        TString style = node && node->Children.empty() ? "text-success" : "text-muted";
                        auto* partitionInfo = GetPartitionInfo(partitionId);

                        TABLER() {
                            TABLED() { DIV_CLASS_ID(style, partitionAnchor(partitionId)) {
                                __stream << partitionId << " ";
                                if (partitionInfo) {
                                    HREF(TStringBuilder() << "?TabletID=" << partitionInfo->TabletId) { __stream << "#"; }
                                }
                            } }
                            TABLED() {
                                if (family) {
                                    HREF("#" + familyAnchor(family->Id)) { __stream << family->Id; }
                                }
                            }
                            TABLED() {
                                if (family) {
                                    if (partition.IsInactive()) {
                                        __stream << "Finished";
                                        ++finished;
                                    } else {
                                        __stream << "Read";
                                        ++read;
                                    }
                                } else if (consumer->IsReadable(partitionId)) {
                                    __stream << "Ready";
                                    ++ready;
                                } else {
                                    __stream << "Free";
                                    ++free;
                                }
                            }
                            TABLED() {
                                if (node) {
                                    for (auto* parent : node->Parents) {
                                        HREF("#" + partitionAnchor(parent->Id)) { __stream << parent->Id; }
                                        __stream << ", ";
                                    }
                                } else {
                                    __stream << "error: not found";
                                }
                            }
                            TABLED() {
                                if (partition.Commited) {
                                    __stream << "commited";
                                } else if (partition.ReadingFinished) {
                                    if (partition.ScaleAwareSDK) {
                                        __stream << "reading child";
                                    } else if (partition.StartedReadingFromEndOffset) {
                                        __stream << "finished";
                                    } else {
                                        __stream << "scheduled. iteration: " << partition.Iteration;
                                    }
                                } else if (partition.Iteration) {
                                    __stream << "iteration: " << partition.Iteration;
                                }
                            }
                            TABLED() { __stream << partition.PartitionGeneration; }
                            TABLED() { __stream << partition.PartitionCookie; }
                        }
                    }
                }
            }

            TABLE_CLASS("table") {
                CAPTION() { __stream << "Statistics"; }
                TABLEBODY() {
                    TABLER() {
                        TABLED() { __stream << "Free"; }
                        TABLED() { __stream << free; }
                    }
                    TABLER() {
                        TABLED() { __stream << "Ready"; }
                        TABLED() { __stream << ready; }
                    }
                    TABLER() {
                        TABLED() { __stream << "Read"; }
                        TABLED() { __stream << read; }
                    }
                    TABLER() {
                        TABLED() { __stream << "Finished"; }
                        TABLED() { __stream << finished; }
                    }
                    TABLER() {
                        TABLED() { STRONG() { __stream << "Total"; }}
                        TABLED() { __stream << (finished + read + ready + free); }
                    }
                }
            }

            TABLE_CLASS("table") {
                CAPTION() { __stream << "Sessions"; }
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { }
                        TABLEH() { __stream << "Id"; }
                        TABLEH() { __stream << "Partitions"; }
                        TABLEH() { __stream << "<span title=\"All families / Active / Releasing\">Families</span>"; }
                        TABLEH() { __stream << "<span title=\"All partitions / Active / Inactive / Releasing\">Statistics</span>"; };
                        TABLEH() { __stream << "Client node"; }
                        TABLEH() { __stream << "Proxy node"; }
                    }
                }
                TABLEBODY() {
                    size_t familyAllCount = 0;
                    size_t activeFamilyCount = 0;
                    size_t releasingFamilyCount = 0;
                    size_t activePartitionCount = 0;
                    size_t inactivePartitionCount = 0;
                    size_t releasingPartitionCount = 0;

                    size_t i = 0;
                    for (auto& [pipe, session] : Sessions) {
                        if (session->ClientId != consumerName) {
                            continue;
                        }

                        familyAllCount += session->Families.size();
                        activeFamilyCount += session->ActiveFamilyCount;
                        releasingFamilyCount += session->ReleasingFamilyCount;
                        activePartitionCount += session->ActivePartitionCount;
                        inactivePartitionCount += session->InactivePartitionCount;
                        releasingPartitionCount += session->ReleasingPartitionCount;

                        TABLER() {
                            TABLED() { __stream << ++i; }
                            TABLED() { __stream << session->SessionName; }
                            TABLED() { __stream << (session->Partitions.empty() ? "" : JoinRange(", ", session->Partitions.begin(), session->Partitions.end())); }
                            TABLED() { __stream << session->Families.size() << " / " << session->ActiveFamilyCount << " / " << session->ReleasingFamilyCount; }
                            TABLED() { __stream << (session->ActivePartitionCount + session->InactivePartitionCount + session->ReleasingPartitionCount)
                                           << " / " << session->ActivePartitionCount << " / " << session->InactivePartitionCount << " / " << session->ReleasingPartitionCount; }
                            TABLED() { __stream << session->ClientNode; }
                            TABLED() { __stream << session->ProxyNodeId; }
                        }
                    }
                    TABLER() {
                        TABLED() { }
                        TABLED() { __stream << "<strong>Total:</strong>"; }
                        TABLED() { }
                        TABLED() { __stream << familyAllCount << " / " << activeFamilyCount << " / " << releasingFamilyCount; }
                        TABLED() { __stream << (activePartitionCount + inactivePartitionCount + releasingPartitionCount) << " / " << activePartitionCount << " / "
                                       << inactivePartitionCount << " / " << releasingPartitionCount; }
                        TABLED() { }
                        TABLED() { }
                    }
                }
            }
        }
    }
}

}
