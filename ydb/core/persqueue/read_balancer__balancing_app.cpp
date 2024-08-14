#include "read_balancer__balancing.h"

#include <library/cpp/monlib/service/pages/templates.h>

#define DEBUG(message)


namespace NKikimr::NPQ::NBalancing {

void TBalancer::RenderApp(TStringStream& str) const {
    auto& __stream = str;

    for (auto& [consumerName, consumer] : Consumers) {
        auto consumerAnchor = "c_" + EncodeAnchor(consumerName);

        auto familyAnchor = [&](const size_t familyId) {
            return TStringBuilder() << consumerAnchor << "_F" << familyId;
        };
        auto partitionAnchor = [&](const ui32 partitionId) {
            return TStringBuilder() << consumerAnchor << "_P" << partitionId;
        };

        DIV_CLASS_ID("tab-pane fade", consumerAnchor) {
            TABLE_CLASS("table") {
                CAPTION() { str << "Families"; }
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { str << "Id"; }
                        TABLEH() { str << "Status"; }
                        TABLEH() { str << "Partitions"; }
                        TABLEH() { str << "Session"; }
                        TABLEH() { str << "Statistics"; }
                    }
                }

                TABLEBODY() {
                    for (auto& [familyId, family] : consumer->Families) {
                        TABLER() {
                            TABLED() {  DIV_CLASS_ID("text-info", familyAnchor(familyId)) { str << familyId; } }
                            TABLED() { str << family->Status; }
                            TABLED() {
                                for (auto partitionId : family->Partitions) {
                                    HREF("#" + partitionAnchor(partitionId)) { str << partitionId; }
                                    str << ", ";
                                }
                            }
                            TABLED() { str << (family->Session ? family->Session->SessionName : ""); }
                            TABLED() { str << "Active " << family->ActivePartitionCount << " / Inactive " << family->InactivePartitionCount << " / Locked " << family->LockedPartitions.size(); }
                        }
                    }
                }
            }

            size_t free = 0;
            size_t finished = 0;
            size_t read = 0;
            size_t ready = 0;

            TABLE_CLASS("table") {
                CAPTION() { str << "Partitions"; }
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { str << "Id"; }
                        TABLEH() { str << "Family"; }
                        TABLEH() { str << "Status"; };
                        TABLEH() { str << "Parents"; }
                        TABLEH() { str << "Description"; }
                        TABLEH() { str << "P Generation"; }
                        TABLEH() { str << "P Cookie"; }
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
                                str << partitionId << " ";
                                if (partitionInfo) {
                                    HREF(TStringBuilder() << "?TabletID=" << partitionInfo->TabletId) { str << "#"; }
                                }
                            } }
                            TABLED() {
                                if (family) {
                                    HREF("#" + familyAnchor(family->Id)) { str << family->Id; }
                                }
                            }
                            TABLED() {
                                if (family) {
                                    if (partition.IsInactive()) {
                                        str << "Finished";
                                        ++finished;
                                    } else {
                                        str << "Read";
                                        ++read;
                                    }
                                } else if (consumer->IsReadable(partitionId)) {
                                    str << "Ready";
                                    ++ready;
                                } else {
                                    str << "Free";
                                    ++free;
                                }
                            }
                            TABLED() {
                                if (node) {
                                    for (auto* parent : node->Parents) {
                                        HREF("#" + partitionAnchor(parent->Id)) { str << parent->Id; }
                                        str << ", ";
                                    }
                                } else {
                                    str << "error: not found";
                                }
                            }
                            TABLED() {
                                if (partition.Commited) {
                                    str << "commited";
                                } else if (partition.ReadingFinished) {
                                    if (partition.ScaleAwareSDK) {
                                        str << "reading child";
                                    } else if (partition.StartedReadingFromEndOffset) {
                                        str << "finished";
                                    } else {
                                        str << "scheduled. iteration: " << partition.Iteration;
                                    }
                                } else if (partition.Iteration) {
                                    str << "iteration: " << partition.Iteration;
                                }
                            }
                            TABLED() { str << partition.PartitionGeneration; }
                            TABLED() { str << partition.PartitionCookie; }
                        }
                    }
                }
            }

            TABLE_CLASS("table") {
                CAPTION() { str << "Statistics"; }
                TABLEBODY() {
                    TABLER() {
                        TABLED() { str << "Free"; }
                        TABLED() { str << free; }
                    }
                    TABLER() {
                        TABLED() { str << "Ready"; }
                        TABLED() { str << ready; }
                    }
                    TABLER() {
                        TABLED() { str << "Read"; }
                        TABLED() { str << read; }
                    }
                    TABLER() {
                        TABLED() { str << "Finished"; }
                        TABLED() { str << finished; }
                    }
                    TABLER() {
                        TABLED() { STRONG() { str << "Total"; }}
                        TABLED() { str << (finished + read + ready + free); }
                    }
                }
            }

            TABLE_CLASS("table") {
                CAPTION() { str << "Sessions"; }
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { }
                        TABLEH() { str << "Id"; }
                        TABLEH() { str << "Partitions"; }
                        TABLEH() { str << "<span title=\"All families / Active / Releasing\">Families</span>"; }
                        TABLEH() { str << "<span title=\"All partitions / Active / Inactive / Releasing\">Statistics</span>"; };
                        TABLEH() { str << "Client node"; }
                        TABLEH() { str << "Proxy node"; }
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
                            TABLED() { str << ++i; }
                            TABLED() { str << session->SessionName; }
                            TABLED() { str << (session->Partitions.empty() ? "" : JoinRange(", ", session->Partitions.begin(), session->Partitions.end())); }
                            TABLED() { str << session->Families.size() << " / " << session->ActiveFamilyCount << " / " << session->ReleasingFamilyCount; }
                            TABLED() { str << (session->ActivePartitionCount + session->InactivePartitionCount) << "/ " << session->ActivePartitionCount
                                           << " / " << session->InactivePartitionCount << " / " << session->ReleasingPartitionCount; }
                            TABLED() { str << session->ClientNode; }
                            TABLED() { str << session->ProxyNodeId; }
                        }
                    }
                    TABLER() {
                        TABLED() { }
                        TABLED() { str << "<strong>Total:</strong>"; }
                        TABLED() { }
                        TABLED() { str << familyAllCount << " / " << activeFamilyCount << " / " << releasingFamilyCount; }
                        TABLED() { str << (activePartitionCount + inactivePartitionCount) << " / " << activePartitionCount << " / " << inactivePartitionCount
                                       << " / " << releasingPartitionCount; }
                        TABLED() { }
                        TABLED() { }
                    }
                }
            }
        }
    }
}

}
