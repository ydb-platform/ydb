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

        TAG(TH3) { str << "Families"; }
        DIV_CLASS_ID("tab-pane fade", consumerAnchor) {
            TABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { str << "Id"; }
                        TABLEH() { str << "Status"; }
                        TABLEH() { str << "Partitions"; }
                        TABLEH() { str << "Session"; }
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
                        }
                    }
                }
            }

            TAG(TH3) { str << "Partitions"; }
            TABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { str << "Id"; }
                        TABLEH() { str << "Family"; }
                        TABLEH() { str << "Status"; };
                        TABLEH() { str << "Parents"; }
                        TABLEH() { str << "Commited"; }
                        TABLEH() { str << "Reading finished"; }
                        TABLEH() { str << "Scale aware SDK"; }
                        TABLEH() { str << "Read from end"; }
                        TABLEH() { str << "Iteration"; }
                        TABLEH() { str << "P Generation"; }
                        TABLEH() { str << "P Cookie"; }
                    }

                    TABLEBODY() {
                        for (auto& [partitionId, partition] : consumer->Partitions) {
                            const auto* family = consumer->FindFamily(partitionId);
                            const auto* node = consumer->GetPartitionGraph().GetPartition(partitionId);
                            TString style = node && node->Children.empty() ? "text-success" : "text-muted";
                            TString status = "Free";
                            if (family) {
                                status = partition.IsInactive() ? "Finished" : "Read";
                            } else if (consumer->IsReadable(partitionId)) {
                                status = "Ready";
                            }

                            TABLER() {
                                TABLED() { DIV_CLASS_ID(style, partitionAnchor(partitionId)) { str << partitionId; } }
                                TABLED() {
                                    if (family) {
                                        HREF("#" + familyAnchor(family->Id)) { str << family->Id; }
                                    }
                                }
                                TABLED() { str << status; }
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
                                TABLED() { str << partition.Commited; }
                                TABLED() { str << partition.ReadingFinished; }
                                TABLED() { str << partition.ScaleAwareSDK; }
                                TABLED() { str << partition.StartedReadingFromEndOffset; }
                                TABLED() { str << partition.Iteration; }
                                TABLED() { str << partition.PartitionGeneration; }
                                TABLED() { str << partition.PartitionCookie; }
                            }
                        }
                    }
                }
            }
/*
            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { str << "session"; }
                        TABLEH() { str << "suspended partitions"; }
                        TABLEH() { str << "active partitions"; }
                        TABLEH() { str << "inactive partitions"; }
                        TABLEH() { str << "total partitions"; }
                    }
                }
                TABLEBODY() {

                    for (auto& session : balancerStatistcs.Sessions) {
                        TABLER() {
                            TABLED() { str << session.Session; }
                            TABLED() { str << session.SuspendedPartitionCount; }
                            TABLED() { str << session.ActivePartitionCount; }
                            TABLED() { str << session.InactivePartitionCount; }
                            TABLED() { str << session.TotalPartitionCount; }
                        }
                    }

                    TABLER() {
                        TABLED() { str << "FREE"; }
                        TABLED() { str << 0; }
                        TABLED() { str << balancerStatistcs.FreePartitions; }
                        TABLED() { str << balancerStatistcs.FreePartitions; }
                    }
                }
            }
        */
        }
    }
}

}
