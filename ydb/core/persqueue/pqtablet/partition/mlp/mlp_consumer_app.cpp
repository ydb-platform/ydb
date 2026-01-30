#include "mlp_consumer.h"
#include "mlp_storage.h"

#include <ydb/core/persqueue/common/common_app.h>


namespace NKikimr::NPQ::NMLP {

void TConsumerActor::Handle(TEvPQ::TEvMLPConsumerMonRequest::TPtr& ev) {
    auto& consumerName = ev->Get()->Consumer;
    auto& replyTo = ev->Get()->ReplyTo;

    auto& metrics = Storage->GetMetrics();

    absl::flat_hash_set<ui32> freeMessageGroups;
    if (Storage->GetKeepMessageOrder()) {
        for (auto it = Storage->begin(); it != Storage->end(); ++it) {
            auto msg = *it;
            if (msg.MessageGroupIdHash && !msg.MessageGroupIsLocked) {
                freeMessageGroups.insert(msg.MessageGroupIdHash.value());
            }
        }
    }

    TStringStream str;
    HTML_APP_PAGE(str, "MLP consumer '" << consumerName << "'") {
        NAVIGATION_BAR() {
            NAVIGATION_TAB("generic", "Generic Info");
            NAVIGATION_TAB("messages", "Messages");

            NAVIGATION_TAB_CONTENT("generic") {
                LAYOUT_ROW() {
                    LAYOUT_COLUMN() {
                        PROPERTIES("Generic") {
                            PROPERTY("First offset", Storage->GetFirstOffset());
                            PROPERTY("Last offset", Storage->GetLastOffset());
                            PROPERTY("Message counts", Storage->GetMessageCount());
                            PROPERTY("Keep message order", Storage->GetKeepMessageOrder() ? "Yes" : "No");
                            PROPERTY("Locked message groups", Storage->GetLockedMessageGroupsId().size());
                            PROPERTY("Free message groups", freeMessageGroups.size());
                        }

                        PROPERTIES("Total metrics") {
                            PROPERTY("Total committed messages", metrics.TotalCommittedMessageCount);
                            PROPERTY("Total moved to DLQ messages", metrics.TotalMovedToDLQMessageCount);
                            PROPERTY("Total scheduled to DLQ messages", metrics.TotalScheduledToDLQMessageCount);
                            PROPERTY("Total purged messages", metrics.TotalPurgedMessageCount);
                            PROPERTY("Total deleted by deadline policy messages", metrics.TotalDeletedByDeadlinePolicyMessageCount);
                            PROPERTY("Total deleted by retention messages", metrics.TotalDeletedByRetentionMessageCount);
                        }

                        PROPERTIES("Inflight metrics") {
                            PROPERTY("Inflight count", metrics.InflightMessageCount);
                            PROPERTY("Inflight unlocked count", metrics.UnprocessedMessageCount);
                            PROPERTY("Inflight locked count", metrics.LockedMessageCount);
                            PROPERTY("Inflight locked group count", metrics.LockedMessageGroupCount);
                            PROPERTY("Inflight delayed count", metrics.DelayedMessageCount);
                            PROPERTY("Inflight committed count", metrics.CommittedMessageCount);
                            PROPERTY("Inflight scheduled to DLQ count", metrics.DLQMessageCount);
                            PROPERTY("Inflight deadline expired count", metrics.DeadlineExpiredMessageCount);
                        }
                    }

                    LAYOUT_COLUMN() {
                        PROPERTIES("Message locks") {
                            for (size_t i = 0; i < metrics.MessageLocks.GetRangeCount(); ++i) {
                                PROPERTY(metrics.MessageLocks.GetRangeName(i), metrics.MessageLocks.GetRangeValue(i));
                            }
                        }
                        PROPERTIES("Message locking duration") {
                            for (size_t i = 0; i < metrics.MessageLockingDuration.GetRangeCount(); ++i) {
                                PROPERTY(metrics.MessageLockingDuration.GetRangeName(i), metrics.MessageLockingDuration.GetRangeValue(i));
                            }
                        }
                        PROPERTIES("Waiting locking duration") {
                            for (size_t i = 0; i < metrics.WaitingLockingDuration.GetRangeCount(); ++i) {
                                PROPERTY(metrics.WaitingLockingDuration.GetRangeName(i), metrics.WaitingLockingDuration.GetRangeValue(i));
                            }
                        }
                    }
                }
            }

            NAVIGATION_TAB_CONTENT("messages") {
                LAYOUT_ROW() {
                    LAYOUT_COLUMN() {
                        TABLE_CLASS("table") {
                            CAPTION() {str << "Messages";}
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() {str << "";}
                                    TABLEH() {str << "Zone";}
                                    TABLEH() {str << "Offset";}
                                    TABLEH() {str << "Status";}
                                    TABLEH() {str << "Write Timestamp";}
                                    TABLEH() {str << "Group ID";}
                                    TABLEH() {str << "Processing Count";}
                                    TABLEH() {str << "Processing Deadline";}
                                    TABLEH() {str << "Locking Timestamp";}
                                }
                            }
                            TABLEBODY() {
                                size_t i = 0;
                                for (auto it = Storage->begin(); it != Storage->end(); ++it) {
                                    auto message = *it;
                                    TABLER() {
                                        TABLED() { str << ++i; }
                                        TABLED() { str << (message.SlowZone ? "S" : "F"); }
                                        TABLED() { str << message.Offset; }
                                        TABLED() { str << message.Status; }
                                        TABLED() { str << message.WriteTimestamp; }
                                        TABLED() {
                                            if (message.MessageGroupIdHash) {
                                                auto color = message.MessageGroupIsLocked ? "red" : "green";
                                                str << "<span style=\"color: " << color << "\">" << message.MessageGroupIdHash.value() << "</span>";
                                            }
                                        }
                                        TABLED() { str << message.ProcessingCount; }
                                        TABLED() { str << (TInstant::Zero() == message.ProcessingDeadline ? "" : message.ProcessingDeadline.ToString()); }
                                        TABLED() { str << (TInstant::Zero() == message.LockingTimestamp ? "" : message.LockingTimestamp.ToString()); }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Send(replyTo, new NMon::TEvRemoteHttpInfoRes(str.Str()));
}

} // namespace NKikimr::NPQ::NMLP
