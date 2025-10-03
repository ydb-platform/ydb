#include <ydb/core/persqueue/pqtablet/common/event_helpers.h>
#include <ydb/core/persqueue/common/common_app.h>
#include "partition_compactification.h"
#include "partition_util.h"
#include "partition.h"
#include <ydb/core/persqueue/pqtablet/cache/read.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/path.h>
#include <ydb/core/quoter/public/quoter.h>
#include <ydb/core/protos/counters_pq.pb.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/library/protobuf_printer/security_printer.h>
#include <ydb/public/lib/base/msgbus.h>
#include <library/cpp/html/pcdata/pcdata.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/time_provider/time_provider.h>
#include <util/folder/path.h>
#include <util/string/escape.h>
#include <util/system/byteorder.h>

namespace NKikimr::NPQ {

void TPartition::HandleMonitoring(TEvPQ::TEvMonRequest::TPtr& ev, const TActorContext& ctx) {
    auto GetPartitionState = [&]() {
        if (CurrentStateFunc() == &TThis::StateInit) {
            return "Init";
        } else if (CurrentStateFunc() == &TThis::StateIdle) {
            return "Idle";
        } else {
            Y_ABORT("");
        }
    };
    static constexpr std::pair<TStringBuf, const TPartitionBlobEncoder TPartition::*> encoders[2]{
        {"Compacted"sv, &TPartition::CompactionBlobEncoder},
        {"FastWrite"sv, &TPartition::BlobEncoder},
    };

    TStringStream out;
    HTML_PART(out) {
        NAVIGATION_TAB_CONTENT_PART("partition_" << Partition.InternalPartitionId) {
            LAYOUT_ROW() {
                LAYOUT_COLUMN() {
                    PROPERTIES("General") {
                        PROPERTY("Partition", Partition);
                        PROPERTY("State", GetPartitionState());
                        PROPERTY("CreationTime", CreationTime.ToStringLocalUpToSeconds());
                        PROPERTY("InitDuration", InitDuration.ToString());
                    }

                    PROPERTIES("Status") {
                        PROPERTY("State", NKikimrPQ::ETopicPartitionStatus_Name(PartitionConfig->GetStatus()));
                        PROPERTY("Disk", (DiskIsFull ? "Full" : "Normal"));
                        PROPERTY("Quota", (WaitingForSubDomainQuota() ? "Out of space" : "Normal"));
                    }

                    PROPERTIES("Information") {
                        PROPERTY("Total partition size, bytes", Size());
                        PROPERTY("Total message count", (GetEndOffset() - GetStartOffset()));
                        PROPERTY("StartOffset", GetStartOffset());
                        PROPERTY("EndOffset", GetEndOffset());
                        PROPERTY("Last message WriteTimestamp", EndWriteTimestamp.ToRfc822String());
                        PROPERTY("HeadOffset", BlobEncoder.Head.Offset << ", count: " << BlobEncoder.Head.GetCount());
                    }

                    if (Compacter) {
                        auto step = [&]() {
                            switch(Compacter->Step) {
                                case TPartitionCompaction::EStep::READING:
                                    return "Reading";
                                case TPartitionCompaction::EStep::COMPACTING:
                                    return "Compacting";
                                case TPartitionCompaction::EStep::PENDING:
                                    return "Pending";
                            }
                        };

                        PROPERTIES("Compaction") {
                            PROPERTY("Step", step());
                            PROPERTY("First uncompacted offset", Compacter->FirstUncompactedOffset);

                            if (Compacter->ReadState) {
                                PROPERTY("OffsetToRead", Compacter->ReadState->OffsetToRead);
                                PROPERTY("LastOffset", Compacter->ReadState->LastOffset);
                                PROPERTY("NextPartNo", Compacter->ReadState->NextPartNo);
                                PROPERTY("SkipOffset", Compacter->ReadState->SkipOffset);
                                PROPERTY("TopicData (size)", Compacter->ReadState->TopicData.size());
                            }

                            if (Compacter->CompactState) {
                                PROPERTY("MaxOffset", Compacter->CompactState->MaxOffset);
                                PROPERTY("TopicData (size)", Compacter->CompactState->TopicData.size());
                                PROPERTY("LastProcessedOffset", Compacter->CompactState->LastProcessedOffset);
                                PROPERTY("CommitCookie", Compacter->CompactState->CommitCookie);
                                PROPERTY("FirstHeadOffset", Compacter->CompactState->FirstHeadOffset);
                                PROPERTY("FirstHeadPartNo", Compacter->CompactState->FirstHeadPartNo);
                                PROPERTY("CommittedOffset", Compacter->CompactState->CommittedOffset);
                                PROPERTY("SkipOffset", Compacter->CompactState->SkipOffset);
                                PROPERTY("DataKeysBody (size)", Compacter->CompactState->DataKeysBody.size());
                                PROPERTY("UpdatedKeys (size)", Compacter->CompactState->UpdatedKeys.size());
                                PROPERTY("DeletedKeys (size)", Compacter->CompactState->DeletedKeys.size());
                                PROPERTY("EmptyBlobs (size)", Compacter->CompactState->EmptyBlobs.size());
                                PROPERTY("LastBatchKey", Compacter->CompactState->LastBatchKey.ToString());
                            }

                            PROPERTY("PartitionRequestInflight", CompacterPartitionRequestInflight);
                            PROPERTY("KvRequestInflight", CompacterKvRequestInflight);
                        }
                    } else {
                        PROPERTIES("Compaction") {
                            PROPERTY("State", "Disabled");
                        }
                    }
                }

                LAYOUT_COLUMN() {
                    PROPERTIES("Runtime information") {
                        PROPERTY("WriteInflightSize", WriteInflightSize);
                        PROPERTY("ReservedBytesSize", ReservedSize);
                        PROPERTY("OwnerPipes", OwnerPipes.size());
                        PROPERTY("Owners", Owners.size());
                        PROPERTY("Currently writing", Responses.size());
                        PROPERTY("MaxCurrently writing", BlobEncoder.MaxWriteResponsesSize);
                        for (const auto& [encoderName, encoderPtr] : encoders) {
                            const TPartitionBlobEncoder& encoder = this->*encoderPtr;
                            PROPERTY(TString::Join(encoderName, " DataKeysBody size"), encoder.DataKeysBody.size());
                        }
                    }


                    for (const auto& [encoderName, encoderPtr] : encoders) {
                        const TPartitionBlobEncoder& encoder = this->*encoderPtr;
                        PROPERTIES(TString::Join(encoderName, " DataKeysHead size")) {
                            for (ui32 i = 0; i < encoder.DataKeysHead.size(); ++i) {
                                PROPERTY(TStringBuilder() << i, encoder.DataKeysHead[i].KeysCount() << " sum: " << encoder.DataKeysHead[i].Sum()
                                    << " border: " << encoder.DataKeysHead[i].Border() << " recs: " << encoder.DataKeysHead[i].RecsCount()
                                    << " intCount: " << encoder.DataKeysHead[i].InternalPartsCount());
                            }
                        }
                    }

                    PROPERTIES("AvgWriteSize, bytes") {
                        for (auto& avg : AvgWriteBytes) {
                            PROPERTY(avg.GetDuration().ToString(), avg.GetValue());
                        }
                    }
                }
            }

            LAYOUT_ROW() {
                LAYOUT_COLUMN() {
                    CONFIGURATION(SecureDebugStringMultiline(Config));
                }
            }

            LAYOUT_ROW() {
                LAYOUT_COLUMN() {
                    TABLE_CLASS("table") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() {out << "Type";}
                                TABLEH() {out << "Pos";}
                                TABLEH() {out << "timestamp";}
                                TABLEH() {out << "Offset";}
                                TABLEH() {out << "PartNo";}
                                TABLEH() {out << "Count";}
                                TABLEH() {out << "InternalPartsCount";}
                                TABLEH() {out << "Size";}
                            }
                        }
                        TABLEBODY() {
                            ui32 i = 0;
                            for (const auto& [encoderName, encoderPtr] : encoders) {
                                const TPartitionBlobEncoder& encoder = this->*encoderPtr;
                                for (const auto& d : encoder.DataKeysBody) {
                                    TABLER() {
                                        TABLED() {out << encoderName << "DataBody";}
                                        TABLED() {out << i++;}
                                        TABLED() {out << ToStringLocalTimeUpToSeconds(d.Timestamp);}
                                        TABLED() {out << d.Key.GetOffset();}
                                        TABLED() {out << d.Key.GetPartNo();}
                                        TABLED() {out << d.Key.GetCount();}
                                        TABLED() {out << d.Key.GetInternalPartsCount();}
                                        TABLED() {out << d.Size;}
                                    }
                                }
                                ui32 currentLevel = 0;
                                const auto& headKeys = encoder.HeadKeys;
                                for (ui32 p = 0; p < headKeys.size(); ++p) {
                                    ui32 size  = headKeys[p].Size;
                                    while (currentLevel + 1 < TotalLevels && size < CompactLevelBorder[currentLevel + 1])
                                        ++currentLevel;
                                    PQ_ENSURE(size < CompactLevelBorder[currentLevel]);
                                    TABLER() {
                                        TABLED() {out << encoderName << "DataHead[" << currentLevel << "]";}
                                        TABLED() {out << i++;}
                                        TABLED() {out << ToStringLocalTimeUpToSeconds(headKeys[p].Timestamp);}
                                        TABLED() {out << headKeys[p].Key.GetOffset();}
                                        TABLED() {out << headKeys[p].Key.GetPartNo();}
                                        TABLED() {out << headKeys[p].Key.GetCount();}
                                        TABLED() {out << headKeys[p].Key.GetInternalPartsCount();}
                                        TABLED() {out << size;}
                                    }
                                }
                            }
                        }
                    }

                    TABLE_CLASS("table") {
                        CAPTION() { out << "Gaps"; }
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() {out << "GapStartOffset";}
                                TABLEH() {out << "GapEndOffset";}
                                TABLEH() {out << "GapSize";}
                                TABLEH() {out << "id";}
                            }
                        }
                        ui32 i = 0;
                        TABLEBODY() {
                            for (const auto& d : GapOffsets) {
                                TABLER() {
                                    TABLED() {out << d.first;}
                                    TABLED() {out << d.second;}
                                    TABLED() {out << (d.second - d.first);}
                                    TABLED() {out << (i++);}
                                }
                            }
                            for (const auto& [encoderName, encoderPtr] : encoders) {
                                const TPartitionBlobEncoder& encoder = this->*encoderPtr;
                                if (!encoder.DataKeysBody.empty() && encoder.DataKeysBody.back().Key.GetOffset() + encoder.DataKeysBody.back().Key.GetCount() < encoder.Head.Offset) {
                                    TABLER() {
                                        TABLED() {out << (encoder.DataKeysBody.back().Key.GetOffset() + encoder.DataKeysBody.back().Key.GetCount());}
                                        TABLED() {out << encoder.Head.Offset;}
                                        TABLED() {out << (encoder.Head.Offset - (encoder.DataKeysBody.back().Key.GetOffset() + encoder.DataKeysBody.back().Key.GetCount()));}
                                        TABLED() {out << (i++);}
                                    }
                                }
                            }
                        }
                    }

                    TABLE_CLASS("table") {
                        CAPTION() { out << "Source ids"; }
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() {out << "SourceId";}
                                TABLEH() {out << "SeqNo";}
                                TABLEH() {out << "Offset";}
                                TABLEH() {out << "WriteTimestamp";}
                                TABLEH() {out << "CreateTimestamp";}
                                TABLEH() {out << "Explicit";}
                                TABLEH() {out << "State";}
                                TABLEH() {out << "LastHeartbeat";}
                            }
                        }
                        TABLEBODY() {
                            for (const auto& [sourceId, sourceIdInfo]: SourceIdStorage.GetInMemorySourceIds()) {
                                TABLER() {
                                    TABLED() {out << EncodeHtmlPcdata(EscapeC(sourceId));}
                                    TABLED() {out << sourceIdInfo.SeqNo;}
                                    TABLED() {out << sourceIdInfo.Offset;}
                                    TABLED() {out << ToStringLocalTimeUpToSeconds(sourceIdInfo.WriteTimestamp);}
                                    TABLED() {out << ToStringLocalTimeUpToSeconds(sourceIdInfo.CreateTimestamp);}
                                    TABLED() {out << (sourceIdInfo.Explicit ? "true" : "false");}
                                    TABLED() {out << sourceIdInfo.State;}
                                    if (const auto& hb = sourceIdInfo.LastHeartbeat) {
                                        TABLED() {out << hb->Version;}
                                    } else {
                                        TABLED() {out << "null";}
                                    }
                                }
                            }
                        }
                    }

                    TABLE_CLASS("table") {
                        CAPTION() { out << "UsersInfo"; }
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() {out << "user";}
                                TABLEH() {out << "offset";}
                                TABLEH() {out << "lag";}
                                TABLEH() {out << "ReadFromTimestamp";}
                                TABLEH() {out << "WriteTimestamp";}
                                TABLEH() {out << "CreateTimestamp";}
                                TABLEH() {out << "ReadOffset";}
                                TABLEH() {out << "ReadWriteTimestamp";}
                                TABLEH() {out << "ReadCreateTimestamp";}
                                TABLEH() {out << "ReadOffsetRewindSum";}
                                TABLEH() {out << "ActiveReads";}
                                TABLEH() {out << "Subscriptions";}
                            }
                        }
                        TABLEBODY() {
                            for (auto&& [user, userInfo]: UsersInfoStorage->GetAll()) {
                                auto snapshot = CreateSnapshot(userInfo);
                                TABLER() {
                                    TABLED() {out << EncodeHtmlPcdata(user);}
                                    TABLED() {out << userInfo.Offset;}
                                    TABLED() {out << (GetEndOffset() - userInfo.Offset);}
                                    TABLED() {out << ToStringLocalTimeUpToSeconds(userInfo.ReadFromTimestamp);}
                                    TABLED() {out << ToStringLocalTimeUpToSeconds(snapshot.LastCommittedMessage.WriteTimestamp);}
                                    TABLED() {out << ToStringLocalTimeUpToSeconds(snapshot.LastCommittedMessage.WriteTimestamp);}
                                    TABLED() {out << (userInfo.GetReadOffset());}
                                    TABLED() {out << ToStringLocalTimeUpToSeconds(snapshot.LastReadMessage.WriteTimestamp);}
                                    TABLED() {out << ToStringLocalTimeUpToSeconds(snapshot.LastReadMessage.CreateTimestamp);}
                                    TABLED() {out << (userInfo.ReadOffsetRewindSum);}
                                    TABLED() {out << userInfo.ActiveReads;}
                                    TABLED() {out << userInfo.Subscriptions;}
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    ctx.Send(ev->Sender, new TEvPQ::TEvMonResponse(Partition, out.Str()));
}

} // namespace NKikimr::NPQ
