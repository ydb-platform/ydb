#include "event_helpers.h"
#include "mirrorer.h"
#include "partition_util.h"
#include "partition.h"
#include "read.h"

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

void HtmlOutput(IOutputStream& out, const TString& line, const std::deque<std::pair<TKey, ui32>>& keys) {
    HTML(out) {
        TABLE() {
        TABLEHEAD() {
            TABLER() {
                TABLEH() {out << line;}
            }
        }
        TABLEBODY() {
            TABLER() {
                TABLEH() {out << "offset";}
                for (auto& p: keys) {
                    TABLED() {out << p.first.GetOffset();}
                }
            }
            TABLER() {
                TABLEH() {out << "partNo";}
                for (auto& p: keys) {
                    TABLED() {out << p.first.GetPartNo();}
                }
            }
            TABLER() {
                TABLEH() {out << "size";}
                for (auto& p: keys) {
                    TABLED() {out << p.second;}
                }
            }
        }
        }
    }
}

IOutputStream& operator <<(IOutputStream& out, const TKeyLevel& value) {
    TStringStream str;
    str << "count=" << value.Keys_.size() << " sum=" << value.Sum_ << " border=" << value.Border_ << " recs= " << value.RecsCount_ << ":";
    HtmlOutput(out, str.Str(), value.Keys_);
    return out;
}

void TPartition::HandleMonitoring(TEvPQ::TEvMonRequest::TPtr& ev, const TActorContext& ctx) {
    TVector<TString> res;
    TString str;
    if (CurrentStateFunc() == &TThis::StateInit) {
        str = "State is StateInit";
    } else if (CurrentStateFunc() == &TThis::StateIdle) {
        str = "State is StateIdle";
    } else {
        Y_ABORT("");
    }
    TStringStream out;
    out << "Partition " << Partition << ": " << str;  res.push_back(out.Str()); out.Clear();
    if (DiskIsFull) {
        out << "DISK IS FULL";
        res.push_back(out.Str());
        out.Clear();
    }
    if (WaitingForSubDomainQuota(ctx)) {
        out << "SubDomain is out of space";
        res.push_back(out.Str());
        out.Clear();
    }
    out << "StartOffset: " << StartOffset; res.push_back(out.Str()); out.Clear();
    out << "EndOffset: " << EndOffset; res.push_back(out.Str()); out.Clear();
    out << "CreationTime: " << CreationTime.ToStringLocalUpToSeconds(); res.push_back(out.Str()); out.Clear();
    out << "InitDuration: " << InitDuration.ToString(); res.push_back(out.Str()); out.Clear();
    out << "TotalCount: " << (Head.GetNextOffset() - StartOffset); res.push_back(out.Str()); out.Clear();
    out << "TotalSize: " << Size(); res.push_back(out.Str()); out.Clear();
    out << "LastOffset: " << (Head.GetNextOffset()); res.push_back(out.Str()); out.Clear();
    out << "HeadOffset: " << Head.Offset << ", count: " << Head.GetCount(); res.push_back(out.Str()); out.Clear();
    out << "WriteInflightSize: " << WriteInflightSize; res.push_back(out.Str()); out.Clear();
    out << "ReservedBytesSize: " << ReservedSize; res.push_back(out.Str()); out.Clear();
    out << "OwnerPipes: " << OwnerPipes.size(); res.push_back(out.Str()); out.Clear();
    out << "Owners: " << Owners.size(); res.push_back(out.Str()); out.Clear();
    out << "Currently writing: " << Responses.size(); res.push_back(out.Str()); out.Clear();
    out << "MaxCurrently writing: " << MaxWriteResponsesSize; res.push_back(out.Str()); out.Clear();
    out << "DataKeysBody size: " << DataKeysBody.size(); res.push_back(out.Str()); out.Clear();
    for (ui32 i = 0; i < DataKeysHead.size(); ++i) {
        out << "DataKeysHead[" << i << "] size: " << DataKeysHead[i].KeysCount() << " sum: " << DataKeysHead[i].Sum()
            << " border: " << DataKeysHead[i].Border() << " recs: " << DataKeysHead[i].RecsCount() << " intCount: " << DataKeysHead[i].InternalPartsCount();
        res.push_back(out.Str()); out.Clear();
    }
    for (auto& avg : AvgWriteBytes) {
        out << "AvgWriteSize per " << avg.GetDuration().ToString() << " is " << avg.GetValue() << " bytes";
        res.push_back(out.Str()); out.Clear();
    }
    out << SecureDebugString(Config); res.push_back(out.Str()); out.Clear();
    HTML(out) {
        DIV_CLASS_ID("tab-pane fade", Sprintf("partition_%u", Partition.InternalPartitionId)) {
            TABLE_SORTABLE_CLASS("table") {
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
                    for (auto& d: DataKeysBody) {
                        TABLER() {
                            TABLED() {out << "DataBody";}
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
                    for (ui32 p = 0; p < HeadKeys.size(); ++p) {
                        ui32 size  = HeadKeys[p].Size;
                        while (currentLevel + 1 < TotalLevels && size < CompactLevelBorder[currentLevel + 1])
                            ++currentLevel;
                        Y_ABORT_UNLESS(size < CompactLevelBorder[currentLevel]);
                        TABLER() {
                            TABLED() {out << "DataHead[" << currentLevel << "]";}
                            TABLED() {out << i++;}
                            TABLED() {out << ToStringLocalTimeUpToSeconds(HeadKeys[p].Timestamp);}
                            TABLED() {out << HeadKeys[p].Key.GetOffset();}
                            TABLED() {out << HeadKeys[p].Key.GetPartNo();}
                            TABLED() {out << HeadKeys[p].Key.GetCount();}
                            TABLED() {out << HeadKeys[p].Key.GetInternalPartsCount();}
                            TABLED() {out << size;}
                        }
                    }
                }
            }

            TABLE_SORTABLE_CLASS("table") {
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
                    for (auto& d: GapOffsets) {
                        TABLER() {
                            TABLED() {out << d.first;}
                            TABLED() {out << d.second;}
                            TABLED() {out << (d.second - d.first);}
                            TABLED() {out << (i++);}
                        }
                    }
                    if (!DataKeysBody.empty() && DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount() < Head.Offset) {
                        TABLER() {
                            TABLED() {out << (DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount());}
                            TABLED() {out << Head.Offset;}
                            TABLED() {out << (Head.Offset - (DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount()));}
                            TABLED() {out << (i++);}
                        }

                    }
                }
            }


            TABLE_SORTABLE_CLASS("table") {
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
            TABLE_SORTABLE_CLASS("table") {
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
                    for (auto& d: UsersInfoStorage->GetAll()) {
                        TABLER() {
                            TABLED() {out << EncodeHtmlPcdata(d.first);}
                            TABLED() {out << d.second.Offset;}
                            TABLED() {out << (EndOffset - d.second.Offset);}
                            TABLED() {out << ToStringLocalTimeUpToSeconds(d.second.ReadFromTimestamp);}
                            TABLED() {out << ToStringLocalTimeUpToSeconds(d.second.WriteTimestamp);}
                            TABLED() {out << ToStringLocalTimeUpToSeconds(d.second.CreateTimestamp);}
                            TABLED() {out << (d.second.GetReadOffset());}
                            TABLED() {out << ToStringLocalTimeUpToSeconds(d.second.GetReadWriteTimestamp());}
                            TABLED() {out << ToStringLocalTimeUpToSeconds(d.second.GetReadCreateTimestamp());}
                            TABLED() {out << (d.second.ReadOffsetRewindSum);}
                            TABLED() {out << d.second.ActiveReads;}
                            TABLED() {out << d.second.Subscriptions;}
                        }
                    }
                }
            }
        }
    }

    ctx.Send(ev->Sender, new TEvPQ::TEvMonResponse(Partition, res, out.Str()));
}

} // namespace NKikimr::NPQ
