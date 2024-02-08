#pragma once

#include "defs.h"

#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/protos/cms.pb.h>

namespace NKikimr::NCms {

template <NKikimrCms::ETextFormat format>
class TLogFormatter {
public:
    static TString Format(const NKikimrCms::TLogRecord &rec) {
        switch (rec.GetRecordType()) {
        case NKikimrCms::TLogRecordData::CMS_LOADED:
            return FormatData(rec.GetData().GetCmsLoaded());
            break;
        case NKikimrCms::TLogRecordData::PDISK_MONITOR_ACTION:
            return FormatData(rec.GetData().GetPDiskMonitorAction());
            break;
        case NKikimrCms::TLogRecordData::MARKERS_MODIFICATION:
            return FormatData(rec.GetData().GetMarkersModification());
            break;
        default:
            return "[unsupported record data type]";
        }
    }

    template <NKikimrCms::ETextFormat U> friend class TLogFormatter;

private:
    TLogFormatter() {};

    template <typename T>
    static TString FormatData(const T &data) {
        Y_UNUSED(data);
        return "[unsupported record data type]";
    }

    static void OutputItem(IOutputStream &os, const NKikimrCms::TLogRecordData::TMarkersModification &data) {
        if (data.HasHost())
            os << "host " << data.GetHost();
        else if (data.HasNode())
            os << "node " << data.GetNode();
        else if (data.HasPDisk())
            os << "PDisk " <<  data.GetPDisk().GetNodeId() << ":" << data.GetPDisk().GetDiskId();
        else if (data.HasVDisk())
            os << "VDisk " << VDiskIDFromVDiskID(data.GetVDisk()).ToString();
    }

    static void OutputMarkers(IOutputStream &os, const ::google::protobuf::RepeatedField<int> &list) {
        if (list.size()) {
            for (auto &elem : list)
                os << " " << static_cast<NKikimrCms::EMarker>(elem);
        } else {
            os << " <empty list>";
        }
    }

    static void OutputDowntimes(IOutputStream &os, const TString &prefix, const NKikimrCms::TAvailabilityStats &stats) {
        os << prefix;
        for (auto &entry : stats.GetDowntimes()) {
            TInstant start = TInstant::FromValue(entry.GetStart());
            TInstant end = TInstant::FromValue(entry.GetEnd());
            os << " [" << start.ToStringLocalUpToSeconds()
               << " - " << end.ToStringLocalUpToSeconds()
               << ": " << entry.GetExplanation() << "]";
        }
        TDuration gap = TDuration::FromValue(stats.GetIgnoredDowntimeGap());
        os << " ignored gap: " << gap.ToString();
    }
};

template <> template <>
TString TLogFormatter<NKikimrCms::TEXT_FORMAT_SHORT>::FormatData(const NKikimrCms::TLogRecordData::TCmsLoaded &data) {
    TStringStream ss;
    ss << "CMS loaded at " << data.GetHost()
       << " (" << data.GetNodeId() << ")"
       << " " << data.GetVersion();
    return ss.Str();
}

template <> template <>
TString TLogFormatter<NKikimrCms::TEXT_FORMAT_DETAILED>::FormatData(const NKikimrCms::TLogRecordData::TCmsLoaded &data) {
    return TLogFormatter<NKikimrCms::TEXT_FORMAT_SHORT>::FormatData(data);
}

template <> template <>
TString TLogFormatter<NKikimrCms::TEXT_FORMAT_SHORT>::FormatData(const NKikimrCms::TLogRecordData::TPDiskMonitorAction &data) {
    TStringStream ss;
    ss << "Send command to BS_CONTROLLER to change status of PDisk "
       << data.GetPDiskId().GetNodeId() << ":" << data.GetPDiskId().GetDiskId()
       << " at " << (data.HasHost() ? data.GetHost() : "<undefined>")
       << " from " << data.GetCurrentStatus()
       << " to " << data.GetRequiredStatus();
    if (data.HasReason()) {
        ss << " (" << data.GetReason() << ")";
    }
    return ss.Str();
}

template <> template <>
TString TLogFormatter<NKikimrCms::TEXT_FORMAT_DETAILED>::FormatData(const NKikimrCms::TLogRecordData::TPDiskMonitorAction &data) {
    TStringStream ss;
    ss << TLogFormatter<NKikimrCms::TEXT_FORMAT_SHORT>::FormatData(data)
       << " disk markers:";
    for (auto marker: data.GetPDiskMarkers())
        ss << " " << marker;
    ss << " node markers:";
    for (auto marker: data.GetNodeMarkers())
        ss << " " << marker;
    OutputDowntimes(ss, " disk downtimes:", data.GetDiskAvailabilityStats());
    OutputDowntimes(ss, " node downtimes:", data.GetNodeAvailabilityStats());
    return ss.Str();
}

template <> template <>
TString TLogFormatter<NKikimrCms::TEXT_FORMAT_SHORT>::FormatData(const NKikimrCms::TLogRecordData::TMarkersModification &data) {
    TStringStream ss;
    ss << "Changed markers for ";
    OutputItem(ss, data);
    ss << " to";
    OutputMarkers(ss, data.GetNewMarkers());
    return ss.Str();
}

template <> template <>
TString TLogFormatter<NKikimrCms::TEXT_FORMAT_DETAILED>::FormatData(const NKikimrCms::TLogRecordData::TMarkersModification &data) {
    TStringStream ss;
    ss << "Changed markers for ";
    OutputItem(ss, data);
    ss << " from";
    OutputMarkers(ss, data.GetOldMarkers());
    ss << " to";
    OutputMarkers(ss, data.GetNewMarkers());
    if (data.GetUserToken()) {
        NACLib::TUserToken userToken(data.GetUserToken());
        ss << " by " << userToken.ShortDebugString();
    }
    return ss.Str();
}

} // namespace NKikimr::NCms
