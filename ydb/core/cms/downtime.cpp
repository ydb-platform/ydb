#include "downtime.h"
#include "cluster_info.h"
#include "scheme.h"

#include <util/generic/algorithm.h>
#include <util/string/join.h>

namespace NKikimr::NCms {

TDowntime::TDowntime(TDuration ignoredDowntimeGap)
    : IgnoredDowntimeGap(ignoredDowntimeGap)
{
}

void TDowntime::AddDowntime(TInstant start, TInstant end, const TString &reason) {
    AddDowntime({start, end, reason});
}

void TDowntime::AddDowntime(TInstant start, TDuration duration, const TString &reason) {
    AddDowntime({start, duration, reason});
}

void TDowntime::AddDowntime(const TSegment segment) {
    if (!segment.Duration())
        return;

    auto it = DowntimeSegments.insert(segment);

    while (it.first != DowntimeSegments.begin()) {
        it = CollapseWithPrev(it.first);
        if (!it.second)
            break;
    }

    while (++it.first != DowntimeSegments.end()) {
        it = CollapseWithPrev(it.first);
        if (!it.second)
            break;
    }
}

void TDowntime::AddDowntime(const TDowntime &downtime) {
    for (auto &segment : downtime.GetDowntimeSegments())
        AddDowntime(segment);
}

void TDowntime::AddDowntime(const TLockableItem &item, TInstant now) {
    AddDowntime(item.Downtime);

    if (item.State != NKikimrCms::UP)
        AddDowntime(item.Timestamp, now, "known downtime");

    if (item.Lock.Defined()) {
        auto end = Min(now + TDuration::MicroSeconds(item.Lock->Action.GetDuration()),
                       item.Lock->ActionDeadline);
        AddDowntime(now, end, item.Lock->PermissionId);
    }

    for (auto &lock : item.ExternalLocks) {
        auto start = Max(lock.LockStart, now);
        AddDowntime(start, lock.LockDeadline, lock.NotificationId);
    }
}

bool TDowntime::HasUpcomingDowntime(TInstant now, TDuration distance, TDuration duration) const {
    for (auto &segment : DowntimeSegments) {
        if (segment.Start > now + distance)
            break;

        if (segment.End >= now && segment.Duration() >= duration)
            return true;
    }

    return false;
}

TDuration TDowntime::GetIgnoredDowntimeGap() const {
    return IgnoredDowntimeGap;
}

void TDowntime::SetIgnoredDowntimeGap(TDuration gap) {
    bool merge = IgnoredDowntimeGap < gap;
    IgnoredDowntimeGap = gap;

    if (merge && !DowntimeSegments.empty()) {
        for (auto it = ++DowntimeSegments.begin(); it != DowntimeSegments.end(); ++it)
            it = CollapseWithPrev(it).first;
    }
}

void TDowntime::CleanupOldSegments(TInstant now) {
    while (!DowntimeSegments.empty()) {
        auto it = DowntimeSegments.begin();
        if (it->End + IgnoredDowntimeGap < now)
            DowntimeSegments.erase(it);
        else
            break;
    }
}

void TDowntime::Serialize(NKikimrCms::TAvailabilityStats *rec) const {
    for (auto &segment : DowntimeSegments) {
        auto &entry = *rec->AddDowntimes();
        entry.SetStart(segment.Start.GetValue());
        entry.SetEnd(segment.End.GetValue());
        entry.SetExplanation(segment.Reason);
    }
    rec->SetIgnoredDowntimeGap(IgnoredDowntimeGap.GetValue());
}

void TDowntime::Deserialize(const NKikimrCms::TAvailabilityStats &rec) {
    SetIgnoredDowntimeGap(TDuration::FromValue(rec.GetIgnoredDowntimeGap()));

    for (auto &entry : rec.GetDowntimes()) {
        AddDowntime(TInstant::FromValue(entry.GetStart()),
                    TInstant::FromValue(entry.GetEnd()),
                    entry.GetExplanation());
    }
}

std::pair<TDowntime::TSegments::iterator, bool> TDowntime::CollapseWithPrev(TSegments::iterator it) {
    auto prev = it;
    --prev;

    if (it->Start - prev->End <= IgnoredDowntimeGap) {
        TString newReason;
        if (!prev->Reason || prev->Reason == it->Reason)
            newReason = it->Reason;
        else if (!it->Reason)
            newReason = prev->Reason;
        else
            newReason = Join(", ", prev->Reason, it->Reason);

        TSegment collapsed = {prev->Start, Max(prev->End, it->End), newReason};
        DowntimeSegments.erase(prev);
        DowntimeSegments.erase(it);
        return {DowntimeSegments.insert(collapsed).first, true};
    }

    return {it, false};
}

TDowntimes::TDowntimes()
{
}

void TDowntimes::SetIgnoredDowntimeGap(TDuration gap) {
    if (IgnoredDowntimeGap == gap)
        return;

    for (auto &pr : NodeDowntimes)
        pr.second.SetIgnoredDowntimeGap(gap);
    for (auto &pr : PDiskDowntimes)
        pr.second.SetIgnoredDowntimeGap(gap);
}

void TDowntimes::CleanupOld(TInstant now) {
    for (auto &pr : NodeDowntimes)
        pr.second.CleanupOldSegments(now);
    for (auto &pr : PDiskDowntimes)
        pr.second.CleanupOldSegments(now);
}

void TDowntimes::CleanupEmpty() {
    for (auto it = NodeDowntimes.begin(); it != NodeDowntimes.end(); ) {
        auto next = it;
        ++next;
        if (it->second.Empty())
            NodeDowntimes.erase(it);
        it = next;
    }

    for (auto it = PDiskDowntimes.begin(); it != PDiskDowntimes.end(); ) {
        auto next = it;
        ++next;
        if (it->second.Empty())
            PDiskDowntimes.erase(it);
        it = next;
    }
}

bool TDowntimes::DbLoadState(TTransactionContext &txc, const TActorContext &ctx) {
    Y_UNUSED(ctx);

    NIceDb::TNiceDb db(txc.DB);
    auto nodeRowset = db.Table<Schema::NodeDowntimes>().Range().Select<Schema::NodeDowntimes::TColumns>();
    auto pdiskRowset = db.Table<Schema::PDiskDowntimes>().Range().Select<Schema::PDiskDowntimes::TColumns>();

    if (!nodeRowset.IsReady() || !pdiskRowset.IsReady())
        return false;

    NodeDowntimes.clear();
    PDiskDowntimes.clear();

    while (!nodeRowset.EndOfSet()) {
        ui32 nodeId = nodeRowset.GetValue<Schema::NodeDowntimes::NodeId>();
        NKikimrCms::TAvailabilityStats rec = nodeRowset.GetValue<Schema::NodeDowntimes::Downtime>();

        NodeDowntimes[nodeId].Deserialize(rec);

        if (!nodeRowset.Next())
            return false;
    }

    while (!pdiskRowset.EndOfSet()) {
        ui32 nodeId = pdiskRowset.GetValue<Schema::PDiskDowntimes::NodeId>();
        ui32 diskId = pdiskRowset.GetValue<Schema::PDiskDowntimes::DiskId>();
        NKikimrCms::TAvailabilityStats rec = pdiskRowset.GetValue<Schema::PDiskDowntimes::Downtime>();

        PDiskDowntimes[TPDiskID(nodeId, diskId)].Deserialize(rec);

        if (!pdiskRowset.Next())
            return false;
    }

    return true;
}

void TDowntimes::DbStoreState(TTransactionContext &txc, const TActorContext &ctx) {
    NIceDb::TNiceDb db(txc.DB);

    for (auto &pr : NodeDowntimes) {
        if (pr.second.Empty()) {
            db.Table<Schema::NodeDowntimes>().Key(pr.first).Delete();

            LOG_TRACE_S(ctx, NKikimrServices::CMS,
                        "Removed downtime for node " << pr.first << " from local DB");
        } else {
            NKikimrCms::TAvailabilityStats rec;
            pr.second.Serialize(&rec);
            db.Table<Schema::NodeDowntimes>().Key(pr.first)
                .Update<Schema::NodeDowntimes::Downtime>(rec);

            LOG_TRACE_S(ctx, NKikimrServices::CMS,
                        "Updated downtime for node " << pr.first
                        << " in local DB downtime=" << pr.second);
        }
    }

    for (auto &pr : PDiskDowntimes) {
        if (pr.second.Empty()) {
            db.Table<Schema::PDiskDowntimes>().Key(pr.first.NodeId, pr.first.DiskId)
                .Delete();

            LOG_TRACE_S(ctx, NKikimrServices::CMS,                        "Removed downtime for pdisk " << pr.first.ToString()
                        << " from local DB");
        } else {
            NKikimrCms::TAvailabilityStats rec;
            pr.second.Serialize(&rec);
            db.Table<Schema::PDiskDowntimes>().Key(pr.first.NodeId, pr.first.DiskId)
                .Update<Schema::PDiskDowntimes::Downtime>(rec);

            LOG_TRACE_S(ctx, NKikimrServices::CMS,
                        "Updated downtime for pdisk " << pr.first.ToString()
                        << " in local DB downtime=" << pr.second);
        }
    }
}

} // namespace NKikimr::NCms

Y_DECLARE_OUT_SPEC(, NKikimr::NCms::TDowntime::TSegment, stream, value) {
    stream << "[" << value.Start.ToStringLocalUpToSeconds() << "-"
           << value.End.ToStringLocalUpToSeconds() << "]("
           << value.Reason << ")";
}

Y_DECLARE_OUT_SPEC(, NKikimr::NCms::TDowntime, stream, value) {
    stream << JoinSeq(", ", value.GetDowntimeSegments());
}
