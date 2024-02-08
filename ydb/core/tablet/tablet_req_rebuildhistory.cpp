#include "tablet_impl.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/tablet/tablet_metrics.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/string/builder.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <google/protobuf/text_format.h>

#include "tablet_tracing_signals.h"

#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR
#error log macro definition clash
#endif

#define BLOG_D(stream, marker) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_MAIN, "TabletId# " << Info->TabletID << (FollowerCookie ? "f " : " ") << stream << " Marker# " << marker)
#define BLOG_W(stream, marker) LOG_WARN_S(*TlsActivationContext, NKikimrServices::TABLET_MAIN, "TabletId# " << Info->TabletID << (FollowerCookie ? "f " : " ") << stream << " Marker# " << marker)
#define BLOG_ERROR(stream, marker) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TABLET_MAIN, "TabletId# " << Info->TabletID << (FollowerCookie ? "f " : " ") << stream << " Marker# " << marker)
#define BLOG_TRACE(stream, marker) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TABLET_MAIN, "TabletId# " << Info->TabletID << (FollowerCookie ? "f " : " ") << stream << " Marker# " << marker)
#define BLOG_CRIT(stream, marker) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TABLET_MAIN, "TabletId# " << Info->TabletID << (FollowerCookie ? "f " : " ") << stream << " Marker# " << marker)

namespace NKikimr {

    // TODO: handle input error condition with Panic notification

class TTabletReqRebuildHistoryGraph : public TActorBootstrapped<TTabletReqRebuildHistoryGraph> {
    struct TLogEntry {
        enum EStatus {
            StatusUnknown,
            StatusOk,
            StatusBody,

            // set by zero-entry tail definition
            StatusMustBePresent,
            StatusMustBeIgnored,
            StatusMustBeIgnoredBody,
        } Status;

        TVector<TLogoBlobID> References;
        TVector<ui32> DependsOn;
        bool IsSnapshot;
        bool IsTotalSnapshot;
        TString EmbeddedLogBody;
        TVector<TEvTablet::TCommitMetadata> EmbeddedMetadata;

        TVector<TLogoBlobID> GcDiscovered;
        TVector<TLogoBlobID> GcLeft;

        void BecomeConfirmed() {
            switch (Status) {
            case StatusUnknown:
                Status = StatusMustBePresent;
                break;
            case StatusOk:
                break;
            case StatusBody:
                Status = StatusOk;
                break;
            case StatusMustBePresent:
                break;
            case StatusMustBeIgnored:
                Status = StatusMustBePresent;
                break;
            case StatusMustBeIgnoredBody:
                Status = StatusOk;
                break;
            default:
                Y_ABORT();
            }
        }

        void BecomeDeclined() {
            switch (Status) {
            case StatusUnknown:
                Status = StatusMustBeIgnored;
                break;
            case StatusOk:
            case StatusBody:
                Status = StatusMustBeIgnoredBody;
                break;
            case StatusMustBePresent:
                Status = StatusMustBeIgnored;
                break;
            case StatusMustBeIgnored:
            case StatusMustBeIgnoredBody:
                break;
            default:
                Y_ABORT();
            }
        }

        void UpdateReferences(const NKikimrTabletBase::TTabletLogEntry &x) {
            if (const ui32 referencesSz = (ui32)x.ReferencesSize()) {
                References.resize(referencesSz);
                for (ui32 i = 0; i != referencesSz; ++i)
                    References[i] = LogoBlobIDFromLogoBlobID(x.GetReferences(i));
            }

            if (x.DependsOnSize())
                DependsOn.insert(DependsOn.begin(), x.GetDependsOn().begin(), x.GetDependsOn().end());

            if (x.HasIsTotalSnapshot())
                IsTotalSnapshot = x.GetIsTotalSnapshot();

            if (x.HasIsSnapshot())
                IsSnapshot = x.GetIsSnapshot();

            if (x.HasEmbeddedLogBody()) {
                Y_ABORT_UNLESS(References.empty(), "must not mix embedded and referenced log bodies");
                Y_ABORT_UNLESS(IsSnapshot == false, "log snapshot could not be embedded");
                EmbeddedLogBody = x.GetEmbeddedLogBody();
            }

            if (const ui32 gcDiscoveredSize = x.GcDiscoveredSize()) {
                GcDiscovered.resize(gcDiscoveredSize);
                for (ui32 i= 0; i != gcDiscoveredSize; ++i)
                    GcDiscovered[i] = LogoBlobIDFromLogoBlobID(x.GetGcDiscovered(i));
            }

            if (const ui32 gcLeftSize = x.GcLeftSize()) {
                GcLeft.resize(gcLeftSize);
                for (ui32 i= 0; i != gcLeftSize; ++i)
                    GcLeft[i] = LogoBlobIDFromLogoBlobID(x.GetGcLeft(i));
            }

            if (const size_t metaSize = x.EmbeddedMetadataSize()) {
                EmbeddedMetadata.reserve(metaSize);
                for (size_t i = 0; i < metaSize; ++i) {
                    const auto& meta = x.GetEmbeddedMetadata(i);
                    EmbeddedMetadata.emplace_back(meta.GetKey(), meta.GetData());
                }
            }

            switch (Status) {
            case StatusUnknown:
                Status = StatusBody;
                break;
            case StatusOk:
                Y_DEBUG_ABORT_UNLESS(false);
                break;
            case StatusBody:
                break;
            case StatusMustBePresent:
                Status = StatusOk;
                break;
            case StatusMustBeIgnored:
                Status = StatusMustBeIgnoredBody;
                break;
            case StatusMustBeIgnoredBody:
                break;
            default:
                Y_ABORT();
            }
        }

        TLogEntry()
            : Status(StatusUnknown)
            , IsSnapshot(false)
            , IsTotalSnapshot(false)
        {}
    };

    struct TGenerationEntry {
        TVector<TLogEntry> Body;
        std::pair<ui32, ui32> PrevGeneration; // gen : confirmed-state
        ui32 NextGeneration;
        ui32 Base;
        ui32 Cutoff;
        bool HasZeroEntry;
        NKikimrTabletBase::TTabletLogEntry ZeroEntryContent;

        TGenerationEntry()
            : NextGeneration(0)
            , Base(1)
            , Cutoff(Max<ui32>())
            , HasZeroEntry(false)
        {}

        TLogEntry& Entry(ui32 step) {
            Y_ABORT_UNLESS(step >= Base);
            const ui32 idx = step - Base;
            Y_ABORT_UNLESS(idx < Body.size());
            return Body[idx];
        }

        void Ensure(ui32 step) {
            Y_ABORT_UNLESS(step >= Base);
            const ui32 idx = step - Base;
            if (idx >= Body.size())
                Body.resize(idx + 1);
        }
    };

    const TActorId Owner;
    TIntrusivePtr<TTabletStorageInfo> Info;
    const ui32 BlockedGen;

    std::pair<ui32, ui32> LatestKnownStep;
    std::pair<ui32, ui32> Snapshot;
    std::pair<ui32, ui32> Confirmed;

    TMap<ui32, TGenerationEntry> LogInfo;
    TSet<TLogoBlobID> RefsToCheck;
    TMap<ui32, TVector<TLogoBlobID>> RefsToCheckByGroup;
    TSet<TLogoBlobID> RangesToDiscover;

    ui32 RequestsLeft;
    NMetrics::TTabletThroughputRawValue GroupReadBytes;
    NMetrics::TTabletIopsRawValue GroupReadOps;

    THolder<NTracing::ITrace> IntrospectionTrace;
    const ui64 FollowerCookie;

    TGenerationEntry& GenerationInfo(ui32 gen) {
        TGenerationEntry& x = LogInfo[gen];
        if (gen == Snapshot.first)
            x.Base = Snapshot.second;
        return x;
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status, const TString &reason) {
        Send(Owner, new TEvTabletBase::TEvRebuildGraphResult(status, IntrospectionTrace.Release(), reason), 0, FollowerCookie);
        PassAway();
    }

    void ProcessZeroEntry(ui32 gen, NKikimrTabletBase::TTabletLogEntry &logEntry) {

        BLOG_D("TTabletReqRebuildHistoryGraph::ProcessZeroEntry - generation " << gen, "TRRH01");
        if (IntrospectionTrace) {
            IntrospectionTrace->Attach(MakeHolder<NTracing::TOnProcessZeroEntry>(gen, Snapshot, Confirmed));
        }

        Y_ABORT_UNLESS(logEntry.HasZeroConfirmed() && logEntry.HasZeroTailSz());

        TGenerationEntry &current = GenerationInfo(gen);

        current.HasZeroEntry = true;
        current.ZeroEntryContent.CopyFrom(logEntry);
    }

    bool RebuildGenSequenceByZeroEntries() {
        if (LogInfo.empty())
            return true;

        // not reverse iterators cuz of simpler erase logic
        auto it = LogInfo.end();
        --it;

        for (;;) {
            const ui32 gen = it->first;

            TGenerationEntry &current = it->second;
            if (!current.HasZeroEntry || gen == Snapshot.first) {
                break;
            }

            NKikimrTabletBase::TTabletLogEntry &zeroLogEntry = current.ZeroEntryContent;
            std::pair<ui32, ui32> confirmed = ExpandGenStepPair(zeroLogEntry.GetZeroConfirmed());
            const ui32 prevGeneration = confirmed.first;

            if (prevGeneration < Snapshot.first) {
                BLOG_CRIT("snapshot overrun in gen " << gen << " zero entry, declared prev gen " << prevGeneration << " while known snapshot is " << Snapshot.first << ":" << Snapshot.second, "TRRH02");

                if (IntrospectionTrace)
                    IntrospectionTrace->Attach(MakeHolder<NTracing::TErrorRebuildGraph>(gen, 0));

                return false;
            }

            current.PrevGeneration = confirmed;

            TGenerationEntry &prev = GenerationInfo(prevGeneration);
            prev.NextGeneration = gen;

            if (confirmed.first > 0)
                FillGenerationEntries(confirmed, prev, zeroLogEntry);

            // here we could erase intermediate entries but who cares as they would be skipped?

            it = LogInfo.find(prevGeneration);

            // Note: prevGeneration is guaranteed to exist because of GenerationInfo call above
            Y_ABORT_UNLESS(it != LogInfo.end());
        }

        // cleanup front entries
        LogInfo.erase(LogInfo.begin(), it);
        return true;
    }

    void FillGenerationEntries(std::pair<ui32, ui32> &confirmed, TGenerationEntry &prev, NKikimrTabletBase::TTabletLogEntry &logEntry) {
        if (confirmed.first == Snapshot.first)
            prev.Base = Snapshot.second;

        const ui32 tailsz = logEntry.GetZeroTailSz();
        Y_ABORT_UNLESS(logEntry.ZeroTailBitmaskSize() == ((tailsz + 63) / 64));

        const ui32 gensz = confirmed.second + tailsz;
        prev.Ensure(gensz);
        prev.Cutoff = gensz; // last entry we interested in, later entries has no interest for us

        { // static part, mark as confirmed
            ui32 step = prev.Base;
            for (ui32 end = confirmed.second; step <= end; ++step) {
                TLogEntry &x = prev.Entry(step);
                x.BecomeConfirmed();
            }
        }

        { // tail part, mark accordingly to flags
            ui64 mask = 0;
            ui64 val = 0;
            ui32 step = confirmed.second + 1;
            for (ui32 i = 0; i < tailsz; ++i, mask <<= 1, ++step) {
                if (mask == 0) {
                    mask = 1;
                    val = logEntry.GetZeroTailBitmask(i / 64);
                }
                const bool ok = val & mask;

                if (step >= prev.Base) {
                    TLogEntry &x = prev.Entry(step);
                    if (ok)
                        x.BecomeConfirmed();
                    else
                        x.BecomeDeclined();
                }
            }

            for (ui32 end = (ui32)(prev.Body.size() + prev.Base); step < end; ++step) {
                TLogEntry &x = prev.Entry(step);
                x.BecomeDeclined();
            }
        }
    }

    void ProcessLogEntry(const TLogoBlobID &id, NKikimrTabletBase::TTabletLogEntry &logEntry) {
        if (IntrospectionTrace) {
            IntrospectionTrace->Attach(MakeHolder<NTracing::TOnProcessLogEntry>(id, Snapshot, Confirmed, logEntry));
        }
        Y_ABORT_UNLESS(logEntry.HasSnapshot() && logEntry.HasConfirmed());

        LOG_DEBUG(*TlsActivationContext, NKikimrServices::TABLET_MAIN, [&](){
            TStringBuilder sb;
            sb << "TTabletReqRebuildHistoryGraph::ProcessLogEntry - TabletID: " << id.TabletID() << ", id " << id
                << ", refs: [";

            for (auto&& t : logEntry.GetReferences())
                sb << LogoBlobIDFromLogoBlobID(t).ToString() << ",";

            sb << "] for " << Info->TabletID;
            return (TString)sb;
        }());

        const ui32 step = id.Step();
        TGenerationEntry &gx = GenerationInfo(id.Generation());

        gx.Ensure(step);

        // ignore synth log entries, they are for follower sync only
        if (id.Cookie() == 0) {
            gx.Entry(step).UpdateReferences(logEntry);
        }
    }

    void DiscoverRange(std::pair<ui32, ui32> from, std::pair<ui32, ui32> to, bool mustRestoreFirst) {
        const TTabletChannelInfo * const channel = Info->ChannelInfo(0);
        const ui64 tabletId = Info->TabletID;

        auto it = channel->History.begin();
        auto endIt = channel->History.end();
        do {
            const ui32 fromGen = it->FromGeneration;
            const ui32 group = it->GroupID;

            const bool last = (++it == endIt);
            const ui32 toGen = last ? Max<ui32>() : it->FromGeneration;

            if (toGen <= from.first)
                continue;

            if (fromGen > to.first)
                return;

            const bool lastGen = (to.first < toGen);

            const TLogoBlobID fromId(tabletId, from.first, from.second, 0, 0, 0);
            const TLogoBlobID toId(tabletId, lastGen ? to.first : toGen - 1, lastGen ? to.second : Max<ui32>(), 0, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie);

            SendToBSProxy(SelfId(), group, new TEvBlobStorage::TEvRange(tabletId, fromId, toId, mustRestoreFirst, TInstant::Max(), false, BlockedGen));
            RangesToDiscover.insert(toId);
            if (IntrospectionTrace) {
                IntrospectionTrace->Attach(MakeHolder<NTracing::TOnDiscoverRangeRequest>(group, fromId, toId));
            }

            if (lastGen)
                return;

            from = std::make_pair(toGen, 0);
        } while (it != endIt);
    }

    void ProcessKeyEntry(const TLogoBlobID &id, const TString &logBody) {
        NKikimrTabletBase::TTabletLogEntry logEntry;
        if (!logEntry.ParseFromString(logBody)) {
            BLOG_ERROR("TTabletReqRebuildHistoryGraph::ProcessKeyEntry logBody ParseFromString error, id# " << id, "TRRH03");
            if (IntrospectionTrace) {
                IntrospectionTrace->Attach(MakeHolder<NTracing::TErrorParsingFromString>(id));
            }
            return ReplyAndDie(NKikimrProto::ERROR, "Log entry parse failed");
        }

        LatestKnownStep = std::pair<ui32, ui32>(id.Generation(), id.Step());
        Snapshot = ExpandGenStepPair(logEntry.GetSnapshot());

        BLOG_D("TTabletReqRebuildHistoryGraph::ProcessKeyEntry, LastBlobID: " << id.ToString()
            << " Snap: " << Snapshot.first << ":" << Snapshot.second
            << " for " << Info->TabletID, "TRRH04");

        const bool isZeroStep = id.Step() == 0;
        const bool isSynthEntry = id.Cookie() != 0;

        ui32 lastGen = 0;
        ui32 lastStep = 0;

        if (isZeroStep) {
            Confirmed = std::pair<ui32, ui32>(LatestKnownStep.first, 0);

            ProcessZeroEntry(id.Generation(), logEntry);
            lastGen = LatestKnownStep.first;
            lastStep = 0;
        } else {
            Confirmed = std::pair<ui32, ui32>(LatestKnownStep.first, logEntry.GetConfirmed());

            ProcessLogEntry(id, logEntry);
            lastGen = LatestKnownStep.first;
            lastStep = isSynthEntry ? (LatestKnownStep.second) : (LatestKnownStep.second - 1);

            TGenerationEntry &gx = GenerationInfo(lastGen);

            for (ui32 i = gx.Base, e = Confirmed.second; i <= e; ++i)
                gx.Entry(i).BecomeConfirmed();
        }

        // request snapshot to confirmed range
        if (Confirmed.first)
            DiscoverRange(Snapshot, Confirmed, false);

        if (FollowerCookie == 0) {
            if (lastGen != Confirmed.first || lastStep != Confirmed.second)
                DiscoverRange({ Confirmed.first, Confirmed.second + 1 }, { lastGen, lastStep }, true);
        }

        Become(&TThis::StateReadLog);
    }

    void ApplyDiscoveryRange(TEvBlobStorage::TEvRangeResult *msg) {
        if (IntrospectionTrace) {
            IntrospectionTrace->Attach(MakeHolder<NTracing::TOnApplyDiscoveryRange>(msg->GroupId, msg->From, msg->To));
        }
        Y_ABORT_UNLESS(RangesToDiscover.erase(msg->To));
        for (TVector<TEvBlobStorage::TEvRangeResult::TResponse>::iterator it = msg->Responses.begin(), end = msg->Responses.end(); it != end; ++it) {
            const TLogoBlobID &id = it->Id;

            GroupReadBytes[std::make_pair(id.Channel(), msg->GroupId)] += it->Buffer.size();
            GroupReadOps[std::make_pair(id.Channel(), msg->GroupId)] += 1;

            NKikimrTabletBase::TTabletLogEntry logEntry;
            if (!logEntry.ParseFromString(it->Buffer)) {
                BLOG_ERROR("TTabletReqRebuildHistoryGraph::ApplyDiscoveryRange it->Buffer ParseFromString error, id# " << id, "TRRH05");
                return ReplyAndDie(NKikimrProto::ERROR, "Log entry parse failed");
            }

            const bool isZeroStep = (id.Step() == 0);
            if (isZeroStep) {
                ProcessZeroEntry(id.Generation(), logEntry);
            } else {
                ProcessLogEntry(id, logEntry);
            }
        }
    }

    void MakeHistory() {
        if (!RebuildGenSequenceByZeroEntries()) {
            return ReplyAndDie(NKikimrProto::ERROR, "RebuildGenSequenceByZeroEntries failed");
        }

        ScanRefsToCheck();
        if (IntrospectionTrace) {
            IntrospectionTrace->Attach(MakeHolder<NTracing::TOnMakeHistory>(RefsToCheck));
        }
        if (RefsToCheckByGroup.empty())
            return BuildHistory();

        for (auto &xpair : RefsToCheckByGroup) {
            if (!SendRefsCheck(xpair.second, xpair.first)) {
                BLOG_ERROR("TTabletReqRebuildHistoryGraph::MakeHistory SendRefsCheck A error", "TRRH06");
                if (IntrospectionTrace) {
                    IntrospectionTrace->Attach(MakeHolder<NTracing::TErrorSendRefsCheck>());
                }
                return ReplyAndDie(NKikimrProto::ERROR, "SendRefsCheck failed");
            }
        }

        RefsToCheckByGroup.clear();
        Become(&TThis::StateCheckReferences);
    }

    bool SendRefsCheck(const TVector<TLogoBlobID> &refs, ui32 group) {
        if (refs.empty())
            return true;

        ui64 endIdx = refs.size();
        ui64 firstRequestIdx = 0;
        while(firstRequestIdx < endIdx) {
            ui64 endRequestIdx = endIdx;
            ui64 totalSize = 0;
            for (ui64 i = firstRequestIdx; i != endIdx; ++i) {
                ui64 size = refs[i].BlobSize();
                Y_ABORT_UNLESS(size != 0);

                const ui64 replyDataSize = totalSize + size + NKikimr::BlobProtobufHeaderMaxSize;
                if (replyDataSize <= NKikimr::MaxProtobufSize) {
                    totalSize += size + NKikimr::BlobProtobufHeaderMaxSize;
                } else {
                    endRequestIdx = i;
                    break;
                }
            }

            ui64 count = endRequestIdx - firstRequestIdx;
            Y_ABORT_UNLESS(count > 0);

            TArrayHolder<TEvBlobStorage::TEvGet::TQuery> q(new TEvBlobStorage::TEvGet::TQuery[count]);
            for (ui64 i = 0; i < count; ++i) {
                q[i].Set(refs[i + firstRequestIdx] /*must be index read*/);
            }
            SendToBSProxy(SelfId(), group, new TEvBlobStorage::TEvGet(q, (ui32)count, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead, true, true, TEvBlobStorage::TEvGet::TForceBlockTabletData(Info->TabletID, BlockedGen)));
            ++RequestsLeft;

            firstRequestIdx = endRequestIdx;
        }

        return true;
    }

    void ScanRefsToCheck() {
        if (LatestKnownStep.first != Confirmed.first)
            return;

        const ui32 tailGeneration = LatestKnownStep.first;
        TGenerationEntry *gx = LogInfo.FindPtr(tailGeneration);
        if (!gx)
            return;

        const ui64 tabletId = Info->TabletID;

        for (i64 pi = gx->Body.size() - 1; pi >= 0; --pi) {
            const ui32 step = gx->Base + (ui32)pi;
            if (step <= Confirmed.second)
                break;
            TLogEntry &entry = gx->Entry(step);

            if (entry.Status == TLogEntry::StatusBody) {
                for (TLogoBlobID &ref : entry.References) {
                    if (ref.TabletID() != tabletId)
                        continue;

                    const ui32 group = Info->GroupFor(ref.Channel(), ref.Generation());
                    RefsToCheckByGroup[group].push_back(ref);
                    RefsToCheck.insert(ref);
                }
            }
        }
    }

    void CheckReferences(TEvBlobStorage::TEvGetResult *msg) {
        if (IntrospectionTrace) {
            IntrospectionTrace->Attach(MakeHolder<NTracing::TOnCheckRefsGetResult>(msg->ResponseSz));
        }
        Y_DEBUG_ABORT_UNLESS(msg->Status == NKikimrProto::OK);

        for (ui32 i = 0, e = msg->ResponseSz; i != e; ++i) {
            const TEvBlobStorage::TEvGetResult::TResponse &response = msg->Responses[i];
            switch (response.Status) {
            case NKikimrProto::OK:
                Y_ABORT_UNLESS(1 == RefsToCheck.erase(response.Id));
                GroupReadBytes[std::make_pair(response.Id.Channel(), msg->GroupId)] += response.Buffer.size();
                GroupReadOps[std::make_pair(response.Id.Channel(), msg->GroupId)] += 1;
                break;
            case NKikimrProto::NODATA:
                BLOG_W("TTabletReqRebuildHistoryGraph::CheckReferences - NODATA for blob " << response.Id, "TRRH07");
                break; // must left as unchecked
            default:
                BLOG_ERROR("TTabletReqRebuildHistoryGraph::CheckReferences - blob " << response.Id
                            << " Status# " << NKikimrProto::EReplyStatus_Name(response.Status), "TRRH08");
                if (IntrospectionTrace) {
                    IntrospectionTrace->Attach(MakeHolder<NTracing::TErrorUnknownStatus>(response.Status, msg->ErrorReason));
                }
                return ReplyAndDie(NKikimrProto::ERROR, msg->ErrorReason);
            }
        }

        --RequestsLeft;
        if (RequestsLeft == 0)
            return BuildHistory();
    }

    void BuildHistory() {
        TAutoPtr<TEvTablet::TDependencyGraph> graph(new TEvTablet::TDependencyGraph(Snapshot));

        std::pair<ui32, ui32> invalidLogEntry = std::make_pair(Max<ui32>(), Max<ui32>());
        ui32 lastUnbrokenTailEntry = Confirmed.second;
        for (TMap<ui32, TGenerationEntry>::iterator gen = LogInfo.begin(), egen = LogInfo.end();;) {
            const ui32 generation = gen->first;
            TGenerationEntry &gx = gen->second;
            const bool isTailGeneration = LatestKnownStep.first == generation && Confirmed.first == generation;
            bool hasSnapshotInGeneration = (generation == 0);

            BLOG_D("TTabletReqRebuildHistoryGraph::BuildHistory - Process generation " << generation
                << " from " << (ui32)gx.Base << " with " << gx.Body.size() << " steps", "TRRH09");

            for (ui32 i = 0, e = (ui32)gx.Body.size(); i != e; ++i) {
                const ui32 step = gx.Base + i;
                const bool isTail = isTailGeneration && step > Confirmed.second;
                ui32 generationSnapshotStep = 0;

                TLogEntry &entry = gx.Entry(step);
                std::pair<ui32, ui32> id(generation, step);

                if (isTail) {
                    // Ignore unconfirmed commits on followers
                    if (FollowerCookie != 0) {
                        break;
                    }

                    switch (entry.Status) {
                    case TLogEntry::StatusUnknown:
                        break;
                    case TLogEntry::StatusOk:
                        Y_ABORT();
                    case TLogEntry::StatusBody:
                        {
                            bool dependsOk = true;
                            for (TVector<ui32>::const_iterator it = entry.DependsOn.begin(), end = entry.DependsOn.end(); dependsOk && it != end; ++it) {
                                const ui32 x = *it;
                                Y_ABORT_UNLESS(x < step, "depends on future step %" PRIu32 " from %" PRIu32 ":%" PRIu32, x, generation, step);
                                dependsOk = x < gx.Base || x <= Confirmed.second || gx.Entry(x).Status == TLogEntry::StatusOk || x <= generationSnapshotStep;
                            }

                            bool refsOk = true;
                            for (TVector<TLogoBlobID>::const_iterator it = entry.References.begin(), end = entry.References.end(); refsOk && it != end; ++it) {
                                const TLogoBlobID &x = *it;
                                refsOk = !RefsToCheck.contains(x);
                            }

                            const bool snapOk = entry.IsTotalSnapshot || (entry.IsSnapshot && (id.second == lastUnbrokenTailEntry + 1));
                            const bool satisfied = refsOk && (snapOk || dependsOk);

                            if (satisfied) {
                                entry.Status = TLogEntry::StatusOk;

                                LOG_DEBUG(*TlsActivationContext, NKikimrServices::TABLET_MAIN, [&](){
                                    TStringBuilder sb;
                                    sb << "TTabletReqRebuildHistoryGraph::BuildHistory - THE TAIL - ";

                                    sb << "References: [";
                                    for (auto&& t : entry.References)
                                        sb << t.ToString() << ",";
                                    sb << "] for " << Info->TabletID;

                                    if (entry.GcDiscovered) {
                                        sb << ", Gc+: [";
                                        for (auto&& t : entry.GcDiscovered)
                                            sb << t.ToString() << ",";
                                        sb << "]";
                                    }

                                    if (entry.GcLeft) {
                                        sb << ", Gc-: [";
                                        for (auto&& t : entry.GcLeft)
                                            sb << t.ToString() << ",";
                                        sb << "]";
                                    }

                                    return (TString) sb;
                                }());

                                if (entry.EmbeddedLogBody)
                                    graph->AddEntry(id, std::move(entry.EmbeddedLogBody), std::move(entry.GcDiscovered), std::move(entry.GcLeft), std::move(entry.EmbeddedMetadata));
                                else
                                    graph->AddEntry(id, std::move(entry.References), entry.IsSnapshot, std::move(entry.GcDiscovered), std::move(entry.GcLeft), std::move(entry.EmbeddedMetadata));

                                if (lastUnbrokenTailEntry + 1 == id.second)
                                    lastUnbrokenTailEntry = id.second;

                                if (entry.IsSnapshot) {
                                    generationSnapshotStep = step;
                                    hasSnapshotInGeneration = true;
                                }
                            } else {
                                BLOG_D("TTabletReqRebuildHistoryGraph::BuildHistory - THE TAIL - miss " << id.first << ":" << id.second, "TRRH10");
                            }
                        }
                        break;
                    default:
                        Y_ABORT();
                    }
                } else {
                    if (step <= gx.Cutoff) {
                        switch (entry.Status) {
                        case TLogEntry::StatusOk:

                            LOG_DEBUG(*TlsActivationContext, NKikimrServices::TABLET_MAIN, [&](){
                                TStringBuilder sb;
                                sb << "TTabletReqRebuildHistoryGraph::BuildHistory - NOT A TAIL - ";
                                sb << "References: [";
                                for (auto&& t : entry.References) {
                                    sb << t.ToString();
                                    sb << ",";
                                }
                                sb << "] for " << Info->TabletID;

                                return (TString) sb;
                            }());

                            if (entry.EmbeddedLogBody)
                                graph->AddEntry(id, std::move(entry.EmbeddedLogBody), std::move(entry.GcDiscovered), std::move(entry.GcLeft), std::move(entry.EmbeddedMetadata));
                            else
                                graph->AddEntry(id, std::move(entry.References), entry.IsSnapshot, std::move(entry.GcDiscovered), std::move(entry.GcLeft), std::move(entry.EmbeddedMetadata));

                            hasSnapshotInGeneration |= entry.IsSnapshot;
                            break;
                        case TLogEntry::StatusMustBeIgnored:
                        case TLogEntry::StatusMustBeIgnoredBody:
                            break;
                        default:
                            graph->Invalidate();
                            invalidLogEntry = id;
                            break;
                        }
                    }
                }
            }

            if (!hasSnapshotInGeneration && !gx.HasZeroEntry) {
                graph->Invalidate();
                if (invalidLogEntry.first < generation)
                    invalidLogEntry =  std::make_pair(generation, 0);
            }

            if (gx.NextGeneration == 0) {
                ++gen;
                if (gen == egen)
                    break;

                graph->Invalidate();
                invalidLogEntry = std::make_pair(generation, Max<ui32>());
            } else {
                gen = LogInfo.find(gx.NextGeneration);
                Y_ABORT_UNLESS(gen != egen);
            }
        }

        if (!graph->IsValid()) {
            LOG_ALERT(*TlsActivationContext, NKikimrServices::TABLET_MAIN, [&]() {
                TStringBuilder sb;
                sb << "TTabletReqRebuildHistoryGraph::BuildHistory - Graph rebuild error - no Log entry for ";
                sb << Info->TabletID << ":" << invalidLogEntry.first << ":" << invalidLogEntry.second;
                return (TString)sb;
            }());
            if (IntrospectionTrace) {
                IntrospectionTrace->Attach(MakeHolder<NTracing::TErrorRebuildGraph>(invalidLogEntry.first, invalidLogEntry.second));
            }

            return ReplyAndDie(NKikimrProto::ERROR, "Graph has missing log entries");
        }
        if (IntrospectionTrace) {
            IntrospectionTrace->Attach(MakeHolder<NTracing::TOnBuildHistoryGraph>(graph.Get()));
        }

        Send(Owner, new TEvTabletBase::TEvRebuildGraphResult(
                graph.Release(),
                std::move(GroupReadBytes),
                std::move(GroupReadOps),
                IntrospectionTrace.Release()),
            0, FollowerCookie);

        PassAway();
    }

    void Handle(TEvents::TEvUndelivered::TPtr&) {
        return ReplyAndDie(NKikimrProto::ERROR, "BlobStorage proxy unavailable");
    }

    void Handle(TEvTabletBase::TEvFindLatestLogEntryResult::TPtr &ev) {
        TEvTabletBase::TEvFindLatestLogEntryResult *msg = ev->Get();

        switch (msg->Status) {
        case NKikimrProto::OK:
            if (FollowerCookie == 0 && msg->Latest.Generation() > BlockedGen) {
                BLOG_ERROR("TTabletReqRebuildHistoryGraph - Found entry beyond blocked generation"
                    << " LastBlobID: " << msg->Latest.ToString() << ". Blocked: " << BlockedGen, "TRRH11");
                if (IntrospectionTrace) {
                    IntrospectionTrace->Attach(MakeHolder<NTracing::TErrorEntryBeyondBlocked>(msg->Latest, BlockedGen));
                }
                return ReplyAndDie(NKikimrProto::ERROR, "Found entry beyond blocked generation");
            }

            if (IntrospectionTrace) {
                IntrospectionTrace->Attach(MakeHolder<NTracing::TOnProcessKeyEntry>(msg->Latest));
            }

            return ProcessKeyEntry(msg->Latest, msg->Buffer);
        case NKikimrProto::RACE:
        case NKikimrProto::NODATA:
            return ReplyAndDie(msg->Status, msg->ErrorReason); // valid condition, nothing known in blob-storage
        default:
            BLOG_ERROR("TTabletReqRebuildHistoryGraph::Handle TEvFindLatestLogEntryResult"
                << " Status# " << NKikimrProto::EReplyStatus_Name(msg->Status), "TRRH12");
            if (IntrospectionTrace) {
                IntrospectionTrace->Attach(MakeHolder<NTracing::TErrorUnknownStatus>(msg->Status, msg->ErrorReason));
            }
            return ReplyAndDie(NKikimrProto::ERROR, msg->ErrorReason);
        }
    }

    void Handle(TEvBlobStorage::TEvRangeResult::TPtr &ev) {
        TEvBlobStorage::TEvRangeResult *msg = ev->Get();

        switch (msg->Status) {
        case NKikimrProto::OK:
            ApplyDiscoveryRange(msg);
            break;
        case NKikimrProto::RACE:
            return ReplyAndDie(NKikimrProto::RACE, msg->ErrorReason);
        default:
            BLOG_ERROR("TTabletReqRebuildHistoryGraph::HandleDiscover TEvRangeResult"
                << " Status# " << NKikimrProto::EReplyStatus_Name(msg->Status)
                << " Result# " << msg->Print(false), "TRRH13");
            if (IntrospectionTrace) {
                IntrospectionTrace->Attach(MakeHolder<NTracing::TErrorUnknownStatus>(msg->Status, msg->ErrorReason));
            }
            return ReplyAndDie(NKikimrProto::ERROR, msg->ErrorReason);
        }

        if (!RangesToDiscover.empty())
            return;

        MakeHistory();
    }

    void Handle(TEvBlobStorage::TEvGetResult::TPtr &ev) {
        TEvBlobStorage::TEvGetResult *msg = ev->Get();

        switch (msg->Status) {
        case NKikimrProto::OK:
            return CheckReferences(msg);
        case NKikimrProto::RACE:
            return ReplyAndDie(NKikimrProto::RACE, msg->ErrorReason);
        default:
            BLOG_ERROR("TTabletReqRebuildHistoryGraph::Handle TEvGetResult"
                << " Status# " << NKikimrProto::EReplyStatus_Name(msg->Status)
                << " Result# " << msg->Print(false), "TRRH14");
            if (IntrospectionTrace) {
                IntrospectionTrace->Attach(MakeHolder<NTracing::TErrorUnknownStatus>(msg->Status, msg->ErrorReason));
            }
            return ReplyAndDie(NKikimrProto::ERROR, msg->ErrorReason);
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_REQ_REBUILD_GRAPH;
    }

    TTabletReqRebuildHistoryGraph(const TActorId &owner, TTabletStorageInfo *info, ui32 blockedGen, NTracing::ITrace *trace, ui64 followerCookie)
        : Owner(owner)
        , Info(info)
        , BlockedGen(blockedGen)
        , RequestsLeft(0)
        , IntrospectionTrace(trace)
        , FollowerCookie(followerCookie)
    {}

    void Bootstrap() {
        if (IntrospectionTrace) {
            IntrospectionTrace->Attach(MakeHolder<NTracing::TRebuildGraphBootstrap>(BlockedGen));
        }

        if (FollowerCookie == 0)
            Register(CreateTabletFindLastEntry(SelfId(), true, Info.Get(), BlockedGen, true));
        else
            Register(CreateTabletFindLastEntry(SelfId(), true, Info.Get(), 0, false));

        Become(&TThis::StateWaitLatestEntry);
    }

    STATEFN(StateWaitLatestEntry) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletBase::TEvFindLatestLogEntryResult, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    STATEFN(StateReadLog) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvRangeResult, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    STATEFN(StateCheckReferences) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvGetResult, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

IActor* CreateTabletReqRebuildHistoryGraph(const TActorId &owner, TTabletStorageInfo *info, ui32 blockedGen, NTracing::ITrace *trace, ui64 followerCookie) {
    return new TTabletReqRebuildHistoryGraph(owner, info, blockedGen, trace, followerCookie);
}


}
