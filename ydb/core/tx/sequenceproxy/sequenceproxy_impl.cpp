#include "sequenceproxy_impl.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <yql/essentials/public/issue/yql_issue_manager.h>

#include <ydb/library/actors/core/log.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NSequenceProxy {

    TSequenceProxyCounters::TSequenceProxyCounters() {
        auto group = GetServiceCounters(AppData()->Counters, "proxy");
        SequenceShardAllocateCount = group->GetHistogram(
            "SequenceProxy/SequenceShard/AllocateCountPerRequest",
            NMonitoring::ExponentialHistogram(20, 2, 1));

        ErrorsCount = group->GetCounter("SequenceProxy/Errors", true);
        RequestCount = group->GetCounter("SequenceProxy/Requests", true);
        ResponseCount = group->GetCounter("SequenceProxy/Responses", true);
        NextValLatency = group->GetHistogram("SequenceProxy/Latency",
            NMonitoring::ExponentialHistogram(20, 2, 1));
    };

    void TSequenceProxy::Bootstrap() {
        Counters.Reset(new TSequenceProxyCounters());
        LogPrefix = TStringBuilder() << "TSequenceProxy [Node " << SelfId().NodeId() << "] ";
        Become(&TThis::StateWork);
    }

    void TSequenceProxy::HandlePoison() {
        PassAway();
    }

    void TSequenceProxy::Handle(TEvSequenceProxy::TEvNextVal::TPtr& ev) {
        auto* msg = ev->Get();
        TNextValRequestInfo request;
        request.Sender = ev->Sender;
        request.Cookie = ev->Cookie;
        request.UserToken = std::move(msg->UserToken);
        request.StartAt = AppData()->MonotonicTimeProvider->Now();
        std::visit(
            [&](const auto& path) {
                DoNextVal(std::move(request), msg->Database, path);
            },
            msg->Path);
    }

    void TSequenceProxy::Handle(TEvSequenceProxy::TEvGetSequence::TPtr& ev) {
        auto* msg = ev->Get();
        TGetSequenceRequestInfo request;
        request.Sender = ev->Sender;
        request.Cookie = ev->Cookie;
        request.UserToken = std::move(msg->UserToken);
        request.StartAt = AppData()->MonotonicTimeProvider->Now();
        DoGetSequence(std::move(request), msg->Database, msg->PathId);
    }

    void TSequenceProxy::Handle(TEvSequenceProxy::TEvAdvanceSequence::TPtr& ev) {
        auto* msg = ev->Get();
        TAdvanceRequestInfo request;
        request.Sender = ev->Sender;
        request.Cookie = ev->Cookie;
        request.ObservedMin = msg->ObservedMin;
        request.ObservedMax = msg->ObservedMax;
        std::visit(
            [&](const auto& path) {
                DoAdvance(std::move(request), msg->Database, path);
            },
            msg->Path);
    }

    void TSequenceProxy::Reply(const TNextValRequestInfo& request, Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) {
        Counters->ResponseCount->Inc();
        auto milliseconds = (AppData()->MonotonicTimeProvider->Now() - request.StartAt).MilliSeconds();
        Counters->NextValLatency->Collect(milliseconds);
        Counters->ErrorsCount->Inc();
        Send(request.Sender, new TEvSequenceProxy::TEvNextValResult(status, issues), 0, request.Cookie);
    }

    void TSequenceProxy::Reply(const TNextValRequestInfo& request, const TPathId& pathId, i64 value) {
        Counters->ResponseCount->Inc();
        auto milliseconds = (AppData()->MonotonicTimeProvider->Now() - request.StartAt).MilliSeconds();
        Counters->NextValLatency->Collect(milliseconds);
        Send(request.Sender, new TEvSequenceProxy::TEvNextValResult(pathId, value), 0, request.Cookie);
    }

    void TSequenceProxy::Reply(const TGetSequenceRequestInfo& request, Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) {
        Counters->ResponseCount->Inc();
        Counters->ErrorsCount->Inc();
        Send(request.Sender, new TEvSequenceProxy::TEvGetSequenceResult(status, issues), 0, request.Cookie);
    }

    void TSequenceProxy::Reply(const TGetSequenceRequestInfo& request, const TEvSequenceProxy::TEvGetSequenceResult& ev) {
        Counters->ResponseCount->Inc();
        Send(request.Sender, new TEvSequenceProxy::TEvGetSequenceResult(ev), 0, request.Cookie);
    }

    void TSequenceProxy::Reply(const TAdvanceRequestInfo& request, Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) {
        Counters->ResponseCount->Inc();
        Counters->ErrorsCount->Inc();
        Send(request.Sender, new TEvSequenceProxy::TEvAdvanceSequenceResult(status, issues), 0, request.Cookie);
    }

    void TSequenceProxy::Reply(const TAdvanceRequestInfo& request, const TPathId& pathId, i64 nextValue, bool nextUsed) {
        Counters->ResponseCount->Inc();
        Send(request.Sender, new TEvSequenceProxy::TEvAdvanceSequenceResult(pathId, nextValue, nextUsed), 0, request.Cookie);
    }

    void TSequenceProxy::MaybeStartResolve(const TString& database, const TString& path, TSequenceByName& info) {
        if (!info.ResolveInProgress && (!info.NewNextValResolve.empty() || !info.NewAdvanceResolve.empty())) {
            info.PendingNextValResolve = std::move(info.NewNextValResolve);
            info.PendingAdvanceResolve = std::move(info.NewAdvanceResolve);
            StartResolve(database, path, !info.PathId);
            info.ResolveInProgress = true;
        }
    }

    void TSequenceProxy::DoNextVal(TNextValRequestInfo&& request, const TString& database, const TString& path) {
        Counters->RequestCount->Inc();
        auto& info = Databases[database].SequenceByName[path];
        info.NewNextValResolve.emplace_back(std::move(request));
        MaybeStartResolve(database, path, info);
    }

    void TSequenceProxy::DoNextVal(TNextValRequestInfo&& request, const TString& database, const TPathId& pathId) {
        Counters->RequestCount->Inc();

        auto& info = Databases[database].SequenceByPathId[pathId];
        if (!info.SequenceInfo) {
            if (!info.ResolveInProgress) {
                StartResolve(database, pathId, true);
                info.ResolveInProgress = true;
            }
            info.PendingNextValResolve.emplace_back(std::move(request));
            return;
        }

        if (DoMaybeReplyUnauthorized(request, pathId, info)) {
            return;
        }

        if (info.TotalCached > 0) {
            DoReplyFromCache(request, pathId, info);
            return;
        }

        info.PendingNextVal.emplace_back(std::move(request));
        ++info.TotalRequested;

        OnChanged(database, pathId, info);
    }

    void TSequenceProxy::DoGetSequence(TGetSequenceRequestInfo&& request, const TString& database, const TPathId& pathId) {
        Counters->RequestCount->Inc();

        auto& info = Databases[database].SequenceByPathId[pathId];
        if (!info.ResolveInProgress) {
            StartResolve(database, pathId, !info.SequenceInfo);
            info.ResolveInProgress = true;
        }
        if (!info.SequenceInfo) {
            info.PendingGetSequenceResolve.emplace_back(std::move(request));
            return;
        }

        if (DoMaybeReplyUnauthorized(request, pathId, info)) {
            return;
        }

        info.PendingGetSequence.emplace_back(std::move(request));

        OnChanged(database, pathId, info);
    }

    void TSequenceProxy::DoAdvance(TAdvanceRequestInfo&& request, const TString& database, const TString& path) {
        Counters->RequestCount->Inc();
        auto& info = Databases[database].SequenceByName[path];
        info.NewAdvanceResolve.emplace_back(std::move(request));
        MaybeStartResolve(database, path, info);
    }

    void TSequenceProxy::DoAdvance(TAdvanceRequestInfo&& request, const TString& database, const TPathId& pathId) {
        Counters->RequestCount->Inc();

        auto& info = Databases[database].SequenceByPathId[pathId];
        if (!info.SequenceInfo) {
            if (!info.ResolveInProgress) {
                StartResolve(database, pathId, true);
                info.ResolveInProgress = true;
            }
            info.PendingAdvanceResolve.emplace_back(std::move(request));
            return;
        }

        StartAdvance(info.LastKnownTabletId, database, pathId, request);
    }

    void TSequenceProxy::OnResolveError(const TString& database, const TString& path, Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) {
        auto& info = Databases[database].SequenceByName[path];
        Y_ABORT_UNLESS(info.ResolveInProgress);
        info.ResolveInProgress = false;

        while (!info.PendingNextValResolve.empty()) {
            Reply(info.PendingNextValResolve.front(), status, issues);
            info.PendingNextValResolve.pop_front();
        }

        while (!info.PendingAdvanceResolve.empty()) {
            Reply(info.PendingAdvanceResolve.front(), status, issues);
            info.PendingAdvanceResolve.pop_front();
        }

        MaybeStartResolve(database, path, info);
    }

    void TSequenceProxy::OnResolveResult(const TString& database, const TString& path, TResolveResult&& result) {
        auto& info = Databases[database].SequenceByName[path];
        Y_ABORT_UNLESS(info.ResolveInProgress);
        info.ResolveInProgress = false;

        auto pathId = result.PathId;
        Y_ABORT_UNLESS(pathId);

        info.PathId = pathId;

        Y_ABORT_UNLESS(result.SequenceInfo);

        auto& infoById = Databases[database].SequenceByPathId[pathId];
        infoById.SequenceInfo = result.SequenceInfo;
        infoById.SecurityObject = result.SecurityObject;

        TList<TGetSequenceRequestInfo> resolvedGetSequence;
        OnResolved(database, pathId, infoById, info.PendingNextValResolve, resolvedGetSequence, info.PendingAdvanceResolve);

        MaybeStartResolve(database, path, info);
    }

    void TSequenceProxy::OnResolveError(const TString& database, const TPathId& pathId, Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) {
        auto& info = Databases[database].SequenceByPathId[pathId];
        Y_ABORT_UNLESS(info.ResolveInProgress);
        info.ResolveInProgress = false;

        while (!info.PendingNextValResolve.empty()) {
            Reply(info.PendingNextValResolve.front(), status, issues);
            info.PendingNextValResolve.pop_front();
        }

        while (!info.PendingGetSequenceResolve.empty()) {
            Reply(info.PendingGetSequenceResolve.front(), status, issues);
            info.PendingGetSequenceResolve.pop_front();
        }

        while (!info.PendingAdvanceResolve.empty()) {
            Reply(info.PendingAdvanceResolve.front(), status, issues);
            info.PendingAdvanceResolve.pop_front();
        }
    }

    void TSequenceProxy::OnResolveResult(const TString& database, const TPathId& pathId, TResolveResult&& result) {
        auto& info = Databases[database].SequenceByPathId[pathId];
        Y_ABORT_UNLESS(info.ResolveInProgress);
        info.ResolveInProgress = false;

        Y_ABORT_UNLESS(result.SequenceInfo);
        info.SequenceInfo = result.SequenceInfo;
        info.SecurityObject = result.SecurityObject;
        OnResolved(database, pathId, info, info.PendingNextValResolve, info.PendingGetSequenceResolve, info.PendingAdvanceResolve);
    }

    void TSequenceProxy::OnResolved(const TString& database, const TPathId& pathId, TSequenceByPathId& info,
            TList<TNextValRequestInfo>& resolvedNextVal, TList<TGetSequenceRequestInfo>& resolvedGetSequence,
            TList<TAdvanceRequestInfo>& resolvedAdvance) {
        info.LastKnownTabletId = info.SequenceInfo->Description.GetSequenceShard();
        info.DefaultCacheSize = Max(info.SequenceInfo->Description.GetCache(), ui64(1));

        while (!resolvedNextVal.empty()) {
            auto& request = resolvedNextVal.front();
            if (!DoMaybeReplyUnauthorized(request, pathId, info)) {
                info.PendingNextVal.emplace_back(std::move(request));
                ++info.TotalRequested;
            }
            resolvedNextVal.pop_front();
        }

        while (!resolvedGetSequence.empty()) {
            auto& request = resolvedGetSequence.front();
            if (!DoMaybeReplyUnauthorized(request, pathId, info)) {
                info.PendingGetSequence.emplace_back(std::move(request));
            }
            resolvedGetSequence.pop_front();
        }

        // Advance is an internal operation of replication, it has no user to check access for.
        while (!resolvedAdvance.empty()) {
            StartAdvance(info.LastKnownTabletId, database, pathId, resolvedAdvance.front());
            resolvedAdvance.pop_front();
        }

        OnChanged(database, pathId, info);
    }

    void TSequenceProxy::Handle(TEvPrivate::TEvAllocateResult::TPtr& ev) {
        auto it = AllocateInFlight.find(ev->Cookie);
        Y_ABORT_UNLESS(it != AllocateInFlight.end());
        auto database = it->second.Database;
        auto pathId = it->second.PathId;
        AllocateInFlight.erase(it);

        auto& info = Databases[database].SequenceByPathId[pathId];
        Y_ABORT_UNLESS(info.AllocateInProgress);
        info.AllocateInProgress = false;
        ui64 cache = std::exchange(info.TotalAllocating, 0);

        auto* msg = ev->Get();

        if (msg->Status == Ydb::StatusIds::SUCCESS) {
            if (msg->Moved) {
                // The sequence shard we had in the resolve cache is not the current one anymore,
                // drop it so that the next request resolves the sequence again.
                info.SequenceInfo = nullptr;
                info.SecurityObject = nullptr;
            }

            auto& allocated = info.CachedAllocations.emplace_back();
            allocated.Start = msg->Start;
            allocated.Increment = msg->Increment;
            allocated.Count = msg->Count;
            info.TotalCached += msg->Count;
        } else {
            // We will answer up to cache requests with this error
            while (cache > 0 && !info.PendingNextVal.empty()) {
                Reply(info.PendingNextVal.front(), msg->Status, msg->Issues);
                info.PendingNextVal.pop_front();
                --info.TotalRequested;
                --cache;
            }
        }

        OnChanged(database, pathId, info);
    }

    void TSequenceProxy::Handle(TEvPrivate::TEvAdvanceResult::TPtr& ev) {
        auto it = AdvanceInFlight.find(ev->Cookie);
        Y_ABORT_UNLESS(it != AdvanceInFlight.end());
        auto database = it->second.Database;
        auto pathId = it->second.PathId;
        auto request = std::move(it->second.Request);
        AdvanceInFlight.erase(it);

        auto* msg = ev->Get();

        if (msg->Status != Ydb::StatusIds::SUCCESS) {
            return Reply(request, msg->Status, msg->Issues);
        }

        auto& info = Databases[database].SequenceByPathId[pathId];

        if (msg->Moved) {
            info.SequenceInfo = nullptr;
            info.SecurityObject = nullptr;
        }

        // Clear cache on advance.
        info.CachedAllocations.clear();
        info.TotalCached = 0;

        OnChanged(database, pathId, info);

        Reply(request, pathId, msg->NextValue, msg->NextUsed);
    }

    void TSequenceProxy::Handle(TEvSequenceProxy::TEvGetSequenceResult::TPtr& ev) {
        auto it = GetSequenceInFlight.find(ev->Cookie);
        Y_ABORT_UNLESS(it != GetSequenceInFlight.end());
        auto database = it->second.Database;
        auto pathId = it->second.PathId;
        GetSequenceInFlight.erase(it);

        auto& info = Databases[database].SequenceByPathId[pathId];
        Y_ABORT_UNLESS(info.GetSequenceInProgress);
        info.GetSequenceInProgress = false;

        auto* msg = ev->Get();

        if (msg->Status == Ydb::StatusIds::SUCCESS) {
            while (!info.PendingGetSequence.empty()) {
                Reply(info.PendingGetSequence.front(), *msg);
                info.PendingGetSequence.pop_front();
            }
        } else {
            while (!info.PendingGetSequence.empty()) {
                Reply(info.PendingGetSequence.front(), msg->Status, msg->Issues);
                info.PendingGetSequence.pop_front();
            }
        }

        OnChanged(database, pathId, info);
    }

    void TSequenceProxy::OnChanged(const TString& database, const TPathId& pathId, TSequenceByPathId& info) {
        while (info.TotalCached > 0 && !info.PendingNextVal.empty()) {
            const auto& request = info.PendingNextVal.front();
            DoReplyFromCache(request, pathId, info);
            info.PendingNextVal.pop_front();
            --info.TotalRequested;
        }

        if (info.TotalRequested > info.TotalAllocating && !info.AllocateInProgress) {
            Y_ABORT_UNLESS(info.TotalAllocating == 0);
            ui64 cache = Max(info.DefaultCacheSize, info.TotalRequested);
            StartAllocate(info.LastKnownTabletId, database, pathId, cache);
            info.AllocateInProgress = true;
            info.TotalAllocating += cache;
        }

        if (!info.PendingGetSequence.empty() && !info.GetSequenceInProgress) {
            StartGetSequence(info.LastKnownTabletId, database, pathId);
            info.GetSequenceInProgress = true;
        }
    }

    template <class TRequestInfo>
    bool TSequenceProxy::DoMaybeReplyUnauthorized(const TRequestInfo& request, const TPathId& pathId, TSequenceByPathId& info) {
        if (request.UserToken && info.SecurityObject) {
            ui32 access = NACLib::EAccessRights::SelectRow;
            if (!info.SecurityObject->CheckAccess(access, *request.UserToken)) {
                const TString error = TStringBuilder()
                    << "Access denied for " << request.UserToken->GetUserSID() << " to sequence " << pathId;
                NYql::TIssueManager issueManager;
                issueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error));
                Reply(request, Ydb::StatusIds::UNAUTHORIZED, issueManager.GetIssues());
                return true;
            }
        }

        return false;
    }

    bool TSequenceProxy::DoReplyFromCache(const TNextValRequestInfo& request, const TPathId& pathId, TSequenceByPathId& info) {
        if (DoMaybeReplyUnauthorized(request, pathId, info)) {
            return false;
        }

        Y_ABORT_UNLESS(info.TotalCached > 0);
        Y_ABORT_UNLESS(!info.CachedAllocations.empty());
        auto& front = info.CachedAllocations.front();
        Y_ABORT_UNLESS(front.Count > 0);
        Reply(request, pathId, front.Start);
        --info.TotalCached;
        if (--front.Count > 0) {
            front.Start += front.Increment;
        } else {
            info.CachedAllocations.pop_front();
        }
        return true;
    }

} // namespace NSequenceProxy
} // namespace NKikimr
