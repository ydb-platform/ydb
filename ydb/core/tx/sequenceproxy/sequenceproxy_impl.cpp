#include "sequenceproxy_impl.h"

#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/library/yql/public/issue/yql_issue_manager.h>

#include <library/cpp/actors/core/log.h>
#include <util/string/builder.h>

#define TXLOG_LOG(priority, stream) \
    LOG_LOG_S(*TlsActivationContext, priority, NKikimrServices::SEQUENCEPROXY, LogPrefix << stream)
#define TXLOG_DEBUG(stream) TXLOG_LOG(NActors::NLog::PRI_DEBUG, stream)
#define TXLOG_NOTICE(stream) TXLOG_LOG(NActors::NLog::PRI_NOTICE, stream)
#define TXLOG_ERROR(stream) TXLOG_LOG(NActors::NLog::PRI_ERROR, stream)

namespace NKikimr {
namespace NSequenceProxy {

    void TSequenceProxy::Bootstrap() {
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
        std::visit(
            [&](const auto& path) {
                DoNextVal(std::move(request), msg->Database, path);
            },
            msg->Path);
    }

    void TSequenceProxy::MaybeStartResolve(const TString& database, const TString& path, TSequenceByName& info) {
        if (!info.ResolveInProgress && !info.NewNextValResolve.empty()) {
            info.PendingNextValResolve = std::move(info.NewNextValResolve);
            StartResolve(database, path, !info.PathId);
            info.ResolveInProgress = true;
        }
    }

    void TSequenceProxy::DoNextVal(TNextValRequestInfo&& request, const TString& database, const TString& path) {
        auto& info = Databases[database].SequenceByName[path];
        info.NewNextValResolve.emplace_back(std::move(request));
        MaybeStartResolve(database, path, info);
    }

    void TSequenceProxy::DoNextVal(TNextValRequestInfo&& request, const TString& database, const TPathId& pathId, bool needRefresh) {
        auto& info = Databases[database].SequenceByPathId[pathId];
        if (!info.ResolveInProgress && (needRefresh || !info.SequenceInfo)) {
            StartResolve(database, pathId, !info.SequenceInfo);
            info.ResolveInProgress = true;
        }
        if (!info.SequenceInfo) {
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

    void TSequenceProxy::OnResolveError(const TString& database, const TString& path, Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) {
        auto& info = Databases[database].SequenceByName[path];
        Y_ABORT_UNLESS(info.ResolveInProgress);
        info.ResolveInProgress = false;

        while (!info.PendingNextValResolve.empty()) {
            const auto& request = info.PendingNextValResolve.front();
            Send(request.Sender, new TEvSequenceProxy::TEvNextValResult(status, issues), 0, request.Cookie);
            info.PendingNextValResolve.pop_front();
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
        OnResolved(database, pathId, infoById, info.PendingNextValResolve);

        MaybeStartResolve(database, path, info);
    }

    void TSequenceProxy::OnResolveError(const TString& database, const TPathId& pathId, Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) {
        auto& info = Databases[database].SequenceByPathId[pathId];
        Y_ABORT_UNLESS(info.ResolveInProgress);
        info.ResolveInProgress = false;

        while (!info.PendingNextValResolve.empty()) {
            const auto& request = info.PendingNextValResolve.front();
            Send(request.Sender, new TEvSequenceProxy::TEvNextValResult(status, issues), 0, request.Cookie);
            info.PendingNextValResolve.pop_front();
        }
    }

    void TSequenceProxy::OnResolveResult(const TString& database, const TPathId& pathId, TResolveResult&& result) {
        auto& info = Databases[database].SequenceByPathId[pathId];
        Y_ABORT_UNLESS(info.ResolveInProgress);
        info.ResolveInProgress = false;

        Y_ABORT_UNLESS(result.SequenceInfo);
        info.SequenceInfo = result.SequenceInfo;
        info.SecurityObject = result.SecurityObject;
        OnResolved(database, pathId, info, info.PendingNextValResolve);
    }

    void TSequenceProxy::OnResolved(const TString& database, const TPathId& pathId, TSequenceByPathId& info, TList<TNextValRequestInfo>& resolved) {
        info.LastKnownTabletId = info.SequenceInfo->Description.GetSequenceShard();
        info.DefaultCacheSize = Max(info.SequenceInfo->Description.GetCache(), ui64(1));

        while (!resolved.empty()) {
            auto& request = resolved.front();
            if (!DoMaybeReplyUnauthorized(request, pathId, info)) {
                info.PendingNextVal.emplace_back(std::move(request));
                ++info.TotalRequested;
            }
            resolved.pop_back();
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
            auto& allocated = info.CachedAllocations.emplace_back();
            allocated.Start = msg->Start;
            allocated.Increment = msg->Increment;
            allocated.Count = msg->Count;
            info.TotalCached += msg->Count;
        } else {
            // We will answer up to cache requests with this error
            while (cache > 0 && !info.PendingNextVal.empty()) {
                const auto& request = info.PendingNextVal.front();
                Send(request.Sender, new TEvSequenceProxy::TEvNextValResult(msg->Status, msg->Issues), 0, request.Cookie);
                info.PendingNextVal.pop_front();
                --info.TotalRequested;
                --cache;
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
    }

    bool TSequenceProxy::DoMaybeReplyUnauthorized(const TNextValRequestInfo& request, const TPathId& pathId, TSequenceByPathId& info) {
        if (request.UserToken && info.SecurityObject) {
            ui32 access = NACLib::EAccessRights::SelectRow;
            if (!info.SecurityObject->CheckAccess(access, *request.UserToken)) {
                const TString error = TStringBuilder()
                    << "Access denied for " << request.UserToken->GetUserSID() << " to sequence " << pathId;
                NYql::TIssueManager issueManager;
                issueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error));
                Send(request.Sender, new TEvSequenceProxy::TEvNextValResult(Ydb::StatusIds::UNAUTHORIZED, issueManager.GetIssues()));
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
        Send(request.Sender, new TEvSequenceProxy::TEvNextValResult(pathId, front.Start), 0, request.Cookie);
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
