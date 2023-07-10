#include "sequenceproxy_impl.h"

#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/library/yql/public/issue/yql_issue_manager.h>

#include <library/cpp/actors/core/log.h>
#include <util/string/builder.h>

#define TXLOG_LOG(priority, stream) \
    LOG_LOG_S(*TlsActivationContext, priority, NKikimrServices::LONG_TX_SERVICE, LogPrefix << stream)
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

    void TSequenceProxy::DoNextVal(TNextValRequestInfo&& request, const TString& database, const TString& path) {
        auto& info = Databases[database].SequenceByName[path];
        if (!info.ResolveInProgress) {
            StartResolve(database, path, !info.PathId);
            info.ResolveInProgress = true;
        }
        if (!info.PathId) {
            info.PendingNextValResolve.emplace_back(std::move(request));
            return;
        }

        DoNextVal(std::move(request), database, info.PathId, /* needRefresh */ false);
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

    void TSequenceProxy::Handle(TEvPrivate::TEvResolveResult::TPtr& ev) {
        auto* msg = ev->Get();
        auto it = ResolveInFlight.find(ev->Cookie);
        Y_VERIFY(it != ResolveInFlight.end());
        auto database = it->second.Database;
        auto path = it->second.Path;
        ResolveInFlight.erase(it);

        std::visit(
            [&](const auto& path) {
                OnResolveResult(database, path, msg);
            },
            path);
    }

    void TSequenceProxy::OnResolveResult(const TString& database, const TString& path, TEvPrivate::TEvResolveResult* msg) {
        auto& info = Databases[database].SequenceByName[path];
        Y_VERIFY(info.ResolveInProgress);
        info.ResolveInProgress = false;

        if (msg->Status != Ydb::StatusIds::SUCCESS) {
            while (!info.PendingNextValResolve.empty()) {
                const auto& request = info.PendingNextValResolve.front();
                Send(request.Sender, new TEvSequenceProxy::TEvNextValResult(msg->Status, msg->Issues), 0, request.Cookie);
                info.PendingNextValResolve.pop_front();
            }
            return;
        }

        auto pathId = msg->PathId;
        Y_VERIFY(pathId);

        info.PathId = pathId;

        Y_VERIFY(msg->SequenceInfo);

        auto& infoById = Databases[database].SequenceByPathId[pathId];
        infoById.SequenceInfo = msg->SequenceInfo;
        infoById.SecurityObject = msg->SecurityObject;
        infoById.PendingNextValResolve.splice(infoById.PendingNextValResolve.end(), info.PendingNextValResolve);
        OnResolved(database, pathId, infoById);
    }

    void TSequenceProxy::OnResolveResult(const TString& database, const TPathId& pathId, TEvPrivate::TEvResolveResult* msg) {
        auto& info = Databases[database].SequenceByPathId[pathId];
        Y_VERIFY(info.ResolveInProgress);
        info.ResolveInProgress = false;

        if (msg->Status != Ydb::StatusIds::SUCCESS) {
            while (!info.PendingNextValResolve.empty()) {
                const auto& request = info.PendingNextValResolve.front();
                Send(request.Sender, new TEvSequenceProxy::TEvNextValResult(msg->Status, msg->Issues), 0, request.Cookie);
                info.PendingNextValResolve.pop_front();
            }
            return;
        }

        Y_VERIFY(msg->SequenceInfo);
        info.SequenceInfo = msg->SequenceInfo;
        info.SecurityObject = msg->SecurityObject;
        OnResolved(database, pathId, info);
    }

    void TSequenceProxy::OnResolved(const TString& database, const TPathId& pathId, TSequenceByPathId& info) {
        info.LastKnownTabletId = info.SequenceInfo->Description.GetSequenceShard();
        info.DefaultCacheSize = Max(info.SequenceInfo->Description.GetCache(), ui64(1));

        while (!info.PendingNextValResolve.empty()) {
            auto& request = info.PendingNextValResolve.front();
            if (!DoMaybeReplyUnauthorized(request, pathId, info)) {
                info.PendingNextVal.emplace_back(std::move(request));
                ++info.TotalRequested;
            }
            info.PendingNextValResolve.pop_back();
        }

        OnChanged(database, pathId, info);
    }

    void TSequenceProxy::Handle(TEvPrivate::TEvAllocateResult::TPtr& ev) {
        auto it = AllocateInFlight.find(ev->Cookie);
        Y_VERIFY(it != AllocateInFlight.end());
        auto database = it->second.Database;
        auto pathId = it->second.PathId;
        AllocateInFlight.erase(it);

        auto& info = Databases[database].SequenceByPathId[pathId];
        Y_VERIFY(info.AllocateInProgress);
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
            Y_VERIFY(info.TotalAllocating == 0);
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

        Y_VERIFY(info.TotalCached > 0);
        Y_VERIFY(!info.CachedAllocations.empty());
        auto& front = info.CachedAllocations.front();
        Y_VERIFY(front.Count > 0);
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
