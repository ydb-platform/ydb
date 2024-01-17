#include "sequenceproxy_impl.h"

#include <ydb/core/base/path.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/yql/public/issue/yql_issue_manager.h>

#include <ydb/library/actors/core/log.h>
#include <util/string/builder.h>

#define TXLOG_LOG(priority, stream) \
    LOG_LOG_S(*TlsActivationContext, priority, NKikimrServices::SEQUENCEPROXY, LogPrefix << stream)
#define TXLOG_DEBUG(stream) TXLOG_LOG(NActors::NLog::PRI_DEBUG, stream)
#define TXLOG_NOTICE(stream) TXLOG_LOG(NActors::NLog::PRI_NOTICE, stream)
#define TXLOG_ERROR(stream) TXLOG_LOG(NActors::NLog::PRI_ERROR, stream)

namespace NKikimr {
namespace NSequenceProxy {

    namespace {

        using TSchemeCacheNavigate = NSchemeCache::TSchemeCacheNavigate;
        using TEntry = TSchemeCacheNavigate::TEntry;
        using ERequestType = TEntry::ERequestType;

        void InitPath(TEntry& entry, const TString& path) {
            entry.Path = SplitPath(path);
            entry.RequestType = ERequestType::ByPath;
        }

        void InitPath(TEntry& entry, const TPathId& pathId) {
            entry.TableId.PathId = pathId;
            entry.RequestType = ERequestType::ByTableId;
        }

        void InitPath(TEntry& entry, const std::variant<TString, TPathId>& path) {
            std::visit(
                [&entry](const auto& path) {
                    InitPath(entry, path);
                },
                path);
        }

        NYql::TIssues MakeResolveIssues(const TString& message) {
            NYql::TIssueManager issueManager;
            issueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, message));
            return issueManager.GetIssues();
        }

    } // namespace

    ui64 TSequenceProxy::StartResolve(const TString& database, const std::variant<TString, TPathId>& path, bool syncVersion) {
        ui64 cookie = ++LastCookie;
        auto& info = ResolveInFlight[cookie];
        info.Database = database;
        info.Path = path;

        auto schemeCache = MakeSchemeCacheID();
        auto req = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
        req->DatabaseName = database;
        auto& entry = req->ResultSet.emplace_back();
        InitPath(entry, path);
        entry.ShowPrivatePath = true;
        entry.SyncVersion = syncVersion;
        Send(schemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(req.release()), 0, cookie);

        return cookie;
    }

    void TSequenceProxy::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        ui64 cookie = ev->Cookie;
        auto it = ResolveInFlight.find(cookie);
        Y_ABORT_UNLESS(it != ResolveInFlight.end(), "TEvNavigateKeySetResult with cookie %" PRIu64 " does not match a previous request", cookie);
        auto database = std::move(it->second.Database);
        auto path = std::move(it->second.Path);
        ResolveInFlight.erase(it);

        auto req = std::move(ev->Get()->Request);
        if (req->ErrorCount > 0) {
            std::visit(
                [this, &database](auto& path) {
                    OnResolveError(database, path, Ydb::StatusIds::SCHEME_ERROR, MakeResolveIssues(TStringBuilder()
                        << "Failed to resolve sequence " << path));
                },
                path);
            return;
        }

        auto& entry = req->ResultSet.at(0);
        if (!entry.SequenceInfo || entry.SequenceInfo->Description.GetSequenceShard() == 0) {
            std::visit(
                [this, &database](auto& path) {
                    OnResolveError(database, path, Ydb::StatusIds::SCHEME_ERROR, MakeResolveIssues(TStringBuilder()
                        << "Failed to resolve sequence " << path));
                },
                path);
            return;
        }

        TResolveResult result{
            entry.TableId.PathId,
            std::move(entry.SequenceInfo),
            std::move(entry.SecurityObject),
        };
        std::visit(
            [this, &database, &result](auto& path) {
                OnResolveResult(database, path, std::move(result));
            },
            path);
    }

} // namespace NSequenceProxy
} // namespace NKikimr
