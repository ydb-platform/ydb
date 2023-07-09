#include "sequenceproxy_impl.h"

#include <ydb/core/base/path.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
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

    class TSequenceProxy::TResolveActor : public TActorBootstrapped<TResolveActor> {
        using TSchemeCacheNavigate = NSchemeCache::TSchemeCacheNavigate;
        using TEntry = TSchemeCacheNavigate::TEntry;
        using ERequestType = TEntry::ERequestType;

    private:
        struct TOutputHelper {
            const std::variant<TString, TPathId>& value;

            friend inline IOutputStream& operator<<(IOutputStream& out, const TOutputHelper& helper) {
                std::visit(
                    [&out](const auto& value) {
                        out << value;
                    },
                    helper.value);
                return out;
            }
        };

    public:
        TResolveActor(
                TActorId owner, ui64 cookie,
                const TString& database,
                const std::variant<TString, TPathId>& path,
                bool syncVersion)
            : Owner(owner)
            , Cookie(cookie)
            , Database(database)
            , Path(path)
            , SyncVersion(syncVersion)
        { }

        void Bootstrap() {
            auto schemeCache = MakeSchemeCacheID();
            auto req = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
            req->DatabaseName = Database;
            auto& entry = req->ResultSet.emplace_back();
            std::visit(
                [&entry](const auto& path) {
                    InitPath(entry, path);
                },
                Path);
            entry.ShowPrivatePath = true;
            entry.SyncVersion = SyncVersion;
            Send(schemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(req.Release()));
            Become(&TThis::StateWork);
        }

        static void InitPath(TEntry& entry, const TString& path) {
            entry.Path = SplitPath(path);
            entry.RequestType = ERequestType::ByPath;
        }

        static void InitPath(TEntry& entry, const TPathId& pathId) {
            entry.TableId.PathId = pathId;
            entry.RequestType = ERequestType::ByTableId;
        }

    private:
        void ReplyAndDie(Ydb::StatusIds::StatusCode status, const TString& error) {
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, error));
            auto res = MakeHolder<TEvPrivate::TEvResolveResult>();
            res->Status = status;
            res->Issues = IssueManager.GetIssues();
            Send(Owner, res.Release(), 0, Cookie);
            PassAway();
        }

    private:
        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            }
        }

        void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            auto req = std::move(ev->Get()->Request);
            if (req->ErrorCount > 0) {
                ReplyAndDie(Ydb::StatusIds::SCHEME_ERROR, TStringBuilder()
                    << "Failed to resolve sequence " << TOutputHelper{ Path });
                return;
            }

            auto& res = req->ResultSet.at(0);
            if (!res.SequenceInfo || res.SequenceInfo->Description.GetSequenceShard() == 0) {
                ReplyAndDie(Ydb::StatusIds::SCHEME_ERROR, TStringBuilder()
                    << "Failed to resolve sequence " << TOutputHelper{ Path });
                return;
            }

            auto reply = MakeHolder<TEvPrivate::TEvResolveResult>();
            reply->Status = Ydb::StatusIds::SUCCESS;
            reply->PathId = res.TableId.PathId;
            reply->SequenceInfo = res.SequenceInfo;
            reply->SecurityObject = res.SecurityObject;
            Send(Owner, reply.Release(), 0, Cookie);
            PassAway();
        }

    private:
        const TActorId Owner;
        const ui64 Cookie;
        const TString Database;
        const std::variant<TString, TPathId> Path;
        const bool SyncVersion;
        NYql::TIssueManager IssueManager;
    };

    ui64 TSequenceProxy::StartResolve(const TString& database, const std::variant<TString, TPathId>& path, bool syncVersion) {
        ui64 cookie = ++LastCookie;
        auto& info = ResolveInFlight[cookie];
        info.Database = database;
        info.Path = path;
        Register(new TResolveActor(SelfId(), cookie, database, path, syncVersion));
        return cookie;
    }

} // namespace NSequenceProxy
} // namespace NKikimr
