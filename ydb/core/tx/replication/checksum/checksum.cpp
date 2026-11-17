#include "checksum.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/request_types.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/scheme/scheme_type_info.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/query_actor/query_actor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>

#include <util/generic/algorithm.h>
#include <util/generic/map.h>
#include <util/string/builder.h>
#include <util/string/escape.h>
#include <util/string/hex.h>
#include <util/string/subst.h>

namespace NKikimr::NReplication::NChecksum {

TRowVersion FromProto(const Ydb::VirtualTimestamp& version) {
    return {version.plan_step(), version.tx_id()};
}

void ToProto(TRowVersion version, Ydb::VirtualTimestamp* proto) {
    proto->set_plan_step(version.Step);
    proto->set_tx_id(version.TxId);
}

bool Equal(const Ydb::Replication::TableChecksum& lhs, const Ydb::Replication::TableChecksum& rhs) {
    return lhs.row_count() == rhs.row_count() && lhs.hash() == rhs.hash();
}

namespace {

struct TEvQueryResult: TEventLocal<TEvQueryResult, EventSpaceBegin(TKikimrEvents::ES_REPLICATION_CONTROLLER) + 101> {
    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;
    Ydb::Replication::TableChecksum Checksum;
};

class TChecksumQueryActor: public TQueryBase {
public:
    TChecksumQueryActor(const TActorId& parent, const TSettings& settings, TString path, TRowVersion snapshot)
        : TQueryBase(NKikimrServices::REPLICATION_CONTROLLER, {}, settings.Database, !settings.UserToken, false, settings.UserToken)
        , Parent(parent)
        , Path(std::move(path))
        , Deadline(settings.Deadline)
        , MaxShardsInFlight(settings.MaxShardsInFlight)
    {
        RequestType = TString(NRequestTypes::ChecksumTable);
        MvccVersion = snapshot;
    }

private:
    void OnRunQuery() override {
        if (Cancelled) {
            return Finish(Ydb::StatusIds::CANCELLED, "Checksum cancelled");
        }
        const auto now = TActivationContext::Now();
        if (now >= Deadline) {
            return Finish(Ydb::StatusIds::TIMEOUT, "Checksum deadline expired");
        }
        auto path = EscapeC(Path);
        SubstGlobal(path, "`", "\\`");
        TStringBuilder query;
        if (MaxShardsInFlight) {
            query << "PRAGMA ydb.MaxSequentialReadsInFlight = \"" << MaxShardsInFlight << "\";\n";
        }
        RunDataQuery(query << "CHECKSUM TABLE `" << path << "` WITH (ALGORITHM = \"CityHash64\");",
            nullptr, TTxControl::BeginAndCommitTx(true), Deadline - now);
    }

    void OnQueryResult() override {
        if (Finished) {
            return;
        }
        if (ResultSets.size() != 1 || ResultSets[0].RowsCount() != 1 || ResultSets[0].ColumnsCount() != 4) {
            return Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected checksum result shape");
        }
        NYdb::TResultSetParser parser(ResultSets[0]);
        parser.TryNextRow();
        if (parser.ColumnParser("version").GetUint32() != 1
            || parser.ColumnParser("algorithm").GetUtf8() != "CityHash64")
        {
            return Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unsupported checksum result format");
        }
        const auto count = parser.ColumnParser("count").GetUint64();
        const auto hash = parser.ColumnParser("hash").GetString();
        if (hash.size() != sizeof(ui64)) {
            return Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected checksum hash width");
        }
        Checksum.set_row_count(count);
        Checksum.set_hash(HexEncode(hash.data(), hash.size()));
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (Reported) {
            return;
        }
        Reported = true;
        auto result = MakeHolder<TEvQueryResult>();
        result->Status = status;
        result->Issues = std::move(issues);
        result->Checksum = std::move(Checksum);
        Send(Parent, result.Release());
    }

    void StateFunc(TAutoPtr<IEventHandle>& ev) override {
        if (ev->GetTypeRewrite() == TEvents::TEvPoison::EventType) {
            Cancelled = true;
            // If CreateSession is still in flight, OnRunQuery will close it as
            // soon as it arrives instead of leaving an orphan KQP session.
            if (SessionId && !Finished) {
                Finish(Ydb::StatusIds::CANCELLED, "Checksum cancelled", false);
            }
        } else {
            TQueryBase::StateFunc(ev);
        }
    }

    const TActorId Parent;
    const TString Path;
    const TInstant Deadline;
    const ui32 MaxShardsInFlight;
    Ydb::Replication::TableChecksum Checksum;
    bool Cancelled = false;
    bool Reported = false;
};

class TChecksumTablesActor: public TActorBootstrapped<TChecksumTablesActor> {
    using TLongTx = NLongTxService::TEvLongTxService;
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
public:
    TChecksumTablesActor(const TActorId& parent, TSettings settings)
        : Parent(parent)
        , Settings(std::move(settings))
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        if (!Settings.Snapshot && !AppData()->FeatureFlags.GetEnableAsyncReplicationChecksumCheck()) {
            return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Replication checksum checks are disabled");
        }
        if (!AppData()->FeatureFlags.GetEnableSnapshotsLocking()) {
            return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Snapshot locking is disabled");
        }
        if (Settings.Request.tables().empty() || Settings.Deadline <= TActivationContext::Now()) {
            return Fail(Ydb::StatusIds::BAD_REQUEST, "Expected tables and a positive checksum timeout");
        }
        Schedule(Settings.Deadline - TActivationContext::Now(), new TEvents::TEvWakeup(1));
        Navigate();
    }

private:
    void Navigate() {
        auto request = MakeHolder<TNavigate>();
        request->DatabaseName = Settings.Database;
        if (Settings.UserToken) {
            request->UserToken = MakeIntrusive<NACLib::TUserToken>(*Settings.UserToken);
        }
        for (const auto& table : Settings.Request.tables()) {
            auto& entry = request->ResultSet.emplace_back();
            entry.Path = SplitPath(table.path());
            entry.Operation = TNavigate::OpTable;
            entry.Access = NACLib::DescribeSchema | NACLib::SelectRow;
            entry.SyncVersion = true;
        }
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& entries = ev->Get()->Request->ResultSet;
        if (entries.size() != static_cast<size_t>(Settings.Request.tables_size())) {
            return Fail(Ydb::StatusIds::INTERNAL_ERROR, "Invalid checksum schema response");
        }
        for (size_t i = 0; i < entries.size(); ++i) {
            const auto& entry = entries[i];
            const auto& table = Settings.Request.tables(i);
            if (entry.Status != TNavigate::EStatus::Ok) {
                return Fail(entry.Status == TNavigate::EStatus::AccessDenied ? Ydb::StatusIds::UNAUTHORIZED
                    : Ydb::StatusIds::SCHEME_ERROR, TStringBuilder() << "Cannot resolve checksum table " << table.path());
            }
            if (entry.Kind != TNavigate::KindTable && entry.Kind != TNavigate::KindColumnTable) {
                return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Checksum target must be a table");
            }
            if (entry.Kind == TNavigate::KindTable && !AppData()->FeatureFlags.GetEnableDataShardSnapshotLocking()) {
                return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "DataShard snapshot locking is disabled");
            }
            if (!Settings.ExpectedTables.empty()
                && (Settings.ExpectedTables.size() != entries.size() || !Settings.ExpectedTables[i].HasSamePath(entry.TableId)))
            {
                return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Checksum destination table was replaced");
            }
            if (!table.changefeed_name().empty()) {
                const auto* stream = FindIfPtr(entry.CdcStreams, [&](const auto& stream) {
                    return stream.GetName() == table.changefeed_name();
                });
                if (!stream || stream->GetState() != NKikimrSchemeOp::ECdcStreamStateReady) {
                    auto result = MakeHolder<TEvResult>();
                    result->Record.set_status(Ydb::StatusIds::SUCCESS);
                    result->Record.set_deferred(true);
                    result->Record.set_complete(true);
                    Send(Parent, result.Release());
                    return PassAway();
                }
            }
            TMap<TString, TString> columns;
            for (const auto& [_, column] : entry.Columns) {
                if (column.IsBuildInProgress || column.SetNotNullInProgress) {
                    return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Table schema is changing");
                }
                columns[column.Name] = TStringBuilder() << NScheme::TypeName(column.PType, column.PTypeMod)
                    << ":" << column.IsNotNullColumn;
            }
            TStringBuilder schema;
            for (const auto& [name, type] : columns) {
                schema << name.size() << ":" << name << type.size() << ":" << type;
            }
            if (Verifying) {
                if (TableIds[i] != entry.TableId || Schemas[i] != schema) {
                    return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Checksum table identity or schema changed");
                }
            } else {
                TableIds.push_back(entry.TableId);
                Schemas.push_back(schema);
            }
        }
        if (Verifying || Settings.Request.probe_only()) {
            auto result = MakeHolder<TEvResult>();
            result->Record.set_status(Ydb::StatusIds::SUCCESS);
            result->Record.set_complete(true);
            Send(Parent, result.Release());
            return PassAway();
        }
        if (Settings.Snapshot) {
            Snapshot = *Settings.Snapshot;
            RunQuery();
        } else {
            AcquireSnapshot();
        }
    }

    void AcquireSnapshot() {
        Send(NLongTxService::MakeLongTxServiceID(SelfId().NodeId()),
            new TLongTx::TEvAcquireReadSnapshot(Settings.Database, TableIds));
    }

    void Handle(TLongTx::TEvAcquireReadSnapshotResult::TPtr& ev) {
        auto& result = *ev->Get();
        if (result.Status != Ydb::StatusIds::SUCCESS) {
            return Fail(result.Status, result.Issues);
        }
        const auto promotion = TDuration::Seconds(AppData()->LongTxServiceConfig.GetLocalSnapshotPromotionTimeSeconds());
        if (result.Snapshot < FromProto(Settings.Request.minimum_snapshot())
            || TInstant::MilliSeconds(result.Snapshot.Step) + promotion <= TActivationContext::Now())
        {
            Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup(0));
            return;
        }
        Snapshot = result.Snapshot;
        Settings.SnapshotHandle = std::move(result.SnapshotHandle);
        auto ready = MakeHolder<TEvResult>();
        ready->Record.set_status(Ydb::StatusIds::SUCCESS);
        ToProto(Snapshot, ready->Record.mutable_snapshot());
        Send(Parent, ready.Release());
        RunQuery();
    }

    void RunQuery() {
        Query = Register(new TChecksumQueryActor(SelfId(), Settings, Settings.Request.tables(NextTable).path(), Snapshot));
    }

    void Handle(TEvQueryResult::TPtr& ev) {
        if (ev->Sender != Query) {
            return;
        }
        Query = {};
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            return Fail(ev->Get()->Status, ev->Get()->Issues);
        }
        auto result = MakeHolder<TEvResult>();
        result->Record.set_status(Ydb::StatusIds::SUCCESS);
        auto* table = result->Record.mutable_table();
        table->set_id(Settings.Request.tables(NextTable).id());
        *table->mutable_checksum() = std::move(ev->Get()->Checksum);
        table->set_schema(Schemas[NextTable]);
        Send(Parent, result.Release());
        if (++NextTable == Settings.Request.tables_size()) {
            Verifying = true;
            Navigate();
        } else {
            RunQuery();
        }
    }

    void Fail(Ydb::StatusIds::StatusCode status, const TString& message) {
        Fail(status, NYql::TIssues{NYql::TIssue(message)});
    }

    void Fail(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) {
        auto result = MakeHolder<TEvResult>();
        result->Record.set_status(status);
        NYql::IssuesToMessage(issues, result->Record.mutable_issues());
        Send(Parent, result.Release());
        PassAway();
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag == 1) {
            Fail(Ydb::StatusIds::TIMEOUT, "Checksum deadline expired");
        } else {
            AcquireSnapshot();
        }
    }

    void PassAway() override {
        if (Query) {
            Send(Query, new TEvents::TEvPoison());
        }
        Settings.SnapshotHandle = NKqp::TSnapshotHandle{};
        TActorBootstrapped::PassAway();
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle)
        hFunc(TLongTx::TEvAcquireReadSnapshotResult, Handle)
        hFunc(TEvQueryResult, Handle)
        hFunc(TEvents::TEvWakeup, Handle)
        sFunc(TEvents::TEvPoison, PassAway)
    )

    const TActorId Parent;
    TSettings Settings;
    TVector<TTableId> TableIds;
    TVector<TString> Schemas;
    TRowVersion Snapshot = TRowVersion::Min();
    TActorId Query;
    int NextTable = 0;
    bool Verifying = false;
};

} // namespace

IActor* CreateChecksumTables(const TActorId& parent, TSettings settings) {
    return new TChecksumTablesActor(parent, std::move(settings));
}

} // namespace NKikimr::NReplication::NChecksum
