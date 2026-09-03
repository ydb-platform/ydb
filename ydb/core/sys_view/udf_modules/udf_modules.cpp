#include "udf_modules.h"

#include <ydb/core/sys_view/common/registry.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/library/query_actor/query_actor.h>

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/string/builder.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::SYSTEM_VIEWS

namespace NKikimr::NSysView {

namespace {

struct TEvPrivate {
    enum EEv : ui32 {
        EvStart = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvFetchUdfModulesResult = EvStart,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    struct TModuleRow {
        TString Uid;
        TString Md5;
        TString Name;
        TString Type;
        ui64 Version = 0;
        ui64 Size = 0;
        ui64 ChunkCount = 0;
        TString CompileStatus;
        TString CompileError;
        TInstant CreatedAt;
        TInstant CompileStartedAt;
        TInstant CompileFinishedAt;
        TString Manifest;
    };

    struct TEvFetchUdfModulesResult : public TEventLocal<TEvFetchUdfModulesResult, EvFetchUdfModulesResult> {
        TEvFetchUdfModulesResult(Ydb::StatusIds::StatusCode status, TVector<TModuleRow>&& rows, NYql::TIssues&& issues)
            : Status(status)
            , Rows(std::move(rows))
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        TVector<TModuleRow> Rows;
        const NYql::TIssues Issues;
    };
};

class TUdfModulesFetcherActor final
    : public TQueryBase
    , public TQueryRetryActorMixin<TUdfModulesFetcherActor, TEvPrivate::TEvFetchUdfModulesResult>
{
public:
    TUdfModulesFetcherActor(const TString& database, const TString& modulesTablePath, bool reverse)
        : TQueryBase(NKikimrServices::SYSTEM_VIEWS, /* sessionId */ {}, database, /* isSystemUser */ true)
        , ModulesTablePath_(modulesTablePath)
        , Reverse_(reverse)
    {
        SetOperationInfo(__func__, database);
    }

private:
    void OnRunQuery() final {
        const TString sql = TStringBuilder()
            << "SELECT "
            << "uid, md5, name, type, version, size, chunk_count, "
            << "compile_status, compile_error, "
            << "created_at, compile_started_at, compile_finished_at, "
            << "CAST(manifest AS Utf8) AS manifest "
            << "FROM `" << ModulesTablePath_ << "` "
            << "ORDER BY uid " << (Reverse_ ? "DESC" : "ASC") << ";";

        RunDataQuery(sql, nullptr, TTxControl::BeginAndCommitTx(/* snapshotRead */ true));
    }

    void OnQueryResult() final {
        if (ResultSets.size() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected result set count for udf_modules");
            return;
        }

        NYdb::TResultSetParser parser(ResultSets[0]);
        while (parser.TryNextRow()) {
            TEvPrivate::TModuleRow row;
            if (auto value = parser.ColumnParser("uid").GetOptionalUtf8()) {
                row.Uid = *value;
            } else {
                continue;
            }
            if (auto value = parser.ColumnParser("md5").GetOptionalUtf8()) {
                row.Md5 = *value;
            }
            if (auto value = parser.ColumnParser("name").GetOptionalUtf8()) {
                row.Name = *value;
            }
            if (auto value = parser.ColumnParser("type").GetOptionalUtf8()) {
                row.Type = *value;
            }
            if (auto value = parser.ColumnParser("version").GetOptionalUint64()) {
                row.Version = *value;
            }
            if (auto value = parser.ColumnParser("size").GetOptionalUint64()) {
                row.Size = *value;
            }
            if (auto value = parser.ColumnParser("chunk_count").GetOptionalUint64()) {
                row.ChunkCount = *value;
            }
            if (auto value = parser.ColumnParser("compile_status").GetOptionalUtf8()) {
                row.CompileStatus = *value;
            }
            if (auto value = parser.ColumnParser("compile_error").GetOptionalUtf8()) {
                row.CompileError = *value;
            }
            if (auto value = parser.ColumnParser("created_at").GetOptionalTimestamp()) {
                row.CreatedAt = *value;
            }
            if (auto value = parser.ColumnParser("compile_started_at").GetOptionalTimestamp()) {
                row.CompileStartedAt = *value;
            }
            if (auto value = parser.ColumnParser("compile_finished_at").GetOptionalTimestamp()) {
                row.CompileFinishedAt = *value;
            }
            if (auto value = parser.ColumnParser("manifest").GetOptionalUtf8()) {
                row.Manifest = *value;
            }
            Rows_.push_back(std::move(row));
        }
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) final {
        // Missing modules table (udf_store disabled) → empty result, not an error.
        if (status == Ydb::StatusIds::SCHEME_ERROR || status == Ydb::StatusIds::NOT_FOUND) {
            Send(Owner, new TEvPrivate::TEvFetchUdfModulesResult(
                Ydb::StatusIds::SUCCESS, TVector<TEvPrivate::TModuleRow>(), NYql::TIssues()));
            return;
        }
        Send(Owner, new TEvPrivate::TEvFetchUdfModulesResult(status, std::move(Rows_), std::move(issues)));
    }

private:
    const TString ModulesTablePath_;
    const bool Reverse_;
    TVector<TEvPrivate::TModuleRow> Rows_;
};

class TUdfModulesScan : public TScanActorWithoutBackPressure<TUdfModulesScan> {
public:
    using TBase = TScanActorWithoutBackPressure<TUdfModulesScan>;
    using TSchema = Schema::UdfModules;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    TUdfModulesScan(
        const NActors::TActorId& ownerId,
        ui32 scanId,
        const TString& database,
        const NKikimrSysView::TSysViewDescription& sysViewInfo,
        const TTableRange& tableRange,
        const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken,
        bool reverse)
        : TBase(ownerId, scanId, database, sysViewInfo, tableRange, columns)
        , UserToken_(std::move(userToken))
        , Reverse_(reverse)
    {
        Y_UNUSED(UserToken_);
    }

    STFUNC(StateScan) {
        switch (ev->GetTypeRewrite()) {
            sFunc(NKqp::TEvKqpCompute::TEvScanDataAck, HandleAck);
            hFunc(TEvPrivate::TEvFetchUdfModulesResult, Handle);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, HandleAbortExecution);
            cFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                    "NSysView::TUdfModulesScan: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

private:
    void StartScan() final {
        // Matches NMetadata::NProvider default path and udf_store/storage_paths.
        const TString modulesTablePath = TStringBuilder()
            << DatabaseName << "/.metadata/udf_store/modules";

        YDB_LOG_DEBUG("UdfModules: start fetch from modules table",
            {"path", modulesTablePath});
        Register(TUdfModulesFetcherActor::MakeRetry(SelfId(), DatabaseName, modulesTablePath, Reverse_));
    }

    void Handle(TEvPrivate::TEvFetchUdfModulesResult::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            YDB_LOG_ERROR("UdfModules: fetch failed",
                {"issues", ev->Get()->Issues.ToOneLineString()});
            ReplyErrorAndDie(ev->Get()->Status, ev->Get()->Issues);
            return;
        }

        SendResponse(ev->Get()->Rows);
    }

    void SendResponse(const TVector<TEvPrivate::TModuleRow>& rows) {
        using TExtractor = std::function<TCell(const TEvPrivate::TModuleRow&)>;
        struct TExtractorsMap : public THashMap<NTable::TTag, TExtractor> {
            TExtractorsMap() {
                insert({TSchema::Uid::ColumnId, [](const TEvPrivate::TModuleRow& row) {
                    return TCell(row.Uid.data(), row.Uid.size());
                }});
                insert({TSchema::Md5::ColumnId, [](const TEvPrivate::TModuleRow& row) {
                    return TCell(row.Md5.data(), row.Md5.size());
                }});
                insert({TSchema::Name::ColumnId, [](const TEvPrivate::TModuleRow& row) {
                    return TCell(row.Name.data(), row.Name.size());
                }});
                insert({TSchema::ModuleType::ColumnId, [](const TEvPrivate::TModuleRow& row) {
                    return TCell(row.Type.data(), row.Type.size());
                }});
                insert({TSchema::Version::ColumnId, [](const TEvPrivate::TModuleRow& row) {
                    return TCell::Make<ui64>(row.Version);
                }});
                insert({TSchema::Size::ColumnId, [](const TEvPrivate::TModuleRow& row) {
                    return TCell::Make<ui64>(row.Size);
                }});
                insert({TSchema::ChunkCount::ColumnId, [](const TEvPrivate::TModuleRow& row) {
                    return TCell::Make<ui64>(row.ChunkCount);
                }});
                insert({TSchema::CompileStatus::ColumnId, [](const TEvPrivate::TModuleRow& row) {
                    return TCell(row.CompileStatus.data(), row.CompileStatus.size());
                }});
                insert({TSchema::CompileError::ColumnId, [](const TEvPrivate::TModuleRow& row) {
                    return TCell(row.CompileError.data(), row.CompileError.size());
                }});
                insert({TSchema::CreatedAt::ColumnId, [](const TEvPrivate::TModuleRow& row) {
                    return row.CreatedAt
                        ? TCell::Make<ui64>(row.CreatedAt.MicroSeconds())
                        : TCell();
                }});
                insert({TSchema::CompileStartedAt::ColumnId, [](const TEvPrivate::TModuleRow& row) {
                    return row.CompileStartedAt
                        ? TCell::Make<ui64>(row.CompileStartedAt.MicroSeconds())
                        : TCell();
                }});
                insert({TSchema::CompileFinishedAt::ColumnId, [](const TEvPrivate::TModuleRow& row) {
                    return row.CompileFinishedAt
                        ? TCell::Make<ui64>(row.CompileFinishedAt.MicroSeconds())
                        : TCell();
                }});
                insert({TSchema::Manifest::ColumnId, [](const TEvPrivate::TModuleRow& row) {
                    return TCell(row.Manifest.data(), row.Manifest.size());
                }});
            }
        };
        static TExtractorsMap extractors;

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);
        batch->Finished = true;

        for (const auto& row : rows) {
            if (!StringKeyIsInTableRange({row.Uid})) {
                continue;
            }

            TVector<TCell> cells;
            cells.reserve(Columns.size());
            for (const auto& column : Columns) {
                auto it = extractors.find(column.Tag);
                if (it == extractors.end()) {
                    cells.push_back(TCell());
                } else {
                    cells.push_back(it->second(row));
                }
            }
            batch->Rows.emplace_back(TOwnedCellVec::Make(TArrayRef<const TCell>(cells)));
        }

        SendBatch(std::move(batch));
    }

private:
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken_;
    const bool Reverse_;
};

} // namespace

THolder<NActors::IActor> CreateUdfModulesScan(
    const NActors::TActorId& ownerId,
    ui32 scanId,
    const TString& database,
    const NKikimrSysView::TSysViewDescription& sysViewInfo,
    const TTableRange& tableRange,
    const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    bool reverse)
{
    return MakeHolder<TUdfModulesScan>(
        ownerId, scanId, database, sysViewInfo, tableRange, columns, std::move(userToken), reverse);
}

} // namespace NKikimr::NSysView
