#include <ydb/services/udf_store/table_query.h>
#include <ydb/services/udf_store/metadata_subscription/wasm_artifact.h>

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NUdfStore::NTableQuery {
namespace {

class TStorage {
public:
    TStorage()
        : Session(Runner.GetTableClient().CreateSession().GetValueSync().GetSession())
    {
        for (const auto& path : {CpuA, CpuB}) {
            TStringBuilder ddl;
            ddl << "CREATE TABLE `" << path << "` (";
            for (const auto& column : TUdfWasmArtifact::GetColumnDescription()) {
                ddl << column.GetName() << " " << column.GetType() << ", ";
            }
            ddl << "PRIMARY KEY (id, kind, uid));";
            const auto result = Session.ExecuteSchemeQuery(ddl).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    NYdb::NTable::TDataQueryResult Execute(const TString& query,
        const TString& uid = "uid-1", const TString& error = "")
    {
        Ydb::Table::ExecuteDataQueryRequest request;
        if (error.empty()) {
            SetSelectArtifactParams(request, "module", "module", uid);
        } else {
            SetMarkArtifactFailedParams(request, "module", "module", uid, error);
        }
        return Execute(query, request);
    }

    NYdb::NTable::TDataQueryResult Execute(const TString& query,
        const Ydb::Table::ExecuteDataQueryRequest& request)
    {
        auto params = NYdb::TParamsBuilder().Build();
        *NYdb::TProtoAccessor::GetProtoMapPtr(params) = request.parameters();
        auto result = Session.ExecuteDataQuery(query,
            NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx(),
            params).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString() << "\n" << query);
        return result;
    }

    TMaybe<TArtifactCompileState> Read(const TString& path, const TString& uid = "uid-1") {
        const auto result = Execute(BuildSelectArtifactCompileStateQuery(path), uid);
        Ydb::Table::ExecuteQueryResult proto;
        *proto.add_result_sets() = NYdb::TProtoAccessor::GetProto(result.GetResultSet(0));
        Ydb::Table::ExecuteDataQueryResponse response;
        response.mutable_operation()->set_status(Ydb::StatusIds::SUCCESS);
        response.mutable_operation()->mutable_result()->PackFrom(proto);
        TMaybe<TArtifactCompileState> state;
        UNIT_ASSERT(ParseArtifactCompileStateResponse(response, state));
        return state;
    }

    void Publish(const TString& path) {
        TWasmArtifactRow artifact;
        artifact.Id = "module";
        artifact.Kind = "module";
        artifact.Uid = "uid-1";
        artifact.ObjectCodeSize = 16;
        artifact.ObjectCodeChunkCount = 1;
        Ydb::Table::ExecuteDataQueryRequest request;
        SetUpsertArtifactParams(request, artifact);
        Execute(BuildUpsertArtifactQuery(path), request);
    }

    const TString CpuA = "/Root/cpu_a";
    const TString CpuB = "/Root/cpu_b";
    NKqp::TKikimrRunner Runner;
    NYdb::NTable::TSession Session;
};

} // namespace

Y_UNIT_TEST_SUITE(ArtifactCompileState) {
    Y_UNIT_TEST(RejectsMalformedStateAndPreservesTimestampPresence) {
        Ydb::Table::ExecuteQueryResult proto;
        auto& rows = *proto.add_result_sets();
        for (const auto& name : {"compile_status", "compile_error", "compile_started_at", "compile_finished_at"}) {
            rows.add_columns()->set_name(name);
        }
        auto& row = *rows.add_rows();
        row.add_items()->set_text_value("compiling");
        row.add_items()->set_text_value("");
        row.add_items()->set_uint64_value(0);
        row.add_items()->set_null_flag_value(google::protobuf::NULL_VALUE);
        Ydb::Table::ExecuteDataQueryResponse response;
        response.mutable_operation()->set_status(Ydb::StatusIds::SUCCESS);
        auto parse = [&](TMaybe<TArtifactCompileState>& state) {
            response.mutable_operation()->mutable_result()->PackFrom(proto);
            return ParseArtifactCompileStateResponse(response, state);
        };
        TMaybe<TArtifactCompileState> state;
        UNIT_ASSERT(parse(state));
        UNIT_ASSERT(state && state->StartedAt);
        UNIT_ASSERT_VALUES_EQUAL(state->StartedAt->MicroSeconds(), 0);
        UNIT_ASSERT(!state->FinishedAt);
        row.mutable_items(0)->set_text_value("unknown");
        UNIT_ASSERT(!parse(state));
        UNIT_ASSERT(state->Status == ECompileStatus::Compiling);
        row.mutable_items(0)->set_text_value("compiling");
        rows.set_truncated(true);
        UNIT_ASSERT(!parse(state));
        rows.set_truncated(false);
        row.mutable_items(2)->set_text_value("invalid timestamp");
        UNIT_ASSERT(!parse(state));
        rows.clear_rows();
        UNIT_ASSERT(parse(state));
        UNIT_ASSERT(!state);
        response.mutable_operation()->set_status(Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT(!parse(state));
    }

    Y_UNIT_TEST(IndependentPlatformsAndUploads) {
        TStorage store;
        UNIT_ASSERT(!store.Read(store.CpuA));
        for (const auto& path : {store.CpuA, store.CpuB}) {
            store.Execute(BuildEnsurePendingArtifactQuery(path));
            auto state = store.Read(path);
            UNIT_ASSERT(state);
            UNIT_ASSERT(state->Status == ECompileStatus::Pending);
            UNIT_ASSERT(state->Error.empty());
            UNIT_ASSERT(!state->StartedAt && !state->FinishedAt);
            store.Execute(BuildMarkArtifactCompilingQuery(path));
            state = store.Read(path);
            UNIT_ASSERT(state->Status == ECompileStatus::Compiling);
            UNIT_ASSERT(state->StartedAt && !state->FinishedAt);
            const auto started = state->StartedAt;
            store.Execute(BuildEnsurePendingArtifactQuery(path));
            UNIT_ASSERT(store.Read(path)->StartedAt == started);
        }

        const auto startedA = store.Read(store.CpuA)->StartedAt;
        store.Publish(store.CpuA);
        store.Execute(BuildMarkArtifactFailedQuery(store.CpuB), "uid-1", "unsupported CPU");
        auto a = store.Read(store.CpuA);
        auto b = store.Read(store.CpuB);
        UNIT_ASSERT(a->Status == ECompileStatus::Ready);
        UNIT_ASSERT(a->StartedAt == startedA);
        UNIT_ASSERT(a->FinishedAt && *a->FinishedAt >= *a->StartedAt);
        UNIT_ASSERT(a->Error.empty());
        UNIT_ASSERT(b->Status == ECompileStatus::Failed);
        UNIT_ASSERT_VALUES_EQUAL(b->Error, "unsupported CPU");
        UNIT_ASSERT(b->FinishedAt && *b->FinishedAt >= *b->StartedAt);

        for (const auto& path : {store.CpuA, store.CpuB}) {
            store.Execute(BuildEnsurePendingArtifactQuery(path));
        }
        UNIT_ASSERT(store.Read(store.CpuA)->Status == ECompileStatus::Ready);
        UNIT_ASSERT(store.Read(store.CpuB)->Status == ECompileStatus::Failed);

        store.Execute(BuildEnsurePendingArtifactQuery(store.CpuB), "uid-2");
        store.Execute(BuildMarkArtifactCompilingQuery(store.CpuB));
        b = store.Read(store.CpuB);
        UNIT_ASSERT(b->Status == ECompileStatus::Compiling);
        UNIT_ASSERT(b->Error.empty());
        UNIT_ASSERT(b->StartedAt && !b->FinishedAt);
        UNIT_ASSERT(store.Read(store.CpuB, "uid-2")->Status == ECompileStatus::Pending);
        UNIT_ASSERT(store.Read(store.CpuA)->Status == ECompileStatus::Ready);

        // Late failure/start requests cannot demote a successfully published artifact.
        store.Execute(BuildMarkArtifactFailedQuery(store.CpuA), "uid-1", "late error");
        store.Execute(BuildMarkArtifactCompilingQuery(store.CpuA));
        UNIT_ASSERT(store.Read(store.CpuA)->Status == ECompileStatus::Ready);
        UNIT_ASSERT(store.Read(store.CpuA)->FinishedAt == a->FinishedAt);
    }
}

} // namespace NKikimr::NUdfStore::NTableQuery
