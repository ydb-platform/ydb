#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/simple/mvcc_version.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpQueryRequest) {
    Y_UNIT_TEST(MvccVersionSurvivesSerialization) {
        using TRpc = NGRpcService::TGrpcRequestOperationCall<Ydb::Table::ExecuteDataQueryRequest, Ydb::Table::ExecuteDataQueryResponse>;
        using TCallback = std::function<void(const Ydb::Table::ExecuteDataQueryResponse&)>;
        using TContext = NRpcService::TLocalRpcCtx<TRpc, TCallback>;

        const TMaybe<TString> values[] = { Nothing(), TString(), TString("123:456"), TString("invalid") };
        for (const auto& value : values) {
            for (bool internal : { false, true }) {
                auto context = std::make_shared<TContext>(Ydb::Table::ExecuteDataQueryRequest(), [](const auto&) {
                }, "/Root", Nothing(), TString("test-request"), internal);
                if (value) {
                    context->PutPeerMeta(YDB_MVCC_VERSION_HEADER, *value);
                }
                TEvKqp::TEvQueryRequest request(NKikimrKqp::QUERY_ACTION_EXECUTE, NKikimrKqp::QUERY_TYPE_SQL_DML, TActorId(), context, "session",
                    TString("SELECT 1"), TString(), nullptr, nullptr, Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE, nullptr, nullptr);
                UNIT_ASSERT(request.GetMvccVersion() == value);

                const auto size = request.CalculateSerializedSize();   // Drops RequestCtx.
                UNIT_ASSERT(request.GetMvccVersion() == value);
                NActors::TAllocChunkSerializer serializer;
                UNIT_ASSERT(request.SerializeToArcadiaStream(&serializer));
                auto buffer = serializer.Release(request.CreateSerializationInfo(false));
                UNIT_ASSERT_VALUES_EQUAL(buffer->GetSize(), size);
                std::unique_ptr<TEvKqp::TEvQueryRequest> restored(TEvKqp::TEvQueryRequest::Load(buffer.Get()));
                UNIT_ASSERT(restored->GetMvccVersion() == value);
                UNIT_ASSERT_VALUES_EQUAL(restored->IsInternalCall(), internal);
                UNIT_ASSERT_VALUES_EQUAL(restored->GetRequestType(), "test-request");
            }
        }
    }
}

}   // namespace NKikimr::NKqp
