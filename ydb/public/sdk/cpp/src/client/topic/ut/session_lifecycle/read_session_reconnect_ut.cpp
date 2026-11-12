#include <ydb/public/sdk/cpp/src/client/topic/impl/read_session_impl.ipp>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::inline Dev::NTopic {
namespace {

template <bool UseMigrationProtocol>
void CheckDoesNotReconnectAfterAbort() {
    NYdbGrpc::TGRpcClientLow grpcClient(1);
    TAReadSessionSettings<UseMigrationProtocol> settings;
    auto events = std::make_shared<TReadSessionEventsQueue<UseMigrationProtocol>>(settings);
    auto context = MakeWithCallbackContext<TSingleClusterReadSessionImpl<UseMigrationProtocol>>(
        settings, "", "read-session", "", TLog{}, nullptr, events, grpcClient.CreateContext(), 1, 1);
    auto session = context->TryGet();

    session->Abort();
    const bool reconnected = session->Reconnect(TPlainStatus());
    context->Cancel();

    UNIT_ASSERT(!reconnected);
}

} // namespace

Y_UNIT_TEST_SUITE(TReadSessionReconnect) {
    Y_UNIT_TEST(TopicDoesNotReconnectAfterAbort) {
        CheckDoesNotReconnectAfterAbort<false>();
    }

    Y_UNIT_TEST(PersQueueDoesNotReconnectAfterAbort) {
        CheckDoesNotReconnectAfterAbort<true>();
    }
}

} // namespace NYdb::NTopic
