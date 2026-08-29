#include <ydb/core/persqueue/writer/partition_chooser_impl__abstract_chooser_actor.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/persqueue/public/nameresolver/nameresolver.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <library/cpp/testing/unittest/registar.h>

#include <stdexcept>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NPQ;
using namespace NKikimr::NPQ::NPartitionChooser;
using namespace Ydb::PersQueue::ErrorCode;

namespace {

struct TStubPipeCreator {
    static IActor* CreateClient(const TActorId&, ui64, const NTabletPipe::TClientConfig& = {}) {
        Y_ABORT("pipe is not expected in this test");
    }

    static void SendData(const TActorContext&, const TActorId&, IEventBase*, ui64 = 0, NWilson::TTraceId = {}) {
        Y_ABORT("pipe is not expected in this test");
    }
};

class TThrowingPartitionChooserActor
    : public TAbstractPartitionChooserActor<TThrowingPartitionChooserActor, TStubPipeCreator>
{
public:
    using TParent = TAbstractPartitionChooserActor<TThrowingPartitionChooserActor, TStubPipeCreator>;

    TThrowingPartitionChooserActor(
        TActorId parentId,
        const std::shared_ptr<IPartitionChooser>& chooser,
        NKikimr::NPQ::NNameResolver::TTopicNamesPtr& fullConverter)
        : TParent(parentId, chooser, fullConverter, "source", std::nullopt, {})
    {
    }

    void Bootstrap(const TActorContext&) {
        throw std::runtime_error("boom");
    }

    void OnSelected(const TActorContext&) override {
        Y_ABORT("not expected");
    }
};

NKikimr::NPQ::NNameResolver::TTopicNamesPtr MakeTopicConverter() {
    NKikimrSchemeOp::TPersQueueGroupDescription config;
    auto* pqConfig = config.MutablePQTabletConfig();
    pqConfig->SetTopicName("topic-1");
    pqConfig->SetTopicPath("/Root/topic-1");
    return NKikimr::NPQ::NNameResolver::MakeTopicNamesPtr(
        NKikimr::NPQ::NNameResolver::NamesFromFirstClassConfig(*pqConfig));
}

} // namespace

Y_UNIT_TEST_SUITE(TPartitionChooserException) {

Y_UNIT_TEST(OnExceptionRepliesError) {
    TTestBasicRuntime runtime(1, false);
    TAppPrepare app;
    app.FeatureFlags.SetEnableTabletRestartOnUnhandledExceptions(true);
    runtime.Initialize(app.Unwrap());

    auto edge = runtime.AllocateEdgeActor();
    auto converter = MakeTopicConverter();

    auto actorId = runtime.Register(new TThrowingPartitionChooserActor(edge, /*chooser=*/nullptr, converter));
    runtime.EnableScheduleForActor(actorId);
    runtime.DispatchEvents();

    auto error = runtime.GrabEdgeEvent<TEvPartitionChooser::TEvChooseError>();
    UNIT_ASSERT(error);
    UNIT_ASSERT_EQUAL(error->Code, ErrorCode::ERROR);
    UNIT_ASSERT(error->ErrorMessage.Contains("Unhandled exception"));
    UNIT_ASSERT(error->ErrorMessage.Contains("boom"));
}

} // Y_UNIT_TEST_SUITE(TPartitionChooserException)
