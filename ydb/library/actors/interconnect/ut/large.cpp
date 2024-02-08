#include "lib/ic_test_cluster.h"
#include "lib/test_events.h"
#include "lib/test_actors.h"

#include <ydb/library/actors/interconnect/interconnect_tcp_proxy.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/event.h>
#include <util/system/sanitizers.h>

Y_UNIT_TEST_SUITE(LargeMessage) {
    using namespace NActors;

    class TProducer: public TActorBootstrapped<TProducer> {
        const TActorId RecipientActorId;

    public:
        TProducer(const TActorId& recipientActorId)
            : RecipientActorId(recipientActorId)
        {}

        void Bootstrap(const TActorContext& ctx) {
            Become(&TThis::StateFunc);
            ctx.Send(RecipientActorId, new TEvTest(1, "hello"), IEventHandle::FlagTrackDelivery, 1);
            ctx.Send(RecipientActorId, new TEvTest(2, TString(150 * 1024 * 1024, 'X')), IEventHandle::FlagTrackDelivery, 2);
        }

        void Handle(TEvents::TEvUndelivered::TPtr ev, const TActorContext& ctx) {
            if (ev->Cookie == 2) {
                Cerr << "TEvUndelivered\n";
                ctx.Send(RecipientActorId, new TEvTest(3, "hello"), IEventHandle::FlagTrackDelivery, 3);
            }
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvents::TEvUndelivered, Handle)
        )
    };

    class TConsumer : public TActorBootstrapped<TConsumer> {
        TManualEvent& Done;
        TActorId SessionId;

    public:
        TConsumer(TManualEvent& done)
            : Done(done)
        {
        }

        void Bootstrap(const TActorContext& /*ctx*/) {
            Become(&TThis::StateFunc);
        }

        void Handle(TEvTest::TPtr ev, const TActorContext& /*ctx*/) {
            const auto& record = ev->Get()->Record;
            Cerr << "RECEIVED TEvTest\n";
            if (record.GetSequenceNumber() == 1) {
                Y_ABORT_UNLESS(!SessionId);
                SessionId = ev->InterconnectSession;
            } else if (record.GetSequenceNumber() == 3) {
                Y_ABORT_UNLESS(SessionId != ev->InterconnectSession);
                Done.Signal();
            } else {
                Y_ABORT("incorrect sequence number");
            }
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvTest, Handle)
        )
    };

    Y_UNIT_TEST(Test) {
        TTestICCluster testCluster(2);

        TManualEvent done;
        TConsumer* consumer = new TConsumer(done);
        const TActorId recp = testCluster.RegisterActor(consumer, 1);
        testCluster.RegisterActor(new TProducer(recp), 2);
        done.WaitI();
    }

}
