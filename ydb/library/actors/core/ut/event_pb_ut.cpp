#include "event_pb.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/protos/unittests.pb.h>

Y_UNIT_TEST_SUITE(TEventSerialization) {
    struct TMockEvent: public NActors::IEventBase {
        TBigMessage* msg;
        bool
        SerializeToArcadiaStream(NActors::TChunkSerializer* chunker) const override {
            return msg->SerializeToZeroCopyStream(chunker);
        }
        bool IsSerializable() const override {
            return true;
        }
        TString ToStringHeader() const override {
            return TString();
        }
        virtual TString Serialize() const {
            return TString();
        }
        ui32 Type() const override {
            return 0;
        };
    };

    Y_UNIT_TEST(Coroutine) {
        TString strA(507, 'a');
        TString strB(814, 'b');
        TString strC(198, 'c');

        TBigMessage bm;

        TSimple* simple0 = bm.AddSimples();
        simple0->SetStr1(strA);
        simple0->SetStr2(strB);
        simple0->SetNumber1(213431324);

        TSimple* simple1 = bm.AddSimples();
        simple1->SetStr1(strC);
        simple1->SetStr2(strA);
        simple1->SetNumber1(21039313);

        bm.AddManyStr(strA);
        bm.AddManyStr(strC);
        bm.AddManyStr(strB);

        bm.SetOneMoreStr(strB);
        bm.SetYANumber(394143);

        TString bmSerialized;
        Y_PROTOBUF_SUPPRESS_NODISCARD bm.SerializeToString(&bmSerialized);
        UNIT_ASSERT_UNEQUAL(bmSerialized.size(), 0);
        UNIT_ASSERT_UNEQUAL(0, 0);

        NActors::TCoroutineChunkSerializer chunker;
        for (int i = 0; i < 4; ++i) {
            TMockEvent event;
            event.msg = &bm;
            chunker.SetSerializingEvent(&event);
            char buf1[87];
            TString bmChunkedSerialized;
            while (!chunker.IsComplete()) {
                auto range = chunker.FeedBuf(&buf1[0], sizeof(buf1));
                for (auto [data, size] : range) {
                    bmChunkedSerialized.append(data, size);
                }
            }
            UNIT_ASSERT_EQUAL(bmSerialized, bmChunkedSerialized);
        }
    }
}
