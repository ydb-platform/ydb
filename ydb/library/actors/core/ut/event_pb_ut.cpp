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
    
    Y_UNIT_TEST(DumpRopeStreamTest) {
        TString testData1 = "Hello";
        TString testData2 = " World!";
        TString testData3 = "\x01\x02\xFF\xFE";

        TRope rope;
        rope.Insert(rope.Begin(), TRope(testData1));
        rope.Insert(rope.Begin() + testData1.size(), TRope(testData2));
        rope.Insert(rope.Begin() + testData1.size() + testData2.size(), TRope(testData3));

        NActors::TRopeStream stream(rope.Begin(), rope.GetSize());
        
        TString dump = NActors::DumpRopeStreamData(&stream);
        
        TString expected = TString(
            "RopeStream dump (size: 16):\n"
            "00000000: 48 65 6c 6c 6f 20 57 6f 72 6c 64 21 01 02 ff fe \n"
        );
        
        UNIT_ASSERT_STRINGS_EQUAL(dump, expected);

        TRope emptyRope;
        NActors::TRopeStream emptyStream(emptyRope.Begin(), emptyRope.GetSize());
        TString emptyDump = NActors::DumpRopeStreamData(&emptyStream);
        UNIT_ASSERT_STRINGS_EQUAL(emptyDump, "RopeStream dump (size: 0):\n");

        TString exactData(16, 'A');
        TRope exactRope;
        exactRope.Insert(exactRope.Begin(), TRope(exactData));
        NActors::TRopeStream exactStream(exactRope.Begin(), exactRope.GetSize());
        TString exactDump = NActors::DumpRopeStreamData(&exactStream);
        TString exactExpected = TString(
            "RopeStream dump (size: 16):\n"
            "00000000: 41 41 41 41 41 41 41 41 41 41 41 41 41 41 41 41 \n"
        );
        UNIT_ASSERT_STRINGS_EQUAL(exactDump, exactExpected);

        TString longData = "This is a longer string that spans multiple lines!";
        TRope longRope;
        longRope.Insert(longRope.Begin(), TRope(longData));
        NActors::TRopeStream longStream(longRope.Begin(), longData.size());
        TString longDump = NActors::DumpRopeStreamData(&longStream);
        TString longExpected = TString(
            "RopeStream dump (size: 50):\n"
            "00000000: 54 68 69 73 20 69 73 20 61 20 6c 6f 6e 67 65 72 \n"
            "00000010: 20 73 74 72 69 6e 67 20 74 68 61 74 20 73 70 61 \n"
            "00000020: 6e 73 20 6d 75 6c 74 69 70 6c 65 20 6c 69 6e 65 \n"
            "00000030: 73 21 \n"
        );
        UNIT_ASSERT_STRINGS_EQUAL(longDump, longExpected);
    }
}
