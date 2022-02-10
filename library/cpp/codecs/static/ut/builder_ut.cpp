#include <library/cpp/testing/unittest/registar.h> 
#include <library/cpp/codecs/static/builder.h>
#include <library/cpp/codecs/static/static_codec_info.pb.h>
#include <util/string/vector.h>

class TStaticCodecInfoBuilderTest: public NUnitTest::TTestBase {
    UNIT_TEST_SUITE(TStaticCodecInfoBuilderTest)
    UNIT_TEST(TestBuild)
    UNIT_TEST_SUITE_END();

private:
    TVector<TString> PrepareData() {
        TVector<TString> data;
        for (ui32 i = 'a'; i <= 'z'; ++i) {
            data.push_back(TString(1, (char)i));
        }
        return data;
    }

    void TestBuild() {
        TVector<TString> data;
        NCodecs::TCodecBuildInfo info;
        info.CodecName = "huffman";
        info.SampleSizeMultiplier = 2;
        info.Timestamp = 1467494385;
        info.RevisionInfo = "r2385905";
        info.TrainingSetComment = "some dummy data";
        info.TrainingSetResId = "sbr://1234";
        auto res = NCodecs::BuildStaticCodec(PrepareData(), info);
        UNIT_ASSERT_VALUES_EQUAL(res.ShortUtf8DebugString(),
                                 "StoredCodec: \"\\007\\000huffman@S\\000a"
                                 "\\006b\\005c\\005d\\005e\\005f\\005g\\005h\\005i\\005j\\005k\\005l\\005m\\005n\\005o"
                                 "\\005p\\005q\\005r\\005s\\005t\\005u\\004v\\004w\\004x\\004y\\004z\\004\xC7?\xC8>"
                                 "\xC9=\xCA<\xCB;\xCC:\3159\3168\3177\3206\3215\3224\3233\3242\3251\3260\xD7/\xD8."
                                 "\xD9-\xDA,\xDB+\xDC*\xDD)\xDE(\xDF\\'\xE0&\xE1%\xE2$\xE3#\xE4\\\"\xE5!\xE6 \xE7"
                                 "\\037\xE8\\036\xE9\\035\xEA\\034\xEB\\033\xEC\\032\xED\\031\xEE\\030\xEF\\027\xF0"
                                 "\\026\xF1\\025\xF2\\024\xF3\\023\xF4\\022\xF5\\021\xF6\\020\xF7\\017\xF8\\016\xF9"
                                 "\\r\xFA\\014\xFB\\013\xFC\\n\xFD\\t\xFE\\010\xFF\\007\" "
                                 "DebugInfo { "
                                 "CodecName: \"huffman\" "
                                 "Timestamp: 1467494385 "
                                 "RevisionInfo: \"r2385905\" "
                                 "SampleSizeMultiplier: 2 "
                                 "TrainingSetComment: \"some dummy data\" "
                                 "TrainingSetResId: \"sbr://1234\" "
                                 "StoredCodecHash: 2509195835471488613 "
                                 "}");

        UNIT_ASSERT_VALUES_EQUAL(NCodecs::GetStandardFileName(res), "huffman.1467494385.codec_info");
        UNIT_ASSERT_VALUES_EQUAL(res.GetDebugInfo().GetStoredCodecHash(), 2509195835471488613ULL);

        auto res1 = NCodecs::LoadCodecInfoFromString(NCodecs::SaveCodecInfoToString(res));
        UNIT_ASSERT_VALUES_EQUAL(res1.ShortUtf8DebugString(), res.ShortUtf8DebugString());
    }
};

UNIT_TEST_SUITE_REGISTRATION(TStaticCodecInfoBuilderTest);
