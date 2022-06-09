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
                                 "\\005p\\005q\\005r\\005s\\005t\\005u\\004v\\004w\\004x\\004y\\004z\\004\\307?\\310>"
                                 "\\311=\\312<\\313;\\314:\\3159\\3168\\3177\\3206\\3215\\3224\\3233\\3242\\3251\\3260\\327/\\330."
                                 "\\331-\\332,\\333+\\334*\\335)\\336(\\337\\'\\340&\\341%\\342$\\343#\\344\\\"\\345!\\346 \\347"
                                 "\\037\\350\\036\\351\\035\\352\\034\\353\\033\\354\\032\\355\\031\\356\\030\\357\\027\\360"
                                 "\\026\\361\\025\\362\\024\\363\\023\\364\\022\\365\\021\\366\\020\\367\\017\\370\\016\\371"
                                 "\\r\\372\\014\\373\\013\\374\\n\\375\\t\\376\\010\\377\\007\" "
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
