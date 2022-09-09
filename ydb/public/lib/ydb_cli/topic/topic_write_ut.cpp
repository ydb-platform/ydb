#include "topic_write.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NConsoleClient {
    class TTopicWriterTests: public NUnitTest::TTestBase {
        using TMessages = TVector<TTopicWriter::TSendMessageData>;

    public:
        UNIT_TEST_SUITE(TTopicWriterTests);
        UNIT_TEST(TestEnterMessage_EmptyInput);
        UNIT_TEST(TestEnterMessage_OnlyDelimiters);
        UNIT_TEST(TestEnterMessage_1KiB_No_Delimiter);
        UNIT_TEST(TestEnterMessage_1KiB_Newline_Delimiter);
        UNIT_TEST(TestEnterMessage_1KiB_Newline_Delimited_With_Two_Delimiters_In_A_Row);
        UNIT_TEST(TestEnterMessage_SomeBinaryData);
        UNIT_TEST(TestEnterMessage_ZeroSymbol_Delimited);
        UNIT_TEST(TestEnterMessage_Custom_Delimiter_Delimited);
        UNIT_TEST(TestEnterMessage_No_Base64_Transform);
        UNIT_TEST(TestEnterMessage_With_Base64_Transform_Invalid_Encode);
        UNIT_TEST(TestEnterMessage_With_Base64_Transform);
        UNIT_TEST(TestEnterMessage_With_Base64_Transform_NewlineDelimited);

        UNIT_TEST(TestTopicWriterParams_Format_NewlineDelimited);
        UNIT_TEST(TestTopicWriterParams_Format_Concatenated);
        UNIT_TEST(TestTopicWriterParams_No_Delimiter);
        UNIT_TEST(TestTopicWriterParams_InvalidDelimiter);
        UNIT_TEST_SUITE_END();

        void TestEnterMessage_EmptyInput() {
            TStringStream str;
            TTopicWriter wr;
            TMessages got = EnterMessageHelper(wr, str);
            AssertMessagesEqual({{
                                    .Data = "",
                                    .NeedSend = true,
                                    .ContinueSending = false,
                                }},
                                got);
        }

        void TestEnterMessage_OnlyDelimiters() {
            TStringStream str = TString("\n") * 6;
            TTopicWriter wr(nullptr, TTopicWriterParams(EMessagingFormat::SingleMessage, "\n", 0, Nothing(), Nothing(), Nothing(), ETransformBody::None));
            TMessages got = EnterMessageHelper(wr, str);
            AssertMessagesEqual({
                                    {
                                        .Data = "",
                                        .NeedSend = true,
                                        .ContinueSending = true,
                                    },
                                    {
                                        .Data = "",
                                        .NeedSend = true,
                                        .ContinueSending = true,
                                    },
                                    {
                                        .Data = "",
                                        .NeedSend = true,
                                        .ContinueSending = true,
                                    },
                                    {
                                        .Data = "",
                                        .NeedSend = true,
                                        .ContinueSending = true,
                                    },
                                    {
                                        .Data = "",
                                        .NeedSend = true,
                                        .ContinueSending = true,
                                    },
                                    {
                                        .Data = "",
                                        .NeedSend = true,
                                        .ContinueSending = true,
                                    },
                                    {
                                        .Data = "",
                                        .NeedSend = false,
                                        .ContinueSending = false,
                                    },
                                },
                                got);
        }

        void TestEnterMessage_1KiB_No_Delimiter() {
            /*
                Only one message with a * 1024 size expected
            */
            TStringStream str = TString("a") * 1_KB;
            TTopicWriter wr;
            TMessages got = EnterMessageHelper(wr, str);
            AssertMessagesEqual({
                                    {
                                        .Data = TString("a") * 1_KB,
                                        .NeedSend = true,
                                        .ContinueSending = false,
                                    },
                                },
                                got);
        }

        void TestEnterMessage_1KiB_Newline_Delimiter() {
            TStringStream str = TString("a") * 512 + "\n" + TString("b") * 512;
            TTopicWriter wr(nullptr, TTopicWriterParams(EMessagingFormat::SingleMessage, "\n", 0, Nothing(), Nothing(), Nothing(), ETransformBody::None));
            TMessages got = EnterMessageHelper(wr, str);
            AssertMessagesEqual({
                                    {
                                        .Data = TString("a") * 512,
                                        .NeedSend = true,
                                        .ContinueSending = true,
                                    },
                                    {
                                        .Data = TString("b") * 512,
                                        .NeedSend = true,
                                        .ContinueSending = true,
                                    },
                                    {
                                        .Data = "",
                                        .NeedSend = false,
                                        .ContinueSending = false,
                                    },
                                },
                                got);
        }

        void TestEnterMessage_1KiB_Newline_Delimited_With_Two_Delimiters_In_A_Row() {
            TStringStream str = TString("a") * 512 + "\n\n" + TString("b") * 512;
            TTopicWriter wr(nullptr, TTopicWriterParams(EMessagingFormat::NewlineDelimited, Nothing(), 0, Nothing(), Nothing(), Nothing(),
                                                        ETransformBody::None));
            TMessages got = EnterMessageHelper(wr, str);
            AssertMessagesEqual({
                                    {
                                        .Data = TString("a") * 512,
                                        .NeedSend = true,
                                        .ContinueSending = true,
                                    },
                                    {
                                        .Data = "",
                                        .NeedSend = true,
                                        .ContinueSending = true,
                                    },
                                    {
                                        .Data = TString("b") * 512,
                                        .NeedSend = true,
                                        .ContinueSending = true,
                                    },
                                    {
                                        .Data = "",
                                        .NeedSend = false,
                                        .ContinueSending = false,
                                    },
                                },
                                got);
        }

        void TestEnterMessage_SomeBinaryData() {
            TStringStream str = TString("\0\0\n\n\r\n\r\n");
            TTopicWriter wr;
            TMessages got = EnterMessageHelper(wr, str);
            AssertMessagesEqual({
                                    {
                                        .Data = TString("\0\0\n\n\r\n\r\n"),
                                        .NeedSend = true,
                                        .ContinueSending = false,
                                    },
                                },
                                got);
        }

        void TestEnterMessage_ZeroSymbol_Delimited() {
            auto& s = "\0\0\0\0\n\nprivet";
            TStringStream str = TString(std::begin(s), std::end(s));
            TTopicWriter wr(nullptr, TTopicWriterParams(EMessagingFormat::SingleMessage, "\0", 0, Nothing(), Nothing(), Nothing(), ETransformBody::None));
            TMessages got = EnterMessageHelper(wr, str);
            AssertMessagesEqual({
                                    {
                                        .Data = "",
                                        .NeedSend = true,
                                        .ContinueSending = true,
                                    },
                                    {
                                        .Data = "",
                                        .NeedSend = true,
                                        .ContinueSending = true,
                                    },
                                    {
                                        .Data = "",
                                        .NeedSend = true,
                                        .ContinueSending = true,
                                    },
                                    {
                                        .Data = "",
                                        .NeedSend = true,
                                        .ContinueSending = true,
                                    },
                                    {
                                        .Data = "\n\nprivet",
                                        .NeedSend = true,
                                        .ContinueSending = true,
                                    },
                                    {
                                        .Data = "",
                                        .NeedSend = false,
                                        .ContinueSending = false,
                                    },
                                },
                                got);
        }

        void TestEnterMessage_Custom_Delimiter_Delimited() {
            TStringStream str = TString("privet_vasya_kak_dela?");
            TTopicWriter wr(nullptr, TTopicWriterParams(EMessagingFormat::SingleMessage, "_", 0, Nothing(), Nothing(), Nothing(), ETransformBody::None));
            TMessages got = EnterMessageHelper(wr, str);
            AssertMessagesEqual({
                                    {
                                        .Data = "privet",
                                        .NeedSend = true,
                                        .ContinueSending = true,
                                    },
                                    {
                                        .Data = "vasya",
                                        .NeedSend = true,
                                        .ContinueSending = true,
                                    },
                                    {
                                        .Data = "kak",
                                        .NeedSend = true,
                                        .ContinueSending = true,
                                    },
                                    {
                                        .Data = "dela?",
                                        .NeedSend = true,
                                        .ContinueSending = true,
                                    },
                                    {
                                        .Data = "",
                                        .NeedSend = false,
                                        .ContinueSending = false,
                                    },
                                },
                                got);
        }

        void TestEnterMessage_No_Base64_Transform() {
            TStringStream str = TString("privet_vasya_kak_dela?");
            TTopicWriter wr(nullptr, TTopicWriterParams(EMessagingFormat::SingleMessage, Nothing(), 0, Nothing(), Nothing(), Nothing(), ETransformBody::None));
            TMessages got = EnterMessageHelper(wr, str);

            AssertMessagesEqual({
                {
                    .Data = "privet_vasya_kak_dela?",
                    .NeedSend = true,
                    .ContinueSending = false,
                },
            }, got);
        }

        void TestEnterMessage_With_Base64_Transform_Invalid_Encode() {
            TStringStream str = TString("cHJpdmV0X3Zhc3lhX2tha19kZWxPw==");
            TTopicWriter wr(nullptr, TTopicWriterParams(EMessagingFormat::SingleMessage, Nothing(), 0, Nothing(), Nothing(), Nothing(), ETransformBody::Base64));
            UNIT_ASSERT_EXCEPTION(EnterMessageHelper(wr, str), yexception);
        }

        void TestEnterMessage_With_Base64_Transform() {
            TStringStream str = TString("cHJpdmV0X3Zhc3lhX2tha19kZWxhPw==");
            TTopicWriter wr(nullptr, TTopicWriterParams(EMessagingFormat::SingleMessage, Nothing(), 0, Nothing(), Nothing(), Nothing(), ETransformBody::Base64));
            TMessages got = EnterMessageHelper(wr, str);

            AssertMessagesEqual(
                {
                    {
                        .Data = "privet_vasya_kak_dela?",
                        .NeedSend = true,
                        .ContinueSending = false,
                    },
                },
                got);
        }

        void TestEnterMessage_With_Base64_Transform_NewlineDelimited() {
            TStringStream str = TString("aG93IGRvIHlvdSBkbw==\ncHJpdmV0X3Zhc3lhX2tha19kZWxhPw==");
            TTopicWriter wr(nullptr, TTopicWriterParams(EMessagingFormat::NewlineDelimited, Nothing(), 0, Nothing(), Nothing(), Nothing(), ETransformBody::Base64));
            TMessages got = EnterMessageHelper(wr, str);

            AssertMessagesEqual(
                {
                    {
                        .Data = "how do you do",
                        .NeedSend = true,
                        .ContinueSending = true,
                    },
                    {
                        .Data = "privet_vasya_kak_dela?",
                        .NeedSend = true,
                        .ContinueSending = true,
                    },
                    {
                        .Data = "",
                        .NeedSend = false,
                        .ContinueSending = false,
                    },
                },
                got);
        }

        void TestTopicWriterParams_Format_NewlineDelimited() {
            TTopicWriterParams p = TTopicWriterParams(EMessagingFormat::NewlineDelimited, Nothing(), 0, Nothing(), Nothing(), Nothing(), ETransformBody::None);
            UNIT_ASSERT_VALUES_EQUAL(p.MessagingFormat(), EMessagingFormat::NewlineDelimited);
            UNIT_ASSERT_VALUES_EQUAL(p.Delimiter(), '\n');
        }

        void TestTopicWriterParams_Format_Concatenated() {
            TTopicWriterParams p = TTopicWriterParams(EMessagingFormat::Concatenated, Nothing(), 0, Nothing(), Nothing(), Nothing(), ETransformBody::None);
            UNIT_ASSERT_VALUES_EQUAL(p.MessagingFormat(), EMessagingFormat::Concatenated);
            UNIT_ASSERT_VALUES_EQUAL(p.Delimiter(), '\n');
        }

        void TestTopicWriterParams_No_Delimiter() {
            TTopicWriterParams p = TTopicWriterParams(EMessagingFormat::SingleMessage, Nothing(), 0, Nothing(), Nothing(), Nothing(), ETransformBody::None);
            UNIT_ASSERT_VALUES_EQUAL(p.MessagingFormat(), EMessagingFormat::SingleMessage);
            UNIT_ASSERT_VALUES_EQUAL(p.Delimiter(), Nothing());
        }

        void TestTopicWriterParams_InvalidDelimiter() {
            UNIT_ASSERT_EXCEPTION(TTopicWriterParams(EMessagingFormat::SingleMessage, "invalid", 0, Nothing(), Nothing(), Nothing(), ETransformBody::None), yexception);
        }

    private:
        TMessages EnterMessageHelper(TTopicWriter& wr, IInputStream& input) {
            TMessages result;
            while (true) {
                TTopicWriter::TSendMessageData message = wr.EnterMessage(input);
                result.emplace_back(message);
                if (result.back().ContinueSending == false) {
                    break;
                }
            }
            return result;
        }

        void AssertMessagesEqual(TMessages want, TMessages& got) {
            UNIT_ASSERT_VALUES_EQUAL(want.size(), got.size());
            for (size_t i = 0; i < want.size(); ++i) {
                TTopicWriter::TSendMessageData w = want[i];
                TTopicWriter::TSendMessageData g = got[i];
                UNIT_ASSERT_VALUES_EQUAL(g.ContinueSending, w.ContinueSending);
                UNIT_ASSERT_VALUES_EQUAL(g.Data, w.Data);
                UNIT_ASSERT_VALUES_EQUAL(g.NeedSend, w.NeedSend);
            }
        }
    };

    UNIT_TEST_SUITE_REGISTRATION(TTopicWriterTests);
} // namespace NYdb::NConsoleClient
