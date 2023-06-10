#include <library/cpp/scheme/scimpl_private.h>
#include <library/cpp/scheme/ut_utils/scheme_ut_utils.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>
#include <util/string/subst.h>
#include <util/string/util.h>

#include <type_traits>
#include <library/cpp/string_utils/quote/quote.h>

using namespace std::string_view_literals;

Y_UNIT_TEST_SUITE(TSchemeJsonTest) {
    Y_UNIT_TEST(TestJson) {
        const char* json = "[\n"
                           "    {\n"
                           "        \"url\":\"foo\",\n"
                           "        \"title\":\"bar\",\n"
                           "        \"passages\":[\"foo\", \"bar\"],\n"
                           "    }\n"
                           "]";

        {
            const NSc::TValue& v = NSc::TValue::FromJson(json);
            UNIT_ASSERT_VALUES_EQUAL("bar", (TStringBuf)v.TrySelect("0/passages/1"));
            UNIT_ASSERT(v.PathExists("0/passages/0"));
            UNIT_ASSERT(v.PathExists("[0]/[passages]/[0]"));
            UNIT_ASSERT(v.PathExists("[0][passages][0]"));
            UNIT_ASSERT(v.PathExists(""));
            UNIT_ASSERT(!v.PathExists("`"));
            UNIT_ASSERT(v.TrySelect("").Has(0));
            UNIT_ASSERT(!v.PathExists("1"));
            UNIT_ASSERT(!v.PathExists("0/passages1"));
            UNIT_ASSERT(!v.PathExists("0/passages/2"));
            UNIT_ASSERT(!v.PathExists("0/passages/2"));
            UNIT_ASSERT_VALUES_EQUAL(0, (double)v.TrySelect("0/passages/2"));
            UNIT_ASSERT(!v.PathExists("0/passages/2"));
        }
        {
            const NSc::TValue& vv = NSc::TValue::FromJson("[ test ]]");
            UNIT_ASSERT(vv.IsNull());
        }
        {
            const char* json = "[a,b],[a,b]";
            const NSc::TValue& v = NSc::TValue::FromJson(json);
            UNIT_ASSERT(v.IsNull());
        }
        {
            const char* json = "[null,null]";
            const NSc::TValue& v = NSc::TValue::FromJson(json);
            UNIT_ASSERT(v.PathExists("1"));
            UNIT_ASSERT(!v.PathExists("2"));
        }
        {
            const char* json = "{ a : b : c }";
            NSc::TValue v;
            UNIT_ASSERT(!NSc::TValue::FromJson(v, json));
            UNIT_ASSERT(v.IsNull());
        }
        {
            const char* json = "[a:b]";
            UNIT_ASSERT(NSc::TValue::FromJson(json).IsNull());
        }
        {
            UNIT_ASSERT_VALUES_EQUAL("{\n    \"a\" : \"b\",\n    \"c\" : \"d\"\n}",
                                     NSc::TValue::FromJson("{a:b,c:d}").ToJson(NSc::TValue::JO_PRETTY));
        }
    }

    Y_UNIT_TEST(TestSafeJson) {
        TString ss;
        ss.reserve(256);

        for (int i = 0; i < 256; ++i) {
            ss.append((char)i);
        }

        NSc::TValue v;
        v[ss] = "xxx";
        v["xxx"] = ss;

        UNIT_ASSERT_VALUES_EQUAL("{\"xxx\":null}", v.ToJson(NSc::TValue::JO_SKIP_UNSAFE));
        UNIT_ASSERT_VALUES_EQUAL("{\"xxx\":null}", v.ToJson(NSc::TValue::JO_SAFE));

        UNIT_ASSERT_VALUES_EQUAL("{"
                                 "\"\\u0000\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006\\u0007\\b\\t\\n\\u000B\\f\\r"
                                 "\\u000E\\u000F\\u0010\\u0011\\u0012\\u0013\\u0014\\u0015\\u0016\\u0017\\u0018"
                                 "\\u0019\\u001A\\u001B\\u001C\\u001D\\u001E\\u001F !\\\"#$%&'()*+,-./0123456789"
                                 ":;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\\u007F"
                                 "\x80\x81\x82\x83\x84\x85\x86\x87\x88\x89\x8A\x8B\x8C\x8D\x8E\x8F\x90\x91\x92\x93"
                                 "\x94\x95\x96\x97\x98\x99\x9A\x9B\x9C\x9D\x9E\x9F\xA0\xA1\xA2\xA3\xA4\xA5\xA6\xA7"
                                 "\xA8\xA9\xAA\xAB\xAC\xAD\xAE\xAF\xB0\xB1\xB2\xB3\xB4\xB5\xB6\xB7\xB8\xB9\xBA\xBB"
                                 "\xBC\xBD\xBE\xBF\\xC0\\xC1\xC2\xC3\xC4\xC5\xC6\xC7\xC8\xC9\xCA\xCB\xCC\xCD\xCE"
                                 "\xCF\xD0\xD1\xD2\xD3\xD4\xD5\xD6\xD7\xD8\xD9\xDA\xDB\xDC\xDD\xDE\xDF\xE0\xE1"
                                 "\xE2\xE3\xE4\xE5\xE6\xE7\xE8\xE9\xEA\xEB\xEC\xED\xEE\xEF\xF0\xF1\xF2\xF3\xF4"
                                 "\\xF5\\xF6\\xF7\\xF8\\xF9\\xFA\\xFB\\xFC\\xFD\\xFE\\xFF\":"
                                 "\"xxx\","
                                 "\"xxx\":"
                                 "\"\\u0000\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006\\u0007\\b\\t\\n\\u000B\\f\\r"
                                 "\\u000E\\u000F\\u0010\\u0011\\u0012\\u0013\\u0014\\u0015\\u0016\\u0017\\u0018"
                                 "\\u0019\\u001A\\u001B\\u001C\\u001D\\u001E\\u001F !\\\"#$%&'()*+,-./0123456789"
                                 ":;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\\u007F"
                                 "\x80\x81\x82\x83\x84\x85\x86\x87\x88\x89\x8A\x8B\x8C\x8D\x8E\x8F\x90\x91\x92\x93"
                                 "\x94\x95\x96\x97\x98\x99\x9A\x9B\x9C\x9D\x9E\x9F\xA0\xA1\xA2\xA3\xA4\xA5\xA6\xA7"
                                 "\xA8\xA9\xAA\xAB\xAC\xAD\xAE\xAF\xB0\xB1\xB2\xB3\xB4\xB5\xB6\xB7\xB8\xB9\xBA\xBB"
                                 "\xBC\xBD\xBE\xBF\\xC0\\xC1\xC2\xC3\xC4\xC5\xC6\xC7\xC8\xC9\xCA\xCB\xCC\xCD\xCE"
                                 "\xCF\xD0\xD1\xD2\xD3\xD4\xD5\xD6\xD7\xD8\xD9\xDA\xDB\xDC\xDD\xDE\xDF\xE0\xE1"
                                 "\xE2\xE3\xE4\xE5\xE6\xE7\xE8\xE9\xEA\xEB\xEC\xED\xEE\xEF\xF0\xF1\xF2\xF3\xF4"
                                 "\\xF5\\xF6\\xF7\\xF8\\xF9\\xFA\\xFB\\xFC\\xFD\\xFE\\xFF\""
                                 "}",
                                 v.ToJson(NSc::TValue::JO_SORT_KEYS));
        UNIT_ASSERT(NSc::TValue::Equal(v, NSc::TValue::FromJson(v.ToJson())));

        {
            NSc::TValue value;
            TString articleName{"\xC2\xC2\xCF"};
            value["text"] = articleName;
            UNIT_ASSERT_VALUES_EQUAL(value.ToJson(), "{\"text\":\"\xC2\xC2\xCF\"}");
            UNIT_ASSERT_VALUES_EQUAL(value.ToJsonSafe(), "{\"text\":null}");
        }
    }

    Y_UNIT_TEST(TestJsonEscape) {
        NSc::TValue v("\10\7\6\5\4\3\2\1\0"sv);
        UNIT_ASSERT_VALUES_EQUAL(v.ToJson(), "\"\\b\\u0007\\u0006\\u0005\\u0004\\u0003\\u0002\\u0001\\u0000\"");
    }

    Y_UNIT_TEST(TestStrictJson) {
        UNIT_ASSERT_NO_EXCEPTION(NSc::TValue::FromJsonThrow("{a:b}"));
        UNIT_ASSERT_EXCEPTION(NSc::TValue::FromJsonThrow("{a:b}", NSc::TValue::JO_PARSER_STRICT), yexception);
        UNIT_ASSERT_NO_EXCEPTION(NSc::TValue::FromJsonThrow("{\"a\":\"b\"}", NSc::TValue::JO_PARSER_STRICT));
    }

    Y_UNIT_TEST(TestJsonValue) {
        NSc::TValue a = NSc::NUt::AssertFromJson("{a:[null,-1,2,3.4,str,{b:{c:d}}],e:f}");
        NSc::TValue b = NSc::TValue::FromJsonValue(a.ToJsonValue());
        UNIT_ASSERT_JSON_EQ_JSON(a, b);
    }

    Y_UNIT_TEST(TestJsonEmptyContainers) {
        {
            NSc::TValue a = NSc::NUt::AssertFromJson("{a:[]}");
            NSc::TValue b = NSc::TValue::FromJsonValue(a.ToJsonValue());
            UNIT_ASSERT_JSON_EQ_JSON(a, b);
        }
        {
            NSc::TValue a = NSc::NUt::AssertFromJson("{a:{}}");
            NSc::TValue b = NSc::TValue::FromJsonValue(a.ToJsonValue());
            UNIT_ASSERT_JSON_EQ_JSON(a, b);
        }
    }

    Y_UNIT_TEST(TestDuplicateKeys) {
        const TStringBuf duplicatedKeys = "{\"a\":[{\"b\":1, \"b\":42}]}";
        UNIT_ASSERT_NO_EXCEPTION(NSc::TValue::FromJsonThrow(duplicatedKeys));
        UNIT_ASSERT_EXCEPTION(NSc::TValue::FromJsonThrow(duplicatedKeys, NSc::TValue::JO_PARSER_DISALLOW_DUPLICATE_KEYS), yexception);
        UNIT_ASSERT(NSc::TValue::FromJson(duplicatedKeys).IsDict());
        UNIT_ASSERT(NSc::TValue::FromJson(duplicatedKeys, NSc::TValue::JO_PARSER_DISALLOW_DUPLICATE_KEYS).IsNull());
    }
}
