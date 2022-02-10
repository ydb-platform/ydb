#include <ydb/core/ymq/base/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSQS {

Y_UNIT_TEST_SUITE(StringValidationTest) {
    Y_UNIT_TEST(IsAlphaNumAndPunctuationTest) {
        UNIT_ASSERT(IsAlphaNumAndPunctuation("123"));
        UNIT_ASSERT(IsAlphaNumAndPunctuation(""));
        UNIT_ASSERT(IsAlphaNumAndPunctuation("[abd]"));
        UNIT_ASSERT(IsAlphaNumAndPunctuation("ABC-ZYX"));
        UNIT_ASSERT(IsAlphaNumAndPunctuation("{_<=>_}"));
        UNIT_ASSERT(IsAlphaNumAndPunctuation("**"));
        UNIT_ASSERT(IsAlphaNumAndPunctuation("\"\\/~"));
        UNIT_ASSERT(IsAlphaNumAndPunctuation("(!)"));
        UNIT_ASSERT(IsAlphaNumAndPunctuation(":-)"));


        UNIT_ASSERT(!IsAlphaNumAndPunctuation(" "));
        UNIT_ASSERT(!IsAlphaNumAndPunctuation(TStringBuf("\0", 1)));
        UNIT_ASSERT(!IsAlphaNumAndPunctuation("\t\n"));
        UNIT_ASSERT(!IsAlphaNumAndPunctuation("айди"));
        UNIT_ASSERT(!IsAlphaNumAndPunctuation("§"));
    }
}

Y_UNIT_TEST_SUITE(MessageAttributeValidationTest) {
    Y_UNIT_TEST(MessageAttributeValidationTest) {
        auto AssertValidAttrImpl = [](TStringBuf name, bool valid, bool allowYandexPrefix = false, bool hasYandexPrefix = false) {
            bool yandexPrefixFound = false;
            UNIT_ASSERT_VALUES_EQUAL_C(ValidateMessageAttributeName(name, yandexPrefixFound, allowYandexPrefix), valid,
                "Attribute name: \"" << name << "\", AllowYandexPrefix: " << allowYandexPrefix << ", YandexPrefixFound: " << yandexPrefixFound);
            UNIT_ASSERT_VALUES_EQUAL_C(hasYandexPrefix, yandexPrefixFound,
                "Attribute name: \"" << name << "\", AllowYandexPrefix: " << allowYandexPrefix << ", YandexPrefixFound: " << yandexPrefixFound);
        };

        auto AssertValidAttr = [&](TStringBuf name, bool allowYandexPrefix = false, bool hasYandexPrefix = false) {
            AssertValidAttrImpl(name, true, allowYandexPrefix, hasYandexPrefix);
        };

        auto AssertInvalidAttr = [&](TStringBuf name, bool allowYandexPrefix = false, bool hasYandexPrefix = false) {
            AssertValidAttrImpl(name, false, allowYandexPrefix, hasYandexPrefix);
        };

        AssertInvalidAttr("");
        AssertInvalidAttr("TooManyCharacters__skdhfkjhsfjdhfkshfkjsdhkfhsdkfhkshfkhskfhdskjfhsdkfhksdhfkjdshfksdhfgjhsdgf1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111122222222222222222222222222222222222222222222222222233333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333");
        AssertInvalidAttr("aWs.trololo");
        AssertInvalidAttr("aws.");
        AssertInvalidAttr("amazon.");
        AssertInvalidAttr("amazon.33");
        AssertInvalidAttr("invalid_characters!");
        AssertInvalidAttr("space ");
        AssertInvalidAttr(".StartsWithPeriod");
        AssertInvalidAttr("EndsWithPeriod.");
        AssertInvalidAttr("Two..Periods");

        // Yandex reserved prefixes:
        AssertInvalidAttr("ya.reserved", false, true);
        AssertInvalidAttr("YC.reserved", false, true);
        AssertInvalidAttr("YANDEX.reserved", false, true);

        AssertValidAttr("not.prefix.ya", false, false);
        AssertValidAttr("ya.reserved", true, true);
        AssertValidAttr("YC.reserved", true, true);
        AssertValidAttr("Yandex.reserved", true, true);

        // Valid
        AssertValidAttr("OK");
        AssertValidAttr("Name");
        AssertValidAttr("name");
        AssertValidAttr("alpha-num");
        AssertValidAttr("with.period");
        AssertValidAttr("with_underscore");
    }
}

Y_UNIT_TEST_SUITE(NameValidationTest) {
    Y_UNIT_TEST(NameValidationTest) {
        UNIT_ASSERT(!ValidateQueueNameOrUserName(""));
        UNIT_ASSERT(!ValidateQueueNameOrUserName(".fifo"));
        UNIT_ASSERT(!ValidateQueueNameOrUserName("TooManyCharacters__skdhfkjhsfjdhfkshfkjsdhkfhsdkfhkshfkhskfhdskjfhsdkfhksdhfkjdshfksdhfgjhsdgf1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111122222222222222222222222222222222222222222222222222233333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333"));
        UNIT_ASSERT(!ValidateQueueNameOrUserName("point."));
        UNIT_ASSERT(!ValidateQueueNameOrUserName("space "));
        UNIT_ASSERT(!ValidateQueueNameOrUserName("русские буквы"));
        UNIT_ASSERT(!ValidateQueueNameOrUserName("*"));
        UNIT_ASSERT(!ValidateQueueNameOrUserName("!"));
        UNIT_ASSERT(!ValidateQueueNameOrUserName("/"));


        UNIT_ASSERT(ValidateQueueNameOrUserName("OK"));
        UNIT_ASSERT(ValidateQueueNameOrUserName("Name.fifo"));
        UNIT_ASSERT(ValidateQueueNameOrUserName("name.FIFO"));
        UNIT_ASSERT(ValidateQueueNameOrUserName("name"));
        UNIT_ASSERT(ValidateQueueNameOrUserName("alpha-num"));
        UNIT_ASSERT(ValidateQueueNameOrUserName("0123"));
        UNIT_ASSERT(ValidateQueueNameOrUserName("with_underscore"));
    }
}

Y_UNIT_TEST_SUITE(MessageBodyValidationTest) {
    Y_UNIT_TEST(MessageBodyValidationTest) {
        TString desc;
        UNIT_ASSERT(ValidateMessageBody("english text.", desc));
        UNIT_ASSERT(ValidateMessageBody("русский текст.", desc));
        UNIT_ASSERT(ValidateMessageBody("\n", desc));
        UNIT_ASSERT(ValidateMessageBody("\t", desc));
        UNIT_ASSERT(ValidateMessageBody("Δx", desc));
        UNIT_ASSERT(ValidateMessageBody(":)", desc));
        UNIT_ASSERT(ValidateMessageBody("\U00010000", desc));
        UNIT_ASSERT(ValidateMessageBody("\uE000", desc));
        UNIT_ASSERT(ValidateMessageBody("\uFFFD", desc));
        UNIT_ASSERT(ValidateMessageBody("\uD7FF", desc));
        UNIT_ASSERT(ValidateMessageBody("\u00FF", desc));

        UNIT_ASSERT(!ValidateMessageBody(TStringBuf("\0", 1), desc));
        UNIT_ASSERT(!ValidateMessageBody("\u0002", desc));
        UNIT_ASSERT(!ValidateMessageBody("\u0019", desc));
        UNIT_ASSERT(!ValidateMessageBody("\uFFFF", desc));
    }
}

} // namespace NKikimr::NSQS
