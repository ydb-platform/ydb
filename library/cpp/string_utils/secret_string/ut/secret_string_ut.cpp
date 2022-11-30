#include <library/cpp/string_utils/secret_string/secret_string.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NSecretString;

Y_UNIT_TEST_SUITE(SecretTest) {
    Y_UNIT_TEST(Common) {
        TSecretString s;
        UNIT_ASSERT_VALUES_EQUAL("", s.Value());
        UNIT_ASSERT_VALUES_EQUAL("", (TStringBuf)s);

        TSecretString s2("qwerty");
        UNIT_ASSERT_VALUES_EQUAL("qwerty", s2.Value());
        UNIT_ASSERT_VALUES_EQUAL("qwerty", (TStringBuf)s2);
    }

    Y_UNIT_TEST(CopyCtor1) {
        TSecretString s1("qwerty");

        UNIT_ASSERT_VALUES_EQUAL("qwerty", s1.Value());

        {
            TSecretString s2(s1);
            UNIT_ASSERT_VALUES_EQUAL("qwerty", s1.Value());
            UNIT_ASSERT_VALUES_EQUAL("qwerty", s2.Value());
        }

        UNIT_ASSERT_VALUES_EQUAL("qwerty", s1.Value());
    }

    Y_UNIT_TEST(CopyCtor2) {
        auto s1 = MakeHolder<TSecretString>("qwerty");
        UNIT_ASSERT_VALUES_EQUAL("qwerty", s1->Value());

        TSecretString s2(*s1);
        UNIT_ASSERT_VALUES_EQUAL("qwerty", s1->Value());
        UNIT_ASSERT_VALUES_EQUAL("qwerty", s2.Value());

        s1.Reset();
        UNIT_ASSERT_VALUES_EQUAL("qwerty", s2.Value());
    }

    Y_UNIT_TEST(MoveCtor1) {
        TSecretString s1("qwerty");

        UNIT_ASSERT_VALUES_EQUAL("qwerty", s1.Value());

        {
            TSecretString s2(std::move(s1));
            UNIT_ASSERT_VALUES_EQUAL("", s1.Value());
            UNIT_ASSERT_VALUES_EQUAL("qwerty", s2.Value());
        }

        UNIT_ASSERT_VALUES_EQUAL("", s1.Value());
    }

    Y_UNIT_TEST(MoveCtor2) {
        auto s1 = MakeHolder<TSecretString>("qwerty");
        UNIT_ASSERT_VALUES_EQUAL("qwerty", s1->Value());

        TSecretString s2(std::move(*s1));
        UNIT_ASSERT_VALUES_EQUAL("", s1->Value());
        UNIT_ASSERT_VALUES_EQUAL("qwerty", s2.Value());

        s1.Reset();
        UNIT_ASSERT_VALUES_EQUAL("qwerty", s2.Value());
    }

    Y_UNIT_TEST(CopyAssignment1) {
        TSecretString s1("qwerty");

        UNIT_ASSERT_VALUES_EQUAL("qwerty", s1.Value());

        {
            TSecretString s2;
            UNIT_ASSERT_VALUES_EQUAL("", s2.Value());

            s2 = s1;
            UNIT_ASSERT_VALUES_EQUAL("qwerty", s1.Value());
            UNIT_ASSERT_VALUES_EQUAL("qwerty", s2.Value());
        }

        UNIT_ASSERT_VALUES_EQUAL("qwerty", s1.Value());
    }

    Y_UNIT_TEST(CopyAssignment2) {
        auto s1 = MakeHolder<TSecretString>("qwerty");
        UNIT_ASSERT_VALUES_EQUAL("qwerty", s1->Value());

        TSecretString s2;
        UNIT_ASSERT_VALUES_EQUAL("", s2.Value());

        s2 = *s1;
        UNIT_ASSERT_VALUES_EQUAL("qwerty", s1->Value());
        UNIT_ASSERT_VALUES_EQUAL("qwerty", s2.Value());

        s1.Reset();
        UNIT_ASSERT_VALUES_EQUAL("qwerty", s2.Value());

        TSecretString s3;
        s2 = s3;
        UNIT_ASSERT_VALUES_EQUAL("", s2.Value());
    }

    Y_UNIT_TEST(MoveAssignment1) {
        TSecretString s1("qwerty");

        UNIT_ASSERT_VALUES_EQUAL("qwerty", s1.Value());

        {
            TSecretString s2;
            UNIT_ASSERT_VALUES_EQUAL("", s2.Value());

            s2 = std::move(s1);
            UNIT_ASSERT_VALUES_EQUAL("", s1.Value());
            UNIT_ASSERT_VALUES_EQUAL("qwerty", s2.Value());
        }

        UNIT_ASSERT_VALUES_EQUAL("", s1.Value());
    }

    Y_UNIT_TEST(MoveAssignment2) {
        auto s1 = MakeHolder<TSecretString>("qwerty");
        UNIT_ASSERT_VALUES_EQUAL("qwerty", s1->Value());

        TSecretString s2;
        UNIT_ASSERT_VALUES_EQUAL("", s2.Value());

        s2 = std::move(*s1);
        UNIT_ASSERT_VALUES_EQUAL("", s1->Value());
        UNIT_ASSERT_VALUES_EQUAL("qwerty", s2.Value());

        s1.Reset();
        UNIT_ASSERT_VALUES_EQUAL("qwerty", s2.Value());

        TSecretString s3;
        s2 = std::move(s3);
        UNIT_ASSERT_VALUES_EQUAL("", s2.Value());
    }

    Y_UNIT_TEST(ZeroTerminated) {
        TSecretString s("qwerty");

        UNIT_ASSERT_VALUES_EQUAL(s.Value().size(), strlen(s.Value().data()));
    }
}
