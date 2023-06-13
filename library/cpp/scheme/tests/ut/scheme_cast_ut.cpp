#include <library/cpp/scheme/scheme.h>
#include <library/cpp/scheme/scheme_cast.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>
#include <util/string/subst.h>
#include <util/string/util.h>

using namespace NJsonConverters;

using TVI = TVector<int>;
using THI = THashMap<int, int>;
using TMI = TMap<int, int>;
using THSI = THashSet<int>;
using TSI = TSet<int>;
using TPI = std::pair<int, int>;

Y_UNIT_TEST_SUITE(TSchemeCastTest) {
    Y_UNIT_TEST(TestYVector) {
        TVI v;
        for (int i = 0; i < 3; ++i)
            v.push_back(i);

        UNIT_ASSERT_VALUES_EQUAL("[0,1,2]", ToJson(v));

        TVI y(FromJson<TVI>(ToJson(v)));
        UNIT_ASSERT_VALUES_EQUAL(v.size(), y.size());
        UNIT_ASSERT(std::equal(v.begin(), v.end(), y.begin()));
    }

    Y_UNIT_TEST(TestYHash) {
        THI h;
        for (int i = 0; i < 3; ++i)
            h[i] = i * i;

        const TString etalon = "{\"0\":0,\"1\":1,\"2\":4}";
        UNIT_ASSERT_VALUES_EQUAL(etalon, ToJson(h, true));

        THI h2(FromJson<THI>(ToJson(h)));
        UNIT_ASSERT_VALUES_EQUAL(ToJson(h2, true), ToJson(h, true));
    }

    Y_UNIT_TEST(TestYMap) {
        TMI h;
        for (int i = 0; i < 3; ++i)
            h[i] = i * i;

        const TString etalon = "{\"0\":0,\"1\":1,\"2\":4}";
        UNIT_ASSERT_VALUES_EQUAL(etalon, ToJson(h, true));

        TMI h2(FromJson<TMI>(ToJson(h)));
        UNIT_ASSERT_VALUES_EQUAL(ToJson(h2, true), ToJson(h, true));
    }

    Y_UNIT_TEST(TestYHashSet) {
        THSI h;
        for (int i = 0; i < 3; ++i)
            h.insert(i * i);

        const TString etalon = "{\"0\":null,\"1\":null,\"4\":null}";
        UNIT_ASSERT_VALUES_EQUAL(etalon, ToJson(h, true));

        THSI h2(FromJson<THSI>(ToJson(h)));
        UNIT_ASSERT_VALUES_EQUAL(ToJson(h2, true), ToJson(h, true));
    }

    Y_UNIT_TEST(TestYSet) {
        TSI h;
        for (int i = 0; i < 3; ++i)
            h.insert(i * i);

        const TString etalon = "{\"0\":null,\"1\":null,\"4\":null}";
        UNIT_ASSERT_VALUES_EQUAL(etalon, ToJson(h, true));

        TSI h2(FromJson<TSI>(ToJson(h)));
        UNIT_ASSERT_VALUES_EQUAL(ToJson(h2, true), ToJson(h, true));
    }

    Y_UNIT_TEST(TestTPair) {
        TPI p(1, 1);

        const TString etalon = "[1,1]";
        UNIT_ASSERT_VALUES_EQUAL(etalon, ToJson(p, true));

        TPI p2(FromJson<TPI>(ToJson(p)));
        UNIT_ASSERT_VALUES_EQUAL(ToJson(p2, true), ToJson(p, true));
    }

    struct TCustom: public IJsonSerializable {
        int A, B;
        TCustom()
            : A(0)
            , B(0){}
        TCustom(int a, int b)
            : A(a)
            , B(b)
        {
        }
        NSc::TValue ToTValue() const override {
            NSc::TValue res;
            res["a"] = A;
            res["b"] = B;
            return res;
        }
        void FromTValue(const NSc::TValue& v, const bool) override {
            A = v["a"].GetNumber();
            B = v["b"].GetNumber();
        }

        bool operator<(const TCustom& rhs) const {
            return A < rhs.A;
        }
    };

    Y_UNIT_TEST(TestTCustom) {
        TCustom x(2, 3);

        const TString etalon = "{\"a\":2,\"b\":3}";
        UNIT_ASSERT_VALUES_EQUAL(etalon, ToJson(x, true));

        TCustom x2(FromJson<TCustom>(ToJson(x)));
        UNIT_ASSERT_VALUES_EQUAL(ToJson(x2, true), ToJson(x, true));
    }

    Y_UNIT_TEST(TestVectorOfPairs) {
        typedef TVector<TPI> TVPI;
        TVPI v;

        for (int i = 0; i < 3; ++i)
            v.push_back(TPI(i, i * i));

        const TString etalon = "[[0,0],[1,1],[2,4]]";
        UNIT_ASSERT_VALUES_EQUAL(etalon, ToJson(v, true));

        TVPI v2(FromJson<TVPI>(ToJson(v)));
        UNIT_ASSERT_VALUES_EQUAL(ToJson(v2, true), ToJson(v, true));
    }

    Y_UNIT_TEST(TestSetOfCustom) {
        typedef TSet<TCustom> TSC;
        TSC s;
        s.insert(TCustom(2, 3));

        const TString etalon = "{\"{\\\"a\\\":2,\\\"b\\\":3}\":null}";
        UNIT_ASSERT_VALUES_EQUAL(etalon, ToJson(s, true));

        TSC s2(FromJson<TSC>(ToJson(s)));
        UNIT_ASSERT_VALUES_EQUAL(ToJson(s2, true), ToJson(s, true));
    }

    Y_UNIT_TEST(TestExceptions) {
        NSc::TValue v = 1;
        const TString json = v.ToJson();
        UNIT_ASSERT_EXCEPTION(FromJson<TVI>(json, true), yexception);
        UNIT_ASSERT_EXCEPTION(FromJson<THI>(json, true), yexception);
        UNIT_ASSERT_EXCEPTION(FromJson<TMI>(json, true), yexception);
        UNIT_ASSERT_EXCEPTION(FromJson<THSI>(json, true), yexception);
        UNIT_ASSERT_EXCEPTION(FromJson<TSI>(json, true), yexception);
        UNIT_ASSERT_EXCEPTION(FromJson<TPI>(json, true), yexception);
    }
}
