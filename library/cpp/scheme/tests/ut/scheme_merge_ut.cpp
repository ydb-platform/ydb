#include <library/cpp/scheme/scimpl_private.h>
#include <library/cpp/scheme/ut_utils/scheme_ut_utils.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>
#include <library/cpp/string_utils/quote/quote.h>
#include <util/string/subst.h>
#include <util/string/util.h>

#include <type_traits>


Y_UNIT_TEST_SUITE(TSchemeMergeTest) {

    void DoTestReverseMerge(TStringBuf lhs, TStringBuf rhs, TStringBuf res) {
        NSc::TValue v = NSc::TValue::FromJson(lhs);
        v.ReverseMerge(NSc::TValue::FromJson(rhs));
        UNIT_ASSERT(NSc::TValue::Equal(v, NSc::TValue::FromJson(res)));
    }

    Y_UNIT_TEST(TestReverseMerge) {
        DoTestReverseMerge("{a:{x:y, b:c}}", "{a:{u:w, b:d}}", "{a:{u:w, x:y, b:c}}");
        DoTestReverseMerge("null", "{x:y}", "{x:y}");
        DoTestReverseMerge("null", "[b]", "[b]");
        DoTestReverseMerge("[a]", "[b]", "[a]");
        DoTestReverseMerge("{x:null}", "{x:b}", "{x:b}");
    }

    Y_UNIT_TEST(TestMerge) {
        TStringBuf data = "{ a : [ { b : 1, d : { e : -1.e5 } }, { f : 0, g : [ h, i ] } ] }";
        NSc::TValue v = NSc::TValue::FromJson(data);
        UNIT_ASSERT_VALUES_EQUAL(v.ToJson(true), v.Clone().ToJson(true));
        UNIT_ASSERT(v.Has("a"));
        UNIT_ASSERT(v["a"].Has(1));
        UNIT_ASSERT(v["a"][0].Has("b"));
        UNIT_ASSERT(v["a"][0].Has("d"));
        UNIT_ASSERT(1 == v["a"][0]["b"]);
        UNIT_ASSERT(v["a"][0]["d"].Has("e"));
        UNIT_ASSERT(-1.e5 == v["a"][0]["d"]["e"]);
        UNIT_ASSERT(v["a"][1].Has("f"));
        UNIT_ASSERT(v["a"][1].Has("g"));
        UNIT_ASSERT(0. == v["a"][1]["f"]);
        UNIT_ASSERT(v["a"][1]["g"].IsArray());
        UNIT_ASSERT(v["a"][1]["g"].Has(1));
        UNIT_ASSERT(TStringBuf("h") == v["a"][1]["g"][0]);
        UNIT_ASSERT(TStringBuf("i") == v["a"][1]["g"][1]);

        {
            TStringBuf data = "{ a : [ { d : 42 }, { g : [ 3 ] } ], q : r }";

            NSc::TValue v1 = NSc::TValue::FromJson(data);
            UNIT_ASSERT_VALUES_EQUAL(v1.ToJson(true), v1.Clone().ToJson(true));
            UNIT_ASSERT(NSc::TValue::Equal(v1, v1.FromJson(v1.ToJson())));

            NSc::TValue v2;
            v2.MergeUpdate(v["a"]);
            UNIT_ASSERT_C(NSc::TValue::Equal(v["a"], v2), Sprintf("\n%s\n!=\n%s\n", v["a"].ToJson().data(), v2.ToJson().data()));

            v.MergeUpdate(v1);
            UNIT_ASSERT_C(!NSc::TValue::Equal(v["a"], v2), Sprintf("\n%s\n!=\n%s\n", v["a"].ToJson().data(), v2.ToJson().data()));
            v2.MergeUpdate(v1["a"]);
            UNIT_ASSERT_C(NSc::TValue::Equal(v["a"], v2), Sprintf("\n%s\n!=\n%s\n", v["a"].ToJson().data(), v2.ToJson().data()));
        }

        UNIT_ASSERT(v.Has("a"));
        UNIT_ASSERT(v.Has("q"));
        UNIT_ASSERT(TStringBuf("r") == v["q"]);
        UNIT_ASSERT(v["a"].Has(1));
        UNIT_ASSERT(!v["a"][0].Has("b"));
        UNIT_ASSERT(v["a"][0].Has("d"));
        UNIT_ASSERT(!v["a"][0]["d"].IsArray());
        UNIT_ASSERT(!v["a"][0]["d"].IsDict());
        UNIT_ASSERT(42 == v["a"][0]["d"]);
        UNIT_ASSERT(!v["a"][1].Has("f"));
        UNIT_ASSERT(v["a"][1].Has("g"));
        UNIT_ASSERT(v["a"][1]["g"].IsArray());
        UNIT_ASSERT(!v["a"][1]["g"].Has(1));
        UNIT_ASSERT(3 == v["a"][1]["g"][0]);
    }

    Y_UNIT_TEST(TestMerge1) {
        TStringBuf data = "[ { a : { b : d } } ]";

        NSc::TValue wcopy = NSc::TValue::FromJson(data);

        TStringBuf data1 = "[ { a : { b : c } } ]";

        wcopy.MergeUpdateJson(data1);

        {
            TString json = wcopy.ToJson(true);
            SubstGlobal(json, "\"", "");
            UNIT_ASSERT_VALUES_EQUAL(json, "[{a:{b:c}}]");
        }
    }

    Y_UNIT_TEST(TestMerge2) {
        TStringBuf data = "{ a : { b : c }, q : { x : y } }";

        NSc::TValue wcopy = NSc::TValue::FromJson(data);

        TStringBuf data1 = "{ a : { e : f } }";

        wcopy.MergeUpdateJson(data1);

        {
            TString json = wcopy.ToJson(true);
            SubstGlobal(json, "\"", "");
            UNIT_ASSERT_VALUES_EQUAL(json, "{a:{b:c,e:f},q:{x:y}}");
        }
    }

    Y_UNIT_TEST(TestMerge3) {
        TStringBuf data = "{ g : { x : { a : { b : c }, q : { x : y } }, y : fff } }";

        NSc::TValue wcopy = NSc::TValue::FromJson(data);

        TStringBuf data1 = "{ g : { x : { a : { e : f } } } }";

        wcopy.MergeUpdateJson(data1);

        {
            TString json = wcopy.ToJson(true);
            SubstGlobal(json, "\"", "");
            UNIT_ASSERT_VALUES_EQUAL(json, "{g:{x:{a:{b:c,e:f},q:{x:y}},y:fff}}");
        }
    }

    Y_UNIT_TEST(TestMerge4) {
        TStringBuf data = "{ a : 1, b : { c : 2, d : { q : f } } }";
        NSc::TValue val = NSc::TValue::FromJson(data);

        TStringBuf data1 = "{ a : 2, b : { c : 3, d : { q : e }, g : h } }";

        val.MergeUpdateJson(data1);

        {
            TString json = val.ToJson(true);
            SubstGlobal(json, "\"", "");

            UNIT_ASSERT_VALUES_EQUAL(json, "{a:2,b:{c:3,d:{q:e},g:h}}");
        }
    }

    Y_UNIT_TEST(TestMerge5) {
        NSc::TValue v0;
        v0.GetOrAdd("x").MergeUpdate(NSc::TValue(1));
        UNIT_ASSERT_VALUES_EQUAL(v0.ToJson(), "{\"x\":1}");
    }

    Y_UNIT_TEST(TestMerge6) {
        NSc::TValue va = NSc::TValue::FromJson("{\"x\":\"abc\",\"y\":\"def\"}");
        NSc::TValue vb = va.Get("y");
        NSc::TValue diff;
        diff["y"] = vb;
        va.MergeUpdate(diff);
        UNIT_ASSERT_VALUES_EQUAL(va.ToJson(), "{\"x\":\"abc\",\"y\":\"def\"}");
        UNIT_ASSERT_VALUES_EQUAL(vb.ToJson(), "\"def\"");
        UNIT_ASSERT_VALUES_EQUAL(diff.ToJson(), "{\"y\":\"def\"}");
    }

    Y_UNIT_TEST(TestMerge7) {
        NSc::TValue v;
        v["a"] = NSc::TValue::FromJson("[0.125,0.12,0.1,0.08,0.06]");
        UNIT_ASSERT_JSON_EQ_JSON(v, "{a:[0.125,0.12,0.1,0.08,0.06]}");

        NSc::TValue a = v.TrySelectOrAdd("a")->MergeUpdateJson("[1,2,3]");

        UNIT_ASSERT_JSON_EQ_JSON(a, "[1,2,3]");
        UNIT_ASSERT_JSON_EQ_JSON(v, "{a:[1,2,3]}");
    }

    Y_UNIT_TEST(TestMerge8) {
        NSc::TValue v;
        v["a"] = NSc::TValue::FromJson("[0.125,0.12,0.1,0.08,0.06]");
        UNIT_ASSERT_JSON_EQ_JSON(v, "{a:[0.125,0.12,0.1,0.08,0.06]}");

        NSc::TMergeOptions options = {.ArrayMergeMode = NSc::TMergeOptions::EArrayMergeMode::Merge};
        NSc::TValue a = v.TrySelectOrAdd("a")->MergeUpdateJson("[1,2,3]", options);

        UNIT_ASSERT_JSON_EQ_JSON(a, "[1,2,3,0.08,0.06]");
        UNIT_ASSERT_JSON_EQ_JSON(v, "{a:[1,2,3,0.08,0.06]}");
    }

    Y_UNIT_TEST(TestMerge9) {
        NSc::TValue v;
        v["a"] = NSc::TValue::FromJson("[{a:1},{b:2,d:2}]");

        NSc::TValue update;
        update["a"] = NSc::TValue::FromJson("[{},{a:1,d:1},{b:2}]");

        NSc::TMergeOptions options = {.ArrayMergeMode = NSc::TMergeOptions::EArrayMergeMode::Merge};
        v.MergeUpdate(update, options);

        UNIT_ASSERT_JSON_EQ_JSON(v, "{a:[{a:1},{a:1,b:2,d:1},{b:2}]}");

        //no uncontrolled grow
        v.MergeUpdate(update, options);
        v.MergeUpdate(update, options);

        UNIT_ASSERT_JSON_EQ_JSON(v, "{a:[{a:1},{a:1,b:2,d:1},{b:2}]}");
    }

    Y_UNIT_TEST(TestMerge10) {
        NSc::TValue v;
        v["a"] = NSc::TValue::FromJson("[0.125,0.12,0.1,0.08,0.06]");
        UNIT_ASSERT_JSON_EQ_JSON(v, "{a:[0.125,0.12,0.1,0.08,0.06]}");

        NSc::TMergeOptions options = {.ArrayMergeMode = NSc::TMergeOptions::EArrayMergeMode::Merge};
        NSc::TValue delta = NSc::TValue::FromJson("{a:[1,2,3,4,5,6]}");
        v.ReverseMerge(delta, options);

        UNIT_ASSERT_JSON_EQ_JSON(v, "{a:[0.125,0.12,0.1,0.08,0.06,6]}");
    }

    Y_UNIT_TEST(TestMerge11) {
        NSc::TValue v;
        v["a"] = NSc::TValue::FromJson("[{a:1},{b:2,d:2}]");

        NSc::TValue update;
        update["a"] = NSc::TValue::FromJson("[{},{a:1,d:1},{b:2}]");

        NSc::TMergeOptions options = {.ArrayMergeMode = NSc::TMergeOptions::EArrayMergeMode::Merge};
        v.ReverseMerge(update, options);

        UNIT_ASSERT_JSON_EQ_JSON(v, "{a:[{a:1},{a:1,b:2,d:2},{b:2}]}");

        //no uncontrolled grow
        v.ReverseMerge(update, options);
        v.ReverseMerge(update, options);

        UNIT_ASSERT_JSON_EQ_JSON(v, "{a:[{a:1},{a:1,b:2,d:2},{b:2}]}");
    }

}
