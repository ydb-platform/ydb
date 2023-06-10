#include <library/cpp/scheme/scimpl_private.h>
#include <library/cpp/scheme/ut_utils/scheme_ut_utils.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>
#include <library/cpp/string_utils/quote/quote.h>
#include <util/string/subst.h>
#include <util/string/util.h>

#include <type_traits>

Y_UNIT_TEST_SUITE(TSchemeTest) {

    Y_UNIT_TEST(TestNaN) {
        UNIT_ASSERT_VALUES_EQUAL("null", NSc::TValue(std::numeric_limits<double>::quiet_NaN()).ToJson());
        UNIT_ASSERT_VALUES_EQUAL("null", NSc::TValue(-std::numeric_limits<double>::infinity()).ToJson());
        UNIT_ASSERT_VALUES_EQUAL("null", NSc::TValue(std::numeric_limits<double>::infinity()).ToJson());
        UNIT_ASSERT_VALUES_EQUAL("1", NSc::TValue(1.0).ToJson());
    }

    Y_UNIT_TEST(TestNumbers) {
        {
            NSc::TValue vd;
            UNIT_ASSERT_VALUES_EQUAL(2.5, vd.GetNumberMutable(2.5));
            UNIT_ASSERT_VALUES_EQUAL(2, vd.GetIntNumberMutable(-1));
        }
        {
            NSc::TValue vi;
            UNIT_ASSERT_VALUES_EQUAL(2, vi.GetIntNumberMutable(2));
            UNIT_ASSERT_VALUES_EQUAL(2., vi.GetNumberMutable(-1));
        }
        {
            NSc::TValue vb = NSc::TValue::FromJson("true");

            UNIT_ASSERT_VALUES_EQUAL("true", vb.ToJson());

            UNIT_ASSERT(vb.IsBool());
            UNIT_ASSERT(vb.IsIntNumber());
            UNIT_ASSERT(vb.IsNumber());
            UNIT_ASSERT_VALUES_EQUAL(true, vb.GetBool());
            UNIT_ASSERT_VALUES_EQUAL(1, vb.GetIntNumber());
            UNIT_ASSERT_VALUES_EQUAL(1.0, vb.GetNumber());

            NSc::TValue vb1 = vb.Clone();

            UNIT_ASSERT(NSc::TValue::Equal(vb, vb1));

            UNIT_ASSERT_VALUES_EQUAL(true, vb.GetBool());
            UNIT_ASSERT_VALUES_EQUAL(1, vb.GetIntNumber());
            UNIT_ASSERT_VALUES_EQUAL(1.0, vb.GetNumber());
            UNIT_ASSERT(vb.IsBool());
            UNIT_ASSERT_VALUES_EQUAL(1, vb.GetIntNumberMutable());
            UNIT_ASSERT(!vb.IsBool());

            UNIT_ASSERT(NSc::TValue::Equal(vb, vb1));

            UNIT_ASSERT(vb1.IsBool());
            UNIT_ASSERT_VALUES_EQUAL(1.0, vb1.GetNumberMutable());
            UNIT_ASSERT(!vb1.IsBool());

            vb.SetBool(true);

            UNIT_ASSERT(vb.IsBool());
            UNIT_ASSERT(NSc::TValue::Equal(vb, vb1));

            vb = NSc::TValue::FromJson("false");

            UNIT_ASSERT_VALUES_EQUAL("false", vb.ToJson());
            UNIT_ASSERT(!NSc::TValue::Equal(vb, vb1));

            UNIT_ASSERT(vb.IsBool());
            UNIT_ASSERT(vb.IsIntNumber());
            UNIT_ASSERT(vb.IsNumber());
            UNIT_ASSERT_VALUES_EQUAL(false, vb.GetBool());
            UNIT_ASSERT_VALUES_EQUAL(0.0, vb.GetNumber());
            UNIT_ASSERT_VALUES_EQUAL(0, vb.GetIntNumber());

            NSc::TValue vd = NSc::TValue::FromJson("1.0");

            UNIT_ASSERT(vd.IsNumber());
            UNIT_ASSERT(!vd.IsIntNumber());
            UNIT_ASSERT_VALUES_EQUAL(1.0, vd.GetNumber());
            UNIT_ASSERT_VALUES_EQUAL(1, vd.GetIntNumber());
            UNIT_ASSERT_VALUES_EQUAL(1.0, vd.GetNumberMutable());

            NSc::TValue vi = NSc::TValue::FromJson("1");

            UNIT_ASSERT(vi.IsNumber());
            UNIT_ASSERT(vi.IsIntNumber());
            UNIT_ASSERT_VALUES_EQUAL(1.0, vi.GetNumber());
            UNIT_ASSERT_VALUES_EQUAL(1, vi.GetIntNumber());
            UNIT_ASSERT_VALUES_EQUAL(1, vi.GetIntNumberMutable());

            UNIT_ASSERT(NSc::TValue::Equal(vd, vi));

            vd.SetNumber(1.5);
            UNIT_ASSERT(vd.IsNumber());
            UNIT_ASSERT(!vd.IsIntNumber());
            UNIT_ASSERT_VALUES_EQUAL(1.5, vd.GetNumber());
            UNIT_ASSERT_VALUES_EQUAL(1, vd.GetIntNumber());
            UNIT_ASSERT_VALUES_EQUAL(1.5, vd.GetNumberMutable());

            UNIT_ASSERT(!NSc::TValue::Equal(vd, vi));

            UNIT_ASSERT_VALUES_EQUAL("1", vi.ToJson());
            UNIT_ASSERT_VALUES_EQUAL("1.5", vd.ToJson());

            UNIT_ASSERT_VALUES_EQUAL(1, vd.GetIntNumberMutable());
            UNIT_ASSERT(NSc::TValue::Equal(vd, vi));
            vd.SetIntNumber(2);
            UNIT_ASSERT(!NSc::TValue::Equal(vd, vi));
            vi.SetNumber(2.);
            UNIT_ASSERT(NSc::TValue::Equal(vd, vi));
            vd.SetNumber(2.);
            UNIT_ASSERT(NSc::TValue::Equal(vd, vi));
            vi.SetIntNumber(5);
            vd.MergeUpdate(vi);
            UNIT_ASSERT(vd.IsNumber());
            UNIT_ASSERT(vd.IsIntNumber());
            UNIT_ASSERT_VALUES_EQUAL(5, vd.GetIntNumber());
            vd.SetNumber(3.3);
            vi.MergeUpdate(vd);
            UNIT_ASSERT(vi.IsNumber());
            UNIT_ASSERT(!vi.IsIntNumber());
            UNIT_ASSERT_VALUES_EQUAL(3.3, vi.GetNumber());

            vi.SetIntNumber(Max<i64>());
            UNIT_ASSERT_VALUES_EQUAL("9223372036854775807", vi.ToJson());
        }
    }

    template <typename T>
    void DoTestForce(T t) {
        UNIT_ASSERT_VALUES_EQUAL_C(i64(t), NSc::TValue(i64(t)).ForceIntNumber(), ToString(t));
        UNIT_ASSERT_VALUES_EQUAL_C(double(t), NSc::TValue(double(t)).ForceNumber(), ToString(t));

        UNIT_ASSERT_VALUES_EQUAL_C(i64(t), NSc::TValue(TStringBuf(ToString(i64(t)))).ForceIntNumber(), ToString(t));
        UNIT_ASSERT_VALUES_EQUAL_C(ToString(double(t)), ToString(NSc::TValue(TStringBuf(ToString(double(t)))).ForceNumber()), ToString(t));

        UNIT_ASSERT_VALUES_EQUAL_C(ToString(i64(t)), NSc::TValue(TStringBuf(ToString(i64(t)))).ForceString(), ToString(t));
        UNIT_ASSERT_VALUES_EQUAL_C(ToString(double(t)), NSc::TValue(TStringBuf(ToString(double(t)))).ForceString(), ToString(t));
    }

    Y_UNIT_TEST(TestForce) {
        DoTestForce(Max<i64>());
        DoTestForce(Min<i64>());
        DoTestForce(1.5);
        DoTestForce(-1.5);

        UNIT_ASSERT_VALUES_EQUAL(1, NSc::TValue("32a").ForceIntNumber(1));
        UNIT_ASSERT_VALUES_EQUAL(1.5, NSc::TValue("32a").ForceNumber(1.5));
    }

    template <typename T>
    void DoCheckRelations(T t, T tless, T tmore, const NSc::TValue& v, TStringBuf ss) {
        UNIT_ASSERT_C((t == v), ss);
        UNIT_ASSERT_C(!(t != v), ss);
        UNIT_ASSERT_C((t <= v), ss);
        UNIT_ASSERT_C((t >= v), ss);
        UNIT_ASSERT_C(!(t < v), ss);
        UNIT_ASSERT_C(!(t > v), ss);

        UNIT_ASSERT_C(!(tless == v), ss);
        UNIT_ASSERT_C((tless != v), ss);
        UNIT_ASSERT_C((tless <= v), ss);
        UNIT_ASSERT_C(!(tless >= v), ss);
        UNIT_ASSERT_C((tless < v), ss);
        UNIT_ASSERT_C(!(tless > v), ss);
        UNIT_ASSERT_C(!(tmore == v), ss);
        UNIT_ASSERT_C((tmore != v), ss);
        UNIT_ASSERT_C(!(tmore <= v), ss);
        UNIT_ASSERT_C((tmore >= v), ss);
        UNIT_ASSERT_C(!(tmore < v), ss);
        UNIT_ASSERT_C((tmore > v), ss);
    }

    void DoCheckRelations(const NSc::TValue& t, const NSc::TValue&, const NSc::TValue&, const NSc::TValue& v, TStringBuf ss) {
        UNIT_ASSERT_C((t == v), ss);
        UNIT_ASSERT_C(!(t != v), ss);
    }

    //    void DoCheckRelations(bool t, bool, bool, const NSc::TValue& v, TStringBuf ss) {
    //        UNIT_ASSERT_C((t == v), ss);
    //        UNIT_ASSERT_C(!(t != v), ss);
    //    }

    template <typename T>
    void DoCheckAssignment(T t, T tless, T tmore, TStringBuf s, TStringBuf ss) {
        bool expectint = std::is_integral<T>::value;
        bool expectnum = std::is_arithmetic<T>::value;
        bool expectbool = std::is_same<bool, T>::value;

        {
            NSc::TValue v(t);
            UNIT_ASSERT_VALUES_EQUAL_C(expectnum, v.IsNumber(), ss);
            UNIT_ASSERT_VALUES_EQUAL_C(expectint, v.IsIntNumber(), ss);
            UNIT_ASSERT_VALUES_EQUAL_C(expectbool, v.IsBool(), ss);
            UNIT_ASSERT_VALUES_EQUAL_C(s, v.ToJson(), ss);
            DoCheckRelations(t, tless, tmore, v, ss);
        }
        {
            NSc::TValue v;
            UNIT_ASSERT(v.IsNull());
            v = t;
            UNIT_ASSERT(!v.IsNull());
            UNIT_ASSERT_VALUES_EQUAL_C(expectnum, v.IsNumber(), ss);
            UNIT_ASSERT_VALUES_EQUAL_C(expectint, v.IsIntNumber(), ss);
            UNIT_ASSERT_VALUES_EQUAL_C(expectbool, v.IsBool(), ss);
            UNIT_ASSERT_VALUES_EQUAL_C(s, v.ToJson(), ss);
            DoCheckRelations(t, tless, tmore, v, ss);
        }
    }

    template <size_t N>
    void DoCheckAssignmentArr(const char (&t)[N], const char (&tless)[N], const char (&tmore)[N], TStringBuf s, TStringBuf ss) {
        {
            NSc::TValue v(t);

            UNIT_ASSERT_C(v.IsString(), ss);
            UNIT_ASSERT_VALUES_EQUAL_C(s, v.ToJson(), ss);
            DoCheckRelations(t, tless, tmore, v, ss);
        }
        {
            NSc::TValue v;
            v = t;
            UNIT_ASSERT_C(v.IsString(), ss);
            UNIT_ASSERT_VALUES_EQUAL_C(s, v.ToJson(), ss);
            DoCheckRelations(t, tless, tmore, v, ss);
        }
    }

    template <typename T>
    void DoCheckAssignmentNum(T t, T tless, T tmore, TStringBuf s, TStringBuf ss) {
        DoCheckAssignment(t, tless, tmore, s, ss);
        {
            NSc::TValue v;
            T tt = (v = t);
            UNIT_ASSERT_VALUES_EQUAL_C(t, tt, ss);
        }
    }

    Y_UNIT_TEST(TestAssignments) {
        for (int i = -2; i < 3; ++i) {
            TString ii = ToString(i);
            int iless = i - 1;
            int imore = i + 1;
            DoCheckAssignmentNum<signed char>(i, iless, imore, ii, "schar");
            DoCheckAssignmentNum<short>(i, iless, imore, ii, "short");
            DoCheckAssignmentNum<int>(i, iless, imore, ii, "int");
            DoCheckAssignmentNum<long>(i, iless, imore, ii, "long");
            DoCheckAssignmentNum<long long>(i, iless, imore, ii, "longlong");
            DoCheckAssignmentNum<i8>(i, iless, imore, ii, "i8");
            DoCheckAssignmentNum<i16>(i, iless, imore, ii, "i16");
            DoCheckAssignmentNum<i32>(i, iless, imore, ii, "i32");
            DoCheckAssignmentNum<i64>(i, iless, imore, ii, "i64");

            DoCheckAssignmentNum<float>(i, iless, imore, ii, "float");
            DoCheckAssignmentNum<double>(i, iless, imore, ii, "double");
        }

        //        DoCheckAssignment<bool>(true, true, true, "true", "bool");
        //        DoCheckAssignment<bool>(false, false, false, "false", "bool");

        for (int i = 1; i < 3; ++i) {
            TString ii = ToString(i);
            int iless = i - 1;
            int imore = i + 1;

            DoCheckAssignmentNum<char>(i, iless, imore, ii, "char");

            DoCheckAssignmentNum<signed char>(i, iless, imore, ii, "schar");
            DoCheckAssignmentNum<short>(i, iless, imore, ii, "short");
            DoCheckAssignmentNum<int>(i, iless, imore, ii, "int");
            DoCheckAssignmentNum<long>(i, iless, imore, ii, "long");
            DoCheckAssignmentNum<long long>(i, iless, imore, ii, "longlong");
            DoCheckAssignmentNum<i8>(i, iless, imore, ii, "i8");
            DoCheckAssignmentNum<i16>(i, iless, imore, ii, "i16");
            DoCheckAssignmentNum<i32>(i, iless, imore, ii, "i32");
            DoCheckAssignmentNum<i64>(i, iless, imore, ii, "i64");

            DoCheckAssignmentNum<unsigned char>(i, iless, imore, ii, "uchar");
            DoCheckAssignmentNum<unsigned short>(i, iless, imore, ii, "ushort");
            DoCheckAssignmentNum<unsigned int>(i, iless, imore, ii, "uint");
            DoCheckAssignmentNum<unsigned long>(i, iless, imore, ii, "ulong");
            DoCheckAssignmentNum<unsigned long long>(i, iless, imore, ii, "ulonglong");
            DoCheckAssignmentNum<ui8>(i, iless, imore, ii, "ui8");
            DoCheckAssignmentNum<ui16>(i, iless, imore, ii, "ui16");
            DoCheckAssignmentNum<ui32>(i, iless, imore, ii, "ui32");
            DoCheckAssignmentNum<ui64>(i, iless, imore, ii, "ui64");

            DoCheckAssignmentNum<float>(i, iless, imore, ii, "float");
            DoCheckAssignmentNum<double>(i, iless, imore, ii, "double");
        }

        TString uuu = "uuu";
        TString uua = "uua";
        TString uuz = "uuz";
        DoCheckAssignment<char*>(uuu.begin(), uua.begin(), uuz.begin(), "\"uuu\"", "char*");
        DoCheckAssignment<const char*>("www", "wwa", "wwz", "\"www\"", "const char*");
        DoCheckAssignmentArr("xxx", "xxa", "xxz", "\"xxx\"", "const char[]");
        DoCheckAssignment<TStringBuf>("yyy", "yya", "yyz", "\"yyy\"", "TStringBuf");

#if defined(_MSC_VER)
        //TODO
#else
        DoCheckAssignment<TString>("ttt", "tta", "ttz", "\"ttt\"", "TString");
#endif

        NSc::TValue v;
        v.SetDict();
        DoCheckAssignment<NSc::TValue>(v, v, v, "{}", "TValue");
        DoCheckAssignment<NSc::TValue&>(v, v, v, "{}", "TValue&");
        DoCheckAssignment<const NSc::TValue&>(v, v, v, "{}", "const TValue&");

        NSc::TValue v1{1};
        UNIT_ASSERT_VALUES_EQUAL(v1.ToJson(), "1");
    }

    Y_UNIT_TEST(TestAssignmentDictChild) {
        {
            NSc::TValue v;
            {
                NSc::TValue b;
                v["a"] = b;
            }
            v = v["a"];
        }
        {
            NSc::TValue v;
            {
                NSc::TValue b;
                v["a"] = b;
            }
            v = v.Get("a");
        }
        {
            NSc::TValue v;
            {
                NSc::TValue b;
                v["a"] = b;
            }
            v = std::move(v["a"]);
        }
    }

    Y_UNIT_TEST(TestInsert) {
        NSc::TValue v;
        v.Insert(0, "b");
        v.Insert(0, "a");
        v.Insert(2, "d");
        v.Insert(2, "c");
        UNIT_ASSERT_VALUES_EQUAL(v.ToJson(), R"(["a","b","c","d"])");

        v.AppendAll({1, 2, 3});
        UNIT_ASSERT_VALUES_EQUAL(v.ToJson(), R"(["a","b","c","d",1,2,3])");

        TVector<int> d{4, 5, 6};
        v.AppendAll(d);
        UNIT_ASSERT_VALUES_EQUAL(v.ToJson(), R"(["a","b","c","d",1,2,3,4,5,6])");
        UNIT_ASSERT_VALUES_EQUAL(d.size(), 3u);
        TVector<TStringBuf> s{"x", "y", "z"};
        v.AppendAll(s.begin(), s.end());
        UNIT_ASSERT_VALUES_EQUAL(v.ToJson(), R"(["a","b","c","d",1,2,3,4,5,6,"x","y","z"])");

        UNIT_ASSERT_VALUES_EQUAL(v.Clone().Clear().AppendAll(s).ToJson(), R"(["x","y","z"])");
        UNIT_ASSERT_VALUES_EQUAL(v.Clone().Clear().AppendAll(TVector<TStringBuf>()).ToJson(), R"([])");

        v.AddAll({{"a", "b"}, {"c", "d"}});
        UNIT_ASSERT_VALUES_EQUAL(v.ToJson(), R"({"a":"b","c":"d"})");
    }

    Y_UNIT_TEST(TestFrontBack) {
        NSc::TValue v;
        const NSc::TValue& vv = v;
        UNIT_ASSERT(NSc::TValue::Same(vv.Front(), NSc::Null()));
        UNIT_ASSERT(NSc::TValue::Same(vv.Back(), NSc::Null()));
        UNIT_ASSERT(!vv.IsArray());
        v.Back() = "a";
        UNIT_ASSERT_VALUES_EQUAL("a", vv.Front().GetString());
        UNIT_ASSERT_VALUES_EQUAL("a", vv.Back().GetString());
        UNIT_ASSERT(vv.IsArray());
        v.Push("b");
        UNIT_ASSERT_VALUES_EQUAL("a", vv.Front().GetString());
        UNIT_ASSERT_VALUES_EQUAL("b", vv.Back().GetString());

        UNIT_ASSERT_VALUES_EQUAL("b", v.Pop().GetString());

        UNIT_ASSERT_VALUES_EQUAL("a", vv.Front().GetString());
        UNIT_ASSERT_VALUES_EQUAL("a", vv.Back().GetString());

        UNIT_ASSERT_VALUES_EQUAL("a", v.Pop().GetString());

        UNIT_ASSERT(NSc::TValue::Same(vv.Front(), NSc::Null()));
        UNIT_ASSERT(NSc::TValue::Same(vv.Back(), NSc::Null()));

        v.Front() = "a";
        UNIT_ASSERT_VALUES_EQUAL("a", vv.Front().GetString());
        UNIT_ASSERT_VALUES_EQUAL("a", vv.Back().GetString());
    }

    Y_UNIT_TEST(TestAssign) {
        NSc::TValue v;
        v.SetArray();
        v.Push() = "test";
        UNIT_ASSERT_VALUES_EQUAL(v.ToJson(), "[\"test\"]");
        UNIT_ASSERT(NSc::TValue::SamePool(v[0], v));
    }

    NSc::TValue MutableRef(const NSc::TValue& v) {
        return v;
    }

    NSc::TValue Clone(const NSc::TValue& v) {
        NSc::TValue v1 = v.Clone();
        UNIT_ASSERT_VALUES_EQUAL(v1.ToJson(true), v.ToJson(true));
        return v1;
    }

    Y_UNIT_TEST(TestCOW) {
        NSc::TValue vd = NSc::TValue::FromJson("{ a : 1, b : c}");
        NSc::TValue va = NSc::TValue::FromJson("[ x, y]");
        NSc::TValue vs = NSc::TValue::FromJson("foo");
        NSc::TValue vn = NSc::TValue::FromJson("1");
        TString sd = "{\"a\":1,\"b\":\"c\"}";
        TString sa = "[\"x\",\"y\"]";
        TString ss = "\"foo\"";
        TString sn = "1";
        UNIT_ASSERT_VALUES_EQUAL(sd, vd.ToJson(true));
        UNIT_ASSERT_VALUES_EQUAL(sa, va.ToJson(true));
        UNIT_ASSERT_VALUES_EQUAL(ss, vs.ToJson(true));
        UNIT_ASSERT_VALUES_EQUAL(sn, vn.ToJson(true));

        {
            NSc::TValue v2 = MutableRef(vn);
            v2 = -1;

            UNIT_ASSERT_VALUES_EQUAL(sn, vn.ToJson(true));

            NSc::TValue v3 = Clone(vn);
            v3 = -1;

            UNIT_ASSERT_VALUES_EQUAL(v3.ToJson(true), v2.ToJson(true));
        }

        {
            NSc::TValue v2 = MutableRef(vs);
            v2 = "xxx";

            UNIT_ASSERT_VALUES_EQUAL(ss, vs.ToJson(true));

            NSc::TValue v3 = Clone(vs);
            v3 = "xxx";

            UNIT_ASSERT_VALUES_EQUAL(v3.ToJson(true), v2.ToJson(true));
        }

        {
            NSc::TValue v2 = MutableRef(vd);
            v2["a"] = "zzz";

            UNIT_ASSERT_VALUES_EQUAL(sd, vd.ToJson(true));

            NSc::TValue v3 = Clone(vd);
            v3["a"] = "zzz";

            UNIT_ASSERT_VALUES_EQUAL(v3.ToJson(true), v2.ToJson(true));
        }

        {
            NSc::TValue v2 = MutableRef(va);
            v2[0] = "zzz";

            UNIT_ASSERT_VALUES_EQUAL(sa, va.ToJson(true));

            NSc::TValue v3 = Clone(va);
            v3[0] = "zzz";

            UNIT_ASSERT_VALUES_EQUAL(v3.ToJson(true), v2.ToJson(true));
        }

        {
            NSc::TValue v2 = MutableRef(va);

            UNIT_ASSERT_VALUES_EQUAL(sa, va.ToJson(true));

            NSc::TValue v3 = Clone(va);

            UNIT_ASSERT_VALUES_EQUAL(v3.ToJson(true), v2.ToJson(true));
        }

        {
            NSc::TValue v2 = MutableRef(va);
            v2.ClearArray();

            UNIT_ASSERT_VALUES_EQUAL(sa, va.ToJson(true));

            NSc::TValue v3 = Clone(va);
            v3.ClearArray();

            UNIT_ASSERT_VALUES_EQUAL(v3.ToJson(true), v2.ToJson(true));
        }
    }

    Y_UNIT_TEST(TestOperators) {
        UNIT_ASSERT("test" == NSc::TValue("test"));
        UNIT_ASSERT(NSc::TValue("test") == "test");

        UNIT_ASSERT("test1" != NSc::TValue("test"));
        UNIT_ASSERT(NSc::TValue("test") != "test1");

        UNIT_ASSERT("test1" > NSc::TValue("test"));
        UNIT_ASSERT(NSc::TValue("test") < "test1");

        UNIT_ASSERT("test" < NSc::TValue("test1"));
        UNIT_ASSERT(NSc::TValue("test1") > "test");

        UNIT_ASSERT(1 == NSc::TValue(1));
        UNIT_ASSERT(NSc::TValue(1) == 1);

        UNIT_ASSERT(2 != NSc::TValue(1));
        UNIT_ASSERT(NSc::TValue(1) != 2);

        UNIT_ASSERT(1 < NSc::TValue(2));
        UNIT_ASSERT(NSc::TValue(2) > 1);

        UNIT_ASSERT(2 > NSc::TValue(1));
        UNIT_ASSERT(NSc::TValue(1) < 2);

        UNIT_ASSERT(TString("test") == NSc::TValue("test"));
    }

    Y_UNIT_TEST(TestDestructor) {
        NSc::TValue v;
        const NSc::TValue& v1 = v;
        v1.GetString();
        v1.GetArray();
        v1.GetNumber();
        v1.GetDict();
        v.GetString();
        v.GetArray();
        v.GetNumber();
        v.GetDict();
    }

    void DoTestSamePool(TStringBuf json, TStringBuf jpath) {
        NSc::TValue v = NSc::TValue::FromJson(json);
        UNIT_ASSERT_C(NSc::TValue::SamePool(v, v.TrySelect(jpath)), json);
    }

    Y_UNIT_TEST(TestSamePool) {
        DoTestSamePool("", "");
        DoTestSamePool("a", "");
        DoTestSamePool("[a]", "0");
        DoTestSamePool("{a:b}", "a");
        DoTestSamePool("{a:{b:c}}", "a/b");
        DoTestSamePool("{a:{b:[c, {}]}}", "a/b/1");
        DoTestSamePool("{a:{b:[c, {d:{e:[]}}]}}", "a/b/1/d/e");
        UNIT_ASSERT(!NSc::TValue::SamePool(NSc::TValue(), NSc::TValue()));
        UNIT_ASSERT(!NSc::TValue::SamePool(NSc::Null().Clone(), NSc::Null()));
        UNIT_ASSERT(!NSc::TValue::SamePool(NSc::TValue() = 0, NSc::TValue()));
        UNIT_ASSERT(!NSc::TValue::SamePool(NSc::TValue::FromJson("a"), NSc::TValue::FromJson("a")));
        NSc::TValue v, vv;
        v["x"] = vv;
        UNIT_ASSERT(!NSc::TValue::SamePool(v, vv));
        UNIT_ASSERT(!NSc::TValue::SamePool(v, v["x"]));
        v = vv;
        UNIT_ASSERT(NSc::TValue::SamePool(v, vv));
    }

    Y_UNIT_TEST(TestLoopDetection) {
        NSc::NImpl::GetTlsInstance<NSc::NImpl::TSelfLoopContext>().ReportingMode
            = NSc::NImpl::TSelfLoopContext::EMode::Stderr;

        NSc::TValue x;

        x["a"]["x"] = x;
        x["b"][0] = x;

        UNIT_ASSERT(x.IsSameOrAncestorOf(x));
        UNIT_ASSERT(x.IsSameOrAncestorOf(x.Get("a")));
        UNIT_ASSERT(x.Get("a").IsSameOrAncestorOf(x));

        UNIT_ASSERT_VALUES_EQUAL(x.ToJson(), "{\"a\":{\"x\":null},\"b\":[null]}");

        NSc::TValue y = x.Clone();

        UNIT_ASSERT(y.Has("a"));
        UNIT_ASSERT(y.Get("a").Has("x"));
        UNIT_ASSERT(y.Get("a").Get("x").IsNull());
        UNIT_ASSERT(y.Get("a").Get("x").IsNull());

        UNIT_ASSERT(y.Has("b"));
        UNIT_ASSERT(y.Get("b").Has(0));
        UNIT_ASSERT(y.Get("b").Get(0).IsNull());

        UNIT_ASSERT_VALUES_EQUAL(y.ToJson(), "{\"a\":{\"x\":null},\"b\":[null]}");

        NSc::TValue z;
        z.MergeUpdate(x);

        UNIT_ASSERT(z.Has("a"));
        UNIT_ASSERT(z.Get("a").Has("x"));
        UNIT_ASSERT(z.Get("a").Get("x").IsNull());

        UNIT_ASSERT(z.Has("b"));
        UNIT_ASSERT(z.Get("b").Has(0));
        UNIT_ASSERT(z.Get("b").Get(0).IsNull());

        UNIT_ASSERT_VALUES_EQUAL(z.ToJson(), "{\"a\":{\"x\":null},\"b\":[null]}");

        x["a"].Delete("x");
        x["b"].Delete(0);

        UNIT_ASSERT(x.IsSameOrAncestorOf(x));
        UNIT_ASSERT(x.IsSameOrAncestorOf(x.Get("a")));
        UNIT_ASSERT(!x.Get("a").IsSameOrAncestorOf(x));
    }

    Y_UNIT_TEST(TestLoopDetectionThrow) {
        NSc::NImpl::GetTlsInstance<NSc::NImpl::TSelfLoopContext>().ReportingMode
            = NSc::NImpl::TSelfLoopContext::EMode::Throw;

        {
            NSc::TValue x;
            x["a"]["x"] = x;

            UNIT_ASSERT(x.IsSameOrAncestorOf(x));
            UNIT_ASSERT(x.IsSameOrAncestorOf(x.Get("a")));
            UNIT_ASSERT(x.Get("a").IsSameOrAncestorOf(x));

            UNIT_ASSERT_EXCEPTION(x.ToJson(), NSc::TSchemeException);
            UNIT_ASSERT_EXCEPTION(x.Clone(), NSc::TSchemeException);

            NSc::TValue z;
            UNIT_ASSERT_EXCEPTION(z.MergeUpdate(x), NSc::TSchemeException);

            UNIT_ASSERT_VALUES_EQUAL(z.ToJson(), "{\"a\":{\"x\":null}}");

            x["a"].Delete("x");

            UNIT_ASSERT(x.IsSameOrAncestorOf(x));
            UNIT_ASSERT(x.IsSameOrAncestorOf(x.Get("a")));
            UNIT_ASSERT(!x.Get("a").IsSameOrAncestorOf(x));

            UNIT_ASSERT_VALUES_EQUAL(x.ToJson(), "{\"a\":{}}");
        }

        {
            NSc::TValue x;
            x["a"][0] = x;

            UNIT_ASSERT(x.IsSameOrAncestorOf(x));
            UNIT_ASSERT(x.IsSameOrAncestorOf(x.Get("a")));
            UNIT_ASSERT(x.Get("a").IsSameOrAncestorOf(x));

            UNIT_ASSERT_EXCEPTION(x.ToJson(), NSc::TSchemeException);
            UNIT_ASSERT_EXCEPTION(x.Clone(), NSc::TSchemeException);

            NSc::TValue z;
            UNIT_ASSERT_EXCEPTION(z.MergeUpdate(x), NSc::TSchemeException);

            UNIT_ASSERT_VALUES_EQUAL(z.ToJson(), "{\"a\":[null]}");

            x["a"].Delete(0);

            UNIT_ASSERT(x.IsSameOrAncestorOf(x));
            UNIT_ASSERT(x.IsSameOrAncestorOf(x.Get("a")));
            UNIT_ASSERT(!x.Get("a").IsSameOrAncestorOf(x));

            UNIT_ASSERT_VALUES_EQUAL(x.ToJson(), "{\"a\":[]}");
        }
    }

    Y_UNIT_TEST(TestIsSameOrAncestorOf) {
        NSc::TValue x;
        UNIT_ASSERT(x.IsSameOrAncestorOf(x));

        x["a"] = NSc::Null();
        UNIT_ASSERT(x.IsSameOrAncestorOf(x.Get("a")));

        NSc::TValue a = 1;
        NSc::TValue b = 2;
        NSc::TValue c = 3;
        NSc::TValue d = 4;

        x["a"] = a;
        x["b"] = b;
        x["c"] = a;
        UNIT_ASSERT(x.IsSameOrAncestorOf(a));
        UNIT_ASSERT(x.IsSameOrAncestorOf(b));
        UNIT_ASSERT(x.Get("a").IsSameOrAncestorOf(a));
        UNIT_ASSERT(x.Get("b").IsSameOrAncestorOf(b));
        UNIT_ASSERT(x.Get("c").IsSameOrAncestorOf(a));

        UNIT_ASSERT(!x.Get("a").IsSameOrAncestorOf(b));
        UNIT_ASSERT(!x.Get("b").IsSameOrAncestorOf(a));

        UNIT_ASSERT(!x.Get("a").Get(0).IsSameOrAncestorOf(a));

        b.Push() = c;
        b.Push() = d;
        b.Push() = c;

        UNIT_ASSERT(x.Get("b").IsSameOrAncestorOf(b));
        UNIT_ASSERT(x.IsSameOrAncestorOf(c));
        UNIT_ASSERT(x.IsSameOrAncestorOf(d));
        UNIT_ASSERT(x.Get("b").Get(0).IsSameOrAncestorOf(c));
        UNIT_ASSERT(x.Get("b").Get(1).IsSameOrAncestorOf(d));
        UNIT_ASSERT(x.Get("b").Get(2).IsSameOrAncestorOf(c));

        UNIT_ASSERT(b.Get(0).IsSameOrAncestorOf(b.Get(2)));
        UNIT_ASSERT(b.Get(2).IsSameOrAncestorOf(b.Get(0)));
        UNIT_ASSERT(b.Get(0).IsSameOrAncestorOf(c));
        UNIT_ASSERT(b.Get(1).IsSameOrAncestorOf(d));

        UNIT_ASSERT(!b.Get(0).IsSameOrAncestorOf(d));
        UNIT_ASSERT(!b.Get(1).IsSameOrAncestorOf(c));
    }

    static void ByVal(NSc::TValue v) {
        v["VAL"] = 1;
    }

    static void ByRef(NSc::TValue& v) {
        ByVal(v);
    }

    static void ByRefAndModify(NSc::TValue& v) {
        v["REF"] = 1;
        ByVal(v);
    }

    Y_UNIT_TEST(TestMove) {
        using namespace NSc;
        {
            TValue v = TValue::FromJson("{}");
            ByRef(v);
            UNIT_ASSERT_VALUES_EQUAL(v.ToJson(), "{\"VAL\":1}");
        }
        {
            TValue v = TValue::FromJson("{}");
            ByVal(v);
            UNIT_ASSERT_VALUES_EQUAL(v.ToJson(), "{\"VAL\":1}");
        }
        {
            TValue v;
            ByVal(v);
            UNIT_ASSERT_VALUES_EQUAL(v.ToJson(), "{\"VAL\":1}");
        }
        {
            TValue v = TValue::FromJson("{}");
            ByRefAndModify(v);
            UNIT_ASSERT_VALUES_EQUAL(v.ToJson(), "{\"REF\":1,\"VAL\":1}");
        }
        {
            TValue v = TValue::FromJson("{foo:bar}");
            TValue w(std::move(v));
            UNIT_ASSERT(v.IsNull());
            v = static_cast<TValue&>(v);
            UNIT_ASSERT(v.IsNull());
            UNIT_ASSERT_VALUES_EQUAL(w.Get("foo").GetString(), "bar");
            v = std::move(w);
            UNIT_ASSERT_VALUES_EQUAL(v.Get("foo").GetString(), "bar");
            UNIT_ASSERT(w.IsNull());
            UNIT_ASSERT(w.Get("foo").IsNull()); // no crash here
            w["foo"] = "baz";  // no crash here
            UNIT_ASSERT(w.IsDict());
            UNIT_ASSERT_VALUES_EQUAL(w.Get("foo").GetString(), "baz");
        }
        UNIT_ASSERT(NSc::TValue::DefaultValue().IsNull());
    }

    //SPI-25156
    Y_UNIT_TEST(TestMoveNotCorruptingDefault) {
        using namespace NSc;
        TValue w = TValue::FromJson("{foo:bar}");
        TValue v = std::move(w);
        w["foo"] = "baz";  // no crash here
        UNIT_ASSERT(NSc::TValue::DefaultValue().IsNull());
    }

    Y_UNIT_TEST(TestCopyFrom) {
        {
            TString sa = "[1,2]";
            const NSc::TValue& va = NSc::TValue::FromJson(sa);
            NSc::TValue vb = va;
            vb.CopyFrom(va);
            UNIT_ASSERT_VALUES_EQUAL(va.ToJson(), sa);
            UNIT_ASSERT_VALUES_EQUAL(vb.ToJson(), sa);
        }
        {
            TString sa = "[1,2]";
            NSc::TValue va = NSc::TValue::FromJson(sa);
            NSc::TValue vb = va;
            vb.CopyFrom(va);
            UNIT_ASSERT_VALUES_EQUAL(va.ToJson(), sa);
            UNIT_ASSERT_VALUES_EQUAL(vb.ToJson(), sa);
        }
        {
            TString sa = "[1,2]";
            NSc::TValue va = NSc::TValue::FromJson(sa);
            const NSc::TValue& vb = va;
            va.CopyFrom(vb);
            UNIT_ASSERT_VALUES_EQUAL(va.ToJson(), sa);
            UNIT_ASSERT_VALUES_EQUAL(vb.ToJson(), sa);
        }
        {
            TString sa = "[1,2]";
            NSc::TValue va = NSc::TValue::FromJson(sa);
            NSc::TValue vb = va;
            va.CopyFrom(vb);
            UNIT_ASSERT_VALUES_EQUAL(va.ToJson(), sa);
            UNIT_ASSERT_VALUES_EQUAL(vb.ToJson(), sa);
        }
        {
            NSc::TValue va = NSc::TValue::FromJson("{\"x\":\"ab\",\"y\":{\"p\":\"cd\",\"q\":\"ef\"}}");
            NSc::TValue vb = va.Get("y");
            va.CopyFrom(vb);
            TString sa = "{\"p\":\"cd\",\"q\":\"ef\"}";
            UNIT_ASSERT_VALUES_EQUAL(va.ToJson(), sa);
            UNIT_ASSERT_VALUES_EQUAL(vb.ToJson(), sa);
        }
        {
            NSc::TValue va = NSc::TValue::FromJson("{\"x\":\"ab\",\"y\":{\"p\":\"cd\",\"q\":\"ef\"}}");
            const NSc::TValue& vb = va.Get("y");
            va.CopyFrom(vb);
            TString sa = "{\"p\":\"cd\",\"q\":\"ef\"}";
            UNIT_ASSERT_VALUES_EQUAL(va.ToJson(), sa);
        }
        {
            NSc::TValue va = NSc::TValue::FromJson("{\"x\":\"ab\",\"y\":{\"p\":\"cd\",\"q\":\"ef\"}}");
            NSc::TValue vb = va.Get("y");
            va = vb;
            TString sa = "{\"p\":\"cd\",\"q\":\"ef\"}";
            UNIT_ASSERT_VALUES_EQUAL(va.ToJson(), sa);
            UNIT_ASSERT_VALUES_EQUAL(vb.ToJson(), sa);
        }
        {
            NSc::TValue va = NSc::TValue::FromJson("{\"x\":\"ab\",\"y\":{\"p\":\"cd\",\"q\":\"ef\"}}");
            const NSc::TValue& vb = va.Get("y");
            va = vb;
            TString sa = "{\"p\":\"cd\",\"q\":\"ef\"}";
            UNIT_ASSERT_VALUES_EQUAL(va.ToJson(), sa);
            UNIT_ASSERT_VALUES_EQUAL(vb.ToJson(), sa);
        }
    }

    Y_UNIT_TEST(TestCopyingDictIntoSelf) { //Found by fuzzing
        NSc::TValue a;
        NSc::TValue b = a.GetOrAdd("aa");
        b.CopyFrom(a);
        NSc::TValue target = NSc::TValue::FromJsonThrow("{\"aa\":null}");
        UNIT_ASSERT_VALUES_EQUAL(b, target);
        UNIT_ASSERT_VALUES_EQUAL(a, target);
    }

    Y_UNIT_TEST(TestCopyingDictIntoSelfByRef) { //Found by fuzzing
        NSc::TValue a;
        NSc::TValue& b = a.GetOrAdd("aa");
        b.CopyFrom(a);
        UNIT_ASSERT_VALUES_EQUAL(b, NSc::TValue::FromJsonThrow("{\"aa\":null}"));
        UNIT_ASSERT_VALUES_EQUAL(a, NSc::TValue::FromJsonThrow("{\"aa\": {\"aa\": null}}"));
    }

    Y_UNIT_TEST(TestGetNoAdd) {
        NSc::TValue v = NSc::NUt::AssertFromJson("{a:[null,-1,2,3.4],b:3,c:{d:5}}");
        UNIT_ASSERT(v.GetNoAdd("a") != nullptr);
        UNIT_ASSERT(v.GetNoAdd("b") != nullptr);
        UNIT_ASSERT(v.GetNoAdd("c") != nullptr);
        UNIT_ASSERT(v.GetNoAdd("d") == nullptr);
        UNIT_ASSERT(v.GetNoAdd("value") == nullptr);

        NSc::TValue* child = v.GetNoAdd("c");
        UNIT_ASSERT(child != nullptr);
        (*child)["e"]["f"] = 42;
        const NSc::TValue expectedResult = NSc::NUt::AssertFromJson("{a:[null,-1,2,3.4],b:3,c:{d:5,e:{f:42}}}");
        UNIT_ASSERT_VALUES_EQUAL(v, expectedResult);
    }

    Y_UNIT_TEST(TestNewNodeOnCurrentPool) {
        NSc::TValue parent;
        parent["foo"] = "bar";

        // standalone
        NSc::TValue firstChild(10);
        UNIT_ASSERT(!NSc::TValue::SamePool(parent, firstChild));

        // shares a memory pool
        NSc::TValue secondChild = parent.CreateNew();
        UNIT_ASSERT(secondChild.IsNull());
        UNIT_ASSERT(NSc::TValue::SamePool(parent, secondChild));
        secondChild = 20;
        UNIT_ASSERT(secondChild.IsIntNumber());
        UNIT_ASSERT(NSc::TValue::SamePool(parent, secondChild));

        // attach children to parent
        parent["first"] = std::move(firstChild);
        parent["second"] = std::move(secondChild);
        UNIT_ASSERT_VALUES_EQUAL(parent["first"].GetIntNumber(), 10);
        UNIT_ASSERT_VALUES_EQUAL(parent["second"].GetIntNumber(), 20);
        UNIT_ASSERT(!NSc::TValue::SamePool(parent, parent["first"]));
        UNIT_ASSERT(NSc::TValue::SamePool(parent, parent["second"]));
    }

    Y_UNIT_TEST(TestNewNodeAfterMove) {
        NSc::TValue a;
        NSc::TValue b = std::move(a); // after this, `a` has no memory pool
        NSc::TValue a2 = a.CreateNew(); // no crash here
        NSc::TValue b2 = b.CreateNew();
        UNIT_ASSERT(!NSc::TValue::SamePool(a, b));
        UNIT_ASSERT(NSc::TValue::SamePool(b, b2));
        UNIT_ASSERT(!NSc::TValue::SamePool(a2, b2));
    }

    namespace {
        bool FillChild(int x, NSc::TValue& result) {
            if (x % 2 == 0) {
                result["value"] = x;
                return true;
            }
            return false;
        }
    }

    Y_UNIT_TEST(TestHierarchyCreationPatterns) {
        const int n = 1000;

        // Good: the code looks so clear and intuitive.
        // Bad: it creates one thousand of independent memory pools, half of them remain alive at the end.
        NSc::TValue list1;
        for (int i = 0; i < n; ++i) {
            NSc::TValue child;
            if (FillChild(i, child)) {
                list1.Push(std::move(child));
            }
        }

        // Good: a single memory pool is reused.
        // Bad: we have to add and remove a child manually.
        // Bad: some memory on pool remains unused (gaps are left after removing nodes).
        NSc::TValue list2;
        for (int i = 0; i < n; ++i) {
            auto& child = list2.Push();
            if (!FillChild(i, child)) {
                list2.Pop();
            }
        }

        // Good: a single memory pool is reused.
        // Good: the code looks quite intuitive.
        // Good: there could be multiple parents on the same pool.
        // Bad: some memory on pool remains unused.
        // Bad: you have to know about the special method `CreateNew()`.
        NSc::TValue list3;
        for (int i = 0; i < n; ++i) {
            auto child = list3.CreateNew();
            if (FillChild(i, child)) {
                list3.Push(std::move(child));
            }
        }

        // Results are the same
        UNIT_ASSERT_VALUES_EQUAL(list1.ToJson(), list2.ToJson());
        UNIT_ASSERT_VALUES_EQUAL(list2.ToJson(), list3.ToJson());
    }
}
