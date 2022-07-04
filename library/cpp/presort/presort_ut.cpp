#include "presort.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/algorithm.h>
#include <util/stream/format.h>
#include <util/string/escape.h>

using namespace NPresort;

class TEscapedOutput: public IOutputStream {
public:
    TEscapedOutput(IOutputStream* out)
        : Out(out)
    {
    }

    ~TEscapedOutput() override {
    }

private:
    void DoWrite(const void* data, size_t size) override {
        *Out << EscapeC((const char*)data, size);
    }

private:
    IOutputStream* Out;
};

Y_UNIT_TEST_SUITE(PresortTests) {
    struct TTester: public TResultOps {
        TStringStream Enc;
        TStringStream Raw;
        TEscapedOutput Dec;
        bool First;
        bool Hex;
        TVector<TString> Rows;

        TTester(bool hex = false)
            : Dec(&Raw)
            , First(true)
            , Hex(hex)
        {
        }

        template <typename T>
        TTester& Asc(const T& val) {
            Encode(Enc, val);
            return *this;
        }

        template <typename T>
        TTester& Desc(const T& val) {
            Encode(Enc, val, true);
            return *this;
        }

        TTester& AscU(ui64 val) {
            EncodeUnsignedInt(Enc, val);
            return *this;
        }

        TTester& DescU(ui64 val) {
            EncodeUnsignedInt(Enc, val, true);
            return *this;
        }

        TTester& AscS(const TString& str) {
            EncodeString(Enc, UnescapeC(str));
            return *this;
        }

        TTester& DescS(const TString& str) {
            EncodeString(Enc, UnescapeC(str), true);
            return *this;
        }

        void AddRow() {
            Rows.push_back(Enc.Str());
            Enc.clear();
        }

        void TestCodec(const TString& good) {
            Decode(*this, Enc.Str());

            TStringStream s;
            s << EscapeC(Enc.Str()) << Endl;
            s << Raw.Str() << Endl;

            //~ Y_UNUSED(good);
            //~ Cerr << s.Str() << Endl;
            UNIT_ASSERT_NO_DIFF(good, s.Str());
        }

        void TestOrder(const TString& good) {
            Sort(Rows.begin(), Rows.end());
            TStringStream s;
            for (auto row : Rows) {
                Decode(*this, row);
                s << Raw.Str() << Endl;
                Raw.clear();
                First = true;
            }

            //~ Y_UNUSED(good);
            //~ Cerr << s.Str() << Endl;
            UNIT_ASSERT_NO_DIFF(good, s.Str());
        }

        void Clear() {
            Enc.clear();
            Raw.clear();
            First = true;
            Rows.clear();
        }

        void SetError(const TString& err) {
            Raw.clear();
            Raw << err;
        }

        void SetSignedInt(i64 val) {
            Put() << val;
        }

        void SetUnsignedInt(ui64 val) {
            if (Hex) {
                Put() << ::Hex(val, HF_ADDX);
            } else {
                Put() << val;
            }
        }

        void SetFloat(float val) {
            Put() << val;
        }

        void SetDouble(double val) {
            Put() << val;
        }

        void SetString(const TString& str) {
            Put() << str;
        }

        void SetOptional(bool filled) {
            Put() << (filled ? "" : "[]");
            First = filled;
        }

        IOutputStream& Put() {
            if (!First) {
                Raw << "\t";
            }
            First = false;
            return Dec;
        }
    };

    Y_UNIT_TEST(BasicIntsCodec) {
        TTester tester;
        tester.Asc(0).Asc(1);
        tester.TestCodec("01\\1\n0\t1\n");
        tester.Clear();
        tester.Desc(0).Desc(1);
        tester.TestCodec("\\xCF\\xCE\\xFE\n0\t1\n");
    }

    Y_UNIT_TEST(BasicNegativeIntsCodec) {
        TTester tester;
        tester.Asc(-1).Asc(-1000);
        tester.TestCodec(".\\xFE-\\xFC\\x17\n-1\t-1000\n");
        tester.Clear();
        tester.Desc(-1).Desc(-1000);
        tester.TestCodec("\\xD1\\1\\xD2\\3\\xE8\n-1\t-1000\n");
    }

#ifndef PRESORT_FP_DISABLED
    Y_UNIT_TEST(BasicDoublesCodec) {
        TTester tester;
        tester.Asc(0.0).Asc(3.1415).Asc(-3.1415);
        tester.TestCodec(
            "dg1\\0027\\x19!\\xCA\\xC0\\x83\\x12oa1\\2(\\xE6\\3365?|\\xED\\x90\n"
            "0\t3.1415\t-3.1415\n");
        tester.Clear();
        tester.Desc(0.0).Desc(3.1415).Desc(-3.1415);
        tester.TestCodec(
            "\\x9B\\x98\\xCE\\xFD\\xC8\\xE6\\3365?|\\xED\\x90\\x9E\\xCE\\xFD\\xD7\\x19!\\xCA\\xC0\\x83\\x12o\n"
            "0\t3.1415\t-3.1415\n");
    }

    Y_UNIT_TEST(NegExpDoublesCodec) {
        TTester tester;
        tester.Asc(-0.1).Asc(0.1);
        tester.TestCodec(
            "b.\\xFC(\\346fffffef.\\3747\\x19\\x99\\x99\\x99\\x99\\x99\\x9A\n"
            "-0.1\t0.1\n");
        tester.Clear();
        tester.Desc(-0.1).Desc(0.1);
        tester.TestCodec(
            "\\x9D\\xD1\\3\\xD7\\x19\\x99\\x99\\x99\\x99\\x99\\x9A\\x99\\xD1\\3\\xC8\\346fffffe\n"
            "-0.1\t0.1\n");
    }

    Y_UNIT_TEST(DenormDoublesCodec) {
        TTester tester;
        const double val = std::numeric_limits<double>::denorm_min();
        //~ Cerr << val << Endl;
        tester.Asc(val);
        tester.TestCodec(
            "e-\\xFB\\3167\\x10\\0\\0\\0\\0\\0\\0\n"
            "4.940656458e-324\n");
        tester.Clear();
        tester.Desc(val);
        tester.TestCodec(
            "\\x9A\\xD2\\0041\\xC8\\xEF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\n"
            "4.940656458e-324\n");
    }

    Y_UNIT_TEST(ExtremalDoublesCodec) {
        TTester tester;
        const double inf = std::numeric_limits<double>::infinity();
        const double nan = std::sqrt(-1.0);
        tester.Asc(-inf).Asc(inf).Asc(nan);
        tester.TestCodec(
            "`hi\n"
            "-inf\tinf\tnan\n");
        tester.Clear();
        tester.Desc(-inf).Desc(inf).Desc(nan);
        tester.TestCodec(
            "\\x9F\\x97\\x96\n"
            "-inf\tinf\tnan\n");
    }

    Y_UNIT_TEST(NegExpFloatsCodec) {
        TTester tester;
        const float val = 0.1;
        tester.Asc(-val).Asc(val);
        tester.TestCodec(
            "R.\\xFC+\\377332V.\\3744\\0\\xCC\\xCC\\xCD\n"
            "-0.1\t0.1\n");
        tester.Clear();
        tester.Desc(-val).Desc(val);
        tester.TestCodec(
            "\\xAD\\xD1\\3\\xD4\\0\\xCC\\xCC\\xCD\\xA9\\xD1\\3\\xCB\\377332\n"
            "-0.1\t0.1\n");
    }

    Y_UNIT_TEST(DenormFloatsCodec) {
        TTester tester;
        const float val = std::numeric_limits<float>::denorm_min();
        //~ Cerr << val << Endl;
        tester.Asc(val);
        tester.TestCodec(
            "U-\\xFFk4\\0\\x80\\0\\0\n"
            "1.4013e-45\n");
        tester.Clear();
        tester.Desc(val);
        tester.TestCodec(
            "\\xAA\\xD2\\0\\x94\\xCB\\xFF\\x7F\\xFF\\xFF\n"
            "1.4013e-45\n");
    }

    Y_UNIT_TEST(ExtremalFloatsCodec) {
        TTester tester;
        const float inf = std::numeric_limits<float>::infinity();
        const float nan = std::sqrt(-1.0);
        tester.Asc(-inf).Asc(inf).Asc(nan);
        tester.TestCodec(
            "PXY\n"
            "-inf\tinf\tnan\n");
        tester.Clear();
        tester.Desc(-inf).Desc(inf).Desc(nan);
        tester.TestCodec(
            "\\xAF\\xA7\\xA6\n"
            "-inf\tinf\tnan\n");
    }

    Y_UNIT_TEST(DisabledDoublesCodec) {
        TTester tester;
        Decode(tester, "o");

        //~ Cerr << tester.Raw.Str() << Endl;
        UNIT_ASSERT_NO_DIFF("Floating point numbers support was disabled on encoding", tester.Raw.Str());
    }
#else
    Y_UNIT_TEST(DisabledDoublesCodec) {
        TTester tester;
        tester.Asc(3.1415);
        tester.TestCodec(
            "o\n"
            "Floating point numbers support is disabled\n");
    }
#endif

    Y_UNIT_TEST(BasicStringsCodec) {
        TTester tester;
        tester.Asc("aaaa").Asc("aaaabbbbccccdddd");
        tester.TestCodec(
            "\\037aaaa\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\1\\4"
            "\\037aaaabbbbccccddd\\037d\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\1\\1\n"
            "aaaa\taaaabbbbccccdddd\n");
        tester.Clear();
        tester.Desc("aaaa").Desc("aaaabbbbccccdddd");
        tester.TestCodec(
            "\\xE0\\x9E\\x9E\\x9E\\x9E\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFE\\xFB"
            "\\xE0\\x9E\\x9E\\x9E\\x9E\\x9D\\x9D\\x9D\\x9D\\x9C\\x9C\\x9C\\x9C\\x9B\\x9B\\x9B"
            "\\xE0\\x9B\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFE\\xFE\n"
            "aaaa\taaaabbbbccccdddd\n");
    }

    Y_UNIT_TEST(LongIntsCodec) {
        TTester tester;
        tester.Asc(LL(1234567890123456789)).Asc(LL(-1234567890123456789));
        tester.TestCodec(
            "8\\x11\\\"\\x10\\xF4}\\xE9\\x81\\x15'\\xEE\\xDD\\xEF\\x0B\\x82\\x16~\\xEA\n"
            "1234567890123456789\t-1234567890123456789\n");
        tester.Clear();
        tester.Desc(1234567890123456789).Desc(-1234567890123456789);
        tester.TestCodec(
            "\\xC7\\xEE\\xDD\\xEF\\x0B\\x82\\x16~\\xEA\\xD8\\x11\\\"\\x10\\xF4}\\xE9\\x81\\x15\n"
            "1234567890123456789\t-1234567890123456789\n");
    }

    Y_UNIT_TEST(LongUnsignedIntsCodec) {
        TTester tester(true);
        tester.AscU(ULL(0xABCDEF1234567890));
        tester.TestCodec(
            "I\\0\\xAB\\xCD\\xEF\\0224Vx\\x90\n"
            "0xABCDEF1234567890\n");
        tester.Clear();
        tester.DescU(ULL(0xABCDEF1234567890));
        tester.TestCodec(
            "\\xB6\\xFFT2\\x10\\xED\\xCB\\xA9\\x87o\n"
            "0xABCDEF1234567890\n");
    }

    Y_UNIT_TEST(BasicOptionalsCodec) {
        TTester tester;
        tester.Asc(TMaybe<ui64>(1)).Asc(TMaybe<ui64>()).Asc(TMaybe<TStringBuf>("FOO")).Asc(TMaybe<TStringBuf>());
        tester.TestCodec(
            "sA\\1qs\\037FOO\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\1\\3q\n"
            "1\t[]\tFOO\t[]\n");
        tester.Clear();
        tester.Desc(TMaybe<ui64>(1)).Desc(TMaybe<ui64>()).Desc(TMaybe<TStringBuf>("FOO")).Desc(TMaybe<TStringBuf>());
        tester.TestCodec(
            "\\x8C\\xBE\\xFE\\x8E\\x8C\\xE0\\xB9\\xB0\\xB0\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFE\\xFC\\x8E\n"
            "1\t[]\tFOO\t[]\n");
    }

    Y_UNIT_TEST(BasicIntsOrder) {
        TTester tester;
        tester.Asc(1).Asc(0).AddRow();
        tester.Asc(0).Asc(-1).AddRow();
        tester.Asc(0).Asc(1).AddRow();

        tester.TestOrder("0\t-1\n0\t1\n1\t0\n");
        tester.Clear();

        tester.Desc(0).Desc(1).AddRow();
        tester.Desc(1).Desc(0).AddRow();
        tester.Desc(0).Desc(-1).AddRow();

        tester.TestOrder("1\t0\n0\t1\n0\t-1\n");
    }

#ifndef PRESORT_FP_DISABLED
    Y_UNIT_TEST(BasicDoublesOrder) {
        TTester tester;
        tester.Asc(-1.1).Asc(0.0).AddRow();
        tester.Asc(0.0).Asc(1.1).AddRow();
        tester.Asc(0.0).Asc(0.0).AddRow();

        tester.TestOrder("-1.1\t0\n0\t0\n0\t1.1\n");
        tester.Clear();

        tester.Desc(1.1).Desc(-1.0).AddRow();
        tester.Desc(1.1).Desc(0.0).AddRow();
        tester.Desc(1.0).Desc(0.0).AddRow();

        tester.TestOrder("1.1\t0\n1.1\t-1\n1\t0\n");
    }

    Y_UNIT_TEST(DoublesOrder) {
        TTester tester;

        const double den = std::numeric_limits<double>::denorm_min();
        const double inf = std::numeric_limits<double>::infinity();
        const double nan = std::sqrt(-1.0);

        tester.Asc(1.1).AddRow();
        tester.Asc(0.1).AddRow();
        tester.Asc(0.0).AddRow();
        tester.Asc(-0.1).AddRow();
        tester.Asc(-1.1).AddRow();
        tester.Asc(inf).AddRow();
        tester.Asc(-inf).AddRow();
        tester.Asc(nan).AddRow();
        tester.Asc(den).AddRow();
        tester.Asc(-den).AddRow();

        tester.TestOrder("-inf\n-1.1\n-0.1\n-4.940656458e-324\n0\n4.940656458e-324\n0.1\n1.1\ninf\nnan\n");
    }

    Y_UNIT_TEST(FloatsOrder) {
        TTester tester;

        const float a = 1.1;
        const float b = 0.1;
        const float z = 0.0;
        const float den = std::numeric_limits<float>::denorm_min();
        const float inf = std::numeric_limits<float>::infinity();
        const float nan = std::sqrt(-1.0);

        tester.Asc(a).AddRow();
        tester.Asc(b).AddRow();
        tester.Asc(z).AddRow();
        tester.Asc(-b).AddRow();
        tester.Asc(-a).AddRow();
        tester.Asc(inf).AddRow();
        tester.Asc(-inf).AddRow();
        tester.Asc(nan).AddRow();
        tester.Asc(den).AddRow();
        tester.Asc(-den).AddRow();

        tester.TestOrder("-inf\n-1.1\n-0.1\n-1.4013e-45\n0\n1.4013e-45\n0.1\n1.1\ninf\nnan\n");
    }
#endif

    Y_UNIT_TEST(BasicIntsMixedOrder) {
        TTester tester;
        tester.Asc(1).Desc(0).AddRow();
        tester.Asc(0).Desc(1).AddRow();
        tester.Asc(0).Desc(0).AddRow();

        tester.TestOrder("0\t1\n0\t0\n1\t0\n");
    }

    Y_UNIT_TEST(BasicStringsAndIntsOrder) {
        TTester tester;
        tester.Asc("foo").Desc(0).AddRow();
        tester.Asc("bar").Desc(1).AddRow();
        tester.Asc("foo").Desc(1).AddRow();

        tester.TestOrder("bar\t1\nfoo\t1\nfoo\t0\n");
    }

    Y_UNIT_TEST(LongIntsOrder) {
        TTester tester;
        tester.Asc(LL(1234567890123456789)).AddRow();
        tester.Asc(LL(-1234567890123456789)).AddRow();
        tester.TestOrder("-1234567890123456789\n1234567890123456789\n");
        tester.Clear();
        tester.Desc(-1234567890123456789).AddRow();
        tester.Desc(1234567890123456789).AddRow();
        tester.TestOrder("1234567890123456789\n-1234567890123456789\n");
    }

    Y_UNIT_TEST(LongUnsignedIntsOrder) {
        TTester tester(true);
        tester.AscU(ULL(0xABCDEF1234567890)).AddRow();
        tester.AscU(ULL(0xABCDEF1234567891)).AddRow();
        tester.TestOrder("0xABCDEF1234567890\n0xABCDEF1234567891\n");
        tester.Clear();
        tester.DescU(ULL(0xABCDEF1234567891)).AddRow();
        tester.DescU(ULL(0xABCDEF1234567890)).AddRow();
        tester.TestOrder("0xABCDEF1234567891\n0xABCDEF1234567890\n");
    }

    Y_UNIT_TEST(ZeroSuffixStringsOrder) {
        TTester tester;
        tester.Asc("foo").Asc(1).AddRow();
        tester.Asc("bar").Asc(0).AddRow();
        tester.AscS("foo\\0\\0").Asc(3).AddRow();
        tester.AscS("foo\\0").Asc(2).AddRow();

        tester.TestOrder("bar\t0\nfoo\t1\nfoo\\0\t2\nfoo\\0\\0\t3\n");
        tester.Clear();

        tester.Desc("foo").Asc(1).AddRow();
        tester.Desc("bar").Asc(0).AddRow();
        tester.DescS("foo\\0\\0").Asc(3).AddRow();
        tester.DescS("foo\\0").Asc(2).AddRow();

        tester.TestOrder("foo\\0\\0\t3\nfoo\\0\t2\nfoo\t1\nbar\t0\n");
    }

    Y_UNIT_TEST(SimpleStringsOrder) {
        TTester tester;
        tester.Asc("q").Asc(4).AddRow();
        tester.Asc("q").Asc(5).AddRow();
        tester.Asc("abc").Asc(1).AddRow();
        tester.Asc("ddd").Asc(3).AddRow();
        tester.Asc("ddd").Asc(2).AddRow();
        tester.Asc("qzz").Asc(6).AddRow();

        tester.TestOrder("abc\t1\nddd\t2\nddd\t3\nq\t4\nq\t5\nqzz\t6\n");
        tester.Clear();

        tester.Desc("q").Desc(4).AddRow();
        tester.Desc("q").Desc(5).AddRow();
        tester.Desc("abc").Desc(1).AddRow();
        tester.Desc("ddd").Desc(3).AddRow();
        tester.Desc("ddd").Desc(2).AddRow();
        tester.Desc("qzz").Desc(6).AddRow();

        tester.TestOrder("qzz\t6\nq\t5\nq\t4\nddd\t3\nddd\t2\nabc\t1\n");
    }

    Y_UNIT_TEST(SimpleOptionalsOrder) {
        TTester tester;
        tester.Asc(TMaybe<ui64>(1)).Asc(TMaybe<TStringBuf>()).AddRow();
        tester.Asc(TMaybe<ui64>()).Asc(TMaybe<TStringBuf>("FOO")).AddRow();
        tester.Asc(TMaybe<ui64>(1)).Asc(TMaybe<TStringBuf>("BAR")).AddRow();
        tester.Asc(TMaybe<ui64>(1)).Asc(TMaybe<TStringBuf>("")).AddRow();

        tester.TestOrder("[]\tFOO\n1\t[]\n1\t\n1\tBAR\n");
        tester.Clear();

        tester.Desc(TMaybe<ui64>(1)).Desc(TMaybe<TStringBuf>()).AddRow();
        tester.Desc(TMaybe<ui64>()).Desc(TMaybe<TStringBuf>("FOO")).AddRow();
        tester.Desc(TMaybe<ui64>(1)).Desc(TMaybe<TStringBuf>("BAR")).AddRow();
        tester.Desc(TMaybe<ui64>(1)).Desc(TMaybe<TStringBuf>("")).AddRow();

        tester.TestOrder("1\tBAR\n1\t\n1\t[]\n[]\tFOO\n");
    }
}
