#include <library/cpp/protobuf/util/is_equal.h>
#include <library/cpp/scheme/scheme.h>
#include <library/cpp/scheme/tests/ut/scheme_ut.pb.h>
#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(TSchemeProtoTest) {
    void DoTestProtobuf(bool fromProto, bool mapAsDict);

    Y_UNIT_TEST(TestFromProtobuf) {
        DoTestProtobuf(true, false);
    }

    Y_UNIT_TEST(TestToProtobuf) {
        DoTestProtobuf(false, false);
    }

    Y_UNIT_TEST(TestFromProtobufWithDict) {
        DoTestProtobuf(true, true);
    }

    Y_UNIT_TEST(TestToProtobufWithDict) {
        DoTestProtobuf(false, true);
    }

    template <class T>
    void AddMapPair(NSc::TValue& v, TStringBuf key, T value, bool mapAsDict) {
        if (mapAsDict) {
            v[key] = value;
        } else {
            auto& newElement = v.Push();
            newElement["key"] = key;
            newElement["value"] = value;
        }
    }

    void DoTestProtobuf(bool fromProto, bool mapAsDict) {
        NSc::TMessage m;
        NSc::TValue v;
        m.SetDouble((double)1 / 3), v["Double"] = (double)1 / 3;
        m.SetFloat((float)1 / 3), v["Float"] = (float)1 / 3;
        m.SetInt32(1000000), v["Int32"] = 1000000;
        m.SetInt64(1000000000000LL), v["Int64"] = 1000000000000LL;
        m.SetUInt32(555555), v["UInt32"] = 555555;
        m.SetUInt64(555555555555LL), v["UInt64"] = 555555555555LL;
        m.SetSInt32(-555555), v["SInt32"] = -555555;
        m.SetSInt64(-555555555555LL), v["SInt64"] = -555555555555LL;
        m.SetFixed32(123456), v["Fixed32"] = 123456;
        m.SetFixed64(123456123456LL), v["Fixed64"] = 123456123456LL;
        m.SetSFixed32(-123456), v["SFixed32"] = -123456;
        m.SetSFixed64(-123456123456LL), v["SFixed64"] = -123456123456LL;
        m.SetBool(true), v["Bool"] = true;
        m.SetString("String"), v["String"] = "String";
        m.SetBytes("Bytes"), v["Bytes"] = "Bytes";
        m.SetEnum(NSc::VALUE1), v["Enum"] = "VALUE1";

        auto& mapDoublesP = *m.mutable_mapdoubles();
        auto& mapDoublesV = v["MapDoubles"];
        mapDoublesP["pi"] = 3.14;
        AddMapPair(mapDoublesV, "pi", 3.14, mapAsDict);
        if (fromProto && mapAsDict) {
            // can not add two entries in dict, as it represent over array with unstable order
            mapDoublesP["back_e"] = 1 / 2.7;
            AddMapPair(mapDoublesV, "back_e", 1 / 2.7, mapAsDict);
        }

        auto& mapInt32sP = *m.mutable_mapint32s();
        auto& mapInt32sV = v["MapInt32s"];
        mapInt32sP["pi"] = -7;
        AddMapPair(mapInt32sV, "pi", -7, mapAsDict);
        if (fromProto && mapAsDict) {
            // can not add two entries in dict, as it represent over array with unstable order
            mapInt32sP["back_e"] = 42;
            AddMapPair(mapInt32sV, "back_e", 42, mapAsDict);
        }

        auto& mapStringP = *m.mutable_mapstring();
        auto& mapStringV = v["MapString"];
        mapStringP["intro"] = "body";
        AddMapPair(mapStringV, "intro", "body", mapAsDict);
        if (fromProto && mapAsDict) {
            // can not add two entries in dict, as it represent over array with unstable order
            mapStringP["deep"] = "blue";
            AddMapPair(mapStringV, "deep", "blue", mapAsDict);
        }

        m.AddDoubles((double)1 / 3), v["Doubles"][0] = (double)1 / 3;
        m.AddDoubles((double)1 / 4), v["Doubles"][1] = (double)1 / 4;

        m.AddFloats((float)1 / 3), v["Floats"][0] = (float)1 / 3;
        m.AddFloats((float)1 / 4), v["Floats"][1] = (float)1 / 4;

        m.AddInt32s(1000000), v["Int32s"][0] = 1000000;
        m.AddInt32s(2000000), v["Int32s"][1] = 2000000;

        m.AddInt64s(1000000000000LL), v["Int64s"][0] = 1000000000000LL;
        m.AddInt64s(2000000000000LL), v["Int64s"][1] = 2000000000000LL;

        m.AddUInt32s(555555), v["UInt32s"][0] = 555555;
        m.AddUInt32s(655555), v["UInt32s"][1] = 655555;

        m.AddUInt64s(555555555555LL);
        v["UInt64s"][0] = 555555555555LL;
        m.AddUInt64s(655555555555LL);
        v["UInt64s"][1] = 655555555555LL;

        m.AddSInt32s(-555555), v["SInt32s"][0] = -555555;
        m.AddSInt32s(-655555), v["SInt32s"][1] = -655555;

        m.AddSInt64s(-555555555555LL), v["SInt64s"][0] = -555555555555LL;
        m.AddSInt64s(-655555555555LL), v["SInt64s"][1] = -655555555555LL;

        m.AddFixed32s(123456), v["Fixed32s"][0] = 123456;
        m.AddFixed32s(223456), v["Fixed32s"][1] = 223456;

        m.AddFixed64s(123456123456LL), v["Fixed64s"][0] = 123456123456LL;
        m.AddFixed64s(223456123456LL), v["Fixed64s"][1] = 223456123456LL;

        m.AddSFixed32s(-123456), v["SFixed32s"][0] = -123456;
        m.AddSFixed32s(-223456), v["SFixed32s"][1] = -223456;

        m.AddSFixed64s(-123456123456LL), v["SFixed64s"][0] = -123456123456LL;
        m.AddSFixed64s(-223456123456LL), v["SFixed64s"][1] = -223456123456LL;

        m.AddBools(false), v["Bools"][0] = false;
        m.AddBools(true), v["Bools"][1] = true;

        m.AddStrings("String1"), v["Strings"][0] = "String1";
        m.AddStrings("String2"), v["Strings"][1] = "String2";

        m.AddBytess("Bytes1"), v["Bytess"][0] = "Bytes1";
        m.AddBytess("Bytes2"), v["Bytess"][1] = "Bytes2";

        m.AddEnums(NSc::VALUE1), v["Enums"][0] = "VALUE1";
        m.AddEnums(NSc::VALUE2), v["Enums"][1] = "VALUE2";

        NSc::TMessage2 m2;
        NSc::TValue v2;
        m2.SetDouble((double)1 / 3), v2["Double"] = (double)1 / 3;
        m2.SetFloat((float)1 / 3), v2["Float"] = (float)1 / 3;
        m2.SetInt32(1000000), v2["Int32"] = 1000000;
        m2.SetInt64(1000000000000LL), v2["Int64"] = 1000000000000LL;
        m2.SetUInt32(555555), v2["UInt32"] = 555555;
        m2.SetUInt64(555555555555LL), v2["UInt64"] = 555555555555LL;
        m2.SetSInt32(-555555), v2["SInt32"] = -555555;
        m2.SetSInt64(-555555555555LL), v2["SInt64"] = -555555555555LL;
        m2.SetFixed32(123456), v2["Fixed32"] = 123456;
        m2.SetFixed64(123456123456LL), v2["Fixed64"] = 123456123456LL;
        m2.SetSFixed32(-123456), v2["SFixed32"] = -123456;
        m2.SetSFixed64(-123456123456LL), v2["SFixed64"] = -123456123456LL;
        m2.SetBool(true), v2["Bool"] = true;
        m2.SetString("String"), v2["String"] = "String";
        m2.SetBytes("Bytes"), v2["Bytes"] = "Bytes";
        m2.SetEnum(NSc::VALUE1), v2["Enum"] = "VALUE1";

        m2.AddDoubles((double)1 / 3), v2["Doubles"][0] = (double)1 / 3;
        m2.AddDoubles((double)1 / 4), v2["Doubles"][1] = (double)1 / 4;

        m2.AddFloats((float)1 / 3), v2["Floats"][0] = (float)1 / 3;
        m2.AddFloats((float)1 / 4), v2["Floats"][1] = (float)1 / 4;

        m2.AddInt32s(1000000), v2["Int32s"][0] = 1000000;
        m2.AddInt32s(2000000), v2["Int32s"][1] = 2000000;

        m2.AddInt64s(1000000000000LL), v2["Int64s"][0] = 1000000000000LL;
        m2.AddInt64s(2000000000000LL), v2["Int64s"][1] = 2000000000000LL;

        m2.AddUInt32s(555555), v2["UInt32s"][0] = 555555;
        m2.AddUInt32s(655555), v2["UInt32s"][1] = 655555;

        m2.AddUInt64s(555555555555LL);
        v2["UInt64s"][0] = 555555555555LL;
        m2.AddUInt64s(655555555555LL);
        v2["UInt64s"][1] = 655555555555LL;

        m2.AddSInt32s(-555555), v2["SInt32s"][0] = -555555;
        m2.AddSInt32s(-655555), v2["SInt32s"][1] = -655555;

        m2.AddSInt64s(-555555555555LL), v2["SInt64s"][0] = -555555555555LL;
        m2.AddSInt64s(-655555555555LL), v2["SInt64s"][1] = -655555555555LL;

        m2.AddFixed32s(123456), v2["Fixed32s"][0] = 123456;
        m2.AddFixed32s(223456), v2["Fixed32s"][1] = 223456;

        m2.AddFixed64s(123456123456LL), v2["Fixed64s"][0] = 123456123456LL;
        m2.AddFixed64s(223456123456LL), v2["Fixed64s"][1] = 223456123456LL;

        m2.AddSFixed32s(-123456), v2["SFixed32s"][0] = -123456;
        m2.AddSFixed32s(-223456), v2["SFixed32s"][1] = -223456;

        m2.AddSFixed64s(-123456123456LL), v2["SFixed64s"][0] = -123456123456LL;
        m2.AddSFixed64s(-223456123456LL), v2["SFixed64s"][1] = -223456123456LL;

        m2.AddBools(false), v2["Bools"][0] = false;
        m2.AddBools(true), v2["Bools"][1] = true;

        m2.AddStrings("String1"), v2["Strings"][0] = "String1";
        m2.AddStrings("String2"), v2["Strings"][1] = "String2";

        m2.AddBytess("Bytes1"), v2["Bytess"][0] = "Bytes1";
        m2.AddBytess("Bytes2"), v2["Bytess"][1] = "Bytes2";

        m2.AddEnums(NSc::VALUE1), v2["Enums"][0] = "VALUE1";
        m2.AddEnums(NSc::VALUE2), v2["Enums"][1] = "VALUE2";

        *(m.MutableMessage()) = m2, v["Message"] = v2;

        *(m.AddMessages()) = m2, v["Messages"][0] = v2;
        *(m.AddMessages()) = m2, v["Messages"][1] = v2;

        if (fromProto) {
            UNIT_ASSERT(NSc::TValue::Equal(v, NSc::TValue::From(m, mapAsDict)));
        } else {
            NSc::TMessage proto;
            v.To(proto);

            TString differentPath;
            UNIT_ASSERT_C(NProtoBuf::IsEqual(m, proto, &differentPath), differentPath);
        }
    }
}
