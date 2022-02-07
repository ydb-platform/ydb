#include "walk.h"
#include "simple_reflection.h"
#include <library/cpp/protobuf/util/ut/common_ut.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NProtoBuf;

Y_UNIT_TEST_SUITE(ProtobufWalk) {
    static void InitProto(NProtobufUtilUt::TWalkTest & p, int level = 0) {
        p.SetOptInt(1);
        p.AddRepInt(2);
        p.AddRepInt(3);

        p.SetOptStr("123");
        p.AddRepStr("*");
        p.AddRepStr("abcdef");
        p.AddRepStr("1234");

        if (level == 0) {
            InitProto(*p.MutableOptSub(), 1);
            InitProto(*p.AddRepSub(), 1);
            InitProto(*p.AddRepSub(), 1);
        }
    }

    static bool IncreaseInts(Message & msg, const FieldDescriptor* fd) {
        TMutableField f(msg, fd);
        if (f.IsInstance<ui32>()) {
            for (size_t i = 0; i < f.Size(); ++i)
                f.Set(f.Get<ui64>(i) + 1, i); // ui64 should be ok!
        }
        return true;
    }

    static bool RepeatString1(Message & msg, const FieldDescriptor* fd) {
        TMutableField f(msg, fd);
        if (f.IsString()) {
            for (size_t i = 0; i < f.Size(); ++i)
                if (f.Get<TString>(i).StartsWith('1'))
                    f.Set(f.Get<TString>(i) + f.Get<TString>(i), i);
        }
        return true;
    }

    static bool ClearXXX(Message & msg, const FieldDescriptor* fd) {
        const FieldOptions& opt = fd->options();
        if (opt.HasExtension(NProtobufUtilUt::XXX) && opt.GetExtension(NProtobufUtilUt::XXX))
            TMutableField(msg, fd).Clear();

        return true;
    }

    struct TestStruct {
        bool Ok = false;
        
        TestStruct() = default;
        bool operator()(Message&, const FieldDescriptor*) {
            Ok = true;
            return false;
        }
    };

    Y_UNIT_TEST(TestWalkRefl) {
        NProtobufUtilUt::TWalkTest p;
        InitProto(p);

        {
            UNIT_ASSERT_EQUAL(p.GetOptInt(), 1);
            UNIT_ASSERT_EQUAL(p.RepIntSize(), 2);
            UNIT_ASSERT_EQUAL(p.GetRepInt(0), 2);
            UNIT_ASSERT_EQUAL(p.GetRepInt(1), 3);

            WalkReflection(p, IncreaseInts);

            UNIT_ASSERT_EQUAL(p.GetOptInt(), 2);
            UNIT_ASSERT_EQUAL(p.RepIntSize(), 2);
            UNIT_ASSERT_EQUAL(p.GetRepInt(0), 3);
            UNIT_ASSERT_EQUAL(p.GetRepInt(1), 4);

            UNIT_ASSERT_EQUAL(p.GetOptSub().GetOptInt(), 2);
            UNIT_ASSERT_EQUAL(p.GetOptSub().RepIntSize(), 2);
            UNIT_ASSERT_EQUAL(p.GetOptSub().GetRepInt(0), 3);
            UNIT_ASSERT_EQUAL(p.GetOptSub().GetRepInt(1), 4);

            UNIT_ASSERT_EQUAL(p.RepSubSize(), 2);
            UNIT_ASSERT_EQUAL(p.GetRepSub(1).GetOptInt(), 2);
            UNIT_ASSERT_EQUAL(p.GetRepSub(1).RepIntSize(), 2);
            UNIT_ASSERT_EQUAL(p.GetRepSub(1).GetRepInt(0), 3);
            UNIT_ASSERT_EQUAL(p.GetRepSub(1).GetRepInt(1), 4);
        }
        {
            UNIT_ASSERT_EQUAL(p.GetOptStr(), "123");
            UNIT_ASSERT_EQUAL(p.GetRepStr(2), "1234");

            WalkReflection(p, RepeatString1);

            UNIT_ASSERT_EQUAL(p.GetOptStr(), "123123");
            UNIT_ASSERT_EQUAL(p.RepStrSize(), 3);
            UNIT_ASSERT_EQUAL(p.GetRepStr(0), "*");
            UNIT_ASSERT_EQUAL(p.GetRepStr(1), "abcdef");
            UNIT_ASSERT_EQUAL(p.GetRepStr(2), "12341234");

            UNIT_ASSERT_EQUAL(p.RepSubSize(), 2);
            UNIT_ASSERT_EQUAL(p.GetRepSub(0).GetOptStr(), "123123");
            UNIT_ASSERT_EQUAL(p.GetRepSub(0).RepStrSize(), 3);
            UNIT_ASSERT_EQUAL(p.GetRepSub(0).GetRepStr(0), "*");
            UNIT_ASSERT_EQUAL(p.GetRepSub(0).GetRepStr(1), "abcdef");
            UNIT_ASSERT_EQUAL(p.GetRepSub(0).GetRepStr(2), "12341234");
        }
        {
            UNIT_ASSERT(p.HasOptInt());
            UNIT_ASSERT(p.RepStrSize() == 3);
            UNIT_ASSERT(p.HasOptSub());

            WalkReflection(p, ClearXXX);

            UNIT_ASSERT(!p.HasOptInt());
            UNIT_ASSERT(p.RepIntSize() == 2);
            UNIT_ASSERT(p.HasOptStr());
            UNIT_ASSERT(p.RepStrSize() == 0);
            UNIT_ASSERT(!p.HasOptSub());
            UNIT_ASSERT(p.RepSubSize() == 2);
        }
    }

    Y_UNIT_TEST(TestMutableCallable) {
        TestStruct testStruct;
        NProtobufUtilUt::TWalkTest p;
        InitProto(p);

        WalkReflection(p, testStruct);
        UNIT_ASSERT(testStruct.Ok);
    }

    Y_UNIT_TEST(TestWalkDescr) {
        NProtobufUtilUt::TWalkTestCyclic p;

        TStringBuilder printedSchema;
        auto func = [&](const FieldDescriptor* desc) mutable {
            printedSchema << desc->DebugString();
            return true;
        };
        WalkSchema(p.GetDescriptor(), func);

        TString schema = 
            "optional .NProtobufUtilUt.TWalkTestCyclic.TNested OptNested = 1;\n"
            "optional uint32 OptInt32 = 1;\n"
            "optional .NProtobufUtilUt.TWalkTestCyclic OptSubNested = 2;\n"
            "repeated string RepStr = 3;\n"
            "optional .NProtobufUtilUt.TWalkTestCyclic.TNested OptNested = 4;\n"
            "repeated uint64 OptInt64 = 2;\n"
            "optional .NProtobufUtilUt.TWalkTestCyclic OptSub = 3;\n"
            "optional .NProtobufUtilUt.TWalkTestCyclic.TEnum OptEnum = 4;\n";
        
        UNIT_ASSERT_STRINGS_EQUAL(printedSchema, schema);
    }
}
