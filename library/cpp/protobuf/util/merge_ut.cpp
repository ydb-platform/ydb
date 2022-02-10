#include "merge.h"
#include <library/cpp/protobuf/util/ut/common_ut.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NProtoBuf;

Y_UNIT_TEST_SUITE(ProtobufMerge) {
    static void InitProto(NProtobufUtilUt::TMergeTest & p, bool isSrc) {
        size_t start = isSrc ? 0 : 100;

        p.AddMergeInt(start + 1);
        p.AddMergeInt(start + 2);

        p.AddNoMergeInt(start + 3);
        p.AddNoMergeInt(start + 4);

        NProtobufUtilUt::TMergeTestMerge* m = p.MutableMergeSub();
        m->SetA(start + 5);
        m->AddB(start + 6);
        m->AddB(start + 7);
        m->AddC(start + 14);

        if (!isSrc) {
            // only for dst
            NProtobufUtilUt::TMergeTestMerge* mm1 = p.AddNoMergeRepSub();
            mm1->SetA(start + 8);
            mm1->AddB(start + 9);
            mm1->AddB(start + 10);
        }

        NProtobufUtilUt::TMergeTestNoMerge* mm3 = p.MutableNoMergeOptSub();
        mm3->SetA(start + 11);
        mm3->AddB(start + 12);
        mm3->AddB(start + 13);
    }

    Y_UNIT_TEST(CustomMerge) {
        NProtobufUtilUt::TMergeTest src, dst;
        InitProto(src, true);
        InitProto(dst, false);

        //        Cerr << "\nsrc: " << src.ShortDebugString() << Endl;
        //        Cerr << "dst: " << dst.ShortDebugString() << Endl;
        NProtoBuf::CustomMerge(src, dst);
        //        Cerr << "dst2:" << dst.ShortDebugString() << Endl;

        // repeated uint32 MergeInt   = 1;
        UNIT_ASSERT_EQUAL(dst.MergeIntSize(), 4);
        UNIT_ASSERT_EQUAL(dst.GetMergeInt(0), 101);
        UNIT_ASSERT_EQUAL(dst.GetMergeInt(1), 102);
        UNIT_ASSERT_EQUAL(dst.GetMergeInt(2), 1);
        UNIT_ASSERT_EQUAL(dst.GetMergeInt(3), 2);

        // repeated uint32 NoMergeInt = 2 [(DontMergeField)=true];
        UNIT_ASSERT_EQUAL(dst.NoMergeIntSize(), 2);
        UNIT_ASSERT_EQUAL(dst.GetNoMergeInt(0), 3);
        UNIT_ASSERT_EQUAL(dst.GetNoMergeInt(1), 4);

        // optional TMergeTestMerge MergeSub      = 3;
        UNIT_ASSERT_EQUAL(dst.GetMergeSub().GetA(), 5);
        UNIT_ASSERT_EQUAL(dst.GetMergeSub().BSize(), 4);
        UNIT_ASSERT_EQUAL(dst.GetMergeSub().GetB(0), 106);
        UNIT_ASSERT_EQUAL(dst.GetMergeSub().GetB(1), 107);
        UNIT_ASSERT_EQUAL(dst.GetMergeSub().GetB(2), 6);
        UNIT_ASSERT_EQUAL(dst.GetMergeSub().GetB(3), 7);
        UNIT_ASSERT_EQUAL(dst.GetMergeSub().CSize(), 1);
        UNIT_ASSERT_EQUAL(dst.GetMergeSub().GetC(0), 14);

        // repeated TMergeTestMerge NoMergeRepSub = 4 [(DontMergeField)=true];
        UNIT_ASSERT_EQUAL(dst.NoMergeRepSubSize(), 1);
        UNIT_ASSERT_EQUAL(dst.GetNoMergeRepSub(0).GetA(), 108);
        UNIT_ASSERT_EQUAL(dst.GetNoMergeRepSub(0).BSize(), 2);
        UNIT_ASSERT_EQUAL(dst.GetNoMergeRepSub(0).GetB(0), 109);
        UNIT_ASSERT_EQUAL(dst.GetNoMergeRepSub(0).GetB(1), 110);

        // optional TMergeTestNoMerge NoMergeOptSub  = 5;
        UNIT_ASSERT_EQUAL(dst.GetNoMergeOptSub().GetA(), 11);
        UNIT_ASSERT_EQUAL(dst.GetNoMergeOptSub().BSize(), 2);
        UNIT_ASSERT_EQUAL(dst.GetNoMergeOptSub().GetB(0), 12);
        UNIT_ASSERT_EQUAL(dst.GetNoMergeOptSub().GetB(1), 13);
    }
}
