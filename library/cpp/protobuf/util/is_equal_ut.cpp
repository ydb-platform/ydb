#include "is_equal.h"
#include <library/cpp/protobuf/util/ut/sample_for_is_equal.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/descriptor.h>

Y_UNIT_TEST_SUITE(ProtobufIsEqual) {
    const ::google::protobuf::Descriptor* Descr = TSampleForIsEqual::descriptor();
    const ::google::protobuf::FieldDescriptor* NameDescr = Descr->field(0);
    const ::google::protobuf::FieldDescriptor* InnerDescr = Descr->field(1);

    Y_UNIT_TEST(CheckDescriptors) {
        UNIT_ASSERT(Descr);
        UNIT_ASSERT(NameDescr);
        UNIT_ASSERT_VALUES_EQUAL(NameDescr->name(), "Name");
        UNIT_ASSERT_VALUES_EQUAL(InnerDescr->name(), "Inner");
    }

    Y_UNIT_TEST(IsEqual1) {
        TSampleForIsEqual a;
        TSampleForIsEqual b;

        a.SetName("aaa");
        b.SetName("bbb");

        TString path;

        bool equal = NProtoBuf::IsEqual(a, b, &path);
        UNIT_ASSERT(!equal);
        UNIT_ASSERT_VALUES_EQUAL("Name", path);

        UNIT_ASSERT(!NProtoBuf::IsEqualField(a, b, *NameDescr));
    }

    Y_UNIT_TEST(IsEqual2) {
        TSampleForIsEqual a;
        TSampleForIsEqual b;

        a.MutableInner()->SetBrbrbr("aaa");
        b.MutableInner()->SetBrbrbr("bbb");

        TString path;

        bool equal = NProtoBuf::IsEqual(a, b, &path);
        UNIT_ASSERT(!equal);
        UNIT_ASSERT_VALUES_EQUAL("Inner/Brbrbr", path);

        bool equalField = NProtoBuf::IsEqualField(a, b, *InnerDescr);
        UNIT_ASSERT(!equalField);
    }

    Y_UNIT_TEST(IsEqual3) {
        TSampleForIsEqual a;
        TSampleForIsEqual b;

        a.SetName("aaa");
        a.MutableInner()->SetBrbrbr("bbb");

        b.SetName("aaa");
        b.MutableInner()->SetBrbrbr("bbb");

        TString path;

        UNIT_ASSERT(NProtoBuf::IsEqual(a, b));
        UNIT_ASSERT(NProtoBuf::IsEqualField(a, b, *NameDescr));
        UNIT_ASSERT(NProtoBuf::IsEqualField(a, b, *InnerDescr));

        b.MutableInner()->SetBrbrbr("ccc");
        UNIT_ASSERT(!NProtoBuf::IsEqual(a, b));
        UNIT_ASSERT(!NProtoBuf::IsEqualField(a, b, *InnerDescr));

        b.SetName("ccc");
        UNIT_ASSERT(!NProtoBuf::IsEqualField(a, b, *NameDescr));
    }

    Y_UNIT_TEST(IsEqualDefault) {
        TSampleForIsEqual a;
        TSampleForIsEqual b;

        a.SetName("");
        UNIT_ASSERT(NProtoBuf::IsEqualDefault(a, b));
        UNIT_ASSERT(!NProtoBuf::IsEqual(a, b));

        UNIT_ASSERT(!NProtoBuf::IsEqualField(a, b, *NameDescr));
        UNIT_ASSERT(NProtoBuf::IsEqualFieldDefault(a, b, *NameDescr));
    }
}
