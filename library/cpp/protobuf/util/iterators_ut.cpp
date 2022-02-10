#include "iterators.h"
#include "simple_reflection.h"
#include <library/cpp/protobuf/util/ut/common_ut.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>

using NProtoBuf::TFieldsIterator;
using NProtoBuf::TConstField;

Y_UNIT_TEST_SUITE(Iterators) {
    Y_UNIT_TEST(Count) {
        const NProtobufUtilUt::TWalkTest proto;
        const NProtoBuf::Descriptor* d = proto.GetDescriptor();
        TFieldsIterator dbegin(d), dend(d, d->field_count());
        size_t steps = 0;

        UNIT_ASSERT_EQUAL(dbegin, begin(*d));
        UNIT_ASSERT_EQUAL(dend, end(*d));

        for (; dbegin != dend; ++dbegin)
            ++steps;
        UNIT_ASSERT_VALUES_EQUAL(steps, d->field_count());
    }

    Y_UNIT_TEST(RangeFor) {
        size_t steps = 0, values = 0;
        NProtobufUtilUt::TWalkTest proto;
        proto.SetOptStr("yandex");
        for (const auto& field : *proto.GetDescriptor()) {
            values += TConstField(proto, field).HasValue();
            ++steps;
        }
        UNIT_ASSERT_VALUES_EQUAL(steps, proto.GetDescriptor()->field_count());
        UNIT_ASSERT_VALUES_EQUAL(values, 1);
    }

    Y_UNIT_TEST(AnyOf) {
        NProtobufUtilUt::TWalkTest proto;
        const NProtoBuf::Descriptor* d = proto.GetDescriptor();
        TFieldsIterator begin(d), end(d, d->field_count());
        UNIT_ASSERT(!AnyOf(begin, end, [&proto](const NProtoBuf::FieldDescriptor* f){
            return TConstField(proto, f).HasValue();
        }));

        proto.SetOptStr("yandex");
        UNIT_ASSERT(AnyOf(begin, end, [&proto](const NProtoBuf::FieldDescriptor* f){
            return TConstField(proto, f).HasValue();
        }));
    }
}
