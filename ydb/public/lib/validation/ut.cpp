#include <ydb/public/lib/validation/ut/protos/validation_test.pb.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(ValidationTests) {
    using namespace Ydb::ValidationTest;

    template <typename T>
    bool Validate(const T& proto, TString& error) {
        return proto.validate(error);
    }

    template <typename T>
    bool Validate(const T& proto) {
        TString unused;
        return Validate(proto, unused);
    }

    template <typename T>
    void Validate(const T& proto, const TString& errorContains) {
        TString error;
        UNIT_ASSERT(!Validate(proto, error));
        UNIT_ASSERT_STRING_CONTAINS(error, errorContains);
    }

    Y_UNIT_TEST(Empty) {
        UNIT_ASSERT(Validate(EmptyMessage()));
    }

    Y_UNIT_TEST(WithouValidators) {
        UNIT_ASSERT(Validate(MessageWithoutValidators()));
    }

    Y_UNIT_TEST(Basic) {
        MessageBasic basic;

        // required
        {
            Validate(basic, "required but not set");
            basic.set_check_required("set");
        }

        // size: range
        {
            Validate(basic, "size is not in");
            basic.mutable_check_size_range_1_2()->Add("string");
            basic.mutable_check_size_range_1_2()->Add("string");
            basic.mutable_check_size_range_1_2()->Add("string"); // extra

            Validate(basic, "size is not in");
            basic.mutable_check_size_range_1_2()->RemoveLast();
        }

        // size: lt
        {
            basic.mutable_check_size_lt_2()->Add("1");
            basic.mutable_check_size_lt_2()->Add("2"); // extra
            Validate(basic, "size is not <");
            basic.mutable_check_size_lt_2()->RemoveLast();
        }

        // size: le
        {
            basic.mutable_check_size_le_1()->Add("1");
            basic.mutable_check_size_le_1()->Add("2"); // extra
            Validate(basic, "size is not <=");
            basic.mutable_check_size_le_1()->RemoveLast();
        }

        // size: eq
        {
            basic.mutable_check_size_eq_1()->Add("1");
            basic.mutable_check_size_eq_1()->Add("2"); // extra
            Validate(basic, "size is not ==");
            basic.mutable_check_size_eq_1()->RemoveLast();
        }

        // size: ge
        {
            Validate(basic, "size is not >=");
            basic.mutable_check_size_ge_1()->Add("1");
        }

        // size: gt
        {
            basic.mutable_check_size_gt_1()->Add("1");
            Validate(basic, "size is not >");
            basic.mutable_check_size_gt_1()->Add("1");
        }

        // length: range
        {
            Validate(basic, "length is not in");
            basic.set_check_length_range_1_3("value");

            Validate(basic, "length is not in");
            basic.set_check_length_range_1_3("val");
        }

        // length: lt
        {
            basic.set_check_length_lt_4("value");
            Validate(basic, "length is not <");
            basic.set_check_length_lt_4("val");
        }

        // length: le
        {
            basic.set_check_length_le_3("value");
            Validate(basic, "length is not <=");
            basic.set_check_length_le_3("val");
        }

        // length: eq
        {
            basic.set_check_length_eq_3("value");
            Validate(basic, "length is not ==");
            basic.set_check_length_eq_3("val");
        }

        // length: ge
        {
            basic.set_check_length_ge_3("v");
            Validate(basic, "length is not >=");
            basic.set_check_length_ge_3("val");
        }

        // length: gt
        {
            basic.set_check_length_gt_3("val");
            Validate(basic, "length is not >");
            basic.set_check_length_gt_3("value");
        }

        // value: int32
        {
            basic.set_check_value_int32(-1);
            Validate(basic, "value is not");
            basic.set_check_value_int32(-2);
        }

        // value: int64
        {
            basic.set_check_value_int64(-5'000'000'000);
            Validate(basic, "value is not");
            basic.set_check_value_int64(-5'000'000'001);
        }

        // value: uint32
        {
            basic.set_check_value_uint32(1);
            Validate(basic, "value is not");
            basic.set_check_value_uint32(2);
        }

        // value: uint64
        {
            basic.set_check_value_uint64(5'000'000'000);
            Validate(basic, "value is not");
            basic.set_check_value_uint64(5'000'000'001);
        }

        // value: double
        {
            basic.set_check_value_double(0.99);
            Validate(basic, "value is not");
            basic.set_check_value_double(1.01);
        }

        // value: float
        {
            basic.set_check_value_float(-0.99);
            Validate(basic, "value is not");
            basic.set_check_value_float(-1.01);
        }

        // value: bool
        {
            basic.set_check_value_bool(false);
            Validate(basic, "value is not");
            basic.set_check_value_bool(true);
        }

        // value: string
        {
            basic.set_check_value_string("changeme");
            Validate(basic, "value is not");
            basic.set_check_value_string("changed");
        }

        // value: range [1;10]
        {
            basic.set_check_value_range_1(11);
            Validate(basic, "value is not");
            basic.set_check_value_range_1(10);
        }

        // value: range (1;10]
        {
            basic.set_check_value_range_2(1);
            Validate(basic, "value is not");
            basic.set_check_value_range_2(2);
        }

        // value: range [1;10)
        {
            basic.set_check_value_range_3(10);
            Validate(basic, "value is not");
            basic.set_check_value_range_3(9);
        }

        // value: range (1;10)
        {
            basic.set_check_value_range_4(1);
            Validate(basic, "value is not");
            basic.set_check_value_range_4(5);
        }

        UNIT_ASSERT(Validate(basic));
    }

    Y_UNIT_TEST(Map) {
        MessageMap map;

        // size
        {
            Validate(map, "size is not >");
            map.mutable_values()->insert({"key", "value"});
        }

        // value's length
        {
            Validate(map, "value length is not <");
            map.mutable_values()->at("key") = "v";
        }

        // key's length
        {
            Validate(map, "key length is not <");
            map.mutable_values()->erase("key");
            map.mutable_values()->insert({"k", "v"});
        }

        // key
        {
            Validate(map, "key is not");
            map.mutable_values()->erase("k");
            map.mutable_values()->insert({"a", "v"});
        }

        // value
        {
            Validate(map, "value is not");
            map.mutable_values()->erase("a");
            map.mutable_values()->insert({"a", "b"});
        }

        UNIT_ASSERT(Validate(map));
    }

    Y_UNIT_TEST(Oneof) {
        MessageOneof oneof;

        // required
        {
            Validate(oneof, "required but not set");
            oneof.set_check_required("set");
        }

        // none of oneof values are set
        UNIT_ASSERT(Validate(oneof));

        // check oneof (1)
        {
            oneof.set_check_ge_1("");
            Validate(oneof, "length is not >=");

            oneof.set_check_ge_1("22");
            UNIT_ASSERT(Validate(oneof));
        }

        // check oneof (2)
        {
            oneof.set_check_ge_2("1");
            Validate(oneof, "length is not >=");

            oneof.set_check_ge_2("333");
            UNIT_ASSERT(Validate(oneof));
        }
    }

    Y_UNIT_TEST(Optional) {
        MessageOptional optional;

        {
            Validate(optional, "required but not set");
            optional.set_check_required(10);
        }

        // empty is correct
        UNIT_ASSERT(Validate(optional));

        // incorrect value
        {
            optional.set_check_value_int32(0);
            Validate(optional, "value is not");
        }

        // correct value
        {
            optional.set_check_value_int32(1);
            UNIT_ASSERT(Validate(optional));
        }

        // incorrect length
        {

            optional.set_check_length("abacaba");
            Validate(optional, "length is not in");
        }

        // correct length
        {
            optional.set_check_length("val");
            UNIT_ASSERT(Validate(optional));
        }
    }
}
