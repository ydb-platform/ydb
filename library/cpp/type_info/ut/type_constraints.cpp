#include <library/cpp/testing/unittest/gtest.h>

#include <library/cpp/type_info/type_info.h>

#include "utils.h"

TEST_TF(TypeConstraints, DecimalScale) {
    UNIT_ASSERT_EXCEPTION_CONTAINS([&f]() {
        f.Decimal(20, 21);
    }(),
                                   NTi::TIllegalTypeException, "decimal scale 21 should be no greater than decimal precision 20");
}

TEST_TF(TypeConstraints, StructDuplicateItem) {
    UNIT_ASSERT_EXCEPTION_CONTAINS([&f]() {
        f.Struct({{"a", f.Void()}, {"a", f.String()}});
    }(),
                                   NTi::TIllegalTypeException, "duplicate struct item 'a'");

    UNIT_ASSERT_EXCEPTION_CONTAINS([&f]() {
        f.Struct({{"a", f.Void()}, {"b", f.Bool()}, {"a", f.String()}});
    }(),
                                   NTi::TIllegalTypeException, "duplicate struct item 'a'");
}

TEST_TF(TypeConstraints, StructEmpty) {
    f.Struct({}); // empty structs are ok, this should not fail
}

TEST_TF(TypeConstraints, TupleEmpty) {
    f.Tuple({}); // empty tuples are ok, this should not fail
}

TEST_TF(TypeConstraints, VariantStructEmpty) {
    UNIT_ASSERT_EXCEPTION_CONTAINS([&f]() {
        f.Variant(f.Struct({}));
    }(),
                                   NTi::TIllegalTypeException, "variant should contain at least one alternative");
}

TEST_TF(TypeConstraints, VariantTupleEmpty) {
    UNIT_ASSERT_EXCEPTION_CONTAINS([&f]() {
        f.Variant(f.Tuple({}));
    }(),
                                   NTi::TIllegalTypeException, "variant should contain at least one alternative");
}

TEST_TF(TypeConstraints, VariantWrongInnerType) {
    UNIT_ASSERT_EXCEPTION_CONTAINS([&f]() {
        f.Variant(f.String());
    }(),
                                   NTi::TIllegalTypeException, "variants can only contain structs and tuples, got String instead");
}
