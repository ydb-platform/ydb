#include <library/cpp/testing/unittest/gtest.h>

#include <library/cpp/type_info/type_info.h>

#include <util/generic/serialized_enum.h>

TEST(TypeList, PrimitiveTypeNameSequence) {
    auto primitiveNames = GetEnumAllValues<NTi::EPrimitiveTypeName>();

    ASSERT_GT(primitiveNames.size(), 0);

    ASSERT_EQ(static_cast<i32>(primitiveNames[0]), 0);

    for (size_t i = 0; i < primitiveNames.size() - 1; ++i) {
        ASSERT_EQ(static_cast<i32>(primitiveNames[i]) + 1, static_cast<i32>(primitiveNames[i + 1]));
    }
}

TEST(TypeList, PrimitiveTypeGroup) {
    auto primitiveNames = GetEnumAllValues<NTi::EPrimitiveTypeName>();

    for (auto typeName : primitiveNames) {
        ASSERT_TRUE(NTi::IsPrimitive(static_cast<NTi::ETypeName>(typeName)));
    }
}

TEST(TypeList, TypeNameInExactlyOneGroup) {
    auto allNames = GetEnumAllValues<NTi::ETypeName>();

    for (auto typeName : allNames) {
        int groups = 0;
        groups += NTi::IsPrimitive(typeName);
        groups += NTi::IsSingular(typeName);
        groups += NTi::IsContainer(typeName);
        ASSERT_EQ(groups, 1);
    }
}

TEST(TypeList, EnumCoherence) {
    auto primitiveNames = GetEnumAllValues<NTi::EPrimitiveTypeName>();
    auto allNames = GetEnumAllValues<NTi::ETypeName>();

    ASSERT_GT(allNames.size(), primitiveNames.size());

    size_t i = 0;

    for (; i < primitiveNames.size(); ++i) {
        ASSERT_EQ(static_cast<i32>(primitiveNames[i]), static_cast<i32>(allNames[i]));
        ASSERT_EQ(ToString(primitiveNames[i]), ToString(allNames[i]));
        ASSERT_TRUE(NTi::IsPrimitive(allNames[i]));
    }

    for (; i < allNames.size(); ++i) {
        ASSERT_FALSE(NTi::IsPrimitive(allNames[i]));
    }
}

TEST(TypeList, Cast) {
    auto primitiveNames = GetEnumAllValues<NTi::EPrimitiveTypeName>();

    for (auto typeName : primitiveNames) {
        ASSERT_EQ(typeName, NTi::ToPrimitiveTypeName(NTi::ToTypeName(typeName)));
    }
}
