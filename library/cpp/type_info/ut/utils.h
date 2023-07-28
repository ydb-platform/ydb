#pragma once

//! @file utils.h
//!
//! Infrastructure for running type info tests with different factories and settings.

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/yson/consumer.h>
#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_builder.h>
#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/type_info/fwd.h>

template <typename T>
inline TString ToCanonicalYson(const T& value) {
    return NYT::NodeToCanonicalYsonString(NYT::NodeFromYsonString(TStringBuf(value)));
}

/// Assert that two YSON strings are equal.
#define ASSERT_YSON_EQ(L, R) ASSERT_EQ(ToCanonicalYson(L), ToCanonicalYson(R))

/// Assert that two types are strictly equal.
#define ASSERT_STRICT_EQ(a, b)                                          \
    do {                                                                \
        auto lhs = (a);                                                 \
        auto rhs = (b);                                                 \
        ASSERT_TRUE(NTi::NEq::TStrictlyEqual()(lhs, rhs));              \
        ASSERT_TRUE(NTi::NEq::TStrictlyEqual().IgnoreHash(lhs, rhs));   \
        ASSERT_EQ(lhs->GetHash(), NTi::NEq::TStrictlyEqualHash()(lhs)); \
        ASSERT_EQ(rhs->GetHash(), NTi::NEq::TStrictlyEqualHash()(rhs)); \
        ASSERT_EQ(lhs->GetHash(), rhs->GetHash());                      \
    } while (false)

/// Assert that two types are strictly unequal.
///
/// Note: we check that, if types are not equal, their hashes are also not equal.
/// While this is not guaranteed, we haven't seen any collisions so far.
/// If some collision happen, check if hashing isn't broken before removing the assert.
#define ASSERT_STRICT_NE(a, b)                                          \
    do {                                                                \
        auto lhs = (a);                                                 \
        auto rhs = (b);                                                 \
        ASSERT_FALSE(NTi::NEq::TStrictlyEqual()(lhs, rhs));             \
        ASSERT_FALSE(NTi::NEq::TStrictlyEqual().IgnoreHash(lhs, rhs));  \
        ASSERT_EQ(lhs->GetHash(), NTi::NEq::TStrictlyEqualHash()(lhs)); \
        ASSERT_EQ(rhs->GetHash(), NTi::NEq::TStrictlyEqualHash()(rhs)); \
        ASSERT_NE(lhs->GetHash(), rhs->GetHash());                      \
    } while (false)

/// Assert that a type string is equal to the given type after deserialization.
#define ASSERT_DESERIALIZED_EQ(canonicalType, serializedType)                                                          \
    do {                                                                                                               \
        auto reader = NYsonPull::TReader(NYsonPull::NInput::FromMemory(serializedType), NYsonPull::EStreamType::Node); \
        auto deserializedType = NTi::NIo::DeserializeYson(*NTi::HeapFactory(), reader);                                    \
        ASSERT_STRICT_EQ(deserializedType, canonicalType);                                                             \
        ASSERT_YSON_EQ(NTi::NIo::SerializeYson(canonicalType.Get()), NTi::NIo::SerializeYson(deserializedType.Get()));         \
    } while (false)

/// Test parametrized over different type factories.
#define TEST_TF(N, NN)                         \
    void Test##N##NN(NTi::ITypeFactory& f);    \
    TEST(N, NN##_Heap) {                       \
        Test##N##NN(*NTi::HeapFactory());      \
    }                                          \
    TEST(N, NN##_Pool) {                       \
        Test##N##NN(*NTi::PoolFactory(false)); \
    }                                          \
    TEST(N, NN##_PoolDedup) {                  \
        Test##N##NN(*NTi::PoolFactory(true));  \
    }                                          \
    void Test##N##NN(NTi::ITypeFactory& f)
