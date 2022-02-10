#pragma once

#include <util/generic/hash_set.h>
#include <util/generic/string.h>

#include <util/system/defaults.h>

namespace NProtobufJsonTest {
    template <typename TProto>
    inline void
    FillFlatProto(TProto* proto,
                  const THashSet<TString>& skippedFields = THashSet<TString>()) {
#define DEFINE_FIELD(name, value)                         \
    if (skippedFields.find(#name) == skippedFields.end()) \
        proto->Set##name(value);
#include <library/cpp/protobuf/json/ut/fields.incl>
#undef DEFINE_FIELD
    }

    template <typename TRepeatedField, typename TValue>
    inline void
    AddValue(TRepeatedField* field, TValue value) {
        field->Add(value);
    }

    inline void
    AddValue(google::protobuf::RepeatedPtrField<TString>* field, const TString& value) {
        *(field->Add()) = value;
    }

    inline void
    FillRepeatedProto(TFlatRepeated* proto,
                      const THashSet<TString>& skippedFields = THashSet<TString>()) {
#define DEFINE_REPEATED_FIELD(name, type, ...)                         \
    if (skippedFields.find(#name) == skippedFields.end()) {            \
        type values[] = {__VA_ARGS__};                                 \
        for (size_t i = 0, end = Y_ARRAY_SIZE(values); i < end; ++i) { \
            AddValue(proto->Mutable##name(), values[i]);               \
        }                                                              \
    }
#include <library/cpp/protobuf/json/ut/repeated_fields.incl>
#undef DEFINE_REPEATED_FIELD
    }

    template <typename TProto>
    inline void
    FillCompositeProto(TProto* proto, const THashSet<TString>& skippedFields = THashSet<TString>()) {
        FillFlatProto(proto->MutablePart(), skippedFields);
    }

#define UNIT_ASSERT_PROTOS_EQUAL(lhs, rhs)                                               \
    do {                                                                                 \
        if (lhs.SerializeAsString() != rhs.SerializeAsString()) {                        \
            Cerr << ">>>>>>>>>> lhs != rhs:" << Endl;                                    \
            Cerr << lhs.DebugString() << Endl;                                           \
            Cerr << rhs.DebugString() << Endl;                                           \
            UNIT_ASSERT_STRINGS_EQUAL(lhs.DebugString(), rhs.DebugString());             \
            UNIT_ASSERT_STRINGS_EQUAL(lhs.SerializeAsString(), rhs.SerializeAsString()); \
        }                                                                                \
    } while (false);

}
