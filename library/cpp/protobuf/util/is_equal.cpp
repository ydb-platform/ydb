#include "is_equal.h"
#include "traits.h"

#include <google/protobuf/descriptor.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>
#include <util/string/vector.h>

namespace NProtoBuf {
    template <bool useDefault>
    static bool IsEqualImpl(const Message& m1, const Message& m2, TVector<TString>* differentPath);

    namespace {
        template <FieldDescriptor::CppType CppType, bool useDefault>
        struct TCompareValue {
            typedef typename TCppTypeTraits<CppType>::T T;
            static inline bool IsEqual(T value1, T value2, TVector<TString>*) {
                return value1 == value2;
            }
        };

        template <bool useDefault>
        struct TCompareValue<FieldDescriptor::CPPTYPE_MESSAGE, useDefault> {
            static inline bool IsEqual(const Message* value1, const Message* value2, TVector<TString>* differentPath) {
                return NProtoBuf::IsEqualImpl<useDefault>(*value1, *value2, differentPath);
            }
        };

        template <FieldDescriptor::CppType CppType, bool useDefault>
        class TCompareField {
            typedef TCppTypeTraits<CppType> TTraits;
            typedef TCompareValue<CppType, useDefault> TCompare;

        public:
            static inline bool IsEqual(const Message& m1, const Message& m2, const FieldDescriptor& field, TVector<TString>* differentPath) {
                if (field.is_repeated())
                    return IsEqualRepeated(m1, m2, &field, differentPath);
                else
                    return IsEqualSingle(m1, m2, &field, differentPath);
            }

        private:
            static bool IsEqualSingle(const Message& m1, const Message& m2, const FieldDescriptor* field, TVector<TString>* differentPath) {
                bool has1 = m1.GetReflection()->HasField(m1, field);
                bool has2 = m2.GetReflection()->HasField(m2, field);

                if (has1 != has2) {
                    if (!useDefault || field->is_required()) {
                        return false;
                    }
                } else if (!has1)
                    return true;

                return TCompare::IsEqual(TTraits::Get(m1, field),
                                         TTraits::Get(m2, field),
                                         differentPath);
            }

            static bool IsEqualRepeated(const Message& m1, const Message& m2, const FieldDescriptor* field, TVector<TString>* differentPath) {
                int fieldSize = m1.GetReflection()->FieldSize(m1, field);
                if (fieldSize != m2.GetReflection()->FieldSize(m2, field))
                    return false;
                for (int i = 0; i < fieldSize; ++i)
                    if (!IsEqualRepeatedValue(m1, m2, field, i, differentPath)) {
                        if (!!differentPath) {
                            differentPath->push_back(ToString(i));
                        }
                        return false;
                    }
                return true;
            }

            static inline bool IsEqualRepeatedValue(const Message& m1, const Message& m2, const FieldDescriptor* field, int index, TVector<TString>* differentPath) {
                return TCompare::IsEqual(TTraits::GetRepeated(m1, field, index),
                                         TTraits::GetRepeated(m2, field, index),
                                         differentPath);
            }
        };

        template <bool useDefault>
        bool IsEqualField(const Message& m1, const Message& m2, const FieldDescriptor& field, TVector<TString>* differentPath) {
#define CASE_CPPTYPE(cpptype)                                                                                          \
    case FieldDescriptor::CPPTYPE_##cpptype: {                                                                         \
        bool r = TCompareField<FieldDescriptor::CPPTYPE_##cpptype, useDefault>::IsEqual(m1, m2, field, differentPath); \
        if (!r && !!differentPath) {                                                                                   \
            differentPath->push_back(field.name());                                                                    \
        }                                                                                                              \
        return r;                                                                                                      \
    }

            switch (field.cpp_type()) {
                CASE_CPPTYPE(INT32)
                CASE_CPPTYPE(INT64)
                CASE_CPPTYPE(UINT32)
                CASE_CPPTYPE(UINT64)
                CASE_CPPTYPE(DOUBLE)
                CASE_CPPTYPE(FLOAT)
                CASE_CPPTYPE(BOOL)
                CASE_CPPTYPE(ENUM)
                CASE_CPPTYPE(STRING)
                CASE_CPPTYPE(MESSAGE)
                default:
                    ythrow yexception() << "Unsupported cpp-type field comparison";
            }

#undef CASE_CPPTYPE
        }
    }

    template <bool useDefault>
    bool IsEqualImpl(const Message& m1, const Message& m2, TVector<TString>* differentPath) {
        const Descriptor* descr = m1.GetDescriptor();
        if (descr != m2.GetDescriptor()) {
            return false;
        }
        for (int i = 0; i < descr->field_count(); ++i)
            if (!IsEqualField<useDefault>(m1, m2, *descr->field(i), differentPath)) {
                return false;
            }
        return true;
    }

    bool IsEqual(const Message& m1, const Message& m2) {
        return IsEqualImpl<false>(m1, m2, nullptr);
    }

    bool IsEqual(const Message& m1, const Message& m2, TString* differentPath) {
        TVector<TString> differentPathVector;
        TVector<TString>* differentPathVectorPtr = !!differentPath ? &differentPathVector : nullptr;
        bool r = IsEqualImpl<false>(m1, m2, differentPathVectorPtr);
        if (!r && differentPath) {
            *differentPath = JoinStrings(differentPathVector.rbegin(), differentPathVector.rend(), "/");
        }
        return r;
    }

    bool IsEqualDefault(const Message& m1, const Message& m2) {
        return IsEqualImpl<true>(m1, m2, nullptr);
    }

    template <bool useDefault>
    static bool IsEqualFieldImpl(
        const Message& m1,
        const Message& m2,
        const FieldDescriptor& field,
        TVector<TString>* differentPath) {
        const Descriptor* descr = m1.GetDescriptor();
        if (descr != m2.GetDescriptor()) {
            return false;
        }
        return IsEqualField<useDefault>(m1, m2, field, differentPath);
    }

    bool IsEqualField(const Message& m1, const Message& m2, const FieldDescriptor& field) {
        return IsEqualFieldImpl<false>(m1, m2, field, nullptr);
    }

    bool IsEqualFieldDefault(const Message& m1, const Message& m2, const FieldDescriptor& field) {
        return IsEqualFieldImpl<true>(m1, m2, field, nullptr);
    }

}
