#pragma once

#include <util/generic/typetraits.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

namespace NProtoBuf {
// this nasty windows.h macro interfers with protobuf::Reflection::GetMessage()
#if defined(GetMessage)
#undef GetMessage
#endif

    struct TCppTypeTraitsBase {
        static inline bool Has(const Message& msg, const FieldDescriptor* field) { // non-repeated
            return msg.GetReflection()->HasField(msg, field);
        }
        static inline size_t Size(const Message& msg, const FieldDescriptor* field) { // repeated
            return msg.GetReflection()->FieldSize(msg, field);
        }

        static inline void Clear(Message& msg, const FieldDescriptor* field) {
            msg.GetReflection()->ClearField(&msg, field);
        }

        static inline void RemoveLast(Message& msg, const FieldDescriptor* field) {
            msg.GetReflection()->RemoveLast(&msg, field);
        }

        static inline void SwapElements(Message& msg, const FieldDescriptor* field, int index1, int index2) {
            msg.GetReflection()->SwapElements(&msg, field, index1, index2);
        }
    };

    // default value accessor
    template <FieldDescriptor::CppType cpptype>
    struct TCppTypeTraitsDefault;

#define DECLARE_CPPTYPE_DEFAULT(cpptype, method)          \
    template <>                                           \
    struct TCppTypeTraitsDefault<cpptype> {               \
        static auto GetDefault(const FieldDescriptor* fd) \
            -> decltype(fd->default_value_##method()) {   \
            Y_ASSERT(fd);                                 \
            return fd->default_value_##method();          \
        }                                                 \
    }

    DECLARE_CPPTYPE_DEFAULT(FieldDescriptor::CppType::CPPTYPE_INT32, int32);
    DECLARE_CPPTYPE_DEFAULT(FieldDescriptor::CppType::CPPTYPE_INT64, int64);
    DECLARE_CPPTYPE_DEFAULT(FieldDescriptor::CppType::CPPTYPE_UINT32, uint32);
    DECLARE_CPPTYPE_DEFAULT(FieldDescriptor::CppType::CPPTYPE_UINT64, uint64);
    DECLARE_CPPTYPE_DEFAULT(FieldDescriptor::CppType::CPPTYPE_FLOAT, float);
    DECLARE_CPPTYPE_DEFAULT(FieldDescriptor::CppType::CPPTYPE_DOUBLE, double);
    DECLARE_CPPTYPE_DEFAULT(FieldDescriptor::CppType::CPPTYPE_BOOL, bool);
    DECLARE_CPPTYPE_DEFAULT(FieldDescriptor::CppType::CPPTYPE_ENUM, enum);
    DECLARE_CPPTYPE_DEFAULT(FieldDescriptor::CppType::CPPTYPE_STRING, string);

#undef DECLARE_CPPTYPE_DEFAULT

    // getters/setters of field with specified CppType
    template <FieldDescriptor::CppType cpptype>
    struct TCppTypeTraits : TCppTypeTraitsBase {
        static const FieldDescriptor::CppType CppType = cpptype;

        struct T {};
        static T Get(const Message& msg, const FieldDescriptor* field);
        static T GetRepeated(const Message& msg, const FieldDescriptor* field, int index);
        static T GetDefault(const FieldDescriptor* field);

        static void Set(Message& msg, const FieldDescriptor* field, T value);
        static void AddRepeated(Message& msg, const FieldDescriptor* field, T value);
        static void SetRepeated(Message& msg, const FieldDescriptor* field, int index, T value);
    };

    // any type T -> CppType
    template <typename T>
    struct TSelectCppType {
        //static const FieldDescriptor::CppType Result = FieldDescriptor::MAX_CPPTYPE;
    };

#define DECLARE_CPPTYPE_TRAITS(cpptype, type, method)                                                    \
    template <>                                                                                          \
    struct TCppTypeTraits<cpptype>: public TCppTypeTraitsBase {                                          \
        typedef type T;                                                                                  \
        static const FieldDescriptor::CppType CppType = cpptype;                                         \
                                                                                                         \
        static inline T Get(const Message& msg, const FieldDescriptor* field) {                          \
            return msg.GetReflection()->Get##method(msg, field);                                         \
        }                                                                                                \
        static inline T GetRepeated(const Message& msg, const FieldDescriptor* field, int index) {       \
            return msg.GetReflection()->GetRepeated##method(msg, field, index);                          \
        }                                                                                                \
        static inline T GetDefault(const FieldDescriptor* field) {                                       \
            return TCppTypeTraitsDefault<cpptype>::GetDefault(field);                                    \
        }                                                                                                \
        static inline void Set(Message& msg, const FieldDescriptor* field, T value) {                    \
            msg.GetReflection()->Set##method(&msg, field, value);                                        \
        }                                                                                                \
        static inline void AddRepeated(Message& msg, const FieldDescriptor* field, T value) {            \
            msg.GetReflection()->Add##method(&msg, field, value);                                        \
        }                                                                                                \
        static inline void SetRepeated(Message& msg, const FieldDescriptor* field, int index, T value) { \
            msg.GetReflection()->SetRepeated##method(&msg, field, index, value);                         \
        }                                                                                                \
    };                                                                                                   \
    template <>                                                                                          \
    struct TSelectCppType<type> {                                                                        \
        static const FieldDescriptor::CppType Result = cpptype;                                          \
        typedef type T;                                                                                  \
    }

    DECLARE_CPPTYPE_TRAITS(FieldDescriptor::CPPTYPE_INT32, i32, Int32);
    DECLARE_CPPTYPE_TRAITS(FieldDescriptor::CPPTYPE_INT64, i64, Int64);
    DECLARE_CPPTYPE_TRAITS(FieldDescriptor::CPPTYPE_UINT32, ui32, UInt32);
    DECLARE_CPPTYPE_TRAITS(FieldDescriptor::CPPTYPE_UINT64, ui64, UInt64);
    DECLARE_CPPTYPE_TRAITS(FieldDescriptor::CPPTYPE_DOUBLE, double, Double);
    DECLARE_CPPTYPE_TRAITS(FieldDescriptor::CPPTYPE_FLOAT, float, Float);
    DECLARE_CPPTYPE_TRAITS(FieldDescriptor::CPPTYPE_BOOL, bool, Bool);
    DECLARE_CPPTYPE_TRAITS(FieldDescriptor::CPPTYPE_ENUM, const EnumValueDescriptor*, Enum);
    DECLARE_CPPTYPE_TRAITS(FieldDescriptor::CPPTYPE_STRING, TString, String);
    //DECLARE_CPPTYPE_TRAITS(FieldDescriptor::CPPTYPE_MESSAGE, const Message&, Message);

#undef DECLARE_CPPTYPE_TRAITS

    // specialization for message pointer
    template <>
    struct TCppTypeTraits<FieldDescriptor::CPPTYPE_MESSAGE>: public TCppTypeTraitsBase {
        typedef const Message* T;
        static const FieldDescriptor::CppType CppType = FieldDescriptor::CPPTYPE_MESSAGE;

        static inline T Get(const Message& msg, const FieldDescriptor* field) {
            return &(msg.GetReflection()->GetMessage(msg, field));
        }
        static inline T GetRepeated(const Message& msg, const FieldDescriptor* field, int index) {
            return &(msg.GetReflection()->GetRepeatedMessage(msg, field, index));
        }
        static inline Message* Set(Message& msg, const FieldDescriptor* field, const Message* value) {
            Message* ret = msg.GetReflection()->MutableMessage(&msg, field);
            ret->CopyFrom(*value);
            return ret;
        }
        static inline Message* AddRepeated(Message& msg, const FieldDescriptor* field, const Message* value) {
            Message* ret = msg.GetReflection()->AddMessage(&msg, field);
            ret->CopyFrom(*value);
            return ret;
        }
        static inline Message* SetRepeated(Message& msg, const FieldDescriptor* field, int index, const Message* value) {
            Message* ret = msg.GetReflection()->MutableRepeatedMessage(&msg, field, index);
            ret->CopyFrom(*value);
            return ret;
        }
    };

    template <>
    struct TSelectCppType<const Message*> {
        static const FieldDescriptor::CppType Result = FieldDescriptor::CPPTYPE_MESSAGE;
        typedef const Message* T;
    };

    template <>
    struct TSelectCppType<Message> {
        static const FieldDescriptor::CppType Result = FieldDescriptor::CPPTYPE_MESSAGE;
        typedef const Message* T;
    };

    template <FieldDescriptor::CppType CppType, bool Repeated>
    struct TFieldTraits {
        typedef TCppTypeTraits<CppType> TBaseTraits;
        typedef typename TBaseTraits::T T;

        static inline T Get(const Message& msg, const FieldDescriptor* field, size_t index = 0) {
            Y_ASSERT(index == 0);
            return TBaseTraits::Get(msg, field);
        }

        static inline T GetDefault(const FieldDescriptor* field) {
            return TBaseTraits::GetDefault(field);
        }

        static inline bool Has(const Message& msg, const FieldDescriptor* field) {
            return TBaseTraits::Has(msg, field);
        }

        static inline size_t Size(const Message& msg, const FieldDescriptor* field) {
            return Has(msg, field);
        }

        static inline void Set(Message& msg, const FieldDescriptor* field, T value, size_t index = 0) {
            Y_ASSERT(index == 0);
            TBaseTraits::Set(msg, field, value);
        }

        static inline void Add(Message& msg, const FieldDescriptor* field, T value) {
            TBaseTraits::Set(msg, field, value);
        }
    };

    template <FieldDescriptor::CppType CppType>
    struct TFieldTraits<CppType, true> {
        typedef TCppTypeTraits<CppType> TBaseTraits;
        typedef typename TBaseTraits::T T;

        static inline T Get(const Message& msg, const FieldDescriptor* field, size_t index = 0) {
            return TBaseTraits::GetRepeated(msg, field, index);
        }

        static inline T GetDefault(const FieldDescriptor* field) {
            return TBaseTraits::GetDefault(field);
        }

        static inline size_t Size(const Message& msg, const FieldDescriptor* field) {
            return TBaseTraits::Size(msg, field);
        }

        static inline bool Has(const Message& msg, const FieldDescriptor* field) {
            return Size(msg, field) > 0;
        }

        static inline void Set(Message& msg, const FieldDescriptor* field, T value, size_t index = 0) {
            TBaseTraits::SetRepeated(msg, field, index, value);
        }

        static inline void Add(Message& msg, const FieldDescriptor* field, T value) {
            TBaseTraits::AddRepeated(msg, field, value);
        }
    };

    // Simpler interface at the cost of checking is_repeated() on each call
    template <FieldDescriptor::CppType CppType>
    struct TSimpleFieldTraits {
        typedef TFieldTraits<CppType, true> TRepeated;
        typedef TFieldTraits<CppType, false> TSingle;
        typedef typename TRepeated::T T;

        static inline size_t Size(const Message& msg, const FieldDescriptor* field) {
            if (field->is_repeated())
                return TRepeated::Size(msg, field);
            else
                return TSingle::Size(msg, field);
        }

        static inline bool Has(const Message& msg, const FieldDescriptor* field) {
            if (field->is_repeated())
                return TRepeated::Has(msg, field);
            else
                return TSingle::Has(msg, field);
        }

        static inline T Get(const Message& msg, const FieldDescriptor* field, size_t index = 0) {
            Y_ASSERT(index < Size(msg, field) || !field->is_repeated() && index == 0); // Get for single fields is always allowed because of default values
            if (field->is_repeated())
                return TRepeated::Get(msg, field, index);
            else
                return TSingle::Get(msg, field, index);
        }

        static inline T GetDefault(const FieldDescriptor* field) {
            return TSingle::GetDefault(field);
        }

        static inline void Set(Message& msg, const FieldDescriptor* field, T value, size_t index = 0) {
            Y_ASSERT(!field->is_repeated() && index == 0 || index < Size(msg, field));
            if (field->is_repeated())
                TRepeated::Set(msg, field, value, index);
            else
                TSingle::Set(msg, field, value, index);
        }

        static inline void Add(Message& msg, const FieldDescriptor* field, T value) {
            if (field->is_repeated())
                TRepeated::Add(msg, field, value);
            else
                TSingle::Add(msg, field, value);
        }
    };

    // some cpp-type groups

    template <FieldDescriptor::CppType CppType>
    struct TIsIntegerCppType {
        enum {
            Result = CppType == FieldDescriptor::CPPTYPE_INT32 ||
                     CppType == FieldDescriptor::CPPTYPE_INT64 ||
                     CppType == FieldDescriptor::CPPTYPE_UINT32 ||
                     CppType == FieldDescriptor::CPPTYPE_UINT64
        };
    };

    template <FieldDescriptor::CppType CppType>
    struct TIsFloatCppType {
        enum {
            Result = CppType == FieldDescriptor::CPPTYPE_FLOAT ||
                     CppType == FieldDescriptor::CPPTYPE_DOUBLE
        };
    };

    template <FieldDescriptor::CppType CppType>
    struct TIsNumericCppType {
        enum {
            Result = CppType == FieldDescriptor::CPPTYPE_BOOL ||
                     TIsIntegerCppType<CppType>::Result ||
                     TIsFloatCppType<CppType>::Result
        };
    };

    // a helper macro for splitting flow by cpp-type (e.g. in a switch)

#define APPLY_TMP_MACRO_FOR_ALL_CPPTYPES()                            \
    TMP_MACRO_FOR_CPPTYPE(NProtoBuf::FieldDescriptor::CPPTYPE_INT32)  \
    TMP_MACRO_FOR_CPPTYPE(NProtoBuf::FieldDescriptor::CPPTYPE_INT64)  \
    TMP_MACRO_FOR_CPPTYPE(NProtoBuf::FieldDescriptor::CPPTYPE_UINT32) \
    TMP_MACRO_FOR_CPPTYPE(NProtoBuf::FieldDescriptor::CPPTYPE_UINT64) \
    TMP_MACRO_FOR_CPPTYPE(NProtoBuf::FieldDescriptor::CPPTYPE_DOUBLE) \
    TMP_MACRO_FOR_CPPTYPE(NProtoBuf::FieldDescriptor::CPPTYPE_FLOAT)  \
    TMP_MACRO_FOR_CPPTYPE(NProtoBuf::FieldDescriptor::CPPTYPE_BOOL)   \
    TMP_MACRO_FOR_CPPTYPE(NProtoBuf::FieldDescriptor::CPPTYPE_ENUM)   \
    TMP_MACRO_FOR_CPPTYPE(NProtoBuf::FieldDescriptor::CPPTYPE_STRING) \
    TMP_MACRO_FOR_CPPTYPE(NProtoBuf::FieldDescriptor::CPPTYPE_MESSAGE)
}
