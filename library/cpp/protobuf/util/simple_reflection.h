#pragma once

#include "cast.h"
#include "path.h"
#include "traits.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <util/generic/maybe.h>
#include <util/generic/typetraits.h>
#include <util/generic/vector.h>
#include <util/system/defaults.h>

namespace NProtoBuf {
    class TConstField {
    public:
        TConstField(const Message& msg, const FieldDescriptor* fd)
            : Msg(msg)
            , Fd(fd)
        {
            Y_ASSERT(Fd && Fd->containing_type() == Msg.GetDescriptor());
        }

        static TMaybe<TConstField> ByPath(const Message& msg, const TStringBuf& path);
        static TMaybe<TConstField> ByPath(const Message& msg, const TVector<const FieldDescriptor*>& fieldsPath);
        static TMaybe<TConstField> ByPath(const Message& msg, const TFieldPath& fieldsPath);

        const Message& Parent() const {
            return Msg;
        }

        const FieldDescriptor* Field() const {
            return Fd;
        }

        bool HasValue() const {
            return IsRepeated() ? Refl().FieldSize(Msg, Fd) > 0
                                : Refl().HasField(Msg, Fd);
        }

        // deprecated, use HasValue() instead
        bool Has() const {
            return HasValue();
        }

        size_t Size() const {
            return IsRepeated() ? Refl().FieldSize(Msg, Fd)
                                : (Refl().HasField(Msg, Fd) ? 1 : 0);
        }

        template <typename T>
        inline typename TSelectCppType<T>::T Get(size_t index = 0) const;

        template <typename TMsg>
        inline const TMsg* GetAs(size_t index = 0) const {
            // casting version of Get
            return IsMessageInstance<TMsg>() ? CheckedCast<const TMsg*>(Get<const Message*>(index)) : nullptr;
        }

        template <typename T>
        bool IsInstance() const {
            return CppType() == TSelectCppType<T>::Result;
        }

        template <typename TMsg>
        bool IsMessageInstance() const {
            return IsMessage() && Fd->message_type() == TMsg::descriptor();
        }

        template <typename TMsg>
        bool IsInstance(std::enable_if_t<std::is_base_of<Message, TMsg>::value && !std::is_same<Message, TMsg>::value, void>* = NULL) const { // template will be selected when specifying Message children types
            return IsMessage() && Fd->message_type() == TMsg::descriptor();
        }

        bool IsString() const {
            return CppType() == FieldDescriptor::CPPTYPE_STRING;
        }

        bool IsMessage() const {
            return CppType() == FieldDescriptor::CPPTYPE_MESSAGE;
        }

        bool HasSameType(const TConstField& other) const {
            if (CppType() != other.CppType())
                return false;
            if (IsMessage() && Field()->message_type() != other.Field()->message_type())
                return false;
            if (CppType() == FieldDescriptor::CPPTYPE_ENUM && Field()->enum_type() != other.Field()->enum_type())
                return false;
            return true;
        }

    protected:
        bool IsRepeated() const {
            return Fd->is_repeated();
        }

        FieldDescriptor::CppType CppType() const {
            return Fd->cpp_type();
        }

        const Reflection& Refl() const {
            return *Msg.GetReflection();
        }

        [[noreturn]] void RaiseUnknown() const {
            ythrow yexception() << "Unknown field cpp-type: " << (size_t)CppType();
        }

        bool IsSameField(const TConstField& other) const {
            return &Parent() == &other.Parent() && Field() == other.Field();
        }

    protected:
        const Message& Msg;
        const FieldDescriptor* Fd;
    };

    class TMutableField: public TConstField {
    public:
        TMutableField(Message& msg, const FieldDescriptor* fd)
            : TConstField(msg, fd)
        {
        }

        static TMaybe<TMutableField> ByPath(Message& msg, const TStringBuf& path, bool createPath = false);
        static TMaybe<TMutableField> ByPath(Message& msg, const TVector<const FieldDescriptor*>& fieldsPath, bool createPath = false);
        static TMaybe<TMutableField> ByPath(Message& msg, const TFieldPath& fieldsPath, bool createPath = false);

        Message* MutableParent() {
            return Mut();
        }

        template <typename T>
        inline void Set(T value, size_t index = 0);

        template <typename T>
        inline void Add(T value);

        inline void MergeFrom(const TConstField& src);

        inline void Clear() {
            Refl().ClearField(Mut(), Fd);
        }
        /*
    void Swap(TMutableField& f) {
        Y_ASSERT(Field() == f.Field());

        // not implemented yet, TODO: implement when Reflection::Mutable(Ptr)RepeatedField
        // is ported into arcadia protobuf library from up-stream.
    }
*/
        inline void RemoveLast() {
            Y_ASSERT(HasValue());
            if (IsRepeated())
                Refl().RemoveLast(Mut(), Fd);
            else
                Clear();
        }

        inline void SwapElements(size_t index1, size_t index2) {
            Y_ASSERT(IsRepeated());
            Y_ASSERT(index1 < Size());
            Y_ASSERT(index2 < Size());
            if (index1 == index2)
                return;
            Refl().SwapElements(Mut(), Fd, index1, index2);
        }

        inline void Remove(size_t index) {
            if (index >= Size())
                return;

            // Move to the end
            for (size_t i = index, size = Size(); i < size - 1; ++i)
                SwapElements(i, i + 1);
            RemoveLast();
        }

        Message* MutableMessage(size_t index = 0) {
            Y_ASSERT(IsMessage());
            if (IsRepeated()) {
                Y_ASSERT(index < Size());
                return Refl().MutableRepeatedMessage(Mut(), Fd, index);
            } else {
                Y_ASSERT(index == 0);
                return Refl().MutableMessage(Mut(), Fd);
            }
        }

        template <typename TMsg>
        inline TMsg* AddMessage() {
            return CheckedCast<TMsg*>(AddMessage());
        }

        inline Message* AddMessage() {
            Y_ASSERT(IsMessage() && IsRepeated());
            return Refl().AddMessage(Mut(), Fd);
        }

    private:
        Message* Mut() {
            return const_cast<Message*>(&Msg);
        }

        template <typename T>
        inline void MergeValue(T srcValue);
    };

    // template implementations

    template <typename T>
    inline typename TSelectCppType<T>::T TConstField::Get(size_t index) const {
        Y_ASSERT(index < Size() || !Fd->is_repeated() && index == 0); // Get for single fields is always allowed because of default values
#define TMP_MACRO_FOR_CPPTYPE(CPPTYPE) \
    case CPPTYPE:                      \
        return CompatCast<CPPTYPE, TSelectCppType<T>::Result>(TSimpleFieldTraits<CPPTYPE>::Get(Msg, Fd, index));
        switch (CppType()) {
            APPLY_TMP_MACRO_FOR_ALL_CPPTYPES()
            default:
                RaiseUnknown();
        }
#undef TMP_MACRO_FOR_CPPTYPE
    }

    template <typename T>
    inline void TMutableField::Set(T value, size_t index) {
        Y_ASSERT(!IsRepeated() && index == 0 || index < Size());
#define TMP_MACRO_FOR_CPPTYPE(CPPTYPE)                                                                              \
    case CPPTYPE:                                                                                                   \
        TSimpleFieldTraits<CPPTYPE>::Set(*Mut(), Fd, CompatCast<TSelectCppType<T>::Result, CPPTYPE>(value), index); \
        break;
        switch (CppType()) {
            APPLY_TMP_MACRO_FOR_ALL_CPPTYPES()
            default:
                RaiseUnknown();
        }
#undef TMP_MACRO_FOR_CPPTYPE
    }

    template <typename T>
    inline void TMutableField::Add(T value) {
#define TMP_MACRO_FOR_CPPTYPE(CPPTYPE)                                                                       \
    case CPPTYPE:                                                                                            \
        TSimpleFieldTraits<CPPTYPE>::Add(*Mut(), Fd, CompatCast<TSelectCppType<T>::Result, CPPTYPE>(value)); \
        break;
        switch (CppType()) {
            APPLY_TMP_MACRO_FOR_ALL_CPPTYPES()
            default:
                RaiseUnknown();
        }
#undef TMP_MACRO_FOR_CPPTYPE
    }

    template <typename T>
    inline void TMutableField::MergeValue(T srcValue) {
        Add(srcValue);
    }

    template <>
    inline void TMutableField::MergeValue<const Message*>(const Message* srcValue) {
        if (IsRepeated()) {
            Add(srcValue);
        } else {
            MutableMessage()->MergeFrom(*srcValue);
        }
    }

    inline void TMutableField::MergeFrom(const TConstField& src) {
        Y_ASSERT(HasSameType(src));
        if (IsSameField(src))
            return;
#define TMP_MACRO_FOR_CPPTYPE(CPPTYPE)                                                        \
    case CPPTYPE: {                                                                           \
        for (size_t itemIdx = 0; itemIdx < src.Size(); ++itemIdx) {                           \
            MergeValue(TSimpleFieldTraits<CPPTYPE>::Get(src.Parent(), src.Field(), itemIdx)); \
        }                                                                                     \
        break;                                                                                \
    }
        switch (CppType()) {
            APPLY_TMP_MACRO_FOR_ALL_CPPTYPES()
            default:
                RaiseUnknown();
        }
#undef TMP_MACRO_FOR_CPPTYPE
    }

}
