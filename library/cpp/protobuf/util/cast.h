#pragma once

#include "traits.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <util/generic/cast.h>

namespace NProtoBuf {
    // C++ compatible conversions of FieldDescriptor::CppType's

    using ECppType = FieldDescriptor::CppType;

    namespace NCast {
        template <ECppType src, ECppType dst>
        struct TIsCompatibleCppType {
            enum {
                Result = src == dst ||
                         (TIsNumericCppType<src>::Result && TIsNumericCppType<dst>::Result)
            };
        };

        template <ECppType src, ECppType dst>
        struct TIsEnumToNumericCppType {
            enum {
                Result = (src == FieldDescriptor::CPPTYPE_ENUM && TIsNumericCppType<dst>::Result)
            };
        };

        template <ECppType src, ECppType dst, bool compatible> // compatible == true
        struct TCompatCastBase {
            static const bool IsCompatible = true;

            typedef typename TCppTypeTraits<src>::T TSrc;
            typedef typename TCppTypeTraits<dst>::T TDst;

            static inline TDst Cast(TSrc value) {
                return value;
            }
        };

        template <ECppType src, ECppType dst> // compatible == false
        struct TCompatCastBase<src, dst, false> {
            static const bool IsCompatible = false;

            typedef typename TCppTypeTraits<src>::T TSrc;
            typedef typename TCppTypeTraits<dst>::T TDst;

            static inline TDst Cast(TSrc) {
                ythrow TBadCastException() << "Incompatible FieldDescriptor::CppType conversion: #"
                                           << (size_t)src << " to #" << (size_t)dst;
            }
        };

        template <ECppType src, ECppType dst, bool isEnumToNum> // enum -> numeric
        struct TCompatCastImpl {
            static const bool IsCompatible = true;

            typedef typename TCppTypeTraits<dst>::T TDst;

            static inline TDst Cast(const EnumValueDescriptor* value) {
                Y_ASSERT(value != nullptr);
                return value->number();
            }
        };

        template <ECppType src, ECppType dst>
        struct TCompatCastImpl<src, dst, false>: public TCompatCastBase<src, dst, TIsCompatibleCppType<src, dst>::Result> {
            using TCompatCastBase<src, dst, TIsCompatibleCppType<src, dst>::Result>::IsCompatible;
        };

        template <ECppType src, ECppType dst>
        struct TCompatCast: public TCompatCastImpl<src, dst, TIsEnumToNumericCppType<src, dst>::Result> {
            typedef TCompatCastImpl<src, dst, TIsEnumToNumericCppType<src, dst>::Result> TBase;

            typedef typename TCppTypeTraits<src>::T TSrc;
            typedef typename TCppTypeTraits<dst>::T TDst;

            using TBase::Cast;
            using TBase::IsCompatible;

            inline bool Try(TSrc value, TDst& res) {
                if (IsCompatible) {
                    res = Cast(value);
                    return true;
                }
                return false;
            }
        };

    }

    template <ECppType src, ECppType dst>
    inline typename TCppTypeTraits<dst>::T CompatCast(typename TCppTypeTraits<src>::T value) {
        return NCast::TCompatCast<src, dst>::Cast(value);
    }

    template <ECppType src, ECppType dst>
    inline bool TryCompatCast(typename TCppTypeTraits<src>::T value, typename TCppTypeTraits<dst>::T& res) {
        return NCast::TCompatCast<src, dst>::Try(value, res);
    }

    // Message static/dynamic checked casts

    template <typename TpMessage>
    inline const TpMessage* TryCast(const Message* msg) {
        if (!msg || TpMessage::descriptor() != msg->GetDescriptor())
            return NULL;
        return CheckedCast<const TpMessage*>(msg);
    }

    template <typename TpMessage>
    inline const TpMessage* TryCast(const Message* msg, const TpMessage*& ret) {
        ret = TryCast<TpMessage>(msg);
        return ret;
    }

    template <typename TpMessage>
    inline TpMessage* TryCast(Message* msg) {
        if (!msg || TpMessage::descriptor() != msg->GetDescriptor())
            return nullptr;
        return CheckedCast<TpMessage*>(msg);
    }

    template <typename TpMessage>
    inline TpMessage* TryCast(Message* msg, TpMessage*& ret) {
        ret = TryCast<TpMessage>(msg);
        return ret;
    }

    // specialize for Message itself

    template <>
    inline const Message* TryCast<Message>(const Message* msg) {
        return msg;
    }

    template <>
    inline Message* TryCast<Message>(Message* msg) {
        return msg;
    }

    // Binary serialization compatible conversion
    inline bool TryBinaryCast(const Message* from, Message* to, TString* buffer = nullptr) {
        TString tmpbuf;
        if (!buffer)
            buffer = &tmpbuf;

        if (!from->SerializeToString(buffer))
            return false;

        return to->ParseFromString(*buffer);
    }

}
