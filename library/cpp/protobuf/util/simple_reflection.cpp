#include "simple_reflection.h"

namespace NProtoBuf {
    const Message* GetMessageHelper(const TConstField& curField, bool) {
        return curField.HasValue() && curField.IsMessage() ? curField.Get<Message>() : nullptr;
    }

    Message* GetMessageHelper(TMutableField& curField, bool createPath) {
        if (curField.IsMessage()) {
            if (!curField.HasValue()) {
                if (createPath)
                    return curField.Field()->is_repeated() ? curField.AddMessage() : curField.MutableMessage();
            } else {
                return curField.MutableMessage();
            }
        }
        return nullptr;
    }

    template <class TField, class TMsg>
    TMaybe<TField> ByPathImpl(TMsg& msg, const TVector<const FieldDescriptor*>& fieldsPath, bool createPath) {
        if (fieldsPath.empty())
            return TMaybe<TField>();
        TMsg* curParent = &msg;
        for (size_t i = 0, size = fieldsPath.size(); i < size; ++i) {
            const FieldDescriptor* field = fieldsPath[i];
            if (!curParent)
                return TMaybe<TField>();
            TField curField(*curParent, field);
            if (size - i == 1) // last element in path
                return curField;
            curParent = GetMessageHelper(curField, createPath);
        }
        if (curParent)
            return TField(*curParent, fieldsPath.back());
        else
            return TMaybe<TField>();
    }

    TMaybe<TConstField> TConstField::ByPath(const Message& msg, const TVector<const FieldDescriptor*>& fieldsPath) {
        return ByPathImpl<TConstField, const Message>(msg, fieldsPath, false);
    }

    TMaybe<TConstField> TConstField::ByPath(const Message& msg, const TStringBuf& path) {
        TFieldPath fieldPath;
        if (!fieldPath.InitUnsafe(msg.GetDescriptor(), path))
            return TMaybe<TConstField>();
        return ByPathImpl<TConstField, const Message>(msg, fieldPath.Fields(), false);
    }

    TMaybe<TConstField> TConstField::ByPath(const Message& msg, const TFieldPath& path) {
        return ByPathImpl<TConstField, const Message>(msg, path.Fields(), false);
    }

    TMaybe<TMutableField> TMutableField::ByPath(Message& msg, const TVector<const FieldDescriptor*>& fieldsPath, bool createPath) {
        return ByPathImpl<TMutableField, Message>(msg, fieldsPath, createPath);
    }

    TMaybe<TMutableField> TMutableField::ByPath(Message& msg, const TStringBuf& path, bool createPath) {
        TFieldPath fieldPath;
        if (!fieldPath.InitUnsafe(msg.GetDescriptor(), path))
            return TMaybe<TMutableField>();
        return ByPathImpl<TMutableField, Message>(msg, fieldPath.Fields(), createPath);
    }

    TMaybe<TMutableField> TMutableField::ByPath(Message& msg, const TFieldPath& path, bool createPath) {
        return ByPathImpl<TMutableField, Message>(msg, path.Fields(), createPath);
    }

}
