#pragma once

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <util/generic/vector.h>

namespace NProtoBuf {
    class TFieldPath {
    public:
        TFieldPath();
        TFieldPath(const Descriptor* msgType, const TStringBuf& path); // throws exception if path doesn't exist
        TFieldPath(const TVector<const FieldDescriptor*>& path);
        TFieldPath(const TFieldPath&) = default;
        TFieldPath& operator=(const TFieldPath&) = default;

        bool InitUnsafe(const Descriptor* msgType, const TStringBuf path); // noexcept
        void Init(const Descriptor* msgType, const TStringBuf& path);      // throws

        const TVector<const FieldDescriptor*>& Fields() const {
            return Path;
        }

        void AddField(const FieldDescriptor* field) {
            Path.push_back(field);
        }

        const Descriptor* ParentType() const {
            return Empty() ? nullptr : Path.front()->containing_type();
        }

        const FieldDescriptor* FieldDescr() const {
            return Empty() ? nullptr : Path.back();
        }

        bool Empty() const {
            return Path.empty();
        }

        explicit operator bool() const {
            return !Empty();
        }

        bool operator!() const {
            return Empty();
        }

    private:
        TVector<const FieldDescriptor*> Path;
    };

}
