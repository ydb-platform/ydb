#pragma once

#include <google/protobuf/descriptor.h>

namespace NProtoBuf {
    class TFieldsIterator {
    public:
        explicit TFieldsIterator(const NProtoBuf::Descriptor* descriptor, int position = 0)
        : Descriptor(descriptor)
        , Position(position)
        { }

        TFieldsIterator& operator++() {
            ++Position;
            return *this;
        }

        TFieldsIterator& operator++(int) {
            auto& ret = *this;
            ++*this;
            return ret;
        }

        const NProtoBuf::FieldDescriptor* operator*() const {
            return Descriptor->field(Position);
        }

        bool operator== (const TFieldsIterator& other) const {
            return Position == other.Position && Descriptor == other.Descriptor;
        }

        bool operator!= (const TFieldsIterator& other) const {
            return !(*this == other);
        }

    private:
        const NProtoBuf::Descriptor* Descriptor = nullptr;
        int Position = 0;
    };
}

// Namespaces required by `range-based for` ADL:
namespace google {
    namespace protobuf {
        NProtoBuf::TFieldsIterator begin(const NProtoBuf::Descriptor& descriptor) {
            return NProtoBuf::TFieldsIterator(&descriptor);
        }

        NProtoBuf::TFieldsIterator end(const NProtoBuf::Descriptor& descriptor) {
            return NProtoBuf::TFieldsIterator(&descriptor, descriptor.field_count());
        }
    }
}
