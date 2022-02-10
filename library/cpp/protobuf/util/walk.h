#pragma once

#include "simple_reflection.h"

#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>

#include <functional>

namespace NProtoBuf {
    // Apply @onField processor to each field in @msg (even empty)
    // Do not walk deeper the field if the field is an empty message
    // Returned bool defines if we should walk down deeper to current node children (true), or not (false)
    void WalkReflection(Message& msg,
                        std::function<bool(Message&, const FieldDescriptor*)> onField);
    void WalkReflection(const Message& msg,
                        std::function<bool(const Message&, const FieldDescriptor*)> onField);

    template <typename TOnField>
    inline void WalkReflection(Message& msg, TOnField& onField) { // is used when TOnField is a callable class instance
        WalkReflection(msg, std::function<bool(Message&, const FieldDescriptor*)>(std::ref(onField)));
    }
    template <typename TOnField>
    inline void WalkReflection(const Message& msg, TOnField& onField) {
        WalkReflection(msg, std::function<bool(const Message&, const FieldDescriptor*)>(std::ref(onField)));
    }

    // Apply @onField processor to each descriptor of a field
    // Walk every field including nested messages. Avoid cyclic fields pointing to themselves
    // Returned bool defines if we should walk down deeper to current node children (true), or not (false)
    void WalkSchema(const Descriptor* descriptor, 
                    std::function<bool(const FieldDescriptor*)> onField);
}
