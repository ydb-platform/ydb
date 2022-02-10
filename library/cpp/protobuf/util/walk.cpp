#include "walk.h"

#include <util/generic/hash_set.h>

namespace {
    using namespace NProtoBuf;

    template <typename TMessage, typename TOnField>
    void DoWalkReflection(TMessage& msg, TOnField& onField) {
        const Descriptor* descr = msg.GetDescriptor();
        for (int i1 = 0; i1 < descr->field_count(); ++i1) {
            const FieldDescriptor* fd = descr->field(i1);
            if (!onField(msg, fd)) {
                continue;
            }

            std::conditional_t<std::is_const_v<TMessage>, TConstField, TMutableField> ff(msg, fd);
            if (ff.IsMessage()) {
                for (size_t i2 = 0; i2 < ff.Size(); ++i2) {
                    if constexpr (std::is_const_v<TMessage>) {
                        WalkReflection(*ff.template Get<Message>(i2), onField);
                    } else {
                        WalkReflection(*ff.MutableMessage(i2), onField);
                    }
                }
            }
        }
    }

    void DoWalkSchema(const Descriptor* descriptor,
                      std::function<bool(const FieldDescriptor*)>& onField,
                      THashSet<const Descriptor*>& visited)
    {
        if (!visited.emplace(descriptor).second) {
            return;
        }
        for (int i1 = 0; i1 < descriptor->field_count(); ++i1) {
            const FieldDescriptor* fd = descriptor->field(i1);
            if (!onField(fd)) {
                continue;
            }

            if (fd->type() == FieldDescriptor::Type::TYPE_MESSAGE) {
                DoWalkSchema(fd->message_type(), onField, visited);
            }
        }
        visited.erase(descriptor);
    }

}

namespace NProtoBuf {
    void WalkReflection(Message& msg,
                        std::function<bool(Message&, const FieldDescriptor*)> onField) 
    {
        DoWalkReflection(msg, onField);
    }

    void WalkReflection(const Message& msg,
                        std::function<bool(const Message&, const FieldDescriptor*)> onField)
    {
        DoWalkReflection(msg, onField);
    }

    void WalkSchema(const Descriptor* descriptor,
                    std::function<bool(const FieldDescriptor*)> onField) 
    {
        THashSet<const Descriptor*> visited;
        DoWalkSchema(descriptor, onField, visited);
    }

} // namespace NProtoBuf
