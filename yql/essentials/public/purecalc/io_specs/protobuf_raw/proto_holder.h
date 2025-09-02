#pragma once

#include <google/protobuf/arena.h>

#include <util/generic/ptr.h>

#include <type_traits>

namespace NYql::NPureCalc {
    class TProtoDestroyer {
    public:
        template <typename T>
        static inline void Destroy(T* t) noexcept {
            if (t->GetArena() == nullptr) {
                CheckedDelete(t);
            }
        }
    };

    template <typename TProto>
    concept IsProtoMessage = std::is_base_of_v<NProtoBuf::Message, TProto>;

    template <IsProtoMessage TProto>
    using TProtoHolder = THolder<TProto, TProtoDestroyer>;

    template <IsProtoMessage TProto, typename... TArgs>
    TProtoHolder<TProto> MakeProtoHolder(NProtoBuf::Arena* arena, TArgs&&... args) {
        auto* ptr = NProtoBuf::Arena::CreateMessage<TProto>(arena, std::forward<TArgs>(args)...);
        return TProtoHolder<TProto>(ptr);
    }
}
