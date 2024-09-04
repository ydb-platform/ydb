#ifndef REQUESTS_INL_H_
#error "Direct inclusion of this file is not allowed, include requests.h"
// For the sake of sane code completion.
#include "requests.h"
#endif
#undef REQUESTS_INL_H_

namespace NYT::NKafka {

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename ...Args>
void Serialize(const std::vector<T>& data, IKafkaProtocolWriter* writer, bool isCompact, Args&&... args)
{
    if (isCompact) {
        auto size = data.size();
        if constexpr (!std::is_same_v<T, TTaggedField>) {
            ++size;
        }
        writer->WriteUnsignedVarInt(size);
    } else {
        writer->WriteInt32(data.size());
    }
    for (const auto& item : data) {
        item.Serialize(writer, args...);
    }
}

template <typename T, typename ...Args>
void Deserialize(std::vector<T>& data, IKafkaProtocolReader* reader, bool isCompact, Args&&...args)
{
    if (isCompact) {
        auto size = reader->ReadUnsignedVarInt();
        if (size == 0) {
            return;
        }
        if constexpr (!std::is_same_v<T, TTaggedField>) {
            --size;
        }
        data.resize(size);
    } else {
        data.resize(reader->ReadInt32());
    }
    for (auto& item : data) {
        item.Deserialize(reader, args...);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafka
