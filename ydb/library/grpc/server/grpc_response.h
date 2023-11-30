#pragma once

#include <grpc++/impl/codegen/byte_buffer.h>
#include <grpc++/impl/codegen/proto_utils.h>

#include <variant>

namespace NYdbGrpc {

/**
 * Universal response that owns underlying message or buffer.
 */
template <typename TMsg>
class TUniversalResponse: public TAtomicRefCount<TUniversalResponse<TMsg>>, public TMoveOnly {
    friend class grpc::SerializationTraits<TUniversalResponse<TMsg>>;

public:
    explicit TUniversalResponse(NProtoBuf::Message* msg) noexcept
        : Data_{TMsg{}}
    {
        std::get<TMsg>(Data_).Swap(static_cast<TMsg*>(msg));
    }

    explicit TUniversalResponse(grpc::ByteBuffer* buffer) noexcept
        : Data_{grpc::ByteBuffer{}}
    {
        std::get<grpc::ByteBuffer>(Data_).Swap(buffer);
    }

private:
    std::variant<TMsg, grpc::ByteBuffer> Data_;
};

/**
 * Universal response that only keeps reference to underlying message or buffer.
 */
template <typename TMsg>
class TUniversalResponseRef: private TMoveOnly {
    friend class grpc::SerializationTraits<TUniversalResponseRef<TMsg>>;

public:
    explicit TUniversalResponseRef(const NProtoBuf::Message* msg)
        : Data_{msg}
    {
    }

    explicit TUniversalResponseRef(const grpc::ByteBuffer* buffer)
        : Data_{buffer}
    {
    }

private:
    std::variant<const NProtoBuf::Message*, const grpc::ByteBuffer*> Data_;
};

} // namespace NYdbGrpc

namespace grpc {

template <typename TMsg>
class SerializationTraits<NYdbGrpc::TUniversalResponse<TMsg>> {
public:
    static Status Serialize(
            const NYdbGrpc::TUniversalResponse<TMsg>& resp,
            ByteBuffer* buffer,
            bool* ownBuffer)
    {
        return std::visit([&](const auto& data) {
            using T = std::decay_t<decltype(data)>;
            return SerializationTraits<T>::Serialize(data, buffer, ownBuffer);
        }, resp.Data_);
    }
};

template <typename TMsg>
class SerializationTraits<NYdbGrpc::TUniversalResponseRef<TMsg>> {
public:
    static Status Serialize(
        const NYdbGrpc::TUniversalResponseRef<TMsg>& resp,
        ByteBuffer* buffer,
        bool* ownBuffer)
    {
        return std::visit([&](const auto* data) {
            using T = std::decay_t<std::remove_pointer_t<decltype(data)>>;
            return SerializationTraits<T>::Serialize(*data, buffer, ownBuffer);
        }, resp.Data_);
    }
};

} // namespace grpc
