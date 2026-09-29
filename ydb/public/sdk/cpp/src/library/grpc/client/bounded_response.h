#pragma once

#include <google/protobuf/message.h>
#include <grpcpp/impl/serialization_traits.h>
#include <grpcpp/support/async_stream.h>
#include <grpcpp/support/async_unary_call.h>
#include <grpcpp/support/byte_buffer.h>

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>

namespace NYdbGrpc::inline Dev {

    // Bounded mode deliberately accepts a strict subset of protobuf wire encodings.
    // Validation runs before generated protobuf parsers allocate response objects.
    struct TBoundedResponseLimits {
        static constexpr size_t StreamBytes = 8 * 1024 * 1024;
        static constexpr size_t UnaryBytes = 256 * 1024;
        static constexpr size_t Fields = 4096;
        static constexpr size_t UnaryFields = 8192;
        static constexpr size_t Depth = 32;
        static constexpr size_t MessageBytes = 1024;
    };

    bool ValidateBoundedProtobuf(std::string_view wire, const google::protobuf::Descriptor* descriptor,
                                 size_t maxBytes);

    grpc::Status DeserializeBoundedProtobuf(grpc::ByteBuffer* buffer, google::protobuf::Message* message,
                                            size_t maxBytes);

    struct TBoundedResponse {
        google::protobuf::Message* Message = nullptr;
        size_t MaxBytes = TBoundedResponseLimits::StreamBytes;
        grpc::Status* DecodeStatus = nullptr;
    };

} // namespace NYdbGrpc::inline Dev

namespace grpc {

    template <>
    class SerializationTraits<NYdbGrpc::TBoundedResponse> {
    public:
        static Status Deserialize(ByteBuffer* buffer, NYdbGrpc::TBoundedResponse* response) {
            auto status = NYdbGrpc::DeserializeBoundedProtobuf(buffer, response->Message, response->MaxBytes);
            if (response->DecodeStatus) {
                *response->DecodeStatus = status;
            }
            return status;
        }
    };

} // namespace grpc

namespace NYdbGrpc::inline Dev {

    template <typename TResponse>
    class TBoundedAsyncReader final: public grpc::ClientAsyncReaderInterface<TResponse> {
    public:
        template <typename TRequest>
        TBoundedAsyncReader(grpc::ChannelInterface* channel, grpc::CompletionQueue* queue,
                            const std::string& method, grpc::ClientContext* context, const TRequest& request, void* tag, grpc::Status* decodeStatus)
            : Reader_(grpc::internal::ClientAsyncReaderFactory<TBoundedResponse>::Create(channel, queue,
                                                                                         grpc::internal::RpcMethod(method.c_str(), grpc::internal::RpcMethod::SERVER_STREAMING),
                                                                                         context, request, true, tag))
        {
            Response_.DecodeStatus = decodeStatus;
        }

        void StartCall(void* tag) override {
            Reader_->StartCall(tag);
        }

        void ReadInitialMetadata(void* tag) override {
            Reader_->ReadInitialMetadata(tag);
        }

        void Read(TResponse* response, void* tag) override {
            Response_.Message = response;
            Reader_->Read(&Response_, tag);
        }

        void Finish(grpc::Status* status, void* tag) override {
            Reader_->Finish(status, tag);
        }

    private:
        TBoundedResponse Response_;
        std::unique_ptr<grpc::ClientAsyncReader<TBoundedResponse>> Reader_;
    };

    template <typename TResponse>
    class TBoundedAsyncResponseReader final: public grpc::ClientAsyncResponseReaderInterface<TResponse> {
    public:
        template <typename TRequest>
        TBoundedAsyncResponseReader(grpc::ChannelInterface* channel, grpc::CompletionQueue* queue,
                                    const std::string& method, grpc::ClientContext* context, const TRequest& request, grpc::Status* decodeStatus)
            : Reader_(grpc::internal::ClientAsyncResponseReaderFactory<TBoundedResponse>::Create(channel, queue,
                                                                                                 grpc::internal::RpcMethod(method.c_str(), grpc::internal::RpcMethod::NORMAL_RPC),
                                                                                                 context, request, true))
        {
            Response_.MaxBytes = TBoundedResponseLimits::UnaryBytes;
            Response_.DecodeStatus = decodeStatus;
        }

        void StartCall() override {
            Reader_->StartCall();
        }

        void ReadInitialMetadata(void* tag) override {
            Reader_->ReadInitialMetadata(tag);
        }

        void Finish(TResponse* response, grpc::Status* status, void* tag) override {
            Response_.Message = response;
            Reader_->Finish(&Response_, status, tag);
        }

    private:
        TBoundedResponse Response_;
        std::unique_ptr<grpc::ClientAsyncResponseReader<TBoundedResponse>> Reader_;
    };

} // namespace NYdbGrpc::inline Dev
