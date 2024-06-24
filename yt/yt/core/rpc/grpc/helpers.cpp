#include "helpers.h"
#include "config.h"

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/crypto/config.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt_proto/yt/core/misc/proto/protobuf_helpers.pb.h>
#include <yt/yt_proto/yt/core/misc/proto/error.pb.h>

#include <yt/yt/core/ytree/node.h>

#include <contrib/libs/grpc/include/grpc/grpc.h>
#include <contrib/libs/grpc/include/grpc/byte_buffer.h>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <contrib/libs/grpc/include/grpc/support/alloc.h>

#include <openssl/pem.h>

#include <array>

namespace NYT::NRpc::NGrpc {

using NYTree::ENodeType;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr ui32 MinusOne = static_cast<ui32>(-1);

////////////////////////////////////////////////////////////////////////////////

TGprString MakeGprString(char* str)
{
    return TGprString(str, gpr_free);
}

TStringBuf ToStringBuf(const grpc_slice& slice)
{
    return TStringBuf(
        reinterpret_cast<const char*>(GRPC_SLICE_START_PTR(slice)),
        GRPC_SLICE_LENGTH(slice));
}

////////////////////////////////////////////////////////////////////////////////

TStringBuf TGrpcMetadataArray::Find(const char* key) const
{
    for (size_t index = 0; index < Native_.count; ++index) {
        const auto& metadata = Native_.metadata[index];
        if (ToStringBuf(metadata.key) == key) {
            return ToStringBuf(metadata.value);
        }
    }

    return TStringBuf();
}

THashMap<TString, TString> TGrpcMetadataArray::ToMap() const
{
    THashMap<TString, TString> result;
    for (size_t index = 0; index < Native_.count; ++index) {
        const auto& metadata = Native_.metadata[index];
        result[NYT::ToString(metadata.key)] = NYT::ToString(metadata.value);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TGrpcSlice::~TGrpcSlice()
{
    grpc_slice_unref(Native_);
}

void TGrpcSlice::Reset(grpc_slice&& other)
{
    grpc_slice_unref(Native_);
    Native_ = std::move(other);
}

grpc_slice* TGrpcSlice::Unwrap()
{
    return &Native_;
}

const ui8* TGrpcSlice::Data() const
{
    return GRPC_SLICE_START_PTR(Native_);
}

size_t TGrpcSlice::Size() const
{
    return GRPC_SLICE_LENGTH(Native_);
}

TString TGrpcSlice::AsString() const
{
    return NYT::ToString(Native_);
}

////////////////////////////////////////////////////////////////////////////////

void TGrpcMetadataArrayBuilder::Add(const char* key, TString value)
{
    Strings_.push_back(TSharedRef::FromString(std::move(value)));
    grpc_metadata metadata;
    metadata.key = grpc_slice_from_static_string(key);
    metadata.value = grpc_slice_from_static_buffer(Strings_.back().Begin(), Strings_.back().Size());
    NativeMetadata_.push_back(metadata);
}

size_t TGrpcMetadataArrayBuilder::GetSize() const
{
    return NativeMetadata_.size();
}

grpc_metadata* TGrpcMetadataArrayBuilder::Unwrap()
{
    return NativeMetadata_.data();
}

////////////////////////////////////////////////////////////////////////////////

TGrpcChannelArgs::TGrpcChannelArgs(const THashMap<TString, NYTree::INodePtr>& args)
{
    for (const auto& pair : args) {
        Items_.emplace_back();
        auto& item = Items_.back();
        const auto& key = pair.first;
        const auto& node = pair.second;
        item.key = const_cast<char*>(key.c_str());

        auto setIntegerValue = [&] (auto value) {
            item.type = GRPC_ARG_INTEGER;
            item.value.integer = static_cast<int>(value);
        };

        auto setStringValue = [&] (const auto& value) {
            item.type = GRPC_ARG_STRING;
            item.value.string = const_cast<char*>(value.c_str());
        };

        switch (node->GetType()) {
            case ENodeType::Int64: {
                auto value = node->AsInt64()->GetValue();
                if (value < std::numeric_limits<int>::min() || value > std::numeric_limits<int>::max()) {
                    THROW_ERROR_EXCEPTION("Value %v of GRPC argument %Qv is out of range",
                        value,
                        key);
                }
                setIntegerValue(value);
                break;
            }
            case ENodeType::Uint64: {
                auto value = node->AsUint64()->GetValue();
                if (value > static_cast<ui64>(std::numeric_limits<int>::max())) {
                    THROW_ERROR_EXCEPTION("Value %v of GRPC argument %Qv is out of range",
                        value,
                        key);
                }
                setIntegerValue(value);
                break;
            }
            case ENodeType::String:
                setStringValue(node->AsString()->GetValue());
                break;
            default:
                THROW_ERROR_EXCEPTION("Invalid type %Qlv of GRPC argument %Qv in channel configuration",
                    node->GetType(),
                    key);
        }
    }

    Native_.num_args = args.size();
    Native_.args = Items_.data();
}

grpc_channel_args* TGrpcChannelArgs::Unwrap()
{
    return &Native_;
}

////////////////////////////////////////////////////////////////////////////////

TGrpcPemKeyCertPair::TGrpcPemKeyCertPair(TString privateKey, TString certChain)
    : PrivateKey_(std::move(privateKey))
    , CertChain_(std::move(certChain))
    , Native_({
        PrivateKey_.c_str(),
        CertChain_.c_str()
    })
{ }

grpc_ssl_pem_key_cert_pair* TGrpcPemKeyCertPair::Unwrap()
{
    return &Native_;
}

////////////////////////////////////////////////////////////////////////////////

TGrpcByteBufferStream::TGrpcByteBufferStream(grpc_byte_buffer* buffer)
    : RemainingBytes_(grpc_byte_buffer_length(buffer))
{
    YT_VERIFY(grpc_byte_buffer_reader_init(&Reader_, buffer) == 1);
}

TGrpcByteBufferStream::~TGrpcByteBufferStream()
{
    grpc_byte_buffer_reader_destroy(&Reader_);
}

bool TGrpcByteBufferStream::ReadNextSlice()
{
    CurrentSlice_.Reset(grpc_empty_slice());

    if (grpc_byte_buffer_reader_next(&Reader_, CurrentSlice_.Unwrap()) == 0) {
        return false;
    }

    AvailableBytes_ = CurrentSlice_.Size();
    return true;
}

size_t TGrpcByteBufferStream::DoRead(void* buf, size_t len)
{
    // NB: Theoretically empty slice can be read, skipping such
    // slices to avoid early EOS.
    while (AvailableBytes_ == 0) {
        if (!ReadNextSlice()) {
            return 0;
        }
    }

    const auto* sliceData = CurrentSlice_.Data();
    auto offset = CurrentSlice_.Size() - AvailableBytes_;

    size_t toRead = std::min(len, AvailableBytes_);
    ::memcpy(buf, sliceData + offset, toRead);
    AvailableBytes_ -= toRead;
    RemainingBytes_ -= toRead;

    return toRead;
}

bool TGrpcByteBufferStream::IsExhausted() const
{
    return RemainingBytes_ == 0;
}

////////////////////////////////////////////////////////////////////////////////

struct TMessageTag
{ };

TMessageWithAttachments ByteBufferToMessageWithAttachments(
    grpc_byte_buffer* buffer,
    std::optional<ui32> messageBodySize,
    bool enveloped)
{
    TMessageWithAttachments result;

    ui32 bufferSize = grpc_byte_buffer_length(buffer);

    // NB: Message body size is not specified, assuming that
    // the whole data is message body.
    if (!messageBodySize) {
        messageBodySize = bufferSize;
    }

    TSharedMutableRef data;
    char* targetMessage;

    if (enveloped) {
        NYT::NProto::TSerializedMessageEnvelope envelope;
        // Codec remains "none".

        TEnvelopeFixedHeader fixedHeader;
        fixedHeader.EnvelopeSize = envelope.ByteSize();
        fixedHeader.MessageSize = *messageBodySize;

        size_t totalMessageSize =
            sizeof (TEnvelopeFixedHeader) +
            fixedHeader.EnvelopeSize +
            fixedHeader.MessageSize;

        data = TSharedMutableRef::Allocate<TMessageTag>(
            totalMessageSize,
            {.InitializeStorage = false});

        char* targetFixedHeader = data.Begin();
        char* targetHeader = targetFixedHeader + sizeof (TEnvelopeFixedHeader);
        targetMessage = targetHeader + fixedHeader.EnvelopeSize;

        memcpy(targetFixedHeader, &fixedHeader, sizeof (fixedHeader));
        YT_VERIFY(envelope.SerializeToArray(targetHeader, fixedHeader.EnvelopeSize));
    } else {
        data = TSharedMutableRef::Allocate<TMessageTag>(
            *messageBodySize,
            {.InitializeStorage = false});

        targetMessage = data.begin();
    }

    TGrpcByteBufferStream stream(buffer);

    if (stream.Load(targetMessage, *messageBodySize) != *messageBodySize) {
        THROW_ERROR_EXCEPTION("Unexpected end of stream while reading message body");
    }

    result.Message = data;

    ui32 attachmentsSize = bufferSize - *messageBodySize;

    if (attachmentsSize == 0) {
        return result;
    }

    auto attachmentsData = TSharedMutableRef::Allocate<TMessageTag>(
        attachmentsSize,
        {.InitializeStorage = false});

    char* attachmentsBuffer = attachmentsData.Begin();

    while (!stream.IsExhausted()) {
        ui32 attachmentSize;

        if (stream.Load(&attachmentSize, sizeof(attachmentSize)) != sizeof(attachmentSize)) {
            THROW_ERROR_EXCEPTION("Unexpected end of stream while reading attachment size");
        }

        if (attachmentSize == MinusOne) {
            result.Attachments.push_back(TSharedRef());
        } else if (attachmentSize == 0) {
            result.Attachments.push_back(TSharedRef::MakeEmpty());
        } else {
            if (stream.Load(attachmentsBuffer, attachmentSize) != attachmentSize) {
                THROW_ERROR_EXCEPTION("Unexpected end of stream while reading message attachment");
            }

            result.Attachments.push_back(
                attachmentsData.Slice(attachmentsBuffer, attachmentsBuffer + attachmentSize));

            attachmentsBuffer += attachmentSize;
        }
    }

    return result;
}

TGrpcByteBufferPtr MessageWithAttachmentsToByteBuffer(const TMessageWithAttachments& messageWithAttachments)
{
    struct THolder
    {
        TSharedRef Data;
    };

    auto sliceFromRef = [] (TSharedRef ref) {
        auto* holder = new THolder();
        holder->Data = std::move(ref);
        return grpc_slice_new_with_user_data(
            const_cast<char*>(holder->Data.Begin()),
            holder->Data.Size(),
            [] (void* untypedHolder) {
                delete static_cast<THolder*>(untypedHolder);
            },
            holder);
    };

    std::vector<grpc_slice> slices;
    slices.reserve(1 + 2 * messageWithAttachments.Attachments.size());

    slices.push_back(sliceFromRef(messageWithAttachments.Message));

    for (const auto& attachment : messageWithAttachments.Attachments) {
        ui32 size = attachment ? attachment.Size() : MinusOne;
        slices.push_back(grpc_slice_from_copied_buffer(reinterpret_cast<const char*>(&size), sizeof(size)));
        if (attachment) {
            slices.push_back(sliceFromRef(attachment));
        }
    }

    auto* buffer = grpc_raw_byte_buffer_create(slices.data(), slices.size());

    for (const auto& slice : slices) {
        grpc_slice_unref(slice);
    }

    return TGrpcByteBufferPtr(buffer);
}

TSharedRef ExtractMessageFromEnvelopedMessage(const TSharedRef& data)
{
    YT_VERIFY(data.Size() >= sizeof(TEnvelopeFixedHeader));
    const auto* fixedHeader = reinterpret_cast<const TEnvelopeFixedHeader*>(data.Begin());
    const char* sourceHeader = data.Begin() + sizeof(TEnvelopeFixedHeader);
    const char* sourceMessage = sourceHeader + fixedHeader->EnvelopeSize;

    NYT::NProto::TSerializedMessageEnvelope envelope;
    YT_VERIFY(envelope.ParseFromArray(sourceHeader, fixedHeader->EnvelopeSize));

    auto compressedMessage = data.Slice(sourceMessage, sourceMessage + fixedHeader->MessageSize);

    auto codecId = CheckedEnumCast<NCompression::ECodec>(envelope.codec());
    auto* codec = NCompression::GetCodec(codecId);
    return codec->Decompress(compressedMessage);
}

TErrorCode StatusCodeToErrorCode(grpc_status_code statusCode)
{
    switch (statusCode) {
        case GRPC_STATUS_OK:
            return NYT::EErrorCode::OK;
        case GRPC_STATUS_CANCELLED:
            return NYT::EErrorCode::Canceled;
        case GRPC_STATUS_DEADLINE_EXCEEDED:
            return NYT::EErrorCode::Timeout;
        case GRPC_STATUS_INVALID_ARGUMENT:
        case GRPC_STATUS_RESOURCE_EXHAUSTED:
            return NRpc::EErrorCode::ProtocolError;
        case GRPC_STATUS_UNAUTHENTICATED:
            return NRpc::EErrorCode::AuthenticationError;
        case GRPC_STATUS_PERMISSION_DENIED:
            return NRpc::EErrorCode::InvalidCredentials;
        case GRPC_STATUS_UNIMPLEMENTED:
            return NRpc::EErrorCode::NoSuchMethod;
        case GRPC_STATUS_UNAVAILABLE:
            return NRpc::EErrorCode::TransportError;
        default:
            // Do not retry request after unclassified error.
            return NYT::EErrorCode::Generic;
    }
}

TString SerializeError(const TError& error)
{
    TString serializedError;
    google::protobuf::io::StringOutputStream output(&serializedError);
    NYT::NProto::TError protoError;
    ToProto(&protoError, error);
    YT_VERIFY(protoError.SerializeToZeroCopyStream(&output));
    return serializedError;
}

TError DeserializeError(TStringBuf serializedError)
{
    NYT::NProto::TError protoError;
    google::protobuf::io::ArrayInputStream input(serializedError.data(), serializedError.size());
    if (!protoError.ParseFromZeroCopyStream(&input)) {
        THROW_ERROR_EXCEPTION("Error deserializing error");
    }
    return FromProto<TError>(protoError);
}

TGrpcPemKeyCertPair LoadPemKeyCertPair(const TSslPemKeyCertPairConfigPtr& config)
{
    return TGrpcPemKeyCertPair(
        config->PrivateKey->LoadBlob(),
        config->CertChain->LoadBlob());
}

TGrpcChannelCredentialsPtr LoadChannelCredentials(const TChannelCredentialsConfigPtr& config)
{
    TString rootCerts;
    TString identityCerts;
    TString identityPrivateKey;
    if (config->PemRootCerts) {
        rootCerts = config->PemRootCerts->LoadBlob();
    }
    if (config->PemKeyCertPair && config->PemKeyCertPair->CertChain && config->PemKeyCertPair->PrivateKey) {
        identityCerts = config->PemKeyCertPair->CertChain->LoadBlob();
        identityPrivateKey = config->PemKeyCertPair->PrivateKey->LoadBlob();
    }

    grpc_tls_identity_pairs* tlsPairs = nullptr;
    if (identityCerts && identityPrivateKey) {
        tlsPairs = grpc_tls_identity_pairs_create();
        grpc_tls_identity_pairs_add_pair(
            tlsPairs,
            identityPrivateKey.c_str(),
            identityCerts.c_str());
    }

    TGrpcTlsCertificateProviderPtr certProvider(grpc_tls_certificate_provider_static_data_create(
        rootCerts ? rootCerts.c_str() : nullptr,
        tlsPairs));

    grpc_tls_credentials_options* tlsOptions = grpc_tls_credentials_options_create();
    grpc_tls_credentials_options_set_verify_server_cert(tlsOptions, config->VerifyServerCert);
    grpc_tls_credentials_options_set_certificate_provider(tlsOptions, certProvider.Unwrap());
    if (rootCerts) {
        grpc_tls_credentials_options_watch_root_certs(tlsOptions);
    }
    if (identityCerts && identityPrivateKey) {
        grpc_tls_credentials_options_watch_identity_key_cert_pairs(tlsOptions);
    }

    return TGrpcChannelCredentialsPtr(grpc_tls_credentials_create(tlsOptions));
}

TGrpcServerCredentialsPtr LoadServerCredentials(const TServerCredentialsConfigPtr& config)
{
    auto rootCerts = config->PemRootCerts ? config->PemRootCerts->LoadBlob() : TString();
    std::vector<TGrpcPemKeyCertPair> keyCertPairs;
    std::vector<grpc_ssl_pem_key_cert_pair> nativeKeyCertPairs;
    for (const auto& pairConfig : config->PemKeyCertPairs) {
        keyCertPairs.push_back(LoadPemKeyCertPair(pairConfig));
        nativeKeyCertPairs.push_back(*keyCertPairs.back().Unwrap());
    }
    return TGrpcServerCredentialsPtr(grpc_ssl_server_credentials_create_ex(
        rootCerts ? rootCerts.c_str() : nullptr,
        nativeKeyCertPairs.data(),
        nativeKeyCertPairs.size(),
        static_cast<grpc_ssl_client_certificate_request_type>(config->ClientCertificateRequest),
        nullptr));
}

std::optional<TString> ParseIssuerFromX509(TStringBuf x509String)
{
    auto* bio = BIO_new(BIO_s_mem());
    auto bioGuard = Finally([&] {
        BIO_free(bio);
    });

    BIO_write(bio, x509String.data(), x509String.length());

    auto* x509 = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
    auto x509Guard = Finally([&] {
        X509_free(x509);
    });

    if (!x509) {
        return std::nullopt;
    }

    auto* issuerName = X509_get_issuer_name(x509);

    std::array<char, 1024> buf;
    auto* issuerString = X509_NAME_oneline(issuerName, buf.data(), buf.size());
    if (!issuerString) {
        return std::nullopt;
    }

    return TString(issuerString);
}

////////////////////////////////////////////////////////////////////////////////

TGuardedGrpcCompletionQueue::TGuardedGrpcCompletionQueue(TGrpcCompletionQueuePtr completionQueue)
    : CompletionQueue_(std::move(completionQueue))
{ }

std::optional<TGuardedGrpcCompletionQueue::TLockGuard> TGuardedGrpcCompletionQueue::TryLock()
{
    auto guard = ReaderGuard(SpinLock_);
    if (State_ != EState::Opened) {
        return std::nullopt;
    }
    LocksCount_.fetch_add(1, std::memory_order::acquire);
    return std::optional<TGuardedGrpcCompletionQueue::TLockGuard>(this);
}

void TGuardedGrpcCompletionQueue::Shutdown()
{
    {
        auto guard = WriterGuard(SpinLock_);
        if (State_ == EState::Shutdown) {
            return;
        }
        State_ = EState::Shutdown;
        if (LocksCount_.load() != 0) {
            guard.Release();
            ReleaseDone_.Wait();
        }
    }
    grpc_completion_queue_shutdown(CompletionQueue_.Unwrap());
}

void TGuardedGrpcCompletionQueue::Reset()
{
    auto guard = WriterGuard(SpinLock_);
    YT_VERIFY(State_ == EState::Shutdown);
    CompletionQueue_.Reset();
}

grpc_completion_queue* TGuardedGrpcCompletionQueue::UnwrapUnsafe()
{
    return CompletionQueue_.Unwrap();
}

void TGuardedGrpcCompletionQueue::Release()
{
    auto guard = ReaderGuard(SpinLock_);
    if (LocksCount_.fetch_sub(1, std::memory_order::release) == 1 && State_ == EState::Shutdown) {
        guard.Release();
        ReleaseDone_.NotifyOne();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc

void FormatValue(NYT::TStringBuilderBase* builder, const grpc_slice& slice, TStringBuf spec)
{
    FormatValue(builder, NYT::NRpc::NGrpc::ToStringBuf(slice), spec);
}
