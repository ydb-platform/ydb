#include <ydb/core/fq/libs/wasm_services/profile.h>
#include <ydb/core/fq/libs/wasm_services/wire.h>
#include <ydb/services/udf_store/wasm/abi/async.h>

#include "proto/profile.pb.h"

#include <pb_decode.h>
#include <pb_encode.h>
#include <rapidjson/memorystream.h>
#include <rapidjson/reader.h>

#include <memory>
#include <new>
#include <optional>

using namespace NYdb::NWasm::NAsync;
using namespace NFq::NWasmServices;

namespace {

// Reusable fixture-only storage: observable lifetime without libc++ string
// imports.
constexpr unsigned Slots = 32;
alignas(16) unsigned char Arena[Slots][17408];
bool Used[Slots];
uint64_t LiveObjects = 0;

} // namespace

void* operator new(std::size_t size) {
    if (size > sizeof(Arena[0])) {
        __builtin_trap();
    }
    for (unsigned i = 0; i < Slots; ++i) {
        if (!Used[i]) {
            Used[i] = true;
            ++LiveObjects;
            return Arena[i];
        }
    }
    __builtin_trap();
}

void* operator new(std::size_t size, std::align_val_t alignment) {
    if (static_cast<size_t>(alignment) > 16) {
        __builtin_trap();
    }
    return ::operator new(size);
}

void operator delete(void* pointer) noexcept {
    if (!pointer) {
        return;
    }
    const auto offset = reinterpret_cast<uintptr_t>(pointer) - reinterpret_cast<uintptr_t>(Arena);
    const auto index = offset / sizeof(Arena[0]);
    if (index >= Slots || offset % sizeof(Arena[0]) || !Used[index]) {
        __builtin_trap();
    }
    Used[index] = false;
    --LiveObjects;
}

void operator delete(void* pointer, std::size_t) noexcept {
    ::operator delete(pointer);
}

void operator delete(void* pointer, std::align_val_t) noexcept {
    ::operator delete(pointer);
}

void operator delete(void* pointer, std::size_t, std::align_val_t) noexcept {
    ::operator delete(pointer);
}

void* operator new[](std::size_t size) {
    return ::operator new(size);
}

void* operator new[](std::size_t size, std::align_val_t alignment) {
    return ::operator new(size, alignment);
}

void operator delete[](void* pointer) noexcept {
    ::operator delete(pointer);
}

void operator delete[](void* pointer, std::size_t) noexcept {
    ::operator delete(pointer);
}

void operator delete[](void* pointer, std::align_val_t alignment) noexcept {
    ::operator delete(pointer, alignment);
}

void operator delete[](void* pointer, std::size_t size, std::align_val_t alignment) noexcept {
    ::operator delete(pointer, size, alignment);
}

namespace {

struct TBytes {
    std::unique_ptr<char[]> Buffer;
    size_t Size = 0;

    TBytes() = default;
    explicit TBytes(size_t size)
        : Buffer(size ? new char[size] : nullptr), Size(size)
    {
    }

    std::string_view View() const {
        return {Buffer.get(), Size};
    }
};

template <class T> TBytes Pack(const T& header, std::string_view payload) {
    TBytes bytes(sizeof(T) + payload.size());
    std::memcpy(bytes.Buffer.get(), &header, sizeof(T));
    if (!payload.empty()) {
        std::memcpy(bytes.Buffer.get() + sizeof(T), payload.data(), payload.size());
    }
    return bytes;
}

struct TReply {
    EOperationStatus Status;
    TBytes Bytes;
};

bool HasZeroByte(const char* data, size_t size) {
    for (size_t i = 0; i < size; ++i) {
        if (!data[i])
            return true;
    }
    return false;
}

struct TProfileJsonHandler : rapidjson::BaseReaderHandler<rapidjson::UTF8<>, TProfileJsonHandler> {
    TProfile Profile;
    uint8_t Seen = 0;
    bool Valid = true;
    bool ProfileValid = true;
    bool InObject = false;
    enum class EField { None, Id, Name, Score, Version } Field = EField::None;

    bool StartObject() {
        return Valid && !InObject ? (InObject = true) : false;
    }
    bool EndObject(rapidjson::SizeType) {
        if (!Valid || !InObject)
            return Valid = false;
        InObject = false;
        return true;
    }
    bool Key(const char* value, rapidjson::SizeType size, bool) {
        if (!InObject || Field != EField::None)
            return Valid = false;
        const auto equals = [value, size](const char* expected, size_t expectedSize) {
            if (size != expectedSize)
                return false;
            for (size_t i = 0; i < size; ++i) {
                if (value[i] != expected[i])
                    return false;
            }
            return true;
        };
        if (equals("id", 2) && !(Seen & 1))
            Field = EField::Id;
        else if (equals("name", 4) && !(Seen & 2))
            Field = EField::Name;
        else if (equals("score", 5) && !(Seen & 4))
            Field = EField::Score;
        else if (equals("version", 7) && !(Seen & 8))
            Field = EField::Version;
        else
            return Valid = false;
        return true;
    }
    bool String(const char* value, rapidjson::SizeType size, bool) {
        if (Field != EField::Name)
            return Valid = false;
        Seen |= 2;
        Field = EField::None;
        if (!size || size > MaxProfileNameBytes || HasZeroByte(value, size)) {
            ProfileValid = false;
            return true;
        }
        std::memcpy(Profile.Name, value, size);
        Profile.NameBytes = size;
        return true;
    }
    bool Uint64(uint64_t value) {
        if (Field == EField::Id) {
            Profile.Id = value;
            Seen |= 1;
            ProfileValid &= value != 0;
        } else if (Field == EField::Score) {
            ProfileValid &= value <= 100;
            Profile.Score = value <= UINT32_MAX ? value : 0;
            Seen |= 4;
        } else if (Field == EField::Version) {
            ProfileValid &= value == ProfileVersion;
            Seen |= 8;
        } else
            return Valid = false;
        Field = EField::None;
        return true;
    }
    bool Uint(unsigned value) {
        return Uint64(value);
    }
    bool Int(int value) {
        return value >= 0 ? Uint64(value) : (Valid = false);
    }
    bool Int64(int64_t value) {
        return value >= 0 ? Uint64(value) : (Valid = false);
    }
    bool Double(double) {
        return Valid = false;
    }
    bool Bool(bool) {
        return Valid = false;
    }
    bool Null() {
        return Valid = false;
    }
    bool StartArray() {
        return Valid = false;
    }
    bool EndArray(rapidjson::SizeType) {
        return Valid = false;
    }
};

struct TFixedJsonStackAllocator {
    alignas(16) char Buffer[4096];

    void* Malloc(size_t size) {
        return size <= sizeof(Buffer) ? Buffer : nullptr;
    }
    void* Realloc(void* original, size_t, size_t newSize) {
        return (!original || original == Buffer) && newSize <= sizeof(Buffer) ? Buffer : nullptr;
    }
    static void Free(void*) {
    }
};

EServiceError DecodeHttpProfile(std::string_view bytes, TProfile& profile) {
    rapidjson::MemoryStream stream(bytes.data(), bytes.size());
    TFixedJsonStackAllocator allocator;
    rapidjson::GenericReader<rapidjson::UTF8<>, rapidjson::UTF8<>, TFixedJsonStackAllocator> reader(&allocator, 256);
    TProfileJsonHandler handler;
    const auto result = reader.Parse<rapidjson::kParseValidateEncodingFlag>(stream, handler);
    if (!result)
        return EServiceError::Decode;
    if (!handler.Valid || handler.InObject || handler.Field != TProfileJsonHandler::EField::None || handler.Seen != 15) {
        return EServiceError::Decode;
    }
    if (!handler.ProfileValid)
        return EServiceError::InvalidProfile;
    profile = handler.Profile;
    return EServiceError::None;
}

bool IsValidUtf8(const char* data, size_t size) {
    const auto* bytes = reinterpret_cast<const uint8_t*>(data);
    for (size_t i = 0; i < size;) {
        const uint8_t first = bytes[i++];
        if (first < 0x80)
            continue;
        size_t continuation = 0;
        uint8_t lower = 0x80, upper = 0xbf;
        if (first >= 0xc2 && first <= 0xdf)
            continuation = 1;
        else if (first == 0xe0) {
            continuation = 2;
            lower = 0xa0;
        } else if (first >= 0xe1 && first <= 0xec)
            continuation = 2;
        else if (first == 0xed) {
            continuation = 2;
            upper = 0x9f;
        } else if (first >= 0xee && first <= 0xef)
            continuation = 2;
        else if (first == 0xf0) {
            continuation = 3;
            lower = 0x90;
        } else if (first >= 0xf1 && first <= 0xf3)
            continuation = 3;
        else if (first == 0xf4) {
            continuation = 3;
            upper = 0x8f;
        } else
            return false;
        if (continuation > size - i || bytes[i] < lower || bytes[i] > upper)
            return false;
        ++i;
        for (size_t j = 1; j < continuation; ++j, ++i) {
            if (bytes[i] < 0x80 || bytes[i] > 0xbf)
                return false;
        }
    }
    return true;
}

EServiceError DecodeGrpcProfile(std::string_view bytes, TProfile& profile) {
    NFq_NWasmServices_NTest_ProfileReply reply = NFq_NWasmServices_NTest_ProfileReply_init_zero;
    auto input = pb_istream_from_buffer(reinterpret_cast<const pb_byte_t*>(bytes.data()), bytes.size());
    if (!pb_decode(&input, NFq_NWasmServices_NTest_ProfileReply_fields, &reply) || input.bytes_left)
        return EServiceError::Decode;
    NFq_NWasmServices_NTest_Profile value = NFq_NWasmServices_NTest_Profile_init_zero;
    auto payload = pb_istream_from_buffer(reply.payload.bytes, reply.payload.size);
    if (!pb_decode(&payload, NFq_NWasmServices_NTest_Profile_fields, &value) || payload.bytes_left)
        return EServiceError::Decode;
    if (!value.has_id || !value.id || !value.has_name || !value.name.size || !value.has_score || value.score > 100 || !value.has_version ||
        value.version != ProfileVersion || value.name.size > MaxProfileNameBytes ||
        HasZeroByte(reinterpret_cast<const char*>(value.name.bytes), value.name.size) ||
        !IsValidUtf8(reinterpret_cast<const char*>(value.name.bytes), value.name.size))
        return EServiceError::InvalidProfile;
    profile.Id = value.id;
    profile.Score = value.score;
    profile.NameBytes = value.name.size;
    std::memcpy(profile.Name, value.name.bytes, profile.NameBytes);
    return EServiceError::None;
}

TProfileResult ProfileFailure(EServiceError error, const TReply& reply) {
    TProfileResult result;
    result.Error = error;
    TResponseHeader header;
    std::string_view payload;
    if (Decode(reply.Bytes.View(), header, payload)) {
        result.ClientError = header.Error;
        result.Code = header.Code;
        result.NativeCode = header.NativeCode;
    }
    return result;
}

TProfileResult DecodeProfileResponse(TOperation& operation, EOperationStatus status, bool grpc) {
    if (operation.Size() > MaxProfilePayloadBytes + sizeof(TResponseHeader))
        return ProfileFailure(EServiceError::Transport, {});
    TReply reply{status, TBytes(operation.Size())};
    operation.Read(reply.Bytes.Buffer.get(), reply.Bytes.Size);
    TResponseHeader header;
    std::string_view responsePayload;
    if (!Decode(reply.Bytes.View(), header, responsePayload) || header.Version != WireVersion ||
        header.PayloadBytes != responsePayload.size())
        return ProfileFailure(EServiceError::Transport, reply);
    if (status != EOperationStatus::Ready || header.Error != EClientError::None)
        return ProfileFailure(EServiceError::Transport, reply);
    TProfileResult result;
    result.Error = grpc ? DecodeGrpcProfile(responsePayload, result.Profile) : DecodeHttpProfile(responsePayload, result.Profile);
    return result;
}

TBytes ProfileRequestBytes(uint64_t id, bool grpc) {
    if (!grpc) {
        char text[64];
        char* cursor = text + sizeof(text);
        do {
            *--cursor = static_cast<char>('0' + id % 10);
            id /= 10;
        } while (id);
        TBytes bytes(static_cast<size_t>(text + sizeof(text) - cursor) + 7);
        const auto digits = static_cast<size_t>(text + sizeof(text) - cursor);
        bytes.Buffer[0] = '{';
        std::memcpy(bytes.Buffer.get() + 1, "\"id\":", 5);
        std::memcpy(bytes.Buffer.get() + 6, cursor, digits);
        bytes.Buffer[6 + digits] = '}';
        bytes.Size = 7 + digits;
        return bytes;
    }
    NFq_NWasmServices_NTest_ProfileRequest value = NFq_NWasmServices_NTest_ProfileRequest_init_zero;
    value.id = id;
    TBytes bytes(NFq_NWasmServices_NTest_ProfileRequest_size);
    auto output = pb_ostream_from_buffer(reinterpret_cast<pb_byte_t*>(bytes.Buffer.get()), bytes.Size);
    if (!pb_encode(&output, NFq_NWasmServices_NTest_ProfileRequest_fields, &value))
        __builtin_trap();
    bytes.Size = output.bytes_written;
    return bytes;
}

TReply ProfileResultReply(TProfileResult first, TProfileResult* second = nullptr) {
    TProfileResults results;
    results.Count = second ? 2 : 1;
    results.Items[0] = first;
    if (second)
        results.Items[1] = *second;
    return {EOperationStatus::Ready, Pack(results, {})};
}

TTask<TProfileResult> FetchProfile(TCallContext& context, uint32_t binding, uint64_t id, bool grpc) {
    auto requestPayload = ProfileRequestBytes(id, grpc);
    const auto request = Pack(TRequestHeader{WireVersion, binding, requestPayload.Size}, requestPayload.View());
    TOperation operation(request.Buffer.get(), request.Size);
    const auto status = co_await operation.Wait(context);
    co_return DecodeProfileResponse(operation, status, grpc);
}

TTask<TReply> RunProfile(TCallContext& context, const TArgumentsHeader* args) {
    uint64_t id = 0;
    const auto body = std::string_view(reinterpret_cast<const char*>(args + 1), args->PayloadBytes);
    if (body.size() != sizeof(id)) {
        co_return ProfileResultReply(ProfileFailure(EServiceError::InvalidArguments, {}));
    }
    std::memcpy(&id, body.data(), sizeof(id));
    if (!id)
        co_return ProfileResultReply(ProfileFailure(EServiceError::InvalidArguments, {}));
    const auto mode = static_cast<EProfileMode>(args->Mode);
    if (mode == EProfileMode::Http || mode == EProfileMode::Grpc) {
        auto result = co_await FetchProfile(context, args->BindingA, id, mode == EProfileMode::Grpc);
        co_return ProfileResultReply(result);
    }
    if (mode == EProfileMode::Parallel) {
        auto httpPayload = ProfileRequestBytes(id, false);
        auto grpcPayload = ProfileRequestBytes(id, true);
        const auto httpRequest = Pack(TRequestHeader{WireVersion, args->BindingA, httpPayload.Size}, httpPayload.View());
        const auto grpcRequest = Pack(TRequestHeader{WireVersion, args->BindingB, grpcPayload.Size}, grpcPayload.View());
        TOperation httpOperation(httpRequest.Buffer.get(), httpRequest.Size);
        TOperation grpcOperation(grpcRequest.Buffer.get(), grpcRequest.Size);
        const auto httpStatus = co_await httpOperation.Wait(context);
        const auto grpcStatus = co_await grpcOperation.Wait(context);
        auto first = DecodeProfileResponse(httpOperation, httpStatus, false);
        auto second = DecodeProfileResponse(grpcOperation, grpcStatus, true);
        co_return ProfileResultReply(first, &second);
    }
    const bool firstGrpc = mode == EProfileMode::GrpcThenHttp;
    auto first = co_await FetchProfile(context, args->BindingA, id, firstGrpc);
    if (first.Error != EServiceError::None)
        co_return ProfileResultReply(first);
    auto second = co_await FetchProfile(context, args->BindingB, first.Profile.Id, !firstGrpc);
    co_return ProfileResultReply(first, &second);
}

TTask<TReply> Fetch(TCallContext& context, uint32_t binding, std::string_view body) {
    const auto request = Pack(TRequestHeader{WireVersion, binding, body.size()}, body);
    TOperation operation(request.Buffer.get(), request.Size);
    auto status = co_await operation.Wait(context);
    TReply reply{status, {}};
    const auto size = operation.Size();
    if (size > 8192) {
        co_return TReply{EOperationStatus::Failed, {}};
    }
    reply.Bytes = TBytes(size);
    operation.Read(reply.Bytes.Buffer.get(), size);
    TResponseHeader header;
    std::string_view payload;
    if (!Decode(reply.Bytes.View(), header, payload) || header.Version != WireVersion || header.PayloadBytes != payload.size()) {
        reply.Status = EOperationStatus::Failed;
    }
    co_return reply;
}

TReply Collect(TReply first, std::optional<TReply> second = {}) {
    TResultHeader header{WireVersion, second ? 2u : 1u, first.Bytes.Size, second ? second->Bytes.Size : 0};
    TBytes result(sizeof(header) + header.FirstBytes + header.SecondBytes);
    std::memcpy(result.Buffer.get(), &header, sizeof(header));
    if (header.FirstBytes) {
        std::memcpy(result.Buffer.get() + sizeof(header), first.Bytes.Buffer.get(), header.FirstBytes);
    }
    if (header.SecondBytes) {
        std::memcpy(result.Buffer.get() + sizeof(header) + header.FirstBytes, second->Bytes.Buffer.get(), header.SecondBytes);
    }
    return {first.Status == EOperationStatus::Ready && (!second || second->Status == EOperationStatus::Ready) ? EOperationStatus::Ready
                                                                                                              : EOperationStatus::Failed,
            std::move(result)};
}

TTask<TReply> Run(TCallContext& context, const TArgumentsHeader* args) {
    if (args->Mode >= static_cast<uint64_t>(EProfileMode::Http)) {
        co_return co_await RunProfile(context, args);
    }
    const auto body = std::string_view(reinterpret_cast<const char*>(args + 1), args->PayloadBytes);
    if (args->Mode == 0) {
        co_return Collect(co_await Fetch(context, args->BindingA, body));
    }
    if (args->Mode == 1) {
        auto first = co_await Fetch(context, args->BindingA, body);
        if (first.Status != EOperationStatus::Ready) {
            co_return Collect(std::move(first));
        }
        TResponseHeader header;
        std::string_view payload;
        if (!Decode(first.Bytes.View(), header, payload)) {
            __builtin_trap();
        }
        auto second = co_await Fetch(context, args->BindingB, payload);
        co_return Collect(std::move(first), std::move(second));
    }
    // Start both operations before awaiting either: transport can run
    // concurrently.
    const auto requestA = Pack(TRequestHeader{WireVersion, args->BindingA, body.size()}, body);
    const auto requestB = Pack(TRequestHeader{WireVersion, args->BindingB, body.size()}, body);
    TOperation a(requestA.Buffer.get(), requestA.Size);
    TOperation b(requestB.Buffer.get(), requestB.Size);
    auto statusB = co_await b.Wait(context);
    auto statusA = co_await a.Wait(context);
    if (a.Size() > 8192 || b.Size() > 8192) {
        co_return TReply{EOperationStatus::Failed, {}};
    }
    TReply first{statusA, TBytes(a.Size())};
    TReply second{statusB, TBytes(b.Size())};
    a.Read(first.Bytes.Buffer.get(), first.Bytes.Size);
    b.Read(second.Bytes.Buffer.get(), second.Bytes.Size);
    co_return Collect(std::move(first), std::move(second));
}

struct TCall {
    TCallContext Context;
    std::optional<TTask<TReply>> Task;
    explicit TCall(const TArgumentsHeader* args) {
        Task.emplace(Run(Context, args));
        Context.Runnable = Task->Handle();
    }
};

} // namespace

extern "C" uint32_t WasmAsyncAbiVersion() {
    return AbiVersion;
}

extern "C" uint64_t WasmAsyncCallStart(uint64_t arguments, uint64_t size) {
    if (size < sizeof(TArgumentsHeader)) {
        __builtin_trap();
    }
    const auto* args = reinterpret_cast<const TArgumentsHeader*>(arguments);
    if (args->PayloadBytes != size - sizeof(*args) || args->Mode > static_cast<uint64_t>(EProfileMode::Parallel)) {
        __builtin_trap();
    }
    return reinterpret_cast<uint64_t>(new TCall(args));
}

extern "C" void WasmAsyncCallPoll(uint64_t frame) {
    auto& call = *reinterpret_cast<TCall*>(frame);
    call.Context.Resume();
    if (call.Task->Done()) {
        auto reply = call.Task->TakeResult();
        WasmAsyncCallComplete(reply.Bytes.Buffer.get(), reply.Bytes.Size, static_cast<uint32_t>(reply.Status));
    }
}

extern "C" void WasmAsyncCallCancel(uint64_t frame) {
    reinterpret_cast<TCall*>(frame)->Task.reset();
}

extern "C" void WasmAsyncCallDrop(uint64_t frame) {
    delete reinterpret_cast<TCall*>(frame);
}

extern "C" uint64_t WasmAsyncLiveObjects() {
    return LiveObjects;
}
