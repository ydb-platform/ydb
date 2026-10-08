#pragma once

#include <bit>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <type_traits>

namespace NFq::NWasmServices {

// Internal P2 harness protocol, not a public connection or typed YQL ABI.
inline constexpr uint32_t WireVersion = 2;
static_assert(std::endian::native == std::endian::little);

enum class EClientError : uint32_t {
    None, Connection, Tls, Deadline, Cancelled, ResourceLimit, InvalidRequest, Authentication, HttpStatus, GrpcStatus
};

struct TRequestHeader {
    uint32_t Version = WireVersion;
    uint32_t Binding = 0;
    uint64_t PayloadBytes = 0;
};

struct TResponseHeader {
    uint32_t Version = WireVersion;
    int32_t Code = 0;
    EClientError Error = EClientError::None;
    int32_t NativeCode = 0;
    uint64_t PayloadBytes = 0;
};

struct TArgumentsHeader {
    uint64_t Mode = 0; // Single, sequential, parallel.
    uint32_t BindingA = 0;
    uint32_t BindingB = 0;
    uint64_t PayloadBytes = 0;
};

struct TResultHeader {
    uint32_t Version = WireVersion;
    uint32_t Count = 0;
    uint64_t FirstBytes = 0;
    uint64_t SecondBytes = 0;
};

static_assert(sizeof(TRequestHeader) == 16 && sizeof(TResponseHeader) == 24);
static_assert(sizeof(TArgumentsHeader) == 24 && sizeof(TResultHeader) == 24);

template <class T> std::string Encode(const T& header, std::string_view payload = {}) {
    static_assert(std::is_trivially_copyable_v<T>);
    std::string bytes(sizeof(T), '\0');
    std::memcpy(bytes.data(), &header, sizeof(T));
    if (!payload.empty()) {
        bytes.append(payload.data(), payload.size());
    }
    return bytes;
}

template <class T> bool Decode(std::string_view bytes, T& header, std::string_view& payload) {
    static_assert(std::is_trivially_copyable_v<T>);
    if (bytes.size() < sizeof(T)) {
        return false;
    }
    std::memcpy(&header, bytes.data(), sizeof(T));
    payload = bytes.substr(sizeof(T));
    return true;
}

} // namespace NFq::NWasmServices
