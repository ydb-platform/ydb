#pragma once

#include <bit>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <string>
#include <string_view>
#include <type_traits>

namespace NYdb::NWasm::NServices {

// Internal P2 harness protocol, not a public connection or typed YQL ABI.
inline constexpr uint32_t WireVersion = 3;   // responses and legacy requests
inline constexpr uint32_t RequestWireVersion = 4;   // adds a per-call monitoring alias
inline constexpr uint32_t MaxAliasBytes = 64;

inline std::string_view MonitoringAlias(std::string_view alias) {
    if (alias.size() > MaxAliasBytes) {
        return "other";
    }
    for (const unsigned char c : alias) {
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.')) {
            return "other";
        }
    }
    return alias;
}

static_assert(std::endian::native == std::endian::little);

enum class EClientError: uint32_t {
    None,
    Connection,
    Tls,
    Deadline,
    Cancelled,
    ResourceLimit,
    InvalidRequest,
    Authentication,
    HttpStatus,
    GrpcStatus
};

struct TRequestHeader {
    uint32_t Version = RequestWireVersion;
    uint32_t Binding = 0;
    uint64_t PayloadBytes = 0;
    uint32_t MethodBytes = 0;
    uint32_t PathBytes = 0;
    uint32_t AliasBytes = 0;
    uint32_t Reserved = 0;
};

inline constexpr uint32_t MaxMethodBytes = 1024;
inline constexpr uint32_t MaxPathBytes = 4096;

// The binding fixes the remote authority and credentials. The guest selects the
// HTTP verb and origin-relative target, or a gRPC /Service/Method (empty path).
inline size_t RequestSize(std::string_view payload, std::string_view method, std::string_view path = {}, std::string_view alias = {}) {
    return sizeof(TRequestHeader) + method.size() + path.size() + MonitoringAlias(alias).size() + payload.size();
}

// Allocate RequestSize() bytes before calling this function.
inline void WriteRequest(
    char* out, uint32_t binding, std::string_view payload, std::string_view method, std::string_view path = {}, std::string_view alias = {}) {
    alias = MonitoringAlias(alias);
    const TRequestHeader header{ RequestWireVersion, binding, payload.size(), uint32_t(method.size()), uint32_t(path.size()),
        uint32_t(alias.size()), 0 };
    std::memcpy(out, &header, sizeof(header));
    out += sizeof(header);
    for (auto part : { method, path, alias, payload }) {
        if (!part.empty()) {
            std::memcpy(out, part.data(), part.size());
        }
        out += part.size();
    }
}

inline bool ReadRequest(std::string_view bytes, TRequestHeader& header, std::string_view& payload, std::string_view& method,
    std::string_view& path, std::string_view& alias) {
    // v3 has the same first 24 bytes and no alias. Keep existing modules usable.
    constexpr size_t LegacySize = 24;
    if (bytes.size() < LegacySize) {
        return false;
    }
    header = {};
    std::memcpy(&header, bytes.data(), LegacySize);
    size_t size = LegacySize;
    if (header.Version == RequestWireVersion) {
        if (bytes.size() < sizeof(header)) {
            return false;
        }
        std::memcpy(&header, bytes.data(), sizeof(header));
        size = sizeof(header);
    } else if (header.Version != WireVersion) {
        return false;
    }
    bytes.remove_prefix(size);
    if (header.Reserved || !header.MethodBytes || header.MethodBytes > MaxMethodBytes || header.PathBytes > MaxPathBytes ||
        header.AliasBytes > MaxAliasBytes || header.MethodBytes > bytes.size()) {
        return false;
    }
    method = bytes.substr(0, header.MethodBytes);
    bytes.remove_prefix(header.MethodBytes);
    if (header.PathBytes > bytes.size()) {
        return false;
    }
    path = bytes.substr(0, header.PathBytes);
    bytes.remove_prefix(header.PathBytes);
    if (header.AliasBytes > bytes.size()) {
        return false;
    }
    alias = bytes.substr(0, header.AliasBytes);
    bytes.remove_prefix(header.AliasBytes);
    payload = bytes;
    return header.PayloadBytes == payload.size();
}

inline bool ReadRequest(
    std::string_view bytes, TRequestHeader& header, std::string_view& payload, std::string_view& method, std::string_view& path) {
    std::string_view alias;
    return ReadRequest(bytes, header, payload, method, path, alias);
}

struct TResponseHeader {
    uint32_t Version = WireVersion;
    int32_t Code = 0;
    EClientError Error = EClientError::None;
    int32_t NativeCode = 0;
    uint64_t PayloadBytes = 0;
};

struct TArgumentsHeader {
    uint64_t Mode = 0;   // Single, sequential, parallel.
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

static_assert(sizeof(TRequestHeader) == 32 && sizeof(TResponseHeader) == 24);
static_assert(sizeof(TArgumentsHeader) == 24 && sizeof(TResultHeader) == 24);

template <class T>
std::string Encode(const T& header, std::string_view payload = {}) {
    static_assert(std::is_trivially_copyable_v<T>);
    std::string bytes(sizeof(T), '\0');
    std::memcpy(bytes.data(), &header, sizeof(T));
    if (!payload.empty()) {
        bytes.append(payload.data(), payload.size());
    }
    return bytes;
}

template <class T>
bool Decode(std::string_view bytes, T& header, std::string_view& payload) {
    static_assert(std::is_trivially_copyable_v<T>);
    if (bytes.size() < sizeof(T)) {
        return false;
    }
    std::memcpy(&header, bytes.data(), sizeof(T));
    payload = bytes.substr(sizeof(T));
    return true;
}

}   // namespace NYdb::NWasm::NServices
