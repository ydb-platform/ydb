#pragma once

#include <cstdint>
#include <bit>
#include <cstring>
#include <string_view>
#include <type_traits>

namespace NYdb::NWasm::NServices {

// Experimental row ABI, independent of any service's network protocol.
inline constexpr uint32_t ServiceMagic = 0x31565357;
inline constexpr uint32_t ServiceVersion = 2;
inline constexpr uint32_t MaxServiceBatchRows = 64;
inline constexpr uint32_t MaxServiceBatchBytes = 32768;
static_assert(std::endian::native == std::endian::little);

// FNV-1a over the ASCII method name; kept in sync with generate.py.
constexpr uint32_t ServiceMethodId(std::string_view name) {
    uint32_t value = 2166136261u;
    for (const auto byte : name)
        value = (value ^ static_cast<unsigned char>(byte)) * 16777619u;
    return value;
}

enum class EValueType : uint32_t { Uint64, Uint32, Int64, Bool, String, Utf8 };

struct TServiceRequest {
    uint32_t Magic = ServiceMagic;
    uint32_t Version = ServiceVersion;
    uint32_t Method = 0;
    uint32_t Binding = 0;
    uint32_t Protocol = 0; // HTTP=0, gRPC=1; endpoint and auth stay on the host.
    uint32_t Count = 0;
    uint32_t MaxBytes = MaxServiceBatchBytes;
    uint32_t Batch = 0;
};

struct TServiceResult {
    uint32_t Version = ServiceVersion;
    uint32_t Count = 0;
    uint32_t Error = 0;
    uint32_t Detail = 0;
};

static_assert(sizeof(TServiceRequest) == 32 && sizeof(TServiceResult) == 16);

// Numeric cells are little-endian; String/Utf8 cells carry a uint32 byte length.
// Field order comes from the selected method's manifest, not the SQL struct order.
class TRowReader {
  public:
    explicit TRowReader(std::string_view bytes)
        : Bytes(bytes)
    {
    }

    template <class T> bool Get(T& value) {
        static_assert(std::is_trivially_copyable_v<T>);
        if (Bytes.size() < sizeof(T))
            return false;
        std::memcpy(&value, Bytes.data(), sizeof(T));
        Bytes.remove_prefix(sizeof(T));
        return true;
    }

    bool String(std::string_view& value, uint32_t maxBytes) {
        uint32_t size;
        if (!Get(size) || size > maxBytes || size > Bytes.size())
            return false;
        value = Bytes.substr(0, size);
        Bytes.remove_prefix(size);
        return true;
    }

    std::string_view Remaining() const {
        return Bytes;
    }

  private:
    std::string_view Bytes;
};

class TRowWriter {
  public:
    TRowWriter(char* data, size_t capacity)
        : Data(data), Capacity(capacity)
    {
    }

    template <class T> bool Put(const T& value) {
        static_assert(std::is_trivially_copyable_v<T>);
        if (sizeof(T) > Capacity - Used)
            return false;
        std::memcpy(Data + Used, &value, sizeof(T));
        Used += sizeof(T);
        return true;
    }

    bool String(std::string_view value) {
        if (value.size() > UINT32_MAX || value.size() + sizeof(uint32_t) > Capacity - Used)
            return false;
        Put(static_cast<uint32_t>(value.size()));
        if (!value.empty())
            std::memcpy(Data + Used, value.data(), value.size());
        Used += value.size();
        return true;
    }

    size_t Size() const {
        return Used;
    }

  private:
    char* Data;
    size_t Capacity;
    size_t Used = 0;
};

inline bool ReadServiceRequest(std::string_view bytes, TServiceRequest& header, std::string_view& rows) {
    TRowReader reader(bytes);
    if (!reader.Get(header) || header.Magic != ServiceMagic || header.Version != ServiceVersion || !header.Count ||
        header.Count > MaxServiceBatchRows || header.Protocol > 1 || header.Batch > 1 || (!header.Batch && header.Count != 1) ||
        header.MaxBytes < sizeof(TServiceRequest) || header.MaxBytes > MaxServiceBatchBytes || bytes.size() > header.MaxBytes)
        return false;
    rows = reader.Remaining();
    return true;
}

} // namespace NYdb::NWasm::NServices
