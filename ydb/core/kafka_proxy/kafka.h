#pragma once

#include <istream>
#include <optional>
#include <ostream>

#include <ydb/core/raw_socket/sock_impl.h>
#include <ydb/library/yql/public/decimal/yql_wide_int.h>

#include <util/generic/buffer.h>
#include <util/generic/strbuf.h>
#include <util/system/types.h>

namespace NKafka {

/*
 * There are four versions of each field:
 * - present version   - field serialized and deserialized for this version of protocol.
 * - nullable version - field can be null for this versions of protocol. Default field isn't nullable.
 * - flexible version - field write as map item tag->value (or tagged version)
 * - message flexaible version - version of message support tags
 *
 * Fields may be of type:
 * - bool    - fixed length=1
 * - int8    - fixed length=1
 * - int16   - fixed length=2
 * - uint16  - fixed length=2
 * - int32   - fixed length=4
 * - uint32  - fixed length=4
 * - int64   - fixed length=8
 * - uuid    - fixed length=16
 * - float64 - fixed length=8
 * - string  - can be nullable
 * - bytes   - can be nullable
 * - records - can be nullable
 * - struct
 * - array   - can be nullable
 */

using TKafkaBool = ui8;
using TKafkaInt8 = i8;
using TKafkaInt16 = i16;
using TKafkaUint16 = ui16;
using TKafkaInt32 = i32;
using TKafkaUint32 = ui32;
using TKafkaInt64 = i64;
using TKafkaUuid = NYql::TWide<ui64>;
using TKafkaFloat64 = double;
using TKafkaRawString = TString;
using TKafkaString = std::optional<TKafkaRawString>;
using TKafkaRawBytes = TArrayRef<const char>;
using TKafkaBytes = std::optional<TKafkaRawBytes>;
using TKafkaRecords = std::optional<TKafkaRawBytes>;

using TKafkaVersion = i16;

using TWritableBuf = NKikimr::NRawSocket::TBufferedWriter;

template <typename T>
void NormalizeNumber(T& value) {
#ifndef WORDS_BIGENDIAN
    char* b = (char*)&value;
    char* e = b + sizeof(T) - 1;
    while (b < e) {
        std::swap(*b, *e);
        ++b;
        --e;
    }
#endif
}

class TKafkaWritable {
public:
    TKafkaWritable(TWritableBuf& buffer)
        : Buffer(buffer){};

    template <typename T>
    TKafkaWritable& operator<<(const T val) {
        NormalizeNumber(val);
        write((const char*)&val, sizeof(T));
        return *this;
    };

    TKafkaWritable& operator<<(const TKafkaUuid& val);
    TKafkaWritable& operator<<(const TKafkaRawBytes& val);
    TKafkaWritable& operator<<(const TKafkaRawString& val);

    void writeUnsignedVarint(TKafkaUint32 val);
    void writeVarint(TKafkaInt32 val);
    void writeVarint(TKafkaInt64 val);
    void write(const char* val, size_t length);

private:
    TWritableBuf& Buffer;
};

class TKafkaReadable {
public:
    TKafkaReadable(const TBuffer& is)
        : Is(is)
        , Position(0) {
    }

    template <typename T>
    TKafkaReadable& operator>>(T& val) {
        char* v = (char*)&val;
        read(v, sizeof(T));
        NormalizeNumber(val);
        return *this;
    };

    TKafkaReadable& operator>>(TKafkaUuid& val);

    void read(char* val, size_t length);
    char get();
    ui32 readUnsignedVarint();
    TArrayRef<const char> Bytes(size_t length);

    void skip(size_t length);

private:
    void checkEof(size_t length);

    const TBuffer& Is;
    size_t Position;
};

struct TReadDemand {
    constexpr TReadDemand()
        : Buffer(nullptr)
        , Length(0) {
    }

    constexpr TReadDemand(char* buffer, size_t length)
        : Buffer(buffer)
        , Length(length) {
    }

    constexpr TReadDemand(size_t length)
        : Buffer(nullptr)
        , Length(length) {
    }

    char* GetBuffer() const {
        return Buffer;
    }
    size_t GetLength() const {
        return Length;
    }
    explicit operator bool() const {
        return 0 < Length;
    }
    bool Skip() const {
        return nullptr == Buffer;
    }

    char* Buffer;
    size_t Length;
};

static constexpr TReadDemand NoDemand;

class TMessage {
public:
    virtual ~TMessage() = default;

    virtual i32 Size(TKafkaVersion version) const = 0;
    virtual void Read(TKafkaReadable& readable, TKafkaVersion version) = 0;
    virtual void Write(TKafkaWritable& writable, TKafkaVersion version) const = 0;

    bool operator==(const TMessage& other) const = default;
};

class TApiMessage: public TMessage {
public:
    ~TApiMessage() = default;

    virtual i16 ApiKey() const = 0;
};

std::unique_ptr<TApiMessage> CreateRequest(i16 apiKey);
std::unique_ptr<TApiMessage> CreateResponse(i16 apiKey);

i16 RequestHeaderVersion(i16 apiKey, TKafkaVersion version);
i16 ResponseHeaderVersion(i16 apiKey, TKafkaVersion version);

} // namespace NKafka
