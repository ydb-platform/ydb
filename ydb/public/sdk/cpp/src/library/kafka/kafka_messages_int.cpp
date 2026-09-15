#include "kafka_messages_int.h"

#include <util/string/builder.h>

namespace NKafka {

TKafkaWritable& TKafkaWritable::operator<<(const TKafkaRawBytes& val) {
    write(val.data(), val.size());
    return *this;
}

TKafkaWritable& TKafkaWritable::operator<<(const TKafkaRawString& val) {
    write(val.data(), val.length());
    return *this;
}

TKafkaWritable& TKafkaWritable::operator<<(const TKafkaUuid& val) {
    ui64 h = ui64(val >> (sizeof(ui64) << 3));
    ui64 l = ui64(val);
    *this << h << l;
    return *this;
}

void TKafkaWritable::write(const char* val, size_t length) {
    Buffer.write(val, length);
}

TKafkaReadable& TKafkaReadable::operator>>(TKafkaUuid& val) {
    ui64 h;
    ui64 l;

    *this >> h >> l;

    val = TKafkaUuid(h, l);
    return *this;
}


void TKafkaReadable::read(char* val, size_t length) {
    checkEof(length);
    memcpy(val, Is.Data() + Position, length);
    Position += length;
}

char TKafkaReadable::get() {
    char r;
    read(&r, sizeof(r));
    return r;
}

TArrayRef<const char> TKafkaReadable::Bytes(size_t length) {
    checkEof(length);
    TArrayRef<const char> r(Is.Data() + Position, length);
    Position += length;
    return r;
}

void TKafkaReadable::skip(size_t length) {
    checkEof(length);
    Position += length;
}

char TKafkaReadable::take(size_t shift) {
    if (shift >= left()) {
        ythrow yexception() << "unexpected end of stream";
    }
    return *(Is.Data() + Position + shift);
}

size_t TKafkaReadable::left() const {
    return Position < Is.Size() ? Is.Size() - Position : 0;
}

size_t TKafkaReadable::position() const {
    return Position;
}

void TKafkaReadable::checkEof(size_t length) {
    if (length > left()) {
        ythrow yexception() << "unexpected end of stream";
    }
}

namespace NPrivate {

ui32 ReadTaggedFieldsCount(TKafkaReadable& readable) {
    const ui32 count = readable.readUnsignedVarint<ui32>();
    constexpr size_t kMinTaggedFieldBytes = 2;
    if (count > 0 && static_cast<size_t>(count) > readable.left() / kMinTaggedFieldBytes) {
        ythrow yexception() << "tagged fields count " << count << " exceeds remaining bytes";
    }
    return count;
}

void SkipTaggedField(TKafkaReadable& readable, ui32 size) {
    if (static_cast<size_t>(size) > readable.left()) {
        ythrow yexception() << "tagged field had invalid length " << size;
    }
    readable.skip(size);
}

} // namespace NPrivate

char Hex(const unsigned char c) {
    return c < 10 ? '0' + c : 'A' + c - 10;
}

TString Hex(const char* begin, const char *end) {
    TStringBuilder sb;
    for(auto i = begin; i < end; ++i) {
        unsigned char c = *i;
        if (i != begin) {
            sb << ", ";
        }
        sb << "0x" << Hex(c >> 4) << Hex(c & 0x0F);
    }
    return sb;
}

} // namespace NKafka
