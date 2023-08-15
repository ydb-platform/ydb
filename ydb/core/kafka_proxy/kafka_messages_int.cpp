#include "kafka_messages_int.h"

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

void TKafkaWritable::writeUnsignedVarint(TKafkaUint32 value) {
    while ((value & 0xffffff80) != 0L) {
        ui8 b = (ui8) ((value & 0x7f) | 0x80);
        write((const char*)&b, sizeof(b));
        value >>= 7;
    }
    ui8 b = (ui8) value;
    write((const char*)&b, sizeof(b));
}

void TKafkaWritable::writeVarint(TKafkaInt32 value) {
    writeUnsignedVarint((value << 1) ^ (value >> 31));
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

ui32 TKafkaReadable::readUnsignedVarint() {
    ui32 value = 0;
    ui32 i = 0;
    ui16 b;
    while (((b = get()) & 0x80) != 0) {

        value |= ((ui32)(b & 0x7f)) << i;
        i += 7;
        if (i > 28) {
            ythrow yexception() << "illegal varint length";
        }
    }

    value |= b << i;
    return value;
}

i32 TKafkaReadable::readVarint() {
    ui32 v = readUnsignedVarint();
    return (v >> 1) ^ -(v & 1);
}

void TKafkaReadable::skip(size_t length) {
    checkEof(length);
    Position += length;
}

char TKafkaReadable::take(size_t shift) {
    checkEof(shift + sizeof(char));
    return *(Is.Data() + Position + shift);
}

void TKafkaReadable::checkEof(size_t length) {
    if (Position + length > Is.Size()) {
        ythrow yexception() << "unexpected end of stream";
    }
}

} // namespace NKafka
