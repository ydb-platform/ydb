#include "kafka_messages_int.h"

namespace NKafka {

void ErrorOnUnexpectedEnd(std::istream& is) {
    if (is.eof()) {
        ythrow yexception() << "unexpected end of stream";
    }
}

TKafkaWritable& TKafkaWritable::operator<<(const TKafkaRawBytes& val) {
    Os.write(val.data(), val.size());
    return *this;
}

TKafkaWritable& TKafkaWritable::operator<<(const TKafkaRawString& val) {
    Os.write(val.data(), val.length());
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
        Os << b;
        value >>= 7;
    }
    Os << (ui8) value;
}

TKafkaReadable& TKafkaReadable::operator>>(TKafkaUuid& val) {
    ui64 h;
    ui64 l;

    *this >> h >> l;

    val = TKafkaUuid(h, l);
    return *this;
}


void TKafkaReadable::read(char* val, int length) {
    Is.read(val, length);
    ErrorOnUnexpectedEnd(Is);
}

ui32 TKafkaReadable::readUnsignedVarint() {
    ui32 value = 0;
    ui32 i = 0;
    ui16 b;
    while (((b = Is.get()) & 0x80) != 0) {
        ErrorOnUnexpectedEnd(Is);

        value |= ((ui32)(b & 0x7f)) << i;
        i += 7;
        if (i > 28) {
            ythrow yexception() << "illegal varint length";
        }
    }

    ErrorOnUnexpectedEnd(Is);

    value |= b << i;
    return value;
}

void TKafkaReadable::skip(int length) {
    char buffer[64];
    while (length) {
        int l = std::min(length, 64);
        Is.read(buffer, l);
        length -= l;
    }

    ErrorOnUnexpectedEnd(Is);
}

} // namespace NKafka
