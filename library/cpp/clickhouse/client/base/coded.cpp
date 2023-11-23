#include "coded.h"

#include <memory.h>

namespace NClickHouse {
    static const int MAX_VARINT_BYTES = 10;

    TCodedInputStream::TCodedInputStream(IZeroCopyInput* input)
        : Input_(input)
    {
    }

    bool TCodedInputStream::ReadRaw(void* buffer, size_t size) {
        ui8* p = static_cast<ui8*>(buffer);

        while (size > 0) {
            const void* ptr;

            if (size_t len = Input_->Next(&ptr, size)) {
                memcpy(p, ptr, len);

                p += len;
                size -= len;
            } else {
                break;
            }
        }

        return size == 0;
    }

    bool TCodedInputStream::Skip(size_t count) {
        while (count > 0) {
            const void* ptr;
            size_t len = Input_->Next(&ptr, count);

            if (len == 0) {
                return false;
            }

            count -= len;
        }

        return true;
    }

    bool TCodedInputStream::ReadVarint64(ui64* value) {
        *value = 0;

        for (size_t i = 0; i < 9; ++i) {
            ui8 byte;

            if (!Input_->Read(&byte, sizeof(byte))) {
                return false;
            } else {
                *value |= (byte & 0x7F) << (7 * i);

                if (!(byte & 0x80)) {
                    return true;
                }
            }
        }

        // TODO skip invalid
        return false;
    }

    TCodedOutputStream::TCodedOutputStream(IOutputStream* output)
        : Output_(output)
    {
    }

    void TCodedOutputStream::Flush() {
        Output_->Flush();
    }

    void TCodedOutputStream::WriteRaw(const void* buffer, int size) {
        Output_->Write(buffer, size);
    }

    void TCodedOutputStream::WriteVarint64(ui64 value) {
        ui8 bytes[MAX_VARINT_BYTES];
        int size = 0;

        for (size_t i = 0; i < 9; ++i) {
            ui8 byte = value & 0x7F;
            if (value > 0x7F)
                byte |= 0x80;

            bytes[size++] = byte;

            value >>= 7;
            if (!value) {
                break;
            }
        }

        WriteRaw(bytes, size);
    }

}
