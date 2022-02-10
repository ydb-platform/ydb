#include "varint.h"

#include <util/generic/yexception.h>
#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NMonitoring {
    ui32 WriteVarUInt32(IOutputStream* output, ui32 value) {
        bool stop = false;
        ui32 written = 0;
        while (!stop) {
            ui8 byte = static_cast<ui8>(value | 0x80);
            value >>= 7;
            if (value == 0) {
                stop = true;
                byte &= 0x7F;
            }
            output->Write(byte);
            written++;
        }
        return written;
    }

    ui32 ReadVarUInt32(IInputStream* input) {
        ui32 value = 0;
        switch (TryReadVarUInt32(input, &value)) {
            case EReadResult::OK:
                return value;
            case EReadResult::ERR_OVERFLOW:
                ythrow yexception() << "the data is too long to read ui32";
            case EReadResult::ERR_UNEXPECTED_EOF:
                ythrow yexception() << "the data unexpectedly ended";
            default:
                ythrow yexception() << "unknown error while reading varint";
        }
    }

    size_t ReadVarUInt32(const ui8* buf, size_t len, ui32* result) {
        size_t count = 0;
        ui32 value = 0;

        ui8 byte = 0;
        do {
            if (7 * count > 8 * sizeof(ui32)) {
                ythrow yexception() << "the data is too long to read ui32";
            }
            if (count == len) {
                ythrow yexception() << "the data unexpectedly ended";
            }
            byte = buf[count];
            value |= (static_cast<ui32>(byte & 0x7F)) << (7 * count);
            ++count;
        } while (byte & 0x80);

        *result = value;
        return count;
    }

EReadResult TryReadVarUInt32(IInputStream* input, ui32* value) {
        size_t count = 0;
        ui32 result = 0;

        ui8 byte = 0;
        do {
            if (7 * count > 8 * sizeof(ui32)) {
                return EReadResult::ERR_OVERFLOW;
            }
            if (input->Read(&byte, 1) != 1) {
                return EReadResult::ERR_UNEXPECTED_EOF;
            }
            result |= (static_cast<ui32>(byte & 0x7F)) << (7 * count);
            ++count;
        } while (byte & 0x80);

        *value = result;
        return EReadResult::OK;
    }

}
