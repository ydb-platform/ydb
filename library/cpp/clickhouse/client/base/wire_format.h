#pragma once

#include "coded.h"

#include <util/generic/string.h>
#include <util/memory/tempbuf.h>

namespace NClickHouse {
    class TWireFormat {
    public:
        template <typename T>
        static bool ReadFixed(TCodedInputStream* input, T* value);

        static bool ReadString(TCodedInputStream* input, TString* value);

        static bool ReadBytes(TCodedInputStream* input, void* buf, size_t len);

        static bool ReadUInt64(TCodedInputStream* input, ui64* value);

        template <typename T>
        static void WriteFixed(TCodedOutputStream* output, const T& value);

        static void WriteBytes(TCodedOutputStream* output, const void* buf, size_t len);

        static void WriteString(TCodedOutputStream* output, const TString& value);

        static void WriteStringBuf(TCodedOutputStream* output, const TStringBuf value);

        static void WriteUInt64(TCodedOutputStream* output, const ui64 value);
    };

    template <typename T>
    inline bool TWireFormat::ReadFixed(
        TCodedInputStream* input,
        T* value) {
        return input->ReadRaw(value, sizeof(T));
    }

    inline bool TWireFormat::ReadString(
        TCodedInputStream* input,
        TString* value) {
        ui64 len;

        if (input->ReadVarint64(&len)) {
            if (len > 0x00FFFFFFULL) {
                return false;
            }
            TTempBuf buf(len);
            if (input->ReadRaw(buf.Data(), (size_t)len)) {
                value->assign(buf.Data(), len);
                return true;
            }
        }

        return false;
    }

    inline bool TWireFormat::ReadBytes(
        TCodedInputStream* input, void* buf, size_t len) {
        return input->ReadRaw(buf, len);
    }

    inline bool TWireFormat::ReadUInt64(
        TCodedInputStream* input,
        ui64* value) {
        return input->ReadVarint64(value);
    }

    template <typename T>
    inline void TWireFormat::WriteFixed(
        TCodedOutputStream* output,
        const T& value) {
        output->WriteRaw(&value, sizeof(T));
    }

    inline void TWireFormat::WriteBytes(
        TCodedOutputStream* output,
        const void* buf,
        size_t len) {
        output->WriteRaw(buf, len);
    }

    inline void TWireFormat::WriteString(
        TCodedOutputStream* output,
        const TString& value) {
        output->WriteVarint64(value.size());
        output->WriteRaw(value.data(), value.size());
    }

    inline void TWireFormat::WriteStringBuf(
        TCodedOutputStream* output,
        const TStringBuf value) {
        output->WriteVarint64(value.size());
        output->WriteRaw(value.data(), value.size());
    }

    inline void TWireFormat::WriteUInt64(
        TCodedOutputStream* output,
        const ui64 value) {
        output->WriteVarint64(value);
    }

}
