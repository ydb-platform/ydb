#include "yql_yt_index_serialisation.h"

namespace NYql::NFmr {

void WriteVarUint64(IOutputStream* out, ui64 value) {
    constexpr ui8 VARINT_CONTINUE_BIT = 0x80;
    constexpr ui8 VARINT_DATA_MASK = 0x7F;

    while (value >= 0x80) {
        out->Write(static_cast<char>((value & VARINT_DATA_MASK) | VARINT_CONTINUE_BIT));
        value >>= 7;
    }
    out->Write(static_cast<char>(value & VARINT_DATA_MASK));
}

void WriteVarUint32(IOutputStream* out, ui32 value) {
    WriteVarUint64(out, value);
}

void WriteString(IOutputStream* out, const TString& str) {
    WriteVarUint32(out, str.size());
    out->Write(str.data(), str.size());
}

ui64 ReadVarUint64(IInputStream* in) {
    constexpr ui8 VARINT_DATA_MASK = 0x7F;
    constexpr ui8 VARINT_CONTINUE_BIT = 0x80;

    ui64 result = 0;
    int shift = 0;

    while (true) {
        char buffer;
        size_t bytesRead = in->Read(&buffer, 1);
        Y_ENSURE(bytesRead == 1, "Unexpected end of stream while reading varint");

        const ui8 byte = static_cast<ui8>(buffer);
        result |= static_cast<ui64>(byte & VARINT_DATA_MASK) << shift;

        if ((byte & VARINT_CONTINUE_BIT) == 0) {
            break;
        }
        shift += 7;
        Y_ENSURE(shift < 70, "Varint64 too long");
    }
    return result;
}

ui32 ReadVarUint32(IInputStream* in) {
    return static_cast<ui32>(ReadVarUint64(in));
}

TString ReadString(IInputStream* in) {
    ui32 length = ReadVarUint32(in);
    TString result;
    result.resize(length);
    if (length > 0) {
        size_t bytesRead = in->Read(result.begin(), length);
        Y_ENSURE(bytesRead == length, "String read length mismatch");
    }
    return result;
}

void TSortedRowMetadata::Save(IOutputStream* data) const {
    if (data == nullptr) {
        return;
    }

    WriteVarUint64(data, Rows.size());
    for (const auto& row : Rows) {
        Y_ENSURE(row.size() - 1 == KeyColumns.size(), "Row size mismatch");
        for (const auto& columnOffets : row) {
            WriteVarUint64(data, columnOffets.StartOffset);
            WriteVarUint64(data, columnOffets.EndOffset);
        }
    }
}


void TSortedRowMetadata::Load(IInputStream* data, std::vector<TString> keyColumns) {
    if (data == nullptr) {
        return;
    }
    KeyColumns = std::move(keyColumns);
    Rows.clear();
    ui64 numRows = ReadVarUint64(data);
    Rows.reserve(numRows);

    for (ui64 rowIdx = 0; rowIdx < numRows; ++rowIdx) {
        TRowIndexMarkup row;
        row.reserve(KeyColumns.size()+1);
        for (ui64 i = 0; i < KeyColumns.size() + 1; ++i) {
            ui64 startOffset = ReadVarUint64(data);
            ui64 endOffset = ReadVarUint64(data);
            row.push_back(TColumnOffsetRange{startOffset, endOffset});
        }
        Rows.push_back(std::move(row));
    }
}

}
