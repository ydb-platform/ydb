#include "protocol.h"

#include <util/string/cast.h>

namespace NKikimr::NSql {

namespace {

ui32 ReadBe32(const char* data) {
    const auto* bytes = reinterpret_cast<const unsigned char*>(data);
    return (ui32(bytes[0]) << 24) | (ui32(bytes[1]) << 16) | (ui32(bytes[2]) << 8) | ui32(bytes[3]);
}

void AppendBe16(TString& out, ui16 value) {
    char bytes[2] = {
        static_cast<char>((value >> 8) & 0xff),
        static_cast<char>(value & 0xff),
    };
    out.append(bytes, 2);
}

void AppendBe32(TString& out, ui32 value) {
    char bytes[4] = {
        static_cast<char>((value >> 24) & 0xff),
        static_cast<char>((value >> 16) & 0xff),
        static_cast<char>((value >> 8) & 0xff),
        static_cast<char>(value & 0xff),
    };
    out.append(bytes, 4);
}

void AppendCString(TString& out, TStringBuf value) {
    out.append(value);
    out.push_back('\0');
}

TString MakeMessage(char type, TStringBuf payload) {
    TString out;
    out.push_back(type);
    AppendBe32(out, static_cast<ui32>(4 + payload.size()));
    out.append(payload);
    return out;
}

} // namespace

bool TryParseStartup(TString& buffer, TStartupMessage& message, TString& error) {
    if (buffer.size() < 4) {
        return false;
    }
    const ui32 length = ReadBe32(buffer.data());
    if (length < 8 || length > 10000) {
        error = "invalid startup length";
        return true;
    }
    if (buffer.size() < length) {
        return false;
    }

    const ui32 code = ReadBe32(buffer.data() + 4);
    message = {};
    message.Protocol = code;
    if (length == 8 && code == SslRequestCode) {
        message.SslRequest = true;
        buffer.remove(0, length);
        return true;
    }
    if (length == 16 && code == CancelRequestCode) {
        message.CancelRequest = true;
        buffer.remove(0, length);
        return true;
    }
    if (code != ProtocolVersion3) {
        error = "unsupported protocol";
        buffer.remove(0, length);
        return true;
    }

    size_t offset = 8;
    while (offset < length) {
        if (buffer[offset] == '\0') {
            ++offset;
            break;
        }
        const size_t nameEnd = buffer.find('\0', offset);
        if (nameEnd == TString::npos || nameEnd >= length) {
            error = "truncated startup parameter";
            buffer.remove(0, length);
            return true;
        }
        const size_t valueOffset = nameEnd + 1;
        const size_t valueEnd = buffer.find('\0', valueOffset);
        if (valueEnd == TString::npos || valueEnd >= length) {
            error = "truncated startup parameter";
            buffer.remove(0, length);
            return true;
        }
        message.Parameters[buffer.substr(offset, nameEnd - offset)] = buffer.substr(valueOffset, valueEnd - valueOffset);
        offset = valueEnd + 1;
    }

    buffer.remove(0, length);
    return true;
}

bool TryParseMessage(TString& buffer, TClientMessage& message, TString& error) {
    if (buffer.size() < 5) {
        return false;
    }
    const ui32 length = ReadBe32(buffer.data() + 1);
    if (length < 4 || length > 16 * 1024 * 1024) {
        error = "invalid message length";
        return true;
    }
    if (buffer.size() < 1 + length) {
        return false;
    }
    message.Type = buffer[0];
    message.Payload = buffer.substr(5, length - 4);
    buffer.remove(0, 1 + length);
    return true;
}

TString MakeAuthenticationOk() {
    TString payload;
    AppendBe32(payload, 0);
    return MakeMessage('R', payload);
}

TString MakeParameterStatus(TStringBuf name, TStringBuf value) {
    TString payload;
    AppendCString(payload, name);
    AppendCString(payload, value);
    return MakeMessage('S', payload);
}

TString MakeBackendKeyData(ui32 backendPid, ui32 secret) {
    TString payload;
    AppendBe32(payload, backendPid);
    AppendBe32(payload, secret);
    return MakeMessage('K', payload);
}

TString MakeReadyForQuery() {
    return MakeMessage('Z', TStringBuf("I", 1));
}

TString MakeErrorResponse(TStringBuf sqlState, TStringBuf message) {
    TString payload;
    payload.push_back('S');
    AppendCString(payload, "ERROR");
    payload.push_back('V');
    AppendCString(payload, "ERROR");
    payload.push_back('C');
    AppendCString(payload, sqlState);
    payload.push_back('M');
    AppendCString(payload, message);
    payload.push_back('\0');
    return MakeMessage('E', payload);
}

TString MakeTextResult(TArrayRef<const TResultColumn> columns, TArrayRef<const TVector<TString>> rows) {
    TString description;
    AppendBe16(description, static_cast<ui16>(columns.size()));
    for (const TResultColumn& column : columns) {
        AppendCString(description, column.Name);
        AppendBe32(description, 0);
        AppendBe16(description, 0);
        AppendBe32(description, column.TypeOid);
        AppendBe16(description, static_cast<ui16>(column.TypeSize));
        AppendBe32(description, static_cast<ui32>(-1));
        AppendBe16(description, 0);
    }

    TString out = MakeMessage('T', description);
    for (const TVector<TString>& row : rows) {
        TString data;
        AppendBe16(data, static_cast<ui16>(row.size()));
        for (const TString& value : row) {
            AppendBe32(data, static_cast<ui32>(value.size()));
            data.append(value);
        }
        out.append(MakeMessage('D', data));
    }

    TString complete = "SELECT " + ToString(rows.size());
    complete.push_back('\0');
    out.append(MakeMessage('C', complete));
    out.append(MakeReadyForQuery());
    return out;
}

TString MakeHandshake(ui32 backendPid, ui32 secret) {
    TString out;
    out.append(MakeAuthenticationOk());
    out.append(MakeParameterStatus("server_version", "18.0"));
    out.append(MakeParameterStatus("client_encoding", "UTF8"));
    out.append(MakeParameterStatus("DateStyle", "ISO, MDY"));
    out.append(MakeParameterStatus("integer_datetimes", "on"));
    out.append(MakeParameterStatus("standard_conforming_strings", "on"));
    out.append(MakeBackendKeyData(backendPid, secret));
    out.append(MakeReadyForQuery());
    return out;
}

} // namespace NKikimr::NSql
