#pragma once

#include <cstdint>
#include <util/stream/format.h>
#include <arpa/inet.h>

namespace NPG {

#pragma pack(push, 1)
struct TPGMessage {
    char Message;
    uint32_t Length;

    TPGMessage() {
        Length = htonl(sizeof(*this) - sizeof(Message));
    }

    const char* GetData() const {
        return reinterpret_cast<const char*>(this) + sizeof(*this);
    }

    size_t GetDataSize() const {
        return ntohl(Length) - sizeof(Length);
    }

    size_t GetMessageSize() const {
        return sizeof(*this) + GetDataSize();
    }
};

struct TPGInitial : TPGMessage { // it's not true, because we don't receive message code from a network, but imply it on the start
    uint32_t Protocol;

    TPGInitial() {
        Message = 'i'; // fake code
    }

    const char* GetData() const {
        return reinterpret_cast<const char*>(this) + sizeof(*this);
    }

    TString Dump() const {
        TStringBuilder text;
        if (Protocol == 773247492) { // 80877102 cancellation message
            const uint32_t* data = reinterpret_cast<const uint32_t*>(GetData());
            if (GetDataSize() >= 8) {
                uint32_t pid = data[0];
                uint32_t key = data[1];
                text << "cancellation PID " << pid << " KEY " << key;
            }
        } else if (Protocol == 790024708) { // 790024708 SSL handshake
            text << "SSL handshake";
        } else {
            text << "protocol(" << Hex(Protocol) << ") ";
            const char* begin = GetData();
            const char* end = GetData() + GetDataSize();
            for (const char* ptr = begin; ptr < end;) {
                TStringBuf key;
                TStringBuf value;
                size_t size = strnlen(ptr, end - ptr);
                key = TStringBuf(ptr, size);
                if (key.empty()) {
                    break;
                }
                ptr += size + 1;
                if (ptr >= end) {
                    break;
                }
                size = strnlen(ptr, end - ptr);
                value = TStringBuf(ptr, size);
                ptr += size + 1;
                text << key << "=" << value << " ";
            }
        }
        return text;
    }

    std::unordered_map<TString, TString> GetClientParams() const {
        std::unordered_map<TString, TString> params;
        const char* begin = GetData();
        const char* end = GetData() + GetDataSize();
        for (const char* ptr = begin; ptr < end;) {
            TString key;
            TString value;
            size_t size = strnlen(ptr, end - ptr);
            key = TStringBuf(ptr, size);
            if (key.empty()) {
                break;
            }
            ptr += size + 1;
            if (ptr >= end) {
                break;
            }
            size = strnlen(ptr, end - ptr);
            value = TStringBuf(ptr, size);
            ptr += size + 1;
            params[key] = value;
        }
        return params;
    }
};

struct TPGAuth : TPGMessage {
    enum class EAuthCode : uint32_t {
        OK = 0,
        ClearText = 3,
    };

    TPGAuth() {
        Message = 'R';
    }

    EAuthCode GetAuthCode() const {
        return static_cast<EAuthCode>(ntohl(*reinterpret_cast<const uint32_t*>(GetData())));
    }

    TString Dump() const {
        switch (GetAuthCode()) {
            case EAuthCode::OK:
                return "OK";
            case EAuthCode::ClearText:
                return "ClearText";
        }
        return "Unknown";
    }
};

struct TPGPasswordMessage : TPGMessage {
    TPGPasswordMessage() {
        Message = 'r';
    }

    TStringBuf GetPassword() const {
        TStringBuf password(GetData(), GetDataSize());
        while (!password.empty() && password.EndsWith('\0')) {
            password.Chop(1);
        }
        return password;
    }
};

struct TPGParameterStatus : TPGMessage {
    TPGParameterStatus() {
        Message = 'S';
    }

    TStringBuf GetValue() const {
        TStringBuf data(GetData(), GetDataSize());
        data = data.SplitOff('\0');
        while (!data.empty() && data.EndsWith('\0')) {
            data.Chop(1);
        }
        return data;
    }

    TStringBuf GetName() const {
        TStringBuf data(GetData(), GetDataSize());
        while (!data.empty() && data.EndsWith('\0')) {
            data.Chop(1);
        }
        return data.RSplitOff('\0');
    }

    TString Dump() const {
        return TStringBuilder() << GetName() << "=" << GetValue();
    }
};

struct TPGReadyForQuery : TPGMessage {
    TPGReadyForQuery() {
        Message = 'Z';
    }

    TString Dump() const {
        TStringBuf data(GetData(), GetDataSize());
        return TStringBuilder() << "Status: " << data;
    }
};

struct TPGQuery : TPGMessage {
    TPGQuery() {
        Message = 'Q';
    }

    TStringBuf GetQuery() const {
        TStringBuf query(GetData(), GetDataSize());
        while (!query.empty() && query.EndsWith('\0')) {
            query.Chop(1);
        }
        return query;
    }

    TString Dump() const {
        return TStringBuilder() << " Query: \"" << GetQuery() << "\"";
    }
};

struct TPGCommandComplete : TPGMessage {
    TPGCommandComplete() {
        Message = 'C';
    }

    TStringBuf GetTag() const {
        TStringBuf query(GetData(), GetDataSize());
        while (!query.empty() && query.EndsWith('\0')) {
            query.Chop(1);
        }
        return query;
    }

    TString Dump() const {
        return TStringBuilder() << " Tag: \"" << GetTag() << "\"";
    }
};

struct TPGErrorResponse : TPGMessage {
    TPGErrorResponse() {
        Message = 'E';
    }
};

struct TPGTerminate : TPGMessage {
    TPGTerminate() {
        Message = 'X';
    }
};

struct TPGRowDescription : TPGMessage {
    TPGRowDescription() {
        Message = 'T';
    }
};

struct TPGDataRow : TPGMessage {
    TPGDataRow() {
        Message = 'D';
    }
};

struct TPGEmptyQueryResponse : TPGMessage {
    TPGEmptyQueryResponse() {
        Message = 'I';
    }
};

#pragma pack(pop)

template<typename TPGMessageType>
std::unique_ptr<TPGMessageType> MakePGMessageCopy(const TPGMessageType* message) {
    size_t size = message->GetMessageSize();
    std::unique_ptr<TPGMessageType> msg{reinterpret_cast<TPGMessageType*>(new uint8_t[size])};
    std::memcpy(msg.get(), message, size);
    return msg;
}

}
