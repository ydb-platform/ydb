#pragma once

#include <cstdint>
#include <unordered_map>
#include <vector>
#include <util/stream/format.h>
#include <util/string/builder.h>
#if defined (_win_)
   #include <winsock2.h>
#elif defined (_unix_)
   #include <arpa/inet.h>
#endif
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

    void operator delete(void* p) {
        ::operator delete(p);
    }

    bool Empty() const {
        return GetDataSize() == 0;
    }
};

struct TPGInitial : TPGMessage { // it's not true, because we don't receive message code from a network, but imply it on the start
    TPGInitial() {
        Message = 'i'; // fake code
    }

    TString Dump() const;
    std::unordered_map<TString, TString> GetClientParams() const;
    uint32_t GetProtocol() const;
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
        if (!Empty()) {
            return TStringBuilder() << GetName() << "=" << GetValue();
        } else {
            return {};
        }
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

    TString Dump() const;
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

    TString Dump() const;
};

struct TPGEmptyQueryResponse : TPGMessage {
    TPGEmptyQueryResponse() {
        Message = 'I';
    }
};

struct TPGParse : TPGMessage {
    TPGParse() {
        Message = 'P';
    }

    struct TQueryData {
        TString Name;
        TString Query;
        std::vector<int32_t> ParametersTypes; // types
    };

    TQueryData GetQueryData() const;
    TString Dump() const;
};

struct TPGParseComplete : TPGMessage {
    TPGParseComplete() {
        Message = '1';
    }
};

struct TPGBind : TPGMessage {
    TPGBind() {
        Message = 'B';
    }

    struct TBindData {
        TString PortalName;
        TString StatementName;
        std::vector<int16_t> ParametersFormat; // format codes 0=text, 1=binary
        std::vector<std::vector<uint8_t>> ParametersValue; // parameters content
        std::vector<int16_t> ResultsFormat; // result format codes 0=text, 1=binary
    };

    TBindData GetBindData() const;
    TString Dump() const;
};

struct TPGBindComplete : TPGMessage {
    TPGBindComplete() {
        Message = '2';
    }
};

struct TPGDescribe : TPGMessage {
    TPGDescribe() {
        Message = 'D';
    }

    struct TDescribeData {
        enum class EDescribeType : char {
            Portal = 'P',
            Statement = 'S',
        };
        EDescribeType Type;
        TString Name;
    };

    TDescribeData GetDescribeData() const;
    TString Dump() const;
};

struct TPGExecute : TPGMessage {
    TPGExecute() {
        Message = 'E';
    }

    struct TExecuteData {
        TString PortalName;
        uint32_t MaxRows;
    };

    TExecuteData GetExecuteData() const;
    TString Dump() const;
};
#pragma pack(pop)

template<typename TPGMessageType>
std::unique_ptr<TPGMessageType> MakePGMessageCopy(const TPGMessageType* message) {
    size_t size = message->GetMessageSize();
    std::unique_ptr<TPGMessageType> msg{reinterpret_cast<TPGMessageType*>(::operator new(size))};
    std::memcpy(msg.get(), message, size);
    return msg;
}

}
