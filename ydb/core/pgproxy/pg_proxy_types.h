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

    TString Dump() const {
        return {};
    }
};

template<char code>
struct TPGMessageType : TPGMessage {
    static constexpr char CODE = code;
    TPGMessageType() {
        Message = code;
    }
};

struct TPGInitial : TPGMessageType<'i'> { // it's not true, because we don't receive message code from a network, but imply it on the start
    TString Dump() const;
    std::unordered_map<TString, TString> GetClientParams() const;
    uint32_t GetProtocol() const;

    struct TPGBackendData {
        uint32_t Pid;
        uint32_t Key;
    };

    TPGBackendData GetBackendData() const;
};

struct TPGAuth : TPGMessageType<'R'> {
    enum class EAuthCode : uint32_t {
        OK = 0,
        ClearText = 3,
    };

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

struct TPGPasswordMessage : TPGMessageType<'p'> {
    TStringBuf GetPassword() const {
        TStringBuf password(GetData(), GetDataSize());
        while (!password.empty() && password.EndsWith('\0')) {
            password.Chop(1);
        }
        return password;
    }
};

struct TPGParameterStatus : TPGMessageType<'S'> {
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

struct TPGBackendKeyData : TPGMessageType<'K'> {
    TString Dump() const;
};

struct TPGSync : TPGMessageType<'S'> {
};

struct TPGReadyForQuery : TPGMessageType<'Z'> {
    TString Dump() const {
        TStringBuf data(GetData(), GetDataSize());
        return TStringBuilder() << "Status: " << data;
    }
};

struct TPGQuery : TPGMessageType<'Q'> {
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

struct TPGCommandComplete : TPGMessageType<'C'> {
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

struct TPGErrorResponse : TPGMessageType<'E'> {
    TString Dump() const;
};

struct TPGNoticeResponse : TPGMessageType<'N'> {
    TString Dump() const;
};

struct TPGTerminate : TPGMessageType<'X'> {
};

struct TPGRowDescription : TPGMessageType<'T'> {
};

struct TPGParameterDescription : TPGMessageType<'t'> {
};

struct TPGNoData : TPGMessageType<'n'> {
};

struct TPGDataRow : TPGMessageType<'D'> {
    TString Dump() const;
};

struct TPGEmptyQueryResponse : TPGMessageType<'I'> {
};

struct TPGParse : TPGMessageType<'P'> {
    struct TQueryData {
        TString Name;
        TString Query;
        std::vector<int32_t> ParametersTypes; // types
    };

    TQueryData GetQueryData() const;
    TString Dump() const;
};

struct TPGParseComplete : TPGMessageType<'1'> {
};

struct TPGBind : TPGMessageType<'B'> {
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

struct TPGBindComplete : TPGMessageType<'2'> {
};

struct TPGDescribe : TPGMessageType<'D'> {
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

struct TPGExecute : TPGMessageType<'E'> {
    struct TExecuteData {
        TString PortalName;
        uint32_t MaxRows;
    };

    TExecuteData GetExecuteData() const;
    TString Dump() const;
};

struct TPGClose : TPGMessageType<'C'> {
    struct TCloseData {
        enum class ECloseType : char {
            Portal = 'P',
            Statement = 'S',
        };
        ECloseType Type;
        TString Name;
    };

    TCloseData GetCloseData() const;
    TString Dump() const;
};

struct TPGCloseComplete : TPGMessageType<'3'> {
};

struct TPGFlush : TPGMessageType<'H'> {
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
