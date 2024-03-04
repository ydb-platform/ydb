#include "pg_proxy_types.h"
#include "pg_stream.h"
#include <library/cpp/string_utils/base64/base64.h>
#include <ydb/core/base/path.h>

namespace NPG {

TString TPGInitial::Dump() const {
    TPGStreamInput stream(*this);
    TStringBuilder text;
    uint32_t protocol = 0;
    stream >> protocol;
    protocol = htonl(protocol);
    if (protocol == 773247492) { // 80877102 cancellation message
        uint32_t pid = 0;
        uint32_t key = 0;
        stream >> pid >> key;
        text << "cancellation PID " << pid << " KEY " << key;
    } else if (protocol == 790024708) { // 790024708 SSL handshake
        text << "SSL handshake";
    } else {
        text << "protocol(" << Hex(protocol) << ") ";
        while (!stream.Empty()) {
            TStringBuf key;
            TStringBuf value;
            stream >> key >> value;
            if (key.empty()) {
                break;
            }
            text << key << "=" << value << " ";
        }
    }
    return text;
}

uint32_t TPGInitial::GetProtocol() const {
    TPGStreamInput stream(*this);
    uint32_t protocol = 0;
    stream >> protocol;
    protocol = htonl(protocol);
    return protocol;
}

TPGInitial::TPGBackendData TPGInitial::GetBackendData() const {
    TPGBackendData backendData;
    TPGStreamInput stream(*this);
    uint32_t protocol = 0;
    stream >> protocol;
    stream >> backendData.Pid >> backendData.Key;
    return backendData;
}

std::unordered_map<TString, TString> TPGInitial::GetClientParams() const {
    std::unordered_map<TString, TString> params;
    TPGStreamInput stream(*this);
    TStringBuilder text;
    uint32_t protocol = 0;
    stream >> protocol;
    while (!stream.Empty()) {
        TString key;
        TString value;
        stream >> key >> value;
        if (key.empty()) {
            break;
        }
        if (key == "database") {
            value = NKikimr::CanonizePath(value);
        }
        params[TString(key)] = value;
    }
    return params;
}

TString TPGBackendKeyData::Dump() const {
    TPGStreamInput stream(*this);
    uint32_t pid = 0;
    uint32_t key = 0;
    stream >> pid >> key;
    return TStringBuilder() << "cancellation PID " << pid << " KEY " << key;
}

TString TPGNoticeResponse::Dump() const {
    TPGStreamInput stream(*this);
    TStringBuilder text;

    while (!stream.Empty()) {
        char code;
        TString message;
        stream >> code;
        if (code == 0) {
            break;
        }
        stream >> message;
        if (!text.empty()) {
            text << ' ';
        }
        text << code << "=\"" << message << "\"";
    }
    return text;
}

TString TPGErrorResponse::Dump() const {
    TPGStreamInput stream(*this);
    TStringBuilder text;

    while (!stream.Empty()) {
        char code;
        TString message;
        stream >> code;
        if (code == 0) {
            break;
        }
        stream >> message;
        if (!text.empty()) {
            text << ' ';
        }
        text << code << "=\"" << message << "\"";
    }
    return text;
}

TString TPGParse::Dump() const {
    TPGStreamInput stream(*this);
    TStringBuf name;
    TStringBuf query;
    stream >> name;
    stream >> query;
    while (!query.empty() && query.EndsWith('\0')) {
        query.Chop(1);
    }
    return TStringBuilder() << "Statement: \"" << name << "\" Query: \"" << query <<"\"";
}

TPGParse::TQueryData TPGParse::GetQueryData() const {
    TQueryData queryData;
    TPGStreamInput stream(*this);
    stream >> queryData.Name;
    stream >> queryData.Query;
    uint16_t numberOfParameterTypes = 0;
    stream >> numberOfParameterTypes;
    queryData.ParametersTypes.reserve(numberOfParameterTypes);
    for (uint16_t n = 0; n < numberOfParameterTypes; ++n) {
        uint32_t param = 0;
        stream >> param;
        queryData.ParametersTypes.emplace_back(param);
    }
    return queryData;
}

TPGBind::TBindData TPGBind::GetBindData() const {
    TBindData bindData;
    TPGStreamInput stream(*this);
    stream >> bindData.PortalName;
    stream >> bindData.StatementName;
    uint16_t numberOfParameterFormats = 0;
    stream >> numberOfParameterFormats;
    bindData.ParametersFormat.reserve(numberOfParameterFormats);
    for (uint16_t n = 0; n < numberOfParameterFormats; ++n) {
        uint16_t format = 0;
        stream >> format;
        bindData.ParametersFormat.emplace_back(format);
    }
    uint16_t numberOfParameterValues = 0;
    stream >> numberOfParameterValues;
    bindData.ParametersValue.reserve(numberOfParameterValues);
    for (uint16_t n = 0; n < numberOfParameterValues; ++n) {
        uint32_t size = 0;
        stream >> size;
        std::vector<uint8_t> value;
        stream.Read(value, size);
        bindData.ParametersValue.emplace_back(std::move(value));
    }
    uint16_t numberOfResultFormats = 0;
    stream >> numberOfResultFormats;
    bindData.ResultsFormat.reserve(numberOfResultFormats);
    for (uint16_t n = 0; n < numberOfResultFormats; ++n) {
        uint16_t format = 0;
        stream >> format;
        bindData.ResultsFormat.emplace_back(format);
    }
    return bindData;
}

TString TPGBind::Dump() const {
    TStringBuilder text;
    TPGStreamInput stream(*this);
    TStringBuf portalName;
    TStringBuf statementName;
    stream >> portalName;
    stream >> statementName;
    text << "Statement: \"" << statementName << "\"" << " Portal: \"" << portalName << "\"";
    uint16_t numberOfParameterFormats = 0;
    stream >> numberOfParameterFormats;
    std::vector<uint16_t> parameterFormats;
    if (numberOfParameterFormats > 0) {
        text << " ParameterFormat: " << numberOfParameterFormats << " [";
        for (uint16_t n = 0; n < numberOfParameterFormats; ++n) {
            uint16_t format = 0;
            stream >> format;
            parameterFormats.push_back(format);
            text << format;
            if (n + 1 < numberOfParameterFormats) {
                text << ",";
            }
        }
        text << "]";
    }
    uint16_t numberOfParameterValues = 0;
    stream >> numberOfParameterValues;
    if (numberOfParameterFormats > 0) {
        text << " ParameterValues: " << numberOfParameterValues << " [";
        for (uint16_t n = 0; n < numberOfParameterValues; ++n) {
            uint32_t size = 0;
            stream >> size;
            std::vector<uint8_t> value;
            stream.Read(value, size);
            TStringBuf data(reinterpret_cast<const char*>(value.data()), value.size());
            if (parameterFormats.empty() || (parameterFormats.size() > n && parameterFormats[n] == 0) || (parameterFormats.size() == 1 && parameterFormats[0] == 0)) {
                text << "'" << data << "'";
            } else {
                text << Base64Encode(data);
            }
            if (n + 1 < numberOfParameterValues) {
                text << ",";
            }
        }
        text << "]";
    }
    uint16_t numberOfResultFormats = 0;
    stream >> numberOfResultFormats;
    if (numberOfResultFormats > 0) {
        text << " ResultFormat: " << numberOfResultFormats << " [";
        for (uint16_t n = 0; n < numberOfResultFormats; ++n) {
            uint16_t format = 0;
            stream >> format;
            text << format;
            if (n + 1 < numberOfResultFormats) {
                text << ",";
            }
        }
        text << "]";
    }
    return text;
}

TString TPGDataRow::Dump() const {
    TPGStreamInput stream(*this);
    uint16_t numberOfColumns = 0;
    stream >> numberOfColumns;
    return TStringBuilder() << "Columns: " << numberOfColumns;
}

TPGDescribe::TDescribeData TPGDescribe::GetDescribeData() const {
    TPGStreamInput stream(*this);
    TDescribeData data;
    char describeType = 0;
    stream >> describeType;
    data.Type = static_cast<TDescribeData::EDescribeType>(describeType);
    stream >> data.Name;
    return data;
}

TString TPGDescribe::Dump() const {
    TPGStreamInput stream(*this);
    TStringBuilder text;
    char describeType = 0;
    stream >> describeType;
    text << "Type: " << describeType;
    TStringBuf name;
    stream >> name;
    text << " Name: \"" << name << "\"";
    return text;
}

TPGExecute::TExecuteData TPGExecute::GetExecuteData() const {
    TPGStreamInput stream(*this);
    TExecuteData data;
    stream >> data.PortalName >> data.MaxRows;
    return data;
}

TString TPGExecute::Dump() const {
    TPGStreamInput stream(*this);
    TStringBuilder text;
    TStringBuf name;
    uint32_t maxRows = 0;
    stream >> name >> maxRows;
    text << "Portal: \"" << name << "\"";
    if (maxRows) {
        text << " MaxRows: " << maxRows;
    }
    return text;
}

TPGClose::TCloseData TPGClose::GetCloseData() const {
    TCloseData closeData;
    TPGStreamInput stream(*this);
    char type;
    TString name;
    stream >> type;
    closeData.Type = static_cast<TCloseData::ECloseType>(type);
    stream >> closeData.Name;
    return closeData;
}

TString TPGClose::Dump() const {
    TStringBuilder text;
    TPGStreamInput stream(*this);
    char type;
    TStringBuf name;
    stream >> type >> name;
    text << "Type: " << type << " Name: " << name;
    return text;
}



}