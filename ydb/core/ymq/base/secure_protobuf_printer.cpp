#include "secure_protobuf_printer.h"

#include <ydb/library/protobuf_printer/hide_field_printer.h>
#include <ydb/library/protobuf_printer/protobuf_printer.h>
#include <ydb/library/protobuf_printer/token_field_printer.h>

#include <util/generic/strbuf.h>
#include <util/generic/singleton.h>

#include <google/protobuf/message.h>
#include <google/protobuf/text_format.h>

#include <ydb/core/protos/sqs.pb.h>

namespace NKikimr::NSQS {

namespace {

class TSecureTextFormatPrinter : public TCustomizableTextFormatPrinter {
    void RegisterSecureFieldPrinters() {
        // User data
        RegisterFieldValuePrinters<TSendMessageRequest, THideFieldValuePrinter>("MessageBody");
        RegisterFieldValuePrinters<TReceiveMessageResponse::TMessage, THideFieldValuePrinter>("Data");
        RegisterFieldValuePrinters<TMessageAttribute, THideFieldValuePrinter>("StringValue", "BinaryValue");

        // Security
        RegisterFieldValuePrinters<TCredentials, TTokenFieldValuePrinter>("OAuthToken", "TvmTicket", "StaticCreds");
    }

public:
    TSecureTextFormatPrinter() {
        SetSingleLineMode(true);
        SetUseUtf8StringEscaping(true);
        RegisterSecureFieldPrinters();
    }

    TString Print(const ::google::protobuf::Message& msg) const {
        TProtoStringType string;
        PrintToString(msg, &string);

        // Copied from ShortUtf8DebugString() implementation.
        // Single line mode currently might have an extra space at the end.
        if (string.size() > 0 && string[string.size() - 1] == ' ') {
            string.resize(string.size() - 1);
        }

        return string;
    }
};

} // namespace

TString SecureShortUtf8DebugString(const NKikimrClient::TSqsRequest& msg) {
    return Default<TSecureTextFormatPrinter>().Print(msg);
}

TString SecureShortUtf8DebugString(const NKikimrClient::TSqsResponse& msg) {
    return Default<TSecureTextFormatPrinter>().Print(msg);
}

} // namespace NKikimr::NSQS
