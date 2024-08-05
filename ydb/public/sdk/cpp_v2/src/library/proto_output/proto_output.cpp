#include <util/stream/output.h>

#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>

#include <google/protobuf/text_format.h>

namespace {
    std::string ShortUtf8DebugString(const google::protobuf::Message& msg) {
        google::protobuf::TextFormat::Printer printer;
        printer.SetSingleLineMode(true);
        printer.SetUseUtf8StringEscaping(true);

        std::string str;
        printer.PrintToString(msg, &str);

        // Copied from text_format.h
        // Single line mode currently might have an extra space at the end.
        if (str.size() > 0 && str[str.size() - 1] == ' ') {
            str.resize(str.size() - 1);
        }
        return str;
    }
}

Y_DECLARE_OUT_SPEC(, Ydb::VariantType, stream, value) {
    stream << "{ " << ShortUtf8DebugString(value) << " }";
}

Y_DECLARE_OUT_SPEC(, Ydb::Topic::StreamReadMessage_ReadResponse, stream, value) {
    stream << "{ " << ShortUtf8DebugString(value) << " }";
}

Y_DECLARE_OUT_SPEC(, Ydb::Topic::StreamReadMessage_CommitOffsetResponse, stream, value) {
    stream << "{ " << ShortUtf8DebugString(value) << " }";
}
