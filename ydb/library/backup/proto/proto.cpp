#include "proto.h"

#include <google/protobuf/text_format.h>

namespace NYdb::NBackup {

bool ParseProto(const TString& text, google::protobuf::Message& message) {
    google::protobuf::TextFormat::Parser parser;
    parser.AllowUnknownField(true);
    return parser.ParseFromString(text, &message);
}

bool PrintProto(const google::protobuf::Message& message, TString& text) {
    google::protobuf::TextFormat::Printer printer;
    printer.SetHideUnknownFields(true);
    return printer.PrintToString(message, &text);
}

} // namespace NYdb::NBackup
