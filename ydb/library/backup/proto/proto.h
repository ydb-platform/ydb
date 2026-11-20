#pragma once

#include <util/generic/string.h>

namespace google::protobuf {
    class Message;
}

namespace NYdb::NBackup {

bool ParseProto(const TString& text, google::protobuf::Message& message);

bool PrintProto(const google::protobuf::Message& message, TString& text);

} // namespace NYdb::NBackup
