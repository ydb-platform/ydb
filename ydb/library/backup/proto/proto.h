#pragma once

#include <util/generic/string.h>

namespace google::protobuf {
    class Message;
}

namespace NYdb::NBackup {

// Accept fields introduced by newer backup writers, while validating known fields.
bool ParseProto(const TString& text, google::protobuf::Message& message);

// Unknown binary fields cannot be round-tripped through protobuf text format.
bool PrintProto(const google::protobuf::Message& message, TString& text);

} // namespace NYdb::NBackup
