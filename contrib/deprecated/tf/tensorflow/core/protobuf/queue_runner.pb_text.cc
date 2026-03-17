// GENERATED FILE - DO NOT MODIFY

#include <algorithm>

#include "tensorflow/core/protobuf/queue_runner.pb_text-impl.h"

using ::tensorflow::strings::Scanner;
using ::tensorflow::strings::StrCat;

namespace tensorflow {

string ProtoDebugString(
    const ::tensorflow::QueueRunnerDef& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::QueueRunnerDef& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::QueueRunnerDef& msg) {
  o->AppendStringIfNotEmpty("queue_name", ProtobufStringToString(msg.queue_name()));
  for (int i = 0; i < msg.enqueue_op_name_size(); ++i) {
    o->AppendString("enqueue_op_name", ProtobufStringToString(msg.enqueue_op_name(i)));
  }
  o->AppendStringIfNotEmpty("close_op_name", ProtobufStringToString(msg.close_op_name()));
  o->AppendStringIfNotEmpty("cancel_op_name", ProtobufStringToString(msg.cancel_op_name()));
  for (int i = 0; i < msg.queue_closed_exception_types_size(); ++i) {
    const char* enum_name = ::tensorflow::error::EnumName_Code(msg.queue_closed_exception_types(i));
    if (enum_name[0]) {
      o->AppendEnumName("queue_closed_exception_types", enum_name);
    } else {
      o->AppendNumeric("queue_closed_exception_types", msg.queue_closed_exception_types(i));
    }
  }
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::QueueRunnerDef* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::QueueRunnerDef* msg) {
  std::vector<bool> has_seen(5, false);
  while(true) {
    ProtoSpaceAndComments(scanner);
    if (nested && (scanner->Peek() == (close_curly ? '}' : '>'))) {
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      return true;
    }
    if (!nested && scanner->empty()) { return true; }
    scanner->RestartCapture()
        .Many(Scanner::LETTER_DIGIT_UNDERSCORE)
        .StopCapture();
    StringPiece identifier;
    if (!scanner->GetResult(nullptr, &identifier)) return false;
    bool parsed_colon = false;
    (void)parsed_colon;
    ProtoSpaceAndComments(scanner);
    if (scanner->Peek() == ':') {
      parsed_colon = true;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
    }
    if (identifier == "queue_name") {
      if (has_seen[0]) return false;
      has_seen[0] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_queue_name());
    }
    else if (identifier == "enqueue_op_name") {
      const bool is_list = (scanner->Peek() == '[');
      do {
        if (is_list) {
          scanner->One(Scanner::ALL);
          ProtoSpaceAndComments(scanner);
        }
        string str_value;
        if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
            scanner, &str_value)) return false;
        SetProtobufStringSwapAllowed(&str_value, msg->add_enqueue_op_name());
      } while (is_list && scanner->Peek() == ',');
      if (is_list && !scanner->OneLiteral("]").GetResult()) return false;
    }
    else if (identifier == "close_op_name") {
      if (has_seen[2]) return false;
      has_seen[2] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_close_op_name());
    }
    else if (identifier == "cancel_op_name") {
      if (has_seen[3]) return false;
      has_seen[3] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_cancel_op_name());
    }
    else if (identifier == "queue_closed_exception_types") {
      const bool is_list = (scanner->Peek() == '[');
      do {
        if (is_list) {
          scanner->One(Scanner::ALL);
          ProtoSpaceAndComments(scanner);
        }
        StringPiece value;
        if (!parsed_colon || !scanner->RestartCapture().Many(Scanner::LETTER_DIGIT_DASH_UNDERSCORE).GetResult(nullptr, &value)) return false;
        if (value == "OK") {
          msg->add_queue_closed_exception_types(::tensorflow::error::OK);
        } else if (value == "CANCELLED") {
          msg->add_queue_closed_exception_types(::tensorflow::error::CANCELLED);
        } else if (value == "UNKNOWN") {
          msg->add_queue_closed_exception_types(::tensorflow::error::UNKNOWN);
        } else if (value == "INVALID_ARGUMENT") {
          msg->add_queue_closed_exception_types(::tensorflow::error::INVALID_ARGUMENT);
        } else if (value == "DEADLINE_EXCEEDED") {
          msg->add_queue_closed_exception_types(::tensorflow::error::DEADLINE_EXCEEDED);
        } else if (value == "NOT_FOUND") {
          msg->add_queue_closed_exception_types(::tensorflow::error::NOT_FOUND);
        } else if (value == "ALREADY_EXISTS") {
          msg->add_queue_closed_exception_types(::tensorflow::error::ALREADY_EXISTS);
        } else if (value == "PERMISSION_DENIED") {
          msg->add_queue_closed_exception_types(::tensorflow::error::PERMISSION_DENIED);
        } else if (value == "UNAUTHENTICATED") {
          msg->add_queue_closed_exception_types(::tensorflow::error::UNAUTHENTICATED);
        } else if (value == "RESOURCE_EXHAUSTED") {
          msg->add_queue_closed_exception_types(::tensorflow::error::RESOURCE_EXHAUSTED);
        } else if (value == "FAILED_PRECONDITION") {
          msg->add_queue_closed_exception_types(::tensorflow::error::FAILED_PRECONDITION);
        } else if (value == "ABORTED") {
          msg->add_queue_closed_exception_types(::tensorflow::error::ABORTED);
        } else if (value == "OUT_OF_RANGE") {
          msg->add_queue_closed_exception_types(::tensorflow::error::OUT_OF_RANGE);
        } else if (value == "UNIMPLEMENTED") {
          msg->add_queue_closed_exception_types(::tensorflow::error::UNIMPLEMENTED);
        } else if (value == "INTERNAL") {
          msg->add_queue_closed_exception_types(::tensorflow::error::INTERNAL);
        } else if (value == "UNAVAILABLE") {
          msg->add_queue_closed_exception_types(::tensorflow::error::UNAVAILABLE);
        } else if (value == "DATA_LOSS") {
          msg->add_queue_closed_exception_types(::tensorflow::error::DATA_LOSS);
        } else if (value == "DO_NOT_USE_RESERVED_FOR_FUTURE_EXPANSION_USE_DEFAULT_IN_SWITCH_INSTEAD_") {
          msg->add_queue_closed_exception_types(::tensorflow::error::DO_NOT_USE_RESERVED_FOR_FUTURE_EXPANSION_USE_DEFAULT_IN_SWITCH_INSTEAD_);
        } else {
          int32 int_value;
          if (strings::SafeStringToNumeric(value, &int_value)) {
            msg->add_queue_closed_exception_types(static_cast<::tensorflow::error::Code>(int_value));
          } else {
            return false;
          }
        }
      } while (is_list && scanner->Peek() == ',');
      if (is_list && !scanner->OneLiteral("]").GetResult()) return false;
    }
  }
}

}  // namespace internal

}  // namespace tensorflow
