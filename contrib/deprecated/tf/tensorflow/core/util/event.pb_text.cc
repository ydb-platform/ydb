// GENERATED FILE - DO NOT MODIFY

#include <algorithm>

#include "tensorflow/core/util/event.pb_text-impl.h"

using ::tensorflow::strings::Scanner;
using ::tensorflow::strings::StrCat;

namespace tensorflow {

const char* EnumName_WorkerHealth(
    ::tensorflow::WorkerHealth value) {
  switch (value) {
    case 0: return "OK";
    case 1: return "RECEIVED_SHUTDOWN_SIGNAL";
    case 2: return "INTERNAL_ERROR";
    default: return "";
  }
}

const char* EnumName_WorkerShutdownMode(
    ::tensorflow::WorkerShutdownMode value) {
  switch (value) {
    case 0: return "DEFAULT";
    case 1: return "SHUTDOWN_IMMEDIATELY";
    case 2: return "WAIT_FOR_COORDINATOR";
    default: return "";
  }
}

string ProtoDebugString(
    const ::tensorflow::Event& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::Event& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::Event& msg) {
  o->AppendNumericIfNotZero("wall_time", msg.wall_time());
  o->AppendNumericIfNotZero("step", msg.step());
  if (msg.what_case() == ::tensorflow::Event::kFileVersion) {
    o->AppendString("file_version", ProtobufStringToString(msg.file_version()));
  }
  if (msg.what_case() == ::tensorflow::Event::kGraphDef) {
    o->AppendString("graph_def", ProtobufStringToString(msg.graph_def()));
  }
  if (msg.what_case() == ::tensorflow::Event::kSummary) {
    o->OpenNestedMessage("summary");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.summary());
    o->CloseNestedMessage();
  }
  if (msg.what_case() == ::tensorflow::Event::kLogMessage) {
    o->OpenNestedMessage("log_message");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.log_message());
    o->CloseNestedMessage();
  }
  if (msg.what_case() == ::tensorflow::Event::kSessionLog) {
    o->OpenNestedMessage("session_log");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.session_log());
    o->CloseNestedMessage();
  }
  if (msg.what_case() == ::tensorflow::Event::kTaggedRunMetadata) {
    o->OpenNestedMessage("tagged_run_metadata");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.tagged_run_metadata());
    o->CloseNestedMessage();
  }
  if (msg.what_case() == ::tensorflow::Event::kMetaGraphDef) {
    o->AppendString("meta_graph_def", ProtobufStringToString(msg.meta_graph_def()));
  }
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::Event* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::Event* msg) {
  std::vector<bool> has_seen(9, false);
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
    if (identifier == "wall_time") {
      if (has_seen[0]) return false;
      has_seen[0] = true;
      double value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseNumericFromScanner(scanner, &value)) return false;
      msg->set_wall_time(value);
    }
    else if (identifier == "step") {
      if (has_seen[1]) return false;
      has_seen[1] = true;
      int64 value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseNumericFromScanner(scanner, &value)) return false;
      msg->set_step(value);
    }
    else if (identifier == "file_version") {
      if (msg->what_case() != 0) return false;
      if (has_seen[2]) return false;
      has_seen[2] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_file_version());
    }
    else if (identifier == "graph_def") {
      if (msg->what_case() != 0) return false;
      if (has_seen[3]) return false;
      has_seen[3] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_graph_def());
    }
    else if (identifier == "summary") {
      if (msg->what_case() != 0) return false;
      if (has_seen[4]) return false;
      has_seen[4] = true;
      const char open_char = scanner->Peek();
      if (open_char != '{' && open_char != '<') return false;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!::tensorflow::internal::ProtoParseFromScanner(
          scanner, true, open_char == '{', msg->mutable_summary())) return false;
    }
    else if (identifier == "log_message") {
      if (msg->what_case() != 0) return false;
      if (has_seen[5]) return false;
      has_seen[5] = true;
      const char open_char = scanner->Peek();
      if (open_char != '{' && open_char != '<') return false;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!::tensorflow::internal::ProtoParseFromScanner(
          scanner, true, open_char == '{', msg->mutable_log_message())) return false;
    }
    else if (identifier == "session_log") {
      if (msg->what_case() != 0) return false;
      if (has_seen[6]) return false;
      has_seen[6] = true;
      const char open_char = scanner->Peek();
      if (open_char != '{' && open_char != '<') return false;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!::tensorflow::internal::ProtoParseFromScanner(
          scanner, true, open_char == '{', msg->mutable_session_log())) return false;
    }
    else if (identifier == "tagged_run_metadata") {
      if (msg->what_case() != 0) return false;
      if (has_seen[7]) return false;
      has_seen[7] = true;
      const char open_char = scanner->Peek();
      if (open_char != '{' && open_char != '<') return false;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!::tensorflow::internal::ProtoParseFromScanner(
          scanner, true, open_char == '{', msg->mutable_tagged_run_metadata())) return false;
    }
    else if (identifier == "meta_graph_def") {
      if (msg->what_case() != 0) return false;
      if (has_seen[8]) return false;
      has_seen[8] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_meta_graph_def());
    }
  }
}

}  // namespace internal

const char* EnumName_LogMessage_Level(
    ::tensorflow::LogMessage_Level value) {
  switch (value) {
    case 0: return "UNKNOWN";
    case 10: return "DEBUGGING";
    case 20: return "INFO";
    case 30: return "WARN";
    case 40: return "ERROR";
    case 50: return "FATAL";
    default: return "";
  }
}

string ProtoDebugString(
    const ::tensorflow::LogMessage& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::LogMessage& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::LogMessage& msg) {
  if (msg.level() != 0) {
    const char* enum_name = ::tensorflow::EnumName_LogMessage_Level(msg.level());
    if (enum_name[0]) {
      o->AppendEnumName("level", enum_name);
    } else {
      o->AppendNumeric("level", msg.level());
    }
  }
  o->AppendStringIfNotEmpty("message", ProtobufStringToString(msg.message()));
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::LogMessage* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::LogMessage* msg) {
  std::vector<bool> has_seen(2, false);
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
    if (identifier == "level") {
      if (has_seen[0]) return false;
      has_seen[0] = true;
      StringPiece value;
      if (!parsed_colon || !scanner->RestartCapture().Many(Scanner::LETTER_DIGIT_DASH_UNDERSCORE).GetResult(nullptr, &value)) return false;
      if (value == "UNKNOWN") {
        msg->set_level(::tensorflow::LogMessage_Level_UNKNOWN);
      } else if (value == "DEBUGGING") {
        msg->set_level(::tensorflow::LogMessage_Level_DEBUGGING);
      } else if (value == "INFO") {
        msg->set_level(::tensorflow::LogMessage_Level_INFO);
      } else if (value == "WARN") {
        msg->set_level(::tensorflow::LogMessage_Level_WARN);
      } else if (value == "ERROR") {
        msg->set_level(::tensorflow::LogMessage_Level_ERROR);
      } else if (value == "FATAL") {
        msg->set_level(::tensorflow::LogMessage_Level_FATAL);
      } else {
        int32 int_value;
        if (strings::SafeStringToNumeric(value, &int_value)) {
          msg->set_level(static_cast<::tensorflow::LogMessage_Level>(int_value));
        } else {
          return false;
        }
      }
    }
    else if (identifier == "message") {
      if (has_seen[1]) return false;
      has_seen[1] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_message());
    }
  }
}

}  // namespace internal

const char* EnumName_SessionLog_SessionStatus(
    ::tensorflow::SessionLog_SessionStatus value) {
  switch (value) {
    case 0: return "STATUS_UNSPECIFIED";
    case 1: return "START";
    case 2: return "STOP";
    case 3: return "CHECKPOINT";
    default: return "";
  }
}

string ProtoDebugString(
    const ::tensorflow::SessionLog& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::SessionLog& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::SessionLog& msg) {
  if (msg.status() != 0) {
    const char* enum_name = ::tensorflow::EnumName_SessionLog_SessionStatus(msg.status());
    if (enum_name[0]) {
      o->AppendEnumName("status", enum_name);
    } else {
      o->AppendNumeric("status", msg.status());
    }
  }
  o->AppendStringIfNotEmpty("checkpoint_path", ProtobufStringToString(msg.checkpoint_path()));
  o->AppendStringIfNotEmpty("msg", ProtobufStringToString(msg.msg()));
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::SessionLog* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::SessionLog* msg) {
  std::vector<bool> has_seen(3, false);
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
    if (identifier == "status") {
      if (has_seen[0]) return false;
      has_seen[0] = true;
      StringPiece value;
      if (!parsed_colon || !scanner->RestartCapture().Many(Scanner::LETTER_DIGIT_DASH_UNDERSCORE).GetResult(nullptr, &value)) return false;
      if (value == "STATUS_UNSPECIFIED") {
        msg->set_status(::tensorflow::SessionLog_SessionStatus_STATUS_UNSPECIFIED);
      } else if (value == "START") {
        msg->set_status(::tensorflow::SessionLog_SessionStatus_START);
      } else if (value == "STOP") {
        msg->set_status(::tensorflow::SessionLog_SessionStatus_STOP);
      } else if (value == "CHECKPOINT") {
        msg->set_status(::tensorflow::SessionLog_SessionStatus_CHECKPOINT);
      } else {
        int32 int_value;
        if (strings::SafeStringToNumeric(value, &int_value)) {
          msg->set_status(static_cast<::tensorflow::SessionLog_SessionStatus>(int_value));
        } else {
          return false;
        }
      }
    }
    else if (identifier == "checkpoint_path") {
      if (has_seen[1]) return false;
      has_seen[1] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_checkpoint_path());
    }
    else if (identifier == "msg") {
      if (has_seen[2]) return false;
      has_seen[2] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_msg());
    }
  }
}

}  // namespace internal

string ProtoDebugString(
    const ::tensorflow::TaggedRunMetadata& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::TaggedRunMetadata& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::TaggedRunMetadata& msg) {
  o->AppendStringIfNotEmpty("tag", ProtobufStringToString(msg.tag()));
  o->AppendStringIfNotEmpty("run_metadata", ProtobufStringToString(msg.run_metadata()));
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::TaggedRunMetadata* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::TaggedRunMetadata* msg) {
  std::vector<bool> has_seen(2, false);
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
    if (identifier == "tag") {
      if (has_seen[0]) return false;
      has_seen[0] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_tag());
    }
    else if (identifier == "run_metadata") {
      if (has_seen[1]) return false;
      has_seen[1] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_run_metadata());
    }
  }
}

}  // namespace internal

string ProtoDebugString(
    const ::tensorflow::WatchdogConfig& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::WatchdogConfig& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::WatchdogConfig& msg) {
  o->AppendNumericIfNotZero("timeout_ms", msg.timeout_ms());
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::WatchdogConfig* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::WatchdogConfig* msg) {
  std::vector<bool> has_seen(1, false);
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
    if (identifier == "timeout_ms") {
      if (has_seen[0]) return false;
      has_seen[0] = true;
      int64 value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseNumericFromScanner(scanner, &value)) return false;
      msg->set_timeout_ms(value);
    }
  }
}

}  // namespace internal

string ProtoDebugString(
    const ::tensorflow::WorkerHeartbeatRequest& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::WorkerHeartbeatRequest& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::WorkerHeartbeatRequest& msg) {
  if (msg.shutdown_mode() != 0) {
    const char* enum_name = ::tensorflow::EnumName_WorkerShutdownMode(msg.shutdown_mode());
    if (enum_name[0]) {
      o->AppendEnumName("shutdown_mode", enum_name);
    } else {
      o->AppendNumeric("shutdown_mode", msg.shutdown_mode());
    }
  }
  if (msg.has_watchdog_config()) {
    o->OpenNestedMessage("watchdog_config");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.watchdog_config());
    o->CloseNestedMessage();
  }
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::WorkerHeartbeatRequest* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::WorkerHeartbeatRequest* msg) {
  std::vector<bool> has_seen(2, false);
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
    if (identifier == "shutdown_mode") {
      if (has_seen[0]) return false;
      has_seen[0] = true;
      StringPiece value;
      if (!parsed_colon || !scanner->RestartCapture().Many(Scanner::LETTER_DIGIT_DASH_UNDERSCORE).GetResult(nullptr, &value)) return false;
      if (value == "DEFAULT") {
        msg->set_shutdown_mode(::tensorflow::DEFAULT);
      } else if (value == "SHUTDOWN_IMMEDIATELY") {
        msg->set_shutdown_mode(::tensorflow::SHUTDOWN_IMMEDIATELY);
      } else if (value == "WAIT_FOR_COORDINATOR") {
        msg->set_shutdown_mode(::tensorflow::WAIT_FOR_COORDINATOR);
      } else {
        int32 int_value;
        if (strings::SafeStringToNumeric(value, &int_value)) {
          msg->set_shutdown_mode(static_cast<::tensorflow::WorkerShutdownMode>(int_value));
        } else {
          return false;
        }
      }
    }
    else if (identifier == "watchdog_config") {
      if (has_seen[1]) return false;
      has_seen[1] = true;
      const char open_char = scanner->Peek();
      if (open_char != '{' && open_char != '<') return false;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!::tensorflow::internal::ProtoParseFromScanner(
          scanner, true, open_char == '{', msg->mutable_watchdog_config())) return false;
    }
  }
}

}  // namespace internal

string ProtoDebugString(
    const ::tensorflow::WorkerHeartbeatResponse& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::WorkerHeartbeatResponse& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::WorkerHeartbeatResponse& msg) {
  if (msg.health_status() != 0) {
    const char* enum_name = ::tensorflow::EnumName_WorkerHealth(msg.health_status());
    if (enum_name[0]) {
      o->AppendEnumName("health_status", enum_name);
    } else {
      o->AppendNumeric("health_status", msg.health_status());
    }
  }
  for (int i = 0; i < msg.worker_log_size(); ++i) {
    o->OpenNestedMessage("worker_log");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.worker_log(i));
    o->CloseNestedMessage();
  }
  o->AppendStringIfNotEmpty("hostname", ProtobufStringToString(msg.hostname()));
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::WorkerHeartbeatResponse* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::WorkerHeartbeatResponse* msg) {
  std::vector<bool> has_seen(3, false);
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
    if (identifier == "health_status") {
      if (has_seen[0]) return false;
      has_seen[0] = true;
      StringPiece value;
      if (!parsed_colon || !scanner->RestartCapture().Many(Scanner::LETTER_DIGIT_DASH_UNDERSCORE).GetResult(nullptr, &value)) return false;
      if (value == "OK") {
        msg->set_health_status(::tensorflow::OK);
      } else if (value == "RECEIVED_SHUTDOWN_SIGNAL") {
        msg->set_health_status(::tensorflow::RECEIVED_SHUTDOWN_SIGNAL);
      } else if (value == "INTERNAL_ERROR") {
        msg->set_health_status(::tensorflow::INTERNAL_ERROR);
      } else {
        int32 int_value;
        if (strings::SafeStringToNumeric(value, &int_value)) {
          msg->set_health_status(static_cast<::tensorflow::WorkerHealth>(int_value));
        } else {
          return false;
        }
      }
    }
    else if (identifier == "worker_log") {
      const bool is_list = (scanner->Peek() == '[');
      do {
        if (is_list) {
          scanner->One(Scanner::ALL);
          ProtoSpaceAndComments(scanner);
        }
        const char open_char = scanner->Peek();
        if (open_char != '{' && open_char != '<') return false;
        scanner->One(Scanner::ALL);
        ProtoSpaceAndComments(scanner);
        if (!::tensorflow::internal::ProtoParseFromScanner(
            scanner, true, open_char == '{', msg->add_worker_log())) return false;
      } while (is_list && scanner->Peek() == ',');
      if (is_list && !scanner->OneLiteral("]").GetResult()) return false;
    }
    else if (identifier == "hostname") {
      if (has_seen[2]) return false;
      has_seen[2] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_hostname());
    }
  }
}

}  // namespace internal

}  // namespace tensorflow
