// GENERATED FILE - DO NOT MODIFY

#include <algorithm>

#include "tensorflow/core/protobuf/saver.pb_text-impl.h"

using ::tensorflow::strings::Scanner;
using ::tensorflow::strings::StrCat;

namespace tensorflow {

const char* EnumName_SaverDef_CheckpointFormatVersion(
    ::tensorflow::SaverDef_CheckpointFormatVersion value) {
  switch (value) {
    case 0: return "LEGACY";
    case 1: return "V1";
    case 2: return "V2";
    default: return "";
  }
}

string ProtoDebugString(
    const ::tensorflow::SaverDef& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::SaverDef& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::SaverDef& msg) {
  o->AppendStringIfNotEmpty("filename_tensor_name", ProtobufStringToString(msg.filename_tensor_name()));
  o->AppendStringIfNotEmpty("save_tensor_name", ProtobufStringToString(msg.save_tensor_name()));
  o->AppendStringIfNotEmpty("restore_op_name", ProtobufStringToString(msg.restore_op_name()));
  o->AppendNumericIfNotZero("max_to_keep", msg.max_to_keep());
  o->AppendBoolIfTrue("sharded", msg.sharded());
  o->AppendNumericIfNotZero("keep_checkpoint_every_n_hours", msg.keep_checkpoint_every_n_hours());
  if (msg.version() != 0) {
    const char* enum_name = ::tensorflow::EnumName_SaverDef_CheckpointFormatVersion(msg.version());
    if (enum_name[0]) {
      o->AppendEnumName("version", enum_name);
    } else {
      o->AppendNumeric("version", msg.version());
    }
  }
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::SaverDef* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::SaverDef* msg) {
  std::vector<bool> has_seen(7, false);
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
    if (identifier == "filename_tensor_name") {
      if (has_seen[0]) return false;
      has_seen[0] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_filename_tensor_name());
    }
    else if (identifier == "save_tensor_name") {
      if (has_seen[1]) return false;
      has_seen[1] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_save_tensor_name());
    }
    else if (identifier == "restore_op_name") {
      if (has_seen[2]) return false;
      has_seen[2] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_restore_op_name());
    }
    else if (identifier == "max_to_keep") {
      if (has_seen[3]) return false;
      has_seen[3] = true;
      int32 value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseNumericFromScanner(scanner, &value)) return false;
      msg->set_max_to_keep(value);
    }
    else if (identifier == "sharded") {
      if (has_seen[4]) return false;
      has_seen[4] = true;
      bool value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseBoolFromScanner(scanner, &value)) return false;
      msg->set_sharded(value);
    }
    else if (identifier == "keep_checkpoint_every_n_hours") {
      if (has_seen[5]) return false;
      has_seen[5] = true;
      float value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseNumericFromScanner(scanner, &value)) return false;
      msg->set_keep_checkpoint_every_n_hours(value);
    }
    else if (identifier == "version") {
      if (has_seen[6]) return false;
      has_seen[6] = true;
      StringPiece value;
      if (!parsed_colon || !scanner->RestartCapture().Many(Scanner::LETTER_DIGIT_DASH_UNDERSCORE).GetResult(nullptr, &value)) return false;
      if (value == "LEGACY") {
        msg->set_version(::tensorflow::SaverDef_CheckpointFormatVersion_LEGACY);
      } else if (value == "V1") {
        msg->set_version(::tensorflow::SaverDef_CheckpointFormatVersion_V1);
      } else if (value == "V2") {
        msg->set_version(::tensorflow::SaverDef_CheckpointFormatVersion_V2);
      } else {
        int32 int_value;
        if (strings::SafeStringToNumeric(value, &int_value)) {
          msg->set_version(static_cast<::tensorflow::SaverDef_CheckpointFormatVersion>(int_value));
        } else {
          return false;
        }
      }
    }
  }
}

}  // namespace internal

}  // namespace tensorflow
