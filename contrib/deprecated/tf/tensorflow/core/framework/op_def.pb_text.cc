// GENERATED FILE - DO NOT MODIFY

#include <algorithm>

#include "tensorflow/core/framework/op_def.pb_text-impl.h"

using ::tensorflow::strings::Scanner;
using ::tensorflow::strings::StrCat;

namespace tensorflow {

string ProtoDebugString(
    const ::tensorflow::OpDef_ArgDef& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::OpDef_ArgDef& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::OpDef_ArgDef& msg) {
  o->AppendStringIfNotEmpty("name", ProtobufStringToString(msg.name()));
  o->AppendStringIfNotEmpty("description", ProtobufStringToString(msg.description()));
  if (msg.type() != 0) {
    const char* enum_name = ::tensorflow::EnumName_DataType(msg.type());
    if (enum_name[0]) {
      o->AppendEnumName("type", enum_name);
    } else {
      o->AppendNumeric("type", msg.type());
    }
  }
  o->AppendStringIfNotEmpty("type_attr", ProtobufStringToString(msg.type_attr()));
  o->AppendStringIfNotEmpty("number_attr", ProtobufStringToString(msg.number_attr()));
  o->AppendStringIfNotEmpty("type_list_attr", ProtobufStringToString(msg.type_list_attr()));
  o->AppendBoolIfTrue("is_ref", msg.is_ref());
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::OpDef_ArgDef* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::OpDef_ArgDef* msg) {
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
    if (identifier == "name") {
      if (has_seen[0]) return false;
      has_seen[0] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_name());
    }
    else if (identifier == "description") {
      if (has_seen[1]) return false;
      has_seen[1] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_description());
    }
    else if (identifier == "type") {
      if (has_seen[2]) return false;
      has_seen[2] = true;
      StringPiece value;
      if (!parsed_colon || !scanner->RestartCapture().Many(Scanner::LETTER_DIGIT_DASH_UNDERSCORE).GetResult(nullptr, &value)) return false;
      if (value == "DT_INVALID") {
        msg->set_type(::tensorflow::DT_INVALID);
      } else if (value == "DT_FLOAT") {
        msg->set_type(::tensorflow::DT_FLOAT);
      } else if (value == "DT_DOUBLE") {
        msg->set_type(::tensorflow::DT_DOUBLE);
      } else if (value == "DT_INT32") {
        msg->set_type(::tensorflow::DT_INT32);
      } else if (value == "DT_UINT8") {
        msg->set_type(::tensorflow::DT_UINT8);
      } else if (value == "DT_INT16") {
        msg->set_type(::tensorflow::DT_INT16);
      } else if (value == "DT_INT8") {
        msg->set_type(::tensorflow::DT_INT8);
      } else if (value == "DT_STRING") {
        msg->set_type(::tensorflow::DT_STRING);
      } else if (value == "DT_COMPLEX64") {
        msg->set_type(::tensorflow::DT_COMPLEX64);
      } else if (value == "DT_INT64") {
        msg->set_type(::tensorflow::DT_INT64);
      } else if (value == "DT_BOOL") {
        msg->set_type(::tensorflow::DT_BOOL);
      } else if (value == "DT_QINT8") {
        msg->set_type(::tensorflow::DT_QINT8);
      } else if (value == "DT_QUINT8") {
        msg->set_type(::tensorflow::DT_QUINT8);
      } else if (value == "DT_QINT32") {
        msg->set_type(::tensorflow::DT_QINT32);
      } else if (value == "DT_BFLOAT16") {
        msg->set_type(::tensorflow::DT_BFLOAT16);
      } else if (value == "DT_QINT16") {
        msg->set_type(::tensorflow::DT_QINT16);
      } else if (value == "DT_QUINT16") {
        msg->set_type(::tensorflow::DT_QUINT16);
      } else if (value == "DT_UINT16") {
        msg->set_type(::tensorflow::DT_UINT16);
      } else if (value == "DT_COMPLEX128") {
        msg->set_type(::tensorflow::DT_COMPLEX128);
      } else if (value == "DT_HALF") {
        msg->set_type(::tensorflow::DT_HALF);
      } else if (value == "DT_RESOURCE") {
        msg->set_type(::tensorflow::DT_RESOURCE);
      } else if (value == "DT_VARIANT") {
        msg->set_type(::tensorflow::DT_VARIANT);
      } else if (value == "DT_UINT32") {
        msg->set_type(::tensorflow::DT_UINT32);
      } else if (value == "DT_UINT64") {
        msg->set_type(::tensorflow::DT_UINT64);
      } else if (value == "DT_FLOAT_REF") {
        msg->set_type(::tensorflow::DT_FLOAT_REF);
      } else if (value == "DT_DOUBLE_REF") {
        msg->set_type(::tensorflow::DT_DOUBLE_REF);
      } else if (value == "DT_INT32_REF") {
        msg->set_type(::tensorflow::DT_INT32_REF);
      } else if (value == "DT_UINT8_REF") {
        msg->set_type(::tensorflow::DT_UINT8_REF);
      } else if (value == "DT_INT16_REF") {
        msg->set_type(::tensorflow::DT_INT16_REF);
      } else if (value == "DT_INT8_REF") {
        msg->set_type(::tensorflow::DT_INT8_REF);
      } else if (value == "DT_STRING_REF") {
        msg->set_type(::tensorflow::DT_STRING_REF);
      } else if (value == "DT_COMPLEX64_REF") {
        msg->set_type(::tensorflow::DT_COMPLEX64_REF);
      } else if (value == "DT_INT64_REF") {
        msg->set_type(::tensorflow::DT_INT64_REF);
      } else if (value == "DT_BOOL_REF") {
        msg->set_type(::tensorflow::DT_BOOL_REF);
      } else if (value == "DT_QINT8_REF") {
        msg->set_type(::tensorflow::DT_QINT8_REF);
      } else if (value == "DT_QUINT8_REF") {
        msg->set_type(::tensorflow::DT_QUINT8_REF);
      } else if (value == "DT_QINT32_REF") {
        msg->set_type(::tensorflow::DT_QINT32_REF);
      } else if (value == "DT_BFLOAT16_REF") {
        msg->set_type(::tensorflow::DT_BFLOAT16_REF);
      } else if (value == "DT_QINT16_REF") {
        msg->set_type(::tensorflow::DT_QINT16_REF);
      } else if (value == "DT_QUINT16_REF") {
        msg->set_type(::tensorflow::DT_QUINT16_REF);
      } else if (value == "DT_UINT16_REF") {
        msg->set_type(::tensorflow::DT_UINT16_REF);
      } else if (value == "DT_COMPLEX128_REF") {
        msg->set_type(::tensorflow::DT_COMPLEX128_REF);
      } else if (value == "DT_HALF_REF") {
        msg->set_type(::tensorflow::DT_HALF_REF);
      } else if (value == "DT_RESOURCE_REF") {
        msg->set_type(::tensorflow::DT_RESOURCE_REF);
      } else if (value == "DT_VARIANT_REF") {
        msg->set_type(::tensorflow::DT_VARIANT_REF);
      } else if (value == "DT_UINT32_REF") {
        msg->set_type(::tensorflow::DT_UINT32_REF);
      } else if (value == "DT_UINT64_REF") {
        msg->set_type(::tensorflow::DT_UINT64_REF);
      } else {
        int32 int_value;
        if (strings::SafeStringToNumeric(value, &int_value)) {
          msg->set_type(static_cast<::tensorflow::DataType>(int_value));
        } else {
          return false;
        }
      }
    }
    else if (identifier == "type_attr") {
      if (has_seen[3]) return false;
      has_seen[3] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_type_attr());
    }
    else if (identifier == "number_attr") {
      if (has_seen[4]) return false;
      has_seen[4] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_number_attr());
    }
    else if (identifier == "type_list_attr") {
      if (has_seen[5]) return false;
      has_seen[5] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_type_list_attr());
    }
    else if (identifier == "is_ref") {
      if (has_seen[6]) return false;
      has_seen[6] = true;
      bool value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseBoolFromScanner(scanner, &value)) return false;
      msg->set_is_ref(value);
    }
  }
}

}  // namespace internal

string ProtoDebugString(
    const ::tensorflow::OpDef_AttrDef& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::OpDef_AttrDef& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::OpDef_AttrDef& msg) {
  o->AppendStringIfNotEmpty("name", ProtobufStringToString(msg.name()));
  o->AppendStringIfNotEmpty("type", ProtobufStringToString(msg.type()));
  if (msg.has_default_value()) {
    o->OpenNestedMessage("default_value");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.default_value());
    o->CloseNestedMessage();
  }
  o->AppendStringIfNotEmpty("description", ProtobufStringToString(msg.description()));
  o->AppendBoolIfTrue("has_minimum", msg.has_minimum());
  o->AppendNumericIfNotZero("minimum", msg.minimum());
  if (msg.has_allowed_values()) {
    o->OpenNestedMessage("allowed_values");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.allowed_values());
    o->CloseNestedMessage();
  }
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::OpDef_AttrDef* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::OpDef_AttrDef* msg) {
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
    if (identifier == "name") {
      if (has_seen[0]) return false;
      has_seen[0] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_name());
    }
    else if (identifier == "type") {
      if (has_seen[1]) return false;
      has_seen[1] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_type());
    }
    else if (identifier == "default_value") {
      if (has_seen[2]) return false;
      has_seen[2] = true;
      const char open_char = scanner->Peek();
      if (open_char != '{' && open_char != '<') return false;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!::tensorflow::internal::ProtoParseFromScanner(
          scanner, true, open_char == '{', msg->mutable_default_value())) return false;
    }
    else if (identifier == "description") {
      if (has_seen[3]) return false;
      has_seen[3] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_description());
    }
    else if (identifier == "has_minimum") {
      if (has_seen[4]) return false;
      has_seen[4] = true;
      bool value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseBoolFromScanner(scanner, &value)) return false;
      msg->set_has_minimum(value);
    }
    else if (identifier == "minimum") {
      if (has_seen[5]) return false;
      has_seen[5] = true;
      int64 value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseNumericFromScanner(scanner, &value)) return false;
      msg->set_minimum(value);
    }
    else if (identifier == "allowed_values") {
      if (has_seen[6]) return false;
      has_seen[6] = true;
      const char open_char = scanner->Peek();
      if (open_char != '{' && open_char != '<') return false;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!::tensorflow::internal::ProtoParseFromScanner(
          scanner, true, open_char == '{', msg->mutable_allowed_values())) return false;
    }
  }
}

}  // namespace internal

string ProtoDebugString(
    const ::tensorflow::OpDef& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::OpDef& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::OpDef& msg) {
  o->AppendStringIfNotEmpty("name", ProtobufStringToString(msg.name()));
  for (int i = 0; i < msg.input_arg_size(); ++i) {
    o->OpenNestedMessage("input_arg");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.input_arg(i));
    o->CloseNestedMessage();
  }
  for (int i = 0; i < msg.output_arg_size(); ++i) {
    o->OpenNestedMessage("output_arg");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.output_arg(i));
    o->CloseNestedMessage();
  }
  for (int i = 0; i < msg.attr_size(); ++i) {
    o->OpenNestedMessage("attr");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.attr(i));
    o->CloseNestedMessage();
  }
  o->AppendStringIfNotEmpty("summary", ProtobufStringToString(msg.summary()));
  o->AppendStringIfNotEmpty("description", ProtobufStringToString(msg.description()));
  if (msg.has_deprecation()) {
    o->OpenNestedMessage("deprecation");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.deprecation());
    o->CloseNestedMessage();
  }
  o->AppendBoolIfTrue("is_aggregate", msg.is_aggregate());
  o->AppendBoolIfTrue("is_stateful", msg.is_stateful());
  o->AppendBoolIfTrue("is_commutative", msg.is_commutative());
  o->AppendBoolIfTrue("allows_uninitialized_input", msg.allows_uninitialized_input());
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::OpDef* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::OpDef* msg) {
  std::vector<bool> has_seen(11, false);
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
    if (identifier == "name") {
      if (has_seen[0]) return false;
      has_seen[0] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_name());
    }
    else if (identifier == "input_arg") {
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
            scanner, true, open_char == '{', msg->add_input_arg())) return false;
      } while (is_list && scanner->Peek() == ',');
      if (is_list && !scanner->OneLiteral("]").GetResult()) return false;
    }
    else if (identifier == "output_arg") {
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
            scanner, true, open_char == '{', msg->add_output_arg())) return false;
      } while (is_list && scanner->Peek() == ',');
      if (is_list && !scanner->OneLiteral("]").GetResult()) return false;
    }
    else if (identifier == "attr") {
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
            scanner, true, open_char == '{', msg->add_attr())) return false;
      } while (is_list && scanner->Peek() == ',');
      if (is_list && !scanner->OneLiteral("]").GetResult()) return false;
    }
    else if (identifier == "deprecation") {
      if (has_seen[4]) return false;
      has_seen[4] = true;
      const char open_char = scanner->Peek();
      if (open_char != '{' && open_char != '<') return false;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!::tensorflow::internal::ProtoParseFromScanner(
          scanner, true, open_char == '{', msg->mutable_deprecation())) return false;
    }
    else if (identifier == "summary") {
      if (has_seen[5]) return false;
      has_seen[5] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_summary());
    }
    else if (identifier == "description") {
      if (has_seen[6]) return false;
      has_seen[6] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_description());
    }
    else if (identifier == "is_commutative") {
      if (has_seen[7]) return false;
      has_seen[7] = true;
      bool value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseBoolFromScanner(scanner, &value)) return false;
      msg->set_is_commutative(value);
    }
    else if (identifier == "is_aggregate") {
      if (has_seen[8]) return false;
      has_seen[8] = true;
      bool value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseBoolFromScanner(scanner, &value)) return false;
      msg->set_is_aggregate(value);
    }
    else if (identifier == "is_stateful") {
      if (has_seen[9]) return false;
      has_seen[9] = true;
      bool value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseBoolFromScanner(scanner, &value)) return false;
      msg->set_is_stateful(value);
    }
    else if (identifier == "allows_uninitialized_input") {
      if (has_seen[10]) return false;
      has_seen[10] = true;
      bool value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseBoolFromScanner(scanner, &value)) return false;
      msg->set_allows_uninitialized_input(value);
    }
  }
}

}  // namespace internal

string ProtoDebugString(
    const ::tensorflow::OpDeprecation& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::OpDeprecation& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::OpDeprecation& msg) {
  o->AppendNumericIfNotZero("version", msg.version());
  o->AppendStringIfNotEmpty("explanation", ProtobufStringToString(msg.explanation()));
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::OpDeprecation* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::OpDeprecation* msg) {
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
    if (identifier == "version") {
      if (has_seen[0]) return false;
      has_seen[0] = true;
      int32 value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseNumericFromScanner(scanner, &value)) return false;
      msg->set_version(value);
    }
    else if (identifier == "explanation") {
      if (has_seen[1]) return false;
      has_seen[1] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_explanation());
    }
  }
}

}  // namespace internal

string ProtoDebugString(
    const ::tensorflow::OpList& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::OpList& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::OpList& msg) {
  for (int i = 0; i < msg.op_size(); ++i) {
    o->OpenNestedMessage("op");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.op(i));
    o->CloseNestedMessage();
  }
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::OpList* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::OpList* msg) {
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
    if (identifier == "op") {
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
            scanner, true, open_char == '{', msg->add_op())) return false;
      } while (is_list && scanner->Peek() == ',');
      if (is_list && !scanner->OneLiteral("]").GetResult()) return false;
    }
  }
}

}  // namespace internal

}  // namespace tensorflow
