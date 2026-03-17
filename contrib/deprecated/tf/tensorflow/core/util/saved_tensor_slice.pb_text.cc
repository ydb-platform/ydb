// GENERATED FILE - DO NOT MODIFY

#include <algorithm>

#include "tensorflow/core/util/saved_tensor_slice.pb_text-impl.h"

using ::tensorflow::strings::Scanner;
using ::tensorflow::strings::StrCat;

namespace tensorflow {

string ProtoDebugString(
    const ::tensorflow::SavedSliceMeta& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::SavedSliceMeta& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::SavedSliceMeta& msg) {
  o->AppendStringIfNotEmpty("name", ProtobufStringToString(msg.name()));
  if (msg.has_shape()) {
    o->OpenNestedMessage("shape");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.shape());
    o->CloseNestedMessage();
  }
  if (msg.type() != 0) {
    const char* enum_name = ::tensorflow::EnumName_DataType(msg.type());
    if (enum_name[0]) {
      o->AppendEnumName("type", enum_name);
    } else {
      o->AppendNumeric("type", msg.type());
    }
  }
  for (int i = 0; i < msg.slice_size(); ++i) {
    o->OpenNestedMessage("slice");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.slice(i));
    o->CloseNestedMessage();
  }
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::SavedSliceMeta* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::SavedSliceMeta* msg) {
  std::vector<bool> has_seen(4, false);
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
    else if (identifier == "shape") {
      if (has_seen[1]) return false;
      has_seen[1] = true;
      const char open_char = scanner->Peek();
      if (open_char != '{' && open_char != '<') return false;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!::tensorflow::internal::ProtoParseFromScanner(
          scanner, true, open_char == '{', msg->mutable_shape())) return false;
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
    else if (identifier == "slice") {
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
            scanner, true, open_char == '{', msg->add_slice())) return false;
      } while (is_list && scanner->Peek() == ',');
      if (is_list && !scanner->OneLiteral("]").GetResult()) return false;
    }
  }
}

}  // namespace internal

string ProtoDebugString(
    const ::tensorflow::SavedTensorSliceMeta& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::SavedTensorSliceMeta& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::SavedTensorSliceMeta& msg) {
  for (int i = 0; i < msg.tensor_size(); ++i) {
    o->OpenNestedMessage("tensor");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.tensor(i));
    o->CloseNestedMessage();
  }
  if (msg.has_versions()) {
    o->OpenNestedMessage("versions");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.versions());
    o->CloseNestedMessage();
  }
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::SavedTensorSliceMeta* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::SavedTensorSliceMeta* msg) {
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
    if (identifier == "tensor") {
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
            scanner, true, open_char == '{', msg->add_tensor())) return false;
      } while (is_list && scanner->Peek() == ',');
      if (is_list && !scanner->OneLiteral("]").GetResult()) return false;
    }
    else if (identifier == "versions") {
      if (has_seen[1]) return false;
      has_seen[1] = true;
      const char open_char = scanner->Peek();
      if (open_char != '{' && open_char != '<') return false;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!::tensorflow::internal::ProtoParseFromScanner(
          scanner, true, open_char == '{', msg->mutable_versions())) return false;
    }
  }
}

}  // namespace internal

string ProtoDebugString(
    const ::tensorflow::SavedSlice& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::SavedSlice& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::SavedSlice& msg) {
  o->AppendStringIfNotEmpty("name", ProtobufStringToString(msg.name()));
  if (msg.has_slice()) {
    o->OpenNestedMessage("slice");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.slice());
    o->CloseNestedMessage();
  }
  if (msg.has_data()) {
    o->OpenNestedMessage("data");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.data());
    o->CloseNestedMessage();
  }
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::SavedSlice* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::SavedSlice* msg) {
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
    if (identifier == "name") {
      if (has_seen[0]) return false;
      has_seen[0] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_name());
    }
    else if (identifier == "slice") {
      if (has_seen[1]) return false;
      has_seen[1] = true;
      const char open_char = scanner->Peek();
      if (open_char != '{' && open_char != '<') return false;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!::tensorflow::internal::ProtoParseFromScanner(
          scanner, true, open_char == '{', msg->mutable_slice())) return false;
    }
    else if (identifier == "data") {
      if (has_seen[2]) return false;
      has_seen[2] = true;
      const char open_char = scanner->Peek();
      if (open_char != '{' && open_char != '<') return false;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!::tensorflow::internal::ProtoParseFromScanner(
          scanner, true, open_char == '{', msg->mutable_data())) return false;
    }
  }
}

}  // namespace internal

string ProtoDebugString(
    const ::tensorflow::SavedTensorSlices& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::SavedTensorSlices& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::SavedTensorSlices& msg) {
  if (msg.has_meta()) {
    o->OpenNestedMessage("meta");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.meta());
    o->CloseNestedMessage();
  }
  if (msg.has_data()) {
    o->OpenNestedMessage("data");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.data());
    o->CloseNestedMessage();
  }
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::SavedTensorSlices* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::SavedTensorSlices* msg) {
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
    if (identifier == "meta") {
      if (has_seen[0]) return false;
      has_seen[0] = true;
      const char open_char = scanner->Peek();
      if (open_char != '{' && open_char != '<') return false;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!::tensorflow::internal::ProtoParseFromScanner(
          scanner, true, open_char == '{', msg->mutable_meta())) return false;
    }
    else if (identifier == "data") {
      if (has_seen[1]) return false;
      has_seen[1] = true;
      const char open_char = scanner->Peek();
      if (open_char != '{' && open_char != '<') return false;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!::tensorflow::internal::ProtoParseFromScanner(
          scanner, true, open_char == '{', msg->mutable_data())) return false;
    }
  }
}

}  // namespace internal

}  // namespace tensorflow
