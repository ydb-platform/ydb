// GENERATED FILE - DO NOT MODIFY

#include <algorithm>

#include "tensorflow/core/framework/attr_value.pb_text-impl.h"

using ::tensorflow::strings::Scanner;
using ::tensorflow::strings::StrCat;

namespace tensorflow {

string ProtoDebugString(
    const ::tensorflow::AttrValue_ListValue& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::AttrValue_ListValue& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::AttrValue_ListValue& msg) {
  for (int i = 0; i < msg.s_size(); ++i) {
    o->AppendString("s", ProtobufStringToString(msg.s(i)));
  }
  for (int i = 0; i < msg.i_size(); ++i) {
    o->AppendNumeric("i", msg.i(i));
  }
  for (int i = 0; i < msg.f_size(); ++i) {
    o->AppendNumeric("f", msg.f(i));
  }
  for (int i = 0; i < msg.b_size(); ++i) {
    o->AppendBool("b", msg.b(i));
  }
  for (int i = 0; i < msg.type_size(); ++i) {
    const char* enum_name = ::tensorflow::EnumName_DataType(msg.type(i));
    if (enum_name[0]) {
      o->AppendEnumName("type", enum_name);
    } else {
      o->AppendNumeric("type", msg.type(i));
    }
  }
  for (int i = 0; i < msg.shape_size(); ++i) {
    o->OpenNestedMessage("shape");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.shape(i));
    o->CloseNestedMessage();
  }
  for (int i = 0; i < msg.tensor_size(); ++i) {
    o->OpenNestedMessage("tensor");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.tensor(i));
    o->CloseNestedMessage();
  }
  for (int i = 0; i < msg.func_size(); ++i) {
    o->OpenNestedMessage("func");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.func(i));
    o->CloseNestedMessage();
  }
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::AttrValue_ListValue* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::AttrValue_ListValue* msg) {
  std::vector<bool> has_seen(8, false);
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
    if (identifier == "s") {
      const bool is_list = (scanner->Peek() == '[');
      do {
        if (is_list) {
          scanner->One(Scanner::ALL);
          ProtoSpaceAndComments(scanner);
        }
        string str_value;
        if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
            scanner, &str_value)) return false;
        SetProtobufStringSwapAllowed(&str_value, msg->add_s());
      } while (is_list && scanner->Peek() == ',');
      if (is_list && !scanner->OneLiteral("]").GetResult()) return false;
    }
    else if (identifier == "i") {
      const bool is_list = (scanner->Peek() == '[');
      do {
        if (is_list) {
          scanner->One(Scanner::ALL);
          ProtoSpaceAndComments(scanner);
        }
        int64 value;
        if (!parsed_colon || !::tensorflow::strings::ProtoParseNumericFromScanner(scanner, &value)) return false;
        msg->add_i(value);
      } while (is_list && scanner->Peek() == ',');
      if (is_list && !scanner->OneLiteral("]").GetResult()) return false;
    }
    else if (identifier == "f") {
      const bool is_list = (scanner->Peek() == '[');
      do {
        if (is_list) {
          scanner->One(Scanner::ALL);
          ProtoSpaceAndComments(scanner);
        }
        float value;
        if (!parsed_colon || !::tensorflow::strings::ProtoParseNumericFromScanner(scanner, &value)) return false;
        msg->add_f(value);
      } while (is_list && scanner->Peek() == ',');
      if (is_list && !scanner->OneLiteral("]").GetResult()) return false;
    }
    else if (identifier == "b") {
      const bool is_list = (scanner->Peek() == '[');
      do {
        if (is_list) {
          scanner->One(Scanner::ALL);
          ProtoSpaceAndComments(scanner);
        }
        bool value;
        if (!parsed_colon || !::tensorflow::strings::ProtoParseBoolFromScanner(scanner, &value)) return false;
        msg->add_b(value);
      } while (is_list && scanner->Peek() == ',');
      if (is_list && !scanner->OneLiteral("]").GetResult()) return false;
    }
    else if (identifier == "type") {
      const bool is_list = (scanner->Peek() == '[');
      do {
        if (is_list) {
          scanner->One(Scanner::ALL);
          ProtoSpaceAndComments(scanner);
        }
        StringPiece value;
        if (!parsed_colon || !scanner->RestartCapture().Many(Scanner::LETTER_DIGIT_DASH_UNDERSCORE).GetResult(nullptr, &value)) return false;
        if (value == "DT_INVALID") {
          msg->add_type(::tensorflow::DT_INVALID);
        } else if (value == "DT_FLOAT") {
          msg->add_type(::tensorflow::DT_FLOAT);
        } else if (value == "DT_DOUBLE") {
          msg->add_type(::tensorflow::DT_DOUBLE);
        } else if (value == "DT_INT32") {
          msg->add_type(::tensorflow::DT_INT32);
        } else if (value == "DT_UINT8") {
          msg->add_type(::tensorflow::DT_UINT8);
        } else if (value == "DT_INT16") {
          msg->add_type(::tensorflow::DT_INT16);
        } else if (value == "DT_INT8") {
          msg->add_type(::tensorflow::DT_INT8);
        } else if (value == "DT_STRING") {
          msg->add_type(::tensorflow::DT_STRING);
        } else if (value == "DT_COMPLEX64") {
          msg->add_type(::tensorflow::DT_COMPLEX64);
        } else if (value == "DT_INT64") {
          msg->add_type(::tensorflow::DT_INT64);
        } else if (value == "DT_BOOL") {
          msg->add_type(::tensorflow::DT_BOOL);
        } else if (value == "DT_QINT8") {
          msg->add_type(::tensorflow::DT_QINT8);
        } else if (value == "DT_QUINT8") {
          msg->add_type(::tensorflow::DT_QUINT8);
        } else if (value == "DT_QINT32") {
          msg->add_type(::tensorflow::DT_QINT32);
        } else if (value == "DT_BFLOAT16") {
          msg->add_type(::tensorflow::DT_BFLOAT16);
        } else if (value == "DT_QINT16") {
          msg->add_type(::tensorflow::DT_QINT16);
        } else if (value == "DT_QUINT16") {
          msg->add_type(::tensorflow::DT_QUINT16);
        } else if (value == "DT_UINT16") {
          msg->add_type(::tensorflow::DT_UINT16);
        } else if (value == "DT_COMPLEX128") {
          msg->add_type(::tensorflow::DT_COMPLEX128);
        } else if (value == "DT_HALF") {
          msg->add_type(::tensorflow::DT_HALF);
        } else if (value == "DT_RESOURCE") {
          msg->add_type(::tensorflow::DT_RESOURCE);
        } else if (value == "DT_VARIANT") {
          msg->add_type(::tensorflow::DT_VARIANT);
        } else if (value == "DT_UINT32") {
          msg->add_type(::tensorflow::DT_UINT32);
        } else if (value == "DT_UINT64") {
          msg->add_type(::tensorflow::DT_UINT64);
        } else if (value == "DT_FLOAT_REF") {
          msg->add_type(::tensorflow::DT_FLOAT_REF);
        } else if (value == "DT_DOUBLE_REF") {
          msg->add_type(::tensorflow::DT_DOUBLE_REF);
        } else if (value == "DT_INT32_REF") {
          msg->add_type(::tensorflow::DT_INT32_REF);
        } else if (value == "DT_UINT8_REF") {
          msg->add_type(::tensorflow::DT_UINT8_REF);
        } else if (value == "DT_INT16_REF") {
          msg->add_type(::tensorflow::DT_INT16_REF);
        } else if (value == "DT_INT8_REF") {
          msg->add_type(::tensorflow::DT_INT8_REF);
        } else if (value == "DT_STRING_REF") {
          msg->add_type(::tensorflow::DT_STRING_REF);
        } else if (value == "DT_COMPLEX64_REF") {
          msg->add_type(::tensorflow::DT_COMPLEX64_REF);
        } else if (value == "DT_INT64_REF") {
          msg->add_type(::tensorflow::DT_INT64_REF);
        } else if (value == "DT_BOOL_REF") {
          msg->add_type(::tensorflow::DT_BOOL_REF);
        } else if (value == "DT_QINT8_REF") {
          msg->add_type(::tensorflow::DT_QINT8_REF);
        } else if (value == "DT_QUINT8_REF") {
          msg->add_type(::tensorflow::DT_QUINT8_REF);
        } else if (value == "DT_QINT32_REF") {
          msg->add_type(::tensorflow::DT_QINT32_REF);
        } else if (value == "DT_BFLOAT16_REF") {
          msg->add_type(::tensorflow::DT_BFLOAT16_REF);
        } else if (value == "DT_QINT16_REF") {
          msg->add_type(::tensorflow::DT_QINT16_REF);
        } else if (value == "DT_QUINT16_REF") {
          msg->add_type(::tensorflow::DT_QUINT16_REF);
        } else if (value == "DT_UINT16_REF") {
          msg->add_type(::tensorflow::DT_UINT16_REF);
        } else if (value == "DT_COMPLEX128_REF") {
          msg->add_type(::tensorflow::DT_COMPLEX128_REF);
        } else if (value == "DT_HALF_REF") {
          msg->add_type(::tensorflow::DT_HALF_REF);
        } else if (value == "DT_RESOURCE_REF") {
          msg->add_type(::tensorflow::DT_RESOURCE_REF);
        } else if (value == "DT_VARIANT_REF") {
          msg->add_type(::tensorflow::DT_VARIANT_REF);
        } else if (value == "DT_UINT32_REF") {
          msg->add_type(::tensorflow::DT_UINT32_REF);
        } else if (value == "DT_UINT64_REF") {
          msg->add_type(::tensorflow::DT_UINT64_REF);
        } else {
          int32 int_value;
          if (strings::SafeStringToNumeric(value, &int_value)) {
            msg->add_type(static_cast<::tensorflow::DataType>(int_value));
          } else {
            return false;
          }
        }
      } while (is_list && scanner->Peek() == ',');
      if (is_list && !scanner->OneLiteral("]").GetResult()) return false;
    }
    else if (identifier == "shape") {
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
            scanner, true, open_char == '{', msg->add_shape())) return false;
      } while (is_list && scanner->Peek() == ',');
      if (is_list && !scanner->OneLiteral("]").GetResult()) return false;
    }
    else if (identifier == "tensor") {
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
    else if (identifier == "func") {
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
            scanner, true, open_char == '{', msg->add_func())) return false;
      } while (is_list && scanner->Peek() == ',');
      if (is_list && !scanner->OneLiteral("]").GetResult()) return false;
    }
  }
}

}  // namespace internal

string ProtoDebugString(
    const ::tensorflow::AttrValue& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::AttrValue& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::AttrValue& msg) {
  if (msg.value_case() == ::tensorflow::AttrValue::kList) {
    o->OpenNestedMessage("list");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.list());
    o->CloseNestedMessage();
  }
  if (msg.value_case() == ::tensorflow::AttrValue::kS) {
    o->AppendString("s", ProtobufStringToString(msg.s()));
  }
  if (msg.value_case() == ::tensorflow::AttrValue::kI) {
    o->AppendNumeric("i", msg.i());
  }
  if (msg.value_case() == ::tensorflow::AttrValue::kF) {
    o->AppendNumeric("f", msg.f());
  }
  if (msg.value_case() == ::tensorflow::AttrValue::kB) {
    o->AppendBool("b", msg.b());
  }
  if (msg.value_case() == ::tensorflow::AttrValue::kType) {
    const char* enum_name = ::tensorflow::EnumName_DataType(msg.type());
    if (enum_name[0]) {
      o->AppendEnumName("type", enum_name);
    } else {
      o->AppendNumeric("type", msg.type());
    }
  }
  if (msg.value_case() == ::tensorflow::AttrValue::kShape) {
    o->OpenNestedMessage("shape");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.shape());
    o->CloseNestedMessage();
  }
  if (msg.value_case() == ::tensorflow::AttrValue::kTensor) {
    o->OpenNestedMessage("tensor");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.tensor());
    o->CloseNestedMessage();
  }
  if (msg.value_case() == ::tensorflow::AttrValue::kPlaceholder) {
    o->AppendString("placeholder", ProtobufStringToString(msg.placeholder()));
  }
  if (msg.value_case() == ::tensorflow::AttrValue::kFunc) {
    o->OpenNestedMessage("func");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.func());
    o->CloseNestedMessage();
  }
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::AttrValue* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::AttrValue* msg) {
  std::vector<bool> has_seen(10, false);
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
    if (identifier == "s") {
      if (msg->value_case() != 0) return false;
      if (has_seen[0]) return false;
      has_seen[0] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_s());
    }
    else if (identifier == "i") {
      if (msg->value_case() != 0) return false;
      if (has_seen[1]) return false;
      has_seen[1] = true;
      int64 value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseNumericFromScanner(scanner, &value)) return false;
      msg->set_i(value);
    }
    else if (identifier == "f") {
      if (msg->value_case() != 0) return false;
      if (has_seen[2]) return false;
      has_seen[2] = true;
      float value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseNumericFromScanner(scanner, &value)) return false;
      msg->set_f(value);
    }
    else if (identifier == "b") {
      if (msg->value_case() != 0) return false;
      if (has_seen[3]) return false;
      has_seen[3] = true;
      bool value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseBoolFromScanner(scanner, &value)) return false;
      msg->set_b(value);
    }
    else if (identifier == "type") {
      if (msg->value_case() != 0) return false;
      if (has_seen[4]) return false;
      has_seen[4] = true;
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
    else if (identifier == "shape") {
      if (msg->value_case() != 0) return false;
      if (has_seen[5]) return false;
      has_seen[5] = true;
      const char open_char = scanner->Peek();
      if (open_char != '{' && open_char != '<') return false;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!::tensorflow::internal::ProtoParseFromScanner(
          scanner, true, open_char == '{', msg->mutable_shape())) return false;
    }
    else if (identifier == "tensor") {
      if (msg->value_case() != 0) return false;
      if (has_seen[6]) return false;
      has_seen[6] = true;
      const char open_char = scanner->Peek();
      if (open_char != '{' && open_char != '<') return false;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!::tensorflow::internal::ProtoParseFromScanner(
          scanner, true, open_char == '{', msg->mutable_tensor())) return false;
    }
    else if (identifier == "list") {
      if (msg->value_case() != 0) return false;
      if (has_seen[7]) return false;
      has_seen[7] = true;
      const char open_char = scanner->Peek();
      if (open_char != '{' && open_char != '<') return false;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!::tensorflow::internal::ProtoParseFromScanner(
          scanner, true, open_char == '{', msg->mutable_list())) return false;
    }
    else if (identifier == "func") {
      if (msg->value_case() != 0) return false;
      if (has_seen[8]) return false;
      has_seen[8] = true;
      const char open_char = scanner->Peek();
      if (open_char != '{' && open_char != '<') return false;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!::tensorflow::internal::ProtoParseFromScanner(
          scanner, true, open_char == '{', msg->mutable_func())) return false;
    }
    else if (identifier == "placeholder") {
      if (msg->value_case() != 0) return false;
      if (has_seen[9]) return false;
      has_seen[9] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_placeholder());
    }
  }
}

}  // namespace internal

namespace internal {
namespace {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::protobuf::Map<string, ::tensorflow::AttrValue>* map) {
  string map_key;
  bool set_map_key = false;
  ::tensorflow::AttrValue map_value;
  bool set_map_value = false;
  std::vector<bool> has_seen(2, false);
  while(true) {
    ProtoSpaceAndComments(scanner);
    if (nested && (scanner->Peek() == (close_curly ? '}' : '>'))) {
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!set_map_key || !set_map_value) return false;
      (*map)[map_key] = map_value;
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
    if (identifier == "key") {
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, &map_key);
      set_map_key = true;
    }
    else if (identifier == "value") {
      const char open_char = scanner->Peek();
      if (open_char != '{' && open_char != '<') return false;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!::tensorflow::internal::ProtoParseFromScanner(
          scanner, true, open_char == '{', &map_value)) return false;
      set_map_value = true;
    }
  }
}

}  // namespace
}  // namespace internal

string ProtoDebugString(
    const ::tensorflow::NameAttrList& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::NameAttrList& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::NameAttrList& msg) {
  o->AppendStringIfNotEmpty("name", ProtobufStringToString(msg.name()));
  {
    std::vector<string> keys;
    for (const auto& e : msg.attr()) keys.push_back(e.first);
    std::stable_sort(keys.begin(), keys.end());
    for (const auto& key : keys) {
      o->OpenNestedMessage("attr");
      o->AppendString("key", ProtobufStringToString(key));
      o->OpenNestedMessage("value");
      ::tensorflow::internal::AppendProtoDebugString(o, msg.attr().at(key));
      o->CloseNestedMessage();
      o->CloseNestedMessage();
    }
  }
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::NameAttrList* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::NameAttrList* msg) {
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
    if (identifier == "name") {
      if (has_seen[0]) return false;
      has_seen[0] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_name());
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
        if (!ProtoParseFromScanner(
            scanner, true, open_char == '{', msg->mutable_attr())) return false;
      } while (is_list && scanner->Peek() == ',');
      if (is_list && !scanner->OneLiteral("]").GetResult()) return false;
    }
  }
}

}  // namespace internal

}  // namespace tensorflow
