// GENERATED FILE - DO NOT MODIFY

#include <algorithm>

#include "tensorflow/core/protobuf/rewriter_config.pb_text-impl.h"

using ::tensorflow::strings::Scanner;
using ::tensorflow::strings::StrCat;

namespace tensorflow {

string ProtoDebugString(
    const ::tensorflow::AutoParallelOptions& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::AutoParallelOptions& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::AutoParallelOptions& msg) {
  o->AppendBoolIfTrue("enable", msg.enable());
  o->AppendNumericIfNotZero("num_replicas", msg.num_replicas());
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::AutoParallelOptions* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::AutoParallelOptions* msg) {
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
    if (identifier == "enable") {
      if (has_seen[0]) return false;
      has_seen[0] = true;
      bool value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseBoolFromScanner(scanner, &value)) return false;
      msg->set_enable(value);
    }
    else if (identifier == "num_replicas") {
      if (has_seen[1]) return false;
      has_seen[1] = true;
      int32 value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseNumericFromScanner(scanner, &value)) return false;
      msg->set_num_replicas(value);
    }
  }
}

}  // namespace internal

string ProtoDebugString(
    const ::tensorflow::ScopedAllocatorOptions& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::ScopedAllocatorOptions& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::ScopedAllocatorOptions& msg) {
  for (int i = 0; i < msg.enable_op_size(); ++i) {
    o->AppendString("enable_op", ProtobufStringToString(msg.enable_op(i)));
  }
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::ScopedAllocatorOptions* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::ScopedAllocatorOptions* msg) {
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
    if (identifier == "enable_op") {
      const bool is_list = (scanner->Peek() == '[');
      do {
        if (is_list) {
          scanner->One(Scanner::ALL);
          ProtoSpaceAndComments(scanner);
        }
        string str_value;
        if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
            scanner, &str_value)) return false;
        SetProtobufStringSwapAllowed(&str_value, msg->add_enable_op());
      } while (is_list && scanner->Peek() == ',');
      if (is_list && !scanner->OneLiteral("]").GetResult()) return false;
    }
  }
}

}  // namespace internal

const char* EnumName_RewriterConfig_Toggle(
    ::tensorflow::RewriterConfig_Toggle value) {
  switch (value) {
    case 0: return "DEFAULT";
    case 1: return "ON";
    case 2: return "OFF";
    case 3: return "AGGRESSIVE";
    default: return "";
  }
}

const char* EnumName_RewriterConfig_NumIterationsType(
    ::tensorflow::RewriterConfig_NumIterationsType value) {
  switch (value) {
    case 0: return "DEFAULT_NUM_ITERS";
    case 1: return "ONE";
    case 2: return "TWO";
    default: return "";
  }
}

const char* EnumName_RewriterConfig_MemOptType(
    ::tensorflow::RewriterConfig_MemOptType value) {
  switch (value) {
    case 0: return "DEFAULT_MEM_OPT";
    case 1: return "NO_MEM_OPT";
    case 2: return "MANUAL";
    case 4: return "SWAPPING_HEURISTICS";
    case 5: return "RECOMPUTATION_HEURISTICS";
    case 6: return "SCHEDULING_HEURISTICS";
    case 3: return "HEURISTICS";
    default: return "";
  }
}

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
    const ::tensorflow::RewriterConfig_CustomGraphOptimizer& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::RewriterConfig_CustomGraphOptimizer& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::RewriterConfig_CustomGraphOptimizer& msg) {
  o->AppendStringIfNotEmpty("name", ProtobufStringToString(msg.name()));
  {
    std::vector<string> keys;
    for (const auto& e : msg.parameter_map()) keys.push_back(e.first);
    std::stable_sort(keys.begin(), keys.end());
    for (const auto& key : keys) {
      o->OpenNestedMessage("parameter_map");
      o->AppendString("key", ProtobufStringToString(key));
      o->OpenNestedMessage("value");
      ::tensorflow::internal::AppendProtoDebugString(o, msg.parameter_map().at(key));
      o->CloseNestedMessage();
      o->CloseNestedMessage();
    }
  }
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::RewriterConfig_CustomGraphOptimizer* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::RewriterConfig_CustomGraphOptimizer* msg) {
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
    else if (identifier == "parameter_map") {
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
            scanner, true, open_char == '{', msg->mutable_parameter_map())) return false;
      } while (is_list && scanner->Peek() == ',');
      if (is_list && !scanner->OneLiteral("]").GetResult()) return false;
    }
  }
}

}  // namespace internal

string ProtoDebugString(
    const ::tensorflow::RewriterConfig& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, false);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

string ProtoShortDebugString(
    const ::tensorflow::RewriterConfig& msg) {
  string s;
  ::tensorflow::strings::ProtoTextOutput o(&s, true);
  internal::AppendProtoDebugString(&o, msg);
  o.CloseTopMessage();
  return s;
}

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::RewriterConfig& msg) {
  if (msg.layout_optimizer() != 0) {
    const char* enum_name = ::tensorflow::EnumName_RewriterConfig_Toggle(msg.layout_optimizer());
    if (enum_name[0]) {
      o->AppendEnumName("layout_optimizer", enum_name);
    } else {
      o->AppendNumeric("layout_optimizer", msg.layout_optimizer());
    }
  }
  o->AppendBoolIfTrue("disable_model_pruning", msg.disable_model_pruning());
  if (msg.constant_folding() != 0) {
    const char* enum_name = ::tensorflow::EnumName_RewriterConfig_Toggle(msg.constant_folding());
    if (enum_name[0]) {
      o->AppendEnumName("constant_folding", enum_name);
    } else {
      o->AppendNumeric("constant_folding", msg.constant_folding());
    }
  }
  if (msg.memory_optimization() != 0) {
    const char* enum_name = ::tensorflow::EnumName_RewriterConfig_MemOptType(msg.memory_optimization());
    if (enum_name[0]) {
      o->AppendEnumName("memory_optimization", enum_name);
    } else {
      o->AppendNumeric("memory_optimization", msg.memory_optimization());
    }
  }
  if (msg.has_auto_parallel()) {
    o->OpenNestedMessage("auto_parallel");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.auto_parallel());
    o->CloseNestedMessage();
  }
  o->AppendStringIfNotEmpty("memory_optimizer_target_node_name_scope", ProtobufStringToString(msg.memory_optimizer_target_node_name_scope()));
  if (msg.arithmetic_optimization() != 0) {
    const char* enum_name = ::tensorflow::EnumName_RewriterConfig_Toggle(msg.arithmetic_optimization());
    if (enum_name[0]) {
      o->AppendEnumName("arithmetic_optimization", enum_name);
    } else {
      o->AppendNumeric("arithmetic_optimization", msg.arithmetic_optimization());
    }
  }
  if (msg.dependency_optimization() != 0) {
    const char* enum_name = ::tensorflow::EnumName_RewriterConfig_Toggle(msg.dependency_optimization());
    if (enum_name[0]) {
      o->AppendEnumName("dependency_optimization", enum_name);
    } else {
      o->AppendNumeric("dependency_optimization", msg.dependency_optimization());
    }
  }
  if (msg.loop_optimization() != 0) {
    const char* enum_name = ::tensorflow::EnumName_RewriterConfig_Toggle(msg.loop_optimization());
    if (enum_name[0]) {
      o->AppendEnumName("loop_optimization", enum_name);
    } else {
      o->AppendNumeric("loop_optimization", msg.loop_optimization());
    }
  }
  if (msg.function_optimization() != 0) {
    const char* enum_name = ::tensorflow::EnumName_RewriterConfig_Toggle(msg.function_optimization());
    if (enum_name[0]) {
      o->AppendEnumName("function_optimization", enum_name);
    } else {
      o->AppendNumeric("function_optimization", msg.function_optimization());
    }
  }
  if (msg.debug_stripper() != 0) {
    const char* enum_name = ::tensorflow::EnumName_RewriterConfig_Toggle(msg.debug_stripper());
    if (enum_name[0]) {
      o->AppendEnumName("debug_stripper", enum_name);
    } else {
      o->AppendNumeric("debug_stripper", msg.debug_stripper());
    }
  }
  if (msg.meta_optimizer_iterations() != 0) {
    const char* enum_name = ::tensorflow::EnumName_RewriterConfig_NumIterationsType(msg.meta_optimizer_iterations());
    if (enum_name[0]) {
      o->AppendEnumName("meta_optimizer_iterations", enum_name);
    } else {
      o->AppendNumeric("meta_optimizer_iterations", msg.meta_optimizer_iterations());
    }
  }
  if (msg.shape_optimization() != 0) {
    const char* enum_name = ::tensorflow::EnumName_RewriterConfig_Toggle(msg.shape_optimization());
    if (enum_name[0]) {
      o->AppendEnumName("shape_optimization", enum_name);
    } else {
      o->AppendNumeric("shape_optimization", msg.shape_optimization());
    }
  }
  if (msg.remapping() != 0) {
    const char* enum_name = ::tensorflow::EnumName_RewriterConfig_Toggle(msg.remapping());
    if (enum_name[0]) {
      o->AppendEnumName("remapping", enum_name);
    } else {
      o->AppendNumeric("remapping", msg.remapping());
    }
  }
  if (msg.scoped_allocator_optimization() != 0) {
    const char* enum_name = ::tensorflow::EnumName_RewriterConfig_Toggle(msg.scoped_allocator_optimization());
    if (enum_name[0]) {
      o->AppendEnumName("scoped_allocator_optimization", enum_name);
    } else {
      o->AppendNumeric("scoped_allocator_optimization", msg.scoped_allocator_optimization());
    }
  }
  if (msg.has_scoped_allocator_opts()) {
    o->OpenNestedMessage("scoped_allocator_opts");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.scoped_allocator_opts());
    o->CloseNestedMessage();
  }
  o->AppendNumericIfNotZero("min_graph_nodes", msg.min_graph_nodes());
  for (int i = 0; i < msg.optimizers_size(); ++i) {
    o->AppendString("optimizers", ProtobufStringToString(msg.optimizers(i)));
  }
  for (int i = 0; i < msg.custom_optimizers_size(); ++i) {
    o->OpenNestedMessage("custom_optimizers");
    ::tensorflow::internal::AppendProtoDebugString(o, msg.custom_optimizers(i));
    o->CloseNestedMessage();
  }
}

}  // namespace internal

bool ProtoParseFromString(
    const string& s,
    ::tensorflow::RewriterConfig* msg) {
  msg->Clear();
  Scanner scanner(s);
  if (!internal::ProtoParseFromScanner(&scanner, false, false, msg)) return false;
  scanner.Eos();
  return scanner.GetResult();
}

namespace internal {

bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::RewriterConfig* msg) {
  std::vector<bool> has_seen(19, false);
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
    if (identifier == "layout_optimizer") {
      if (has_seen[0]) return false;
      has_seen[0] = true;
      StringPiece value;
      if (!parsed_colon || !scanner->RestartCapture().Many(Scanner::LETTER_DIGIT_DASH_UNDERSCORE).GetResult(nullptr, &value)) return false;
      if (value == "DEFAULT") {
        msg->set_layout_optimizer(::tensorflow::RewriterConfig_Toggle_DEFAULT);
      } else if (value == "ON") {
        msg->set_layout_optimizer(::tensorflow::RewriterConfig_Toggle_ON);
      } else if (value == "OFF") {
        msg->set_layout_optimizer(::tensorflow::RewriterConfig_Toggle_OFF);
      } else if (value == "AGGRESSIVE") {
        msg->set_layout_optimizer(::tensorflow::RewriterConfig_Toggle_AGGRESSIVE);
      } else {
        int32 int_value;
        if (strings::SafeStringToNumeric(value, &int_value)) {
          msg->set_layout_optimizer(static_cast<::tensorflow::RewriterConfig_Toggle>(int_value));
        } else {
          return false;
        }
      }
    }
    else if (identifier == "constant_folding") {
      if (has_seen[1]) return false;
      has_seen[1] = true;
      StringPiece value;
      if (!parsed_colon || !scanner->RestartCapture().Many(Scanner::LETTER_DIGIT_DASH_UNDERSCORE).GetResult(nullptr, &value)) return false;
      if (value == "DEFAULT") {
        msg->set_constant_folding(::tensorflow::RewriterConfig_Toggle_DEFAULT);
      } else if (value == "ON") {
        msg->set_constant_folding(::tensorflow::RewriterConfig_Toggle_ON);
      } else if (value == "OFF") {
        msg->set_constant_folding(::tensorflow::RewriterConfig_Toggle_OFF);
      } else if (value == "AGGRESSIVE") {
        msg->set_constant_folding(::tensorflow::RewriterConfig_Toggle_AGGRESSIVE);
      } else {
        int32 int_value;
        if (strings::SafeStringToNumeric(value, &int_value)) {
          msg->set_constant_folding(static_cast<::tensorflow::RewriterConfig_Toggle>(int_value));
        } else {
          return false;
        }
      }
    }
    else if (identifier == "shape_optimization") {
      if (has_seen[2]) return false;
      has_seen[2] = true;
      StringPiece value;
      if (!parsed_colon || !scanner->RestartCapture().Many(Scanner::LETTER_DIGIT_DASH_UNDERSCORE).GetResult(nullptr, &value)) return false;
      if (value == "DEFAULT") {
        msg->set_shape_optimization(::tensorflow::RewriterConfig_Toggle_DEFAULT);
      } else if (value == "ON") {
        msg->set_shape_optimization(::tensorflow::RewriterConfig_Toggle_ON);
      } else if (value == "OFF") {
        msg->set_shape_optimization(::tensorflow::RewriterConfig_Toggle_OFF);
      } else if (value == "AGGRESSIVE") {
        msg->set_shape_optimization(::tensorflow::RewriterConfig_Toggle_AGGRESSIVE);
      } else {
        int32 int_value;
        if (strings::SafeStringToNumeric(value, &int_value)) {
          msg->set_shape_optimization(static_cast<::tensorflow::RewriterConfig_Toggle>(int_value));
        } else {
          return false;
        }
      }
    }
    else if (identifier == "remapping") {
      if (has_seen[3]) return false;
      has_seen[3] = true;
      StringPiece value;
      if (!parsed_colon || !scanner->RestartCapture().Many(Scanner::LETTER_DIGIT_DASH_UNDERSCORE).GetResult(nullptr, &value)) return false;
      if (value == "DEFAULT") {
        msg->set_remapping(::tensorflow::RewriterConfig_Toggle_DEFAULT);
      } else if (value == "ON") {
        msg->set_remapping(::tensorflow::RewriterConfig_Toggle_ON);
      } else if (value == "OFF") {
        msg->set_remapping(::tensorflow::RewriterConfig_Toggle_OFF);
      } else if (value == "AGGRESSIVE") {
        msg->set_remapping(::tensorflow::RewriterConfig_Toggle_AGGRESSIVE);
      } else {
        int32 int_value;
        if (strings::SafeStringToNumeric(value, &int_value)) {
          msg->set_remapping(static_cast<::tensorflow::RewriterConfig_Toggle>(int_value));
        } else {
          return false;
        }
      }
    }
    else if (identifier == "arithmetic_optimization") {
      if (has_seen[4]) return false;
      has_seen[4] = true;
      StringPiece value;
      if (!parsed_colon || !scanner->RestartCapture().Many(Scanner::LETTER_DIGIT_DASH_UNDERSCORE).GetResult(nullptr, &value)) return false;
      if (value == "DEFAULT") {
        msg->set_arithmetic_optimization(::tensorflow::RewriterConfig_Toggle_DEFAULT);
      } else if (value == "ON") {
        msg->set_arithmetic_optimization(::tensorflow::RewriterConfig_Toggle_ON);
      } else if (value == "OFF") {
        msg->set_arithmetic_optimization(::tensorflow::RewriterConfig_Toggle_OFF);
      } else if (value == "AGGRESSIVE") {
        msg->set_arithmetic_optimization(::tensorflow::RewriterConfig_Toggle_AGGRESSIVE);
      } else {
        int32 int_value;
        if (strings::SafeStringToNumeric(value, &int_value)) {
          msg->set_arithmetic_optimization(static_cast<::tensorflow::RewriterConfig_Toggle>(int_value));
        } else {
          return false;
        }
      }
    }
    else if (identifier == "dependency_optimization") {
      if (has_seen[5]) return false;
      has_seen[5] = true;
      StringPiece value;
      if (!parsed_colon || !scanner->RestartCapture().Many(Scanner::LETTER_DIGIT_DASH_UNDERSCORE).GetResult(nullptr, &value)) return false;
      if (value == "DEFAULT") {
        msg->set_dependency_optimization(::tensorflow::RewriterConfig_Toggle_DEFAULT);
      } else if (value == "ON") {
        msg->set_dependency_optimization(::tensorflow::RewriterConfig_Toggle_ON);
      } else if (value == "OFF") {
        msg->set_dependency_optimization(::tensorflow::RewriterConfig_Toggle_OFF);
      } else if (value == "AGGRESSIVE") {
        msg->set_dependency_optimization(::tensorflow::RewriterConfig_Toggle_AGGRESSIVE);
      } else {
        int32 int_value;
        if (strings::SafeStringToNumeric(value, &int_value)) {
          msg->set_dependency_optimization(static_cast<::tensorflow::RewriterConfig_Toggle>(int_value));
        } else {
          return false;
        }
      }
    }
    else if (identifier == "loop_optimization") {
      if (has_seen[6]) return false;
      has_seen[6] = true;
      StringPiece value;
      if (!parsed_colon || !scanner->RestartCapture().Many(Scanner::LETTER_DIGIT_DASH_UNDERSCORE).GetResult(nullptr, &value)) return false;
      if (value == "DEFAULT") {
        msg->set_loop_optimization(::tensorflow::RewriterConfig_Toggle_DEFAULT);
      } else if (value == "ON") {
        msg->set_loop_optimization(::tensorflow::RewriterConfig_Toggle_ON);
      } else if (value == "OFF") {
        msg->set_loop_optimization(::tensorflow::RewriterConfig_Toggle_OFF);
      } else if (value == "AGGRESSIVE") {
        msg->set_loop_optimization(::tensorflow::RewriterConfig_Toggle_AGGRESSIVE);
      } else {
        int32 int_value;
        if (strings::SafeStringToNumeric(value, &int_value)) {
          msg->set_loop_optimization(static_cast<::tensorflow::RewriterConfig_Toggle>(int_value));
        } else {
          return false;
        }
      }
    }
    else if (identifier == "function_optimization") {
      if (has_seen[7]) return false;
      has_seen[7] = true;
      StringPiece value;
      if (!parsed_colon || !scanner->RestartCapture().Many(Scanner::LETTER_DIGIT_DASH_UNDERSCORE).GetResult(nullptr, &value)) return false;
      if (value == "DEFAULT") {
        msg->set_function_optimization(::tensorflow::RewriterConfig_Toggle_DEFAULT);
      } else if (value == "ON") {
        msg->set_function_optimization(::tensorflow::RewriterConfig_Toggle_ON);
      } else if (value == "OFF") {
        msg->set_function_optimization(::tensorflow::RewriterConfig_Toggle_OFF);
      } else if (value == "AGGRESSIVE") {
        msg->set_function_optimization(::tensorflow::RewriterConfig_Toggle_AGGRESSIVE);
      } else {
        int32 int_value;
        if (strings::SafeStringToNumeric(value, &int_value)) {
          msg->set_function_optimization(static_cast<::tensorflow::RewriterConfig_Toggle>(int_value));
        } else {
          return false;
        }
      }
    }
    else if (identifier == "debug_stripper") {
      if (has_seen[8]) return false;
      has_seen[8] = true;
      StringPiece value;
      if (!parsed_colon || !scanner->RestartCapture().Many(Scanner::LETTER_DIGIT_DASH_UNDERSCORE).GetResult(nullptr, &value)) return false;
      if (value == "DEFAULT") {
        msg->set_debug_stripper(::tensorflow::RewriterConfig_Toggle_DEFAULT);
      } else if (value == "ON") {
        msg->set_debug_stripper(::tensorflow::RewriterConfig_Toggle_ON);
      } else if (value == "OFF") {
        msg->set_debug_stripper(::tensorflow::RewriterConfig_Toggle_OFF);
      } else if (value == "AGGRESSIVE") {
        msg->set_debug_stripper(::tensorflow::RewriterConfig_Toggle_AGGRESSIVE);
      } else {
        int32 int_value;
        if (strings::SafeStringToNumeric(value, &int_value)) {
          msg->set_debug_stripper(static_cast<::tensorflow::RewriterConfig_Toggle>(int_value));
        } else {
          return false;
        }
      }
    }
    else if (identifier == "disable_model_pruning") {
      if (has_seen[9]) return false;
      has_seen[9] = true;
      bool value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseBoolFromScanner(scanner, &value)) return false;
      msg->set_disable_model_pruning(value);
    }
    else if (identifier == "scoped_allocator_optimization") {
      if (has_seen[10]) return false;
      has_seen[10] = true;
      StringPiece value;
      if (!parsed_colon || !scanner->RestartCapture().Many(Scanner::LETTER_DIGIT_DASH_UNDERSCORE).GetResult(nullptr, &value)) return false;
      if (value == "DEFAULT") {
        msg->set_scoped_allocator_optimization(::tensorflow::RewriterConfig_Toggle_DEFAULT);
      } else if (value == "ON") {
        msg->set_scoped_allocator_optimization(::tensorflow::RewriterConfig_Toggle_ON);
      } else if (value == "OFF") {
        msg->set_scoped_allocator_optimization(::tensorflow::RewriterConfig_Toggle_OFF);
      } else if (value == "AGGRESSIVE") {
        msg->set_scoped_allocator_optimization(::tensorflow::RewriterConfig_Toggle_AGGRESSIVE);
      } else {
        int32 int_value;
        if (strings::SafeStringToNumeric(value, &int_value)) {
          msg->set_scoped_allocator_optimization(static_cast<::tensorflow::RewriterConfig_Toggle>(int_value));
        } else {
          return false;
        }
      }
    }
    else if (identifier == "meta_optimizer_iterations") {
      if (has_seen[11]) return false;
      has_seen[11] = true;
      StringPiece value;
      if (!parsed_colon || !scanner->RestartCapture().Many(Scanner::LETTER_DIGIT_DASH_UNDERSCORE).GetResult(nullptr, &value)) return false;
      if (value == "DEFAULT_NUM_ITERS") {
        msg->set_meta_optimizer_iterations(::tensorflow::RewriterConfig_NumIterationsType_DEFAULT_NUM_ITERS);
      } else if (value == "ONE") {
        msg->set_meta_optimizer_iterations(::tensorflow::RewriterConfig_NumIterationsType_ONE);
      } else if (value == "TWO") {
        msg->set_meta_optimizer_iterations(::tensorflow::RewriterConfig_NumIterationsType_TWO);
      } else {
        int32 int_value;
        if (strings::SafeStringToNumeric(value, &int_value)) {
          msg->set_meta_optimizer_iterations(static_cast<::tensorflow::RewriterConfig_NumIterationsType>(int_value));
        } else {
          return false;
        }
      }
    }
    else if (identifier == "min_graph_nodes") {
      if (has_seen[12]) return false;
      has_seen[12] = true;
      int32 value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseNumericFromScanner(scanner, &value)) return false;
      msg->set_min_graph_nodes(value);
    }
    else if (identifier == "memory_optimization") {
      if (has_seen[13]) return false;
      has_seen[13] = true;
      StringPiece value;
      if (!parsed_colon || !scanner->RestartCapture().Many(Scanner::LETTER_DIGIT_DASH_UNDERSCORE).GetResult(nullptr, &value)) return false;
      if (value == "DEFAULT_MEM_OPT") {
        msg->set_memory_optimization(::tensorflow::RewriterConfig_MemOptType_DEFAULT_MEM_OPT);
      } else if (value == "NO_MEM_OPT") {
        msg->set_memory_optimization(::tensorflow::RewriterConfig_MemOptType_NO_MEM_OPT);
      } else if (value == "MANUAL") {
        msg->set_memory_optimization(::tensorflow::RewriterConfig_MemOptType_MANUAL);
      } else if (value == "SWAPPING_HEURISTICS") {
        msg->set_memory_optimization(::tensorflow::RewriterConfig_MemOptType_SWAPPING_HEURISTICS);
      } else if (value == "RECOMPUTATION_HEURISTICS") {
        msg->set_memory_optimization(::tensorflow::RewriterConfig_MemOptType_RECOMPUTATION_HEURISTICS);
      } else if (value == "SCHEDULING_HEURISTICS") {
        msg->set_memory_optimization(::tensorflow::RewriterConfig_MemOptType_SCHEDULING_HEURISTICS);
      } else if (value == "HEURISTICS") {
        msg->set_memory_optimization(::tensorflow::RewriterConfig_MemOptType_HEURISTICS);
      } else {
        int32 int_value;
        if (strings::SafeStringToNumeric(value, &int_value)) {
          msg->set_memory_optimization(static_cast<::tensorflow::RewriterConfig_MemOptType>(int_value));
        } else {
          return false;
        }
      }
    }
    else if (identifier == "memory_optimizer_target_node_name_scope") {
      if (has_seen[14]) return false;
      has_seen[14] = true;
      string str_value;
      if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
          scanner, &str_value)) return false;
      SetProtobufStringSwapAllowed(&str_value, msg->mutable_memory_optimizer_target_node_name_scope());
    }
    else if (identifier == "auto_parallel") {
      if (has_seen[15]) return false;
      has_seen[15] = true;
      const char open_char = scanner->Peek();
      if (open_char != '{' && open_char != '<') return false;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!::tensorflow::internal::ProtoParseFromScanner(
          scanner, true, open_char == '{', msg->mutable_auto_parallel())) return false;
    }
    else if (identifier == "scoped_allocator_opts") {
      if (has_seen[16]) return false;
      has_seen[16] = true;
      const char open_char = scanner->Peek();
      if (open_char != '{' && open_char != '<') return false;
      scanner->One(Scanner::ALL);
      ProtoSpaceAndComments(scanner);
      if (!::tensorflow::internal::ProtoParseFromScanner(
          scanner, true, open_char == '{', msg->mutable_scoped_allocator_opts())) return false;
    }
    else if (identifier == "optimizers") {
      const bool is_list = (scanner->Peek() == '[');
      do {
        if (is_list) {
          scanner->One(Scanner::ALL);
          ProtoSpaceAndComments(scanner);
        }
        string str_value;
        if (!parsed_colon || !::tensorflow::strings::ProtoParseStringLiteralFromScanner(
            scanner, &str_value)) return false;
        SetProtobufStringSwapAllowed(&str_value, msg->add_optimizers());
      } while (is_list && scanner->Peek() == ',');
      if (is_list && !scanner->OneLiteral("]").GetResult()) return false;
    }
    else if (identifier == "custom_optimizers") {
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
            scanner, true, open_char == '{', msg->add_custom_optimizers())) return false;
      } while (is_list && scanner->Peek() == ',');
      if (is_list && !scanner->OneLiteral("]").GetResult()) return false;
    }
  }
}

}  // namespace internal

}  // namespace tensorflow
