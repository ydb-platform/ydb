// GENERATED FILE - DO NOT MODIFY

#include <algorithm>

#include "tensorflow/core/lib/core/error_codes.pb_text-impl.h"

using ::tensorflow::strings::Scanner;
using ::tensorflow::strings::StrCat;

namespace tensorflow {
namespace error {

const char* EnumName_Code(
    ::tensorflow::error::Code value) {
  switch (value) {
    case 0: return "OK";
    case 1: return "CANCELLED";
    case 2: return "UNKNOWN";
    case 3: return "INVALID_ARGUMENT";
    case 4: return "DEADLINE_EXCEEDED";
    case 5: return "NOT_FOUND";
    case 6: return "ALREADY_EXISTS";
    case 7: return "PERMISSION_DENIED";
    case 16: return "UNAUTHENTICATED";
    case 8: return "RESOURCE_EXHAUSTED";
    case 9: return "FAILED_PRECONDITION";
    case 10: return "ABORTED";
    case 11: return "OUT_OF_RANGE";
    case 12: return "UNIMPLEMENTED";
    case 13: return "INTERNAL";
    case 14: return "UNAVAILABLE";
    case 15: return "DATA_LOSS";
    case 20: return "DO_NOT_USE_RESERVED_FOR_FUTURE_EXPANSION_USE_DEFAULT_IN_SWITCH_INSTEAD_";
    default: return "";
  }
}

}  // namespace error
}  // namespace tensorflow
