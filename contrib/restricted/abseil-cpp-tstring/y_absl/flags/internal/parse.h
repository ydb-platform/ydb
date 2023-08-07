//
// Copyright 2019 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef Y_ABSL_FLAGS_INTERNAL_PARSE_H_
#define Y_ABSL_FLAGS_INTERNAL_PARSE_H_

#include <util/generic/string.h>
#include <vector>

#include "y_absl/base/config.h"
#include "y_absl/flags/declare.h"
#include "y_absl/strings/string_view.h"

Y_ABSL_DECLARE_FLAG(std::vector<TString>, flagfile);
Y_ABSL_DECLARE_FLAG(std::vector<TString>, fromenv);
Y_ABSL_DECLARE_FLAG(std::vector<TString>, tryfromenv);
Y_ABSL_DECLARE_FLAG(std::vector<TString>, undefok);

namespace y_absl {
Y_ABSL_NAMESPACE_BEGIN
namespace flags_internal {

enum class ArgvListAction { kRemoveParsedArgs, kKeepParsedArgs };
enum class UsageFlagsAction { kHandleUsage, kIgnoreUsage };
enum class OnUndefinedFlag {
  kIgnoreUndefined,
  kReportUndefined,
  kAbortIfUndefined
};

std::vector<char*> ParseCommandLineImpl(int argc, char* argv[],
                                        ArgvListAction arg_list_act,
                                        UsageFlagsAction usage_flag_act,
                                        OnUndefinedFlag on_undef_flag);

// --------------------------------------------------------------------
// Inspect original command line

// Returns true if flag with specified name was either present on the original
// command line or specified in flag file present on the original command line.
bool WasPresentOnCommandLine(y_absl::string_view flag_name);

// Return existing flags similar to the parameter, in order to help in case of
// misspellings.
std::vector<TString> GetMisspellingHints(y_absl::string_view flag);

}  // namespace flags_internal
Y_ABSL_NAMESPACE_END
}  // namespace y_absl

#endif  // Y_ABSL_FLAGS_INTERNAL_PARSE_H_
