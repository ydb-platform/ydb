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

#include "y_absl/flags/parse.h"

#include <stdlib.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <iterator>
#include <util/generic/string.h>
#include <tuple>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#endif

#include "y_absl/base/attributes.h"
#include "y_absl/base/config.h"
#include "y_absl/base/const_init.h"
#include "y_absl/base/thread_annotations.h"
#include "y_absl/flags/commandlineflag.h"
#include "y_absl/flags/config.h"
#include "y_absl/flags/flag.h"
#include "y_absl/flags/internal/commandlineflag.h"
#include "y_absl/flags/internal/flag.h"
#include "y_absl/flags/internal/parse.h"
#include "y_absl/flags/internal/private_handle_accessor.h"
#include "y_absl/flags/internal/program_name.h"
#include "y_absl/flags/internal/usage.h"
#include "y_absl/flags/reflection.h"
#include "y_absl/flags/usage.h"
#include "y_absl/flags/usage_config.h"
#include "y_absl/strings/ascii.h"
#include "y_absl/strings/str_cat.h"
#include "y_absl/strings/string_view.h"
#include "y_absl/strings/strip.h"
#include "y_absl/synchronization/mutex.h"

// --------------------------------------------------------------------

namespace y_absl {
Y_ABSL_NAMESPACE_BEGIN
namespace flags_internal {
namespace {

Y_ABSL_CONST_INIT y_absl::Mutex processing_checks_guard(y_absl::kConstInit);

Y_ABSL_CONST_INIT bool flagfile_needs_processing
    Y_ABSL_GUARDED_BY(processing_checks_guard) = false;
Y_ABSL_CONST_INIT bool fromenv_needs_processing
    Y_ABSL_GUARDED_BY(processing_checks_guard) = false;
Y_ABSL_CONST_INIT bool tryfromenv_needs_processing
    Y_ABSL_GUARDED_BY(processing_checks_guard) = false;

Y_ABSL_CONST_INIT y_absl::Mutex specified_flags_guard(y_absl::kConstInit);
Y_ABSL_CONST_INIT std::vector<const CommandLineFlag*>* specified_flags
    Y_ABSL_GUARDED_BY(specified_flags_guard) = nullptr;

struct SpecifiedFlagsCompare {
  bool operator()(const CommandLineFlag* a, const CommandLineFlag* b) const {
    return a->Name() < b->Name();
  }
  bool operator()(const CommandLineFlag* a, y_absl::string_view b) const {
    return a->Name() < b;
  }
  bool operator()(y_absl::string_view a, const CommandLineFlag* b) const {
    return a < b->Name();
  }
};

}  // namespace
}  // namespace flags_internal
Y_ABSL_NAMESPACE_END
}  // namespace y_absl

Y_ABSL_FLAG(std::vector<TString>, flagfile, {},
          "comma-separated list of files to load flags from")
    .OnUpdate([]() {
      if (y_absl::GetFlag(FLAGS_flagfile).empty()) return;

      y_absl::MutexLock l(&y_absl::flags_internal::processing_checks_guard);

      // Setting this flag twice before it is handled most likely an internal
      // error and should be reviewed by developers.
      if (y_absl::flags_internal::flagfile_needs_processing) {
        Y_ABSL_INTERNAL_LOG(WARNING, "flagfile set twice before it is handled");
      }

      y_absl::flags_internal::flagfile_needs_processing = true;
    });
Y_ABSL_FLAG(std::vector<TString>, fromenv, {},
          "comma-separated list of flags to set from the environment"
          " [use 'export FLAGS_flag1=value']")
    .OnUpdate([]() {
      if (y_absl::GetFlag(FLAGS_fromenv).empty()) return;

      y_absl::MutexLock l(&y_absl::flags_internal::processing_checks_guard);

      // Setting this flag twice before it is handled most likely an internal
      // error and should be reviewed by developers.
      if (y_absl::flags_internal::fromenv_needs_processing) {
        Y_ABSL_INTERNAL_LOG(WARNING, "fromenv set twice before it is handled.");
      }

      y_absl::flags_internal::fromenv_needs_processing = true;
    });
Y_ABSL_FLAG(std::vector<TString>, tryfromenv, {},
          "comma-separated list of flags to try to set from the environment if "
          "present")
    .OnUpdate([]() {
      if (y_absl::GetFlag(FLAGS_tryfromenv).empty()) return;

      y_absl::MutexLock l(&y_absl::flags_internal::processing_checks_guard);

      // Setting this flag twice before it is handled most likely an internal
      // error and should be reviewed by developers.
      if (y_absl::flags_internal::tryfromenv_needs_processing) {
        Y_ABSL_INTERNAL_LOG(WARNING,
                          "tryfromenv set twice before it is handled.");
      }

      y_absl::flags_internal::tryfromenv_needs_processing = true;
    });

Y_ABSL_FLAG(std::vector<TString>, undefok, {},
          "comma-separated list of flag names that it is okay to specify "
          "on the command line even if the program does not define a flag "
          "with that name");

namespace y_absl {
Y_ABSL_NAMESPACE_BEGIN
namespace flags_internal {

namespace {

class ArgsList {
 public:
  ArgsList() : next_arg_(0) {}
  ArgsList(int argc, char* argv[]) : args_(argv, argv + argc), next_arg_(0) {}
  explicit ArgsList(const std::vector<TString>& args)
      : args_(args), next_arg_(0) {}

  // Returns success status: true if parsing successful, false otherwise.
  bool ReadFromFlagfile(const TString& flag_file_name);

  int Size() const { return args_.size() - next_arg_; }
  int FrontIndex() const { return next_arg_; }
  y_absl::string_view Front() const { return args_[next_arg_]; }
  void PopFront() { next_arg_++; }

 private:
  std::vector<TString> args_;
  int next_arg_;
};

bool ArgsList::ReadFromFlagfile(const TString& flag_file_name) {
  std::ifstream flag_file(flag_file_name);

  if (!flag_file) {
    flags_internal::ReportUsageError(
        y_absl::StrCat("Can't open flagfile ", flag_file_name), true);

    return false;
  }

  // This argument represents fake argv[0], which should be present in all arg
  // lists.
  args_.push_back("");

  std::string line;
  bool success = true;

  while (std::getline(flag_file, line)) {
    y_absl::string_view stripped = y_absl::StripLeadingAsciiWhitespace(line);

    if (stripped.empty() || stripped[0] == '#') {
      // Comment or empty line; just ignore.
      continue;
    }

    if (stripped[0] == '-') {
      if (stripped == "--") {
        flags_internal::ReportUsageError(
            "Flagfile can't contain position arguments or --", true);

        success = false;
        break;
      }

      args_.push_back(TString(stripped));
      continue;
    }

    flags_internal::ReportUsageError(
        y_absl::StrCat("Unexpected line in the flagfile ", flag_file_name, ": ",
                     line),
        true);

    success = false;
  }

  return success;
}

// --------------------------------------------------------------------

// Reads the environment variable with name `name` and stores results in
// `value`. If variable is not present in environment returns false, otherwise
// returns true.
bool GetEnvVar(const char* var_name, TString& var_value) {
#ifdef _WIN32
  char buf[1024];
  auto get_res = GetEnvironmentVariableA(var_name, buf, sizeof(buf));
  if (get_res >= sizeof(buf)) {
    return false;
  }

  if (get_res == 0) {
    return false;
  }

  var_value = TString(buf, get_res);
#else
  const char* val = ::getenv(var_name);
  if (val == nullptr) {
    return false;
  }

  var_value = val;
#endif

  return true;
}

// --------------------------------------------------------------------

// Returns:
//  Flag name or empty if arg= --
//  Flag value after = in --flag=value (empty if --foo)
//  "Is empty value" status. True if arg= --foo=, false otherwise. This is
//  required to separate --foo from --foo=.
// For example:
//      arg           return values
//   "--foo=bar" -> {"foo", "bar", false}.
//   "--foo"     -> {"foo", "", false}.
//   "--foo="    -> {"foo", "", true}.
std::tuple<y_absl::string_view, y_absl::string_view, bool> SplitNameAndValue(
    y_absl::string_view arg) {
  // Allow -foo and --foo
  y_absl::ConsumePrefix(&arg, "-");

  if (arg.empty()) {
    return std::make_tuple("", "", false);
  }

  auto equal_sign_pos = arg.find("=");

  y_absl::string_view flag_name = arg.substr(0, equal_sign_pos);

  y_absl::string_view value;
  bool is_empty_value = false;

  if (equal_sign_pos != y_absl::string_view::npos) {
    value = arg.substr(equal_sign_pos + 1);
    is_empty_value = value.empty();
  }

  return std::make_tuple(flag_name, value, is_empty_value);
}

// --------------------------------------------------------------------

// Returns:
//  found flag or nullptr
//  is negative in case of --nofoo
std::tuple<CommandLineFlag*, bool> LocateFlag(y_absl::string_view flag_name) {
  CommandLineFlag* flag = y_absl::FindCommandLineFlag(flag_name);
  bool is_negative = false;

  if (!flag && y_absl::ConsumePrefix(&flag_name, "no")) {
    flag = y_absl::FindCommandLineFlag(flag_name);
    is_negative = true;
  }

  return std::make_tuple(flag, is_negative);
}

// --------------------------------------------------------------------

// Verify that default values of typed flags must be convertible to string and
// back.
void CheckDefaultValuesParsingRoundtrip() {
#ifndef NDEBUG
  flags_internal::ForEachFlag([&](CommandLineFlag& flag) {
    if (flag.IsRetired()) return;

#define Y_ABSL_FLAGS_INTERNAL_IGNORE_TYPE(T, _) \
  if (flag.IsOfType<T>()) return;

    Y_ABSL_FLAGS_INTERNAL_SUPPORTED_TYPES(Y_ABSL_FLAGS_INTERNAL_IGNORE_TYPE)
#undef Y_ABSL_FLAGS_INTERNAL_IGNORE_TYPE

    flags_internal::PrivateHandleAccessor::CheckDefaultValueParsingRoundtrip(
        flag);
  });
#endif
}

// --------------------------------------------------------------------

// Returns success status, which is true if we successfully read all flag files,
// in which case new ArgLists are appended to the input_args in a reverse order
// of file names in the input flagfiles list. This order ensures that flags from
// the first flagfile in the input list are processed before the second flagfile
// etc.
bool ReadFlagfiles(const std::vector<TString>& flagfiles,
                   std::vector<ArgsList>& input_args) {
  bool success = true;
  for (auto it = flagfiles.rbegin(); it != flagfiles.rend(); ++it) {
    ArgsList al;

    if (al.ReadFromFlagfile(*it)) {
      input_args.push_back(al);
    } else {
      success = false;
    }
  }

  return success;
}

// Returns success status, which is true if were able to locate all environment
// variables correctly or if fail_on_absent_in_env is false. The environment
// variable names are expected to be of the form `FLAGS_<flag_name>`, where
// `flag_name` is a string from the input flag_names list. If successful we
// append a single ArgList at the end of the input_args.
bool ReadFlagsFromEnv(const std::vector<TString>& flag_names,
                      std::vector<ArgsList>& input_args,
                      bool fail_on_absent_in_env) {
  bool success = true;
  std::vector<TString> args;

  // This argument represents fake argv[0], which should be present in all arg
  // lists.
  args.push_back("");

  for (const auto& flag_name : flag_names) {
    // Avoid infinite recursion.
    if (flag_name == "fromenv" || flag_name == "tryfromenv") {
      flags_internal::ReportUsageError(
          y_absl::StrCat("Infinite recursion on flag ", flag_name), true);

      success = false;
      continue;
    }

    const TString envname = y_absl::StrCat("FLAGS_", flag_name);
    TString envval;
    if (!GetEnvVar(envname.c_str(), envval)) {
      if (fail_on_absent_in_env) {
        flags_internal::ReportUsageError(
            y_absl::StrCat(envname, " not found in environment"), true);

        success = false;
      }

      continue;
    }

    args.push_back(y_absl::StrCat("--", flag_name, "=", envval));
  }

  if (success) {
    input_args.emplace_back(args);
  }

  return success;
}

// --------------------------------------------------------------------

// Returns success status, which is true if were able to handle all generator
// flags (flagfile, fromenv, tryfromemv) successfully.
bool HandleGeneratorFlags(std::vector<ArgsList>& input_args,
                          std::vector<TString>& flagfile_value) {
  bool success = true;

  y_absl::MutexLock l(&flags_internal::processing_checks_guard);

  // flagfile could have been set either on a command line or
  // programmatically before invoking ParseCommandLine. Note that we do not
  // actually process arguments specified in the flagfile, but instead
  // create a secondary arguments list to be processed along with the rest
  // of the comamnd line arguments. Since we always the process most recently
  // created list of arguments first, this will result in flagfile argument
  // being processed before any other argument in the command line. If
  // FLAGS_flagfile contains more than one file name we create multiple new
  // levels of arguments in a reverse order of file names. Thus we always
  // process arguments from first file before arguments containing in a
  // second file, etc. If flagfile contains another
  // --flagfile inside of it, it will produce new level of arguments and
  // processed before the rest of the flagfile. We are also collecting all
  // flagfiles set on original command line. Unlike the rest of the flags,
  // this flag can be set multiple times and is expected to be handled
  // multiple times. We are collecting them all into a single list and set
  // the value of FLAGS_flagfile to that value at the end of the parsing.
  if (flags_internal::flagfile_needs_processing) {
    auto flagfiles = y_absl::GetFlag(FLAGS_flagfile);

    if (input_args.size() == 1) {
      flagfile_value.insert(flagfile_value.end(), flagfiles.begin(),
                            flagfiles.end());
    }

    success &= ReadFlagfiles(flagfiles, input_args);

    flags_internal::flagfile_needs_processing = false;
  }

  // Similar to flagfile fromenv/tryfromemv can be set both
  // programmatically and at runtime on a command line. Unlike flagfile these
  // can't be recursive.
  if (flags_internal::fromenv_needs_processing) {
    auto flags_list = y_absl::GetFlag(FLAGS_fromenv);

    success &= ReadFlagsFromEnv(flags_list, input_args, true);

    flags_internal::fromenv_needs_processing = false;
  }

  if (flags_internal::tryfromenv_needs_processing) {
    auto flags_list = y_absl::GetFlag(FLAGS_tryfromenv);

    success &= ReadFlagsFromEnv(flags_list, input_args, false);

    flags_internal::tryfromenv_needs_processing = false;
  }

  return success;
}

// --------------------------------------------------------------------

void ResetGeneratorFlags(const std::vector<TString>& flagfile_value) {
  // Setting flagfile to the value which collates all the values set on a
  // command line and programmatically. So if command line looked like
  // --flagfile=f1 --flagfile=f2 the final value of the FLAGS_flagfile flag is
  // going to be {"f1", "f2"}
  if (!flagfile_value.empty()) {
    y_absl::SetFlag(&FLAGS_flagfile, flagfile_value);
    y_absl::MutexLock l(&flags_internal::processing_checks_guard);
    flags_internal::flagfile_needs_processing = false;
  }

  // fromenv/tryfromenv are set to <undefined> value.
  if (!y_absl::GetFlag(FLAGS_fromenv).empty()) {
    y_absl::SetFlag(&FLAGS_fromenv, {});
  }
  if (!y_absl::GetFlag(FLAGS_tryfromenv).empty()) {
    y_absl::SetFlag(&FLAGS_tryfromenv, {});
  }

  y_absl::MutexLock l(&flags_internal::processing_checks_guard);
  flags_internal::fromenv_needs_processing = false;
  flags_internal::tryfromenv_needs_processing = false;
}

// --------------------------------------------------------------------

// Returns:
//  success status
//  deduced value
// We are also mutating curr_list in case if we need to get a hold of next
// argument in the input.
std::tuple<bool, y_absl::string_view> DeduceFlagValue(const CommandLineFlag& flag,
                                                    y_absl::string_view value,
                                                    bool is_negative,
                                                    bool is_empty_value,
                                                    ArgsList* curr_list) {
  // Value is either an argument suffix after `=` in "--foo=<value>"
  // or separate argument in case of "--foo" "<value>".

  // boolean flags have these forms:
  //   --foo
  //   --nofoo
  //   --foo=true
  //   --foo=false
  //   --nofoo=<value> is not supported
  //   --foo <value> is not supported

  // non boolean flags have these forms:
  // --foo=<value>
  // --foo <value>
  // --nofoo is not supported

  if (flag.IsOfType<bool>()) {
    if (value.empty()) {
      if (is_empty_value) {
        // "--bool_flag=" case
        flags_internal::ReportUsageError(
            y_absl::StrCat(
                "Missing the value after assignment for the boolean flag '",
                flag.Name(), "'"),
            true);
        return std::make_tuple(false, "");
      }

      // "--bool_flag" case
      value = is_negative ? "0" : "1";
    } else if (is_negative) {
      // "--nobool_flag=Y" case
      flags_internal::ReportUsageError(
          y_absl::StrCat("Negative form with assignment is not valid for the "
                       "boolean flag '",
                       flag.Name(), "'"),
          true);
      return std::make_tuple(false, "");
    }
  } else if (is_negative) {
    // "--noint_flag=1" case
    flags_internal::ReportUsageError(
        y_absl::StrCat("Negative form is not valid for the flag '", flag.Name(),
                     "'"),
        true);
    return std::make_tuple(false, "");
  } else if (value.empty() && (!is_empty_value)) {
    if (curr_list->Size() == 1) {
      // "--int_flag" case
      flags_internal::ReportUsageError(
          y_absl::StrCat("Missing the value for the flag '", flag.Name(), "'"),
          true);
      return std::make_tuple(false, "");
    }

    // "--int_flag" "10" case
    curr_list->PopFront();
    value = curr_list->Front();

    // Heuristic to detect the case where someone treats a string arg
    // like a bool or just forgets to pass a value:
    // --my_string_var --foo=bar
    // We look for a flag of string type, whose value begins with a
    // dash and corresponds to known flag or standalone --.
    if (!value.empty() && value[0] == '-' && flag.IsOfType<TString>()) {
      auto maybe_flag_name = std::get<0>(SplitNameAndValue(value.substr(1)));

      if (maybe_flag_name.empty() ||
          std::get<0>(LocateFlag(maybe_flag_name)) != nullptr) {
        // "--string_flag" "--known_flag" case
        Y_ABSL_INTERNAL_LOG(
            WARNING,
            y_absl::StrCat("Did you really mean to set flag '", flag.Name(),
                         "' to the value '", value, "'?"));
      }
    }
  }

  return std::make_tuple(true, value);
}

// --------------------------------------------------------------------

bool CanIgnoreUndefinedFlag(y_absl::string_view flag_name) {
  auto undefok = y_absl::GetFlag(FLAGS_undefok);
  if (std::find(undefok.begin(), undefok.end(), flag_name) != undefok.end()) {
    return true;
  }

  if (y_absl::ConsumePrefix(&flag_name, "no") &&
      std::find(undefok.begin(), undefok.end(), flag_name) != undefok.end()) {
    return true;
  }

  return false;
}

}  // namespace

// --------------------------------------------------------------------

bool WasPresentOnCommandLine(y_absl::string_view flag_name) {
  y_absl::MutexLock l(&specified_flags_guard);
  Y_ABSL_INTERNAL_CHECK(specified_flags != nullptr,
                      "ParseCommandLine is not invoked yet");

  return std::binary_search(specified_flags->begin(), specified_flags->end(),
                            flag_name, SpecifiedFlagsCompare{});
}

// --------------------------------------------------------------------

std::vector<char*> ParseCommandLineImpl(int argc, char* argv[],
                                        ArgvListAction arg_list_act,
                                        UsageFlagsAction usage_flag_act,
                                        OnUndefinedFlag on_undef_flag) {
  Y_ABSL_INTERNAL_CHECK(argc > 0, "Missing argv[0]");

  // Once parsing has started we will not have more flag registrations.
  // If we did, they would be missing during parsing, which is a problem on
  // itself.
  flags_internal::FinalizeRegistry();

  // This routine does not return anything since we abort on failure.
  CheckDefaultValuesParsingRoundtrip();

  std::vector<TString> flagfile_value;

  std::vector<ArgsList> input_args;
  input_args.push_back(ArgsList(argc, argv));

  std::vector<char*> output_args;
  std::vector<char*> positional_args;
  output_args.reserve(argc);

  // This is the list of undefined flags. The element of the list is the pair
  // consisting of boolean indicating if flag came from command line (vs from
  // some flag file we've read) and flag name.
  // TODO(rogeeff): Eliminate the first element in the pair after cleanup.
  std::vector<std::pair<bool, TString>> undefined_flag_names;

  // Set program invocation name if it is not set before.
  if (ProgramInvocationName() == "UNKNOWN") {
    flags_internal::SetProgramInvocationName(argv[0]);
  }
  output_args.push_back(argv[0]);

  y_absl::MutexLock l(&specified_flags_guard);
  if (specified_flags == nullptr) {
    specified_flags = new std::vector<const CommandLineFlag*>;
  } else {
    specified_flags->clear();
  }

  // Iterate through the list of the input arguments. First level are arguments
  // originated from argc/argv. Following levels are arguments originated from
  // recursive parsing of flagfile(s).
  bool success = true;
  while (!input_args.empty()) {
    // 10. First we process the built-in generator flags.
    success &= HandleGeneratorFlags(input_args, flagfile_value);

    // 30. Select top-most (most recent) arguments list. If it is empty drop it
    // and re-try.
    ArgsList& curr_list = input_args.back();

    curr_list.PopFront();

    if (curr_list.Size() == 0) {
      input_args.pop_back();
      continue;
    }

    // 40. Pick up the front remaining argument in the current list. If current
    // stack of argument lists contains only one element - we are processing an
    // argument from the original argv.
    y_absl::string_view arg(curr_list.Front());
    bool arg_from_argv = input_args.size() == 1;

    // 50. If argument does not start with - or is just "-" - this is
    // positional argument.
    if (!y_absl::ConsumePrefix(&arg, "-") || arg.empty()) {
      Y_ABSL_INTERNAL_CHECK(arg_from_argv,
                          "Flagfile cannot contain positional argument");

      positional_args.push_back(argv[curr_list.FrontIndex()]);
      continue;
    }

    if (arg_from_argv && (arg_list_act == ArgvListAction::kKeepParsedArgs)) {
      output_args.push_back(argv[curr_list.FrontIndex()]);
    }

    // 60. Split the current argument on '=' to figure out the argument
    // name and value. If flag name is empty it means we've got "--". value
    // can be empty either if there were no '=' in argument string at all or
    // an argument looked like "--foo=". In a latter case is_empty_value is
    // true.
    y_absl::string_view flag_name;
    y_absl::string_view value;
    bool is_empty_value = false;

    std::tie(flag_name, value, is_empty_value) = SplitNameAndValue(arg);

    // 70. "--" alone means what it does for GNU: stop flags parsing. We do
    // not support positional arguments in flagfiles, so we just drop them.
    if (flag_name.empty()) {
      Y_ABSL_INTERNAL_CHECK(arg_from_argv,
                          "Flagfile cannot contain positional argument");

      curr_list.PopFront();
      break;
    }

    // 80. Locate the flag based on flag name. Handle both --foo and --nofoo
    CommandLineFlag* flag = nullptr;
    bool is_negative = false;
    std::tie(flag, is_negative) = LocateFlag(flag_name);

    if (flag == nullptr) {
      // Usage flags are not modeled as Abseil flags. Locate them separately.
      if (flags_internal::DeduceUsageFlags(flag_name, value)) {
        continue;
      }

      if (on_undef_flag != OnUndefinedFlag::kIgnoreUndefined) {
        undefined_flag_names.emplace_back(arg_from_argv,
                                          TString(flag_name));
      }
      continue;
    }

    // 90. Deduce flag's value (from this or next argument)
    auto curr_index = curr_list.FrontIndex();
    bool value_success = true;
    std::tie(value_success, value) =
        DeduceFlagValue(*flag, value, is_negative, is_empty_value, &curr_list);
    success &= value_success;

    // If above call consumed an argument, it was a standalone value
    if (arg_from_argv && (arg_list_act == ArgvListAction::kKeepParsedArgs) &&
        (curr_index != curr_list.FrontIndex())) {
      output_args.push_back(argv[curr_list.FrontIndex()]);
    }

    // 100. Set the located flag to a new new value, unless it is retired.
    // Setting retired flag fails, but we ignoring it here while also reporting
    // access to retired flag.
    TString error;
    if (!flags_internal::PrivateHandleAccessor::ParseFrom(
            *flag, value, SET_FLAGS_VALUE, kCommandLine, error)) {
      if (flag->IsRetired()) continue;

      flags_internal::ReportUsageError(error, true);
      success = false;
    } else {
      specified_flags->push_back(flag);
    }
  }

  for (const auto& flag_name : undefined_flag_names) {
    if (CanIgnoreUndefinedFlag(flag_name.second)) continue;

    flags_internal::ReportUsageError(
        y_absl::StrCat("Unknown command line flag '", flag_name.second, "'"),
        true);

    success = false;
  }

#if Y_ABSL_FLAGS_STRIP_NAMES
  if (!success) {
    flags_internal::ReportUsageError(
        "NOTE: command line flags are disabled in this build", true);
  }
#endif

  if (!success) {
    flags_internal::HandleUsageFlags(std::cout,
                                     ProgramUsageMessage());
    std::exit(1);
  }

  if (usage_flag_act == UsageFlagsAction::kHandleUsage) {
    int exit_code = flags_internal::HandleUsageFlags(
        std::cout, ProgramUsageMessage());

    if (exit_code != -1) {
      std::exit(exit_code);
    }
  }

  ResetGeneratorFlags(flagfile_value);

  // Reinstate positional args which were intermixed with flags in the arguments
  // list.
  for (auto arg : positional_args) {
    output_args.push_back(arg);
  }

  // All the remaining arguments are positional.
  if (!input_args.empty()) {
    for (int arg_index = input_args.back().FrontIndex(); arg_index < argc;
         ++arg_index) {
      output_args.push_back(argv[arg_index]);
    }
  }

  // Trim and sort the vector.
  specified_flags->shrink_to_fit();
  std::sort(specified_flags->begin(), specified_flags->end(),
            SpecifiedFlagsCompare{});
  return output_args;
}

}  // namespace flags_internal

// --------------------------------------------------------------------

std::vector<char*> ParseCommandLine(int argc, char* argv[]) {
  return flags_internal::ParseCommandLineImpl(
      argc, argv, flags_internal::ArgvListAction::kRemoveParsedArgs,
      flags_internal::UsageFlagsAction::kHandleUsage,
      flags_internal::OnUndefinedFlag::kAbortIfUndefined);
}

Y_ABSL_NAMESPACE_END
}  // namespace y_absl
