#pragma once
#include <library/cpp/regex/pcre/regexp.h>

#include <google/protobuf/repeated_ptr_field.h>

#include <vector>

namespace NKikimr::NBackup {

// Tries to combine a set of regexps into one regexp that matches a string when at least one of inputs matches.
// When it is impossible, retuns a set of separate regexps for every nonempty input.
// Throws when one of regexps is invalid.
std::vector<TRegExMatch> CombineRegexps(const google::protobuf::RepeatedPtrField<TString>& regexps);

} // namespace NKikimr::NBackup
