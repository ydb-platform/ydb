#pragma once

#include <regex>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/utility.h>
#include <util/stream/output.h>
#include <util/string/builder.h>

namespace NKikimr {

/**
 * Test utilities for TLI (Transaction Lock Invalidation) logging and regex pattern matching
 */
namespace NTestTli {

/**
 * Check if a string matches regex patterns with expected counts
 *
 * @param str The string to check
 * @param regexToMatchCount Vector of regex patterns and expected match counts
 */
inline void CheckRegexMatch(
    const TString& str,
    const TVector<std::pair<TString, ui64>>& regexToMatchCount)
{
    for (auto& [regexString, expectedMatchCount]: regexToMatchCount) {
        std::regex expression(regexString.c_str());

        auto matchCount = std::distance(
            std::sregex_iterator(str.begin(), str.end(), expression),
            std::sregex_iterator());

        UNIT_ASSERT_VALUES_EQUAL_C(expectedMatchCount, matchCount,
            TStringBuilder() << "Pattern: " << regexString << "\nLogs:\n" << str);
    }
}

/**
 * Construct a regex pattern for checking logs with a generic message
 *
 * @param logLevel The log level (DEBUG, INFO, etc.)
 * @param component The component name (DataShard, SessionActor, etc.)
 * @param message The message pattern to match
 * @return Constructed regex pattern string
 */
inline TString ConstructRegexToCheckLogs(
    const TString& logLevel,
    const TString& component,
    const TString& message)
{
    TStringBuilder builder;
    // [\\w]+\\.[A-Za-z]+:[0-9]+ match filename and line number
    builder << "TLI " << logLevel
            << ": [\\w]+\\.[A-Za-z]+:[0-9]+: Component: " << component
            << ".*?" << message;
    return builder;
}

} // namespace NTestTli
} // namespace NKikimr
