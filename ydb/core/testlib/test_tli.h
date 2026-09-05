#pragma once

#include <regex>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/utility.h>
#include <util/stream/output.h>
#include <util/string/split.h>
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
    // Split whole log on separate rows. Use rule that each row starts by prefix like "YYYY-MM-DDTHH:MM:SS.XXXXXXZ node 1:"
    auto logRows = SplitString(str, "Z node 1 :");

    for (auto& [regexString, expectedMatchCount]: regexToMatchCount) {
        std::regex expression(regexString.c_str());
        unsigned matchCount = 0;

        for(auto& row: logRows) {
            std::smatch expressionMatch;
            std::regex_search(row.data(), expressionMatch, expression);
            matchCount += expressionMatch.size();
        }

        UNIT_ASSERT_VALUES_EQUAL_C(expectedMatchCount, matchCount,
            TStringBuilder() << "Pattern: " << regexString << " failed\n");
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
            << ": [\\w]+\\.[A-Za-z]+:[0-9]+: .*component=" << component
            << ".*?" << message;
    return builder;
}

} // namespace NTestTli
} // namespace NKikimr
