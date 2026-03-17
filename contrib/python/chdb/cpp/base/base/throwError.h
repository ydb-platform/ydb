#pragma once

/// Throw DB_CHDB::Exception-like exception before its definition.
/// DB_CHDB::Exception derived from CHDBPoco::Exception derived from std::exception.
/// DB_CHDB::Exception generally caught as CHDBPoco::Exception. std::exception generally has other catch blocks and could lead to other outcomes.
/// DB_CHDB::Exception is not defined yet. It'd better to throw CHDBPoco::Exception but we do not want to include any big header here, even <string>.
/// So we throw some std::exception instead in the hope its catch block is the same as DB_CHDB::Exception one.
[[noreturn]] void throwError(const char * err);
