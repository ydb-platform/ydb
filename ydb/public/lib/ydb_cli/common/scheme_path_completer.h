#pragma once

#include <library/cpp/getopt/small/completer.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

#include <optional>

namespace NLastGetopt {
struct TFreeArgSpec;
}

namespace NYdb::NConsoleClient {

enum class ESchemePathKind {
    Tables,
    Topics,
    Dir,
    All,
};

struct TSchemeCompletionContext {
    ESchemePathKind Kind;
    TString Prefix;
    TString Suffix;
};

// Check if the current invocation is a scheme path completion request.
// If so, strips completion-specific arguments from argc/argv and returns the context.
std::optional<TSchemeCompletionContext> DetectSchemeCompletion(int& argc, const char** argv);

// List database directory contents and print matching entries to stdout
// in the format expected by the shell completion script.
void RunSchemeCompletion(
    TDriver& driver,
    const TString& database,
    const TSchemeCompletionContext& ctx);

// Set up shell completion for a free argument that expects a path to a table
// or column table in the YDB database. Connects to the database using
// connection settings already specified on the command line.
void SetSchemePathCompletionForTables(NLastGetopt::TFreeArgSpec& spec);

// Same as SetSchemePathCompletionForTables, but lists topics.
void SetSchemePathCompletionForTopics(NLastGetopt::TFreeArgSpec& spec);

// Same as SetSchemePathCompletionForTables, but lists only directories.
void SetSchemePathCompletionForDir(NLastGetopt::TFreeArgSpec& spec);

// Same as SetSchemePathCompletionForTables, but lists all scheme objects
// (tables, topics, views, etc.).
void SetSchemePathCompletionForAll(NLastGetopt::TFreeArgSpec& spec);

} // namespace NYdb::NConsoleClient
