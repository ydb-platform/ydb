#pragma once

#include <library/cpp/getopt/small/last_getopt_opts.h>
#include <library/cpp/getopt/small/modchooser.h>

#include <util/stream/output.h>

namespace NYdb::NConsoleClient {

// Generate machine-readable JSON description of the CLI command tree.
// Used by external tools to build their own shell completion without
// re-implementing the YDB CLI command structure.
//
// Schema for each node (root has the same shape):
//   {
//     "handlers":  { <subcommand_name>: <node>, ... }, // sorted by key
//     "options":   { "--flag" | "-x": <opt_value>, ... }, // sorted by key
//     "free_args": <free_args_value>
//   }
//
// <opt_value> describes what the option accepts and is one of:
//   * null            -- the option is a flag without an argument (e.g. --help)
//   * [v1, v2, ...]   -- the option accepts only one of the listed values
//                        (TOpt::Choices); the list is sorted lexicographically
//   * "file"          -- the option takes a free-form value that is completed as
//                        a local file/path (the CLI's default for value options)
//   * "value"         -- the option takes a free-form value with custom or
//                        server-side completion (e.g. a YDB scheme path) that a
//                        third party cannot reproduce as local files
//
// <free_args_value> describes the positional (free) arguments and is one of:
//   * null            -- no positional arguments. Also reported on command groups
//                        (where the positional selects a subcommand listed in
//                        "handlers") and on commands that never declared any
//   * "file"          -- positional(s) completed as local files/paths
//   * "value"         -- positional(s) with custom/server-side completion
//
// The schema is additive: new <opt_value>/<free_args_value> markers may appear
// over time, and the "free_args" key is absent in output from older binaries.
// Consumers should tolerate unknown string markers and treat a missing
// "free_args" as null; producers and consumers are expected to evolve in tandem.
//
// Hidden subcommands and options are omitted. Handlers and options are emitted in
// stable (sorted) order so the output can be cached and compared between runs.
void GenerateJsonCompletion(const TModChooser& chooser, const NLastGetopt::TOpts& rootOpts, IOutputStream& out);

} // namespace NYdb::NConsoleClient
