#pragma once

#include <library/cpp/getopt/small/last_getopt_opts.h>
#include <library/cpp/getopt/small/modchooser.h>

#include <util/stream/output.h>

namespace NYdb::NConsoleClient {

// Generate machine-readable JSON description of the CLI command tree.
// Used by external tools (e.g. internal `ya ydb`) to build their own shell completion
// without re-implementing the YDB CLI command structure.
//
// Schema for each node (root has the same shape):
//   {
//     "handlers": { <subcommand_name>: <node>, ... },   // sorted by key
//     "options":  { "--flag" | "-x": <opt_value>, ... } // sorted by key
//   }
//
// <opt_value> is one of:
//   * null            -- the option is a flag without an argument (e.g. --help)
//                        or it accepts an arbitrary value (URL, path, number, ...);
//                        in both cases no completion choices are available
//   * [v1, v2, ...]   -- the option accepts only one of the listed values
//                        (TOpt::Choices); the list is sorted lexicographically
//
// Hidden subcommands and options are omitted. Handlers and options are emitted in
// stable (sorted) order so the output can be cached and compared between runs.
void GenerateJsonCompletion(const TModChooser& chooser, const NLastGetopt::TOpts& rootOpts, IOutputStream& out);

} // namespace NYdb::NConsoleClient
