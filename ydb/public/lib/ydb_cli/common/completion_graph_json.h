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
//     "handlers": { <subcommand_name>: <node>, ... },
//     "options":  { "--flag" | "-x": null, ... }
//   }
//
// Hidden subcommands and options are omitted.
void GenerateJsonCompletion(const TModChooser& chooser, const NLastGetopt::TOpts& rootOpts, IOutputStream& out);

} // namespace NYdb::NConsoleClient
