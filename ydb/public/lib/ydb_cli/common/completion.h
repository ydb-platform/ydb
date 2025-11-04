#pragma once

#include "command.h"
#include <library/cpp/getopt/small/modchooser.h>

namespace NYdb {
namespace NConsoleClient {
class TYdbCommandAutoCompletionWrapper : public TMainClassArgs {
public:
    TYdbCommandAutoCompletionWrapper(TClientCommand *command)
        : command(command) {}

  virtual void RegisterOptions(NLastGetopt::TOpts &opts) override;
  virtual int DoRun(NLastGetopt::TOptsParseResult &&parsedOptions) override;

private:
  const TClientCommand *command;
};

class TYdbCommandTreeAutoCompletionWrapper : public TMainClassModes, public TMainClassArgs {
public:
    TYdbCommandTreeAutoCompletionWrapper(TClientCommandTree *commandTree)
        : commandTree(commandTree) {}

  virtual void RegisterOptions(NLastGetopt::TOpts &opts) override;
  virtual void RegisterModes(TModChooser &modes) override;
  virtual int DoRun(NLastGetopt::TOptsParseResult &&parsedOptions) override;

private:
  const TClientCommandTree *commandTree;
};

NLastGetopt::TOpt GenerateCompletionOption(const TClientCommandTree *commandTree);

} // namespace NConsoleClient
} // namespace NYdb
