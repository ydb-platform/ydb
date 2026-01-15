#pragma once

#include "command.h"
#include <library/cpp/getopt/small/modchooser.h>

namespace NYdb {
namespace NConsoleClient {
class TYdbCommandAutoCompletionWrapper : public TMainClassArgs {
public:
    TYdbCommandAutoCompletionWrapper(TClientCommand *command, TClientCommand::TConfig config)
        : command(command), config(config) {}

  virtual ~TYdbCommandAutoCompletionWrapper() override = default;

  virtual void RegisterOptions(NLastGetopt::TOpts &opts) override;
  virtual int DoRun(NLastGetopt::TOptsParseResult &&parsedOptions) override;

private:
  TClientCommand *command;
  TClientCommand::TConfig config;
};

class TYdbCommandTreeAutoCompletionWrapper : public TMainClassModes, public TMainClassArgs {
public:
    TYdbCommandTreeAutoCompletionWrapper(TClientCommandTree *commandTree, TClientCommand::TConfig config)
        : commandTree(commandTree), config(config) {}

  virtual ~TYdbCommandTreeAutoCompletionWrapper() override = default;

  virtual void RegisterOptions(NLastGetopt::TOpts &opts) override;
  virtual void RegisterModes(TModChooser &modes) override;
  virtual int DoRun(NLastGetopt::TOptsParseResult &&parsedOptions) override;

private:
  TClientCommandTree *commandTree;
  TClientCommand::TConfig config;
  std::vector<std::unique_ptr<TMainClass>> subCommands;
};

NLastGetopt::TOpt GenerateCompletionOption(TStringBuf command, TClientCommandTree *commandTree, TClientCommand::TConfig& config);

} // namespace NConsoleClient
} // namespace NYdb
