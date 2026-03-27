#pragma once

#include "session_runner_interface.h"

#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <util/stream/output.h>

#include <ydb/public/lib/ydb_cli/common/colors.h>

namespace NYdb::NConsoleClient {

class TSessionRunnerBase : public ISessionRunner {
protected:
    inline const static NColorizer::TColors Colors = NConsoleClient::AutoColors(Cout);

public:
    explicit TSessionRunnerBase(const TLineReaderSettings& settings);

    virtual ILineReader::TPtr Setup() override;

protected:
    static void PrintCommonHotKeys(std::vector<ftxui::Element>& elements);

    static void PrintCommonInteractiveCommands(std::vector<ftxui::Element>& elements);

    static ftxui::Element CreateListItem(ftxui::Element element);

    static ftxui::Element CreateEntityName(const TString& name);

protected:
    ILineReader::TPtr LineReader;
    TLineReaderSettings Settings;
};

} // namespace NYdb::NConsoleClient
