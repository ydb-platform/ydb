#include "line_reader.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/complete/ydb_schema.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/complete/yql_completer.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/highlight/yql_highlighter.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>

#include <yql/essentials/sql/v1/complete/sql_complete.h>
#include <yql/essentials/sql/v1/complete/text/word.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/system/file.h>

#include <contrib/restricted/patched/replxx/include/replxx.hxx>

namespace NYdb::NConsoleClient {

namespace {

class FileHandlerLockGuard {
public:
    FileHandlerLockGuard(TFileHandle* handle)
        : Handle(handle)
    {
    }

    ~FileHandlerLockGuard() {
        Handle->Flock(LOCK_UN);
    }

private:
    TFileHandle* Handle = nullptr;
};

std::optional<FileHandlerLockGuard> LockFile(TFileHandle& fileHandle) {
    if (fileHandle.Flock(LOCK_EX) != 0) {
        return {};
    }

    return FileHandlerLockGuard(&fileHandle);
}

class TLineReader: public ILineReader {
public:
    TLineReader(std::string prompt, std::string historyFilePath, TClientCommand::TConfig& config);

    std::optional<std::string> ReadLine() override;
    void ClearHints() override;

private:
    void AddToHistory(const std::string& line);

    std::string Prompt;
    std::string HistoryFilePath;
    TFileHandle HistoryFileHandle;
    IYQLCompleter::TPtr YQLCompleter;
    IYQLHighlighter::TPtr YQLHighlighter;
    replxx::Replxx Rx;
};

TLineReader::TLineReader(std::string prompt, std::string historyFilePath, TClientCommand::TConfig& config)
    : Prompt(std::move(prompt))
    , HistoryFilePath(std::move(historyFilePath))
    , HistoryFileHandle(HistoryFilePath.c_str(), EOpenModeFlag::OpenAlways | EOpenModeFlag::RdWr | EOpenModeFlag::AW | EOpenModeFlag::ARUser | EOpenModeFlag::ARGroup)
    , YQLCompleter(MakeYQLCompleter(TColorSchema::Monaco(), TYdbCommand::CreateDriver(config), config.Database, config.IsVerbose()))
    , YQLHighlighter(MakeYQLHighlighter(TColorSchema::Monaco()))
{
    Rx.install_window_change_handler();

    Rx.set_complete_on_empty(true);
    Rx.set_word_break_characters(NSQLComplete::WordBreakCharacters);
    Rx.set_completion_callback([this](const std::string& prefix, int& contextLen) {
        return YQLCompleter->ApplyHeavy(Rx.get_state().text(), prefix, contextLen);
    });

    Rx.set_hint_delay(100);
    Rx.set_hint_callback([this](const std::string& prefix, int& contextLen, TColor&) {
        return YQLCompleter->ApplyLight(Rx.get_state().text(), prefix, contextLen);
    });

    Rx.set_highlighter_callback([this](const auto& text, auto& colors) {
        YQLHighlighter->Apply(text, colors);
    });

    Rx.bind_key(replxx::Replxx::KEY::control('N'), [&](char32_t code) {
        return Rx.invoke(replxx::Replxx::ACTION::HISTORY_NEXT, code);
    });
    Rx.bind_key(replxx::Replxx::KEY::control('P'), [&](char32_t code) {
        return Rx.invoke(replxx::Replxx::ACTION::HISTORY_PREVIOUS, code);
    });
    Rx.bind_key(replxx::Replxx::KEY::control('D'), [](char32_t) {
        return replxx::Replxx::ACTION_RESULT::BAIL;
    });
    Rx.bind_key(replxx::Replxx::KEY::control('J'), [&](char32_t code) {
        return Rx.invoke(replxx::Replxx::ACTION::COMMIT_LINE, code);
    });

    for (const auto [lhs, rhs] : THashMap<char, char>{
        {'(', ')'},
        {'[', ']'},
        {'{', '}'},
        {'\'', '\''},
        {'"', '"'},
    }) {
        Rx.bind_key(lhs, [&, lhs, rhs](char32_t) {
            Rx.invoke(replxx::Replxx::ACTION::INSERT_CHARACTER, lhs);
            Rx.invoke(replxx::Replxx::ACTION::INSERT_CHARACTER, rhs);
            return Rx.invoke(replxx::Replxx::ACTION::MOVE_CURSOR_LEFT, 0);
        });
    }

    auto fileLockGuard = LockFile(HistoryFileHandle);
    if (!fileLockGuard) {
        Rx.print("Lock of history file failed: %s\n", strerror(errno));
        return;
    }

    Rx.enable_bracketed_paste();
    Rx.set_unique_history(true);
    if (!Rx.history_load(HistoryFilePath)) {
        Rx.print("Loading history failed: %s\n", strerror(errno));
    }
}

void TLineReader::ClearHints() {
    std::cout << std::endl;
    Rx.invoke(replxx::Replxx::ACTION::CLEAR_SELF, 0);
}

std::optional<std::string> TLineReader::ReadLine() {
    while (true) {
        const auto* status = Rx.input(Prompt.c_str());

        if (status == nullptr) {
            if (errno == EAGAIN) {
                continue;
            }

            return {};
        }

        std::string line = status;
        while (!line.empty() && std::isspace(line.back())) {
            line.pop_back();
        }

        AddToHistory(line);

        return line;
    }
}

void TLineReader::AddToHistory(const std::string& line) {
    Rx.history_add(line);

    auto fileLockGuard = LockFile(HistoryFileHandle);
    if (!fileLockGuard) {
        Rx.print("Lock of history file failed: %s\n", strerror(errno));
        return;
    }

    if (!Rx.history_save(HistoryFilePath)) {
        Rx.print("Save history failed: %s\n", strerror(errno));
    }
}

} // namespace

std::unique_ptr<ILineReader> CreateLineReader(
    std::string prompt, std::string historyFilePath, TClientCommand::TConfig& config) {
    return std::make_unique<TLineReader>(std::move(prompt), std::move(historyFilePath), config);
}

} // namespace NYdb::NConsoleClient
