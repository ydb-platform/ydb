#include "line_reader.h"
#include "yql_highlight.h"
#include "yql_suggest.h"

#include <util/generic/string.h>
#include <util/system/file.h>

#include <contrib/restricted/patched/replxx/include/replxx.hxx>

namespace NYdb {
namespace NConsoleClient {

namespace
{

class FileHandlerLockGuard
{
public:
    FileHandlerLockGuard(TFileHandle * handle)
        : Handle(handle)
    {}

    ~FileHandlerLockGuard() {
        Handle->Flock(LOCK_UN);
    }
private:
    TFileHandle * Handle = nullptr;
};

std::optional<FileHandlerLockGuard> LockFile(TFileHandle & fileHandle) {
    if (fileHandle.Flock(LOCK_EX) != 0) {
        return {};
    }

    return FileHandlerLockGuard(&fileHandle);
}

class TLineReader : public ILineReader
{
public:
    TLineReader(std::string prompt, std::string historyFilePath, Suggest suggestions = {});

    std::optional<std::string> ReadLine() override;

private:
    void AddToHistory(const std::string & line);

    static constexpr std::string_view WordBreakCharacters = " \t\v\f\a\b\r\n`~!@#$%^&*-=+[](){}\\|;:'\".,<>/?";

    std::string Prompt;
    std::string HistoryFilePath;
    TFileHandle HistoryFileHandle;
    Suggest Suggestions;
    replxx::Replxx Rx;
};

TLineReader::TLineReader(std::string prompt, std::string historyFilePath, Suggest suggestions)
    : Prompt(std::move(prompt))
    , HistoryFilePath(std::move(historyFilePath))
    , HistoryFileHandle(HistoryFilePath.c_str(), EOpenModeFlag::OpenAlways | EOpenModeFlag::RdWr | EOpenModeFlag::AW | EOpenModeFlag::ARUser | EOpenModeFlag::ARGroup)
    , Suggestions(std::move(suggestions))
{
    Rx.install_window_change_handler();

    auto completion_callback = [](const std::string & prefix, size_t) {
        return YQLSuggestionEngine().Suggest(prefix);
    };

    auto highlighter_callback = [](const auto& text, auto& colors) {
        return YQLHighlight(YQLHighlight::ColorSchema::Monaco()).Apply(text, colors);
    };

    Rx.set_completion_callback(completion_callback);
    Rx.set_highlighter_callback(highlighter_callback);
    Rx.enable_bracketed_paste();
    Rx.set_unique_history(true);
    Rx.set_complete_on_empty(true);
    Rx.set_word_break_characters(WordBreakCharacters.data());
    Rx.bind_key(replxx::Replxx::KEY::control('N'), [&](char32_t code) { return Rx.invoke(replxx::Replxx::ACTION::HISTORY_NEXT, code); });
    Rx.bind_key(replxx::Replxx::KEY::control('P'), [&](char32_t code) { return Rx.invoke(replxx::Replxx::ACTION::HISTORY_PREVIOUS, code); });
    Rx.bind_key(replxx::Replxx::KEY::control('D'), [](char32_t) { return replxx::Replxx::ACTION_RESULT::BAIL; });
    auto commit_action = [&](char32_t code) {
        return Rx.invoke(replxx::Replxx::ACTION::COMMIT_LINE, code);
    };
    Rx.bind_key(replxx::Replxx::KEY::control('J'), commit_action);

    auto fileLockGuard = LockFile(HistoryFileHandle);
    if (!fileLockGuard) {
        Rx.print("Lock of history file failed: %s\n", strerror(errno));
        return;
    }

    if (!Rx.history_load(HistoryFilePath)) {
        Rx.print("Loading history failed: %s\n", strerror(errno));
    }
}

std::optional<std::string> TLineReader::ReadLine() {
    while (true) {
        const auto * status = Rx.input(Prompt.c_str());

        if (status == nullptr) {
            if (errno == EAGAIN)
                continue;

            return {};
        }

        std::string line = status;
        while (!line.empty() && std::isspace(line.back()))
            line.pop_back();

        AddToHistory(line);

        return line;
    }
}

void TLineReader::AddToHistory(const std::string & line) {
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

}

std::unique_ptr<ILineReader> CreateLineReader(std::string prompt, std::string historyFilePath, Suggest suggestions) {
    return std::make_unique<TLineReader>(std::move(prompt), std::move(historyFilePath), std::move(suggestions));
}

}
}
