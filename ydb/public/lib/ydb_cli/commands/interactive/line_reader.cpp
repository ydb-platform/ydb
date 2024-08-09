#include "line_reader.h"
#include "yql_highlight.h"

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

    auto completion_callback = [this](const std::string & prefix, size_t) {
        std::string lastToken;
        for (i64 i = static_cast<i64>(prefix.size()) - 1; i >= 0; --i) {
            if (std::isspace(prefix[i])) {
                break;
            }

            lastToken.push_back(prefix[i]);
        }

        std::reverse(lastToken.begin(), lastToken.end());

        replxx::Replxx::completions_t completions;
        if (lastToken.empty()) {
            return completions;
        }

        for (auto & Word : Suggestions.Words) {
            if (Word.size() < lastToken.size()) {
                continue;
            }

            if (Word.compare(0, lastToken.size(), lastToken) != 0) {
                continue;
            }

            completions.push_back(Word);
        }

        return completions;
    };

    auto highlighter_callback = [](const auto& text, auto& colors) {
        return YQLHighlight(YQLHighlight::ColorSchema::Default()).Apply(text, colors);
    };

    Rx.set_completion_callback(completion_callback);
    Rx.set_highlighter_callback(highlighter_callback);
    Rx.enable_bracketed_paste();
    Rx.set_unique_history(true);
    Rx.set_complete_on_empty(false);
    Rx.set_word_break_characters(WordBreakCharacters.data());
    Rx.bind_key(replxx::Replxx::KEY::control('N'), [&](char32_t code) { return Rx.invoke(replxx::Replxx::ACTION::HISTORY_NEXT, code); });
    Rx.bind_key(replxx::Replxx::KEY::control('P'), [&](char32_t code) { return Rx.invoke(replxx::Replxx::ACTION::HISTORY_PREVIOUS, code); });
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
