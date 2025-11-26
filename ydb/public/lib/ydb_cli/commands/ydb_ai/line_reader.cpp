#include "line_reader.h"

#include <util/string/strip.h>

namespace NYdb::NConsoleClient::NAi {

TLineReader::TLineReader(TString prompt, TString historyFilePath)
    : Prompt(std::move(prompt))
    , HistoryFilePath(historyFilePath)
    , HistoryFileLock(historyFilePath)
{
    Rx.install_window_change_handler();

    Rx.bind_key(replxx::Replxx::KEY::control('N'), [this](char32_t code) {
        return Rx.invoke(replxx::Replxx::ACTION::HISTORY_NEXT, code);
    });
    Rx.bind_key(replxx::Replxx::KEY::control('P'), [this](char32_t code) {
        return Rx.invoke(replxx::Replxx::ACTION::HISTORY_PREVIOUS, code);
    });
    Rx.bind_key(replxx::Replxx::KEY::control('D'), [](char32_t) {
        return replxx::Replxx::ACTION_RESULT::BAIL;
    });
    Rx.bind_key(replxx::Replxx::KEY::control('J'), [this](char32_t code) {
        return Rx.invoke(replxx::Replxx::ACTION::COMMIT_LINE, code);
    });

    Rx.enable_bracketed_paste();
    Rx.set_unique_history(true);

    if (const auto guard = TryLockHistory(); guard && !Rx.history_load(HistoryFilePath)) {
        Rx.print("Loading history failed: %s\n", strerror(errno));
    }
}

std::optional<TString> TLineReader::ReadLine() {
    do {
        if (const char* input = Rx.input(Prompt.c_str())) {
            auto result = Strip(input);
            AddToHistory(result);
            return std::move(result);
        }
    } while (errno == EAGAIN);

    return std::nullopt;
}

TTryGuard<TFileLock> TLineReader::TryLockHistory() {
    // AI-TODO: KIKIMR-24202 - robust file creation and handling
    TTryGuard guard(HistoryFileLock);

    if (!guard) {
        Rx.print("Lock of history file failed: %s\n", strerror(errno));
    }

    return guard;
}

void TLineReader::AddToHistory(const TString& line) {
    Rx.history_add(line);

    if (const auto guard = TryLockHistory(); guard && !Rx.history_save(HistoryFilePath)) {
        Rx.print("Save history failed: %s\n", strerror(errno));
    }
}

} // namespace NYdb::NConsoleClient::NAi
