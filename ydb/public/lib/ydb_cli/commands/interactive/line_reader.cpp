#include "line_reader.h"

#include <ydb/core/base/validation.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/complete/ydb_schema.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/complete/yql_completer.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/highlight/yql_highlighter.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>

#include <yql/essentials/sql/v1/complete/sql_complete.h>
#include <yql/essentials/sql/v1/complete/text/word.h>

#include <contrib/restricted/patched/replxx/include/replxx.hxx>

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/string/strip.h>
#include <util/system/file.h>

namespace NYdb::NConsoleClient {

namespace {

class TLineReader final : public ILineReader {
    inline static const NColorizer::TColors Colors = NColorizer::AutoColors(Cout);

    class TFileHandlerLockGuard {
    public:
        explicit TFileHandlerLockGuard(TFileHandle* handle)
            : Handle(handle)
        {
            Y_DEBUG_VERIFY(Handle);
        }

        ~TFileHandlerLockGuard() {
            Handle->Flock(LOCK_UN);
        }

        static std::optional<TFileHandlerLockGuard> Lock(TFileHandle& fileHandle) {
            if (fileHandle.Flock(LOCK_EX) != 0) {
                return {};
            }

            return TFileHandlerLockGuard(&fileHandle);
        }

    private:
        TFileHandle* Handle = nullptr;
    };

    class THistory {
    public:
        explicit THistory(const TString& historyFilePath)
            : HistoryFilePath(historyFilePath)
        {
            TFsPath(HistoryFilePath).MkDirs();
            HistoryFileHandle = TFileHandle(HistoryFilePath.c_str(), EOpenModeFlag::OpenAlways | EOpenModeFlag::RdWr | EOpenModeFlag::AW | EOpenModeFlag::ARUser | EOpenModeFlag::ARGroup);
            if (!HistoryFileHandle.IsOpen()) {
                throw yexception() << "file handle open error: " << strerror(errno);
            }
        }

        const TString& GetPath() const {
            return HistoryFilePath;
        }

        TFileHandle& GetHandle() {
            return HistoryFileHandle;
        }

    private:
        TString HistoryFilePath;
        TFileHandle HistoryFileHandle;
    };

public:
    TLineReader(const TString& prompt, const TString& historyFilePath, const TDriver& driver, const TString& database, const TInteractiveLogger& log)
        : Log(log)
        , Prompt(prompt)
        , YQLCompleter(MakeYQLCompleter(TColorSchema::Monaco(), driver, database, Log.IsVerbose()))
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
        Rx.bind_key(replxx::Replxx::KEY::control('K'), [&](char32_t code) {
            Cout << "\x1b[J"; // Clear suggestions

            const auto printBold = [&](const TString& text) {
                return TStringBuilder() << Colors.BoldColor() << text << Colors.OldColor();
            };

            const auto printGreen = [&](const TString& text) {
                return TStringBuilder() << Colors.Green() << text << Colors.OldColor();
            };

            Cout << Endl << Endl << "YDB CLI Interactive Mode – Hotkeys and Special Commands." << Endl
                << Endl << printBold("Hotkeys:") << Endl
                << "  " << printBold("Up and Down arrow keys") << ": navigate through query history." << Endl
                << "  " << printBold("TAB") << ": complete the current word based on YQL syntax." << Endl
                << "  " << printBold("Ctrl+R") << ": search for a query in history containing a specified substring." << Endl
                << "  " << printBold("Ctrl+D") << ": exit interactive mode." << Endl
                << Endl << printBold("Special Commands:") << Endl
                << "  " << printGreen("SET stats = ") << printBold("STATS_MODE") << ": set statistics collection mode, allowed modes: "
                << printBold("none") << ", " << printBold("basic") << ", " << printBold("full") << ", " << printBold("profile") << "." << Endl
                << "  " << printGreen("EXPLAIN") << " [" << printGreen("AST") << "] " << printBold("SQL_QUERY") << ": execute query in explain mode and optionally print AST." << Endl
                << "  {" << printGreen("QUIT") << "|" << printGreen("EXIT") << "}: exit interactive mode." << Endl
                << Endl << "By default, any input is treated as an YQL query and sent directly to the YDB server." << Endl << Endl;

            return Rx.invoke(replxx::Replxx::ACTION::ABORT_LINE, code);
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

        Rx.enable_bracketed_paste();

        UpdateHistoryPath(historyFilePath);
        if (!History) {
            return;
        }

        if (const auto fileLockGuard = TFileHandlerLockGuard::Lock(History->GetHandle())) {
            Rx.set_unique_history(true);
            if (!Rx.history_load(History->GetPath())) {
                Log.Error() << "Loading history failed: " << strerror(errno);
            }
        } else {
            Log.Error() << "Lock of history file failed: " << strerror(errno);
        }
    }

    std::optional<TString> ReadLine() final {
        while (true) {
            const char* input = nullptr;
            try {
                input = Rx.input(Prompt.c_str());
            } catch (const std::exception& e) {
                Log.Error() << "Failed to read line: " << e.what();
                continue;
            }

            if (input == nullptr) {
                if (errno == EAGAIN) {
                    continue;
                }
                break;
            }

            TString line = Strip(input);
            AddToHistory(line);
            return std::move(line);
        }

        return std::nullopt;
    }

    void Finish() final  {
        Cout << Endl;
        Rx.invoke(replxx::Replxx::ACTION::CLEAR_SELF, 0);
    }

private:
    void UpdateHistoryPath(const TString& path) {
        try {
            History = THistory(path);
        } catch (const std::exception& e) {
            Log.Error() << "Failed to setup history path '" << path << "': " << e.what();
        }
    }

    void AddToHistory(const TString& line) {
        Rx.history_add(line);

        if (!History) {
            Log.Notice() << "Skip save line '" << line << "' to history, history storage is not set.";
            return;
        }

        if (const auto fileLockGuard = TFileHandlerLockGuard::Lock(History->GetHandle())) {
            if (!Rx.history_save(History->GetPath())) {
                Log.Error() << "Save history failed: " << strerror(errno);
            }
        } else {
            Log.Error() << "Lock of history file failed: " << strerror(errno);
        }
    }

private:
    const TInteractiveLogger Log;
    const TString Prompt;
    const IYQLCompleter::TPtr YQLCompleter;
    const IYQLHighlighter::TPtr YQLHighlighter;
    replxx::Replxx Rx;
    std::optional<THistory> History;
};

} // anonymous namespace

std::unique_ptr<ILineReader> CreateLineReader(const TString& prompt, const TString& historyFilePath, const TDriver& driver, const TString& database, const TInteractiveLogger& log) {
    return std::make_unique<TLineReader>(prompt, historyFilePath, driver, database, log);
}

} // namespace NYdb::NConsoleClient
