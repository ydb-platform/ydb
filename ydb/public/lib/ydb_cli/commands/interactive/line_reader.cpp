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
            if (const auto& parent = TFsPath(HistoryFilePath).Parent(); !parent.Exists()) {
                parent.MkDirs();
            }

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
    TLineReader(const TDriver& driver, const TString& database, const TInteractiveLogger& log)
        : Log(log)
        , YQLCompleter(MakeYQLCompleter(TColorSchema::Monaco(), driver, database, Log.IsVerbose()))
        , YQLHighlighter(MakeYQLHighlighter(TColorSchema::Monaco()))
    {}

    void Setup(const TSessionSettings& settings) final {
        Prompt = settings.Prompt;
        HelpMessage = settings.HelpMessage;

        InitReplxx(settings.EnableYqlCompletion);
        Y_DEBUG_VERIFY(Rx);

        for (const auto& [key, action] : settings.KeyHandlers) {
            Rx->bind_key(replxx::Replxx::KEY::control(key), [&, action](char32_t code) {
                action();
                return Rx->invoke(replxx::Replxx::ACTION::ABORT_LINE, code);
            });
            KeyHandlers.erase(key);
        }
        for (const auto& [key, action] : KeyHandlers) {
            Rx->bind_key(replxx::Replxx::KEY::control(key), [&, action](char32_t) { return replxx::Replxx::ACTION_RESULT::CONTINUE; });
        }
        KeyHandlers = settings.KeyHandlers;

        UpdateHistoryPath(settings.HistoryFilePath);
        if (History) {
            if (const auto fileLockGuard = TFileHandlerLockGuard::Lock(History->GetHandle())) {
                Rx->set_unique_history(true);
                if (!Rx->history_load(History->GetPath())) {
                    Log.Error() << "Loading history failed: " << strerror(errno);
                }
            } else {
                Log.Error() << "Lock of history file failed: " << strerror(errno);
            }
        }
    }

    std::optional<std::variant<TLine, TSwitch>> ReadLine() final {
        Y_DEBUG_VERIFY(Rx, "Can not read lines before Setup call");

        while (true) {
            const char* input = nullptr;
            try {
                input = Rx->input(Prompt.c_str());
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
            return TLine{std::move(line)};
        }

        if (SwitchRequested) {
            SwitchRequested = false;
            return TSwitch{};
        }

        return std::nullopt;
    }

    void Finish() final  {
        Cout << Endl;
        Rx->invoke(replxx::Replxx::ACTION::CLEAR_SELF, 0);
    }

private:
    void InitReplxx(bool enableCompletion) {
        if (Rx) {
            Finish();
            Rx.reset();
        }

        Rx = replxx::Replxx();
        Rx->install_window_change_handler();

        if (enableCompletion) {
            Rx->set_complete_on_empty(true);
            Rx->set_word_break_characters(NSQLComplete::WordBreakCharacters);
            Rx->set_completion_callback([this](const std::string& prefix, int& contextLen) {
                return YQLCompleter->ApplyHeavy(Rx->get_state().text(), prefix, contextLen);
            });

            Rx->set_hint_delay(100);
            Rx->set_hint_callback([this](const std::string& prefix, int& contextLen, TColor&) {
                return YQLCompleter->ApplyLight(Rx->get_state().text(), prefix, contextLen);
            });

            Rx->set_highlighter_callback([this](const auto& text, auto& colors) {
                YQLHighlighter->Apply(text, colors);
            });
        }

        Rx->bind_key(replxx::Replxx::KEY::control('N'), [&](char32_t code) {
            return Rx->invoke(replxx::Replxx::ACTION::HISTORY_NEXT, code);
        });
        Rx->bind_key(replxx::Replxx::KEY::control('P'), [&](char32_t code) {
            return Rx->invoke(replxx::Replxx::ACTION::HISTORY_PREVIOUS, code);
        });
        Rx->bind_key(replxx::Replxx::KEY::control('D'), [](char32_t) {
            return replxx::Replxx::ACTION_RESULT::BAIL;
        });
        Rx->bind_key(replxx::Replxx::KEY::control('J'), [&](char32_t code) {
            return Rx->invoke(replxx::Replxx::ACTION::COMMIT_LINE, code);
        });
        Rx->bind_key(replxx::Replxx::KEY::control('O'), [&](char32_t) {
            Rx->invoke(replxx::Replxx::ACTION::INSERT_CHARACTER, '\n');
            return replxx::Replxx::ACTION_RESULT::CONTINUE;
        });
        Rx->bind_key(replxx::Replxx::KEY::control('K'), [&](char32_t code) {
            ClearScreen();
            Cout << Endl << HelpMessage << Endl;
            return Rx->invoke(replxx::Replxx::ACTION::ABORT_LINE, code);
        });
        Rx->bind_key(replxx::Replxx::KEY::control('T'), [&](char32_t) {
            SwitchRequested = true;
            ClearScreen();
            return replxx::Replxx::ACTION_RESULT::BAIL;
        });

        for (const auto [lhs, rhs] : THashMap<char, char>{
            {'(', ')'},
            {'[', ']'},
            {'{', '}'},
            {'\'', '\''},
            {'"', '"'},
        }) {
            Rx->bind_key(lhs, [&, lhs, rhs](char32_t) {
                Rx->invoke(replxx::Replxx::ACTION::INSERT_CHARACTER, lhs);
                Rx->invoke(replxx::Replxx::ACTION::INSERT_CHARACTER, rhs);
                return Rx->invoke(replxx::Replxx::ACTION::MOVE_CURSOR_LEFT, 0);
            });
        }

        Rx->enable_bracketed_paste();
    }

    void UpdateHistoryPath(const TString& path) {
        try {
            History = THistory(path);
        } catch (const std::exception& e) {
            Log.Error() << "Failed to setup history path '" << path << "': " << e.what();
        }
    }

    void AddToHistory(const TString& line) {
        Rx->history_add(line);

        if (!History) {
            Log.Notice() << "Skip save line '" << line << "' to history, history storage is not set.";
            return;
        }

        if (const auto fileLockGuard = TFileHandlerLockGuard::Lock(History->GetHandle())) {
            if (!Rx->history_save(History->GetPath())) {
                Log.Error() << "Save history failed: " << strerror(errno);
            }
        } else {
            Log.Error() << "Lock of history file failed: " << strerror(errno);
        }
    }

    static void ClearScreen() {
        Cout << "\x1b[J" << Flush;
    }

private:
    const TInteractiveLogger Log;
    const IYQLCompleter::TPtr YQLCompleter;
    const IYQLHighlighter::TPtr YQLHighlighter;
    std::optional<replxx::Replxx> Rx;

    TString Prompt;
    TString HelpMessage;
    std::optional<THistory> History;
    std::unordered_map<char, std::function<void()>> KeyHandlers;
    bool SwitchRequested = false;
};

} // anonymous namespace

ILineReader::TPtr CreateLineReader(const TDriver& driver, const TString& database, const TInteractiveLogger& log) {
    return std::make_shared<TLineReader>(driver, database, log);
}

} // namespace NYdb::NConsoleClient
