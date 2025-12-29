#include "line_reader.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_log_defs.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/complete/ydb_schema.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/complete/yql_completer.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/highlight/yql_highlighter.h>

#include <yql/essentials/sql/v1/complete/sql_complete.h>
#include <yql/essentials/sql/v1/complete/text/word.h>

#include <contrib/restricted/patched/replxx/include/replxx.hxx>

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/string/strip.h>
#include <util/system/env.h>
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
            Y_VALIDATE(Handle, "File handle is not initialized");
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
    TLineReader(const TLineReaderSettings& settings, const TInteractiveLogger& log)
        : Log(log)
        , YQLHighlighter(MakeYQLHighlighter(GetColorSchema()))
        , ContinueAfterCancel(settings.ContinueAfterCancel)
        , EnableSwitchMode(settings.EnableSwitchMode)
        , Prompt(settings.Prompt)
        , Placeholder(settings.Placeholder)
    {
        std::vector<TString> completionCommands(settings.AdditionalCommands.begin(), settings.AdditionalCommands.end());
        if (EnableSwitchMode) {
            completionCommands.push_back("/switch");
        }

        std::optional<TYQLCompleterConfig> yqlCompleterConfig;
        if (settings.EnableYqlCompletion) {
            yqlCompleterConfig = TYQLCompleterConfig{.Color = GetColorSchema(), .Driver = settings.Driver, .Database = settings.Database, .IsVerbose = Log.IsVerbose()};
        }
        YQLCompleter = MakeYQLCompositeCompleter(completionCommands, yqlCompleterConfig);

        InitReplxx(settings.EnableYqlCompletion);
        Y_VALIDATE(Rx, "Replxx is not initialized");

        if (settings.HistoryFilePath) {
            UpdateHistoryPath(settings.HistoryFilePath);
            if (History) {
                if (const auto fileLockGuard = TFileHandlerLockGuard::Lock(History->GetHandle())) {
                    if (!Rx->history_load(History->GetPath())) {
                        YDB_CLI_LOG(Error, "Loading history failed: " << strerror(errno));
                    }
                } else {
                    YDB_CLI_LOG(Error, "Lock of history file failed: " << strerror(errno));
                }
            }
        }
    }

    std::optional<std::variant<TLine, TSwitch>> ReadLine(const TString& defaultValue) final {
        Y_VALIDATE(Rx, "Can not read lines before Setup call");

        if (defaultValue) {
            // TODO use set_preload_buffer_without_changes
            Rx->set_preload_buffer(defaultValue);
        }

        while (true) {
            const char* input = nullptr;
            try {
                input = Rx->input(Prompt.c_str());
            } catch (const std::exception& e) {
                YDB_CLI_LOG(Error, "Failed to read line: " << e.what());
                continue;
            }

            if (input == nullptr) {
                if (errno == EAGAIN && ContinueAfterCancel) {
                    continue;
                }
                break;
            }

            TString line = Strip(input);
            if (EnableSwitchMode && to_lower(line) == "/switch") {
                SwitchRequested = true;
                break;
            }

            AddToHistory(line);
            return TLine{std::move(line)};
        }

        if (SwitchRequested) {
            SwitchRequested = false;
            return TSwitch{};
        }

        return std::nullopt;
    }

    void Finish(bool clear) final {
        if (clear) {
            Rx->invoke(replxx::Replxx::ACTION::CLEAR_SELF, 0);
        } else {
            Cout << Flush;
        }
    }

private:
    void InitReplxx(bool enableYqlCompletion) {
        Rx = replxx::Replxx();
        Rx->install_window_change_handler();

        Rx->set_completion_callback([this](const std::string& prefix, int& contextLen) {
            return YQLCompleter->ApplyHeavy(Rx->get_state().text(), prefix, contextLen);
        });

        Rx->set_hint_delay(100);
        Rx->set_hint_callback([this](const std::string& prefix, int& contextLen, replxx::Replxx::Color& color) {
            const char* text = Rx->get_state().text();
            if ((text == nullptr || text[0] == '\0') && !Placeholder.empty()) {
                color = replxx::Replxx::Color::GRAY;
                return std::vector<std::string>{std::string(Placeholder)};
            }
            return YQLCompleter->ApplyLight(text, prefix, contextLen);
        });

        if (enableYqlCompletion) {
            Rx->set_complete_on_empty(true);
            Rx->set_word_break_characters(NSQLComplete::WordBreakCharacters);
            Rx->set_highlighter_callback([this](const auto& text, auto& colors) {
                YQLHighlighter->Apply(text, colors);
            });
        } else {
            Rx->set_complete_on_empty(false);
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

        Rx->bind_key(replxx::Replxx::KEY::control('J'), [&](char32_t) {
            Rx->invoke(replxx::Replxx::ACTION::INSERT_CHARACTER, '\n');
            return replxx::Replxx::ACTION_RESULT::CONTINUE;
        });

        if (EnableSwitchMode) {
            Rx->bind_key(replxx::Replxx::KEY::control('T'), [&](char32_t) {
                SwitchRequested = true;
                Rx->invoke(replxx::Replxx::ACTION::KILL_TO_BEGINING_OF_LINE, '\n');
                ClearScreen();
                return replxx::Replxx::ACTION_RESULT::BAIL;
            });
        } else {
             Rx->bind_key(replxx::Replxx::KEY::control('T'), [&](char32_t) {
                 return replxx::Replxx::ACTION_RESULT::CONTINUE;
             });
        }

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
        Rx->set_unique_history(true);

        if (TryGetEnv("NO_COLOR").Defined()) {
            Rx->set_no_color(true);
        }
    }

    void UpdateHistoryPath(const std::optional<TString>& path) {
        if (!path) {
            History.reset();
            return;
        }

        try {
            History = THistory(*path);
        } catch (const std::exception& e) {
            YDB_CLI_LOG(Error, "Failed to setup history path '" << path << "': " << e.what());
        }
    }

    void AddToHistory(const TString& line) {
        Rx->history_add(line);

        if (!History) {
            YDB_CLI_LOG(Notice, "Skip save line '" << line << "' to history, history storage is not set.");
            return;
        }

        if (const auto fileLockGuard = TFileHandlerLockGuard::Lock(History->GetHandle())) {
            if (!Rx->history_save(History->GetPath())) {
                YDB_CLI_LOG(Error, "Save history failed: " << strerror(errno));
            }
        } else {
            YDB_CLI_LOG(Error, "Lock of history file failed: " << strerror(errno));
        }
    }

    static void ClearScreen() {
        Cout << "\x1b[J" << Flush;
    }

private:
    const TInteractiveLogger Log;
    IYQLCompleter::TPtr YQLCompleter;
    const IYQLHighlighter::TPtr YQLHighlighter;
    const bool ContinueAfterCancel = true;
    const bool EnableSwitchMode = true;
    const TString Prompt;
    const TString Placeholder;
    std::optional<replxx::Replxx> Rx;
    std::optional<THistory> History;
    bool SwitchRequested = false;
};

} // anonymous namespace

ILineReader::TPtr CreateLineReader(const TLineReaderSettings& settings, const TInteractiveLogger& log) {
    return std::make_shared<TLineReader>(settings, log);
}

} // namespace NYdb::NConsoleClient
