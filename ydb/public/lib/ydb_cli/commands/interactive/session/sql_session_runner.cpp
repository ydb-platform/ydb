#include "sql_session_runner.h"
#include "session_runner_common.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/config_ui.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_config.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/ftxui.h>
#include <ydb/public/lib/ydb_cli/common/query_utils.h>
#include <ydb/public/lib/ydb_cli/common/scheme_query_utils.h>
#include <ydb/public/lib/ydb_cli/common/tx_mode_utils.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/query_stats/stats.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <util/folder/path.h>
#include <util/generic/scope.h>
#include <util/string/builder.h>
#include <util/string/strip.h>

namespace NYdb::NConsoleClient {

namespace {

constexpr NYdb::NIssue::TIssueCode KikimrTransactionNotFoundIssueCode = 2015; // NYql::TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND

bool IsStaleInteractiveTransactionError(const TStatus& status) {
    if (NStatusHelpers::StatusContainsIssueWithCode(status, KikimrTransactionNotFoundIssueCode)) {
        return true;
    }
    if (status.GetStatus() == EStatus::NOT_FOUND) {
        for (const auto& issue : status.GetIssues()) {
            if (issue.GetMessage().contains("Transaction not found")) {
                return true;
            }
        }
    }
    return false;
}

bool IsStaleInteractiveTransactionError(TStringBuf text) {
    return text.find("Transaction not found") != TStringBuf::npos;
}

class TSqlSessionRunner final : public TSessionRunnerBase, public TInterruptableCommand {
    using TBase = TSessionRunnerBase;

    class TLexer {
    public:
        struct TToken {
            std::string_view data;
        };

        explicit TLexer(std::string_view input)
            : Input(input)
            , Position(Input.data())
        {}

        static std::vector<TToken> Tokenize(std::string_view input) {
            std::vector<TToken> tokens;
            TLexer lexer(input);

            while (auto token = lexer.GetNextToken()) {
                tokens.push_back(*token);
            }

            return tokens;
        }

    private:
        std::optional<TToken> GetNextToken() {
            while (Position < Input.end() && std::isspace(*Position)) {
                ++Position;
            }

            if (Position == Input.end()) {
                return {};
            }

            const char* tokenStart = Position;
            if (IsSeparatedTokenSymbol(*Position)) {
                ++Position;
            } else {
                while (Position < Input.end() && !std::isspace(*Position) && !IsSeparatedTokenSymbol(*Position)) {
                    ++Position;
                }
            }

            std::string_view TokenData(tokenStart, Position);
            return TToken{TokenData};
        }

        static bool IsSeparatedTokenSymbol(char c) {
            return c == '=' || c == ';';
        }

    private:
        std::string_view Input;
        const char* Position = nullptr;
    };

public:
    explicit TSqlSessionRunner(const TSqlSessionSettings& settings)
        : TBase(CreateSessionSettings(settings))
        , QueryPlanPrinter(EDataFormat::Default)
        , SqlLazyDriver(settings.SqlLazyDriver)
        , SqlTxLazyDriver(settings.SqlTxLazyDriver)
        , EnableAiInteractive(settings.EnableAiInteractive)
        , EnableInteractiveTransactions(settings.EnableInteractiveTransactions)
        , BasePrompt(Settings.Prompt)
    {
        Y_VALIDATE(SqlLazyDriver, "TSqlSessionRunner requires a non-null SqlLazyDriver");
        Y_VALIDATE(SqlTxLazyDriver, "TSqlSessionRunner requires a non-null SqlTxLazyDriver");
        Y_VALIDATE(settings.CompleterLazyDriver, "TSqlSessionRunner requires a non-null CompleterLazyDriver");
    }

    ~TSqlSessionRunner() override {
        if (!IsInteractiveTransactionActive()) {
            return;
        }
        Cerr << Colors.Yellow() << "Warning: open transaction is rolled back on exit." << Colors.OldColor() << Endl;
        RollbackOpenTransaction(/* silent */ true);
    }

    void HandleLine(const TString& line) final {
        Y_DEFER { ResetInterrupted(); };
        // Stop the drivers at the end of the line if no transaction is active.
        // The flag is re-evaluated at scope exit (after BEGIN/COMMIT/ROLLBACK
        // mutate the state) so the tx driver lives across queries inside a tx.
        Y_DEFER {
            if (!IsInteractiveTransactionActive()) {
                SqlLazyDriver->Stop(true);
                SqlTxLazyDriver->Stop(true);
            }
        };

        if (to_lower(line) == "/help") {
            PrintFtxuiMessage(
                CreateHelpMessage(EnableAiInteractive, EnableInteractiveTransactions),
                "YDB CLI Interactive Mode – Hotkeys and Special Commands",
                ftxui::Color::White);
            return;
        }

        if (to_lower(line) == "/config") {
            TConfigUI::TContext ctx{
                .LineReader = LineReader,
            };
            TConfigUI::Run(ctx);
            return;
        }

        const auto& tokens = TLexer::Tokenize(line);
        if (tokens.empty()) {
            return;
        }

        if (to_lower(TString(tokens[0].data)) == "set") {
            ParseSetCommand(tokens);
            return;
        }

        if (HandleTransactionControlCommand(line)) {
            return;
        }

        if (IsInteractiveTransactionActive()) {
            ExecuteInTransaction(line);
            return;
        }

        if (to_lower(TString(tokens[0].data)) == "explain") {
            size_t tokensSize = tokens.size();
            bool printAst = tokensSize >= 2 && to_lower(TString(tokens[1].data)) == "ast";
            size_t skipTokens = 1 + printAst;
            TString explainQuery;

            for (size_t i = skipTokens; i < tokensSize; ++i) {
                explainQuery += tokens[i].data;
                explainQuery += ' ';
            }

            TExplainGenericQuery explainRunner(SqlLazyDriver->Get());
            const auto& result = explainRunner.Explain(explainQuery);
            if (printAst) {
                Cout << Colors.Green() << "\nQuery AST:" << Colors.OldColor() << Endl << Endl << Strip(result.Ast) << Endl;
            } else {
                Cout << Colors.Green() << "\nQuery Plan:" << Colors.OldColor() << Endl << Endl;
                QueryPlanPrinter.Print(result.PlanJson);
            }

            return;
        }

        NQuery::TExecuteQuerySettings settings;
        settings.StatsMode(CollectStatsMode);
        settings.ConcurrentResultSets(false);
        if (!ResourcePool.empty()) {
            settings.ResourcePool(ResourcePool);
        }

        try {
            TExecuteGenericQuery executeRunner(SqlLazyDriver->Get());
            executeRunner.Execute(line, {.Settings = settings, .AddIndent = true});
        } catch (NStatusHelpers::TYdbErrorException& error) {
            Cerr << Colors.Red() << "\nFailed to execute query:" << Colors.OldColor() << Endl << Strip(ToString(error)) << Endl;
        } catch (std::exception& error) {
            Cerr << Colors.Red() << "\nFailed to execute query:" << Colors.OldColor() << Endl << Strip(error.what()) << Endl;
        }
    }

    // Returns true if the line was a TCL command (regardless of success).
    bool HandleTransactionControlCommand(const TString& line) {
        const bool isBegin = IsBeginCommand(line);
        const bool isCommit = !isBegin && IsCommitCommand(line);
        const bool isRollback = !isBegin && !isCommit && IsRollbackCommand(line);
        if (!isBegin && !isCommit && !isRollback) {
            return false;
        }
        if (!EnableInteractiveTransactions) {
            PrintInteractiveTransactionsNotAvailable();
            return true;
        }
        if (isBegin) {
            HandleBegin(line);
        } else if (isCommit) {
            HandleCommit();
        } else {
            HandleRollback();
        }
        return true;
    }

    void HandleBegin(const TString& line) {
        if (IsInteractiveTransactionActive()) {
            Cerr << Colors.Red() << "\nThere is already a transaction in progress." << Colors.OldColor() << Endl;
            return;
        }

        auto txSettings = ParseBeginTransactionIsolation(line);
        if (!txSettings) {
            const auto modeToken = ExtractBeginModeToken(line);
            if (modeToken && IsOneShotOnlyTxMode(*modeToken) && !HasTrailingBeginTokens(line)) {
                Cerr << Colors.Red()
                     << "\nMode \"" << *modeToken
                     << "\" cannot be used with BEGIN — the Query service does"
                        " not support open transactions for this mode."
                     << Colors.OldColor() << Endl;
                Cerr << "Use it as a one-shot transaction mode for a single"
                        " statement, e.g. \"ydb table query --tx-mode "
                     << *modeToken << " ...\"." << Endl;
            } else {
                Cerr << Colors.Red() << "\nUnknown or malformed transaction mode." << Colors.OldColor() << Endl;
                Cerr << "Supported modes: " << GetTxModeNamesForHelp() << "." << Endl;
            }
            return;
        }

        try {
            EnsureQuerySession();
            auto beginResult = Session->BeginTransaction(*txSettings).GetValueSync();
            NStatusHelpers::ThrowOnErrorOrPrintIssues(beginResult);
            Transaction = beginResult.GetTransaction();
            InteractiveTxActive = true;
            UpdateTransactionPrompt();
            UpdateSchemeQueryCompletionMode();
            Cout << "BEGIN" << Endl;
        } catch (NStatusHelpers::TYdbErrorException& error) {
            PrintTcError("Failed to begin transaction", ToString(error));
            ResetTransactionState();
        } catch (std::exception& error) {
            PrintTcError("Failed to begin transaction", error.what());
            ResetTransactionState();
        }
    }

    void HandleCommit() {
        if (!IsInteractiveTransactionActive()) {
            Cerr << Colors.Red() << "\nThere is no active transaction." << Colors.OldColor() << Endl;
            return;
        }

        Y_DEFER { ResetTransactionState(); };

        try {
            auto commitResult = Transaction->Commit().GetValueSync();
            NStatusHelpers::ThrowOnErrorOrPrintIssues(commitResult);
            Cout << "COMMIT" << Endl;
        } catch (NStatusHelpers::TYdbErrorException& error) {
            PrintTcError("Failed to commit transaction", ToString(error));
            if (IsStaleInteractiveTransactionError(error.GetStatus())) {
                PrintBeginAgainHint();
            }
        } catch (std::exception& error) {
            PrintTcError("Failed to commit transaction", error.what());
            if (IsStaleInteractiveTransactionError(TStringBuf(error.what()))) {
                PrintBeginAgainHint();
            }
        }
    }

    void HandleRollback() {
        if (!IsInteractiveTransactionActive()) {
            Cerr << Colors.Red() << "\nThere is no active transaction." << Colors.OldColor() << Endl;
            return;
        }

        Y_DEFER { ResetTransactionState(); };

        try {
            auto rollbackResult = Transaction->Rollback().GetValueSync();
            NStatusHelpers::ThrowOnErrorOrPrintIssues(rollbackResult);
            Cout << "ROLLBACK" << Endl;
        } catch (NStatusHelpers::TYdbErrorException& error) {
            PrintTcError("Failed to rollback transaction", ToString(error));
            if (IsStaleInteractiveTransactionError(error.GetStatus())) {
                PrintBeginAgainHint();
            }
        } catch (std::exception& error) {
            PrintTcError("Failed to rollback transaction", error.what());
            if (IsStaleInteractiveTransactionError(TStringBuf(error.what()))) {
                PrintBeginAgainHint();
            }
        }
    }

    void ExecuteInTransaction(const TString& line) {
        if (LooksLikeSchemeQuery(line)) {
            Cerr << Colors.Yellow()
                 << "\nDDL statements (CREATE, ALTER, DROP, etc.) cannot be executed inside an"
                    " interactive transaction."
                 << Colors.OldColor() << Endl;
            Cerr << "SHOW CREATE TABLE/VIEW is allowed. COMMIT or ROLLBACK the transaction first,"
                    " then run other DDL outside of a transaction."
                 << Endl;
            return;
        }

        NQuery::TExecuteQuerySettings settings;
        settings.StatsMode(CollectStatsMode);
        settings.ConcurrentResultSets(false);
        if (!ResourcePool.empty()) {
            settings.ResourcePool(ResourcePool);
        }

        try {
            TExecuteGenericQuery executeRunner(SqlTxLazyDriver->Get());
            executeRunner.Execute(line, {
                .Settings = settings,
                .AddIndent = true,
                .Session = *Session,
                .TxControl = NQuery::TTxControl::Tx(*Transaction),
            });
        } catch (NStatusHelpers::TYdbErrorException& error) {
            Cerr << Colors.Red() << "\nFailed to execute query:" << Colors.OldColor() << Endl << Strip(ToString(error)) << Endl;
            InvalidateInteractiveTransactionAfterQueryError();
        } catch (std::exception& error) {
            Cerr << Colors.Red() << "\nFailed to execute query:" << Colors.OldColor() << Endl << Strip(error.what()) << Endl;
            InvalidateInteractiveTransactionAfterQueryError();
        }
    }

    static void PrintInteractiveTransactionsNotAvailable() {
        Cerr << Colors.Red()
             << "\nInteractive transactions (BEGIN/COMMIT/ROLLBACK) are available only in the experimental ydb build."
             << Colors.OldColor() << Endl;
    }

private:
    static ftxui::Element CreateHelpMessage(bool enableAiInteractive, bool enableInteractiveTransactions) {
        using namespace ftxui;

        std::vector<ftxui::Element> elements = {
            paragraph("By default, any input is treated as an YQL query and sent directly to the YDB server."),
            text(""),
            CreateEntityName("Hotkeys:"),
        };

        if (enableAiInteractive) {
            elements.emplace_back(
                CreateListItem(hbox({
                    CreateEntityName("Ctrl+T or /switch"),
                    text(": switch to "),
                    text(ToString(TInteractiveConfigurationManager::EMode::AI)) | color(Color::Cyan),
                    text(" interactive mode.")
                }))
            );
        }

        elements.emplace_back(CreateListItem(hbox({
            CreateEntityName("TAB"), text(": complete the current word based on YQL syntax.")
        })));

        elements.emplace_back(CreateListItem(hbox({
            CreateEntityName("Arrows Ctrl+↑ and Ctrl+↓"), text(": navigate through the current word completion list.")
        })));

        PrintCommonHotKeys(elements);

        elements.emplace_back(text(""));
        elements.emplace_back(CreateEntityName("Special Commands:"));

        const auto keyword = [](const std::string& s) { return text(s) | color(Color::Green); };
        elements.emplace_back(CreateListItem(hbox({
            keyword("SET stats = "), CreateEntityName("STATS_MODE"),
            text(": set statistics collection mode, allowed modes: "),
            CreateEntityName("none"), text(", "), CreateEntityName("basic"), text(", "),
            CreateEntityName("full"), text(", "), CreateEntityName("profile"), text(".")
        })));

        elements.emplace_back(CreateListItem(hbox({
            keyword("SET resource_pool = "), CreateEntityName("POOL_NAME"),
            text(": set resource pool for workload manager (empty string to reset).")
        })));

        elements.emplace_back(CreateListItem(hbox({
            keyword("EXPLAIN"), text(" ["), keyword("AST"), text("] "), CreateEntityName("SQL_QUERY"),
            text(": execute query in explain mode and optionally print AST.")
        })));

        if (enableInteractiveTransactions) {
            elements.emplace_back(CreateListItem(hbox({
                keyword("BEGIN"), text(" ["), keyword("TRANSACTION"), text("] ["),
                CreateEntityName("MODE"),
                text("]: start an interactive transaction (Query service session).")
            })));
            elements.emplace_back(CreateListItem(hbox({
                text("  "), CreateEntityName("MODE"), text(": "), CreateEntityName(GetTxModeNamesForHelp()),
                text(" (default: "), CreateEntityName(TString(kTxModeSerializableRw)), text(").")
            })));
            elements.emplace_back(CreateListItem(hbox({
                keyword("COMMIT"), text(" ["), keyword("TRANSACTION"),
                text("]: commit the current transaction.")
            })));
            elements.emplace_back(CreateListItem(hbox({
                keyword("ROLLBACK"), text(" ["), keyword("TRANSACTION"),
                text("]: rollback the current transaction.")
            })));
            elements.emplace_back(CreateListItem(hbox({
                text("Destructive DDL (CREATE, ALTER, DROP, ...) is not supported inside a"
                     " transaction; SHOW CREATE TABLE/VIEW is allowed. A failed query ends the"
                     " transaction on the server — the CLI clears local state automatically.")
            })));
        }

        elements.emplace_back(text(""));
        elements.emplace_back(CreateEntityName("Interactive Commands:"));
        elements.emplace_back(CreateListItem(hbox({
            CreateEntityName("/config"), text(": change interactive mode settings (hints, color theme, colors mode).")
        })));
        PrintCommonInteractiveCommands(elements);

        return vbox(elements);
    }

    static TLineReaderSettings CreateSessionSettings(const TSqlSessionSettings& settings) {
        auto placeholder = TStringBuilder() << "Type YQL query (Enter to execute, Ctrl+Enter for newline";
        if (settings.EnableAiInteractive) {
            placeholder << ", Ctrl+T for AI mode";
        }
        placeholder << ", Ctrl+D to exit)";

        std::vector<TString> tclCommands;
        if (settings.EnableInteractiveTransactions) {
            for (auto& candidate : GetTransactionControlCompletions()) {
                tclCommands.push_back(std::move(candidate));
            }
        }

        return {
            .LazyDriver = settings.CompleterLazyDriver,
            .Database = settings.Database,
            .Prompt = TStringBuilder() << TInteractiveConfigurationManager::ModeToString(TInteractiveConfigurationManager::EMode::YQL) << "> ",
            .HistoryFilePath = TFsPath(HomeDir) / ".ydb_history",
            .AdditionalCommands = {"/help", "/config"},
            .TclCommands = std::move(tclCommands),
            .Placeholder = placeholder,
            .EnableSwitchMode = settings.EnableAiInteractive,
        };
    }

    void ParseSetCommand(const std::vector<TLexer::TToken>& tokens) {
        if (tokens.size() == 1) {
            Cerr << Colors.Red() << "\nMissing variable name for \"SET\" special command." << Colors.OldColor() << Endl;
        } else if (tokens.size() == 2 || tokens[2].data != "=") {
            Cerr << Colors.Red() << "\nMissing \"=\" symbol for \"SET\" special command." << Colors.OldColor() << Endl;
        } else if (tokens.size() == 3) {
            Cerr << Colors.Red() << "\nMissing variable value for \"SET\" special command." << Colors.OldColor() << Endl;
        } else if (to_lower(TString(tokens[1].data)) == "stats") {
            TrySetCollectStatsMode(tokens);
        } else if (to_lower(TString(tokens[1].data)) == "resource_pool") {
            TrySetResourcePool(tokens);
        } else {
            Cerr << Colors.Red() << "\nUnknown variable name \"" << tokens[1].data << "\" for \"SET\" special command." << Colors.OldColor() << Endl;
        }
    }

    void TrySetCollectStatsMode(const std::vector<TLexer::TToken>& tokens) {
        size_t tokensSize = tokens.size();
        Y_VALIDATE(tokensSize >= 4, "Not enough tokens for \"SET stats\" special command.");

        if (tokensSize > 4) {
            Cerr << Colors.Red() << "\nVariable value for \"SET stats\" special command should contain exactly one token." << Colors.OldColor() << Endl;
            return;
        }

        auto statsMode = NQuery::ParseStatsMode(tokens[3].data);
        if (!statsMode) {
            Cerr << Colors.Red() << "\nUnknown stats collection mode: \"" << tokens[3].data << "\"." << Colors.OldColor() << Endl;
            return;
        }

        CollectStatsMode = *statsMode;
    }

    void TrySetResourcePool(const std::vector<TLexer::TToken>& tokens) {
        size_t tokensSize = tokens.size();
        Y_VALIDATE(tokensSize >= 4, "Not enough tokens for \"SET resource_pool\" special command.");

        if (tokensSize > 4) {
            Cerr << Colors.Red() << "\nVariable value for \"SET resource_pool\" special command should contain exactly one token." << Colors.OldColor() << Endl;
            return;
        }

        ResourcePool = std::string(tokens[3].data);
        Cout << "Resource pool set to \"" << ResourcePool << "\"." << Endl;
    }

    void EnsureQuerySession() {
        if (Session) {
            return;
        }
        QueryClient = NQuery::TQueryClient(SqlTxLazyDriver->Get());
        auto sessionResult = QueryClient->GetSession().GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(sessionResult);
        Session = sessionResult.GetSession();
    }

    bool IsInteractiveTransactionActive() const {
        return EnableInteractiveTransactions && InteractiveTxActive;
    }

    void PrintBeginAgainHint() const {
        Cerr << Colors.Yellow()
             << "No open interactive transaction. Issue BEGIN to start a new one."
             << Colors.OldColor() << Endl;
    }

    void PrintTransactionAutoRolledBackNotice() const {
        Cerr << Colors.Yellow()
             << "Transaction was automatically rolled back after the error."
             << Colors.OldColor() << Endl;
        PrintBeginAgainHint();
    }

    void InvalidateInteractiveTransaction() {
        ResetTransactionState();
    }

    void InvalidateInteractiveTransactionAfterQueryError() {
        if (!IsInteractiveTransactionActive()) {
            return;
        }
        InvalidateInteractiveTransaction();
        PrintTransactionAutoRolledBackNotice();
    }

    void UpdateSchemeQueryCompletionMode() {
        if (LineReader && EnableInteractiveTransactions) {
            LineReader->SetExcludeSchemeQueryCompletion(IsInteractiveTransactionActive());
        }
    }

    void PrintTcError(TStringBuf prefix, TStringBuf details) {
        Cerr << Colors.Red() << "\n" << prefix << ":" << Colors.OldColor() << Endl
             << Strip(TString(details)) << Endl;
    }

    void UpdateTransactionPrompt() {
        if (LineReader) {
            LineReader->SetPrompt(TStringBuilder() << BasePrompt << "* ");
        }
    }

    void ResetTransactionPrompt() {
        if (LineReader) {
            LineReader->SetPrompt(BasePrompt);
        }
    }

    void ResetTransactionState() {
        InteractiveTxActive = false;
        Transaction.reset();
        Session.reset();
        QueryClient.reset();
        ResetTransactionPrompt();
        UpdateSchemeQueryCompletionMode();
    }

    // Rollback any transaction left open at shutdown. Errors are swallowed.
    void RollbackOpenTransaction(bool silent) {
        if (!InteractiveTxActive) {
            ResetTransactionState();
            return;
        }
        if (Transaction) {
            try {
                Transaction->Rollback().GetValueSync();
            } catch (...) {
                if (!silent) {
                    throw;
                }
            }
        }
        ResetTransactionState();
    }

private:
    TQueryPlanPrinter QueryPlanPrinter;
    TLazyDriver::TPtr SqlLazyDriver;
    TLazyDriver::TPtr SqlTxLazyDriver;
    NQuery::EStatsMode CollectStatsMode = NQuery::EStatsMode::None;
    std::string ResourcePool;
    bool EnableAiInteractive;
    bool EnableInteractiveTransactions;
    TString BasePrompt;

    std::optional<NQuery::TQueryClient> QueryClient;
    std::optional<NQuery::TSession> Session;
    std::optional<NQuery::TTransaction> Transaction;
    bool InteractiveTxActive = false;
};

} // anonymous namespace

ISessionRunner::TPtr CreateSqlSessionRunner(const TSqlSessionSettings& settings) {
    return std::make_shared<TSqlSessionRunner>(settings);
}

} // namespace NYdb::NConsoleClient
