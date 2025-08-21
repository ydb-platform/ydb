#pragma once

#include <library/cpp/getopt/last_getopt.h>

namespace YAML {
class Node;
}

namespace NYdb::NConsoleClient {

class TClientCommandOption;
class TAuthMethodOption;
class TClientCommandOptions;
class TOptionsParseResult;
class IProfile;

// Value source types in priority order
enum class EOptionValueSource {
    Explicit,                // Command line
    ExplicitProfile,         // Profile that was set via command line
    EnvironmentVariable,     // Env variable
    ActiveProfile,           // Active profile
    DefaultValue,            // Default value for option
};

// Options for YDB CLI that support additional
// parsing features:
// - parsing from environment variables;
// - parsing from profile (for root options);
// - features for control that only one of alternative options is set;
// - features for control that alternative options or options that must be set together
//     are set from one source (command line, env or profile).
class TClientCommandOptions {
public:
    friend class TClientCommandOption;
    friend class TOptionsParseResult;

public:
    TClientCommandOptions();
    TClientCommandOptions(NLastGetopt::TOpts opts);

    // Current command title
    void SetTitle(const TString& title);
    void SetCustomUsage(const TString& usage);
    void SetCmdLineDescr(const TString& descr);
    void SetFreeArgsNum(size_t count);
    void SetFreeArgsNum(size_t min, size_t max);
    void SetFreeArgsMin(size_t min);
    void SetFreeArgsMax(size_t max);
    void MutuallyExclusive(TStringBuf opt1, TStringBuf opt2);
    void MutuallyExclusiveOpt(TClientCommandOption& opt1, TClientCommandOption& opt2);

    TClientCommandOption& AddLongOption(const TString& name, const TString& help = "");
    TClientCommandOption& AddLongOption(char c, const TString& name, const TString& help = "");
    TClientCommandOption& AddCharOption(char c, const TString& help = "");
    TAuthMethodOption& AddAuthMethodOption(const TString& name, const TString& help);
    void AddAnonymousAuthMethodOption();

    const NLastGetopt::TOpts& GetOpts() const {
        return Opts;
    }

    NLastGetopt::TOpts& GetOpts() {
        return Opts;
    }

    void SetHelpCommandVerbosiltyLevel(size_t level) {
        HelpCommandVerbosiltyLevel = level;
    }

    // Priority of parsing auth methods from env, from first to last.
    // If first method is inited from env, others will not be tried.
    template <class TAuthMethodOptionRef, class... T>
    void SetAuthMethodsEnvPriority(TAuthMethodOptionRef& opt, T... args) {
        SetAuthMethodsEnvPriority(opt);
        SetAuthMethodsEnvPriority(args...);
    }

    void SetAuthMethodsEnvPriority(TAuthMethodOption& opt) {
        EnvAuthPriority.emplace_back(&opt);
    }

    void SetAuthMethodsEnvPriority(TAuthMethodOption* opt) {
        if (opt) {
            EnvAuthPriority.emplace_back(opt);
        }
    }

private:
    TClientCommandOption& AddClientOption(NLastGetopt::TOpt& opt);
    TAuthMethodOption& AddAuthMethodClientOption(NLastGetopt::TOpt& opt);

private:
    NLastGetopt::TOpts Opts;
    std::vector<TIntrusivePtr<TClientCommandOption>> ClientOpts;
    std::vector<TIntrusivePtr<TAuthMethodOption>> EnvAuthPriority;
    size_t HelpCommandVerbosiltyLevel = 1;
};

// YDB client command option
class TClientCommandOption : public TSimpleRefCount<TClientCommandOption> {
    friend class TOptionsParseResult;

public:
    using THandler = std::function<void(const TString&)>;
    using TValidator = std::function<std::vector<TString>(const TString&)>; // Returns list of messages with problems. Empty vector if OK

public:
    TClientCommandOption(NLastGetopt::TOpt& opt, TClientCommandOptions* clientOptions);

    virtual ~TClientCommandOption() = default;

    // Additional help notes after main help
    TClientCommandOption& AdditionalHelpNotes(const TString& helpNotes);

    TClientCommandOption& NoArgument();

    TClientCommandOption& RequiredArgument(const TString& title = {});

    TClientCommandOption& Optional();

    TClientCommandOption& Required();

    TClientCommandOption& HasArg(NLastGetopt::EHasArg hasArg);

    TClientCommandOption& Hidden();

    TClientCommandOption& OptionalArgument(const TString& title = {});

    TClientCommandOption& AddLongName(const TString& name);

    TClientCommandOption& IfPresentDisableCompletion();

    const NLastGetopt::EHasArg& GetHasArg() const;

    // Store result. If option is file name, stores the contents of file in result.
    TClientCommandOption& StoreResult(TString* result);

    template <class T>
    TClientCommandOption& StoreResult(T* result) {
        ValueSetter = [result](const TString& v) {
            *result = FromString<T>(v);
        };
        return SetHandler();
    }

    template <class T>
    TClientCommandOption& StoreResult(TMaybe<T>* result) {
        ValueSetter = [result](const TString& v) {
            *result = FromString<T>(v);
        };
        return SetHandler();
    }

    template <class T>
    TClientCommandOption& StoreResult(std::optional<T>* result) {
        ValueSetter = [result](const TString& v) {
            *result = FromString<T>(v);
        };
        return SetHandler();
    }

    TClientCommandOption& StoreTrue(bool* target);

    template <class T, class V>
    TClientCommandOption& StoreValue(T* target, V value) {
        ValueSetter = [target, val = std::move(value)](const TString&) {
            *target = val;
        };
        return SetHandler();
    }

    template <class TContainer>
    TClientCommandOption& AppendTo(TContainer* cont) {
        ValueSetter = [cont](const TString& v) {
            cont->push_back(FromString<typename TContainer::value_type>(v));
        };
        return SetHandler();
    }

    template <class TContainer>
    TClientCommandOption& InsertTo(TContainer* cont) {
        ValueSetter = [cont](const TString& v) {
            cont->insert(FromString<typename TContainer::value_type>(v));
        };
        return SetHandler();
    }

    template <typename T, class TFunc>
    TClientCommandOption& StoreMappedResult(T* target, TFunc&& func) {
        ValueSetter = [target, mapper = std::forward<TFunc>(func)](const TString& v) {
            *target = mapper(v);
        };
        return SetHandler();
    }

    // For options taken from file content
    // Stores source file path
    TClientCommandOption& StoreFilePath(TString* filePath);

    TClientCommandOption& Handler(THandler);
    TClientCommandOption& Validator(TValidator);
    TClientCommandOption& Handler(void (*f)(const NLastGetopt::TOptsParser*));

    // YDB CLI specific options

    // This option means that its value must be read from file that is specified through command line.
    // Sets file content as option result (see StoreResult).
    TClientCommandOption& FileName(const TString& humanReadableFileName = {}, bool fileName = true);

    // Alternative env variable for option.
    // If isFileName is set, interprets current env variable as file name.
    TClientCommandOption& Env(const TString& envName, bool isFileName, const TString& humanReadableFileName = {});

    // Parse option from profile
    TClientCommandOption& ProfileParam(const TString& profileParamName, bool isFileName = false);

    TClientCommandOption& SetSupportsProfile(bool supports = true);

    // Log connection params at high verbosity level
    TClientCommandOption& LogToConnectionParams(const TString& paramName);

    // Doc link for help message
    TClientCommandOption& DocLink(const TString& link);

    TClientCommandOption& DefaultValue(const TString& defaultValue);

    TClientCommandOption& ManualDefaultValueDescription(const TString& description);

    template <class TValue>
    TClientCommandOption& DefaultValue(const TValue& defaultValue) {
        return DefaultValue(ToString(defaultValue));
    }

    NLastGetopt::TOpt& GetOpt() {
        return *Opt;
    }

    const NLastGetopt::TOpt& GetOpt() const {
        return *Opt;
    }

protected:
    TClientCommandOption& SetHandler();
    bool HandlerImpl(TString value, bool isFileName, const TString& humanReadableFileName, EOptionValueSource valueSource);
    void RebuildHelpMessage();
    bool NeedPrintDefinitionsPriority() const;

    // Try parse from profile.
    // if parsedValue is not null, set it with parsed value, if actual
    virtual bool TryParseFromProfile(const std::shared_ptr<IProfile>& profile, TString* parsedValue, bool* isFileName, std::vector<TString>* errors, bool parseOnly) const;

protected:
    struct TEnvInfo {
        TString EnvName;
        bool IsFileName = false;
        TString HumanReadableFileName;
    };

protected:
    NLastGetopt::TOpt* Opt = nullptr;
    TClientCommandOptions* ClientOptions = nullptr;
    TString Help;
    TString HelpNotes;
    TValidator ValidatorHandler;
    THandler ValueHandler;
    THandler ValueSetter;
    bool IsFileName = false;
    TString HumanReadableFileName;
    TString* FilePath = nullptr;
    std::vector<TEnvInfo> EnvInfo;
    TString DefaultOptionValue;
    TString ManualDefaultOptionValueDescription;
    TString ProfileParamName;
    bool ProfileParamIsFileName = false;
    bool CanParseFromProfile = false;
    TString ConnectionParamName;
    TString Documentation;
    bool HandlerIsSet = false;
};

class TAuthMethodOption : public TClientCommandOption {
public:
    using TProfileParser = std::function<bool(const YAML::Node& authData, TString* value, bool* isFileName, std::vector<TString>* errors, bool parseOnly)>;
public:
    TAuthMethodOption(NLastGetopt::TOpt& opt, TClientCommandOptions* clientOptions);

    // Auth method name that is used to parse from profile
    TAuthMethodOption& AuthMethod(const TString& methodName);

    // Add profile parser
    // Default auth method name is taken from AuthMethod("...")
    TAuthMethodOption& AuthProfileParser(TProfileParser, const TString& authMethod = {});

    // Add profile parser from simple data profile param
    // Treat data as string with option value
    // Default auth method name is taken from AuthMethod("...")
    TAuthMethodOption& SimpleProfileDataParam(const TString& authMethod = {}, bool isFileName = false);

    const TString& GetAuthMethod() const {
        return AuthMethodName;
    }

protected:
    bool TryParseFromProfile(const std::shared_ptr<IProfile>& profile, TString* parsedValue, bool* isFileName, std::vector<TString>* errors, bool parseOnly) const override;

protected:
    TString AuthMethodName;
    THashMap<TString, TProfileParser> ProfileParsers; // method -> parser
};

class TAnonymousAuthMethodOption : private NLastGetopt::TOpt, public TAuthMethodOption {
public:
    TAnonymousAuthMethodOption(TClientCommandOptions* clientOptions);
};

class TOptionParseResult {
    friend class TOptionsParseResult;

public:
    TOptionParseResult(const TOptionParseResult&) = default;
    TOptionParseResult(TOptionParseResult&&) = default;
    TOptionParseResult(TIntrusivePtr<TClientCommandOption> opt, const NLastGetopt::TOptParseResult* result)
        : Opt(std::move(opt))
        , ParseFromCommandLineResult(result)
        , ValueSource(EOptionValueSource::Explicit)
    {
        if (ParseFromCommandLineResult && !ParseFromCommandLineResult->Values().empty()) {
            OptValues.assign(ParseFromCommandLineResult->Values().begin(), ParseFromCommandLineResult->Values().end());
        }
    }

    TOptionParseResult(TIntrusivePtr<TClientCommandOption> opt, const TString& value, EOptionValueSource valueSource)
        : Opt(std::move(opt))
        , ValueSource(valueSource)
    {
        OptValues.emplace_back(std::move(value));
    }

    EOptionValueSource GetValueSource() const {
        return ValueSource;
    }

    const std::vector<TString>& Values() const {
        return OptValues;
    }

    size_t Count() const {
        return OptValues.size();
    }

    const TClientCommandOption* GetOpt() const {
        return Opt.Get();
    }

private:
    TIntrusivePtr<TClientCommandOption> Opt;
    const NLastGetopt::TOptParseResult* ParseFromCommandLineResult = nullptr;
    EOptionValueSource ValueSource;
    std::vector<TString> OptValues;
};

class TCommandOptsParseResult: public NLastGetopt::TOptsParseResult {
public:
    TCommandOptsParseResult(const NLastGetopt::TOpts* options, int argc, const char* argv[]) {
        Init(options, argc, argv);
    }

    virtual ~TCommandOptsParseResult() = default;

    void HandleError() const override {
        // Throwing exception to override default behaviour (exit with error code) to be able to handle error in a custom way
        throw;
    }
};

class TOptionsParseResult {
    friend class TClientCommandOptions;

public:
    using TConnectionParamsLogger = std::function<void(const TString& /*paramName*/, const TString& /*value*/, const TString& /*sourceText*/)>;

public:
    TOptionsParseResult(const TClientCommandOptions* options, int argc, const char** argv);

    // Parses from profile and env. Returns erros if they occur during parsing
    std::vector<TString> ParseFromProfilesAndEnv(std::shared_ptr<IProfile> explicitProfile, std::shared_ptr<IProfile> activeProfile);

    // Logging and validation
    std::vector<TString> Validate(); // Validate current values
    std::vector<TString> LogConnectionParams(const TConnectionParamsLogger& logger); // Check and log all connection params

    const TOptionParseResult* FindResult(const TClientCommandOption* opt) const;
    const TOptionParseResult* FindResult(const TString& name) const;
    const TOptionParseResult* FindResult(char name) const;

    size_t GetFreeArgsPos() const;

    size_t GetFreeArgCount() const;

    TVector<TString> GetFreeArgs() const;

    bool Has(const TString& name, bool includeDefault = false) const;
    bool Has(char name, bool includeDefault = false) const;

    const TString& Get(const TString& name, bool includeDefault = true) const;

    const NLastGetopt::TOptsParseResult& GetCommandLineParseResult() const {
        return ParseFromCommandLineResult;
    }

    const TString& GetChosenAuthMethod() const {
        return ChosenAuthMethod;
    }

private:
    const TClientCommandOptions* ClientOptions = nullptr;
    TCommandOptsParseResult ParseFromCommandLineResult; // First parsing stage
    std::vector<TOptionParseResult> Opts;
    std::vector<size_t> AuthMethodOpts; // indexes
    TString ChosenAuthMethod;
    std::shared_ptr<IProfile> ExplicitProfile;
    std::shared_ptr<IProfile> ActiveProfile;
};

} // namespace NYdb::NConsoleClient
