#include <yql/essentials/public/langver/yql_langver.h>
#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/public/udf/udf_type_ops.h>
#include <yql/essentials/public/udf/udf_value_builder.h>

#include <contrib/libs/re2/re2/re2.h>

#include <util/system/env.h>
#include <util/charset/utf8.h>
#include <util/string/cast.h>

using namespace re2;
using namespace NKikimr;
using namespace NUdf;

namespace {

template <typename T>
T Id(T x) {
    return x;
}

re2::RE2::Options::Encoding EncodingFromBool(bool x) {
    return x ? re2::RE2::Options::Encoding::EncodingUTF8 : re2::RE2::Options::Encoding::EncodingLatin1;
}

#define OPTIONS_MAP(xx)                                                                                  \
    xx(Utf8, 0, bool, true, set_encoding, EncodingFromBool)                                              \
        xx(PosixSyntax, 1, bool, false, set_posix_syntax, Id)                                            \
            xx(LongestMatch, 2, bool, false, set_longest_match, Id)                                      \
                xx(LogErrors, 3, bool, true, set_log_errors, Id)                                         \
                    xx(MaxMem, 4, ui64, 8 << 20, set_max_mem, Id)                                        \
                        xx(Literal, 5, bool, false, set_literal, Id)                                     \
                            xx(NeverNl, 6, bool, false, set_never_nl, Id)                                \
                                xx(DotNl, 7, bool, false, set_dot_nl, Id)                                \
                                    xx(NeverCapture, 8, bool, false, set_never_capture, Id)              \
                                        xx(CaseSensitive, 9, bool, true, set_case_sensitive, Id)         \
                                            xx(PerlClasses, 10, bool, false, set_perl_classes, Id)       \
                                                xx(WordBoundary, 11, bool, false, set_word_boundary, Id) \
                                                    xx(OneLine, 12, bool, false, set_one_line, Id)

ui64 GetFailProbability() {
    auto envResult = TryGetEnv("YQL_RE2_REGEXP_PROBABILITY_FAIL");
    if (!envResult) {
        return 0;
    }
    ui64 result;
    bool isValid = TryIntFromString<10, ui64>(envResult->data(), envResult->size(), result);
    Y_ENSURE(isValid, TStringBuilder() << "Error while parsing YQL_RE2_REGEXP_PROBABILITY_FAIL. Actual value is: " << *envResult);
    return result;
}

bool ShouldFailOnInvalidRegexp(const std::string_view regexp, NYql::TLangVersion currentLangVersion) {
    if (currentLangVersion >= NYql::MakeLangVersion(2025, 3)) {
        return true;
    }
    THashType hash = GetStringHash(regexp) % 100;
    static ui64 FailProbability = GetFailProbability();
    return hash < FailProbability;
}

RE2::Options CreateDefaultOptions() {
    RE2::Options options;
#define FIELD_HANDLE(name, index, type, defVal, setter, conv) options.setter(conv(defVal));
    OPTIONS_MAP(FIELD_HANDLE)
#undef FIELD_HANDLE
    options.set_log_errors(false);
    return options;
}

TString FormatRegexpError(const RE2& Regexp) {
    return TStringBuilder() << "Regexp compilation failed. Regexp: \"" << Regexp.pattern() << "\". Original error is: \"" << Regexp.error() << "\"";
}

enum EOptionsField: ui32 {
    OPTIONS_MAP(ENUM_VALUE_GEN)
        Count
};

struct TOptionsSchema {
    TType* StructType;
    std::array<ui32, EOptionsField::Count> Indices;
};

RE2::Options ExtractOptions(std::string_view pattern, TUnboxedValuePod optionsValue, const TOptionsSchema& schema, bool posix) {
    RE2::Options options = CreateDefaultOptions();

    options.set_posix_syntax(posix);
    bool needUtf8 = (UTF8Detect(pattern) == UTF8);
    options.set_encoding(
        needUtf8
            ? RE2::Options::Encoding::EncodingUTF8
            : RE2::Options::Encoding::EncodingLatin1);
    if (optionsValue) {
#define FIELD_HANDLE(name, index, type, defVal, setter, conv) options.setter(conv(optionsValue.GetElement(schema.Indices[index]).Get<type>()));
        OPTIONS_MAP(FIELD_HANDLE)
#undef FIELD_HANDLE
        options.set_log_errors(false);
    }
    return options;
}

struct TRegexpGroups {
    TVector<TString> Names;
    TVector<ui32> Indexes;
};

class TRe2Udf: public TBoxedValue {
public:
    enum EMode {
        MATCH,
        GREP,
        CAPTURE,
        REPLACE,
        COUNT,
        FIND_AND_CONSUME,
    };

    template <bool posix>
    class TFactory: public TBoxedValue {
    public:
        TFactory(
            EMode mode,
            const TOptionsSchema& optionsSchema,
            TSourcePosition pos,
            NYql::TLangVersion currentlangVersion,
            const TRegexpGroups& regexpGroups = TRegexpGroups())
            : Mode_(mode)
            , OptionsSchema_(optionsSchema)
            , Pos_(pos)
            , RegexpGroups_(regexpGroups)
            , CurrentLangVersion_(currentlangVersion)
        {
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override {
            return TUnboxedValuePod(
                new TRe2Udf(
                    valueBuilder,
                    args[0],
                    RegexpGroups_,
                    Mode_,
                    posix,
                    OptionsSchema_,
                    Pos_,
                    CurrentLangVersion_));
        }

        EMode Mode_;
        const TOptionsSchema OptionsSchema_;
        TSourcePosition Pos_;
        const TRegexpGroups RegexpGroups_;
        NYql::TLangVersion CurrentLangVersion_;
    };

    static const TStringRef& Name(EMode mode) {
        static auto Match = TStringRef::Of("Match");
        static auto Grep = TStringRef::Of("Grep");
        static auto Capture = TStringRef::Of("Capture");
        static auto Replace = TStringRef::Of("Replace");
        static auto Count = TStringRef::Of("Count");
        static auto FindAndconsume = TStringRef::Of("FindAndConsume");

        switch (mode) {
            case EMode::MATCH:
                return Match;
            case EMode::GREP:
                return Grep;
            case EMode::CAPTURE:
                return Capture;
            case EMode::REPLACE:
                return Replace;
            case EMode::COUNT:
                return Count;
            case EMode::FIND_AND_CONSUME:
                return FindAndconsume;
        }
        Y_ABORT("Unexpected mode");
    }

    TRe2Udf(
        const IValueBuilder*,
        const TUnboxedValuePod& runConfig,
        const TRegexpGroups regexpGroups,
        EMode mode,
        bool posix,
        const TOptionsSchema& optionsSchema,
        TSourcePosition pos,
        NYql::TLangVersion currentLangVersion)
        : RegexpGroups_(regexpGroups)
        , Mode_(mode)
        , Captured_()
        , OptionsSchema_(optionsSchema)
        , Pos_(pos)
        , CurrentLangVersion_(currentLangVersion)
    {
        try {
            auto patternValue = runConfig.GetElement(0);
            auto optionsValue = runConfig.GetElement(1);
            const std::string_view pattern(patternValue.AsStringRef());

            RE2::Options options = ExtractOptions(pattern, optionsValue, OptionsSchema_, posix);
            Regexp_ = std::make_unique<RE2>(StringPiece(pattern.data(), pattern.size()), options);

            if (!Regexp_->ok() && ShouldFailOnInvalidRegexp(pattern, CurrentLangVersion_)) {
                throw yexception() << FormatRegexpError(*Regexp_);
            }

            if (mode == EMode::CAPTURE) {
                // NOLINTNEXTLINE(modernize-avoid-c-arrays)
                Captured_ = std::make_unique<StringPiece[]>(Regexp_->NumberOfCapturingGroups() + 1);
            }

        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).c_str());
        }
    }

private:
    TUnboxedValue Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const final try {
        RE2::Anchor anchor = RE2::UNANCHORED;
        if (args[0]) {
            const std::string_view input(args[0].AsStringRef());
            const StringPiece piece(input.data(), input.size());

            switch (Mode_) {
                case MATCH:
                    anchor = RE2::ANCHOR_BOTH;
                    [[fallthrough]];
                case GREP:
                    return TUnboxedValuePod(Regexp_->Match(piece, 0, input.size(), anchor, nullptr, 0));
                case CAPTURE: {
                    const int count = Regexp_->NumberOfCapturingGroups() + 1;
                    TUnboxedValue* items = nullptr;
                    const auto result = valueBuilder->NewArray(RegexpGroups_.Names.size(), items);
                    if (Regexp_->Match(piece, 0, input.size(), anchor, Captured_.get(), count)) {
                        for (int i = 0; i < count; ++i) {
                            if (!Captured_[i].empty()) {
                                items[RegexpGroups_.Indexes[i]] = valueBuilder->SubString(args[0], std::distance(piece.begin(), Captured_[i].begin()), Captured_[i].size());
                            }
                        }
                    } else {
                        return BuildEmptyStruct(valueBuilder);
                    }
                    return result;
                }
                case REPLACE: {
                    const std::string_view rewriteRef(args[1].AsStringRef());
                    const StringPiece rewrite(rewriteRef.data(), rewriteRef.size());
                    TString rewriteError;
                    if (!Regexp_->CheckRewriteString(rewrite, &rewriteError)) {
                        UdfTerminate((TStringBuilder() << Pos_ << " [rewrite error] " << rewriteError).c_str());
                    }
                    std::string result(input);
                    RE2::GlobalReplace(&result, *Regexp_, rewrite);
                    return input == result ? TUnboxedValue(args[0]) : valueBuilder->NewString(result);
                }
                case COUNT: {
                    std::string inputHolder(input);
                    const ui32 result = RE2::GlobalReplace(&inputHolder, *Regexp_, "");
                    return TUnboxedValuePod(result);
                }
                case FIND_AND_CONSUME: {
                    StringPiece text(piece);
                    std::vector<TUnboxedValue> matches;
                    for (StringPiece w; text.begin() < text.end() && RE2::FindAndConsume(&text, *Regexp_, &w);) {
                        if (w.size() == 0 && !text.empty()) {
                            text.remove_prefix(1);
                        }
                        matches.emplace_back(valueBuilder->SubString(args[0], std::distance(piece.begin(), w.begin()), w.size()));
                    }
                    return valueBuilder->NewList(matches.data(), matches.size());
                }
            }
            Y_ABORT("Unexpected mode");
        } else {
            switch (Mode_) {
                case MATCH:
                case GREP:
                    return TUnboxedValuePod(false);
                case CAPTURE:
                    return BuildEmptyStruct(valueBuilder);
                case REPLACE:
                    return TUnboxedValuePod();
                case COUNT:
                    return TUnboxedValuePod::Zero();
                case FIND_AND_CONSUME:
                    return valueBuilder->NewEmptyList();
            }
            Y_ABORT("Unexpected mode");
        }
    } catch (const std::exception& e) {
        UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).c_str());
    }

    std::unique_ptr<RE2> Regexp_;
    const TRegexpGroups RegexpGroups_;
    EMode Mode_;
    std::unique_ptr<StringPiece[]> Captured_; // NOLINT(modernize-avoid-c-arrays)
    const TOptionsSchema OptionsSchema_;
    TSourcePosition Pos_;
    NYql::TLangVersion CurrentLangVersion_;

    TUnboxedValue BuildEmptyStruct(const IValueBuilder* valueBuilder) const {
        TUnboxedValue* items = nullptr;
        return valueBuilder->NewArray(RegexpGroups_.Names.size(), items);
    }
};

SIMPLE_STRICT_UDF(TEscape, char*(char*)) {
    const std::string_view input(args[0].AsStringRef());
    const auto& result = RE2::QuoteMeta(StringPiece(input.data(), input.size()));
    return input == result ? TUnboxedValue(args[0]) : valueBuilder->NewString(result);
}

TOptionsSchema MakeOptionsSchema(::NKikimr::NUdf::IFunctionTypeInfoBuilder& builder) {
    TOptionsSchema ret;
    auto structBuilder = builder.Struct(EOptionsField::Count);
#define FIELD_HANDLE(name, index, type, ...) structBuilder->AddField<type>(TStringRef::Of(#name), &ret.Indices[index]);
    OPTIONS_MAP(FIELD_HANDLE)
#undef FIELD_HANDLE

    ret.StructType = structBuilder->Build();
    return ret;
}

class TOptions: public TBoxedValue {
private:
    const TOptionsSchema Schema_;

public:
    explicit TOptions(const TOptionsSchema& schema)
        : Schema_(schema)
    {
    }

    TUnboxedValue Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const override {
        TUnboxedValue* items = nullptr;
        const auto result = valueBuilder->NewArray(EOptionsField::Count, items);
#define FIELD_HANDLE(name, index, type, defVal, ...)                          \
    {                                                                         \
        auto structIndex = Schema_.Indices[index];                            \
        if (!args[index]) {                                                   \
            items[structIndex] = TUnboxedValuePod(static_cast<type>(defVal)); \
        } else {                                                              \
            items[structIndex] = args[index].GetOptionalValue();              \
        }                                                                     \
    }

        OPTIONS_MAP(FIELD_HANDLE)
#undef FIELD_HANDLE
        return result;
    }

    static const ::NKikimr::NUdf::TStringRef& Name() {
        static auto Name = ::NKikimr::NUdf::TStringRef::Of("Options");
        return Name;
    }

    static bool DeclareSignature(
        const ::NKikimr::NUdf::TStringRef& name,
        ::NKikimr::NUdf::TType* userType,
        ::NKikimr::NUdf::IFunctionTypeInfoBuilder& builder,
        bool typesOnly) {
        Y_UNUSED(userType);
        if (Name() == name) {
            builder.IsStrict();

            auto argsBuilder = builder.Args();
#define FIELD_HANDLE(name, index, type, ...) argsBuilder->Add<TOptional<type>>().Name(TStringRef::Of(#name));
            OPTIONS_MAP(FIELD_HANDLE)
#undef FIELD_HANDLE
            auto optionsSchema = MakeOptionsSchema(builder);
            builder.Returns(optionsSchema.StructType);
            builder.OptionalArgs(EOptionsField::Count);
            if (!typesOnly) {
                builder.Implementation(new TOptions(optionsSchema));
            }

            return true;
        } else {
            return false;
        }
    }
};

template <bool posix>
class TIsValidRegexp: public TBoxedValue {
public:
    explicit TIsValidRegexp(const TOptionsSchema optionsSchema)
        : OptionsSchema_(std::move(optionsSchema))
    {
    }

    TUnboxedValue Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const override {
        Y_UNUSED(valueBuilder);
        if (!args[0]) {
            return TUnboxedValuePod(false);
        }
        RE2::Options options = ExtractOptions(args[0].AsStringRef(), args[1], OptionsSchema_, posix);
        RE2 regexp(args[0].AsStringRef(), options);
        return TUnboxedValuePod(regexp.ok());
    }

    static const ::NKikimr::NUdf::TStringRef& Name() {
        static auto Name = ::NKikimr::NUdf::TStringRef::Of("IsValidRegexp");
        return Name;
    }

    static bool DeclareSignature(
        const ::NKikimr::NUdf::TStringRef& name,
        ::NKikimr::NUdf::TType* userType,
        ::NKikimr::NUdf::IFunctionTypeInfoBuilder& builder,
        bool typesOnly) {
        Y_UNUSED(userType);
        if (Name() == name) {
            TOptionsSchema optionsSchema = MakeOptionsSchema(builder);
            auto optOptionsStructType = builder.Optional()->Item(optionsSchema.StructType).Build();
            builder.Args()
                ->Add(builder.Optional()->Item(builder.SimpleType<char*>()))
                .Add(optOptionsStructType)
                .Done()
                .Returns(builder.SimpleType<bool>());

            builder.OptionalArgs(1);
            if (!typesOnly) {
                builder.Implementation(new TIsValidRegexp(std::move(optionsSchema)));
            }
            builder.IsStrict();
            return true;
        } else {
            return false;
        }
    }

private:
    const TOptionsSchema OptionsSchema_;
};

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TPatternFromLike, char*(char*, TOptional<char*>), 1) {
    const std::string_view input(args[0].AsStringRef());
    const bool hasEscape = bool(args[1]);
    char escape = 0;
    if (hasEscape) {
        const std::string_view escapeRef(args[1].AsStringRef());
        if (escapeRef.size() != 1U) {
            UdfTerminate((TStringBuilder() << GetPos() << " Escape should be single character").c_str());
        }
        escape = escapeRef.front();
    }
    const TString escaped(RE2::QuoteMeta(StringPiece(input.data(), input.size())));

    TStringBuilder result;
    result << "(?s)";
    bool slash = false;
    bool escapeOn = false;

    for (const char& c : escaped) {
        switch (c) {
            case '\\':
                if (slash) {
                    result << "\\\\";
                }
                slash = !slash;
                break;
            case '%':
                if (escapeOn) {
                    result << "\\%";
                    escapeOn = false;
                } else {
                    result << ".*";
                }
                slash = false;
                break;
            case '_':
                if (escapeOn) {
                    result << "\\_";
                    escapeOn = false;
                } else {
                    result << '.';
                }
                slash = false;
                break;
            default:
                if (hasEscape && c == escape) {
                    if (escapeOn) {
                        result << RE2::QuoteMeta(StringPiece(&c, 1));
                    }
                    escapeOn = !escapeOn;
                } else {
                    if (slash) {
                        result << '\\';
                    }
                    result << c;
                    escapeOn = false;
                }
                slash = false;
                break;
        }
    }
    return valueBuilder->NewString(result);
}

TType* MakeRunConfigType(IFunctionTypeInfoBuilder& builder, TType* optOptionsStructType) {
    return builder.Tuple()->Add<char*>().Add(optOptionsStructType).Build();
}

template <bool posix>
class TRe2Module: public IUdfModule {
public:
    TStringRef Name() const {
        return posix ? TStringRef::Of("Re2posix") : TStringRef::Of("Re2");
    }

    void CleanupOnTerminate() const final {
    }

    void GetAllFunctions(IFunctionsSink& sink) const final {
        sink.Add(TRe2Udf::Name(TRe2Udf::EMode::MATCH));
        sink.Add(TRe2Udf::Name(TRe2Udf::EMode::GREP));
        sink.Add(TRe2Udf::Name(TRe2Udf::EMode::CAPTURE))->SetTypeAwareness();
        sink.Add(TRe2Udf::Name(TRe2Udf::EMode::REPLACE));
        sink.Add(TRe2Udf::Name(TRe2Udf::EMode::COUNT));
        sink.Add(TRe2Udf::Name(TRe2Udf::EMode::FIND_AND_CONSUME));
        sink.Add(TEscape::Name());
        sink.Add(TPatternFromLike::Name());
        sink.Add(TOptions::Name());
        sink.Add(TIsValidRegexp<posix>::Name());
    }

    void BuildFunctionTypeInfo(
        const TStringRef& name,
        TType* userType,
        const TStringRef& typeConfig,
        ui32 flags,
        IFunctionTypeInfoBuilder& builder) const final try {
        Y_UNUSED(userType);
        TOptionsSchema optionsSchema = MakeOptionsSchema(builder);
        auto optOptionsStructType = builder.Optional()->Item(optionsSchema.StructType).Build();

        bool typesOnly = (flags & TFlags::TypesOnly);
        bool isMatch = (TRe2Udf::Name(TRe2Udf::EMode::MATCH) == name);
        bool isGrep = (TRe2Udf::Name(TRe2Udf::EMode::GREP) == name);
        bool isCapture = (TRe2Udf::Name(TRe2Udf::EMode::CAPTURE) == name);
        bool isReplace = (TRe2Udf::Name(TRe2Udf::EMode::REPLACE) == name);
        bool isCount = (TRe2Udf::Name(TRe2Udf::EMode::COUNT) == name);
        bool isFindAndConsume = (TRe2Udf::Name(TRe2Udf::FIND_AND_CONSUME) == name);

        if (isMatch || isGrep) {
            builder.SimpleSignature<bool(TOptional<char*>)>()
                .RunConfig(MakeRunConfigType(builder, optOptionsStructType));

            if (!typesOnly) {
                const auto mode = isMatch ? TRe2Udf::EMode::MATCH : TRe2Udf::EMode::GREP;
                builder.Implementation(new TRe2Udf::TFactory<posix>(mode, optionsSchema, builder.GetSourcePosition(), builder.GetCurrentLangVer()));
            }
        } else if (isCapture) {
            TRegexpGroups groups;
            auto optionalStringType = builder.Optional()->Item<char*>().Build();
            auto structBuilder = builder.Struct();
            RE2::Options options = CreateDefaultOptions();
            RE2 regexp(StringPiece(typeConfig.Data(), typeConfig.Size()), options);
            if (!regexp.ok()) {
                builder.SetError(FormatRegexpError(regexp));
                return;
            }
            const auto& groupNames = regexp.CapturingGroupNames();
            int groupCount = regexp.NumberOfCapturingGroups();
            if (groupCount >= 0) {
                std::unordered_set<std::string_view> groupNamesSet;
                int unnamedCount = 0;
                ++groupCount;
                groups.Indexes.resize(groupCount);
                groups.Names.resize(groupCount);
                for (int i = 0; i < groupCount; ++i) {
                    TString fieldName;
                    auto it = groupNames.find(i);
                    if (it != groupNames.end()) {
                        if (!groupNamesSet.insert(it->second).second) {
                            builder.SetError(
                                TStringBuilder() << "Regexp contains duplicate capturing group name: " << it->second);
                            return;
                        }
                        fieldName = it->second;
                    } else {
                        fieldName = "_" + ToString(unnamedCount);
                        ++unnamedCount;
                    }
                    groups.Names[i] = fieldName;
                    structBuilder->AddField(fieldName, optionalStringType, &groups.Indexes[i]);
                }
                builder.Args(1)->Add(optionalStringType).Done().Returns(structBuilder->Build()).RunConfig(MakeRunConfigType(builder, optOptionsStructType));

                if (!typesOnly) {
                    builder.Implementation(
                        new TRe2Udf::TFactory<posix>(TRe2Udf::EMode::CAPTURE, optionsSchema, builder.GetSourcePosition(), builder.GetCurrentLangVer(), groups));
                }

            } else {
                Y_ENSURE(regexp.ok());
                builder.SetError("Regexp contains no capturing groups");
            }
        } else if (isReplace) {
            builder.SimpleSignature<TOptional<char*>(TOptional<char*>, char*)>()
                .RunConfig(MakeRunConfigType(builder, optOptionsStructType));

            if (!typesOnly) {
                builder.Implementation(new TRe2Udf::TFactory<posix>(TRe2Udf::EMode::REPLACE, optionsSchema, builder.GetSourcePosition(), builder.GetCurrentLangVer()));
            }
        } else if (isCount) {
            builder.SimpleSignature<ui32(TOptional<char*>)>()
                .RunConfig(MakeRunConfigType(builder, optOptionsStructType));

            if (!typesOnly) {
                builder.Implementation(new TRe2Udf::TFactory<posix>(TRe2Udf::EMode::COUNT, optionsSchema, builder.GetSourcePosition(), builder.GetCurrentLangVer()));
            }
        } else if (isFindAndConsume) {
            builder.SimpleSignature<TListType<char*>(TOptional<char*>)>()
                .RunConfig(MakeRunConfigType(builder, optOptionsStructType));
            if (!typesOnly) {
                builder.Implementation(new TRe2Udf::TFactory<posix>(TRe2Udf::EMode::FIND_AND_CONSUME, optionsSchema, builder.GetSourcePosition(), builder.GetCurrentLangVer()));
            }
        } else if (!(
                       TEscape::DeclareSignature(name, userType, builder, typesOnly) ||
                       TPatternFromLike::DeclareSignature(name, userType, builder, typesOnly) ||
                       TOptions::DeclareSignature(name, userType, builder, typesOnly) ||
                       TIsValidRegexp<posix>::DeclareSignature(name, userType, builder, typesOnly))) {
            builder.SetError(
                TStringBuilder() << "Unknown function name: " << TString(name));
        }
    } catch (const std::exception& e) {
        builder.SetError(CurrentExceptionMessage());
    }
};

} // namespace

REGISTER_MODULES(
    TRe2Module<false>,
    TRe2Module<true>)
