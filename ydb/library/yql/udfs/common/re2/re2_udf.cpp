#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>

#include <contrib/libs/re2/re2/re2.h>

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

    enum EOptionsField : ui32 {
        OPTIONS_MAP(ENUM_VALUE_GEN)
            Count
    };

    struct TOptionsSchema {
        TType* StructType;
        ui32 Indices[EOptionsField::Count];
    };

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
                const TRegexpGroups& regexpGroups = TRegexpGroups())
                : Mode(mode)
                , OptionsSchema(optionsSchema)
                , Pos_(pos)
                , RegexpGroups(regexpGroups)
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
                        RegexpGroups,
                        Mode,
                        posix,
                        OptionsSchema,
                        Pos_));
            }

            EMode Mode;
            const TOptionsSchema OptionsSchema;
            TSourcePosition Pos_;
            const TRegexpGroups RegexpGroups;
        };

        static const TStringRef& Name(EMode mode) {
            static auto match = TStringRef::Of("Match");
            static auto grep = TStringRef::Of("Grep");
            static auto capture = TStringRef::Of("Capture");
            static auto replace = TStringRef::Of("Replace");
            static auto count = TStringRef::Of("Count");
            static auto findAndconsume = TStringRef::Of("FindAndConsume");

            switch (mode) {
                case EMode::MATCH:
                    return match;
                case EMode::GREP:
                    return grep;
                case EMode::CAPTURE:
                    return capture;
                case EMode::REPLACE:
                    return replace;
                case EMode::COUNT:
                    return count;
                case EMode::FIND_AND_CONSUME:
                    return findAndconsume;
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
            TSourcePosition pos)
            : RegexpGroups(regexpGroups)
            , Mode(mode)
            , Captured()
            , OptionsSchema(optionsSchema)
            , Pos_(pos)
        {
            try {
                auto patternValue = runConfig.GetElement(0);
                auto optionsValue = runConfig.GetElement(1);
                const std::string_view pattern(patternValue.AsStringRef());
                RE2::Options options;

                options.set_posix_syntax(posix);
                bool needUtf8 = (UTF8Detect(pattern) == UTF8);
                options.set_encoding(
                    needUtf8
                        ? RE2::Options::Encoding::EncodingUTF8
                        : RE2::Options::Encoding::EncodingLatin1
                );
                if (optionsValue) {
#define FIELD_HANDLE(name, index, type, defVal, setter, conv) options.setter(conv(optionsValue.GetElement(OptionsSchema.Indices[index]).Get<type>()));
                    OPTIONS_MAP(FIELD_HANDLE)
#undef FIELD_HANDLE
                }

                Regexp = std::make_unique<RE2>(StringPiece(pattern.data(), pattern.size()), options);

                if (mode == EMode::CAPTURE) {
                    Captured = std::make_unique<StringPiece[]>(Regexp->NumberOfCapturingGroups() + 1);
                }

            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
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

                switch (Mode) {
                    case MATCH:
                        anchor = RE2::ANCHOR_BOTH;
                        [[fallthrough]];
                    case GREP:
                        return TUnboxedValuePod(Regexp->Match(piece, 0, input.size(), anchor, nullptr, 0));
                    case CAPTURE: {
                        const int count = Regexp->NumberOfCapturingGroups() + 1;
                        TUnboxedValue* items = nullptr;
                        const auto result = valueBuilder->NewArray(RegexpGroups.Names.size(), items);
                        if (Regexp->Match(piece, 0, input.size(), anchor, Captured.get(), count)) {
                            for (int i = 0; i < count; ++i) {
                                if (!Captured[i].empty()) {
                                    items[RegexpGroups.Indexes[i]] = valueBuilder->SubString(args[0], std::distance(piece.begin(), Captured[i].begin()), Captured[i].size());
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
                        if (!Regexp->CheckRewriteString(rewrite, &rewriteError)) {
                            UdfTerminate((TStringBuilder() << Pos_ << " [rewrite error] " << rewriteError).data());
                        }
                        std::string result(input);
                        RE2::GlobalReplace(&result, *Regexp, rewrite);
                        return input == result ? TUnboxedValue(args[0]) : valueBuilder->NewString(result);
                    }
                    case COUNT: {
                        std::string inputHolder(input);
                        const ui32 result = RE2::GlobalReplace(&inputHolder, *Regexp, "");
                        return TUnboxedValuePod(result);
                    }
                    case FIND_AND_CONSUME: {
                        StringPiece text(piece);
                        std::vector<TUnboxedValue> matches;
                        for (StringPiece w; text.begin() < text.end() && RE2::FindAndConsume(&text, *Regexp, &w);) {
                            if (w.size() == 0) {
                                text.remove_prefix(1);
                            }
                            matches.emplace_back(valueBuilder->SubString(args[0], std::distance(piece.begin(), w.begin()), w.size()));
                        }
                        return valueBuilder->NewList(matches.data(), matches.size());
                    }
                }
                Y_ABORT("Unexpected mode");
            } else {
                switch (Mode) {
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
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }

        std::unique_ptr<RE2> Regexp;
        const TRegexpGroups RegexpGroups;
        EMode Mode;
        std::unique_ptr<StringPiece[]> Captured;
        const TOptionsSchema OptionsSchema;
        TSourcePosition Pos_;

        TUnboxedValue BuildEmptyStruct(const IValueBuilder* valueBuilder) const {
            TUnboxedValue* items = nullptr;
            return valueBuilder->NewArray(RegexpGroups.Names.size(), items);
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
        TOptions(const TOptionsSchema& schema)
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
            static auto name = ::NKikimr::NUdf::TStringRef::Of("Options");
            return name;
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

    SIMPLE_UDF_WITH_OPTIONAL_ARGS(TPatternFromLike, char*(char*, TOptional<char*>), 1) {
        const std::string_view input(args[0].AsStringRef());
        const bool hasEscape = bool(args[1]);
        char escape = 0;
        if (hasEscape) {
            const std::string_view escapeRef(args[1].AsStringRef());
            if (escapeRef.size() != 1U) {
                UdfTerminate((TStringBuilder() << GetPos() << " Escape should be single character").data());
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
                        if (slash)
                            result << '\\';
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
                    builder.Implementation(new TRe2Udf::TFactory<posix>(mode, optionsSchema, builder.GetSourcePosition()));
                }
            } else if (isCapture) {
                TRegexpGroups groups;
                auto optionalStringType = builder.Optional()->Item<char*>().Build();
                auto structBuilder = builder.Struct();
                RE2 regexp(StringPiece(typeConfig.Data(), typeConfig.Size()));
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
                            new TRe2Udf::TFactory<posix>(TRe2Udf::EMode::CAPTURE, optionsSchema, builder.GetSourcePosition(), groups));
                    }

                } else {
                    if (regexp.ok()) {
                        builder.SetError("Regexp contains no capturing groups");
                    } else {
                        builder.SetError(regexp.error());
                    }
                }
            } else if (isReplace) {
                builder.SimpleSignature<TOptional<char*>(TOptional<char*>, char*)>()
                    .RunConfig(MakeRunConfigType(builder, optOptionsStructType));

                if (!typesOnly) {
                    builder.Implementation(new TRe2Udf::TFactory<posix>(TRe2Udf::EMode::REPLACE, optionsSchema, builder.GetSourcePosition()));
                }
            } else if (isCount) {
                builder.SimpleSignature<ui32(TOptional<char*>)>()
                    .RunConfig(MakeRunConfigType(builder, optOptionsStructType));

                if (!typesOnly) {
                    builder.Implementation(new TRe2Udf::TFactory<posix>(TRe2Udf::EMode::COUNT, optionsSchema, builder.GetSourcePosition()));
                }
            } else if (isFindAndConsume) {
                builder.SimpleSignature<TListType<char*>(TOptional<char*>)>()
                    .RunConfig(MakeRunConfigType(builder, optOptionsStructType));
                if (!typesOnly) {
                    builder.Implementation(new TRe2Udf::TFactory<posix>(TRe2Udf::EMode::FIND_AND_CONSUME, optionsSchema, builder.GetSourcePosition()));
                }
            } else if (!(
                            TEscape::DeclareSignature(name, userType, builder, typesOnly) ||
                            TPatternFromLike::DeclareSignature(name, userType, builder, typesOnly) ||
                            TOptions::DeclareSignature(name, userType, builder, typesOnly))) {
                builder.SetError(
                    TStringBuilder() << "Unknown function name: " << TString(name));
            }
        } catch (const std::exception& e) {
            builder.SetError(CurrentExceptionMessage());
        }
    };

}

REGISTER_MODULES(
    TRe2Module<false>,
    TRe2Module<true>)
