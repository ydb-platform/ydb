#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/public/udf/udf_registrator.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_terminator.h>

#include <library/cpp/regex/hyperscan/hyperscan.h>
#include <library/cpp/regex/pcre/regexp.h>

#include <util/charset/utf8.h>
#include <util/string/split.h>
#include <util/string/builder.h>
#include <util/system/cpu_id.h>

using namespace NHyperscan;
using namespace NKikimr;
using namespace NUdf;

namespace {
    using TOptions = ui32;
    class THyperscanUdfBase: public TBoxedValue {
    protected:
        constexpr static const char* IGNORE_CASE_PREFIX = "(?i)";
        static void SetCommonOptions(TString& regex, TOptions& options) {
            options |= HS_FLAG_ALLOWEMPTY;
            if (regex.StartsWith(IGNORE_CASE_PREFIX)) {
                options |= HS_FLAG_CASELESS;
                regex = regex.substr(4);
            }
            if (UTF8Detect(regex) == UTF8) {
                options |= HS_FLAG_UTF8;
            }
            if (NX86::HaveAVX2()) {
                options |= HS_CPU_FEATURES_AVX2;
            }
        }
    };

    class THyperscanMatch: public THyperscanUdfBase {
    public:
        enum class EMode {
            NORMAL,
            BACKTRACKING,
            MULTI
        };

        class TFactory: public THyperscanUdfBase {
        public:
            TFactory(
                TSourcePosition pos,
                bool surroundMode,
                THyperscanMatch::EMode mode,
                size_t regexpsCount = 0)
                : Pos_(pos)
                , SurroundMode(surroundMode)
                , Mode(mode)
                , RegexpsCount(regexpsCount)
            {
            }

        private:
            TUnboxedValue Run(
                const IValueBuilder* valueBuilder,
                const TUnboxedValuePod* args) const override {
                return TUnboxedValuePod(
                    new THyperscanMatch(
                        valueBuilder,
                        args[0],
                        SurroundMode,
                        Mode,
                        Pos_,
                        RegexpsCount));
            }

            TSourcePosition Pos_;
            bool SurroundMode;
            THyperscanMatch::EMode Mode;
            size_t RegexpsCount;
        };

        static const TStringRef& Name(bool isGrep, THyperscanMatch::EMode mode) {
            static auto match = TStringRef::Of("Match");
            static auto grep = TStringRef::Of("Grep");
            static auto backtrackingMatch = TStringRef::Of("BacktrackingMatch");
            static auto backtrackingGrep = TStringRef::Of("BacktrackingGrep");
            static auto multiMatch = TStringRef::Of("MultiMatch");
            static auto multiGrep = TStringRef::Of("MultiGrep");
            if (isGrep) {
                switch (mode) {
                    case THyperscanMatch::EMode::NORMAL:
                        return grep;
                    case THyperscanMatch::EMode::BACKTRACKING:
                        return backtrackingGrep;
                    case THyperscanMatch::EMode::MULTI:
                        return multiGrep;
                }
            } else {
                switch (mode) {
                    case THyperscanMatch::EMode::NORMAL:
                        return match;
                    case THyperscanMatch::EMode::BACKTRACKING:
                        return backtrackingMatch;
                    case THyperscanMatch::EMode::MULTI:
                        return multiMatch;
                }
            }

            Y_ABORT("Unexpected");
        }

        THyperscanMatch(
            const IValueBuilder*,
            const TUnboxedValuePod& runConfig,
            bool surroundMode,
            THyperscanMatch::EMode mode,
            TSourcePosition pos,
            size_t regexpsCount)
            : Regex_(runConfig.AsStringRef())
            , Mode(mode)
            , Pos_(pos)
            , RegexpsCount(regexpsCount)
        {
            try {
                TOptions options = 0;
                int pcreOptions = REG_EXTENDED;
                if (Mode == THyperscanMatch::EMode::BACKTRACKING && Regex_.StartsWith(IGNORE_CASE_PREFIX)) {
                    pcreOptions |= REG_ICASE;
                }
                auto regex = Regex_;
                SetCommonOptions(regex, options);
                switch (mode) {
                    case THyperscanMatch::EMode::NORMAL: {
                        if (!surroundMode) {
                            regex = TStringBuilder() << '^' << regex << '$';
                        }
                        Database_ = Compile(regex, options);
                        break;
                    }
                    case THyperscanMatch::EMode::BACKTRACKING: {
                        if (!surroundMode) {
                            regex = TStringBuilder() << '^' << regex << '$';
                        }
                        try {
                            Database_ = Compile(regex, options);
                            Mode = THyperscanMatch::EMode::NORMAL;
                        } catch (const TCompileException&) {
                            options |= HS_FLAG_PREFILTER;
                            Database_ = Compile(regex, options);
                            Fallback_ = TRegExMatch(regex, pcreOptions);
                        }
                        break;
                    }
                    case THyperscanMatch::EMode::MULTI: {
                        std::vector<TString> regexes;
                        TVector<const char*> cregexes;
                        TVector<TOptions> flags;
                        TVector<TOptions> ids;

                        const auto func = [&regexes, &flags, surroundMode](const std::string_view& token) {
                            TString regex(token);

                            TOptions opt = 0;
                            SetCommonOptions(regex, opt);

                            if (!surroundMode) {
                                regex = TStringBuilder() << '^' << regex << '$';
                            }

                            regexes.emplace_back(std::move(regex));
                            flags.emplace_back(opt);
                        };
                        StringSplitter(Regex_).Split('\n').Consume(func);

                        std::transform(regexes.cbegin(), regexes.cend(), std::back_inserter(cregexes), std::bind(&TString::c_str, std::placeholders::_1));
                        ids.resize(regexes.size());
                        std::iota(ids.begin(), ids.end(), 0);

                        Database_ = CompileMulti(cregexes, flags, ids);
                        break;
                    }
                }
                Scratch_ = MakeScratch(Database_);
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const final try {
            TUnboxedValue* items = nullptr;
            TUnboxedValue tuple;
            size_t i = 0;

            if (Mode == THyperscanMatch::EMode::MULTI) {
                tuple = valueBuilder->NewArray(RegexpsCount, items);
                for (i = 0; i < RegexpsCount; ++i) {
                    items[i] = TUnboxedValuePod(false);
                }
            }

            if (args[0]) {
                // XXX: StringRef data might not be a NTBS, though the function
                // <TRegExMatch::Match> expects ASCIIZ string. Explicitly copy
                // the given argument string and append the NUL terminator to it.
                const TString input(args[0].AsStringRef());
                if (Y_UNLIKELY(Mode == THyperscanMatch::EMode::MULTI)) {
                    auto callback = [items] (TOptions id, ui64 /* from */, ui64 /* to */) {
                        items[id] = TUnboxedValuePod(true);
                    };
                    Scan(Database_, Scratch_, input, callback);
                    return tuple;
                } else {
                    bool matches = Matches(Database_, Scratch_, input);
                    if (matches && Mode == THyperscanMatch::EMode::BACKTRACKING) {
                        matches = Fallback_.Match(input.data());
                    }
                    return TUnboxedValuePod(matches);
                }

            } else {
                return Mode == THyperscanMatch::EMode::MULTI ? tuple : TUnboxedValue(TUnboxedValuePod(false));
            }
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }

    private:
        const TString Regex_;
        THyperscanMatch::EMode Mode;
        const TSourcePosition Pos_;
        const size_t RegexpsCount;
        TDatabase Database_;
        TScratch Scratch_;
        TRegExMatch Fallback_;
    };

    class THyperscanCapture: public THyperscanUdfBase {
    public:
        class TFactory: public THyperscanUdfBase {
        public:
            TFactory(TSourcePosition pos)
                : Pos_(pos)
            {}

        private:
            TUnboxedValue Run(const IValueBuilder*,
                const TUnboxedValuePod* args) const final try {
                return TUnboxedValuePod(new THyperscanCapture(args[0], Pos_));
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }

        private:
            TSourcePosition Pos_;
        };

        static const TStringRef& Name() {
            static auto name = TStringRef::Of("Capture");
            return name;
        }

        THyperscanCapture(const TUnboxedValuePod& runConfig, TSourcePosition pos)
            : Pos_(pos)
        {
            Regex_ = runConfig.AsStringRef();
            TOptions options = HS_FLAG_SOM_LEFTMOST;

            SetCommonOptions(Regex_, options);

            Database_ = Compile(Regex_, options);
            Scratch_ = MakeScratch(Database_);
        }


    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const final try {
            if (const auto arg = args[0]) {

                TUnboxedValue result;
                auto callback = [valueBuilder, arg, &result] (TOptions id, ui64 from, ui64 to) {
                    Y_UNUSED(id);
                    if (!result) {
                        result = valueBuilder->SubString(arg, from, to);
                    }
                };
                Scan(Database_, Scratch_, arg.AsStringRef(), callback);
                return result;
            }

            return TUnboxedValue();
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }

        TSourcePosition Pos_;
        TString Regex_;
        TDatabase Database_;
        TScratch Scratch_;
    };

    class THyperscanReplace: public THyperscanUdfBase {
    public:
        class TFactory: public THyperscanUdfBase {
        public:
            TFactory(TSourcePosition pos)
                : Pos_(pos)
            {}

        private:
            TUnboxedValue Run(const IValueBuilder*,
                const TUnboxedValuePod* args) const final try {
                return TUnboxedValuePod(new THyperscanReplace(args[0], Pos_));
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }

        private:
            TSourcePosition Pos_;
        };

        static const TStringRef& Name() {
            static auto name = TStringRef::Of("Replace");
            return name;
        }

        THyperscanReplace(const TUnboxedValuePod& runConfig, TSourcePosition pos)
            : Pos_(pos)
        {
            Regex_ = runConfig.AsStringRef();
            TOptions options = HS_FLAG_SOM_LEFTMOST;

            SetCommonOptions(Regex_, options);


            Database_ = Compile(Regex_, options);
            Scratch_ = MakeScratch(Database_);
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const final try {
            if (args[0]) {
                const std::string_view input(args[0].AsStringRef());
                const std::string_view replacement(args[1].AsStringRef());

                ui64 index = 0;
                TStringBuilder result;
                auto callback = [input, replacement, &index, &result] (TOptions id, ui64 from, ui64 to) {
                    Y_UNUSED(id);
                    if (index != from) {
                        result << input.substr(index, from - index);
                    }
                    result << replacement;
                    index = to;
                };
                Scan(Database_, Scratch_, input, callback);

                if (!index) {
                    return args[0];
                }

                result << input.substr(index);
                return valueBuilder->NewString(result);
            }

            return TUnboxedValue();
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }

        TSourcePosition Pos_;
        TString Regex_;
        TDatabase Database_;
        TScratch Scratch_;
    };

    class THyperscanModule: public IUdfModule {
    public:
        TStringRef Name() const {
            return TStringRef::Of("Hyperscan");
        }

        void CleanupOnTerminate() const final {
        }

        void GetAllFunctions(IFunctionsSink& sink) const final {
            sink.Add(THyperscanMatch::Name(true, THyperscanMatch::EMode::NORMAL));
            sink.Add(THyperscanMatch::Name(false, THyperscanMatch::EMode::NORMAL));
            sink.Add(THyperscanMatch::Name(true, THyperscanMatch::EMode::BACKTRACKING));
            sink.Add(THyperscanMatch::Name(false, THyperscanMatch::EMode::BACKTRACKING));
            sink.Add(THyperscanMatch::Name(true, THyperscanMatch::EMode::MULTI))->SetTypeAwareness();
            sink.Add(THyperscanMatch::Name(false, THyperscanMatch::EMode::MULTI))->SetTypeAwareness();
            sink.Add(THyperscanCapture::Name());
            sink.Add(THyperscanReplace::Name());
        }

        void BuildFunctionTypeInfo(
            const TStringRef& name,
            TType* userType,
            const TStringRef& typeConfig,
            ui32 flags,
            IFunctionTypeInfoBuilder& builder) const final {
            try {
                Y_UNUSED(userType);

                bool typesOnly = (flags & TFlags::TypesOnly);
                bool isMatch = (THyperscanMatch::Name(false, THyperscanMatch::EMode::NORMAL) == name);
                bool isGrep = (THyperscanMatch::Name(true, THyperscanMatch::EMode::NORMAL) == name);
                bool isBacktrackingMatch = (THyperscanMatch::Name(false, THyperscanMatch::EMode::BACKTRACKING) == name);
                bool isBacktrackingGrep = (THyperscanMatch::Name(true, THyperscanMatch::EMode::BACKTRACKING) == name);
                bool isMultiMatch = (THyperscanMatch::Name(false, THyperscanMatch::EMode::MULTI) == name);
                bool isMultiGrep = (THyperscanMatch::Name(true, THyperscanMatch::EMode::MULTI) == name);

                if (isMatch || isGrep) {
                    builder.SimpleSignature<bool(TOptional<char*>)>()
                        .RunConfig<const char*>();

                    if (!typesOnly) {
                        builder.Implementation(new THyperscanMatch::TFactory(builder.GetSourcePosition(), isGrep, THyperscanMatch::EMode::NORMAL));
                    }
                } else if (isBacktrackingMatch || isBacktrackingGrep) {
                    builder.SimpleSignature<bool(TOptional<char*>)>()
                        .RunConfig<const char*>();

                    if (!typesOnly) {
                        builder.Implementation(new THyperscanMatch::TFactory(builder.GetSourcePosition(), isBacktrackingGrep, THyperscanMatch::EMode::BACKTRACKING));
                    }
                } else if (isMultiMatch || isMultiGrep) {
                    auto boolType = builder.SimpleType<bool>();
                    auto optionalStringType = builder.Optional()->Item<char*>().Build();
                    const std::string_view regexp(typeConfig);
                    size_t regexpCount = std::count(regexp.begin(), regexp.end(), '\n') + 1;
                    auto tuple = builder.Tuple();
                    for (size_t i = 0; i < regexpCount; ++i) {
                        tuple->Add(boolType);
                    }
                    auto tupleType = tuple->Build();
                    builder.Args(1)->Add(optionalStringType).Done().Returns(tupleType).RunConfig<char*>();

                    if (!typesOnly) {
                        builder.Implementation(new THyperscanMatch::TFactory(builder.GetSourcePosition(), isMultiGrep, THyperscanMatch::EMode::MULTI, regexpCount));
                    }
                } else if (THyperscanCapture::Name() == name) {
                    builder.SimpleSignature<TOptional<char*>(TOptional<char*>)>()
                            .RunConfig<char*>();

                    if (!typesOnly) {
                        builder.Implementation(new THyperscanCapture::TFactory(builder.GetSourcePosition()));
                    }
                } else if (THyperscanReplace::Name() == name) {
                    builder.SimpleSignature<TOptional<char*>(TOptional<char*>, char*)>()
                        .RunConfig<char*>();

                    if (!typesOnly) {
                        builder.Implementation(new THyperscanReplace::TFactory(builder.GetSourcePosition()));
                    }
                }
            } catch (const std::exception& e) {
                builder.SetError(CurrentExceptionMessage());
            }
        }
    };

    class TPcreModule : public THyperscanModule {
    public:
        TStringRef Name() const {
            return TStringRef::Of("Pcre");
        }
    };
}

REGISTER_MODULES(THyperscanModule, TPcreModule)
