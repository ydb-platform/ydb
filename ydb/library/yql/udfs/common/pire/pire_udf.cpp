#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/public/udf/udf_registrator.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_terminator.h>

#include <library/cpp/regex/pire/regexp.h>
#include <library/cpp/regex/pire/pcre2pire.h>

#include <util/string/builder.h>

using namespace NRegExp;
using namespace NKikimr;
using namespace NUdf;

namespace {
    class TPireUdfBase: public TBoxedValue {
    protected:
        TPireUdfBase(TSourcePosition pos)
            : Pos_(pos)
        {}

        void SetCommonOptions(std::string_view& regex, TFsm::TOptions& options) {
            if (regex.size() >= 4U && regex.substr(0U, 4U) == "(?i)") {
                options.SetCaseInsensitive(true);
                regex.remove_prefix(4U);
            }
            if (UTF8Detect(regex) == UTF8) {
                options.SetCharset(CODES_UTF8);
            }
        }

        TSourcePosition Pos_;
    };

    class TPireMatch: public TPireUdfBase {
    public:
        class TFactory: public TPireUdfBase {
        public:
            TFactory(
                bool surroundMode,
                bool multiMode,
                TSourcePosition pos,
                size_t regexpsCount = 0)
                : TPireUdfBase(pos)
                , SurroundMode(surroundMode)
                , MultiMode(multiMode)
                , RegexpsCount(regexpsCount)
            {
            }

        private:
            TUnboxedValue Run(
                const IValueBuilder* valueBuilder,
                const TUnboxedValuePod* args) const final {
                return TUnboxedValuePod(
                    new TPireMatch(
                        valueBuilder,
                        args[0],
                        SurroundMode,
                        MultiMode,
                        Pos_,
                        RegexpsCount));
            }

            bool SurroundMode;
            bool MultiMode;
            size_t RegexpsCount;
        };

        static const TStringRef& Name(bool surroundMode, bool multiMode) {
            static auto match = TStringRef::Of("Match");
            static auto grep = TStringRef::Of("Grep");
            static auto multiMatch = TStringRef::Of("MultiMatch");
            static auto multiGrep = TStringRef::Of("MultiGrep");
            if (surroundMode) {
                return multiMode ? multiGrep : grep;
            } else {
                return multiMode ? multiMatch : match;
            }
        }

        TPireMatch(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod& runConfig,
            bool surroundMode,
            bool multiMode,
            TSourcePosition pos,
            size_t regexpsCount)
            : TPireUdfBase(pos)
            , MultiMode(multiMode)
            , RegexpsCount(regexpsCount)
            , SurroundMode(surroundMode)
        {
            Y_UNUSED(valueBuilder);
            try {
                std::string_view regex(runConfig.AsStringRef());
                TFsm::TOptions options;
                options.SetSurround(surroundMode);
                SetCommonOptions(regex, options);
                if (multiMode) {
                    std::vector<std::string_view> parts;
                    StringSplitter(regex).Split('\n').AddTo(&parts);
                    for (const auto& part : parts) {
                        if (!part.empty()) {
                            if (Fsm_) try {
                                *Fsm_ = *Fsm_ | TFsm(TString(part), options);
                            } catch (const yexception&) {
                                UdfTerminate((TStringBuilder() << Pos_ << " Failed to glue up regexes, probably the finite state machine appeared to be too large").data());
                            } else {
                                Fsm_.Reset(new TFsm(TString(part), options));
                            }
                        }
                    }
                } else {
                    Fsm_.Reset(new TFsm(TString(regex), options));
                }
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

            if (MultiMode) {
                tuple = valueBuilder->NewArray(RegexpsCount, items);

                for (i = 0; i < RegexpsCount; ++i) {
                    items[i] = TUnboxedValuePod(false);
                }
            }

            if (args[0]) {
                const auto input = args[0].AsStringRef();
                TMatcher matcher(*Fsm_);
                const bool isMatch = matcher.Match(input.Data(), input.Size(), SurroundMode, SurroundMode).Final();
                if (MultiMode) {
                    if (isMatch) {
                        const auto& matchedRegexps = matcher.MatchedRegexps();
                        size_t matchesCount = matchedRegexps.second - matchedRegexps.first;

                        for (i = 0; i < matchesCount; ++i) {
                            items[matchedRegexps.first[i]] = TUnboxedValuePod(true);
                        }
                    }
                    return tuple;

                } else {
                    return TUnboxedValuePod(isMatch);
                }

            } else {
                return MultiMode ? tuple : TUnboxedValue(TUnboxedValuePod(false));
            }
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }

    private:
        TUniquePtr<TFsm> Fsm_;
        bool MultiMode;
        size_t RegexpsCount;
        bool SurroundMode;
    };

    class TPireCapture: public TPireUdfBase {
    public:
        class TFactory: public TPireUdfBase {
        public:
            TFactory(TSourcePosition pos)
                : TPireUdfBase(pos)
            {}

        private:
            TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const final try {
                return TUnboxedValuePod(new TPireCapture(args[0], Pos_));
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        };

        static const TStringRef& Name() {
            static auto name = TStringRef::Of("Capture");
            return name;
        }

        TPireCapture(const TUnboxedValuePod& runConfig, TSourcePosition pos)
            : TPireUdfBase(pos)
        {
            std::string_view regex(runConfig.AsStringRef());
            TFsm::TOptions options;
            SetCommonOptions(regex, options);
            Fsm_.Reset(new TSlowCapturingFsm(TString(regex), options));
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const final try {
            if (args[0]) {
                const std::string_view input = args[0].AsStringRef();

                TSlowSearcher searcher(*Fsm_);
                searcher.Search(input.data(), input.size());

                if (searcher.Captured()) {
                    const auto& captured = searcher.GetCaptured();
                    return valueBuilder->SubString(args[0], std::distance(input.begin(), captured.begin()), captured.length());
                }
            }

            return TUnboxedValue();
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }

        TUniquePtr<TSlowCapturingFsm> Fsm_;
    };

    class TPireReplace: public TPireUdfBase {
    public:
        class TFactory: public TPireUdfBase {
        public:
            TFactory(TSourcePosition pos)
                : TPireUdfBase(pos)
            {}

        private:
            TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const final try {
                return TUnboxedValuePod(new TPireReplace(args[0], Pos_));
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        };

        static const TStringRef& Name() {
            static auto name = TStringRef::Of("Replace");
            return name;
        }

        TPireReplace(const TUnboxedValuePod& runConfig, TSourcePosition pos)
            : TPireUdfBase(pos)
        {
            std::string_view regex(runConfig.AsStringRef());
            TFsm::TOptions options;
            SetCommonOptions(regex, options);
            Fsm_.Reset(new TSlowCapturingFsm(TString(regex), options));
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const final try {
            if (args[0]) {
                const std::string_view input(args[0].AsStringRef());

                TSlowSearcher s(*Fsm_);
                s.Search(input.data(), input.size());
                if (s.Captured()) {
                    const auto& captured = s.GetCaptured();
                    const TString replacement(args[1].AsStringRef());
                    TString replaced(args[0].AsStringRef());
                    replaced.replace(std::distance(input.begin(), captured.begin()), captured.length(), replacement);
                    return valueBuilder->NewString(replaced);
                } else {
                    return TUnboxedValue(args[0]);
                }
            } else {
                return TUnboxedValue();
            }
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }

        TUniquePtr<TSlowCapturingFsm> Fsm_;
    };

    class TPireModule: public IUdfModule {
    public:
        TStringRef Name() const {
            return TStringRef::Of("Pire");
        }

        void CleanupOnTerminate() const final {
        }

        void GetAllFunctions(IFunctionsSink& sink) const final {
            sink.Add(TPireMatch::Name(true, true))->SetTypeAwareness();
            sink.Add(TPireMatch::Name(false, true))->SetTypeAwareness();
            sink.Add(TPireMatch::Name(true, false));
            sink.Add(TPireMatch::Name(false, false));
            sink.Add(TPireCapture::Name());
            sink.Add(TPireReplace::Name());
        }

        void BuildFunctionTypeInfo(
            const TStringRef& name,
            TType*,
            const TStringRef& typeConfig,
            ui32 flags,
            IFunctionTypeInfoBuilder& builder) const final try {
            const bool typesOnly = (flags & TFlags::TypesOnly);
            const bool isMatch = (TPireMatch::Name(false, false) == name);
            const bool isGrep = (TPireMatch::Name(true, false) == name);
            const bool isMultiMatch = (TPireMatch::Name(false, true) == name);
            const bool isMultiGrep = (TPireMatch::Name(true, true) == name);

            if (isMatch || isGrep) {
                builder.SimpleSignature<bool(TOptional<char*>)>()
                    .RunConfig<const char*>();

                if (!typesOnly) {
                    builder.Implementation(new TPireMatch::TFactory(isGrep, false, builder.GetSourcePosition()));
                }
            } else if (isMultiMatch || isMultiGrep) {
                const auto boolType = builder.SimpleType<bool>();
                const auto optionalStringType = builder.Optional()->Item<char*>().Build();
                const std::string_view regexp(typeConfig);
                const size_t regexpCount = std::count(regexp.begin(), regexp.end(), '\n') + 1;
                const auto tuple = builder.Tuple();
                for (size_t i = 0; i < regexpCount; ++i) {
                    tuple->Add(boolType);
                }
                const auto tupleType = tuple->Build();
                builder.Args(1)->Add(optionalStringType).Done().Returns(tupleType).RunConfig<char*>();

                if (!typesOnly) {
                    builder.Implementation(new TPireMatch::TFactory(isMultiGrep, true, builder.GetSourcePosition(), regexpCount));
                }
            } else if (TPireCapture::Name() == name) {
                builder.SimpleSignature<TOptional<char*>(TOptional<char*>)>()
                    .RunConfig<char*>();

                if (!typesOnly) {
                    builder.Implementation(new TPireCapture::TFactory(builder.GetSourcePosition()));
                }
            } else if (TPireReplace::Name() == name) {
                builder.SimpleSignature<TOptional<char*>(TOptional<char*>, char*)>()
                    .RunConfig<char*>();

                if (!typesOnly) {
                    builder.Implementation(new TPireReplace::TFactory(builder.GetSourcePosition()));
                }
            }
        } catch (const std::exception& e) {
            builder.SetError(CurrentExceptionMessage());
        }
    };

}

REGISTER_MODULES(TPireModule)
