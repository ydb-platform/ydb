#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/public/udf/tz/udf_tz.h>
#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/minikql/datetime/datetime.h>
#include <yql/essentials/minikql/datetime/datetime64.h>

#include <yql/essentials/public/udf/arrow/udf_arrow_helpers.h>

#include <util/datetime/base.h>

using namespace NKikimr;
using namespace NUdf;
using namespace NYql::DateTime;

extern const char SplitUDF[] = "Split";
extern const char ToDaysUDF[] = "ToDays";
extern const char ToHoursUDF[] = "ToHours";
extern const char ToMinutesUDF[] = "ToMinutes";
extern const char ToSecondsUDF[] = "ToSeconds";
extern const char ToMillisecondsUDF[] = "ToMilliseconds";
extern const char ToMicrosecondsUDF[] = "ToMicroseconds";
extern const char GetYearUDF[] = "GetYear";
extern const char GetDayOfYearUDF[] = "GetDayOfYear";
extern const char GetMonthUDF[] = "GetMonth";
extern const char GetMonthNameUDF[] = "GetMonthName";
extern const char GetWeekOfYearUDF[] = "GetWeekOfYear";
extern const char GetWeekOfYearIso8601UDF[] = "GetWeekOfYearIso8601";
extern const char GetDayOfMonthUDF[] = "GetDayOfMonth";
extern const char GetDayOfWeekUDF[] = "GetDayOfWeek";
extern const char GetDayOfWeekNameUDF[] = "GetDayOfWeekName";
extern const char GetTimezoneIdUDF[] = "GetTimezoneId";
extern const char GetTimezoneNameUDF[] = "GetTimezoneName";
extern const char GetHourUDF[] = "GetHour";
extern const char GetMinuteUDF[] = "GetMinute";
extern const char GetSecondUDF[] = "GetSecond";
extern const char GetMillisecondOfSecondUDF[] = "GetMillisecondOfSecond";
extern const char GetMicrosecondOfSecondUDF[] = "GetMicrosecondOfSecond";
extern const char StartOfYearUDF[] = "StartOfYear";
extern const char StartOfQuarterUDF[] = "StartOfQuarter";
extern const char StartOfMonthUDF[] = "StartOfMonth";
extern const char StartOfWeekUDF[] = "StartOfWeek";
extern const char StartOfDayUDF[] = "StartOfDay";
extern const char StartOfUDF[] = "StartOf";
extern const char EndOfYearUDF[] = "EndOfYear";
extern const char EndOfQuarterUDF[] = "EndOfQuarter";
extern const char EndOfMonthUDF[] = "EndOfMonth";
extern const char EndOfWeekUDF[] = "EndOfWeek";
extern const char EndOfDayUDF[] = "EndOfDay";
extern const char EndOfUDF[] = "EndOf";
extern const char ShiftYearsUDF[] = "ShiftYears";
extern const char ShiftQuartersUDF[] = "ShiftQuarters";
extern const char ShiftMonthsUDF[] = "ShiftMonths";
extern const char ParseUDF[] = "Parse";
extern const char Parse64UDF[] = "Parse64";

extern const char TMResourceName[] = "DateTime2.TM";
extern const char TM64ResourceName[] = "DateTime2.TM64";

namespace {

template<typename Type>
static void PrintTypeAlternatives(NUdf::IFunctionTypeInfoBuilder& builder,
    ITypeInfoHelper::TPtr typeInfoHelper, TStringBuilder& strBuilder)
{
    TTypePrinter(*typeInfoHelper, builder.SimpleType<Type>()).Out(strBuilder.Out);
}

template<typename Type, typename Head, typename... Tail>
static void PrintTypeAlternatives(NUdf::IFunctionTypeInfoBuilder& builder,
    ITypeInfoHelper::TPtr typeInfoHelper, TStringBuilder& strBuilder)
{
    PrintTypeAlternatives<Type>(builder, typeInfoHelper, strBuilder);
    strBuilder << " or ";
    PrintTypeAlternatives<Head, Tail...>(builder, typeInfoHelper, strBuilder);
}

template<typename... Types>
static void SetInvalidTypeError(NUdf::IFunctionTypeInfoBuilder& builder,
    ITypeInfoHelper::TPtr typeInfoHelper, const TType* argType)
{
    ::TStringBuilder sb;
    sb << "Invalid argument type: got ";
    TTypePrinter(*typeInfoHelper, argType).Out(sb.Out);
    sb << ", but ";
    PrintTypeAlternatives<Types...>(builder, typeInfoHelper, sb);
    sb << " expected";
    builder.SetError(sb);
}

static void SetResourceExpectedError(NUdf::IFunctionTypeInfoBuilder& builder,
    ITypeInfoHelper::TPtr typeInfoHelper, const TType* argType)
{
    SetInvalidTypeError<
        TResource<TMResourceName>,
        TResource<TM64ResourceName>
    >(builder, typeInfoHelper, argType);
}

static void SetIntervalExpectedError(NUdf::IFunctionTypeInfoBuilder& builder,
    ITypeInfoHelper::TPtr typeInfoHelper, const TType* argType)
{
    SetInvalidTypeError<TInterval, TInterval64>(builder, typeInfoHelper, argType);
}

template<const char* TResourceName>
static void PrintTagAlternatives(TStringBuilder& strBuilder) {
    strBuilder << "'" << TResourceName << "'";
}

template<const char* TResourceName, const char* Head, const char*... Tail>
static void PrintTagAlternatives(TStringBuilder& strBuilder) {
    PrintTagAlternatives<TResourceName>(strBuilder);
    strBuilder << " or ";
    PrintTagAlternatives<Head, Tail...>(strBuilder);
}

static void SetUnexpectedTagError(NUdf::IFunctionTypeInfoBuilder& builder,
    TStringRef tag)
{
    ::TStringBuilder sb;
    sb << "Unexpected Resource tag: got '" << tag << "', but ";
    PrintTagAlternatives<TMResourceName, TM64ResourceName>(sb);
    sb << " expected";
    builder.SetError(sb);
}

} // namespace

const auto UsecondsInDay = 86400000000ll;
const auto UsecondsInHour = 3600000000ll;
const auto UsecondsInMinute = 60000000ll;
const auto UsecondsInSecond = 1000000ll;
const auto UsecondsInMilliseconds = 1000ll;

template <const char* TFuncName, typename TResult, typename TWResult, ui32 ScaleAfterSeconds>
class TToUnits {
public:
    typedef bool TTypeAwareMarker;
    using TSignedResult = typename std::make_signed<TResult>::type;

    static TResult DateCore(ui16 value) {
        return value * ui32(86400) * TResult(ScaleAfterSeconds);
    }

    static TWResult Date32Core(i32 value) {
        return value * i64(86400) * TWResult(ScaleAfterSeconds);
    }

    template<typename TTzDate>
    static TResult TzBlockCore(TBlockItem tzDate);

    template<>
    static TResult TzBlockCore<TTzDate>(TBlockItem tzDate) {
        return DateCore(tzDate.Get<ui16>());
    }

    template<>
    static TResult TzBlockCore<TTzDatetime>(TBlockItem tzDate) {
        return DatetimeCore(tzDate.Get<ui32>());
    }

    template<>
    static TResult TzBlockCore<TTzTimestamp>(TBlockItem tzDate) {
        return TimestampCore(tzDate.Get<ui64>());
    }

    static TResult DatetimeCore(ui32 value) {
        return value * TResult(ScaleAfterSeconds);
    }

    static TWResult Datetime64Core(i64 value) {
        return value * TWResult(ScaleAfterSeconds);
    }

    static TResult TimestampCore(ui64 value) {
        return TResult(value / (1000000u / ScaleAfterSeconds));
    }

    static TWResult Timestamp64Core(i64 value) {
        return TWResult(value / (1000000u / ScaleAfterSeconds));
    }

    static TSignedResult IntervalCore(i64 value) {
        return TSignedResult(value / (1000000u / ScaleAfterSeconds));
    }

    static TWResult Interval64Core(i64 value) {
        return TWResult(value / (1000000u / ScaleAfterSeconds));
    }

    static const TStringRef& Name() {
        static auto name = TStringRef(TFuncName, std::strlen(TFuncName));
        return name;
    }

    template<typename TTzDate, typename TOutput>
    static auto MakeTzBlockExec() {
        using TReader = TTzDateBlockReader<TTzDate, /*Nullable*/ false>;
        return UnaryPreallocatedReaderExecImpl<TReader, TOutput, TzBlockCore<TTzDate>>;
    }

    static bool DeclareSignature(
        const TStringRef& name,
        TType* userType,
        IFunctionTypeInfoBuilder& builder,
        bool typesOnly)
    {
        if (Name() != name) {
            return false;
        }

        if (!userType) {
            builder.SetError("User type is missing");
            return true;
        }

        builder.UserType(userType);

        const auto typeInfoHelper = builder.TypeInfoHelper();
        TTupleTypeInspector tuple(*typeInfoHelper, userType);
        Y_ENSURE(tuple, "Tuple with args and options tuples expected");
        Y_ENSURE(tuple.GetElementsCount() > 0,
                 "Tuple has to contain positional arguments");

        TTupleTypeInspector argsTuple(*typeInfoHelper, tuple.GetElementType(0));
        Y_ENSURE(argsTuple, "Tuple with args expected");
        if (argsTuple.GetElementsCount() != 1) {
            builder.SetError("Single argument expected");
            return true;
        }

        auto argType = argsTuple.GetElementType(0);

        TVector<const TType*> argBlockTypes;
        argBlockTypes.push_back(argType);

        TBlockTypeInspector block(*typeInfoHelper, argType);
        if (block) {
            Y_ENSURE(!block.IsScalar());
            argType = block.GetItemType();
        }

        bool isOptional = false;
        if (auto opt = TOptionalTypeInspector(*typeInfoHelper, argType)) {
            argType = opt.GetItemType();
            isOptional = true;
        }


        TDataTypeInspector data(*typeInfoHelper, argType);
        if (!data) {
            builder.SetError("Data type expected");
            return true;
        }

        const auto typeId = data.GetTypeId();
        const auto features = NUdf::GetDataTypeInfo(NUdf::GetDataSlot(typeId)).Features;

        if (!(features & (NUdf::DateType | NUdf::TzDateType | NUdf::TimeIntervalType))) {
            builder.SetError(TStringBuilder()
                             << "Type "
                             << GetDataTypeInfo(GetDataSlot(typeId)).Name
                             << " is not supported");
            return true;
        }

        builder.Args()->Add(argsTuple.GetElementType(0));
        const TType* retType;
        if (features & NUdf::BigDateType) {
            retType = builder.SimpleType<TWResult>();
        } else if (features & NUdf::TimeIntervalType) {
            retType = builder.SimpleType<TSignedResult>();
        } else {
            retType = builder.SimpleType<TResult>();
        }

        if (isOptional) {
            retType = builder.Optional()->Item(retType).Build();
        }

        auto outputType = retType;
        if (block) {
            retType = builder.Block(block.IsScalar())->Item(retType).Build();
        }

        builder.Returns(retType);
        if (!(features & NUdf::BigDateType)) {
            // FIXME: Only non-wide overloads support block rewrite now.
            builder.SupportsBlocks();
        }
        builder.IsStrict();

        if (!typesOnly) {
            if (typeId == TDataType<TDate>::Id || typeId == TDataType<TTzDate>::Id) {
                if (block) {
                    const auto exec = (typeId == TDataType<TTzDate>::Id)
                        ? MakeTzBlockExec<TTzDate, TResult>()
                        : UnaryPreallocatedExecImpl<ui16, TResult, DateCore>;

                    builder.Implementation(new TSimpleArrowUdfImpl(argBlockTypes, outputType, block.IsScalar(),
                        exec, builder, TString(name), arrow::compute::NullHandling::INTERSECTION));
                } else {
                    builder.Implementation(new TUnaryOverOptionalImpl<ui16, TResult, DateCore>());
                }
                return true;
            }

            if (typeId == TDataType<TDatetime>::Id || typeId == TDataType<TTzDatetime>::Id) {
                if (block) {
                    const auto exec = (typeId == TDataType<TTzDatetime>::Id)
                        ? MakeTzBlockExec<TTzDatetime, TResult>()
                        : UnaryPreallocatedExecImpl<ui32, TResult, DatetimeCore>;

                    builder.Implementation(new TSimpleArrowUdfImpl(argBlockTypes, outputType, block.IsScalar(),
                        exec, builder, TString(name), arrow::compute::NullHandling::INTERSECTION));
                } else {
                    builder.Implementation(new TUnaryOverOptionalImpl<ui32, TResult, DatetimeCore>());
                }
                return true;
            }

            if (typeId == TDataType<TTimestamp>::Id || typeId == TDataType<TTzTimestamp>::Id) {
                if (block) {
                    const auto exec = (typeId == TDataType<TTzTimestamp>::Id)
                        ? MakeTzBlockExec<TTzTimestamp, TResult>()
                        : UnaryPreallocatedExecImpl<ui64, TResult, TimestampCore>;

                    builder.Implementation(new TSimpleArrowUdfImpl(argBlockTypes, outputType, block.IsScalar(),
                        exec, builder, TString(name), arrow::compute::NullHandling::INTERSECTION));
                } else {
                    builder.Implementation(new TUnaryOverOptionalImpl<ui64, TResult, TimestampCore>());
                }
                return true;
            }

            if (typeId == TDataType<TInterval>::Id) {
                if (block) {
                    builder.Implementation(new TSimpleArrowUdfImpl(argBlockTypes, outputType, block.IsScalar(),
                        UnaryPreallocatedExecImpl<i64, TSignedResult, IntervalCore>, builder, TString(name), arrow::compute::NullHandling::INTERSECTION));
                } else {
                    builder.Implementation(new TUnaryOverOptionalImpl<i64, TSignedResult, IntervalCore>());
                }
                return true;
            }

            if (typeId == TDataType<TDate32>::Id || typeId == TDataType<TTzDate32>::Id) {
                Y_ABORT_IF(block, "Block overload is not supported");
                builder.Implementation(new TUnaryOverOptionalImpl<i32, TWResult, Date32Core>());
                return true;
            }

            if (typeId == TDataType<TDatetime64>::Id || typeId == TDataType<TTzDatetime64>::Id) {
                Y_ABORT_IF(block, "Block overload is not supported");
                builder.Implementation(new TUnaryOverOptionalImpl<i64, TWResult, Datetime64Core>());
                return true;
            }

            if (typeId == TDataType<TTimestamp64>::Id || typeId == TDataType<TTzTimestamp64>::Id) {
                Y_ABORT_IF(block, "Block overload is not supported");
                builder.Implementation(new TUnaryOverOptionalImpl<i64, TWResult, Timestamp64Core>());
                return true;
            }

            if (typeId == TDataType<TInterval64>::Id) {
                Y_ABORT_IF(block, "Block overload is not supported");
                builder.Implementation(new TUnaryOverOptionalImpl<i64, TWResult, Interval64Core>());
                return true;
            }

            Y_UNREACHABLE();
        }
        return true;
    }
};

template <const char* TFuncName, typename TFieldStorage,
          TFieldStorage (*Accessor)(const TUnboxedValuePod&),
          TFieldStorage (*WAccessor)(const TUnboxedValuePod&),
          ui32 Divisor, ui32 Scale, ui32 Limit, bool Fractional>
struct TGetTimeComponent {
    typedef bool TTypeAwareMarker;

    static const TStringRef& Name() {
        static auto name = TStringRef(TFuncName, std::strlen(TFuncName));
        return name;
    }

    static bool DeclareSignature(
        const TStringRef& name,
        TType* userType,
        IFunctionTypeInfoBuilder& builder,
        bool typesOnly)
    {
        if (Name() != name) {
            return false;
        }

        if (!userType) {
            // XXX: Function became polymorphic when overload for
            // wide resources was implemented. Hence, to make it
            // backward compatible with previous versions, the
            // absence of the userType is considered as using the
            // old version (i.e. without type awareness) that
            // provides implementation only for narrow dates.
            BuildSignature<TFieldStorage, TMResourceName, Accessor>(builder, typesOnly);
            return true;
        }

        builder.UserType(userType);

        const auto typeInfoHelper = builder.TypeInfoHelper();
        TTupleTypeInspector tuple(*typeInfoHelper, userType);
        Y_ENSURE(tuple, "Tuple with args and options tuples expected");
        Y_ENSURE(tuple.GetElementsCount() > 0,
                 "Tuple has to contain positional arguments");

        TTupleTypeInspector argsTuple(*typeInfoHelper, tuple.GetElementType(0));
        Y_ENSURE(argsTuple, "Tuple with args expected");
        if (argsTuple.GetElementsCount() != 1) {
            builder.SetError("Single argument expected");
            return true;
        }

        auto argType = argsTuple.GetElementType(0);

        TVector<const TType*> argBlockTypes;
        argBlockTypes.push_back(argType);

        TBlockTypeInspector block(*typeInfoHelper, argType);
        if (block) {
            Y_ENSURE(!block.IsScalar());
            argType = block.GetItemType();
        }

        bool isOptional = false;
        if (auto opt = TOptionalTypeInspector(*typeInfoHelper, argType)) {
            argType = opt.GetItemType();
            isOptional = true;
        }

        TResourceTypeInspector resource(*typeInfoHelper, argType);
        if (!resource) {
            TDataTypeInspector data(*typeInfoHelper, argType);
            if (!data) {
                builder.SetError("Data type expected");
                return true;
            }

            const auto features = NUdf::GetDataTypeInfo(NUdf::GetDataSlot(data.GetTypeId())).Features;
            if (features & NUdf::BigDateType) {
                BuildSignature<TFieldStorage, TM64ResourceName, WAccessor>(builder, typesOnly);
                return true;
            }
            if (features & NUdf::TzDateType) {
                BuildSignature<TFieldStorage, TMResourceName, Accessor>(builder, typesOnly);
                return true;
            }

            if (features & NUdf::DateType) {
                builder.Args()->Add(argsTuple.GetElementType(0)).Done();
                const TType* retType = builder.SimpleType<TFieldStorage>();

                if (isOptional) {
                    retType = builder.Optional()->Item(retType).Build();
                }

                auto outputType = retType;
                if (block) {
                    retType = builder.Block(block.IsScalar())->Item(retType).Build();
                }

                builder.Returns(retType);
                builder.SupportsBlocks();
                builder.IsStrict();

                if (!typesOnly) {
                    const auto typeId = data.GetTypeId();
                    if (typeId == TDataType<TDate>::Id) {
                        if (block) {
                            builder.Implementation(new TSimpleArrowUdfImpl(argBlockTypes, outputType, block.IsScalar(),
                                UnaryPreallocatedExecImpl<ui16, TFieldStorage, Core<ui16, true, false>>, builder, TString(name), arrow::compute::NullHandling::INTERSECTION));
                        } else {
                            builder.Implementation(new TUnaryOverOptionalImpl<ui16, TFieldStorage, Core<ui16, true, false>>());
                        }
                    }

                    if (typeId == TDataType<TDatetime>::Id) {
                        if (block) {
                            builder.Implementation(new TSimpleArrowUdfImpl(argBlockTypes, outputType, block.IsScalar(),
                                UnaryPreallocatedExecImpl<ui32, TFieldStorage, Core<ui32, false, false>>, builder, TString(name), arrow::compute::NullHandling::INTERSECTION));
                        } else {
                            builder.Implementation(new TUnaryOverOptionalImpl<ui32, TFieldStorage, Core<ui32, false, false>>());
                        }
                    }

                    if (typeId == TDataType<TTimestamp>::Id) {
                        if (block) {
                            builder.Implementation(new TSimpleArrowUdfImpl(argBlockTypes, outputType, block.IsScalar(),
                                UnaryPreallocatedExecImpl<ui64, TFieldStorage, Core<ui64, false, true>>, builder, TString(name), arrow::compute::NullHandling::INTERSECTION));
                        } else {
                            builder.Implementation(new TUnaryOverOptionalImpl<ui64, TFieldStorage, Core<ui64, false, true>>());
                        }
                    }
                }
                return true;
            }

            SetResourceExpectedError(builder, typeInfoHelper, argType);
            return true;
        }

        Y_ENSURE(!block);

        if (resource.GetTag() == TStringRef::Of(TM64ResourceName)) {
            BuildSignature<TFieldStorage, TM64ResourceName, WAccessor>(builder, typesOnly);
            return true;
        }

        if (resource.GetTag() == TStringRef::Of(TMResourceName)) {
            BuildSignature<TFieldStorage, TMResourceName, Accessor>(builder, typesOnly);
            return true;
        }

        SetUnexpectedTagError(builder, resource.GetTag());
        return true;
    }
private:
    template <typename TInput, bool AlwaysZero, bool InputFractional>
    static TFieldStorage Core(TInput val) {
        if constexpr (AlwaysZero) {
            return 0;
        }

        if constexpr (InputFractional) {
            if constexpr (Fractional) {
                return (val / Scale) % Limit;
            } else {
                return (val / 1000000u / Scale) % Limit;
            }
        } else {
            if constexpr (Fractional) {
                return 0;
            } else {
                return (val / Scale) % Limit;
            }
        }
    }

    template<typename TResult, TResult (*Func)(const TUnboxedValuePod&)>
    class TImpl : public TBoxedValue {
    public:
        TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
            Y_UNUSED(valueBuilder);
            EMPTY_RESULT_ON_EMPTY_ARG(0);
            return TUnboxedValuePod((TResult(Func(args[0])) / Divisor));
        }
    };

    template<typename TResult, const char* TResourceName, TResult (*Func)(const TUnboxedValuePod&)>
    static void BuildSignature(NUdf::IFunctionTypeInfoBuilder& builder, bool typesOnly) {
        builder.Returns<TResult>();
        builder.Args()->Add<TAutoMap<TResource<TResourceName>>>();
        builder.IsStrict();
        if (!typesOnly) {
            builder.Implementation(new TImpl<TResult, Func>());
        }
    }
};

namespace {

template<const char* TResourceName, typename TValue,
         typename TStorage = std::conditional_t<TResourceName == TMResourceName,
                                                TTMStorage, TTM64Storage>>
const TStorage& Reference(const TValue& value) {
    return *reinterpret_cast<const TStorage*>(value.GetRawPtr());
}

template<const char* TResourceName, typename TValue,
         typename TStorage = std::conditional_t<TResourceName == TMResourceName,
                                                TTMStorage, TTM64Storage>>
TStorage& Reference(TValue& value) {
    return *reinterpret_cast<TStorage*>(value.GetRawPtr());
}

template<const char* TResourceName>
TUnboxedValuePod DoAddMonths(const TUnboxedValuePod& date, i64 months, const NUdf::IDateBuilder& builder) {
    auto result = date;
    auto& storage = Reference<TResourceName>(result);
    if (!NYql::DateTime::DoAddMonths(storage, months, builder)) {
        return TUnboxedValuePod{};
    }
    return result;
}

template<const char* TResourceName>
TUnboxedValuePod DoAddQuarters(const TUnboxedValuePod& date, i64 quarters, const NUdf::IDateBuilder& builder) {
    return DoAddMonths<TResourceName>(date, quarters * 3ll, builder);
}

template<const char* TResourceName>
TUnboxedValuePod DoAddYears(const TUnboxedValuePod& date, i64 years, const NUdf::IDateBuilder& builder) {
    auto result = date;
    auto& storage = Reference<TResourceName>(result);
    if (!NYql::DateTime::DoAddYears(storage, years, builder)) {
        return TUnboxedValuePod{};
    }
    return result;
}

#define ACCESSORS_POLY(field, type, wtype)                                        \
    template<const char* TResourceName, typename TValue, typename TRetType        \
             = std::conditional_t<TResourceName == TMResourceName, type, wtype>>  \
    inline TRetType Get##field(const TValue& tm) {                                \
        return (TRetType)Reference<TResourceName>(tm).field;                      \
    }                                                                             \
    template<const char* TResourceName, typename TValue, typename TArgType        \
             = std::conditional_t<TResourceName == TMResourceName, type, wtype>>  \
    inline void Set##field(TValue& tm, TArgType value) {                          \
        Reference<TResourceName>(tm).field = value;                               \
    }                                                                             \

#define ACCESSORS(field, type) \
    ACCESSORS_POLY(field, type, type)

    ACCESSORS_POLY(Year, ui16, i32)
    ACCESSORS(DayOfYear, ui16)
    ACCESSORS(WeekOfYear, ui8)
    ACCESSORS(WeekOfYearIso8601, ui8)
    ACCESSORS(DayOfWeek, ui8)
    ACCESSORS(Month, ui8)
    ACCESSORS(Day, ui8)
    ACCESSORS(Hour, ui8)
    ACCESSORS(Minute, ui8)
    ACCESSORS(Second, ui8)
    ACCESSORS(Microsecond, ui32)
    ACCESSORS(TimezoneId, ui16)

#undef ACCESSORS
#undef ACCESSORS_POLY

    template<const char* TResourceName>
    inline bool ValidateYear(std::conditional_t<TResourceName == TMResourceName, ui16, i32> year) {
        if constexpr (TResourceName == TMResourceName) {
            return year >= NUdf::MIN_YEAR || year < NUdf::MAX_YEAR;
        } else {
            return year >= NUdf::MIN_YEAR32 || year < NUdf::MAX_YEAR32;
        }
    }

    inline bool ValidateMonth(ui8 month) {
        return month >= 1 && month <= 12;
    }

    inline bool ValidateDay(ui8 day) {
        return day >= 1 && day <= 31;
    }

    inline bool ValidateHour(ui8 hour) {
        return hour < 24;
    }

    inline bool ValidateMinute(ui8 minute) {
        return minute < 60;
    }

    inline bool ValidateSecond(ui8 second) {
        return second < 60;
    }

    inline bool ValidateMicrosecond(ui32 microsecond) {
        return microsecond < 1000000;
    }

    inline bool ValidateTimezoneId(ui16 timezoneId) {
        const auto& zones = NUdf::GetTimezones();
        return timezoneId < zones.size() && !zones[timezoneId].empty();
    }

    inline bool ValidateMonthShortName(const std::string_view& monthName, ui8& month) {
        static constexpr auto cmp = [](const std::string_view& a, const std::string_view& b) {
            int cmp = strnicmp(a.data(), b.data(), std::min(a.size(), b.size()));
            if (cmp == 0)
                return a.size() < b.size();
            return cmp < 0;
        };
        static const std::map<std::string_view, ui8, decltype(cmp)> mp = {
            {"jan", 1},
            {"feb", 2},
            {"mar", 3},
            {"apr", 4},
            {"may", 5},
            {"jun", 6},
            {"jul", 7},
            {"aug", 8},
            {"sep", 9},
            {"oct", 10},
            {"nov", 11},
            {"dec", 12}
        };
        const auto& it = mp.find(monthName);
        if (it != mp.end()) {
            month = it -> second;
            return true;
        }
        return false;
    }

    inline bool ValidateMonthFullName(const std::string_view& monthName, ui8& month) {
        static constexpr auto cmp = [](const std::string_view& a, const std::string_view& b) {
            int cmp = strnicmp(a.data(), b.data(), std::min(a.size(), b.size()));
            if (cmp == 0)
                return a.size() < b.size();
            return cmp < 0;
        };
        static const std::map<std::string_view, ui8, decltype(cmp)> mp = {
            {"january", 1},
            {"february", 2},
            {"march", 3},
            {"april", 4},
            {"may", 5},
            {"june", 6},
            {"july", 7},
            {"august", 8},
            {"september", 9},
            {"october", 10},
            {"november", 11},
            {"december", 12}
        };
        const auto& it = mp.find(monthName);
        if (it != mp.end()) {
            month = it -> second;
            return true;
        }
        return false;
    }

    template<typename TType>
    inline bool Validate(typename TDataType<TType>::TLayout arg);

    template<>
    inline bool Validate<TTimestamp>(ui64 timestamp) {
        return timestamp < MAX_TIMESTAMP;
    }

    template<>
    inline bool Validate<TTimestamp64>(i64 timestamp) {
        return timestamp >= MIN_TIMESTAMP64 && timestamp <= MAX_TIMESTAMP64;
    }

    template<>
    inline bool Validate<TInterval>(i64 interval) {
        return interval > -i64(MAX_TIMESTAMP) && interval < i64(MAX_TIMESTAMP);
    }

    template<>
    inline bool Validate<TInterval64>(i64 interval) {
        return interval >= -MAX_INTERVAL64 && interval <= MAX_INTERVAL64;
    }

    // Split

    template<typename TUserDataType, bool Nullable>
    using TSplitArgReader = std::conditional_t<TTzDataType<TUserDataType>::Result,
        TTzDateBlockReader<TUserDataType, Nullable>,
        TFixedSizeBlockReader<typename TDataType<TUserDataType>::TLayout, Nullable>>;

    template<typename TUserDataType>
    struct TSplitKernelExec : TUnaryKernelExec<TSplitKernelExec<TUserDataType>, TSplitArgReader<TUserDataType, false>, TResourceArrayBuilder<false>> {
        static void Split(TBlockItem arg, TTMStorage& storage, const IValueBuilder& valueBuilder);

        template<typename TSink>
        static void Process(const IValueBuilder* valueBuilder, TBlockItem arg, const TSink& sink) {
            try {
                TBlockItem res {0};
                Split(arg, Reference<TMResourceName>(res), *valueBuilder);
                sink(res);
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << e.what()).data());
            }
        }
    };

    template <typename TUserDataType>
    class TSplit : public TBoxedValue {
        const TSourcePosition Pos_;

    public:
        explicit TSplit(TSourcePosition pos)
            : Pos_(pos)
        {}

        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override;

        static bool DeclareSignature(
            TStringRef name,
            TType* userType,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly)
        {
            const auto typeInfoHelper = builder.TypeInfoHelper();

            TTupleTypeInspector tuple(*typeInfoHelper, userType);
            Y_ENSURE(tuple);
            Y_ENSURE(tuple.GetElementsCount() > 0);
            TTupleTypeInspector argsTuple(*typeInfoHelper, tuple.GetElementType(0));
            Y_ENSURE(argsTuple);

            if (argsTuple.GetElementsCount() != 1) {
                builder.SetError("Expected one argument");
                return true;
            }
            auto argType = argsTuple.GetElementType(0);

            builder.UserType(userType);
            builder.SupportsBlocks();
            builder.IsStrict();

            TBlockTypeInspector block(*typeInfoHelper, argType);
            if (block) {
                const auto* blockArgType = builder.Block(false)->Item<TUserDataType>().Build();
                builder.Args()->Add(blockArgType).Flags(ICallablePayload::TArgumentFlags::AutoMap);
                const auto* retType = builder.Resource(TMResourceName);
                const auto* blockRetType = builder.Block(false)->Item(retType).Build();
                builder.Returns(blockRetType);

                if (!typesOnly) {
                    builder.Implementation(new TSimpleArrowUdfImpl({blockArgType}, retType, block.IsScalar(),
                            TSplitKernelExec<TUserDataType>::Do, builder, TString(name), arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE));
                }
            } else {
                builder.Args()->Add<TUserDataType>().Flags(ICallablePayload::TArgumentFlags::AutoMap);
                if constexpr (NUdf::TDataType<TUserDataType>::Features & NYql::NUdf::BigDateType) {
                    builder.Returns(builder.Resource(TM64ResourceName));
                } else {
                    builder.Returns(builder.Resource(TMResourceName));
                }

                if (!typesOnly) {
                    builder.Implementation(new TSplit<TUserDataType>(builder.GetSourcePosition()));
                }
            }

            return true;
        }
    };

    template <>
    void TSplitKernelExec<TDate>::Split(TBlockItem arg, TTMStorage &storage, const IValueBuilder& builder) {
        storage.FromDate(builder.GetDateBuilder(), arg.Get<ui16>());
    }

    template <>
    void TSplitKernelExec<TDatetime>::Split(TBlockItem arg, TTMStorage &storage, const IValueBuilder& builder) {
        storage.FromDatetime(builder.GetDateBuilder(), arg.Get<ui32>());
    }

    template <>
    void TSplitKernelExec<TTimestamp>::Split(TBlockItem arg, TTMStorage &storage, const IValueBuilder& builder) {
        storage.FromTimestamp(builder.GetDateBuilder(), arg.Get<ui64>());
    }

    template <>
    void TSplitKernelExec<TTzDate>::Split(TBlockItem arg, TTMStorage &storage, const IValueBuilder& builder) {
        storage.FromDate(builder.GetDateBuilder(), arg.Get<ui16>(), arg.GetTimezoneId());
    }

    template <>
    void TSplitKernelExec<TTzDatetime>::Split(TBlockItem arg, TTMStorage &storage, const IValueBuilder& builder) {
        storage.FromDatetime(builder.GetDateBuilder(), arg.Get<ui32>(), arg.GetTimezoneId());
    }

    template <>
    void TSplitKernelExec<TTzTimestamp>::Split(TBlockItem arg, TTMStorage &storage, const IValueBuilder& builder) {
        storage.FromTimestamp(builder.GetDateBuilder(), arg.Get<ui64>(), arg.GetTimezoneId());
    }

    template <>
    void TSplitKernelExec<TDate32>::Split(TBlockItem, TTMStorage&, const IValueBuilder&) {
        ythrow yexception() << "Not implemented";
    }

    template <>
    void TSplitKernelExec<TDatetime64>::Split(TBlockItem, TTMStorage&, const IValueBuilder&) {
        ythrow yexception() << "Not implemented";
    }

    template <>
    void TSplitKernelExec<TTimestamp64>::Split(TBlockItem, TTMStorage&, const IValueBuilder&) {
        ythrow yexception() << "Not implemented";
    }

    template <>
    void TSplitKernelExec<TTzDate32>::Split(TBlockItem, TTMStorage&, const IValueBuilder&) {
        ythrow yexception() << "Not implemented";
    }

    template <>
    void TSplitKernelExec<TTzDatetime64>::Split(TBlockItem, TTMStorage&, const IValueBuilder&) {
        ythrow yexception() << "Not implemented";
    }

    template <>
    void TSplitKernelExec<TTzTimestamp64>::Split(TBlockItem, TTMStorage&, const IValueBuilder&) {
        ythrow yexception() << "Not implemented";
    }

    template <>
    TUnboxedValue TSplit<TDate>::Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const
    {
        try {
            EMPTY_RESULT_ON_EMPTY_ARG(0);

            auto& builder = valueBuilder->GetDateBuilder();
            TUnboxedValuePod result(0);
            auto& storage = Reference<TMResourceName>(result);
            storage.FromDate(builder, args[0].Get<ui16>());
            return result;
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }
    }

    template <>
    TUnboxedValue TSplit<TDate32>::Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const
    {
        try {
            EMPTY_RESULT_ON_EMPTY_ARG(0);

            TUnboxedValuePod result(0);
            auto& storage = Reference<TM64ResourceName>(result);
            storage.FromDate32(valueBuilder->GetDateBuilder(), args[0].Get<i32>());
            return result;
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }
    }

    template <>
    TUnboxedValue TSplit<TDatetime>::Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const
    {
        try {
            EMPTY_RESULT_ON_EMPTY_ARG(0);

            auto& builder = valueBuilder->GetDateBuilder();
            TUnboxedValuePod result(0);
            auto& storage = Reference<TMResourceName>(result);
            storage.FromDatetime(builder, args[0].Get<ui32>());
            return result;
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }
    }

    template <>
    TUnboxedValue TSplit<TDatetime64>::Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const
    {
        try {
            EMPTY_RESULT_ON_EMPTY_ARG(0);

            TUnboxedValuePod result(0);
            auto& storage = Reference<TM64ResourceName>(result);
            storage.FromDatetime64(valueBuilder->GetDateBuilder(), args[0].Get<i64>());
            return result;
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }
    }

    template <>
    TUnboxedValue TSplit<TTimestamp>::Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const
    {
        try {
            EMPTY_RESULT_ON_EMPTY_ARG(0);

            auto& builder = valueBuilder->GetDateBuilder();
            TUnboxedValuePod result(0);
            auto& storage = Reference<TMResourceName>(result);
            storage.FromTimestamp(builder, args[0].Get<ui64>());
            return result;
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }
    }

    template <>
    TUnboxedValue TSplit<TTimestamp64>::Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const
    {
        try {
            EMPTY_RESULT_ON_EMPTY_ARG(0);

            TUnboxedValuePod result(0);
            auto& storage = Reference<TM64ResourceName>(result);
            storage.FromTimestamp64(valueBuilder->GetDateBuilder(), args[0].Get<i64>());
            return result;
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }
    }

    template <>
    TUnboxedValue TSplit<TTzDate>::Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const
    {
        try {
            EMPTY_RESULT_ON_EMPTY_ARG(0);

            auto& builder = valueBuilder->GetDateBuilder();
            TUnboxedValuePod result(0);
            auto& storage = Reference<TMResourceName>(result);
            storage.FromDate(builder, args[0].Get<ui16>(), args[0].GetTimezoneId());
            return result;
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }
    }

    template <>
    TUnboxedValue TSplit<TTzDate32>::Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const
    {
        try {
            EMPTY_RESULT_ON_EMPTY_ARG(0);

            auto& builder = valueBuilder->GetDateBuilder();
            TUnboxedValuePod result(0);
            auto& storage = Reference<TM64ResourceName>(result);
            storage.FromDate32(builder, args[0].Get<i32>(), args[0].GetTimezoneId());
            return result;
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }
    }


    template <>
    TUnboxedValue TSplit<TTzDatetime>::Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const
    {
        try {
            EMPTY_RESULT_ON_EMPTY_ARG(0);

            auto& builder = valueBuilder->GetDateBuilder();
            TUnboxedValuePod result(0);
            auto& storage = Reference<TMResourceName>(result);
            storage.FromDatetime(builder, args[0].Get<ui32>(), args[0].GetTimezoneId());
            return result;
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }
    }

    template <>
    TUnboxedValue TSplit<TTzDatetime64>::Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const
    {
        try {
            EMPTY_RESULT_ON_EMPTY_ARG(0);

            auto& builder = valueBuilder->GetDateBuilder();
            TUnboxedValuePod result(0);
            auto& storage = Reference<TM64ResourceName>(result);
            storage.FromDatetime64(builder, args[0].Get<i64>(), args[0].GetTimezoneId());
            return result;
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }
    }


    template <>
    TUnboxedValue TSplit<TTzTimestamp>::Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const
    {
        try {
            EMPTY_RESULT_ON_EMPTY_ARG(0);

            auto& builder = valueBuilder->GetDateBuilder();
            TUnboxedValuePod result(0);
            auto& storage = Reference<TMResourceName>(result);
            storage.FromTimestamp(builder, args[0].Get<ui64>(), args[0].GetTimezoneId());
            return result;
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }
    }

    template <>
    TUnboxedValue TSplit<TTzTimestamp64>::Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const
    {
        try {
            EMPTY_RESULT_ON_EMPTY_ARG(0);

            auto& builder = valueBuilder->GetDateBuilder();
            TUnboxedValuePod result(0);
            auto& storage = Reference<TM64ResourceName>(result);
            storage.FromTimestamp64(builder, args[0].Get<i64>(), args[0].GetTimezoneId());
            return result;
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }
    }

    // Make*

    template<typename TUserDataType, bool Nullable>
    using TMakeResBuilder = std::conditional_t<TTzDataType<TUserDataType>::Result,
        TTzDateArrayBuilder<TUserDataType, Nullable>,
        TFixedSizeArrayBuilder<typename TDataType<TUserDataType>::TLayout, Nullable>>;

    template<typename TUserDataType>
    struct TMakeDateKernelExec : TUnaryKernelExec<TMakeDateKernelExec<TUserDataType>, TReaderTraits::TResource<false>, TMakeResBuilder<TUserDataType, false>> {
        static TBlockItem Make(TTMStorage& storage, const IValueBuilder& valueBuilder);

        template<typename TSink>
        static void Process(const IValueBuilder* valueBuilder, TBlockItem item, const TSink& sink) {
            auto& storage = Reference<TMResourceName>(item);
            sink(TBlockItem(Make(storage, *valueBuilder)));
        }
    };

    template<> TBlockItem TMakeDateKernelExec<TDate>::Make(TTMStorage& storage, const IValueBuilder& valueBuilder) {
        TBlockItem res(storage.ToDate(valueBuilder.GetDateBuilder(), /*local*/ false));
        return res;
    }

    template<> TBlockItem TMakeDateKernelExec<TDatetime>::Make(TTMStorage& storage, const IValueBuilder& valueBuilder) {
        TBlockItem res(storage.ToDatetime(valueBuilder.GetDateBuilder()));
        return res;
    }

    template<> TBlockItem TMakeDateKernelExec<TTimestamp>::Make(TTMStorage& storage, const IValueBuilder& valueBuilder) {
        TBlockItem res(storage.ToTimestamp(valueBuilder.GetDateBuilder()));
        return res;
    }

    template<> TBlockItem TMakeDateKernelExec<TTzDate>::Make(TTMStorage& storage, const IValueBuilder& valueBuilder) {
        TBlockItem res(storage.ToDate(valueBuilder.GetDateBuilder(), /*local*/ true));
        res.SetTimezoneId(storage.TimezoneId);
        return res;
    }

    template<> TBlockItem TMakeDateKernelExec<TTzDatetime>::Make(TTMStorage& storage, const IValueBuilder& valueBuilder) {
        TBlockItem res(storage.ToDatetime(valueBuilder.GetDateBuilder()));
        res.SetTimezoneId(storage.TimezoneId);
        return res;
    }

    template<> TBlockItem TMakeDateKernelExec<TTzTimestamp>::Make(TTMStorage& storage, const IValueBuilder& valueBuilder) {
        TBlockItem res(storage.ToTimestamp(valueBuilder.GetDateBuilder()));
        res.SetTimezoneId(storage.TimezoneId);
        return res;
    }

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TMakeDate, TDate(TAutoMap<TResource<TMResourceName>>)) {
        auto& builder = valueBuilder->GetDateBuilder();
        auto& storage = Reference<TMResourceName>(args[0]);
        return TUnboxedValuePod(storage.ToDate(builder, false));
    }
    END_SIMPLE_ARROW_UDF(TMakeDate, TMakeDateKernelExec<TDate>::Do);

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TMakeDatetime, TDatetime(TAutoMap<TResource<TMResourceName>>)) {
        auto& builder = valueBuilder->GetDateBuilder();
        auto& storage = Reference<TMResourceName>(args[0]);
        return TUnboxedValuePod(storage.ToDatetime(builder));
    }
    END_SIMPLE_ARROW_UDF(TMakeDatetime, TMakeDateKernelExec<TDatetime>::Do);

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TMakeTimestamp, TTimestamp(TAutoMap<TResource<TMResourceName>>)) {
        auto& builder = valueBuilder->GetDateBuilder();
        auto& storage = Reference<TMResourceName>(args[0]);
        return TUnboxedValuePod(storage.ToTimestamp(builder));
    }
    END_SIMPLE_ARROW_UDF(TMakeTimestamp, TMakeDateKernelExec<TTimestamp>::Do);

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TMakeTzDate, TTzDate(TAutoMap<TResource<TMResourceName>>)) {
        auto& builder = valueBuilder->GetDateBuilder();
        auto& storage = Reference<TMResourceName>(args[0]);
        try {
            TUnboxedValuePod result(storage.ToDate(builder, true));
            result.SetTimezoneId(storage.TimezoneId);
            return result;
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << "Timestamp "
                                           << storage.ToString()
                                           << " cannot be casted to TzDate"
            ).data());
        }
    }
    END_SIMPLE_ARROW_UDF(TMakeTzDate, TMakeDateKernelExec<TTzDate>::Do);

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TMakeTzDatetime, TTzDatetime(TAutoMap<TResource<TMResourceName>>)) {
        auto& builder = valueBuilder->GetDateBuilder();
        auto& storage = Reference<TMResourceName>(args[0]);
        TUnboxedValuePod result(storage.ToDatetime(builder));
        result.SetTimezoneId(storage.TimezoneId);
        return result;
    }
    END_SIMPLE_ARROW_UDF(TMakeTzDatetime, TMakeDateKernelExec<TTzDatetime>::Do);

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TMakeTzTimestamp, TTzTimestamp(TAutoMap<TResource<TMResourceName>>)) {
        auto& builder = valueBuilder->GetDateBuilder();
        auto& storage = Reference<TMResourceName>(args[0]);
        TUnboxedValuePod result(storage.ToTimestamp(builder));
        result.SetTimezoneId(storage.TimezoneId);
        return result;
    }
    END_SIMPLE_ARROW_UDF(TMakeTzTimestamp, TMakeDateKernelExec<TTzTimestamp>::Do);


    SIMPLE_STRICT_UDF(TConvert, TResource<TM64ResourceName>(TAutoMap<TResource<TMResourceName>>)) {
        Y_UNUSED(valueBuilder);
        TUnboxedValuePod result(0);
        auto& arg = Reference<TMResourceName>(args[0]);
        auto& storage = Reference<TM64ResourceName>(result);
        storage.From(arg);
        return result;
    }

    SIMPLE_STRICT_UDF(TMakeDate32, TDate32(TAutoMap<TResource<TM64ResourceName>>)) {
        auto& storage = Reference<TM64ResourceName>(args[0]);
        return TUnboxedValuePod(storage.ToDate32(valueBuilder->GetDateBuilder(), false));
    }

    SIMPLE_STRICT_UDF(TMakeDatetime64, TDatetime64(TAutoMap<TResource<TM64ResourceName>>)) {
        auto& storage = Reference<TM64ResourceName>(args[0]);
        return TUnboxedValuePod(storage.ToDatetime64(valueBuilder->GetDateBuilder()));
    }

    SIMPLE_STRICT_UDF(TMakeTimestamp64, TTimestamp64(TAutoMap<TResource<TM64ResourceName>>)) {
        auto& storage = Reference<TM64ResourceName>(args[0]);
        return TUnboxedValuePod(storage.ToTimestamp64(valueBuilder->GetDateBuilder()));
    }

    SIMPLE_STRICT_UDF(TMakeTzDate32, TTzDate32(TAutoMap<TResource<TM64ResourceName>>)) {
        auto& builder = valueBuilder->GetDateBuilder();
        auto& storage = Reference<TM64ResourceName>(args[0]);
        try {
            TUnboxedValuePod result(storage.ToDate32(builder, true));
            result.SetTimezoneId(storage.TimezoneId);
            return result;
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << "Timestamp "
                                           << storage.ToString()
                                           << " cannot be casted to TzDate32"
            ).data());
        }
    }

    SIMPLE_STRICT_UDF(TMakeTzDatetime64, TTzDatetime64(TAutoMap<TResource<TM64ResourceName>>)) {
        auto& storage = Reference<TM64ResourceName>(args[0]);
        TUnboxedValuePod result(storage.ToDatetime64(valueBuilder->GetDateBuilder()));
        result.SetTimezoneId(storage.TimezoneId);
        return result;
    }

    SIMPLE_STRICT_UDF(TMakeTzTimestamp64, TTzTimestamp64(TAutoMap<TResource<TM64ResourceName>>)) {
        auto& storage = Reference<TM64ResourceName>(args[0]);
        TUnboxedValuePod result(storage.ToTimestamp64(valueBuilder->GetDateBuilder()));
        result.SetTimezoneId(storage.TimezoneId);
        return result;
    }

    // Get*

// #define GET_METHOD(field, type)                                                 \
//     struct TGet##field##KernelExec : TUnaryKernelExec<TGet##field##KernelExec, TReaderTraits::TResource<false>, TFixedSizeArrayBuilder<type, false>> { \
//         template<typename TSink> \
//         static void Process(TBlockItem item, const IValueBuilder& valueBuilder, const TSink& sink) { \
//             Y_UNUSED(valueBuilder); \
//             sink(TBlockItem(Get##field(item))); \
//         } \
//     }; \
//     BEGIN_SIMPLE_STRICT_ARROW_UDF(TGet##field, type(TAutoMap<TResource<TMResourceName>>)) { \
//         Y_UNUSED(valueBuilder);                                                 \
//         return TUnboxedValuePod(Get##field(args[0]));                           \
//     }  \
//     END_SIMPLE_ARROW_UDF_WITH_NULL_HANDLING(TGet##field, TGet##field##KernelExec::Do, arrow::compute::NullHandling::INTERSECTION);

template<const char* TUdfName,
         typename TResultType,  TResultType (*Accessor)(const TUnboxedValuePod&),
         typename TResultWType, TResultWType (*WAccessor)(const TUnboxedValuePod&)>
class TGetDateComponent: public ::NYql::NUdf::TBoxedValue {
public:
    typedef bool TTypeAwareMarker;
    static const ::NYql::NUdf::TStringRef& Name() {
        static auto name = TStringRef(TUdfName, std::strlen(TUdfName));
        return name;
    }

    static bool DeclareSignature(
        const ::NYql::NUdf::TStringRef& name,
        ::NYql::NUdf::TType* userType,
        ::NYql::NUdf::IFunctionTypeInfoBuilder& builder,
        bool typesOnly)
    {
        if (Name() != name) {
            return false;
        }

        if (!userType) {
            // XXX: Function became polymorphic when overload for
            // wide resources was implemented. Hence, to make it
            // backward compatible with previous versions, the
            // absence of the userType is considered as using the
            // old version (i.e. without type awareness) that
            // provides implementation only for narrow dates.
            BuildSignature<TResultType, TMResourceName, Accessor>(builder, typesOnly);
            return true;
        }

        builder.UserType(userType);

        const auto typeInfoHelper = builder.TypeInfoHelper();
        TTupleTypeInspector tuple(*typeInfoHelper, userType);
        Y_ENSURE(tuple, "Tuple with args and options tuples expected");
        Y_ENSURE(tuple.GetElementsCount() > 0,
                 "Tuple has to contain positional arguments");

        TTupleTypeInspector argsTuple(*typeInfoHelper, tuple.GetElementType(0));
        Y_ENSURE(argsTuple, "Tuple with args expected");
        if (argsTuple.GetElementsCount() != 1) {
            builder.SetError("Single argument expected");
            return true;
        }

        auto argType = argsTuple.GetElementType(0);

        if (const auto optType = TOptionalTypeInspector(*typeInfoHelper, argType)) {
            argType = optType.GetItemType();
        }

        TResourceTypeInspector resource(*typeInfoHelper, argType);
        if (!resource) {
            TDataTypeInspector data(*typeInfoHelper, argType);
            if (!data) {
                builder.SetError("Data type expected");
                return true;
            }

            const auto features = NUdf::GetDataTypeInfo(NUdf::GetDataSlot(data.GetTypeId())).Features;
            if (features & NUdf::BigDateType) {
                BuildSignature<TResultWType, TM64ResourceName, WAccessor>(builder, typesOnly);
                return true;
            }
            if (features & (NUdf::DateType | NUdf::TzDateType)) {
                BuildSignature<TResultType, TMResourceName, Accessor>(builder, typesOnly);
                return true;
            }

            SetResourceExpectedError(builder, typeInfoHelper, argType);
            return true;
        }

        if (resource.GetTag() == TStringRef::Of(TM64ResourceName)) {
            BuildSignature<TResultWType, TM64ResourceName, WAccessor>(builder, typesOnly);
            return true;
        }

        if (resource.GetTag() == TStringRef::Of(TMResourceName)) {
            BuildSignature<TResultType, TMResourceName, Accessor>(builder, typesOnly);
            return true;
        }

        SetUnexpectedTagError(builder, resource.GetTag());
        return true;
    }
private:
    template<typename TResult, TResult (*Func)(const TUnboxedValuePod&)>
    class TImpl : public TBoxedValue {
    public:
        TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
            Y_UNUSED(valueBuilder);
            EMPTY_RESULT_ON_EMPTY_ARG(0);
            return TUnboxedValuePod(TResult(Func(args[0])));
        }
    };

    template<typename TResult, const char* TResourceName, TResult (*Func)(const TUnboxedValuePod&)>
    static void BuildSignature(NUdf::IFunctionTypeInfoBuilder& builder, bool typesOnly) {
        builder.Returns<TResult>();
        builder.Args()->Add<TAutoMap<TResource<TResourceName>>>();
        builder.IsStrict();
        if (!typesOnly) {
            builder.Implementation(new TImpl<TResult, Func>());
        }
    }
};

// TODO: Merge this with <TGetDateComponent> class.
template<const char* TUdfName, auto Accessor, auto WAccessor>
class TGetDateComponentName: public ::NYql::NUdf::TBoxedValue {
public:
    typedef bool TTypeAwareMarker;
    static const ::NYql::NUdf::TStringRef& Name() {
        static auto name = TStringRef(TUdfName, std::strlen(TUdfName));
        return name;
    }

    static bool DeclareSignature(
        const ::NYql::NUdf::TStringRef& name,
        ::NYql::NUdf::TType* userType,
        ::NYql::NUdf::IFunctionTypeInfoBuilder& builder,
        bool typesOnly)
    {
        if (Name() != name) {
            return false;
        }

        if (!userType) {
            // XXX: Function became polymorphic when overload for
            // wide resources was implemented. Hence, to make it
            // backward compatible with previous versions, the
            // absence of the userType is considered as using the
            // old version (i.e. without type awareness) that
            // provides implementation only for narrow dates.
            BuildSignature<TMResourceName, Accessor>(builder, typesOnly);
            return true;
        }

        builder.UserType(userType);

        const auto typeInfoHelper = builder.TypeInfoHelper();
        TTupleTypeInspector tuple(*typeInfoHelper, userType);
        Y_ENSURE(tuple, "Tuple with args and options tuples expected");
        Y_ENSURE(tuple.GetElementsCount() > 0,
                 "Tuple has to contain positional arguments");

        TTupleTypeInspector argsTuple(*typeInfoHelper, tuple.GetElementType(0));
        Y_ENSURE(argsTuple, "Tuple with args expected");
        if (argsTuple.GetElementsCount() != 1) {
            builder.SetError("Single argument expected");
            return true;
        }

        auto argType = argsTuple.GetElementType(0);

        if (const auto optType = TOptionalTypeInspector(*typeInfoHelper, argType)) {
            argType = optType.GetItemType();
        }

        TResourceTypeInspector resource(*typeInfoHelper, argType);
        if (!resource) {
            TDataTypeInspector data(*typeInfoHelper, argType);
            if (!data) {
                builder.SetError("Data type expected");
                return true;
            }

            const auto features = NUdf::GetDataTypeInfo(NUdf::GetDataSlot(data.GetTypeId())).Features;
            if (features & NUdf::BigDateType) {
                BuildSignature<TM64ResourceName, WAccessor>(builder, typesOnly);
                return true;
            }
            if (features & (NUdf::DateType | NUdf::TzDateType)) {
                BuildSignature<TMResourceName, Accessor>(builder, typesOnly);
                return true;
            }

            SetResourceExpectedError(builder, typeInfoHelper, argType);
            return true;
        }

        if (resource.GetTag() == TStringRef::Of(TM64ResourceName)) {
            BuildSignature<TM64ResourceName, WAccessor>(builder, typesOnly);
            return true;
        }

        if (resource.GetTag() == TStringRef::Of(TMResourceName)) {
            BuildSignature<TMResourceName, Accessor>(builder, typesOnly);
            return true;
        }

        SetUnexpectedTagError(builder, resource.GetTag());
        return true;
    }
private:
    template<auto Func>
    class TImpl : public TBoxedValue {
    public:
        TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
            EMPTY_RESULT_ON_EMPTY_ARG(0);
            return Func(valueBuilder, args[0]);
        }
    };

    template<const char* TResourceName, auto Func>
    static void BuildSignature(NUdf::IFunctionTypeInfoBuilder& builder, bool typesOnly) {
        builder.Returns<char*>();
        builder.Args()->Add<TAutoMap<TResource<TResourceName>>>();
        builder.IsStrict();
        if (!typesOnly) {
            builder.Implementation(new TImpl<Func>());
        }
    }
};

    // template<typename TValue>
    // TValue GetMonthNameValue(size_t idx) {
    //     static const std::array<TValue, 12U> monthNames = {{
    //         TValue::Embedded(TStringRef::Of("January")),
    //         TValue::Embedded(TStringRef::Of("February")),
    //         TValue::Embedded(TStringRef::Of("March")),
    //         TValue::Embedded(TStringRef::Of("April")),
    //         TValue::Embedded(TStringRef::Of("May")),
    //         TValue::Embedded(TStringRef::Of("June")),
    //         TValue::Embedded(TStringRef::Of("July")),
    //         TValue::Embedded(TStringRef::Of("August")),
    //         TValue::Embedded(TStringRef::Of("September")),
    //         TValue::Embedded(TStringRef::Of("October")),
    //         TValue::Embedded(TStringRef::Of("November")),
    //         TValue::Embedded(TStringRef::Of("December"))
    //     }};
    //     return monthNames.at(idx);
    // }

    // struct TGetMonthNameKernelExec : TUnaryKernelExec<TGetMonthNameKernelExec, TReaderTraits::TResource<true>, TStringArrayBuilder<arrow::StringType, false>> {
    //     template<typename TSink>
    //     static void Process(const IValueBuilder* valueBuilder, TBlockItem item, const TSink& sink) {
    //         Y_UNUSED(valueBuilder);
    //         sink(GetMonthNameValue<TBlockItem>(GetMonth(item) - 1U));
    //     }
    // };

    // BEGIN_SIMPLE_STRICT_ARROW_UDF(TGetMonthName, char*(TAutoMap<TResource<TMResourceName>>)) {
    //     Y_UNUSED(valueBuilder);
    //     return GetMonthNameValue<TUnboxedValue>(GetMonth(*args) - 1U);
    // }
    // END_SIMPLE_ARROW_UDF_WITH_NULL_HANDLING(TGetMonthName, TGetMonthNameKernelExec::Do, arrow::compute::NullHandling::INTERSECTION);

template<const char* TResourceName>
TUnboxedValue GetMonthName(const IValueBuilder* valueBuilder, const TUnboxedValuePod& arg) {
    Y_UNUSED(valueBuilder);
    static const std::array<TUnboxedValue, 12U> monthNames = {{
        TUnboxedValuePod::Embedded(TStringRef::Of("January")),
        TUnboxedValuePod::Embedded(TStringRef::Of("February")),
        TUnboxedValuePod::Embedded(TStringRef::Of("March")),
        TUnboxedValuePod::Embedded(TStringRef::Of("April")),
        TUnboxedValuePod::Embedded(TStringRef::Of("May")),
        TUnboxedValuePod::Embedded(TStringRef::Of("June")),
        TUnboxedValuePod::Embedded(TStringRef::Of("July")),
        TUnboxedValuePod::Embedded(TStringRef::Of("August")),
        TUnboxedValuePod::Embedded(TStringRef::Of("September")),
        TUnboxedValuePod::Embedded(TStringRef::Of("October")),
        TUnboxedValuePod::Embedded(TStringRef::Of("November")),
        TUnboxedValuePod::Embedded(TStringRef::Of("December"))
    }};
    return monthNames.at(GetMonth<TResourceName>(arg) - 1U);
}

    // struct TGetDayOfMonthKernelExec : TUnaryKernelExec<TGetMonthNameKernelExec, TReaderTraits::TResource<false>, TFixedSizeArrayBuilder<ui8, false>> {
    //     template<typename TSink>
    //     static void Process(TBlockItem item, const TSink& sink) {
    //         sink(GetDay(item));
    //     }
    // };

    // BEGIN_SIMPLE_STRICT_ARROW_UDF(TGetDayOfMonth, ui8(TAutoMap<TResource<TMResourceName>>)) {
    //     Y_UNUSED(valueBuilder);
    //     return TUnboxedValuePod(GetDay(args[0]));
    // }
    // END_SIMPLE_ARROW_UDF_WITH_NULL_HANDLING(TGetDayOfMonth, TGetDayOfMonthKernelExec::Do, arrow::compute::NullHandling::INTERSECTION);

template<const char* TResourceName>
TUnboxedValue GetDayOfWeekName(const IValueBuilder* valueBuilder, const TUnboxedValuePod& arg) {
    Y_UNUSED(valueBuilder);
    static const std::array<TUnboxedValue, 7U> dayNames = {{
        TUnboxedValuePod::Embedded(TStringRef::Of("Monday")),
        TUnboxedValuePod::Embedded(TStringRef::Of("Tuesday")),
        TUnboxedValuePod::Embedded(TStringRef::Of("Wednesday")),
        TUnboxedValuePod::Embedded(TStringRef::Of("Thursday")),
        TUnboxedValuePod::Embedded(TStringRef::Of("Friday")),
        TUnboxedValuePod::Embedded(TStringRef::Of("Saturday")),
        TUnboxedValuePod::Embedded(TStringRef::Of("Sunday"))
    }};
    return dayNames.at(GetDayOfWeek<TResourceName>(arg) - 1U);
}

    // struct TGetDayOfWeekNameKernelExec : TUnaryKernelExec<TGetDayOfWeekNameKernelExec, TReaderTraits::TResource<true>, TStringArrayBuilder<arrow::StringType, false>> {
    //     template<typename TSink>
    //     static void Process(const IValueBuilder* valueBuilder, TBlockItem item, const TSink& sink) {
    //         Y_UNUSED(valueBuilder);
    //         sink(GetDayNameValue<TBlockItem>(GetDayOfWeek(item) - 1U));
    //     }
    // };

    // BEGIN_SIMPLE_STRICT_ARROW_UDF(TGetDayOfWeekName, char*(TAutoMap<TResource<TMResourceName>>)) {
    //     Y_UNUSED(valueBuilder);
    //     return GetDayNameValue<TUnboxedValuePod>(GetDayOfWeek(*args) - 1U);
    // }
    // END_SIMPLE_ARROW_UDF_WITH_NULL_HANDLING(TGetDayOfWeekName, TGetDayOfWeekNameKernelExec::Do, arrow::compute::NullHandling::INTERSECTION);

    struct TTGetTimezoneNameKernelExec : TUnaryKernelExec<TTGetTimezoneNameKernelExec, TReaderTraits::TResource<false>, TStringArrayBuilder<arrow::BinaryType, false>> {
        template<typename TSink>
        static void Process(const IValueBuilder* valueBuilder, TBlockItem item, const TSink& sink) {
            Y_UNUSED(valueBuilder);
            auto timezoneId = GetTimezoneId<TMResourceName>(item);
            if (timezoneId >= NUdf::GetTimezones().size()) {
                sink(TBlockItem{});
            } else {
                sink(TBlockItem{NUdf::GetTimezones()[timezoneId]});
            }
        }
    };

template<const char* TResourceName>
TUnboxedValue GetTimezoneName(const IValueBuilder* valueBuilder, const TUnboxedValuePod& arg) {
    const ui16 tzId = GetTimezoneId<TResourceName>(arg);
    const auto& tzNames = NUdf::GetTimezones();
    if (tzId >= tzNames.size()) {
        return TUnboxedValuePod();
    }
    return valueBuilder->NewString(tzNames[tzId]);
}

    // Update

    class TUpdate : public TBoxedValue {
    public:
        typedef bool TTypeAwareMarker;
        static const TStringRef& Name() {
            static auto name = TStringRef::Of("Update");
            return name;
        }

        static bool DeclareSignature(
            const TStringRef& name,
            TType* userType,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly)
        {
            if (Name() != name) {
                return false;
            }

            if (!userType) {
                // XXX: Function became polymorphic when overload for
                // wide resources was implemented. Hence, to make it
                // backward compatible with previous versions, the
                // absence of the userType is considered as using the
                // old version (i.e. without type awareness) that
                // provides implementation only for narrow dates.
                BuildSignature<TMResourceName>(builder, typesOnly);
                return true;
            }

            builder.UserType(userType);

            const auto typeInfoHelper = builder.TypeInfoHelper();
            TTupleTypeInspector tuple(*typeInfoHelper, userType);
            Y_ENSURE(tuple, "Tuple with args and options tuples expected");
            Y_ENSURE(tuple.GetElementsCount() > 0,
                     "Tuple has to contain positional arguments");

            TTupleTypeInspector argsTuple(*typeInfoHelper, tuple.GetElementType(0));
            Y_ENSURE(argsTuple, "Tuple with args expected");
            if (argsTuple.GetElementsCount() == 0) {
                builder.SetError("At least one argument expected");
                return true;
            }

            auto argType = argsTuple.GetElementType(0);

            if (const auto optType = TOptionalTypeInspector(*typeInfoHelper, argType)) {
                argType = optType.GetItemType();
            }

            TResourceTypeInspector resource(*typeInfoHelper, argType);
            if (!resource) {
                TDataTypeInspector data(*typeInfoHelper, argType);
                if (!data) {
                    SetResourceExpectedError(builder, typeInfoHelper, argType);
                    return true;
                }

                const auto features = NUdf::GetDataTypeInfo(NUdf::GetDataSlot(data.GetTypeId())).Features;
                if (features & NUdf::BigDateType) {
                    BuildSignature<TM64ResourceName>(builder, typesOnly);
                    return true;
                }
                if (features & (NUdf::DateType | NUdf::TzDateType)) {
                    BuildSignature<TMResourceName>(builder, typesOnly);
                    return true;
                }

                SetResourceExpectedError(builder, typeInfoHelper, argType);
                return true;
            }

            if (resource.GetTag() == TStringRef::Of(TM64ResourceName)) {
                BuildSignature<TM64ResourceName>(builder, typesOnly);
                return true;
            }

            if (resource.GetTag() == TStringRef::Of(TMResourceName)) {
                BuildSignature<TMResourceName>(builder, typesOnly);
                return true;
            }

            SetUnexpectedTagError(builder, resource.GetTag());
            return true;
        }
    private:
        template<const char* TResourceName>
        class TImpl : public TBoxedValue {
        public:
            TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
                try {
                    EMPTY_RESULT_ON_EMPTY_ARG(0);
                    auto result = args[0];

                    if (args[1]) {
                        auto year = args[1].Get<std::conditional_t<TResourceName == TMResourceName, ui16, i32>>();
                        if (!ValidateYear<TResourceName>(year)) {
                            return TUnboxedValuePod();
                        }
                        SetYear<TResourceName>(result, year);
                    }
                    if (args[2]) {
                        auto month = args[2].Get<ui8>();
                        if (!ValidateMonth(month)) {
                            return TUnboxedValuePod();
                        }
                        SetMonth<TResourceName>(result, month);
                    }
                    if (args[3]) {
                        auto day = args[3].Get<ui8>();
                        if (!ValidateDay(day)) {
                            return TUnboxedValuePod();
                        }
                        SetDay<TResourceName>(result, day);
                    }
                    if (args[4]) {
                        auto hour = args[4].Get<ui8>();
                        if (!ValidateHour(hour)) {
                            return TUnboxedValuePod();
                        }
                        SetHour<TResourceName>(result, hour);
                    }
                    if (args[5]) {
                        auto minute = args[5].Get<ui8>();
                        if (!ValidateMinute(minute)) {
                            return TUnboxedValuePod();
                        }
                        SetMinute<TResourceName>(result, minute);
                    }
                    if (args[6]) {
                        auto second = args[6].Get<ui8>();
                        if (!ValidateSecond(second)) {
                            return TUnboxedValuePod();
                        }
                        SetSecond<TResourceName>(result, second);
                    }
                    if (args[7]) {
                        auto microsecond = args[7].Get<ui32>();
                        if (!ValidateMicrosecond(microsecond)) {
                            return TUnboxedValuePod();
                        }
                        SetMicrosecond<TResourceName>(result, microsecond);
                    }
                    if (args[8]) {
                        auto timezoneId = args[8].Get<ui16>();
                        if (!ValidateTimezoneId(timezoneId)) {
                            return TUnboxedValuePod();
                        }
                        SetTimezoneId<TResourceName>(result, timezoneId);
                    }

                    auto& builder = valueBuilder->GetDateBuilder();
                    auto& storage = Reference<TResourceName>(result);
                    if (!storage.Validate(builder)) {
                        return TUnboxedValuePod();
                    }
                    return result;
                } catch (const std::exception& e) {
                    TStringBuilder sb;
                    sb << CurrentExceptionMessage();
                    sb << Endl << "[" << TStringBuf(Name()) << "]" ;
                    UdfTerminate(sb.c_str());
                }
            }

        };

        template<const char* TResourceName>
        static void BuildSignature(NUdf::IFunctionTypeInfoBuilder& builder, bool typesOnly) {
            builder.Returns<TOptional<TResource<TResourceName>>>();
            builder.OptionalArgs(8).Args()->Add<TAutoMap<TResource<TResourceName>>>()
                .template Add<TOptional<std::conditional_t<TResourceName == TMResourceName, ui16, i32>>>().Name("Year")
                .template Add<TOptional<ui8>>().Name("Month")
                .template Add<TOptional<ui8>>().Name("Day")
                .template Add<TOptional<ui8>>().Name("Hour")
                .template Add<TOptional<ui8>>().Name("Minute")
                .template Add<TOptional<ui8>>().Name("Second")
                .template Add<TOptional<ui32>>().Name("Microsecond")
                .template Add<TOptional<ui16>>().Name("TimezoneId");
            builder.IsStrict();
            if (!typesOnly) {
                builder.Implementation(new TImpl<TResourceName>());
            }
    }
    };

    // From*

    template<typename TInput, typename TOutput, i64 UsecMultiplier>
    inline TUnboxedValuePod TFromConverter(TInput arg) {
        using TLayout = TDataType<TOutput>::TLayout;
        const TLayout usec = TLayout(arg) * UsecMultiplier;
        return Validate<TOutput>(usec) ? TUnboxedValuePod(usec) : TUnboxedValuePod();
    }


    template<typename TInput, typename TOutput, i64 UsecMultiplier>
    using TFromConverterKernel = TUnaryUnsafeFixedSizeFilterKernel<TInput,
        typename TDataType<TOutput>::TLayout, [] (TInput arg) {
            using TLayout = TDataType<TOutput>::TLayout;
            const TLayout usec = TLayout(arg) * UsecMultiplier;
            return std::make_pair(usec, Validate<TOutput>(usec));
        }>;


#define DATETIME_FROM_CONVERTER_UDF(name, retType, argType, usecMultiplier)              \
    BEGIN_SIMPLE_STRICT_ARROW_UDF(T##name, TOptional<retType>(TAutoMap<argType>)) {      \
        Y_UNUSED(valueBuilder);                                                          \
        return TFromConverter<argType, retType, usecMultiplier>(args[0].Get<argType>()); \
    }                                                                                    \
                                                                                         \
    END_SIMPLE_ARROW_UDF(T##name, (TFromConverterKernel<argType, retType, usecMultiplier>::Do))

    DATETIME_FROM_CONVERTER_UDF(FromSeconds, TTimestamp, ui32, UsecondsInSecond);
    DATETIME_FROM_CONVERTER_UDF(FromMilliseconds, TTimestamp, ui64, UsecondsInMilliseconds);
    DATETIME_FROM_CONVERTER_UDF(FromMicroseconds, TTimestamp, ui64, 1);

    DATETIME_FROM_CONVERTER_UDF(FromSeconds64, TTimestamp64, i64, UsecondsInSecond);
    DATETIME_FROM_CONVERTER_UDF(FromMilliseconds64, TTimestamp64, i64, UsecondsInMilliseconds);
    DATETIME_FROM_CONVERTER_UDF(FromMicroseconds64, TTimestamp64, i64, 1);

    DATETIME_FROM_CONVERTER_UDF(IntervalFromDays, TInterval, i32, UsecondsInDay);
    DATETIME_FROM_CONVERTER_UDF(IntervalFromHours, TInterval, i32, UsecondsInHour);
    DATETIME_FROM_CONVERTER_UDF(IntervalFromMinutes, TInterval, i32, UsecondsInMinute);
    DATETIME_FROM_CONVERTER_UDF(IntervalFromSeconds, TInterval, i32, UsecondsInSecond);
    DATETIME_FROM_CONVERTER_UDF(IntervalFromMilliseconds, TInterval, i64, UsecondsInMilliseconds);
    DATETIME_FROM_CONVERTER_UDF(IntervalFromMicroseconds, TInterval, i64, 1);

    DATETIME_FROM_CONVERTER_UDF(Interval64FromDays, TInterval64, i32, UsecondsInDay);
    DATETIME_FROM_CONVERTER_UDF(Interval64FromHours, TInterval64, i64, UsecondsInHour);
    DATETIME_FROM_CONVERTER_UDF(Interval64FromMinutes, TInterval64, i64, UsecondsInMinute);
    DATETIME_FROM_CONVERTER_UDF(Interval64FromSeconds, TInterval64, i64, UsecondsInSecond);
    DATETIME_FROM_CONVERTER_UDF(Interval64FromMilliseconds, TInterval64, i64, UsecondsInMilliseconds);
    DATETIME_FROM_CONVERTER_UDF(Interval64FromMicroseconds, TInterval64, i64, 1);

    // To*

template<const char* TUdfName, typename TResult, typename TWResult, i64 ScaleSeconds>
class TToConverter : public TBoxedValue {
public:
    typedef bool TTypeAwareMarker;
    static const ::NYql::NUdf::TStringRef& Name() {
        static auto name = TStringRef(TUdfName, std::strlen(TUdfName));
        return name;
    }

    static bool DeclareSignature(
        const TStringRef& name,
        TType* userType,
        IFunctionTypeInfoBuilder& builder,
        bool typesOnly)
    {
        if (Name() != name) {
            return false;
        }

        if (!userType) {
            // XXX: Function became polymorphic when overload for
            // wide resources was implemented. Hence, to make it
            // backward compatible with previous versions, the
            // absence of the userType is considered as using the
            // old version (i.e. without type awareness) that
            // provides implementation only for narrow dates.
            BuildSignature<TInterval, TResult>(builder, typesOnly);
            return true;
        }

        builder.UserType(userType);

        const auto typeInfoHelper = builder.TypeInfoHelper();
        TTupleTypeInspector tuple(*typeInfoHelper, userType);
        Y_ENSURE(tuple, "Tuple with args and options tuples expected");
        Y_ENSURE(tuple.GetElementsCount() > 0,
                 "Tuple has to contain positional arguments");

        TTupleTypeInspector argsTuple(*typeInfoHelper, tuple.GetElementType(0));
        Y_ENSURE(argsTuple, "Tuple with args expected");
        if (argsTuple.GetElementsCount() != 1) {
            builder.SetError("Single argument expected");
            return true;
        }

        auto argType = argsTuple.GetElementType(0);

        if (const auto optType = TOptionalTypeInspector(*typeInfoHelper, argType)) {
            argType = optType.GetItemType();
        }

        TDataTypeInspector data(*typeInfoHelper, argType);
        if (!data) {
            SetResourceExpectedError(builder, typeInfoHelper, argType);
            return true;
        }

        const auto features = NUdf::GetDataTypeInfo(NUdf::GetDataSlot(data.GetTypeId())).Features;
        if (features & NUdf::TimeIntervalType) {
            if (features & NUdf::BigDateType) {
                BuildSignature<TInterval64, TWResult>(builder, typesOnly);
            } else {
                BuildSignature<TInterval, TResult>(builder, typesOnly);
            }
            return true;
        }
        SetIntervalExpectedError(builder, typeInfoHelper, argType);
        return true;
    }
private:
    class TImpl : public TBoxedValue {
    public:
        TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
            try {
                Y_UNUSED(valueBuilder);
                return TUnboxedValuePod(i64(args[0].Get<i64>() / ScaleSeconds));
            } catch (const std::exception& e) {
                TStringBuilder sb;
                sb << CurrentExceptionMessage();
                sb << Endl << "[" << TStringBuf(Name()) << "]" ;
                UdfTerminate(sb.c_str());
            }
        }

    };

    template<typename TInput, typename TOutput>
    static void BuildSignature(NUdf::IFunctionTypeInfoBuilder& builder, bool typesOnly) {
        builder.Returns<TOutput>();
        builder.Args()->Add<TAutoMap<TInput>>();
        builder.IsStrict();
        if (!typesOnly) {
            builder.Implementation(new TImpl());
        }
    }
};

    // StartOf*

    template<auto Core>
    struct TStartOfKernelExec : TUnaryKernelExec<TStartOfKernelExec<Core>, TResourceBlockReader<false>, TResourceArrayBuilder<true>> {
        template<typename TSink>
        static void Process(const IValueBuilder* valueBuilder, TBlockItem item, const TSink& sink) {
            if (auto res = Core(Reference<TMResourceName>(item), *valueBuilder)) {
                Reference<TMResourceName>(item) = res.GetRef();
                sink(item);
            } else {
                sink(TBlockItem{});
            }

        }
    };

    template<const char* TResourceName, auto Core>
    TUnboxedValue SimpleDatetimeToDatetimeUdf(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) {
        auto result = args[0];
        auto& storage = Reference<TResourceName>(result);
        if (auto res = Core(storage, *valueBuilder)) {
            storage = res.GetRef();
            return result;
        }
        return TUnboxedValuePod{};
    }

template<const char* TUdfName, auto Boundary, auto WBoundary>
class TBoundaryOf: public ::NYql::NUdf::TBoxedValue {
public:
    typedef bool TTypeAwareMarker;
    static const ::NYql::NUdf::TStringRef& Name() {
        static auto name = TStringRef(TUdfName, std::strlen(TUdfName));
        return name;
    }

    static bool DeclareSignature(
        const ::NYql::NUdf::TStringRef& name,
        ::NYql::NUdf::TType* userType,
        ::NYql::NUdf::IFunctionTypeInfoBuilder& builder,
        bool typesOnly)
    {
        if (Name() != name) {
            return false;
        }

        if (!userType) {
            // XXX: Function became polymorphic when overload for
            // wide resources was implemented. Hence, to make it
            // backward compatible with previous versions, the
            // absence of the userType is considered as using the
            // old version (i.e. without type awareness) that
            // provides implementation only for narrow dates.
            BuildSignature<TMResourceName, Boundary>(builder, typesOnly);
            return true;
        }

        builder.UserType(userType);

        const auto typeInfoHelper = builder.TypeInfoHelper();
        TTupleTypeInspector tuple(*typeInfoHelper, userType);
        Y_ENSURE(tuple, "Tuple with args and options tuples expected");
        Y_ENSURE(tuple.GetElementsCount() > 0,
                 "Tuple has to contain positional arguments");

        TTupleTypeInspector argsTuple(*typeInfoHelper, tuple.GetElementType(0));
        Y_ENSURE(argsTuple, "Tuple with args expected");
        if (argsTuple.GetElementsCount() != 1) {
            builder.SetError("Single argument expected");
            return true;
        }

        auto argType = argsTuple.GetElementType(0);

        if (const auto optType = TOptionalTypeInspector(*typeInfoHelper, argType)) {
            argType = optType.GetItemType();
        }

        TResourceTypeInspector resource(*typeInfoHelper, argType);
        if (!resource) {
            TDataTypeInspector data(*typeInfoHelper, argType);
            if (!data) {
                SetResourceExpectedError(builder, typeInfoHelper, argType);
                return true;
            }

            const auto features = NUdf::GetDataTypeInfo(NUdf::GetDataSlot(data.GetTypeId())).Features;
            if (features & NUdf::BigDateType) {
                BuildSignature<TM64ResourceName, WBoundary>(builder, typesOnly);
                return true;
            }
            if (features & (NUdf::DateType | NUdf::TzDateType)) {
                BuildSignature<TMResourceName, Boundary>(builder, typesOnly);
                return true;
            }

            SetResourceExpectedError(builder, typeInfoHelper, argType);
            return true;
        }

        if (resource.GetTag() == TStringRef::Of(TM64ResourceName)) {
            BuildSignature<TM64ResourceName, WBoundary>(builder, typesOnly);
            return true;
        }

        if (resource.GetTag() == TStringRef::Of(TMResourceName)) {
            BuildSignature<TMResourceName, Boundary>(builder, typesOnly);
            return true;
        }

        SetUnexpectedTagError(builder, resource.GetTag());
        return true;
    }
private:
    template<auto Func>
    class TImpl : public TBoxedValue {
    public:
        TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
            try {
                return Func(valueBuilder, args);
            } catch (const std::exception&) {
                    TStringBuilder sb;
                    sb << CurrentExceptionMessage();
                    sb << Endl << "[" << TStringBuf(Name()) << "]" ;
                    UdfTerminate(sb.c_str());
            }
        }
    };

    template< const char* TResourceName, auto Func>
    static void BuildSignature(NUdf::IFunctionTypeInfoBuilder& builder, bool typesOnly) {
        builder.Returns<TOptional<TResource<TResourceName>>>();
        builder.Args()->Add<TAutoMap<TResource<TResourceName>>>();
        builder.IsStrict();
        if (!typesOnly) {
            builder.Implementation(new TImpl<Func>());
        }
    }
};

    template<typename TStorage>
    void SetStartOfDay(TStorage& storage) {
        storage.Hour = 0;
        storage.Minute = 0;
        storage.Second = 0;
        storage.Microsecond = 0;
    }

    template<typename TStorage>
    void SetEndOfDay(TStorage& storage) {
        storage.Hour = 23;
        storage.Minute = 59;
        storage.Second = 59;
        storage.Microsecond = 999999;
    }

    template<typename TStorage>
    TMaybe<TStorage> StartOfYear(TStorage storage, const IValueBuilder& valueBuilder) {
        storage.Month = 1;
        storage.Day = 1;
        SetStartOfDay(storage);
        if (!storage.Validate(valueBuilder.GetDateBuilder())) {
            return {};
        }
        return storage;
    }

    template<typename TStorage>
    TMaybe<TStorage> EndOfYear(TStorage storage, const IValueBuilder& valueBuilder) {
        storage.Month = 12;
        storage.Day = 31;
        SetEndOfDay(storage);
        if (!storage.Validate(valueBuilder.GetDateBuilder())) {
            return {};
        }
        return storage;
    }

    template<typename TStorage>
    TMaybe<TStorage> StartOfQuarter(TStorage storage, const IValueBuilder& valueBuilder) {
        storage.Month = (storage.Month - 1) / 3 * 3 + 1;
        storage.Day = 1;
        SetStartOfDay(storage);
        if (!storage.Validate(valueBuilder.GetDateBuilder())) {
            return {};
        }
        return storage;
    }

    template<typename TStorage>
    TMaybe<TStorage> EndOfQuarter(TStorage storage, const IValueBuilder& valueBuilder) {
        storage.Month = ((storage.Month - 1) / 3 + 1) * 3;
        storage.Day = NMiniKQL::GetMonthLength(storage.Month, NMiniKQL::IsLeapYear(storage.Year));
        SetEndOfDay(storage);
        if (!storage.Validate(valueBuilder.GetDateBuilder())) {
            return {};
        }
        return storage;
    }

    template<typename TStorage>
    TMaybe<TStorage> StartOfMonth(TStorage storage, const IValueBuilder& valueBuilder) {
        storage.Day = 1;
        SetStartOfDay(storage);
        if (!storage.Validate(valueBuilder.GetDateBuilder())) {
            return {};
        }
        return storage;
    }

    template<typename TStorage>
    TMaybe<TStorage> EndOfMonth(TStorage storage, const IValueBuilder& valueBuilder) {
        storage.Day = NMiniKQL::GetMonthLength(storage.Month, NMiniKQL::IsLeapYear(storage.Year));
        SetEndOfDay(storage);
        if (!storage.Validate(valueBuilder.GetDateBuilder())) {
            return {};
        }
        return storage;
    }

    template<typename TStorage>
    TMaybe<TStorage> StartOfWeek(TStorage storage, const IValueBuilder& valueBuilder) {
        const ui32 shift = 86400u * (storage.DayOfWeek - 1u);
        if constexpr (std::is_same_v<TStorage, TTMStorage>) {
            if (shift > storage.ToDatetime(valueBuilder.GetDateBuilder())) {
                return {};
            }
            storage.FromDatetime(valueBuilder.GetDateBuilder(), storage.ToDatetime(valueBuilder.GetDateBuilder()) - shift, storage.TimezoneId);
        } else {
            if (shift > storage.ToDatetime64(valueBuilder.GetDateBuilder())) {
                return {};
            }
            storage.FromDatetime64(valueBuilder.GetDateBuilder(), storage.ToDatetime64(valueBuilder.GetDateBuilder()) - shift, storage.TimezoneId);
        }
        SetStartOfDay(storage);
        if (!storage.Validate(valueBuilder.GetDateBuilder())) {
            return {};
        }
        return storage;
    }

    template<typename TStorage>
    TMaybe<TStorage> EndOfWeek(TStorage storage, const IValueBuilder& valueBuilder) {
        const ui32 shift = 86400u * (7u - storage.DayOfWeek);
        if constexpr (std::is_same_v<TStorage, TTMStorage>) {
            auto dt = storage.ToDatetime(valueBuilder.GetDateBuilder());
            if (NUdf::MAX_DATETIME - shift <= dt) {
                return {};
            }
            storage.FromDatetime(valueBuilder.GetDateBuilder(), dt + shift, storage.TimezoneId);
        } else {
            auto dt = storage.ToDatetime64(valueBuilder.GetDateBuilder());
            if (NUdf::MAX_DATETIME64 - shift <= dt) {
                return {};
            }
            storage.FromDatetime64(valueBuilder.GetDateBuilder(), dt + shift, storage.TimezoneId);
        }
        SetEndOfDay(storage);
        if (!storage.Validate(valueBuilder.GetDateBuilder())) {
            return {};
        }
        return storage;
    }

    template<typename TStorage>
    TMaybe<TStorage> StartOfDay(TStorage storage, const IValueBuilder& valueBuilder) {
        SetStartOfDay(storage);
        auto& builder = valueBuilder.GetDateBuilder();
        if (!storage.Validate(builder)) {
            return {};
        }
        return storage;
    }

    template<typename TStorage>
    TMaybe<TStorage> EndOfDay(TStorage storage, const IValueBuilder& valueBuilder) {
        SetEndOfDay(storage);
        auto& builder = valueBuilder.GetDateBuilder();
        if (!storage.Validate(builder)) {
            return {};
        }
        return storage;
    }

    template<typename TStorage>
    TMaybe<TStorage> StartOf(TStorage storage, ui64 interval, const IValueBuilder& valueBuilder) {
        if (interval >= 86400000000ull) {
            // treat as StartOfDay
            SetStartOfDay(storage);
        } else {
            auto current = storage.ToTimeOfDay();
            auto rounded = current / interval * interval;
            storage.FromTimeOfDay(rounded);
        }

        auto& builder = valueBuilder.GetDateBuilder();
        if (!storage.Validate(builder)) {
            return {};
        }
        return storage;
    }

    template<typename TStorage>
    TMaybe<TStorage> EndOf(TStorage storage, ui64 interval, const IValueBuilder& valueBuilder) {
        if (interval >= 86400000000ull) {
            // treat as EndOfDay
            SetEndOfDay(storage);
        } else {
            auto current = storage.ToTimeOfDay();
            auto rounded = current / interval * interval + interval - 1;
            storage.FromTimeOfDay(rounded);
        }

        auto& builder = valueBuilder.GetDateBuilder();
        if (!storage.Validate(builder)) {
            return {};
        }
        return storage;
    }

    template<bool UseEnd>
    struct TStartEndOfBinaryKernelExec : TBinaryKernelExec<TStartEndOfBinaryKernelExec<UseEnd>> {
        template<typename TSink>
        static void Process(const IValueBuilder* valueBuilder, TBlockItem arg1, TBlockItem arg2, const TSink& sink) {
            auto& storage = Reference<TMResourceName>(arg1);
            ui64 interval = std::abs(arg2.Get<i64>());
            if (interval == 0) {
                sink(arg1);
                return;
            }

            if (auto res = (UseEnd ? EndOf<TTMStorage> : StartOf<TTMStorage>)(storage, interval, *valueBuilder)) {
                storage = res.GetRef();
                sink(arg1);
            } else {
                sink(TBlockItem{});
            }
        }
    };

    template<const char* TResourceName, auto Core>
    TUnboxedValue SimpleDatetimeToIntervalUdf(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) {
        auto result = args[0];
        ui64 interval = std::abs(args[1].Get<i64>());
        if (interval == 0) {
            return result;
        }
        auto& storage = Reference<TResourceName>(result);
        if (auto res = Core(storage, interval, *valueBuilder)) {
            storage = res.GetRef();
            return result;
        }
        return TUnboxedValuePod{};
    }

template<const char* TUdfName, auto Boundary, auto WBoundary>
class TBoundaryOfInterval: public ::NYql::NUdf::TBoxedValue {
public:
    typedef bool TTypeAwareMarker;
    static const TStringRef& Name() {
        static auto name = TStringRef(TUdfName, std::strlen(TUdfName));
        return name;
    }

    static bool DeclareSignature(
        const ::NYql::NUdf::TStringRef& name,
        ::NYql::NUdf::TType* userType,
        ::NYql::NUdf::IFunctionTypeInfoBuilder& builder,
        bool typesOnly)
    {
        if (Name() != name) {
            return false;
        }

        if (!userType) {
            // XXX: Function became polymorphic when overload for
            // wide resources was implemented. Hence, to make it
            // backward compatible with previous versions, the
            // absence of the userType is considered as using the
            // old version (i.e. without type awareness) that
            // provides implementation only for narrow dates.
            BuildSignature<TMResourceName, Boundary>(builder, typesOnly);
            return true;
        }

        builder.UserType(userType);

        const auto typeInfoHelper = builder.TypeInfoHelper();
        TTupleTypeInspector tuple(*typeInfoHelper, userType);
        Y_ENSURE(tuple, "Tuple with args and options tuples expected");
        Y_ENSURE(tuple.GetElementsCount() > 0,
                 "Tuple has to contain positional arguments");

        TTupleTypeInspector argsTuple(*typeInfoHelper, tuple.GetElementType(0));
        Y_ENSURE(argsTuple, "Tuple with args expected");
        if (argsTuple.GetElementsCount() != 2) {
            builder.SetError("Single argument expected");
            return true;
        }

        auto argType = argsTuple.GetElementType(0);

        if (const auto optType = TOptionalTypeInspector(*typeInfoHelper, argType)) {
            argType = optType.GetItemType();
        }

        TResourceTypeInspector resource(*typeInfoHelper, argType);
        if (!resource) {
            TDataTypeInspector data(*typeInfoHelper, argType);
            if (!data) {
                SetResourceExpectedError(builder, typeInfoHelper, argType);
                return true;
            }

            const auto features = NUdf::GetDataTypeInfo(NUdf::GetDataSlot(data.GetTypeId())).Features;
            if (features & NUdf::BigDateType) {
                BuildSignature<TM64ResourceName, WBoundary>(builder, typesOnly);
                return true;
            }
            if (features & (NUdf::DateType | NUdf::TzDateType)) {
                BuildSignature<TMResourceName, Boundary>(builder, typesOnly);
                return true;
            }

            SetResourceExpectedError(builder, typeInfoHelper, argType);
            return true;
        }

        if (resource.GetTag() == TStringRef::Of(TM64ResourceName)) {
            BuildSignature<TM64ResourceName, WBoundary>(builder, typesOnly);
            return true;
        }

        if (resource.GetTag() == TStringRef::Of(TMResourceName)) {
            BuildSignature<TMResourceName, Boundary>(builder, typesOnly);
            return true;
        }

        SetUnexpectedTagError(builder, resource.GetTag());
        return true;
    }
private:
    template<auto Func>
    class TImpl : public TBoxedValue {
    public:
        TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
            try {
                return Func(valueBuilder, args);
            } catch (const std::exception&) {
                    TStringBuilder sb;
                    sb << CurrentExceptionMessage();
                    sb << Endl << "[" << TStringBuf(Name()) << "]" ;
                    UdfTerminate(sb.c_str());
            }
        }
    };

    template<const char* TResourceName, auto Func>
    static void BuildSignature(NUdf::IFunctionTypeInfoBuilder& builder, bool typesOnly) {
        builder.Returns<TOptional<TResource<TResourceName>>>();
        builder.Args()->Add<TAutoMap<TResource<TResourceName>>>()
            .template Add<TAutoMap<std::conditional_t<TResourceName == TMResourceName, TInterval, TInterval64>>>();
        builder.IsStrict();
        if (!typesOnly) {
            builder.Implementation(new TImpl<Func>());
        }
    }
};

    struct TTimeOfDayKernelExec : TUnaryKernelExec<TTimeOfDayKernelExec, TReaderTraits::TResource<false>, TFixedSizeArrayBuilder<TDataType<TInterval>::TLayout, false>> {
        template<typename TSink>
        static void Process(const IValueBuilder* valueBuilder, TBlockItem item, const TSink& sink) {
            Y_UNUSED(valueBuilder);
            auto& storage = Reference<TMResourceName>(item);
            sink(TBlockItem{(TDataType<TInterval>::TLayout)storage.ToTimeOfDay()});
        }
    };

class TTimeOfDay: public ::NYql::NUdf::TBoxedValue {
public:
    typedef bool TTypeAwareMarker;
    static const ::NYql::NUdf::TStringRef& Name() {
        static auto name = TStringRef::Of("TimeOfDay");
        return name;
    }

    static bool DeclareSignature(
        const ::NYql::NUdf::TStringRef& name,
        ::NYql::NUdf::TType* userType,
        ::NYql::NUdf::IFunctionTypeInfoBuilder& builder,
        bool typesOnly)
    {
        if (Name() != name) {
            return false;
        }

        if (!userType) {
            // XXX: Function became polymorphic when overload for
            // wide resources was implemented. Hence, to make it
            // backward compatible with previous versions, the
            // absence of the userType is considered as using the
            // old version (i.e. without type awareness) that
            // provides implementation only for narrow dates.
            BuildSignature<TMResourceName>(builder, typesOnly);
            return true;
        }

        builder.UserType(userType);

        const auto typeInfoHelper = builder.TypeInfoHelper();
        TTupleTypeInspector tuple(*typeInfoHelper, userType);
        Y_ENSURE(tuple, "Tuple with args and options tuples expected");
        Y_ENSURE(tuple.GetElementsCount() > 0,
                 "Tuple has to contain positional arguments");

        TTupleTypeInspector argsTuple(*typeInfoHelper, tuple.GetElementType(0));
        Y_ENSURE(argsTuple, "Tuple with args expected");
        if (argsTuple.GetElementsCount() != 1) {
            builder.SetError("Single argument expected");
            return true;
        }

        auto argType = argsTuple.GetElementType(0);

        if (const auto optType = TOptionalTypeInspector(*typeInfoHelper, argType)) {
            argType = optType.GetItemType();
        }

        TResourceTypeInspector resource(*typeInfoHelper, argType);
        if (!resource) {
            TDataTypeInspector data(*typeInfoHelper, argType);
            if (!data) {
                SetResourceExpectedError(builder, typeInfoHelper, argType);
                return true;
            }

            const auto features = NUdf::GetDataTypeInfo(NUdf::GetDataSlot(data.GetTypeId())).Features;
            if (features & NUdf::BigDateType) {
                BuildSignature<TM64ResourceName>(builder, typesOnly);
                return true;
            }
            if (features & (NUdf::DateType | NUdf::TzDateType)) {
                BuildSignature<TMResourceName>(builder, typesOnly);
                return true;
            }

            SetResourceExpectedError(builder, typeInfoHelper, argType);
            return true;
        }

        if (resource.GetTag() == TStringRef::Of(TM64ResourceName)) {
            BuildSignature<TM64ResourceName>(builder, typesOnly);
            return true;
        }

        if (resource.GetTag() == TStringRef::Of(TMResourceName)) {
            BuildSignature<TMResourceName>(builder, typesOnly);
            return true;
        }

        SetUnexpectedTagError(builder, resource.GetTag());
        return true;
    }
private:
    template<const char* TResourceName>
    class TImpl : public TBoxedValue {
    public:
        TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
            try {
                Y_UNUSED(valueBuilder);
                auto& storage = Reference<TResourceName>(args[0]);
                return TUnboxedValuePod((i64)storage.ToTimeOfDay());
            } catch (const std::exception&) {
                    TStringBuilder sb;
                    sb << CurrentExceptionMessage();
                    sb << Endl << "[" << TStringBuf(Name()) << "]" ;
                    UdfTerminate(sb.c_str());
            }
        }
    };

    template< const char* TResourceName>
    static void BuildSignature(NUdf::IFunctionTypeInfoBuilder& builder, bool typesOnly) {
        builder.Returns<std::conditional_t<TResourceName == TMResourceName, TInterval, TInterval64>>();
        builder.Args()->Add<TAutoMap<TResource<TResourceName>>>();
        builder.IsStrict();
        if (!typesOnly) {
            builder.Implementation(new TImpl<TResourceName>());
        }
    }
};


    // Add ...

    template<auto Core>
    struct TAddKernelExec : TBinaryKernelExec<TAddKernelExec<Core>> {
        template<typename TSink>
        static void Process(const IValueBuilder* valueBuilder, TBlockItem date, TBlockItem arg, const TSink& sink) {
            sink(Core(date, arg.Get<i32>(), valueBuilder->GetDateBuilder()));
        }
    };

template<const char* TUdfName, auto Shifter, auto WShifter>
class TShift : public TBoxedValue {
public:
    typedef bool TTypeAwareMarker;

    static const TStringRef& Name() {
        static auto name = TStringRef(TUdfName, std::strlen(TUdfName));
        return name;
    }

    static bool DeclareSignature(
        const TStringRef& name,
        TType* userType,
        IFunctionTypeInfoBuilder& builder,
        bool typesOnly)
    {
        if (Name() != name) {
            return false;
        }

        if (!userType) {
            // XXX: Function became polymorphic when overload for
            // wide resources was implemented. Hence, to make it
            // backward compatible with previous versions, the
            // absence of the userType is considered as using the
            // old version (i.e. without type awareness) that
            // provides implementation only for narrow dates.
            BuildSignature<TMResourceName, Shifter>(builder, typesOnly);
            return true;
        }

        builder.UserType(userType);

        const auto typeInfoHelper = builder.TypeInfoHelper();
        TTupleTypeInspector tuple(*typeInfoHelper, userType);
        Y_ENSURE(tuple, "Tuple with args and options tuples expected");
        Y_ENSURE(tuple.GetElementsCount() > 0,
                 "Tuple has to contain positional arguments");

        TTupleTypeInspector argsTuple(*typeInfoHelper, tuple.GetElementType(0));
        Y_ENSURE(argsTuple, "Tuple with args expected");
        if (argsTuple.GetElementsCount() != 2) {
            builder.SetError("Only two arguments expected");
            return true;
        }

        auto argType = argsTuple.GetElementType(0);

        if (const auto optType = TOptionalTypeInspector(*typeInfoHelper, argType)) {
            argType = optType.GetItemType();
        }

        TResourceTypeInspector resource(*typeInfoHelper, argType);
        if (!resource) {
            TDataTypeInspector data(*typeInfoHelper, argType);
            if (!data) {
                SetResourceExpectedError(builder, typeInfoHelper, argType);
                return true;
            }

            const auto features = NUdf::GetDataTypeInfo(NUdf::GetDataSlot(data.GetTypeId())).Features;
            if (features & NUdf::BigDateType) {
                BuildSignature<TM64ResourceName, WShifter>(builder, typesOnly);
                return true;
            }
            if (features & (NUdf::DateType | NUdf::TzDateType)) {
                BuildSignature<TMResourceName, Shifter>(builder, typesOnly);
                return true;
            }

            SetResourceExpectedError(builder, typeInfoHelper, argType);
            return true;
        }

        if (resource.GetTag() == TStringRef::Of(TM64ResourceName)) {
            BuildSignature<TM64ResourceName, WShifter>(builder, typesOnly);
            return true;
        }

        if (resource.GetTag() == TStringRef::Of(TMResourceName)) {
            BuildSignature<TMResourceName, Shifter>(builder, typesOnly);
            return true;
        }

        SetUnexpectedTagError(builder, resource.GetTag());
        return true;
    }
private:
    template<auto ShiftHanler>
    class TImpl : public TBoxedValue {
    public:
        TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
            return ShiftHanler(args[0], args[1].Get<i32>(), valueBuilder->GetDateBuilder());
        }
    };

    template<const char* TResourceName, auto ShiftHandler>
    static void BuildSignature(NUdf::IFunctionTypeInfoBuilder& builder, bool typesOnly) {
        builder.Returns<TOptional<TResource<TResourceName>>>();
        builder.Args()->Add<TAutoMap<TResource<TResourceName>>>().template Add<i32>();
        builder.IsStrict();
        if (!typesOnly) {
            builder.Implementation(new TImpl<ShiftHandler>());
        }
    }
};

    template<size_t Digits, bool Trailing = true, bool Leading = true>
    struct PrintNDigits;

    template<bool Trailing, bool Leading>
    struct PrintNDigits<0U, Trailing, Leading> {
        static constexpr ui32 Miltiplier = 1U;

        template <typename T>
        static constexpr size_t Do(T, char*) { return 0U; }
    };

    template<size_t Digits, bool Trailing, bool Leading>
    struct PrintNDigits {
        using TNextNoLeadPrint = PrintNDigits<Digits - 1U, Trailing, false>;
        using TNextCommonPrint = PrintNDigits<Digits - 1U, Trailing, true>;
        static_assert(TNextNoLeadPrint::Miltiplier == TNextCommonPrint::Miltiplier);
        static constexpr ui32 Miltiplier = TNextCommonPrint::Miltiplier * 10U;

        template <typename T>
        static constexpr size_t Do(T in, char* out) {
            in %= Miltiplier;
            if (!Trailing && in == 0) {
                return 0U;
            }
            const auto digit = in / TNextCommonPrint::Miltiplier;
            if (!Leading && digit == 0) {
                return TNextNoLeadPrint::Do(in, out);
            }
            *out = "0123456789"[digit];
            return 1U + TNextCommonPrint::Do(in, ++out);
        }
    };

    // Format

    class TFormat : public TBoxedValue {
    public:
        explicit TFormat(TSourcePosition pos)
            : Pos_(pos)
        {}

        static const TStringRef& Name() {
            static auto name = TStringRef::Of("Format");
            return name;
        }

        static bool DeclareSignature(
            const TStringRef& name,
            TType*,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly)
        {
            if (Name() != name) {
                return false;
            }

            builder.OptionalArgs(1).Args()->Add<char*>()
                .Add<TOptional<bool>>().Name("AlwaysWriteFractionalSeconds");
            builder.Returns(
                builder.SimpleSignatureType<char*(TAutoMap<TResource<TM64ResourceName>>)>());
            if (!typesOnly) {
                builder.Implementation(new TFormat(builder.GetSourcePosition()));
            }

            return true;
        }

    private:
        using TPrintersList = std::vector<std::function<size_t(char*, const TUnboxedValuePod&, const IDateBuilder&)>>;

        struct TDataPrinter {
            const std::string_view Data;

            size_t operator()(char* out, const TUnboxedValuePod&, const IDateBuilder&) const {
                std::memcpy(out, Data.data(), Data.size());
                return Data.size();
            }
        };

        TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const final try {
            bool alwaysWriteFractionalSeconds = false;
            if (auto val = args[1]) {
                alwaysWriteFractionalSeconds = val.Get<bool>();
            }

            return TUnboxedValuePod(new TImpl(Pos_, args[0], alwaysWriteFractionalSeconds));
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }

        class TImpl : public TBoxedValue {
        public:
            TUnboxedValue Run(
                const IValueBuilder* valueBuilder,
                const TUnboxedValuePod* args) const override
            {
                try {
                    EMPTY_RESULT_ON_EMPTY_ARG(0);
                    const auto value = args[0];

                    auto& builder = valueBuilder->GetDateBuilder();

                    auto result = valueBuilder->NewStringNotFilled(ReservedSize_);
                    auto pos = result.AsStringRef().Data();
                    ui32 size = 0U;

                    for (const auto& printer : Printers_) {
                        if (const auto plus = printer(pos, value, builder)) {
                            size += plus;
                            pos += plus;
                        }
                    }

                    if (size < ReservedSize_) {
                        result = valueBuilder->SubString(result.Release(), 0U, size);
                    }

                    return result;
                } catch (const std::exception& e) {
                    UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
                }
            }

            TImpl(TSourcePosition pos, TUnboxedValue format, bool alwaysWriteFractionalSeconds)
                : Pos_(pos)
                , Format_(format)
            {
                const std::string_view formatView(Format_.AsStringRef());
                auto dataStart = formatView.begin();
                size_t dataSize = 0U;

                for (auto ptr = formatView.begin(); formatView.end() != ptr; ++ptr) {
                    if (*ptr != '%') {
                        ++dataSize;
                        continue;
                    }

                    if (dataSize) {
                        Printers_.emplace_back(TDataPrinter{std::string_view(&*dataStart, dataSize)});
                        ReservedSize_ += dataSize;
                        dataSize = 0U;
                    }

                    if (formatView.end() == ++ptr) {
                        ythrow yexception() << "format string ends with single %%";
                    }

                    switch (*ptr) {
                    case '%': {
                        static constexpr size_t size = 1;
                        Printers_.emplace_back([](char* out, const TUnboxedValuePod&, const IDateBuilder&) {
                            *out = '%';
                            return size;
                        });
                        ReservedSize_ += size;
                        break;
                    }
                    case 'Y': {
                        static constexpr size_t size = 6;
                        Printers_.emplace_back([](char* out, const TUnboxedValuePod& value, const IDateBuilder&) {
                            i64 year = GetYear<TM64ResourceName>(value);
                            Y_DEBUG_ABORT_UNLESS(year != 0);
                            i64 yearRepr = std::abs(year);
                            if (year < 0) {
                                *out++ = '-';
                            }
                            return (year < 0 ? 1 : 0) + PrintNDigits<size, true, false>::Do(yearRepr, out);
                        });
                        // Reserve one more slot for possible '-' char.
                        ReservedSize_ += size + 1;
                        break;
                    }
                    case 'm': {
                        static constexpr size_t size = 2;
                        Printers_.emplace_back([](char* out, const TUnboxedValuePod& value, const IDateBuilder&) {
                            return PrintNDigits<size>::Do(GetMonth<TM64ResourceName>(value), out);
                        });
                        ReservedSize_ += size;
                        break;
                    }
                    case 'd': {
                        static constexpr size_t size = 2;
                        Printers_.emplace_back([](char* out, const TUnboxedValuePod& value, const IDateBuilder&) {
                            return PrintNDigits<size>::Do(GetDay<TM64ResourceName>(value), out);
                        });
                        ReservedSize_ += size;
                        break;
                    }
                    case 'H': {
                        static constexpr size_t size = 2;
                        Printers_.emplace_back([](char* out, const TUnboxedValuePod& value, const IDateBuilder&) {
                            return PrintNDigits<size>::Do(GetHour<TM64ResourceName>(value), out);
                        });
                        ReservedSize_ += size;
                        break;
                    }
                    case 'M': {
                        static constexpr size_t size = 2;
                        Printers_.emplace_back([](char* out, const TUnboxedValuePod& value, const IDateBuilder&) {
                            return PrintNDigits<size>::Do(GetMinute<TM64ResourceName>(value), out);
                        });
                        ReservedSize_ += size;
                        break;
                    }
                    case 'S':
                        Printers_.emplace_back([alwaysWriteFractionalSeconds](char* out, const TUnboxedValuePod& value, const IDateBuilder&) {
                            constexpr size_t size = 2;
                            if (const auto microsecond = GetMicrosecond<TM64ResourceName>(value); microsecond || alwaysWriteFractionalSeconds) {
                                out += PrintNDigits<size>::Do(GetSecond<TM64ResourceName>(value), out);
                                *out++ = '.';
                                constexpr size_t msize = 6;
                                auto addSz = alwaysWriteFractionalSeconds ?
                                    PrintNDigits<msize, true>::Do(microsecond, out) :
                                    PrintNDigits<msize, false>::Do(microsecond, out);
                                return size + 1U + addSz;
                            }
                            return PrintNDigits<size>::Do(GetSecond<TM64ResourceName>(value), out);
                        });
                        ReservedSize_ += 9;
                        break;

                    case 'z': {
                        static constexpr size_t size = 5;
                        Printers_.emplace_back([](char* out, const TUnboxedValuePod& value, const IDateBuilder& builder) {
                            auto timezoneId = GetTimezoneId<TM64ResourceName>(value);
                            if (TTMStorage::IsUniversal(timezoneId)) {
                                std::memcpy(out, "+0000", size);
                                return size;
                            }
                            i32 shift;
                            if (!builder.GetTimezoneShift(GetYear<TM64ResourceName>(value),
                                                          GetMonth<TM64ResourceName>(value),
                                                          GetDay<TM64ResourceName>(value),
                                                          GetHour<TM64ResourceName>(value),
                                                          GetMinute<TM64ResourceName>(value),
                                                          GetSecond<TM64ResourceName>(value),
                                                          timezoneId, shift))
                            {
                                std::memcpy(out, "+0000", size);
                                return size;
                            }

                            *out++ = shift > 0 ? '+' : '-';
                            shift = std::abs(shift);
                            out += PrintNDigits<2U>::Do(shift / 60U, out);
                            out += PrintNDigits<2U>::Do(shift % 60U, out);
                            return size;
                        });
                        ReservedSize_ += size;
                        break;
                    }
                    case 'Z':
                        Printers_.emplace_back([](char* out, const TUnboxedValuePod& value, const IDateBuilder&) {
                            const auto timezoneId = GetTimezoneId<TM64ResourceName>(value);
                            const auto tzName = NUdf::GetTimezones()[timezoneId];
                            std::memcpy(out, tzName.data(), std::min(tzName.size(), MAX_TIMEZONE_NAME_LEN));
                            return tzName.size();
                        });
                        ReservedSize_ += MAX_TIMEZONE_NAME_LEN;
                        break;
                    case 'b': {
                        static constexpr size_t size = 3;
                        Printers_.emplace_back([](char* out, const TUnboxedValuePod& value, const IDateBuilder&) {
                            static constexpr std::string_view mp[] {
                                "Jan",
                                "Feb",
                                "Mar",
                                "Apr",
                                "May",
                                "Jun",
                                "Jul",
                                "Aug",
                                "Sep",
                                "Oct",
                                "Nov",
                                "Dec"
                            };
                            auto month = GetMonth<TM64ResourceName>(value);
                            Y_ENSURE(month > 0 && month <= sizeof(mp) / sizeof(mp[0]), "Invalid month value");
                            std::memcpy(out, mp[month - 1].data(), size);
                            return size;
                        });
                        ReservedSize_ += size;
                        break;
                    }
                    case 'B': {
                        Printers_.emplace_back([](char* out, const TUnboxedValuePod& value, const IDateBuilder&) {
                            static constexpr std::string_view mp[] {
                                "January",
                                "February",
                                "March",
                                "April",
                                "May",
                                "June",
                                "July",
                                "August",
                                "September",
                                "October",
                                "November",
                                "December"
                            };
                            auto month = GetMonth<TM64ResourceName>(value);
                            Y_ENSURE(month > 0 && month <= sizeof(mp) / sizeof(mp[0]), "Invalid month value");
                            const std::string_view monthFullName = mp[month - 1];
                            std::memcpy(out, monthFullName.data(), monthFullName.size());
                            return monthFullName.size();
                        });
                        ReservedSize_ += 9U; // MAX_MONTH_FULL_NAME_LEN
                        break;
                    }
                    default:
                        ythrow yexception() << "invalid format character: " << *ptr;
                    }

                    dataStart = ptr + 1U;
                }

                if (dataSize) {
                    Printers_.emplace_back(TDataPrinter{std::string_view(dataStart, dataSize)});
                    ReservedSize_ += dataSize;
                }
            }

        private:
            const TSourcePosition Pos_;

            TUnboxedValue Format_;
            TPrintersList Printers_{};
            size_t ReservedSize_ = 0;
        };

        const TSourcePosition Pos_;
    };

    template<size_t Digits, bool Variable = false>
    struct ParseNDigits;

    template<bool Variable>
    struct ParseNDigits<0U, Variable> {
        template <typename T>
        static constexpr bool Do(std::string_view::const_iterator&, T&) {
            return true;
        }
    };

    template<size_t Digits, bool Variable>
    struct ParseNDigits {
        template <typename T>
        static constexpr bool Do(std::string_view::const_iterator& it, T& out) {
            const auto d = *it;
            if (!std::isdigit(d)) {
                // XXX: If the current char is not a digit, the
                // parsing succeeds iff there are no more digits
                // to be parsed (see the class specialization
                // above) or there are given less than N digits
                // to be parsed.
                if constexpr (Variable) {
                    return true;
                }
                return false;
            }
            out *= 10U;
            out += d - '0';
            return ParseNDigits<Digits - 1U, Variable>::Do(++it, out);
        }
    };

    // Parse

    template<const char* TUdfName, const char* TResourceName>
    class TParse : public TBoxedValue {
    public:
        class TFactory : public TBoxedValue {
        public:
            explicit TFactory(TSourcePosition pos)
                : Pos_(pos)
            {}

        private:
            TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const final try {
                return TUnboxedValuePod(new TParse(args[0], Pos_));
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }

            const TSourcePosition Pos_;
        };

        static const TStringRef& Name() {
            static auto name = TStringRef(TUdfName, std::strlen(TUdfName));
            return name;
        }

        static bool DeclareSignature(
            const TStringRef& name,
            TType*,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly)
        {
            if (Name() != name) {
                return false;
            }

            builder.OptionalArgs(1).Args()->Add<char*>()
                .template Add<TOptional<ui16>>();
            builder.Returns(
                builder.SimpleSignatureType<TOptional<TResource<TResourceName>>(TAutoMap<char*>)>());
            if (!typesOnly) {
                builder.Implementation(new TParse::TFactory(builder.GetSourcePosition()));
            }

            return true;
        }

    private:
        const TSourcePosition Pos_;
        const TUnboxedValue Format_;

        std::vector<std::function<bool(std::string_view::const_iterator& it, size_t, TUnboxedValuePod&, const IDateBuilder&)>> Scanners_;

        struct TDataScanner {
            const std::string_view Data_;

            bool operator()(std::string_view::const_iterator& it, size_t limit, TUnboxedValuePod&, const IDateBuilder&) const {
                if (limit < Data_.size() || !std::equal(Data_.begin(), Data_.end(), it)) {
                    return false;
                }
                std::advance(it, Data_.size());
                return true;
            }
        };

        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override
        {
            try {
                EMPTY_RESULT_ON_EMPTY_ARG(0);

                const std::string_view buffer = args[0].AsStringRef();

                TUnboxedValuePod result(0);
                auto& storage = Reference<TResourceName>(result);
                storage.MakeDefault();

                auto& builder = valueBuilder->GetDateBuilder();

                auto it = buffer.begin();
                for (const auto& scanner : Scanners_) {
                    if (!scanner(it, std::distance(it, buffer.end()), result, builder)) {
                        return TUnboxedValuePod();
                    }
                }

                if (buffer.end() != it || !storage.Validate(builder)) {
                    return TUnboxedValuePod();
                }
                return result;
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

        TParse(const TUnboxedValuePod& runConfig, TSourcePosition pos)
            : Pos_(pos)
            , Format_(runConfig)
        {
            const std::string_view formatView(Format_.AsStringRef());
            auto dataStart = formatView.begin();
            size_t dataSize = 0U;

            for (auto ptr = formatView.begin(); formatView.end() != ptr; ++ptr) {
                if (*ptr != '%') {
                    ++dataSize;
                    continue;
                }

                if (dataSize) {
                    Scanners_.emplace_back(TDataScanner{std::string_view(&*dataStart, dataSize)});
                    dataSize = 0;
                }

                if (++ptr == formatView.end()) {
                    ythrow yexception() << "format string ends with single %%";
                }

                switch (*ptr) {
                case '%':
                    Scanners_.emplace_back([](std::string_view::const_iterator& it, size_t limit, TUnboxedValuePod&, const IDateBuilder&) {
                        return limit > 0U && *it++ == '%';
                    });
                    break;

                case 'Y': {
                    Scanners_.emplace_back([](std::string_view::const_iterator& it, size_t limit, TUnboxedValuePod& result, const IDateBuilder&) {
                        if constexpr (TResourceName == TMResourceName) {
                            static constexpr size_t size = 4;
                            ui32 year = 0U;
                            if (limit < size || !ParseNDigits<size>::Do(it, year) || !ValidateYear<TMResourceName>(year)) {
                                return false;
                            }
                            SetYear<TMResourceName>(result, year);
                        } else {
                            static constexpr size_t size = 6;
                            i64 year = 0LL;
                            i64 negative = 1LL;
                            if (*it == '-') {
                                negative = -1LL;
                                it++;
                            }
                            if (!ParseNDigits<size, true>::Do(it, year) || !ValidateYear<TM64ResourceName>(negative * year)) {
                                return false;
                            }
                            SetYear<TM64ResourceName>(result, negative * year);
                        }
                        return true;
                    });
                    break;
                }
                case 'm': {
                    static constexpr size_t size = 2;
                    Scanners_.emplace_back([](std::string_view::const_iterator& it, size_t limit, TUnboxedValuePod& result, const IDateBuilder&) {
                        ui32 month = 0U;
                        if (limit < size || !ParseNDigits<size>::Do(it, month) || !ValidateMonth(month)) {
                            return false;
                        }
                        SetMonth<TResourceName>(result, month);
                        return true;
                    });
                    break;
                }
                case 'd': {
                    static constexpr size_t size = 2;
                    Scanners_.emplace_back([](std::string_view::const_iterator& it, size_t limit, TUnboxedValuePod& result, const IDateBuilder&) {
                        ui32 day = 0U;
                        if (limit < size || !ParseNDigits<size>::Do(it, day) || !ValidateDay(day)) {
                            return false;
                        }
                        SetDay<TResourceName>(result, day);
                        return true;
                    });
                    break;
                }
                case 'H': {
                    static constexpr size_t size = 2;
                    Scanners_.emplace_back([](std::string_view::const_iterator& it, size_t limit, TUnboxedValuePod& result, const IDateBuilder&) {
                        ui32 hour = 0U;
                        if (limit < size || !ParseNDigits<size>::Do(it, hour) || !ValidateHour(hour)) {
                            return false;
                        }
                        SetHour<TResourceName>(result, hour);
                        return true;
                    });
                    break;
                }
                case 'M': {
                    static constexpr size_t size = 2;
                    Scanners_.emplace_back([](std::string_view::const_iterator& it, size_t limit, TUnboxedValuePod& result, const IDateBuilder&) {
                        ui32 minute = 0U;
                        if (limit < size || !ParseNDigits<size>::Do(it, minute) || !ValidateMinute(minute)) {
                            return false;
                        }
                        SetMinute<TResourceName>(result, minute);
                        return true;
                    });
                    break;
                }
                case 'S': {
                    static constexpr size_t size = 2;
                    Scanners_.emplace_back([](std::string_view::const_iterator& it, size_t limit, TUnboxedValuePod& result, const IDateBuilder&) {
                        ui32 second = 0U;
                        if (limit < size || !ParseNDigits<size>::Do(it, second) || !ValidateSecond(second)) {
                            return false;
                        }
                        SetSecond<TResourceName>(result, second);
                        limit -= size;

                        if (!limit || *it != '.') {
                            return true;
                        }

                        ++it;
                        --limit;
                        ui32 usec = 0U;

                        size_t digits = 6U;
                        for (; limit; --limit) {
                            const auto c = *it;
                            if (!digits || !std::isdigit(c)) {
                                break;
                            }
                            usec *= 10U;
                            usec += c - '0';
                            ++it;
                            --digits;
                        }
                        for (; !digits && limit && std::isdigit(*it); --limit, ++it);
                        while (digits--) {
                            usec *= 10U;
                        }
                        SetMicrosecond<TResourceName>(result, usec);
                        return true;
                    });
                    break;
                }
                case 'Z':
                    Scanners_.emplace_back([](std::string_view::const_iterator& it, size_t limit, TUnboxedValuePod& result, const IDateBuilder& builder) {
                        const auto start = it;
                        while (limit > 0 && (std::isalnum(*it) || *it == '/' || *it == '_' || *it == '-' || *it == '+')) {
                            ++it;
                            --limit;
                        }
                        const auto size = std::distance(start, it);

                        ui32 timezoneId;
                        if (!builder.FindTimezoneId(TStringRef(&*start, size), timezoneId)) {
                            return false;
                        }
                        SetTimezoneId<TResourceName>(result, timezoneId);
                        return true;
                    });
                    break;
                case 'b': {
                    static constexpr size_t size = 3;
                    Scanners_.emplace_back([](std::string_view::const_iterator& it, size_t limit, TUnboxedValuePod& result, const IDateBuilder&) {
                        const auto start = it;
                        size_t cnt = 0U;
                        while (limit > 0 && cnt < size && std::isalpha(*it)) {
                            ++it;
                            ++cnt;
                            --limit;
                        }
                        const std::string_view monthName{start, cnt};
                        ui8 month = 0U;
                        if (cnt < size || !ValidateMonthShortName(monthName, month)) {
                            return false;
                        }
                        SetMonth<TResourceName>(result, month);
                        return true;
                    });
                    break;
                }
                case 'B': {
                    Scanners_.emplace_back([](std::string_view::const_iterator& it, size_t limit, TUnboxedValuePod& result, const IDateBuilder&) {
                        const auto start = it;
                        size_t cnt = 0U;
                        while (limit > 0 && std::isalpha(*it)) {
                            ++it;
                            ++cnt;
                            --limit;
                        }

                        const std::string_view monthName{start, cnt};
                        ui8 month = 0U;
                        if (!ValidateMonthFullName(monthName, month)) {
                            return false;
                        }
                        SetMonth<TResourceName>(result, month);
                        return true;
                    });
                    break;
                }
                default:
                    ythrow yexception() << "invalid format character: " << *ptr;
                }

                dataStart = ptr + 1U;
            }

            if (dataSize) {
                Scanners_.emplace_back(TDataScanner{std::string_view(&*dataStart, dataSize)});
            }
        }
    };

#define PARSE_SPECIFIC_FORMAT(format)                                                                                              \
    SIMPLE_STRICT_UDF(TParse##format, TOptional<TResource<TMResourceName>>(TAutoMap<char*>)) {                                     \
        auto str = args[0].AsStringRef();                                                                                          \
        TInstant instant;                                                                                                          \
        if (!TInstant::TryParse##format(TStringBuf(str.Data(), str.Size()), instant) || instant.Seconds() >= NUdf::MAX_DATETIME) { \
            return TUnboxedValuePod();                                                                                             \
        }                                                                                                                          \
        auto& builder = valueBuilder->GetDateBuilder();                                                                            \
        TUnboxedValuePod result(0);                                                                                                \
        auto& storage = Reference<TMResourceName>(result);                                                                         \
        storage.FromTimestamp(builder, instant.MicroSeconds());                                                                    \
        return result;                                                                                                             \
    }

    PARSE_SPECIFIC_FORMAT(Rfc822);
    PARSE_SPECIFIC_FORMAT(Iso8601);
    PARSE_SPECIFIC_FORMAT(Http);
    PARSE_SPECIFIC_FORMAT(X509);

    SIMPLE_MODULE(TDateTime2Module,
        TUserDataTypeFuncFactory<true, true, SplitUDF, TSplit,
            TDate,
            TDatetime,
            TTimestamp,
            TTzDate,
            TTzDatetime,
            TTzTimestamp,
            TDate32,
            TDatetime64,
            TTimestamp64,
            TTzDate32,
            TTzDatetime64,
            TTzTimestamp64>,

        TMakeDate,
        TMakeDatetime,
        TMakeTimestamp,
        TMakeTzDate,
        TMakeTzDatetime,
        TMakeTzTimestamp,

        TConvert,

        TMakeDate32,
        TMakeDatetime64,
        TMakeTimestamp64,
        TMakeTzDate32,
        TMakeTzDatetime64,
        TMakeTzTimestamp64,

        TGetDateComponent<GetYearUDF, ui16, GetYear<TMResourceName>, i32, GetYear<TM64ResourceName>>,
        TGetDateComponent<GetDayOfYearUDF, ui16, GetDayOfYear<TMResourceName>, ui16, GetDayOfYear<TM64ResourceName>>,
        TGetDateComponent<GetMonthUDF, ui8, GetMonth<TMResourceName>, ui8, GetMonth<TM64ResourceName>>,
        TGetDateComponentName<GetMonthNameUDF, GetMonthName<TMResourceName>, GetMonthName<TM64ResourceName>>,
        TGetDateComponent<GetWeekOfYearUDF, ui8, GetWeekOfYear<TMResourceName>, ui8, GetWeekOfYear<TM64ResourceName>>,
        TGetDateComponent<GetWeekOfYearIso8601UDF, ui8, GetWeekOfYearIso8601<TMResourceName>, ui8, GetWeekOfYearIso8601<TM64ResourceName>>,
        TGetDateComponent<GetDayOfMonthUDF, ui8, GetDay<TMResourceName>, ui8, GetDay<TM64ResourceName>>,
        TGetDateComponent<GetDayOfWeekUDF, ui8, GetDayOfWeek<TMResourceName>, ui8, GetDayOfWeek<TM64ResourceName>>,
        TGetDateComponentName<GetDayOfWeekNameUDF, GetDayOfWeekName<TMResourceName>, GetDayOfWeekName<TM64ResourceName>>,
        TGetTimeComponent<GetHourUDF, ui8, GetHour<TMResourceName>, GetHour<TM64ResourceName>, 1u, 3600u, 24u, false>,
        TGetTimeComponent<GetMinuteUDF, ui8, GetMinute<TMResourceName>, GetMinute<TM64ResourceName>, 1u, 60u, 60u, false>,
        TGetTimeComponent<GetSecondUDF, ui8, GetSecond<TMResourceName>, GetSecond<TM64ResourceName>, 1u, 1u, 60u, false>,
        TGetTimeComponent<GetMillisecondOfSecondUDF, ui32, GetMicrosecond<TMResourceName>, GetMicrosecond<TM64ResourceName>, 1000u, 1000u, 1000u, true>,
        TGetTimeComponent<GetMicrosecondOfSecondUDF, ui32, GetMicrosecond<TMResourceName>, GetMicrosecond<TM64ResourceName>, 1u, 1u, 1000000u, true>,
        TGetDateComponent<GetTimezoneIdUDF, ui16, GetTimezoneId<TMResourceName>, ui16, GetTimezoneId<TM64ResourceName>>,
        TGetDateComponentName<GetTimezoneNameUDF, GetTimezoneName<TMResourceName>, GetTimezoneName<TM64ResourceName>>,

        TUpdate,

        TFromSeconds,
        TFromMilliseconds,
        TFromMicroseconds,

        TFromSeconds64,
        TFromMilliseconds64,
        TFromMicroseconds64,

        TIntervalFromDays,
        TIntervalFromHours,
        TIntervalFromMinutes,
        TIntervalFromSeconds,
        TIntervalFromMilliseconds,
        TIntervalFromMicroseconds,

        TInterval64FromDays,
        TInterval64FromHours,
        TInterval64FromMinutes,
        TInterval64FromSeconds,
        TInterval64FromMilliseconds,
        TInterval64FromMicroseconds,

        TToConverter<ToDaysUDF, i32, i32, UsecondsInDay>,
        TToConverter<ToHoursUDF, i32, i64, UsecondsInHour>,
        TToConverter<ToMinutesUDF, i32, i64, UsecondsInMinute>,

        TBoundaryOf<StartOfYearUDF, SimpleDatetimeToDatetimeUdf<TMResourceName, StartOfYear<TTMStorage>>,
                                    SimpleDatetimeToDatetimeUdf<TM64ResourceName, StartOfYear<TTM64Storage>>>,
        TBoundaryOf<StartOfQuarterUDF, SimpleDatetimeToDatetimeUdf<TMResourceName, StartOfQuarter<TTMStorage>>,
                                       SimpleDatetimeToDatetimeUdf<TM64ResourceName, StartOfQuarter<TTM64Storage>>>,
        TBoundaryOf<StartOfMonthUDF, SimpleDatetimeToDatetimeUdf<TMResourceName, StartOfMonth<TTMStorage>>,
                                     SimpleDatetimeToDatetimeUdf<TM64ResourceName, StartOfMonth<TTM64Storage>>>,
        TBoundaryOf<StartOfWeekUDF, SimpleDatetimeToDatetimeUdf<TMResourceName, StartOfWeek<TTMStorage>>,
                                    SimpleDatetimeToDatetimeUdf<TM64ResourceName, StartOfWeek<TTM64Storage>>>,
        TBoundaryOf<StartOfDayUDF, SimpleDatetimeToDatetimeUdf<TMResourceName, StartOfDay<TTMStorage>>,
                                    SimpleDatetimeToDatetimeUdf<TM64ResourceName, StartOfDay<TTM64Storage>>>,
        TBoundaryOfInterval<StartOfUDF, SimpleDatetimeToIntervalUdf<TMResourceName, StartOf<TTMStorage>>,
                                        SimpleDatetimeToIntervalUdf<TM64ResourceName, StartOf<TTM64Storage>>>,
        TTimeOfDay,

        TShift<ShiftYearsUDF, DoAddYears<TMResourceName>, DoAddYears<TM64ResourceName>>,
        TShift<ShiftQuartersUDF, DoAddQuarters<TMResourceName>, DoAddQuarters<TM64ResourceName>>,
        TShift<ShiftMonthsUDF, DoAddMonths<TMResourceName>, DoAddMonths<TM64ResourceName>>,

        TBoundaryOf<EndOfYearUDF, SimpleDatetimeToDatetimeUdf<TMResourceName, EndOfYear<TTMStorage>>,
                                  SimpleDatetimeToDatetimeUdf<TM64ResourceName, EndOfYear<TTM64Storage>>>,
        TBoundaryOf<EndOfQuarterUDF, SimpleDatetimeToDatetimeUdf<TMResourceName, EndOfQuarter<TTMStorage>>,
                                     SimpleDatetimeToDatetimeUdf<TM64ResourceName, EndOfQuarter<TTM64Storage>>>,
        TBoundaryOf<EndOfMonthUDF, SimpleDatetimeToDatetimeUdf<TMResourceName, EndOfMonth<TTMStorage>>,
                                  SimpleDatetimeToDatetimeUdf<TM64ResourceName, EndOfMonth<TTM64Storage>>>,
        TBoundaryOf<EndOfWeekUDF, SimpleDatetimeToDatetimeUdf<TMResourceName, EndOfWeek<TTMStorage>>,
                                 SimpleDatetimeToDatetimeUdf<TM64ResourceName, EndOfWeek<TTM64Storage>>>,
        TBoundaryOf<EndOfDayUDF, SimpleDatetimeToDatetimeUdf<TMResourceName, EndOfDay<TTMStorage>>,
                                 SimpleDatetimeToDatetimeUdf<TM64ResourceName, EndOfDay<TTM64Storage>>>,
        TBoundaryOfInterval<EndOfUDF, SimpleDatetimeToIntervalUdf<TMResourceName, EndOf<TTMStorage>>,
                                      SimpleDatetimeToIntervalUdf<TM64ResourceName, EndOf<TTM64Storage>>>,

        TToUnits<ToSecondsUDF, ui32, i64, 1>,
        TToUnits<ToMillisecondsUDF, ui64, i64, 1000>,
        TToUnits<ToMicrosecondsUDF, ui64, i64, 1000000>,

        TFormat,
        TParse<ParseUDF, TMResourceName>,
        TParse<Parse64UDF, TM64ResourceName>,

        TParseRfc822,
        TParseIso8601,
        TParseHttp,
        TParseX509
    )
}

REGISTER_MODULES(TDateTime2Module)
