#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/library/yql/public/udf/tz/udf_tz.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/minikql/datetime/datetime.h>
#include <ydb/library/yql/minikql/datetime/datetime64.h>

#include <ydb/library/yql/public/udf/arrow/udf_arrow_helpers.h>

#include <util/datetime/base.h>

using namespace NKikimr;
using namespace NUdf;
using namespace NYql::DateTime;

extern const char SplitName[] = "Split";
extern const char ToSecondsName[] = "ToSeconds";
extern const char ToMillisecondsName[] = "ToMilliseconds";
extern const char ToMicrosecondsName[] = "ToMicroseconds";
extern const char GetHourName[] = "GetHour";
extern const char GetMinuteName[] = "GetMinute";
extern const char GetSecondName[] = "GetSecond";
extern const char GetMillisecondOfSecondName[] = "GetMillisecondOfSecond";
extern const char GetMicrosecondOfSecondName[] = "GetMicrosecondOfSecond";

extern const char TMResourceName[] = "DateTime2.TM";
extern const char TM64ResourceName[] = "DateTime2.TM64";

const auto UsecondsInDay = 86400000000ll;
const auto UsecondsInHour = 3600000000ll;
const auto UsecondsInMinute = 60000000ll;
const auto UsecondsInSecond = 1000000ll;
const auto UsecondsInMilliseconds = 1000ll;

template <const char* TFuncName, typename TResult, ui32 ScaleAfterSeconds>
class TToUnits {
public:
    typedef bool TTypeAwareMarker;
    using TSignedResult = typename std::make_signed<TResult>::type;

    static TResult DateCore(ui16 value) {
        return value * ui32(86400) * TResult(ScaleAfterSeconds);
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

    static TResult TimestampCore(ui64 value) {
        return TResult(value / (1000000u / ScaleAfterSeconds));
    }

    static TSignedResult IntervalCore(i64 value) {
        return TSignedResult(value / (1000000u / ScaleAfterSeconds));
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

        try {
            auto typeInfoHelper = builder.TypeInfoHelper();
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
                builder.SetError("Expected data type");
                return true;
            }

            auto typeId = data.GetTypeId();
            if (!(typeId == TDataType<TDate>::Id || typeId == TDataType<TTzDate>::Id ||
                typeId == TDataType<TDatetime>::Id || typeId == TDataType<TTzDatetime>::Id ||
                typeId == TDataType<TTimestamp>::Id || typeId == TDataType<TTzTimestamp>::Id ||
                typeId == TDataType<TInterval>::Id)) {
                builder.SetError(TStringBuilder() << "Type " << GetDataTypeInfo(GetDataSlot(typeId)).Name << " is not supported");
            }

            builder.Args()->Add(argsTuple.GetElementType(0)).Done();
            const TType* retType;
            if (typeId != TDataType<TInterval>::Id) {
                retType = builder.SimpleType<TResult>();
            } else {
                retType = builder.SimpleType<TSignedResult>();
            }

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

            builder.UserType(userType);
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
                }

                if (typeId == TDataType<TInterval>::Id) {
                    if (block) {
                        builder.Implementation(new TSimpleArrowUdfImpl(argBlockTypes, outputType, block.IsScalar(),
                            UnaryPreallocatedExecImpl<i64, TSignedResult, IntervalCore>, builder, TString(name), arrow::compute::NullHandling::INTERSECTION));
                    } else {
                        builder.Implementation(new TUnaryOverOptionalImpl<i64, TSignedResult, IntervalCore>());
                    }
                }
            }
        } catch (const std::exception& e) {
            builder.SetError(TStringBuf(e.what()));
        }

        return true;
    }
};

template <const char* TFuncName, typename TFieldStorage, TFieldStorage (*FieldFunc)(const TUnboxedValuePod&), ui32 Divisor, ui32 Scale, ui32 Limit, bool Fractional>
struct TGetTimeComponent {
    typedef bool TTypeAwareMarker;

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

    class TImpl : public TBoxedValue {
    public:
        TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
            Y_UNUSED(valueBuilder);
            if (!args[0]) {
                return {};
            }

            return TUnboxedValuePod(TFieldStorage((FieldFunc(args[0])) / Divisor));
        }
    };

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

        try {
            auto typeInfoHelper = builder.TypeInfoHelper();
            TTupleTypeInspector tuple(*typeInfoHelper, userType);
            if (tuple) {
                Y_ENSURE(tuple.GetElementsCount() > 0);
                TTupleTypeInspector argsTuple(*typeInfoHelper, tuple.GetElementType(0));
                Y_ENSURE(argsTuple);
                if (argsTuple.GetElementsCount() != 1) {
                    builder.SetError("Expected one argument");
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

                TResourceTypeInspector res(*typeInfoHelper, argType);
                if (!res) {
                    TDataTypeInspector data(*typeInfoHelper, argType);
                    if (!data) {
                        builder.SetError("Expected data type");
                        return true;
                    }

                    auto typeId = data.GetTypeId();
                    if (typeId == TDataType<TDate>::Id ||
                        typeId == TDataType<TDatetime>::Id ||
                        typeId == TDataType<TTimestamp>::Id) {

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

                        builder.UserType(userType);
                        if (!typesOnly) {
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
                } else {
                    Y_ENSURE(!block);
                    if (res.GetTag() != TStringRef::Of(TMResourceName)) {
                        builder.SetError("Unexpected resource tag");
                        return true;
                    }
                }
            }

            // default implementation
            builder.Args()->Add<TResource<TMResourceName>>().Flags(ICallablePayload::TArgumentFlags::AutoMap).Done();
            builder.Returns<TFieldStorage>();
            builder.IsStrict();
            if (!typesOnly) {
                builder.Implementation(new TImpl());
            }
        } catch (const std::exception& e) {
            builder.SetError(TStringBuf(e.what()));
        }

        return true;
    }
};

namespace {

const TTMStorage& Reference(const NUdf::TUnboxedValuePod& value) {
    return *reinterpret_cast<const TTMStorage*>(value.GetRawPtr());
}

TTMStorage& Reference(NUdf::TUnboxedValuePod& value) {
    return *reinterpret_cast<TTMStorage*>(value.GetRawPtr());
}

const TTMStorage& Reference(const TBlockItem& value) {
    return *reinterpret_cast<const TTMStorage*>(value.GetRawPtr());
}

Y_DECLARE_UNUSED TTMStorage& Reference(TBlockItem& value) {
    return *reinterpret_cast<TTMStorage*>(value.GetRawPtr());
}

const TTM64Storage& Reference64(const NUdf::TUnboxedValuePod& value) {
    return *reinterpret_cast<const TTM64Storage*>(value.GetRawPtr());
}

TTM64Storage& Reference64(NUdf::TUnboxedValuePod& value) {
    return *reinterpret_cast<TTM64Storage*>(value.GetRawPtr());
}

template<typename TValue>
TValue DoAddMonths(const TValue& date, i64 months, const NUdf::IDateBuilder& builder) {
    auto result = date;
    auto& storage = Reference(result);
    if (!NYql::DateTime::DoAddMonths(storage, months, builder)) {
        return TValue{};
    }
    return result;
}

template<typename TValue>
TValue DoAddQuarters(const TValue& date, i64 quarters, const NUdf::IDateBuilder& builder) {
    return DoAddMonths(date, quarters * 3ll, builder);
}

template<typename TValue>
TValue DoAddYears(const TValue& date, i64 years, const NUdf::IDateBuilder& builder) {
    auto result = date;
    auto& storage = Reference(result);
    if (!NYql::DateTime::DoAddYears(storage, years, builder)) {
        return TValue{};
    }
    return result;
}

#define ACCESSORS(field, type)                                                  \
    template<typename TValue> \
    inline type Get##field(const TValue& tm) {                        \
        return (type)Reference(tm).field;                           \
    }                                                                           \
    template<typename TValue> \
    Y_DECLARE_UNUSED inline void Set##field(TValue& tm, type value) { \
        Reference(tm).field = value;                                \
    }

    ACCESSORS(Year, ui16)
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

    inline bool ValidateYear(ui16 year) {
        return year >= NUdf::MIN_YEAR - 1 || year <= NUdf::MAX_YEAR + 1;
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

    inline bool ValidateDatetime(ui32 datetime) {
        return datetime < MAX_DATETIME;
    }

    inline bool ValidateTimestamp(ui64 timestamp) {
        return timestamp < MAX_TIMESTAMP;
    }

    inline bool ValidateInterval(i64 interval) {
        return interval > -i64(MAX_TIMESTAMP) && interval < i64(MAX_TIMESTAMP);
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
                Split(arg, Reference(res), *valueBuilder);
                sink(res);
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << e.what()).data());
            }
        }
    };

    template <const char* ResourceName, size_t ResourceSize, typename TUserDataType>
    class TSplitBase : public TBoxedValue {
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
                    builder.Implementation(
                        new TSplitBase<ResourceName, ResourceSize, TUserDataType>(builder.GetSourcePosition()));
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
    TUnboxedValue TSplit<TDate>::Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const
    {
        try {
            EMPTY_RESULT_ON_EMPTY_ARG(0);

            auto& builder = valueBuilder->GetDateBuilder();
            TUnboxedValuePod result(0);
            auto& storage = Reference(result);
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
            auto& storage = Reference64(result);
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
            auto& storage = Reference(result);
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
            auto& storage = Reference64(result);
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
            auto& storage = Reference(result);
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
            auto& storage = Reference64(result);
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
            auto& storage = Reference(result);
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

            TUnboxedValuePod result(0);
            auto& storage = Reference64(result);
            storage.FromDate32(valueBuilder->GetDateBuilder(), args[0].Get<i32>(), args[0].GetTimezoneId());
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
            auto& storage = Reference(result);
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

            TUnboxedValuePod result(0);
            auto& storage = Reference64(result);
            storage.FromDatetime64(valueBuilder->GetDateBuilder(), args[0].Get<i64>(), args[0].GetTimezoneId());
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
            auto& storage = Reference(result);
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

            TUnboxedValuePod result(0);
            auto& storage = Reference64(result);
            storage.FromTimestamp64(valueBuilder->GetDateBuilder(), args[0].Get<i64>(), args[0].GetTimezoneId());
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
            auto& storage = Reference(item);
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
        auto& storage = Reference(args[0]);
        return TUnboxedValuePod(storage.ToDate(builder, false));
    }
    END_SIMPLE_ARROW_UDF(TMakeDate, TMakeDateKernelExec<TDate>::Do);

    SIMPLE_STRICT_UDF(TMakeDate32, TDate32(TAutoMap<TResource<TM64ResourceName>>)) {
        valueBuilder->GetDateBuilder(); // TODO
        auto& storage = Reference64(args[0]);
        return TUnboxedValuePod(storage.ToDate32());
    }

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TMakeDatetime, TDatetime(TAutoMap<TResource<TMResourceName>>)) {
        auto& builder = valueBuilder->GetDateBuilder();
        auto& storage = Reference(args[0]);
        return TUnboxedValuePod(storage.ToDatetime(builder));
    }
    END_SIMPLE_ARROW_UDF(TMakeDatetime, TMakeDateKernelExec<TDatetime>::Do);

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TMakeTimestamp, TTimestamp(TAutoMap<TResource<TMResourceName>>)) {
        auto& builder = valueBuilder->GetDateBuilder();
        auto& storage = Reference(args[0]);
        return TUnboxedValuePod(storage.ToTimestamp(builder));
    }
    END_SIMPLE_ARROW_UDF(TMakeTimestamp, TMakeDateKernelExec<TTimestamp>::Do);

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TMakeTzDate, TTzDate(TAutoMap<TResource<TMResourceName>>)) {
        auto& builder = valueBuilder->GetDateBuilder();
        auto& storage = Reference(args[0]);
        TUnboxedValuePod result(storage.ToDate(builder, true));
        result.SetTimezoneId(storage.TimezoneId);
        return result;
    }
    END_SIMPLE_ARROW_UDF(TMakeTzDate, TMakeDateKernelExec<TTzDate>::Do);

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TMakeTzDatetime, TTzDatetime(TAutoMap<TResource<TMResourceName>>)) {
        auto& builder = valueBuilder->GetDateBuilder();
        auto& storage = Reference(args[0]);
        TUnboxedValuePod result(storage.ToDatetime(builder));
        result.SetTimezoneId(storage.TimezoneId);
        return result;
    }
    END_SIMPLE_ARROW_UDF(TMakeTzDatetime, TMakeDateKernelExec<TTzDatetime>::Do);

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TMakeTzTimestamp, TTzTimestamp(TAutoMap<TResource<TMResourceName>>)) {
        auto& builder = valueBuilder->GetDateBuilder();
        auto& storage = Reference(args[0]);
        TUnboxedValuePod result(storage.ToTimestamp(builder));
        result.SetTimezoneId(storage.TimezoneId);
        return result;
    }
    END_SIMPLE_ARROW_UDF(TMakeTzTimestamp, TMakeDateKernelExec<TTzTimestamp>::Do);


    SIMPLE_STRICT_UDF(TConvert, TResource<TM64ResourceName>(TAutoMap<TResource<TMResourceName>>)) {
        Y_UNUSED(valueBuilder);
        TUnboxedValuePod result(0);
        auto& arg = Reference(args[0]);
        auto& storage = Reference64(result);
        storage.From(arg);
        return result;
    }

    SIMPLE_STRICT_UDF(TMakeDate32, TDate32(TAutoMap<TResource<TM64ResourceName>>)) {
        auto& storage = Reference64(args[0]);
        return TUnboxedValuePod(storage.ToDate32(valueBuilder->GetDateBuilder()));
    }

    SIMPLE_STRICT_UDF(TMakeDatetime64, TDatetime64(TAutoMap<TResource<TM64ResourceName>>)) {
        auto& storage = Reference64(args[0]);
        return TUnboxedValuePod(storage.ToDatetime64(valueBuilder->GetDateBuilder()));
    }

    SIMPLE_STRICT_UDF(TMakeTimestamp64, TTimestamp64(TAutoMap<TResource<TM64ResourceName>>)) {
        auto& storage = Reference64(args[0]);
        return TUnboxedValuePod(storage.ToTimestamp64(valueBuilder->GetDateBuilder()));
    }

    SIMPLE_STRICT_UDF(TMakeTzDate32, TTzDate32(TAutoMap<TResource<TM64ResourceName>>)) {
        valueBuilder->GetDateBuilder();
        auto& storage = Reference64(args[0]);
        TUnboxedValuePod result(storage.ToDate32(valueBuilder->GetDateBuilder()));
        result.SetTimezoneId(storage.TimezoneId);
        return result;
    }

    SIMPLE_STRICT_UDF(TMakeTzDatetime64, TTzDatetime64(TAutoMap<TResource<TM64ResourceName>>)) {
        valueBuilder->GetDateBuilder();
        auto& storage = Reference64(args[0]);
        TUnboxedValuePod result(storage.ToDatetime64(valueBuilder->GetDateBuilder()));
        result.SetTimezoneId(storage.TimezoneId);
        return result;
    }

    SIMPLE_STRICT_UDF(TMakeTzTimestamp64, TTzTimestamp64(TAutoMap<TResource<TM64ResourceName>>)) {
        valueBuilder->GetDateBuilder();
        auto& storage = Reference64(args[0]);
        TUnboxedValuePod result(storage.ToTimestamp64(valueBuilder->GetDateBuilder()));
        result.SetTimezoneId(storage.TimezoneId);
        return result;
    }


    // Get*

#define GET_METHOD(field, type)                                                 \
    SIMPLE_STRICT_UDF(TGet##field, type(TAutoMap<TResource<TMResourceName>>)) { \
        Y_UNUSED(valueBuilder);                                                 \
        return TUnboxedValuePod(Get##field(args[0]));                           \
    }

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

    GET_METHOD(Year, ui16)
    GET_METHOD(DayOfYear, ui16)
    GET_METHOD(Month, ui8)
    
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

    SIMPLE_STRICT_UDF(TGetMonthName, char*(TAutoMap<TResource<TMResourceName>>)) {
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
        return monthNames.at(GetMonth(*args) - 1U);
    }

    GET_METHOD(WeekOfYear, ui8)
    GET_METHOD(WeekOfYearIso8601, ui8)

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
    
    SIMPLE_STRICT_UDF(TGetDayOfMonth, ui8(TAutoMap<TResource<TMResourceName>>)) {
        Y_UNUSED(valueBuilder);
        return TUnboxedValuePod(GetDay(args[0]));
    }

    GET_METHOD(DayOfWeek, ui8)

    template<typename TValue>
    TValue GetDayNameValue(size_t idx) {
        static const std::array<TValue, 7U> dayNames = {{
            TValue::Embedded(TStringRef::Of("Monday")),
            TValue::Embedded(TStringRef::Of("Tuesday")),
            TValue::Embedded(TStringRef::Of("Wednesday")),
            TValue::Embedded(TStringRef::Of("Thursday")),
            TValue::Embedded(TStringRef::Of("Friday")),
            TValue::Embedded(TStringRef::Of("Saturday")),
            TValue::Embedded(TStringRef::Of("Sunday"))
        }};
        return dayNames.at(idx);
    }

    SIMPLE_STRICT_UDF(TGetDayOfWeekName, char*(TAutoMap<TResource<TMResourceName>>)) {
        Y_UNUSED(valueBuilder);
        return GetDayNameValue<TUnboxedValuePod>(GetDayOfWeek(*args) - 1U);
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

    GET_METHOD(TimezoneId, ui16)

    struct TTGetTimezoneNameKernelExec : TUnaryKernelExec<TTGetTimezoneNameKernelExec, TReaderTraits::TResource<false>, TStringArrayBuilder<arrow::BinaryType, false>> {
        template<typename TSink>
        static void Process(const IValueBuilder* valueBuilder, TBlockItem item, const TSink& sink) {
            Y_UNUSED(valueBuilder);
            auto timezoneId = GetTimezoneId(item);
            if (timezoneId >= NUdf::GetTimezones().size()) {
                sink(TBlockItem{});
            } else {
                sink(TBlockItem{NUdf::GetTimezones()[timezoneId]});
            }
        }
    };

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TGetTimezoneName, char*(TAutoMap<TResource<TMResourceName>>)) {
        auto timezoneId = GetTimezoneId(args[0]);
        if (timezoneId >= NUdf::GetTimezones().size()) {
            return TUnboxedValuePod();
        }
        return valueBuilder->NewString(NUdf::GetTimezones()[timezoneId]);
    }
    END_SIMPLE_ARROW_UDF(TGetTimezoneName, TTGetTimezoneNameKernelExec::Do);

    // Update

    class TUpdate : public TBoxedValue {
        const TSourcePosition Pos_;
    public:
        explicit TUpdate(TSourcePosition pos)
            : Pos_(pos)
        {}

        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override
        {
            try {
                EMPTY_RESULT_ON_EMPTY_ARG(0);
                auto result = args[0];

                if (args[1]) {
                    auto year = args[1].Get<ui16>();
                    if (!ValidateYear(year)) {
                        return TUnboxedValuePod();
                    }
                    SetYear(result, year);
                }
                if (args[2]) {
                    auto month = args[2].Get<ui8>();
                    if (!ValidateMonth(month)) {
                        return TUnboxedValuePod();
                    }
                    SetMonth(result, month);
                }
                if (args[3]) {
                    auto day = args[3].Get<ui8>();
                    if (!ValidateDay(day)) {
                        return TUnboxedValuePod();
                    }
                    SetDay(result, day);
                }
                if (args[4]) {
                    auto hour = args[4].Get<ui8>();
                    if (!ValidateHour(hour)) {
                        return TUnboxedValuePod();
                    }
                    SetHour(result, hour);
                }
                if (args[5]) {
                    auto minute = args[5].Get<ui8>();
                    if (!ValidateMinute(minute)) {
                        return TUnboxedValuePod();
                    }
                    SetMinute(result, minute);
                }
                if (args[6]) {
                    auto second = args[6].Get<ui8>();
                    if (!ValidateSecond(second)) {
                        return TUnboxedValuePod();
                    }
                    SetSecond(result, second);
                }
                if (args[7]) {
                    auto microsecond = args[7].Get<ui32>();
                    if (!ValidateMicrosecond(microsecond)) {
                        return TUnboxedValuePod();
                    }
                    SetMicrosecond(result, microsecond);
                }
                if (args[8]) {
                    auto timezoneId = args[8].Get<ui16>();
                    if (!ValidateTimezoneId(timezoneId)) {
                        return TUnboxedValuePod();
                    }
                    SetTimezoneId(result, timezoneId);
                }

                auto& builder = valueBuilder->GetDateBuilder();
                auto& storage = Reference(result);
                if (!storage.Validate(builder)) {
                    return TUnboxedValuePod();
                }
                return result;
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

        static const TStringRef& Name() {
            static auto name = TStringRef::Of("Update");
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

            auto resourceType = builder.Resource(TMResourceName);
            auto optionalResourceType = builder.Optional()->Item(resourceType).Build();

            builder.OptionalArgs(8).Args()->Add(resourceType).Flags(ICallablePayload::TArgumentFlags::AutoMap)
                .Add(builder.Optional()->Item<ui16>().Build()).Name("Year")
                .Add(builder.Optional()->Item<ui8>().Build()).Name("Month")
                .Add(builder.Optional()->Item<ui8>().Build()).Name("Day")
                .Add(builder.Optional()->Item<ui8>().Build()).Name("Hour")
                .Add(builder.Optional()->Item<ui8>().Build()).Name("Minute")
                .Add(builder.Optional()->Item<ui8>().Build()).Name("Second")
                .Add(builder.Optional()->Item<ui32>().Build()).Name("Microsecond")
                .Add(builder.Optional()->Item<ui16>().Build()).Name("TimezoneId");

            builder.Returns(optionalResourceType);

            if (!typesOnly) {
                builder.Implementation(new TUpdate(builder.GetSourcePosition()));
            }

            builder.IsStrict();
            return true;
        }
    };

    // From*
    
    BEGIN_SIMPLE_STRICT_ARROW_UDF(TFromSeconds, TOptional<TTimestamp>(TAutoMap<ui32>)) {
        Y_UNUSED(valueBuilder);
        auto res = args[0].Get<ui32>();
        if (!ValidateDatetime(res)) {
            return TUnboxedValuePod();
        }
        return TUnboxedValuePod((ui64)(res * 1000000ull));
    }

    using TFromSecondsKernel = TUnaryUnsafeFixedSizeFilterKernel<ui32, ui64, 
        [] (ui32 seconds) { return std::make_pair(ui64(seconds * 1000000ull), ValidateDatetime(seconds)); }>;
    END_SIMPLE_ARROW_UDF(TFromSeconds, TFromSecondsKernel::Do);

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TFromMilliseconds, TOptional<TTimestamp>(TAutoMap<ui64>)) {
        Y_UNUSED(valueBuilder);
        auto res = args[0].Get<ui64>();
        if (res >= MAX_TIMESTAMP / 1000u) {
            return TUnboxedValuePod();
        }
        return TUnboxedValuePod(res * 1000u);
    }

    using TFromMillisecondsKernel = TUnaryUnsafeFixedSizeFilterKernel<ui64, ui64, 
        [] (ui64 milliseconds) { return std::make_pair(ui64(milliseconds * 1000u), milliseconds < MAX_TIMESTAMP / 1000u); }>;
    END_SIMPLE_ARROW_UDF(TFromMilliseconds, TFromMillisecondsKernel::Do);

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TFromMicroseconds, TOptional<TTimestamp>(TAutoMap<ui64>)) {
        Y_UNUSED(valueBuilder);
        auto res = args[0].Get<ui64>();
        if (!ValidateTimestamp(res)) {
            return TUnboxedValuePod();
        }
        return TUnboxedValuePod(res);
    }

    using TFromMicrosecondsKernel = TUnaryUnsafeFixedSizeFilterKernel<ui64, ui64, 
        [] (ui64 timestamp) { return std::make_pair(timestamp, ValidateTimestamp(timestamp)); }>;
    END_SIMPLE_ARROW_UDF(TFromMicroseconds, TFromMicrosecondsKernel::Do);

    template <typename TInput, i64 Multiplier>
    using TIntervalFromKernel = TUnaryUnsafeFixedSizeFilterKernel<TInput, i64, 
        [] (TInput interval) { return std::make_pair(i64(interval * Multiplier), ValidateInterval(interval)); }>;

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TIntervalFromDays, TOptional<TInterval>(TAutoMap<i32>)) {
        Y_UNUSED(valueBuilder);
        const i64 res = i64(args[0].Get<i32>()) * UsecondsInDay;
        return ValidateInterval(res) ? TUnboxedValuePod(res) : TUnboxedValuePod();
    }
    END_SIMPLE_ARROW_UDF(TIntervalFromDays, (TIntervalFromKernel<i32, UsecondsInDay>::Do));

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TIntervalFromHours, TOptional<TInterval>(TAutoMap<i32>)) {
        Y_UNUSED(valueBuilder);
        const i64 res = i64(args[0].Get<i32>()) * UsecondsInHour;
        return ValidateInterval(res) ? TUnboxedValuePod(res) : TUnboxedValuePod();
    }
    END_SIMPLE_ARROW_UDF(TIntervalFromHours, (TIntervalFromKernel<i32, UsecondsInHour>::Do));

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TIntervalFromMinutes, TOptional<TInterval>(TAutoMap<i32>)) {
        Y_UNUSED(valueBuilder);
        const i64 res = i64(args[0].Get<i32>()) * UsecondsInMinute;
        return ValidateInterval(res) ? TUnboxedValuePod(res) : TUnboxedValuePod();
    }
    END_SIMPLE_ARROW_UDF(TIntervalFromMinutes, (TIntervalFromKernel<i32, UsecondsInMinute>::Do));

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TIntervalFromSeconds, TOptional<TInterval>(TAutoMap<i32>)) {
        Y_UNUSED(valueBuilder);
        const i64 res = i64(args[0].Get<i32>()) * UsecondsInSecond;
        return ValidateInterval(res) ? TUnboxedValuePod(res) : TUnboxedValuePod();
    }
    END_SIMPLE_ARROW_UDF(TIntervalFromSeconds, (TIntervalFromKernel<i32, UsecondsInSecond>::Do));

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TIntervalFromMilliseconds, TOptional<TInterval>(TAutoMap<i64>)) {
        Y_UNUSED(valueBuilder);
        const i64 res = i64(args[0].Get<i64>()) * UsecondsInMilliseconds;
        return ValidateInterval(res) ? TUnboxedValuePod(res) : TUnboxedValuePod();
    }
    END_SIMPLE_ARROW_UDF(TIntervalFromMilliseconds, (TIntervalFromKernel<i64, UsecondsInMilliseconds>::Do));

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TIntervalFromMicroseconds, TOptional<TInterval>(TAutoMap<i64>)) {
        Y_UNUSED(valueBuilder);
        const i64 res = args[0].Get<i64>();
        return ValidateInterval(res) ? TUnboxedValuePod(res) : TUnboxedValuePod();
    }
    END_SIMPLE_ARROW_UDF(TIntervalFromMicroseconds, (TIntervalFromKernel<i64, 1>::Do));

    // To*

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TToDays, i32(TAutoMap<TInterval>)) {
        Y_UNUSED(valueBuilder);
        return TUnboxedValuePod(i32(args[0].Get<i64>() / UsecondsInDay));
    }
    END_SIMPLE_ARROW_UDF_WITH_NULL_HANDLING(TToDays, 
    (UnaryPreallocatedExecImpl<i64, i32, [] (i64 arg) { return i32(arg / UsecondsInDay); }>),
    arrow::compute::NullHandling::INTERSECTION);

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TToHours, i32(TAutoMap<TInterval>)) {
        Y_UNUSED(valueBuilder);
        return TUnboxedValuePod(i32(args[0].Get<i64>() / UsecondsInHour));
    }
    END_SIMPLE_ARROW_UDF_WITH_NULL_HANDLING(TToHours, 
    (UnaryPreallocatedExecImpl<i64, i32, [] (i64 arg) { return i32(arg / UsecondsInHour); }>),
    arrow::compute::NullHandling::INTERSECTION);

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TToMinutes, i32(TAutoMap<TInterval>)) {
        Y_UNUSED(valueBuilder);
        return TUnboxedValuePod(i32(args[0].Get<i64>() / UsecondsInMinute));
    }
    END_SIMPLE_ARROW_UDF_WITH_NULL_HANDLING(TToMinutes, 
    (UnaryPreallocatedExecImpl<i64, i32, [] (i64 arg) { return i32(arg / UsecondsInMinute); }>),
    arrow::compute::NullHandling::INTERSECTION);

    // StartOf*

    template<auto Core>
    struct TStartOfKernelExec : TUnaryKernelExec<TStartOfKernelExec<Core>, TResourceBlockReader<false>, TResourceArrayBuilder<true>> {
        template<typename TSink>
        static void Process(const IValueBuilder* valueBuilder, TBlockItem item, const TSink& sink) {
            if (auto res = Core(Reference(item), *valueBuilder)) {
                Reference(item) = res.GetRef();
                sink(item);
            } else {
                sink(TBlockItem{});
            }

        }
    };

    TMaybe<TTMStorage> StartOfYear(TTMStorage storage, const IValueBuilder& valueBuilder) {
        storage.Month = 1;
        storage.Day = 1;
        storage.Hour = 0;
        storage.Minute = 0;
        storage.Second = 0;
        storage.Microsecond = 0;
        if (!storage.Validate(valueBuilder.GetDateBuilder())) {
            return {};
        }
        return storage;
    }
    BEGIN_SIMPLE_STRICT_ARROW_UDF(TStartOfYear, TOptional<TResource<TMResourceName>>(TAutoMap<TResource<TMResourceName>>)) {
        auto result = args[0];
        auto& storage = Reference(result);
        if (auto res = StartOfYear(storage, *valueBuilder)) {
            storage = res.GetRef();
            return result;
        }
        return TUnboxedValuePod{};
    }
    END_SIMPLE_ARROW_UDF(TStartOfYear, TStartOfKernelExec<StartOfYear>::Do);

    TMaybe<TTMStorage> StartOfQuarter(TTMStorage storage, const IValueBuilder& valueBuilder) {
        storage.Month = (storage.Month - 1) / 3 * 3 + 1;
        storage.Day = 1;
        storage.Hour = 0;
        storage.Minute = 0;
        storage.Second = 0;
        storage.Microsecond = 0;
        if (!storage.Validate(valueBuilder.GetDateBuilder())) {
            return {};
        }
        return storage;
    }
    BEGIN_SIMPLE_STRICT_ARROW_UDF(TStartOfQuarter, TOptional<TResource<TMResourceName>>(TAutoMap<TResource<TMResourceName>>)) {
        auto result = args[0];
        auto& storage = Reference(result);
        if (auto res = StartOfQuarter(storage, *valueBuilder)) {
            storage = res.GetRef();
            return result;
        }
        return TUnboxedValuePod{};
    }
    END_SIMPLE_ARROW_UDF(TStartOfQuarter, TStartOfKernelExec<StartOfQuarter>::Do);
    
    TMaybe<TTMStorage> StartOfMonth(TTMStorage storage, const IValueBuilder& valueBuilder) {
        storage.Day = 1;
        storage.Hour = 0;
        storage.Minute = 0;
        storage.Second = 0;
        storage.Microsecond = 0;
        if (!storage.Validate(valueBuilder.GetDateBuilder())) {
            return {};
        }
        return storage;
    }
    BEGIN_SIMPLE_STRICT_ARROW_UDF(TStartOfMonth, TOptional<TResource<TMResourceName>>(TAutoMap<TResource<TMResourceName>>)) {
        auto result = args[0];
        auto& storage = Reference(result);
        if (auto res = StartOfMonth(storage, *valueBuilder)) {
            storage = res.GetRef();
            return result;
        }
        return TUnboxedValuePod{};
    }
    END_SIMPLE_ARROW_UDF(TStartOfMonth, TStartOfKernelExec<StartOfMonth>::Do);
    
    TMaybe<TTMStorage> EndOfMonth(TTMStorage storage, const IValueBuilder& valueBuilder) {
        storage.Day = NMiniKQL::GetMonthLength(storage.Month, NMiniKQL::IsLeapYear(storage.Year));
        storage.Hour = 0;
        storage.Minute = 0;
        storage.Second = 0;
        storage.Microsecond = 0;

        if (!storage.Validate(valueBuilder.GetDateBuilder())) {
            return {};
        }
        return storage;
    }

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TEndOfMonth, TOptional<TResource<TMResourceName>>(TAutoMap<TResource<TMResourceName>>)) {
        auto result = args[0];
        auto& storage = Reference(result);
        if (auto res = EndOfMonth(storage, *valueBuilder)) {
            storage = res.GetRef();
            return result;
        }
        return TUnboxedValuePod{};
    }
    END_SIMPLE_ARROW_UDF(TEndOfMonth, TStartOfKernelExec<EndOfMonth>::Do);
    
    TMaybe<TTMStorage> StartOfWeek(TTMStorage storage, const IValueBuilder& valueBuilder) {
        const ui32 shift = 86400u * (storage.DayOfWeek - 1u);
        if (shift > storage.ToDatetime(valueBuilder.GetDateBuilder())) {
            return {};
        }
        storage.FromDatetime(valueBuilder.GetDateBuilder(), storage.ToDatetime(valueBuilder.GetDateBuilder()) - shift, storage.TimezoneId);
        storage.Hour = 0;
        storage.Minute = 0;
        storage.Second = 0;
        storage.Microsecond = 0;
        return storage;
    }

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TStartOfWeek, TOptional<TResource<TMResourceName>>(TAutoMap<TResource<TMResourceName>>)) {
        auto result = args[0];
        auto& storage = Reference(result);
        if (auto res = StartOfWeek(storage, *valueBuilder)) {
            storage = res.GetRef();
            return result;
        }
        return TUnboxedValuePod{};
    }
    END_SIMPLE_ARROW_UDF(TStartOfWeek, TStartOfKernelExec<StartOfWeek>::Do);
    
    TMaybe<TTMStorage> StartOfDay(TTMStorage storage, const IValueBuilder& valueBuilder) {
        storage.Hour = 0;
        storage.Minute = 0;
        storage.Second = 0;
        storage.Microsecond = 0;
        auto& builder = valueBuilder.GetDateBuilder();
        if (!storage.Validate(builder)) {
            return {};
        }
        return storage;
    }

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TStartOfDay, TOptional<TResource<TMResourceName>>(TAutoMap<TResource<TMResourceName>>)) {
        auto result = args[0];
        auto& storage = Reference(result);
        if (auto res = StartOfDay(storage, *valueBuilder)) {
            storage = res.GetRef();
            return result;
        }
        return TUnboxedValuePod{};
    }
    END_SIMPLE_ARROW_UDF(TStartOfDay, TStartOfKernelExec<StartOfDay>::Do);
    
    TMaybe<TTMStorage> StartOf(TTMStorage storage, ui64 interval, const IValueBuilder& valueBuilder) {
        if (interval >= 86400000000ull) {
            // treat as StartOfDay
            storage.Hour = 0;
            storage.Minute = 0;
            storage.Second = 0;
            storage.Microsecond = 0;
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

    struct TStartOfBinaryKernelExec : TBinaryKernelExec<TStartOfBinaryKernelExec> {
        template<typename TSink>
        static void Process(const IValueBuilder* valueBuilder, TBlockItem arg1, TBlockItem arg2, const TSink& sink) {
            auto& storage = Reference(arg1);
            ui64 interval = std::abs(arg2.Get<i64>());
            if (interval == 0) {
                sink(arg1);
                return;
            }

            if (auto res = StartOf(storage, interval, *valueBuilder)) {
                storage = res.GetRef();
                sink(arg1);
            } else {
                sink(TBlockItem{});
            }
        }
    };

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TStartOf, TOptional<TResource<TMResourceName>>(TAutoMap<TResource<TMResourceName>>, TAutoMap<TInterval>)) {
        auto result = args[0];
        ui64 interval = std::abs(args[1].Get<i64>());
        if (interval == 0) {
            return result;
        }
        if (auto res = StartOf(Reference(result), interval, *valueBuilder)) {
            Reference(result) = res.GetRef();
            return result;
        }
        return TUnboxedValuePod{};
    }
    END_SIMPLE_ARROW_UDF(TStartOf, TStartOfBinaryKernelExec::Do);
    
    struct TTimeOfDayKernelExec : TUnaryKernelExec<TTimeOfDayKernelExec, TReaderTraits::TResource<false>, TFixedSizeArrayBuilder<TDataType<TInterval>::TLayout, false>> {
        template<typename TSink>
        static void Process(const IValueBuilder* valueBuilder, TBlockItem item, const TSink& sink) {
            Y_UNUSED(valueBuilder);
            auto& storage = Reference(item);
            sink(TBlockItem{(TDataType<TInterval>::TLayout)storage.ToTimeOfDay()});
        }
    };

    const auto timeOfDayKernelExecDo = TTimeOfDayKernelExec::Do;
    BEGIN_SIMPLE_STRICT_ARROW_UDF(TTimeOfDay, TInterval(TAutoMap<TResource<TMResourceName>>)) {
        Y_UNUSED(valueBuilder);
        auto& storage = Reference(args[0]);
        return TUnboxedValuePod((i64)storage.ToTimeOfDay());
    }
    END_SIMPLE_ARROW_UDF(TTimeOfDay, timeOfDayKernelExecDo);


    // Add ...

    template<auto Core>
    struct TAddKernelExec : TBinaryKernelExec<TAddKernelExec<Core>> {
        template<typename TSink>
        static void Process(const IValueBuilder* valueBuilder, TBlockItem date, TBlockItem arg, const TSink& sink) {
            sink(Core(date, arg.Get<i32>(), valueBuilder->GetDateBuilder()));
        }
    };
    
    BEGIN_SIMPLE_STRICT_ARROW_UDF(TShiftYears, TOptional<TResource<TMResourceName>>(TAutoMap<TResource<TMResourceName>>, i32)) {
        return DoAddYears(args[0], args[1].Get<i32>(), valueBuilder->GetDateBuilder());
    }
    END_SIMPLE_ARROW_UDF(TShiftYears, TAddKernelExec<DoAddYears<TBlockItem>>::Do);

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TShiftQuarters, TOptional<TResource<TMResourceName>>(TAutoMap<TResource<TMResourceName>>, i32)) {
        return DoAddQuarters(args[0], args[1].Get<i32>(), valueBuilder->GetDateBuilder());
    }
    END_SIMPLE_ARROW_UDF(TShiftQuarters, TAddKernelExec<DoAddQuarters<TBlockItem>>::Do);

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TShiftMonths, TOptional<TResource<TMResourceName>>(TAutoMap<TResource<TMResourceName>>, i32)) {
        return DoAddMonths(args[0], args[1].Get<i32>(), valueBuilder->GetDateBuilder());
    }
    END_SIMPLE_ARROW_UDF(TShiftMonths, TAddKernelExec<DoAddMonths<TBlockItem>>::Do);

    template<size_t Digits, bool Exacly = true>
    struct PrintNDigits;

    template<bool Exacly>
    struct PrintNDigits<0U, Exacly> {
        static constexpr ui32 Miltiplier = 1U;

        template <typename T>
        static constexpr size_t Do(T, char*) { return 0U; }
    };

    template<size_t Digits, bool Exacly>
    struct PrintNDigits {
        using TNextPrint = PrintNDigits<Digits - 1U, Exacly>;
        static constexpr ui32 Miltiplier = TNextPrint::Miltiplier * 10U;

        template <typename T>
        static constexpr size_t Do(T in, char* out) {
            in %= Miltiplier;
            if (Exacly || in) {
                *out = "0123456789"[in / TNextPrint::Miltiplier];
                return 1U + TNextPrint::Do(in, ++out);
            }
            return 0U;
        }
    };

    // Format

    class TFormat : public TBoxedValue {
    public:
        class TFactory : public TBoxedValue {
        public:
            explicit TFactory(TSourcePosition pos)
                : Pos_(pos)
            {}

        private:
            TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const final try {
                return TUnboxedValuePod(new TFormat(args[0], Pos_));
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
            const TSourcePosition Pos_;
        };

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

            auto resourceType = builder.Resource(TMResourceName);

            builder.Args()->Add(resourceType).Flags(ICallablePayload::TArgumentFlags::AutoMap);
            builder.RunConfig<char*>().Returns<char*>();

            if (!typesOnly) {
                builder.Implementation(new TFormat::TFactory(builder.GetSourcePosition()));
            }

            return true;
        }

    private:
        const TSourcePosition Pos_;
        const TUnboxedValue Format_;
        std::vector<std::function<size_t(char*, const TUnboxedValuePod&, const IDateBuilder&)>> Printers_;

        size_t ReservedSize_;

        struct TDataPrinter {
            const std::string_view Data;

            size_t operator()(char* out, const TUnboxedValuePod&, const IDateBuilder&) const {
                std::memcpy(out, Data.data(), Data.size());
                return Data.size();
            }
        };

    private:
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

        TFormat(const TUnboxedValuePod& runConfig, TSourcePosition pos)
            : Pos_(pos)
            , Format_(runConfig)
        {
            const std::string_view formatView(Format_.AsStringRef());
            auto dataStart = formatView.begin();
            size_t dataSize = 0U;
            ReservedSize_ = 0U;

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
                    static constexpr size_t size = 4;
                    Printers_.emplace_back([](char* out, const TUnboxedValuePod& value, const IDateBuilder&) {
                        return PrintNDigits<size>::Do(GetYear(value), out);
                    });
                    ReservedSize_ += size;
                    break;
                }
                case 'm': {
                    static constexpr size_t size = 2;
                    Printers_.emplace_back([](char* out, const TUnboxedValuePod& value, const IDateBuilder&) {
                        return PrintNDigits<size>::Do(GetMonth(value), out);
                    });
                    ReservedSize_ += size;
                    break;
                }
                case 'd': {
                    static constexpr size_t size = 2;
                    Printers_.emplace_back([](char* out, const TUnboxedValuePod& value, const IDateBuilder&) {
                        return PrintNDigits<size>::Do(GetDay(value), out);
                    });
                    ReservedSize_ += size;
                    break;
                }
                case 'H': {
                    static constexpr size_t size = 2;
                    Printers_.emplace_back([](char* out, const TUnboxedValuePod& value, const IDateBuilder&) {
                        return PrintNDigits<size>::Do(GetHour(value), out);
                    });
                    ReservedSize_ += size;
                    break;
                }
                case 'M': {
                    static constexpr size_t size = 2;
                    Printers_.emplace_back([](char* out, const TUnboxedValuePod& value, const IDateBuilder&) {
                        return PrintNDigits<size>::Do(GetMinute(value), out);
                    });
                    ReservedSize_ += size;
                    break;
                }
                case 'S':
                    Printers_.emplace_back([](char* out, const TUnboxedValuePod& value, const IDateBuilder&) {
                        constexpr size_t size = 2;
                        if (const auto microsecond = GetMicrosecond(value)) {
                            out += PrintNDigits<size>::Do(GetSecond(value), out);
                            *out++ = '.';
                            constexpr size_t msize = 6;
                            return size + 1U + PrintNDigits<msize, false>::Do(microsecond, out);
                        }
                        return PrintNDigits<size>::Do(GetSecond(value), out);
                    });
                    ReservedSize_ += 9;
                    break;

                case 'z': {
                    static constexpr size_t size = 5;
                    Printers_.emplace_back([](char* out, const TUnboxedValuePod& value, const IDateBuilder& builder) {
                        auto timezoneId = GetTimezoneId(value);
                        if (TTMStorage::IsUniversal(timezoneId)) {
                            std::memcpy(out, "+0000", size);
                            return size;
                        }
                        i32 shift;
                        if (!builder.GetTimezoneShift(GetYear(value), GetMonth(value), GetDay(value),
                            GetHour(value), GetMinute(value), GetSecond(value), timezoneId, shift))
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
                        const auto timezoneId = GetTimezoneId(value);
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
                        auto month = GetMonth(value);
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
                        auto month = GetMonth(value);
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
    };

    template<size_t Digits>
    struct ParseExaclyNDigits;

    template<>
    struct ParseExaclyNDigits<0U> {
        template <typename T>
        static constexpr bool Do(std::string_view::const_iterator&, T&) {
            return true;
        }
    };

    template<size_t Digits>
    struct ParseExaclyNDigits {
        template <typename T>
        static constexpr bool Do(std::string_view::const_iterator& it, T& out) {
            const auto d = *it;
            if (!std::isdigit(d)) {
                return false;
            }
            out *= 10U;
            out += d - '0';
            return ParseExaclyNDigits<Digits - 1U>::Do(++it, out);
        }
    };

    // Parse

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
            static auto name = TStringRef::Of("Parse");
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

            auto resourceType = builder.Resource(TMResourceName);
            auto optionalResourceType = builder.Optional()->Item(resourceType).Build();

            builder.Args()->Add<char*>().Flags(ICallablePayload::TArgumentFlags::AutoMap)
                .Add(builder.Optional()->Item<ui16>())
                .Done()
                .OptionalArgs(1);
            builder.RunConfig<char*>().Returns(optionalResourceType);

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
                auto& storage = Reference(result);
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
                    static constexpr size_t size = 4;
                    Scanners_.emplace_back([](std::string_view::const_iterator& it, size_t limit, TUnboxedValuePod& result, const IDateBuilder&) {
                        ui32 year = 0U;
                        if (limit < size || !ParseExaclyNDigits<size>::Do(it, year) || !ValidateYear(year)) {
                            return false;
                        }
                        SetYear(result, year);
                        return true;
                    });
                    break;
                }
                case 'm': {
                    static constexpr size_t size = 2;
                    Scanners_.emplace_back([](std::string_view::const_iterator& it, size_t limit, TUnboxedValuePod& result, const IDateBuilder&) {
                        ui32 month = 0U;
                        if (limit < size || !ParseExaclyNDigits<size>::Do(it, month) || !ValidateMonth(month)) {
                            return false;
                        }
                        SetMonth(result, month);
                        return true;
                    });
                    break;
                }
                case 'd': {
                    static constexpr size_t size = 2;
                    Scanners_.emplace_back([](std::string_view::const_iterator& it, size_t limit, TUnboxedValuePod& result, const IDateBuilder&) {
                        ui32 day = 0U;
                        if (limit < size || !ParseExaclyNDigits<size>::Do(it, day) || !ValidateDay(day)) {
                            return false;
                        }
                        SetDay(result, day);
                        return true;
                    });
                    break;
                }
                case 'H': {
                    static constexpr size_t size = 2;
                    Scanners_.emplace_back([](std::string_view::const_iterator& it, size_t limit, TUnboxedValuePod& result, const IDateBuilder&) {
                        ui32 hour = 0U;
                        if (limit < size || !ParseExaclyNDigits<size>::Do(it, hour) || !ValidateHour(hour)) {
                            return false;
                        }
                        SetHour(result, hour);
                        return true;
                    });
                    break;
                }
                case 'M': {
                    static constexpr size_t size = 2;
                    Scanners_.emplace_back([](std::string_view::const_iterator& it, size_t limit, TUnboxedValuePod& result, const IDateBuilder&) {
                        ui32 minute = 0U;
                        if (limit < size || !ParseExaclyNDigits<size>::Do(it, minute) || !ValidateMinute(minute)) {
                            return false;
                        }
                        SetMinute(result, minute);
                        return true;
                    });
                    break;
                }
                case 'S': {
                    static constexpr size_t size = 2;
                    Scanners_.emplace_back([](std::string_view::const_iterator& it, size_t limit, TUnboxedValuePod& result, const IDateBuilder&) {
                        ui32 second = 0U;
                        if (limit < size || !ParseExaclyNDigits<size>::Do(it, second) || !ValidateSecond(second)) {
                            return false;
                        }
                        SetSecond(result, second);
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
                        SetMicrosecond(result, usec);
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
                        SetTimezoneId(result, timezoneId);
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
                        SetMonth(result, month);
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
                        SetMonth(result, month);
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
        auto& storage = Reference(result);                                                                                         \
        storage.FromTimestamp(builder, instant.MicroSeconds());                                                                    \
        return result;                                                                                                             \
    }

    PARSE_SPECIFIC_FORMAT(Rfc822);
    PARSE_SPECIFIC_FORMAT(Iso8601);
    PARSE_SPECIFIC_FORMAT(Http);
    PARSE_SPECIFIC_FORMAT(X509);

    SIMPLE_MODULE(TDateTime2Module,
        TUserDataTypeFuncFactory<true, true, SplitName, TSplit,
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

        TGetYear,
        TGetDayOfYear,
        TGetMonth,
        TGetMonthName,
        TGetWeekOfYear,
        TGetWeekOfYearIso8601,
        TGetDayOfMonth,
        TGetDayOfWeek,
        TGetDayOfWeekName,
        TGetTimeComponent<GetHourName, ui8, GetHour, 1u, 3600u, 24u, false>,
        TGetTimeComponent<GetMinuteName, ui8, GetMinute, 1u, 60u, 60u, false>,
        TGetTimeComponent<GetSecondName, ui8, GetSecond, 1u, 1u, 60u, false>,
        TGetTimeComponent<GetMillisecondOfSecondName, ui32, GetMicrosecond, 1000u, 1000u, 1000u, true>,
        TGetTimeComponent<GetMicrosecondOfSecondName, ui32, GetMicrosecond, 1u, 1u, 1000000u, true>,
        TGetTimezoneId,
        TGetTimezoneName,

        TUpdate,

        TFromSeconds,
        TFromMilliseconds,
        TFromMicroseconds,

        TIntervalFromDays,
        TIntervalFromHours,
        TIntervalFromMinutes,
        TIntervalFromSeconds,
        TIntervalFromMilliseconds,
        TIntervalFromMicroseconds,

        TToDays,
        TToHours,
        TToMinutes,

        TStartOfYear,
        TStartOfQuarter,
        TStartOfMonth,
        TStartOfWeek,
        TStartOfDay,
        TStartOf,
        TTimeOfDay,

        TShiftYears,
        TShiftQuarters,
        TShiftMonths,

        TEndOfMonth,

        TToUnits<ToSecondsName, ui32, 1>,
        TToUnits<ToMillisecondsName, ui64, 1000>,
        TToUnits<ToMicrosecondsName, ui64, 1000000>,

        TFormat,
        TParse,

        TParseRfc822,
        TParseIso8601,
        TParseHttp,
        TParseX509
    )
}

REGISTER_MODULES(TDateTime2Module)
