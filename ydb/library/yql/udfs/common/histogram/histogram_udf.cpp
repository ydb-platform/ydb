#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <library/cpp/histogram/adaptive/adaptive_histogram.h>
#include <library/cpp/histogram/adaptive/block_histogram.h>

#include <util/string/printf.h>
#include <util/stream/format.h>

#include <cmath>

using namespace NKikimr;
using namespace NUdf;
using namespace NKiwiAggr;

namespace {
#define REGISTER_METHOD_UDF(name) \
    T##name,

#define HISTOGRAM_ONE_DOUBLE_ARG_METHODS_MAP(XX) \
    XX(GetSumAboveBound)                         \
    XX(GetSumBelowBound)                         \
    XX(CalcUpperBound)                           \
    XX(CalcLowerBound)                           \
    XX(CalcUpperBoundSafe)                       \
    XX(CalcLowerBoundSafe)

#define HISTOGRAM_TWO_DOUBLE_ARG_METHODS_MAP(XX) \
    XX(GetSumInRange)

#define HISTOGRAM_ALGORITHMS_MAP(XX) \
    XX(AdaptiveDistance)             \
    XX(AdaptiveWeight)               \
    XX(AdaptiveWard)                 \
    XX(BlockWeight)                  \
    XX(BlockWard)

#define HISTOGRAM_FUNCTION_MAP(XX, arg) \
    XX(Create, arg)                     \
    XX(AddValue, arg)                   \
    XX(GetResult, arg)                  \
    XX(Serialize, arg)                  \
    XX(Deserialize, arg)                \
    XX(Merge, arg)

#define DECLARE_HISTOGRAM_RESOURCE_NAME(name) extern const char name##HistogramResourceName[] = "Histogram." #name;
    HISTOGRAM_ALGORITHMS_MAP(DECLARE_HISTOGRAM_RESOURCE_NAME)
    DECLARE_HISTOGRAM_RESOURCE_NAME(Linear)
    DECLARE_HISTOGRAM_RESOURCE_NAME(Logarithmic)

    class TLinearHistogram: public TAdaptiveWardHistogram {
    public:
        TLinearHistogram(double step, double begin, double end)
            : TAdaptiveWardHistogram(1ULL << 24)
            , Step(step)
            , Begin(begin)
            , End(end)
        {
        }

        void Add(double value, double weight) override {
            if (value < Begin) {
                value = Begin;
            } else if (value > End) {
                value = End;
            } else {
                value = std::floor(value / Step + 0.5) * Step;
            }
            TAdaptiveWardHistogram::Add(value, weight);
        }

        void Add(const THistoRec&) override {
            Y_ABORT("Not implemented");
        }

    protected:
        double Step;
        double Begin;
        double End;
    };

    class TLogarithmicHistogram: public TLinearHistogram {
    public:
        TLogarithmicHistogram(double step, double begin, double end)
            : TLinearHistogram(step, begin, end)
        {
        }

        void Add(double value, double weight) override {
            double base = std::log(value) / std::log(Step);
            double prev = std::pow(Step, std::floor(base));
            double next = std::pow(Step, std::ceil(base));
            if (std::abs(value - next) > std::abs(value - prev)) {
                value = prev;
            } else {
                value = next;
            }

            if (value < Begin) {
                value = Begin;
            } else if (value > End) {
                value = End;
            }

            if (!std::isnan(value)) {
                TAdaptiveWardHistogram::Add(value, weight);
            }
        }

        void Add(const THistoRec&) override {
            Y_ABORT("Not implemented");
        }
    };

    template <typename THistogramType, const char* ResourceName>
    class THistogram_Create: public TBoxedValue {
    public:
        THistogram_Create(TSourcePosition pos)
            : Pos_(pos)
        {}

        typedef TBoxedResource<THistogramType, ResourceName> THistogramResource;

        static const TStringRef& Name() {
            static auto name = TString(ResourceName).substr(10) + "Histogram_Create";
            static auto nameRef = TStringRef(name);
            return nameRef;
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override {
            try {
                Y_UNUSED(valueBuilder);
                THolder<THistogramResource> histogram(new THistogramResource(args[2].Get<ui32>()));
                histogram->Get()->Add(args[0].Get<double>(), args[1].Get<double>());
                return TUnboxedValuePod(histogram.Release());
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

    public:
        static bool DeclareSignature(
            const TStringRef& name,
            TType* userType,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly) {
            Y_UNUSED(userType);
            if (Name() == name) {
                builder.SimpleSignature<TResource<ResourceName>(double, double, ui32)>();
                if (!typesOnly) {
                    builder.Implementation(new THistogram_Create<THistogramType, ResourceName>(builder.GetSourcePosition()));
                }
                return true;
            } else {
                return false;
            }
        }

    private:
        TSourcePosition Pos_;
    };

    template <typename THistogramType, const char* ResourceName>
    class THistogram_AddValue: public TBoxedValue {
    public:
        THistogram_AddValue(TSourcePosition pos)
            : Pos_(pos)
        {}

        typedef TBoxedResource<THistogramType, ResourceName> THistogramResource;

        static const TStringRef& Name() {
            static auto name = TString(ResourceName).substr(10) + "Histogram_AddValue";
            static auto nameRef = TStringRef(name);
            return nameRef;
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override {
            try {
                Y_UNUSED(valueBuilder);
                THistogramResource* resource = static_cast<THistogramResource*>(args[0].AsBoxed().Get());
                resource->Get()->Add(args[1].Get<double>(), args[2].Get<double>());
                return TUnboxedValuePod(args[0]);
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

    public:
        static bool DeclareSignature(
            const TStringRef& name,
            TType* userType,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly) {
            Y_UNUSED(userType);
            if (Name() == name) {
                builder.SimpleSignature<TResource<ResourceName>(TResource<ResourceName>, double, double)>();
                if (!typesOnly) {
                    builder.Implementation(new THistogram_AddValue<THistogramType, ResourceName>(builder.GetSourcePosition()));
                }
                return true;
            } else {
                return false;
            }
        }

    private:
        TSourcePosition Pos_;
    };

    template <typename THistogramType, const char* ResourceName>
    class THistogram_Serialize: public TBoxedValue {
    public:
        THistogram_Serialize(TSourcePosition pos)
            : Pos_(pos)
        {}

        typedef TBoxedResource<THistogramType, ResourceName> THistogramResource;

        static const TStringRef& Name() {
            static auto name = TString(ResourceName).substr(10) + "Histogram_Serialize";
            static auto nameRef = TStringRef(name);
            return nameRef;
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override {
            try {
                THistogram proto;
                TString result;
                static_cast<THistogramResource*>(args[0].AsBoxed().Get())->Get()->ToProto(proto);
                Y_PROTOBUF_SUPPRESS_NODISCARD proto.SerializeToString(&result);
                return valueBuilder->NewString(result);
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

    public:
        static bool DeclareSignature(
            const TStringRef& name,
            TType* userType,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly) {
            Y_UNUSED(userType);
            if (Name() == name) {
                builder.SimpleSignature<char*(TResource<ResourceName>)>();
                if (!typesOnly) {
                    builder.Implementation(new THistogram_Serialize<THistogramType, ResourceName>(builder.GetSourcePosition()));
                }
                return true;
            } else {
                return false;
            }
        }

    private:
        TSourcePosition Pos_;
    };

    template <typename THistogramType, const char* ResourceName>
    class THistogram_Deserialize: public TBoxedValue {
    public:
        THistogram_Deserialize(TSourcePosition pos)
            : Pos_(pos)
        {}

        typedef TBoxedResource<THistogramType, ResourceName> THistogramResource;

        static const TStringRef& Name() {
            static auto name = TString(ResourceName).substr(10) + "Histogram_Deserialize";
            static auto nameRef = TStringRef(name);
            return nameRef;
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override {
            try {
                Y_UNUSED(valueBuilder);
                THistogram proto;
                Y_PROTOBUF_SUPPRESS_NODISCARD proto.ParseFromString(TString(args[0].AsStringRef()));
                THolder<THistogramResource> histogram(new THistogramResource(args[1].Get<ui32>()));
                histogram->Get()->FromProto(proto);
                return TUnboxedValuePod(histogram.Release());
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

    public:
        static bool DeclareSignature(
            const TStringRef& name,
            TType* userType,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly) {
            Y_UNUSED(userType);
            if (Name() == name) {
                builder.SimpleSignature<TResource<ResourceName>(char*, ui32)>();
                if (!typesOnly) {
                    builder.Implementation(new THistogram_Deserialize<THistogramType, ResourceName>(builder.GetSourcePosition()));
                }
                return true;
            } else {
                return false;
            }
        }

    private:
        TSourcePosition Pos_;
    };

    template <typename THistogramType, const char* ResourceName>
    class THistogram_Merge: public TBoxedValue {
    public:
        THistogram_Merge(TSourcePosition pos)
            : Pos_(pos)
        {}

        typedef TBoxedResource<THistogramType, ResourceName> THistogramResource;

        static const TStringRef& Name() {
            static auto name = TString(ResourceName).substr(10) + "Histogram_Merge";
            static auto nameRef = TStringRef(name);
            return nameRef;
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override {
            try {
                Y_UNUSED(valueBuilder);
                THistogram proto;
                static_cast<THistogramResource*>(args[0].AsBoxed().Get())->Get()->ToProto(proto);
                static_cast<THistogramResource*>(args[1].AsBoxed().Get())->Get()->Merge(proto, 1.0);
                return TUnboxedValuePod(args[1]);
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

    public:
        static bool DeclareSignature(
            const TStringRef& name,
            TType* userType,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly) {
            Y_UNUSED(userType);
            if (Name() == name) {
                builder.SimpleSignature<TResource<ResourceName>(TResource<ResourceName>, TResource<ResourceName>)>();
                if (!typesOnly) {
                    builder.Implementation(new THistogram_Merge<THistogramType, ResourceName>(builder.GetSourcePosition()));
                }
                return true;
            } else {
                return false;
            }
        }

    private:
        TSourcePosition Pos_;
    };

    struct THistogramIndexes {
        static constexpr ui32 BinFieldsCount = 2U;
        static constexpr ui32 ResultFieldsCount = 5U;

        THistogramIndexes(IFunctionTypeInfoBuilder& builder) {
            const auto binStructType = builder.Struct(BinFieldsCount)->AddField<double>("Position", &Position).AddField<double>("Frequency", &Frequency).Build();
            const auto binsList = builder.List()->Item(binStructType).Build();
            ResultStructType = builder.Struct(ResultFieldsCount)->AddField<char*>("Kind", &Kind).AddField<double>("Min", &Min).AddField<double>("Max", &Max).AddField<double>("WeightsSum", &WeightsSum).AddField("Bins", binsList, &Bins).Build();
        }

        ui32 Kind;
        ui32 Min;
        ui32 Max;
        ui32 WeightsSum;
        ui32 Bins;

        ui32 Position;
        ui32 Frequency;

        TType* ResultStructType;
    };

    template <typename THistogramType, const char* ResourceName>
    class THistogram_GetResult: public TBoxedValue {
    public:
        typedef TBoxedResource<THistogramType, ResourceName> THistogramResource;

        THistogram_GetResult(const THistogramIndexes& histogramIndexes, TSourcePosition pos)
            : HistogramIndexes(histogramIndexes)
            , Pos_(pos)
        {
        }

        static const TStringRef& Name() {
            static auto name = TString(ResourceName).substr(10) + "Histogram_GetResult";
            static auto nameRef = TStringRef(name);
            return nameRef;
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override {
            THistogram proto;
            auto histogram = static_cast<THistogramResource*>(args[0].AsBoxed().Get())->Get();
            histogram->ToProto(proto);

            auto size = proto.FreqSize();
            TUnboxedValue* fields = nullptr;
            auto result = valueBuilder->NewArray(HistogramIndexes.ResultFieldsCount, fields);
            fields[HistogramIndexes.Kind] = valueBuilder->NewString(TStringBuf(ResourceName).Skip(10));
            if (size) {
                TUnboxedValue* items = nullptr;
                fields[HistogramIndexes.Bins] = valueBuilder->NewArray(size, items);
                fields[HistogramIndexes.Min] = TUnboxedValuePod(static_cast<double>(histogram->GetMinValue()));
                fields[HistogramIndexes.Max] = TUnboxedValuePod(static_cast<double>(histogram->GetMaxValue()));
                fields[HistogramIndexes.WeightsSum] = TUnboxedValuePod(static_cast<double>(histogram->GetSum()));
                for (ui64 i = 0; i < size; ++i) {
                    TUnboxedValue* binFields = nullptr;
                    *items++ = valueBuilder->NewArray(HistogramIndexes.BinFieldsCount, binFields);
                    binFields[HistogramIndexes.Frequency] = TUnboxedValuePod(static_cast<double>(proto.GetFreq(i)));
                    binFields[HistogramIndexes.Position] = TUnboxedValuePod(static_cast<double>(proto.GetPosition(i)));
                }
            } else {
                fields[HistogramIndexes.Bins] = valueBuilder->NewEmptyList();
                fields[HistogramIndexes.Min] = TUnboxedValuePod(0.0);
                fields[HistogramIndexes.Max] = TUnboxedValuePod(0.0);
                fields[HistogramIndexes.WeightsSum] = TUnboxedValuePod(0.0);
            }

            return result;
        }

    public:
        static bool DeclareSignature(
            const TStringRef& name,
            TType* userType,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly) {
            Y_UNUSED(userType);
            if (Name() == name) {
                auto resource = builder.Resource(TStringRef(ResourceName, std::strlen(ResourceName)));

                THistogramIndexes histogramIndexes(builder);

                builder.Args()->Add(resource).Done().Returns(histogramIndexes.ResultStructType);

                if (!typesOnly) {
                    builder.Implementation(new THistogram_GetResult<THistogramType, ResourceName>(histogramIndexes, builder.GetSourcePosition()));
                }
                return true;
            } else {
                return false;
            }
        }

    private:
        const THistogramIndexes HistogramIndexes;
        TSourcePosition Pos_;
    };

    template <>
    TUnboxedValue THistogram_Create<TLinearHistogram, LinearHistogramResourceName>::Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const {
        using THistogramResource = THistogram_Create<TLinearHistogram, LinearHistogramResourceName>::THistogramResource;
        try {
            Y_UNUSED(valueBuilder);
            THolder<THistogramResource> histogram(new THistogramResource(
                args[1].Get<double>(), args[2].Get<double>(), args[3].Get<double>()));
            histogram->Get()->Add(args[0].Get<double>(), 1.0);
            return TUnboxedValuePod(histogram.Release());
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }
    }

    template <>
    bool THistogram_Create<TLinearHistogram, LinearHistogramResourceName>::DeclareSignature(
        const TStringRef& name,
        TType* userType,
        IFunctionTypeInfoBuilder& builder,
        bool typesOnly) {
        Y_UNUSED(userType);
        if (Name() == name) {
            builder.SimpleSignature<TResource<LinearHistogramResourceName>(double, double, double, double)>();
            if (!typesOnly) {
                builder.Implementation(new THistogram_Create<TLinearHistogram, LinearHistogramResourceName>(builder.GetSourcePosition()));
            }
            return true;
        } else {
            return false;
        }
    }

    template <>
    TUnboxedValue THistogram_Deserialize<TLinearHistogram, LinearHistogramResourceName>::Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const {
        using THistogramResource = THistogram_Deserialize<TLinearHistogram, LinearHistogramResourceName>::THistogramResource;
        try {
            Y_UNUSED(valueBuilder);
            THistogram proto;
            Y_PROTOBUF_SUPPRESS_NODISCARD proto.ParseFromString(TString(args[0].AsStringRef()));
            THolder<THistogramResource> histogram(
                new THistogramResource(args[1].Get<double>(), args[2].Get<double>(), args[3].Get<double>()));
            histogram->Get()->FromProto(proto);
            return TUnboxedValuePod(histogram.Release());
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }
    }

    template <>
    bool THistogram_Deserialize<TLinearHistogram, LinearHistogramResourceName>::DeclareSignature(
        const TStringRef& name,
        TType* userType,
        IFunctionTypeInfoBuilder& builder,
        bool typesOnly) {
        Y_UNUSED(userType);
        if (Name() == name) {
            builder.SimpleSignature<TResource<LinearHistogramResourceName>(char*, double, double, double)>();
            if (!typesOnly) {
                builder.Implementation(new THistogram_Deserialize<TLinearHistogram, LinearHistogramResourceName>(builder.GetSourcePosition()));
            }
            return true;
        } else {
            return false;
        }
    }

    template <>
    TUnboxedValue THistogram_Create<TLogarithmicHistogram, LogarithmicHistogramResourceName>::Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const {
        using THistogramResource = THistogram_Create<TLogarithmicHistogram, LogarithmicHistogramResourceName>::THistogramResource;
        try {
            Y_UNUSED(valueBuilder);
            THolder<THistogramResource> histogram(new THistogramResource(
                args[1].Get<double>(), args[2].Get<double>(), args[3].Get<double>()));
            histogram->Get()->Add(args[0].Get<double>(), 1.0);
            return TUnboxedValuePod(histogram.Release());
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }
    }

    template <>
    bool THistogram_Create<TLogarithmicHistogram, LogarithmicHistogramResourceName>::DeclareSignature(
        const TStringRef& name,
        TType* userType,
        IFunctionTypeInfoBuilder& builder,
        bool typesOnly) {
        Y_UNUSED(userType);
        if (Name() == name) {
            builder.SimpleSignature<TResource<LogarithmicHistogramResourceName>(double, double, double, double)>();
            if (!typesOnly) {
                builder.Implementation(new THistogram_Create<TLogarithmicHistogram, LogarithmicHistogramResourceName>(builder.GetSourcePosition()));
            }
            return true;
        } else {
            return false;
        }
    }

    template <>
    TUnboxedValue THistogram_Deserialize<TLogarithmicHistogram, LogarithmicHistogramResourceName>::Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const {
        using THistogramResource = THistogram_Deserialize<TLogarithmicHistogram, LogarithmicHistogramResourceName>::THistogramResource;
        try {
            Y_UNUSED(valueBuilder);
            THistogram proto;
            Y_PROTOBUF_SUPPRESS_NODISCARD proto.ParseFromString(TString(args[0].AsStringRef()));
            THolder<THistogramResource> histogram(
                new THistogramResource(args[1].Get<double>(), args[2].Get<double>(), args[3].Get<double>()));
            histogram->Get()->FromProto(proto);
            return TUnboxedValuePod(histogram.Release());
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
        }
    }

    template <>
    bool THistogram_Deserialize<TLogarithmicHistogram, LogarithmicHistogramResourceName>::DeclareSignature(
        const TStringRef& name,
        TType* userType,
        IFunctionTypeInfoBuilder& builder,
        bool typesOnly) {
        Y_UNUSED(userType);
        if (Name() == name) {
            builder.SimpleSignature<TResource<LogarithmicHistogramResourceName>(char*, double, double, double)>();
            if (!typesOnly) {
                builder.Implementation(new THistogram_Deserialize<TLogarithmicHistogram, LogarithmicHistogramResourceName>(builder.GetSourcePosition()));
            }
            return true;
        } else {
            return false;
        }
    }

    class THistogramPrint: public TBoxedValue {
    public:
        THistogramPrint(const THistogramIndexes& histogramIndexes)
            : HistogramIndexes(histogramIndexes)
        {
        }

        static const TStringRef& Name() {
            static auto name = TStringRef::Of("Print");
            return name;
        }

        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override {
            auto kind = args[0].GetElement(HistogramIndexes.Kind);
            auto bins = args[0].GetElement(HistogramIndexes.Bins);
            double min = args[0].GetElement(HistogramIndexes.Min).Get<double>();
            double max = args[0].GetElement(HistogramIndexes.Max).Get<double>();
            double weightsSum = args[0].GetElement(HistogramIndexes.WeightsSum).Get<double>();
            auto binsIterator = bins.GetListIterator();

            TStringBuilder result;
            result << "Kind: " << (TStringBuf)kind.AsStringRef() << ' ';
            result << Sprintf("Bins: %" PRIu64 " WeightsSum: %.3f Min: %.3f Max: %.3f",
                              bins.GetListLength(), weightsSum, min, max);
            double maxFrequency = 0.0;
            size_t maxPositionLength = 0;
            size_t maxFrequencyLength = 0;
            const ui8 bars = args[1].GetOrDefault<ui8>(25);

            for (TUnboxedValue current; binsIterator.Next(current);) {
                if (bars) {
                    double frequency = current.GetElement(HistogramIndexes.Frequency).Get<double>();
                    if (frequency > maxFrequency) {
                        maxFrequency = frequency;
                    }
                }
                size_t positionLength = Sprintf("%.3f", current.GetElement(HistogramIndexes.Position).Get<double>()).length();
                size_t frequencyLength = Sprintf("%.3f", current.GetElement(HistogramIndexes.Frequency).Get<double>()).length();

                if (positionLength > maxPositionLength) {
                    maxPositionLength = positionLength;
                }
                if (frequencyLength > maxFrequencyLength) {
                    maxFrequencyLength = frequencyLength;
                }
            }

            binsIterator = bins.GetListIterator();
            for (TUnboxedValue current; binsIterator.Next(current);) {
                double position = current.GetElement(HistogramIndexes.Position).Get<double>();
                double frequency = current.GetElement(HistogramIndexes.Frequency).Get<double>();
                result << "\n";
                if (bars && maxFrequency > 0) {
                    ui8 filledBars = static_cast<ui8>(bars * frequency / maxFrequency);
                    for (ui8 i = 0; i < bars; ++i) {
                        if (i < filledBars) {
                            result << "█";
                        } else {
                            result << "░";
                        }
                    }
                }
                result << " P: " << LeftPad(Sprintf("%.3f", position), maxPositionLength);
                result << " F: " << LeftPad(Sprintf("%.3f", frequency), maxFrequencyLength);
            }

            return valueBuilder->NewString(result);
        }

        static bool DeclareSignature(
            const TStringRef& name,
            TType* userType,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly) {
            Y_UNUSED(userType);
            if (Name() == name) {
                THistogramIndexes histogramIndexes(builder);
                auto optionalUi8 = builder.Optional()->Item<ui8>().Build();

                builder.Args()->Add(histogramIndexes.ResultStructType).Flags(ICallablePayload::TArgumentFlags::AutoMap).Add(optionalUi8).Done().OptionalArgs(1).Returns<char*>();

                if (!typesOnly) {
                    builder.Implementation(new THistogramPrint(histogramIndexes));
                }
                builder.IsStrict();
                return true;
            } else {
                return false;
            }
        }

    private:
        const THistogramIndexes HistogramIndexes;
    };

    class THistogramToCumulativeDistributionFunction: public TBoxedValue {
    public:
        THistogramToCumulativeDistributionFunction(const THistogramIndexes& histogramIndexes)
            : HistogramIndexes(histogramIndexes)
        {
        }

        static const TStringRef& Name() {
            static auto name = TStringRef::Of("ToCumulativeDistributionFunction");
            return name;
        }

        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override {
            TUnboxedValue* fields = nullptr;
            auto result = valueBuilder->NewArray(HistogramIndexes.ResultFieldsCount, fields);
            auto bins = args[0].GetElement(HistogramIndexes.Bins);
            double minValue = args[0].GetElement(HistogramIndexes.Min).Get<double>();
            double maxValue = args[0].GetElement(HistogramIndexes.Max).Get<double>();
            double sum = 0.0;
            double weightsSum = 0.0;
            std::vector<TUnboxedValue> resultBins;
            if (bins.HasFastListLength())
                resultBins.reserve(bins.GetListLength());
            const auto binsIterator = bins.GetListIterator();
            for (TUnboxedValue current; binsIterator.Next(current);) {
                TUnboxedValue* binFields = nullptr;
                auto resultCurrent = valueBuilder->NewArray(HistogramIndexes.BinFieldsCount, binFields);
                const auto frequency = current.GetElement(HistogramIndexes.Frequency).Get<double>();
                sum += frequency;
                weightsSum += sum;
                binFields[HistogramIndexes.Frequency] = TUnboxedValuePod(sum);
                binFields[HistogramIndexes.Position] = current.GetElement(HistogramIndexes.Position);
                resultBins.emplace_back(std::move(resultCurrent));
            }

            auto kind = args[0].GetElement(HistogramIndexes.Kind);
            fields[HistogramIndexes.Kind] = valueBuilder->AppendString(kind, "Cdf");
            fields[HistogramIndexes.Bins] = valueBuilder->NewList(resultBins.data(), resultBins.size());
            fields[HistogramIndexes.Max] = TUnboxedValuePod(maxValue);
            fields[HistogramIndexes.Min] = TUnboxedValuePod(minValue);
            fields[HistogramIndexes.WeightsSum] = TUnboxedValuePod(weightsSum);
            return result;
        }

        static bool DeclareSignature(
            const TStringRef& name,
            TType* userType,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly) {
            Y_UNUSED(userType);
            if (Name() == name) {
                THistogramIndexes histogramIndexes(builder);

                builder.Args()->Add(histogramIndexes.ResultStructType).Flags(ICallablePayload::TArgumentFlags::AutoMap).Done().Returns(histogramIndexes.ResultStructType);

                if (!typesOnly) {
                    builder.Implementation(new THistogramToCumulativeDistributionFunction(histogramIndexes));
                }
                builder.IsStrict();
                return true;
            } else {
                return false;
            }
        }

    private:
        const THistogramIndexes HistogramIndexes;
    };

    class THistogramNormalize: public TBoxedValue {
    public:
        THistogramNormalize(const THistogramIndexes& histogramIndexes)
            : HistogramIndexes(histogramIndexes)
        {
        }

        static const TStringRef& Name() {
            static auto name = TStringRef::Of("Normalize");
            return name;
        }

        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override {
            TUnboxedValue* fields = nullptr;
            auto result = valueBuilder->NewArray(HistogramIndexes.ResultFieldsCount, fields);
            auto bins = args[0].GetElement(HistogramIndexes.Bins);
            double minValue = args[0].GetElement(HistogramIndexes.Min).Get<double>();
            double maxValue = args[0].GetElement(HistogramIndexes.Max).Get<double>();
            double area = args[1].GetOrDefault<double>(100.0);
            bool cdfNormalization = args[2].GetOrDefault<bool>(false);
            double sum = 0.0;
            double weightsSum = 0.0;
            double lastBinFrequency = 0.0;
            std::vector<TUnboxedValue> resultBins;
            if (bins.HasFastListLength())
                resultBins.reserve(bins.GetListLength());
            auto binsIterator = bins.GetListIterator();
            for (TUnboxedValue current; binsIterator.Next(current);) {
                sum += current.GetElement(HistogramIndexes.Frequency).Get<double>();
                lastBinFrequency = current.GetElement(HistogramIndexes.Frequency).Get<double>();
            }
            binsIterator = bins.GetListIterator();
            for (TUnboxedValue current; binsIterator.Next(current);) {
                TUnboxedValue* binFields = nullptr;
                auto resultCurrent = valueBuilder->NewArray(HistogramIndexes.BinFieldsCount, binFields);
                double frequency = current.GetElement(HistogramIndexes.Frequency).Get<double>();
                if (cdfNormalization) {
                    frequency = area * frequency / lastBinFrequency;
                } else {
                    frequency = area * frequency / sum;
                }
                weightsSum += frequency;
                binFields[HistogramIndexes.Frequency] = TUnboxedValuePod(frequency);
                binFields[HistogramIndexes.Position] = current.GetElement(HistogramIndexes.Position);
                resultBins.emplace_back(std::move(resultCurrent));
            }

            TUnboxedValue kind = args[0].GetElement(HistogramIndexes.Kind);
            if (cdfNormalization) {
                kind = valueBuilder->AppendString(kind, "Cdf");
            }

            fields[HistogramIndexes.Kind] = kind;
            fields[HistogramIndexes.Bins] = valueBuilder->NewList(resultBins.data(), resultBins.size());
            fields[HistogramIndexes.Max] = TUnboxedValuePod(maxValue);
            fields[HistogramIndexes.Min] = TUnboxedValuePod(minValue);
            fields[HistogramIndexes.WeightsSum] = TUnboxedValuePod(weightsSum);
            return result;
        }

        static bool DeclareSignature(
            const TStringRef& name,
            TType* userType,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly) {
            Y_UNUSED(userType);
            if (Name() == name) {
                THistogramIndexes histogramIndexes(builder);
                auto optionalDouble = builder.Optional()->Item<double>().Build();
                auto optionalCdfNormalization = builder.Optional()->Item<bool>().Build();
                builder.Args()->Add(histogramIndexes.ResultStructType).Flags(ICallablePayload::TArgumentFlags::AutoMap).Add(optionalDouble).Add(optionalCdfNormalization).Done().Returns(histogramIndexes.ResultStructType);
                builder.OptionalArgs(1);
                builder.OptionalArgs(2);
                if (!typesOnly) {
                    builder.Implementation(new THistogramNormalize(histogramIndexes));
                }
                builder.IsStrict();
                return true;
            } else {
                return false;
            }
        }

    private:
        const THistogramIndexes HistogramIndexes;
    };

    template <bool twoArgs>
    class THistogramMethodBase: public TBoxedValue {
    public:
        THistogramMethodBase(const THistogramIndexes& histogramIndexes, TSourcePosition pos)
            : HistogramIndexes(histogramIndexes)
            , Pos_(pos)
        {
        }

        virtual TUnboxedValue GetResult(
            const THistogram& input,
            const TUnboxedValuePod* args) const = 0;

        TUnboxedValue Run(
            const IValueBuilder*,
            const TUnboxedValuePod* args) const override {
            try {
                auto bins = args[0].GetElement(HistogramIndexes.Bins);
                double min = args[0].GetElement(HistogramIndexes.Min).template Get<double>();
                double max = args[0].GetElement(HistogramIndexes.Max).template Get<double>();
                auto binsIterator = bins.GetListIterator();

                THistogram histogram;
                histogram.SetType(HT_ADAPTIVE_HISTOGRAM);
                histogram.SetMinValue(min);
                histogram.SetMaxValue(max);
                for (TUnboxedValue current; binsIterator.Next(current);) {
                    double frequency = current.GetElement(HistogramIndexes.Frequency).template Get<double>();
                    double position = current.GetElement(HistogramIndexes.Position).template Get<double>();
                    histogram.AddFreq(frequency);
                    histogram.AddPosition(position);
                }

                return GetResult(histogram, args);
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

        static THistogramIndexes DeclareSignatureBase(IFunctionTypeInfoBuilder& builder) {
            THistogramIndexes histogramIndexes(builder);

            if (twoArgs) {
                builder.Args()->Add(histogramIndexes.ResultStructType).Flags(ICallablePayload::TArgumentFlags::AutoMap).Add<double>().Add<double>().Done().Returns<double>();
            } else {
                builder.Args()->Add(histogramIndexes.ResultStructType).Flags(ICallablePayload::TArgumentFlags::AutoMap).Add<double>().Done().Returns<double>();
            }
            return histogramIndexes;
        }

    protected:
        const THistogramIndexes HistogramIndexes;
        TSourcePosition Pos_;
    };

#define DECLARE_ONE_DOUBLE_ARG_METHOD_UDF(name)                               \
    class T##name: public THistogramMethodBase<false> {                       \
    public:                                                                   \
        T##name(const THistogramIndexes& histogramIndexes, TSourcePosition pos) \
            : THistogramMethodBase<false>(histogramIndexes, pos) {            \
        }                                                                     \
        static const TStringRef& Name() {                                     \
            static auto name = TStringRef::Of(#name);                         \
            return name;                                                      \
        }                                                                     \
        static bool DeclareSignature(                                         \
            const TStringRef& name,                                           \
            TType* userType,                                                  \
            IFunctionTypeInfoBuilder& builder,                                \
            bool typesOnly) {                                                 \
            Y_UNUSED(userType);                                               \
            if (Name() == name) {                                             \
                const auto& histogramIndexes = DeclareSignatureBase(builder); \
                if (!typesOnly) {                                             \
                    builder.Implementation(new T##name(histogramIndexes,      \
                        builder.GetSourcePosition()));                        \
                }                                                             \
                return true;                                                  \
            } else {                                                          \
                return false;                                                 \
            }                                                                 \
        }                                                                     \
        TUnboxedValue GetResult(                                              \
            const THistogram& input,                                          \
            const TUnboxedValuePod* args) const override {                    \
            TAdaptiveWardHistogram histo(input, input.FreqSize());            \
            double result = histo.name(args[1].Get<double>());                \
            return TUnboxedValuePod(result);                                  \
        }                                                                     \
    };

#define DECLARE_TWO_DOUBLE_ARG_METHOD_UDF(name)                                       \
    class T##name: public THistogramMethodBase<true> {                                \
    public:                                                                           \
        T##name(const THistogramIndexes& histogramIndexes, TSourcePosition pos)       \
            : THistogramMethodBase<true>(histogramIndexes, pos) {                     \
        }                                                                             \
        static const TStringRef& Name() {                                             \
            static auto name = TStringRef::Of(#name);                                 \
            return name;                                                              \
        }                                                                             \
        static bool DeclareSignature(                                                 \
            const TStringRef& name,                                                   \
            TType* userType,                                                          \
            IFunctionTypeInfoBuilder& builder,                                        \
            bool typesOnly) {                                                         \
            Y_UNUSED(userType);                                                       \
            if (Name() == name) {                                                     \
                const auto& histogramIndexes = DeclareSignatureBase(builder);         \
                if (!typesOnly) {                                                     \
                    builder.Implementation(new T##name(histogramIndexes,              \
                        builder.GetSourcePosition()));                                \
                }                                                                     \
                return true;                                                          \
            } else {                                                                  \
                return false;                                                         \
            }                                                                         \
        }                                                                             \
        TUnboxedValue GetResult(                                                      \
            const THistogram& input,                                                  \
            const TUnboxedValuePod* args) const override {                            \
            TAdaptiveWardHistogram histo(input, input.FreqSize());                    \
            double result = histo.name(args[1].Get<double>(), args[2].Get<double>()); \
            return TUnboxedValuePod(result);                                          \
        }                                                                             \
    };

#define DECLARE_HISTOGRAM_UDF(functionName, histogramName) \
    THistogram_##functionName<T##histogramName##Histogram, histogramName##HistogramResourceName>,

#define DECLARE_HISTOGRAM_UDFS(name) \
    HISTOGRAM_FUNCTION_MAP(DECLARE_HISTOGRAM_UDF, name)

    HISTOGRAM_ONE_DOUBLE_ARG_METHODS_MAP(DECLARE_ONE_DOUBLE_ARG_METHOD_UDF)
    HISTOGRAM_TWO_DOUBLE_ARG_METHODS_MAP(DECLARE_TWO_DOUBLE_ARG_METHOD_UDF)

    SIMPLE_MODULE(THistogramModule,
                  HISTOGRAM_ALGORITHMS_MAP(DECLARE_HISTOGRAM_UDFS)
                      HISTOGRAM_ONE_DOUBLE_ARG_METHODS_MAP(REGISTER_METHOD_UDF)
                          HISTOGRAM_TWO_DOUBLE_ARG_METHODS_MAP(REGISTER_METHOD_UDF)
                              DECLARE_HISTOGRAM_UDFS(Linear)
                                  DECLARE_HISTOGRAM_UDFS(Logarithmic)
                                      THistogramPrint,
                  THistogramNormalize,
                  THistogramToCumulativeDistributionFunction)
}

REGISTER_MODULES(THistogramModule)
