#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <library/cpp/hyperloglog/hyperloglog.h>

#include <util/generic/hash_set.h>

#include <variant>

using namespace NKikimr;
using namespace NUdf;

namespace {
    class THybridHyperLogLog {
    private:
        using THybridSet = THashSet<ui64, std::hash<ui64>, std::equal_to<ui64>, TStdAllocatorForUdf<ui64>>;
        using THybridHll = THyperLogLogWithAlloc<TStdAllocatorForUdf<ui8>>;

        explicit THybridHyperLogLog(unsigned precision)
            : Var(THybridSet()), SizeLimit((1u << precision) / 8), Precision(precision)
        { }

        THybridHll ConvertToHyperLogLog() const {
            auto res = THybridHll::Create(Precision);
            for (auto& el : GetSetRef()) {
                res.Update(el);
            }
            return res;
        }

        bool IsSet() const {
            return Var.index() == 1;
        }

        const THybridSet& GetSetRef() const {
            return std::get<1>(Var);
        }

        THybridSet& GetMutableSetRef() {
            return std::get<1>(Var);
        }

        const THybridHll& GetHllRef() const {
            return std::get<0>(Var);
        }

        THybridHll& GetMutableHllRef() {
            return std::get<0>(Var);
        }

    public:
        THybridHyperLogLog (THybridHyperLogLog&&) = default;

        THybridHyperLogLog& operator=(THybridHyperLogLog&&) = default;

        void Update(ui64 hash) {
            if (IsSet()) {
                GetMutableSetRef().insert(hash);
                if (GetSetRef().size() >= SizeLimit) {
                    Var = ConvertToHyperLogLog();
                }
            } else {
                GetMutableHllRef().Update(hash);
            }
        }

        void Merge(const THybridHyperLogLog& rh) {
            if (IsSet() && rh.IsSet()) {
                GetMutableSetRef().insert(rh.GetSetRef().begin(), rh.GetSetRef().end());
                if (GetSetRef().size() >= SizeLimit) {
                    Var = ConvertToHyperLogLog();
                }
            } else {
                if (IsSet()) {
                    Var = ConvertToHyperLogLog();
                }
                if (rh.IsSet()) {
                    GetMutableHllRef().Merge(rh.ConvertToHyperLogLog());
                } else {
                    GetMutableHllRef().Merge(rh.GetHllRef());
                }
            }
        }

        void Save(IOutputStream& out) const {
            out.Write(static_cast<char>(Var.index()));
            out.Write(static_cast<char>(Precision));
            if (IsSet()) {
                ::Save(&out, GetSetRef());
            } else {
                GetHllRef().Save(out);
            }
        }

        ui64 Estimate() const {
            if (IsSet()) {
                return GetSetRef().size();
            }
            return GetHllRef().Estimate();
        }

        static THybridHyperLogLog Create(unsigned precision) {
            Y_ENSURE(precision >= THyperLogLog::PRECISION_MIN && precision <= THyperLogLog::PRECISION_MAX);
            return THybridHyperLogLog(precision);
        }

        static THybridHyperLogLog Load(IInputStream& in) {
            char type;
            Y_ENSURE(in.ReadChar(type));
            char precision;
            Y_ENSURE(in.ReadChar(precision));
            auto res = Create(precision);
            if (type) {
                ::Load(&in, res.GetMutableSetRef());
            } else {
                res.Var = THybridHll::Load(in);
            }
            return res;
        }

    private:
        std::variant<THybridHll, THybridSet> Var;

        size_t SizeLimit;

        unsigned Precision;
    };

    extern const char HyperLogLogResourceName[] = "HyperLogLog.State";

    using THyperLogLogResource = TBoxedResource<THybridHyperLogLog, HyperLogLogResourceName>;

    class THyperLogLog_Create: public TBoxedValue {
    public:
        THyperLogLog_Create(TSourcePosition pos)
            : Pos_(pos)
        {}

        static const TStringRef& Name() {
            static auto nameRef = TStringRef::Of("Create");
            return nameRef;
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder*,
            const TUnboxedValuePod* args) const override {
            try {
                THolder<THyperLogLogResource> hll(new THyperLogLogResource(THybridHyperLogLog::Create(args[1].Get<ui32>())));
                hll->Get()->Update(args[0].Get<ui64>());
                return TUnboxedValuePod(hll.Release());
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
                builder.SimpleSignature<TResource<HyperLogLogResourceName>(ui64, ui32)>();
                if (!typesOnly) {
                    builder.Implementation(new THyperLogLog_Create(builder.GetSourcePosition()));
                }
                return true;
            } else {
                return false;
            }
        }

    private:
        TSourcePosition Pos_;
    };

    class THyperLogLog_AddValue: public TBoxedValue {
    public:
        THyperLogLog_AddValue(TSourcePosition pos)
            : Pos_(pos)
        {}

        static const TStringRef& Name() {
            static auto nameRef = TStringRef::Of("AddValue");
            return nameRef;
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override {
            try {
                Y_UNUSED(valueBuilder);
                THyperLogLogResource* resource = static_cast<THyperLogLogResource*>(args[0].AsBoxed().Get());
                resource->Get()->Update(args[1].Get<ui64>());
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
                builder.SimpleSignature<TResource<HyperLogLogResourceName>(TResource<HyperLogLogResourceName>, ui64)>();
                if (!typesOnly) {
                    builder.Implementation(new THyperLogLog_AddValue(builder.GetSourcePosition()));
                }
                builder.IsStrict();
                return true;
            } else {
                return false;
            }
        }

    private:
        TSourcePosition Pos_;
    };

    class THyperLogLog_Serialize: public TBoxedValue {
    public:
        THyperLogLog_Serialize(TSourcePosition pos)
            : Pos_(pos)
        {}

    public:
        static const TStringRef& Name() {
            static auto nameRef = TStringRef::Of("Serialize");
            return nameRef;
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override {
            try {
                TStringStream result;
                static_cast<THyperLogLogResource*>(args[0].AsBoxed().Get())->Get()->Save(result);
                return valueBuilder->NewString(result.Str());
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
                builder.SimpleSignature<char*(TResource<HyperLogLogResourceName>)>();
                if (!typesOnly) {
                    builder.Implementation(new THyperLogLog_Serialize(builder.GetSourcePosition()));
                }
                return true;
            } else {
                return false;
            }
        }

    private:
        TSourcePosition Pos_;
    };

    class THyperLogLog_Deserialize: public TBoxedValue {
    public:
        THyperLogLog_Deserialize(TSourcePosition pos)
            : Pos_(pos)
        {}

        static const TStringRef& Name() {
            static auto nameRef = TStringRef::Of("Deserialize");
            return nameRef;
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override {
            try {
                Y_UNUSED(valueBuilder);
                const TString arg(args[0].AsStringRef());
                TStringInput input(arg);
                THolder<THyperLogLogResource> hll(new THyperLogLogResource(THybridHyperLogLog::Load(input)));
                return TUnboxedValuePod(hll.Release());
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
                builder.SimpleSignature<TResource<HyperLogLogResourceName>(char*)>();
                if (!typesOnly) {
                    builder.Implementation(new THyperLogLog_Deserialize(builder.GetSourcePosition()));
                }
                return true;
            } else {
                return false;
            }
        }

    private:
        TSourcePosition Pos_;
    };

    class THyperLogLog_Merge: public TBoxedValue {
    public:
        THyperLogLog_Merge(TSourcePosition pos)
            : Pos_(pos)
        {}

        static const TStringRef& Name() {
            static auto nameRef = TStringRef::Of("Merge");
            return nameRef;
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override {
            try {
                Y_UNUSED(valueBuilder);
                auto left = static_cast<THyperLogLogResource*>(args[0].AsBoxed().Get())->Get();
                static_cast<THyperLogLogResource*>(args[1].AsBoxed().Get())->Get()->Merge(*left);
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
                builder.SimpleSignature<TResource<HyperLogLogResourceName>(TResource<HyperLogLogResourceName>, TResource<HyperLogLogResourceName>)>();
                if (!typesOnly) {
                    builder.Implementation(new THyperLogLog_Merge(builder.GetSourcePosition()));
                }
                builder.IsStrict();
                return true;
            } else {
                return false;
            }
        }

    private:
        TSourcePosition Pos_;
    };

    class THyperLogLog_GetResult: public TBoxedValue {
    public:
        THyperLogLog_GetResult(TSourcePosition pos)
            : Pos_(pos)
        {}

        static const TStringRef& Name() {
            static auto nameRef = TStringRef::Of("GetResult");
            return nameRef;
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override {
            Y_UNUSED(valueBuilder);
            auto hll = static_cast<THyperLogLogResource*>(args[0].AsBoxed().Get())->Get();
            return TUnboxedValuePod(hll->Estimate());
        }

    public:
        static bool DeclareSignature(
            const TStringRef& name,
            TType* userType,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly) {
            Y_UNUSED(userType);
            if (Name() == name) {
                auto resource = builder.Resource(HyperLogLogResourceName);
                builder.Args()->Add(resource).Done().Returns<ui64>();

                if (!typesOnly) {
                    builder.Implementation(new THyperLogLog_GetResult(builder.GetSourcePosition()));
                }
                builder.IsStrict();
                return true;
            } else {
                return false;
            }
        }

    private:
        TSourcePosition Pos_;
    };

    SIMPLE_MODULE(THyperLogLogModule,
                  THyperLogLog_Create,
                  THyperLogLog_AddValue,
                  THyperLogLog_Serialize,
                  THyperLogLog_Deserialize,
                  THyperLogLog_Merge,
                  THyperLogLog_GetResult)
}

REGISTER_MODULES(THyperLogLogModule)
