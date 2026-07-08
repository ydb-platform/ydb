#include "uuid_keygen.h"

#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/public/udf/udf_value_builder.h>

#include <util/system/datetime.h>

using namespace NYql;
using namespace NYql::NUdf;

namespace {

TString BuildDepArgKindsPredicate(TStringBuf argName) {
    return TStringBuilder() << R"(
{cmd=or;value=[
    {cmd=kind;arg=)" << argName << R"(;value=Data};
    {cmd=kind;arg=)" << argName << R"(;value=Optional};
    {cmd=kind;arg=)" << argName << R"(;value=Tuple};
    {cmd=kind;arg=)" << argName << R"(;value=Struct};
    {cmd=kind;arg=)" << argName << R"(;value=List};
    {cmd=kind;arg=)" << argName << R"(;value=Dict};
    {cmd=kind;arg=)" << argName << R"(;value=Stream};
    {cmd=kind;arg=)" << argName << R"(;value=Null};
    {cmd=kind;arg=)" << argName << R"(;value=Void}
]}
)";
}

TString BuildNoPrefixPolyArgs(TStringBuf errorMessage) {
    TStringBuilder sb;
    sb << R"([[
    [[]; {type=[CallableType;[];[];[[[DataType;Uuid]]]]}];)";
    sb << R"(
    [)" << BuildDepArgKindsPredicate("T0") << R"(; {type=[CallableType;[];[];[[[UniversalType];[[DataType;Uuid]]]]}];)";
    sb << R"(
    [{cmd=and;value=[)" << BuildDepArgKindsPredicate("T0") << ";"
       << BuildDepArgKindsPredicate("T1") << R"(]}; {type=[CallableType;[];[];[[[UniversalType];[[UniversalType];[[DataType;Uuid]]]]}];)";
    sb << R"(
    [{cmd=error;message=")" << errorMessage << R"("}; {}]
]])";
    return sb;
}

TString BuildPrefixPolyArgs(TStringBuf errorMessage) {
    TStringBuilder sb;
    sb << R"([[
    [{cmd=type;arg=T0;value=[DataType;Uint64]}; {type=[CallableType;[];[];[[[DataType;Uint64];[[DataType;Uuid]]]]}];)";
    sb << R"(
    [{cmd=and;value=[{cmd=type;arg=T0;value=[DataType;Uint64]}; )" << BuildDepArgKindsPredicate("T1") << R"(]}; {type=[CallableType;[];[];[[[DataType;Uint64];[[UniversalType];[[DataType;Uuid]]]]}];)";
    sb << R"(
    [{cmd=and;value=[{cmd=type;arg=T0;value=[DataType;Uint64]}; )" << BuildDepArgKindsPredicate("T1") << ";"
       << BuildDepArgKindsPredicate("T2") << R"(]}; {type=[CallableType;[];[];[[[DataType;Uint64];[[UniversalType];[[UniversalType];[[DataType;Uuid]]]]}];)";
    sb << R"(
    [{cmd=error;message=")" << errorMessage << R"("}; {}]
]])";
    return sb;
}

TUnboxedValue MakeUuidValue(const IValueBuilder* valueBuilder, bool isV8, ui64 prefix, bool hasPrefix) {
    std::array<ui8, NKikimr::NUuid::UUID_LEN> bytes{};
    if (isV8) {
        const ui64 epochSeconds = MilliSeconds() / 1000;
        bytes = NUuidKeyGen::MakeV8Bytes(prefix, epochSeconds, hasPrefix);
    } else {
        ui64 timestampMs = MilliSeconds();
        if (hasPrefix) {
            timestampMs = NUuidKeyGen::ApplyV7Prefix(timestampMs, prefix);
        }
        bytes = NUuidKeyGen::MakeV7Bytes(timestampMs);
    }

    return valueBuilder->NewString(TStringRef(
        reinterpret_cast<const char*>(bytes.data()),
        bytes.size()));
}

class TNewPrefix: public TBoxedValue {
public:
    explicit TNewPrefix(TSourcePosition pos)
        : Pos_(pos)
    {
    }

    static const TStringRef& Name() {
        static auto name = TStringRef::Of("newPrefix");
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

        builder.Args()->Done();
        builder.Returns<ui64>().IsStrict();

        if (!typesOnly) {
            builder.Implementation(new TNewPrefix(GetSourcePosition(builder)));
        }
        return true;
    }

private:
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
        Y_UNUSED(args);
        try {
            return TUnboxedValuePod(NUuidKeyGen::NewPrefixValue());
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos_) << " " << e.what()).data());
        }
    }

    TSourcePosition Pos_;
};

template <bool IsV8, bool HasPrefix>
class TNewUuid: public TBoxedValue {
public:
    using TTypeAwareMarker = bool;

    explicit TNewUuid(TSourcePosition pos)
        : Pos_(pos)
    {
    }

    static const TStringRef& Name() {
        if constexpr (HasPrefix) {
            if constexpr (IsV8) {
                static auto name = TStringRef::Of("newPrefixV8");
                return name;
            } else {
                static auto name = TStringRef::Of("newPrefixV7");
                return name;
            }
        } else {
            if constexpr (IsV8) {
                static auto name = TStringRef::Of("newV8");
                return name;
            } else {
                static auto name = TStringRef::Of("newV7");
                return name;
            }
        }
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
            builder.SetError("Missing user type.");
            return true;
        }

        builder.UserType(userType);
        const auto typeHelper = builder.TypeInfoHelper();
        const auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
        if (!userTypeInspector || userTypeInspector.GetElementsCount() < 1) {
            builder.SetError("Invalid user type.");
            return true;
        }

        const auto argsTypeTuple = userTypeInspector.GetElementType(0);
        const auto argsTypeInspector = TTupleTypeInspector(*typeHelper, argsTypeTuple);
        if (!argsTypeInspector) {
            builder.SetError("Invalid user type - expected tuple.");
            return true;
        }

        const auto argCount = argsTypeInspector.GetElementsCount();
        if constexpr (HasPrefix) {
            if (argCount < 1) {
                builder.SetError("Expected at least prefix argument.");
                return true;
            }
        }

        auto argsBuilder = builder.Args(argCount);
        for (ui32 i = 0; i < argCount; ++i) {
            argsBuilder->Add(argsTypeInspector.GetElementType(i));
        }
        argsBuilder->Done().Returns<TUuid>();

        if (!typesOnly) {
            builder.Implementation(new TNewUuid(GetSourcePosition(builder)));
        }
        return true;
    }

private:
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
        try {
            ui64 prefix = 0;
            if constexpr (HasPrefix) {
                prefix = args[0].Get<ui64>();
            }
            return MakeUuidValue(valueBuilder, IsV8, prefix, HasPrefix);
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos_) << " " << e.what()).data());
        }
    }

    TSourcePosition Pos_;
};

class TUuidModule: public IUdfModule {
public:
    TStringRef Name() const {
        return TStringRef::Of("Uuid");
    }

    void CleanupOnTerminate() const override {
    }

    void GetAllFunctions(IFunctionsSink& sink) const override {
        static const TString newV7PolyArgs = BuildNoPrefixPolyArgs("Unexpected arguments for Uuid::newV7");
        static const TString newPrefixV7PolyArgs = BuildPrefixPolyArgs("Unexpected arguments for Uuid::newPrefixV7");
        static const TString newV8PolyArgs = BuildNoPrefixPolyArgs("Unexpected arguments for Uuid::newV8");
        static const TString newPrefixV8PolyArgs = BuildPrefixPolyArgs("Unexpected arguments for Uuid::newPrefixV8");

        sink.Add(TNewPrefix::Name());

        auto newV7 = sink.Add(TNewUuid<false, false>::Name());
        newV7->SetTypeAwareness();
        newV7->SetPolyArgs(TStringRef(newV7PolyArgs));

        auto newPrefixV7 = sink.Add(TNewUuid<false, true>::Name());
        newPrefixV7->SetTypeAwareness();
        newPrefixV7->SetPolyArgs(TStringRef(newPrefixV7PolyArgs));

        auto newV8 = sink.Add(TNewUuid<true, false>::Name());
        newV8->SetTypeAwareness();
        newV8->SetPolyArgs(TStringRef(newV8PolyArgs));

        auto newPrefixV8 = sink.Add(TNewUuid<true, true>::Name());
        newPrefixV8->SetTypeAwareness();
        newPrefixV8->SetPolyArgs(TStringRef(newPrefixV8PolyArgs));
    }

    void BuildFunctionTypeInfo(
        const TStringRef& name,
        TType* userType,
        const TStringRef& typeConfig,
        ui32 flags,
        IFunctionTypeInfoBuilder& builder) const override
    {
        Y_UNUSED(typeConfig);
        try {
            const bool typesOnly = (flags & TFlags::TypesOnly);
            const bool found = TNewPrefix::DeclareSignature(name, userType, builder, typesOnly)
                || TNewUuid<false, false>::DeclareSignature(name, userType, builder, typesOnly)
                || TNewUuid<false, true>::DeclareSignature(name, userType, builder, typesOnly)
                || TNewUuid<true, false>::DeclareSignature(name, userType, builder, typesOnly)
                || TNewUuid<true, true>::DeclareSignature(name, userType, builder, typesOnly);
            if (!found) {
                builder.SetError(TStringBuilder() << "Unknown function: " << TStringBuf(name));
            }
        } catch (const std::exception&) {
            builder.SetError(CurrentExceptionMessage());
        }
    }
};

} // namespace

REGISTER_MODULES(TUuidModule)
