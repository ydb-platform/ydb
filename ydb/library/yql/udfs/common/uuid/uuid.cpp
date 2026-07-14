#include "uuid_keygen.h"

#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/public/udf/udf_value_builder.h>

#include <util/system/datetime.h>

// Uuid UDF: key-friendly UUID generators for row tables.
//
// Returned values are raw 16-byte YDB internal Uuid representation.
// Use as primary keys when you want:
//   - newChrono: chronological clustering by creation time;
//   - newSharded: shard spread via random prefix + time locality within a prefix.
//   - newV7: RFC 9562 UUID v7 for external interoperability (non-sortable in YDB);
//   - newV7At: RFC 9562 UUID v7 with a fixed Timestamp/Timestamp64.
// Prefix variants accept Uint64 or Uuid as the first argument; the Uuid overload
// reuses the top PrefixBits of the source value as the generated key prefix.
// For plain random IDs without sort semantics, use RandomUuid() instead.
//
// Assemble bytes in YDB internal (Microsoft GUID) layout and return as Uuid.
// No RFC↔YDB conversion: generators already write sort-order-aware bytes.

using namespace NYql;
using namespace NYql::NUdf;

namespace {

constexpr ui32 MaxDepArgs = 32;

enum class EPrefixArgType {
    None,
    Uint64,
    Uuid,
};

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

TString BuildAndDepArgKindsPredicate(ui32 depCount, ui32 firstArgIndex = 0) {
    Y_ENSURE(depCount > 0);
    TStringBuilder sb;
    sb << "{cmd=and;value=[";
    for (ui32 i = 0; i < depCount; ++i) {
        if (i > 0) {
            sb << ";";
        }
        sb << BuildDepArgKindsPredicate(TStringBuilder() << "T" << (firstArgIndex + i));
    }
    sb << "]}";
    return sb;
}

TString BuildCallableTypeWithUniversalDeps(ui32 depCount, EPrefixArgType prefixArg) {
    TStringBuilder sb;
    sb << "[CallableType;[];[];[";
    if (prefixArg != EPrefixArgType::None) {
        const TStringBuf prefixTypeName = prefixArg == EPrefixArgType::Uuid ? "Uuid" : "Uint64";
        sb << "[[DataType;" << prefixTypeName << "]";
        for (ui32 i = 0; i < depCount; ++i) {
            sb << ";[UniversalType]";
        }
        sb << ";[[DataType;Uuid]]]";
    } else {
        for (ui32 i = 0; i < depCount; ++i) {
            sb << "[UniversalType]";
            if (i + 1 < depCount) {
                sb << ";";
            }
        }
        if (depCount > 0) {
            sb << ";";
        }
        sb << "[[DataType;Uuid]]]";
    }
    sb << "]]";
    return sb;
}

void AppendNoPrefixPolyArgRule(TStringBuilder& sb, ui32 depCount) {
    sb << "[";
    if (depCount == 0) {
        sb << "[]";
    } else {
        sb << BuildAndDepArgKindsPredicate(depCount);
    }
    sb << "; {type=" << BuildCallableTypeWithUniversalDeps(depCount, EPrefixArgType::None) << "}]";
}

void AppendPrefixPolyArgRule(TStringBuilder& sb, ui32 depCount, EPrefixArgType prefixArg) {
    Y_ENSURE(prefixArg != EPrefixArgType::None);
    const TStringBuf prefixTypeName = prefixArg == EPrefixArgType::Uuid ? "Uuid" : "Uint64";

    sb << "[";
    if (depCount == 0) {
        sb << "{cmd=type;arg=T0;value=[DataType;" << prefixTypeName << "]}";
    } else {
        sb << "{cmd=and;value=[{cmd=type;arg=T0;value=[DataType;" << prefixTypeName << "]}";
        for (ui32 i = 0; i < depCount; ++i) {
            sb << ";" << BuildDepArgKindsPredicate(TStringBuilder() << "T" << (i + 1));
        }
        sb << "]}";
    }
    sb << "; {type=" << BuildCallableTypeWithUniversalDeps(depCount, prefixArg) << "}]";
}

TString BuildNoPrefixPolyArgs(TStringBuf errorMessage) {
    TStringBuilder sb;
    sb << "[[";
    bool first = true;
    for (ui32 depCount = MaxDepArgs; depCount > 0; --depCount) {
        if (!first) {
            sb << ";";
        }
        first = false;
        AppendNoPrefixPolyArgRule(sb, depCount);
    }
    if (!first) {
        sb << ";";
    }
    AppendNoPrefixPolyArgRule(sb, 0);
    sb << "; [{cmd=error;message=\"" << errorMessage << "\"}; {}]]";
    return sb;
}

TString BuildPrefixPolyArgs(TStringBuf errorMessage) {
    TStringBuilder sb;
    sb << "[[";
    bool first = true;
    for (ui32 depCount = MaxDepArgs; depCount > 0; --depCount) {
        if (!first) {
            sb << ";";
        }
        first = false;
        AppendPrefixPolyArgRule(sb, depCount, EPrefixArgType::Uuid);
        sb << ";";
        AppendPrefixPolyArgRule(sb, depCount, EPrefixArgType::Uint64);
    }
    if (!first) {
        sb << ";";
    }
    AppendPrefixPolyArgRule(sb, 0, EPrefixArgType::Uuid);
    sb << ";";
    AppendPrefixPolyArgRule(sb, 0, EPrefixArgType::Uint64);
    sb << "; [{cmd=error;message=\"" << errorMessage << "\"}; {}]]";
    return sb;
}

TString BuildCallableTypeWithTimestampAndDeps(ui32 depCount, bool timestamp64) {
    const TStringBuf timestampTypeName = timestamp64 ? "Timestamp64" : "Timestamp";
    TStringBuilder sb;
    sb << "[CallableType;[];[];[";
    sb << "[[DataType;" << timestampTypeName << "]";
    for (ui32 i = 0; i < depCount; ++i) {
        sb << ";[UniversalType]";
    }
    sb << ";[[DataType;Uuid]]]";
    sb << "]]";
    return sb;
}

void AppendTimestampPolyArgRule(TStringBuilder& sb, ui32 depCount, bool timestamp64) {
    const TStringBuf timestampTypeName = timestamp64 ? "Timestamp64" : "Timestamp";

    sb << "[";
    if (depCount == 0) {
        sb << "{cmd=type;arg=T0;value=[DataType;" << timestampTypeName << "]}";
    } else {
        sb << "{cmd=and;value=[{cmd=type;arg=T0;value=[DataType;" << timestampTypeName << "]}";
        for (ui32 i = 0; i < depCount; ++i) {
            sb << ";" << BuildDepArgKindsPredicate(TStringBuilder() << "T" << (i + 1));
        }
        sb << "]}";
    }
    sb << "; {type=" << BuildCallableTypeWithTimestampAndDeps(depCount, timestamp64) << "}]";
}

TString BuildTimestampPolyArgs(TStringBuf errorMessage) {
    TStringBuilder sb;
    sb << "[[";
    bool first = true;
    for (ui32 depCount = MaxDepArgs; depCount > 0; --depCount) {
        if (!first) {
            sb << ";";
        }
        first = false;
        AppendTimestampPolyArgRule(sb, depCount, /*timestamp64=*/true);
        sb << ";";
        AppendTimestampPolyArgRule(sb, depCount, /*timestamp64=*/false);
    }
    if (!first) {
        sb << ";";
    }
    AppendTimestampPolyArgRule(sb, 0, /*timestamp64=*/true);
    sb << ";";
    AppendTimestampPolyArgRule(sb, 0, /*timestamp64=*/false);
    sb << "; [{cmd=error;message=\"" << errorMessage << "\"}; {}]]";
    return sb;
}

ui64 ReadPrefixArg(const TUnboxedValuePod& arg, bool prefixFromUuid) {
    if (prefixFromUuid) {
        const auto ref = arg.AsStringRef();
        if (ref.Size() != NKikimr::NUuid::UUID_LEN) {
            throw std::runtime_error("Expected Uuid value of 16 bytes");
        }
        return NUuidKeyGen::ExtractPrefixFromUuidBytes(
            reinterpret_cast<const ui8*>(ref.Data()));
    }
    return arg.Get<ui64>();
}

bool IsUuidArgType(const ITypeInfoHelper1& typeHelper, const TType* argType) {
    TDataTypeInspector argInspector(typeHelper, argType);
    return argInspector && argInspector.GetTypeId() == NUdf::TDataType<NUdf::TUuid>::Id;
}

bool IsTimestamp64ArgType(const ITypeInfoHelper1& typeHelper, const TType* argType) {
    TDataTypeInspector argInspector(typeHelper, argType);
    return argInspector && argInspector.GetTypeId() == NUdf::TDataType<NUdf::TTimestamp64>::Id;
}

ui64 ReadTimestampMicros(const TUnboxedValuePod& arg, bool timestamp64) {
    const i64 micros = timestamp64 ? arg.Get<i64>() : static_cast<i64>(arg.Get<ui64>());
    if (micros < 0) {
        throw std::runtime_error("Timestamp must be non-negative");
    }
    return static_cast<ui64>(micros);
}

TUnboxedValue MakeRfcV7UuidValue(const IValueBuilder* valueBuilder, ui64 timestampMs) {
    const auto bytes = NUuidKeyGen::MakeRfcV7YdbBytes(timestampMs);
    return valueBuilder->NewString(TStringRef(
        reinterpret_cast<const char*>(bytes.data()),
        bytes.size()));
}

// Returns a Uuid value as 16 raw bytes in the internal format, which is Microsoft-style mixed endian GUID.
TUnboxedValue MakeUuidValue(const IValueBuilder* valueBuilder, bool isSharded, ui64 prefix, bool hasPrefix) {
    std::array<ui8, NKikimr::NUuid::UUID_LEN> bytes{};
    if (isSharded) {
        const ui64 epochSeconds = MilliSeconds() / 1000;
        bytes = NUuidKeyGen::MakeShardedUuidBytes(prefix, epochSeconds, hasPrefix);
    } else {
        ui64 timestampMs = MilliSeconds();
        bytes = NUuidKeyGen::MakeChronoUuidBytes(prefix, timestampMs, hasPrefix);
    }

    return valueBuilder->NewString(TStringRef(
        reinterpret_cast<const char*>(bytes.data()),
        bytes.size()));
}

// IsSharded=true       → prefix-first layout with second-granularity timestamp.
// IsSharded=false      → timestamp-first internal byte layout.
// HasPrefix=true       → caller supplies prefix (Uint64 or Uuid as first argument).
// PrefixFromUuid=true  → take top PrefixBits from the source Uuid MSB.
template <bool IsSharded, bool HasPrefix, bool PrefixFromUuid = false>
class TNewUuid: public TBoxedValue {
public:
    using TTypeAwareMarker = bool;

    explicit TNewUuid(TSourcePosition pos)
        : Pos_(pos)
    {
    }

    static const TStringRef& Name() {
        if constexpr (HasPrefix) {
            if constexpr (IsSharded) {
                static auto name = TStringRef::Of("newShardedPrefix");
                return name;
            } else {
                static auto name = TStringRef::Of("newChronoPrefix");
                return name;
            }
        } else {
            if constexpr (IsSharded) {
                static auto name = TStringRef::Of("newSharded");
                return name;
            } else {
                static auto name = TStringRef::Of("newChrono");
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
            if (IsUuidArgType(*typeHelper, argsTypeInspector.GetElementType(0)) != PrefixFromUuid) {
                return false;
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
                prefix = ReadPrefixArg(args[0], PrefixFromUuid);
            }
            return MakeUuidValue(valueBuilder, IsSharded, prefix, HasPrefix);
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos_) << " " << e.what()).data());
        }
    }

    TSourcePosition Pos_;
};

class TNewV7: public TBoxedValue {
public:
    using TTypeAwareMarker = bool;

    explicit TNewV7(TSourcePosition pos)
        : Pos_(pos)
    {
    }

    static const TStringRef& Name() {
        static auto name = TStringRef::Of("newV7");
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
        auto argsBuilder = builder.Args(argCount);
        for (ui32 i = 0; i < argCount; ++i) {
            argsBuilder->Add(argsTypeInspector.GetElementType(i));
        }
        argsBuilder->Done().Returns<TUuid>();

        if (!typesOnly) {
            builder.Implementation(new TNewV7(GetSourcePosition(builder)));
        }
        return true;
    }

private:
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
        Y_UNUSED(args);
        try {
            return MakeRfcV7UuidValue(valueBuilder, MilliSeconds());
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos_) << " " << e.what()).data());
        }
    }

    TSourcePosition Pos_;
};

template <bool Timestamp64>
class TNewV7At: public TBoxedValue {
public:
    using TTypeAwareMarker = bool;

    explicit TNewV7At(TSourcePosition pos)
        : Pos_(pos)
    {
    }

    static const TStringRef& Name() {
        static auto name = TStringRef::Of("newV7At");
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
        if (argCount < 1) {
            builder.SetError("Expected timestamp argument.");
            return true;
        }
        if (IsTimestamp64ArgType(*typeHelper, argsTypeInspector.GetElementType(0)) != Timestamp64) {
            return false;
        }

        auto argsBuilder = builder.Args(argCount);
        for (ui32 i = 0; i < argCount; ++i) {
            argsBuilder->Add(argsTypeInspector.GetElementType(i));
        }
        argsBuilder->Done().Returns<TUuid>();

        if (!typesOnly) {
            builder.Implementation(new TNewV7At(GetSourcePosition(builder)));
        }
        return true;
    }

private:
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
        try {
            const ui64 timestampUs = ReadTimestampMicros(args[0], Timestamp64);
            const ui64 timestampMs = timestampUs / 1000;
            return MakeRfcV7UuidValue(valueBuilder, timestampMs);
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
        static const TString newChronoPolyArgs = BuildNoPrefixPolyArgs("Unexpected arguments for Uuid::newChrono");
        static const TString newChronoPrefixPolyArgs = BuildPrefixPolyArgs("Unexpected arguments for Uuid::newChronoPrefix");
        static const TString newShardedPolyArgs = BuildNoPrefixPolyArgs("Unexpected arguments for Uuid::newSharded");
        static const TString newShardedPrefixPolyArgs = BuildPrefixPolyArgs("Unexpected arguments for Uuid::newShardedPrefix");
        static const TString newV7PolyArgs = BuildNoPrefixPolyArgs("Unexpected arguments for Uuid::newV7");
        static const TString newV7AtPolyArgs = BuildTimestampPolyArgs("Unexpected arguments for Uuid::newV7At");

        auto newChrono = sink.Add(TNewUuid<false, false>::Name());
        newChrono->SetTypeAwareness();
        newChrono->SetPolyArgs(TStringRef(newChronoPolyArgs));

        auto newChronoPrefix = sink.Add(TNewUuid<false, true>::Name());
        newChronoPrefix->SetTypeAwareness();
        newChronoPrefix->SetPolyArgs(TStringRef(newChronoPrefixPolyArgs));

        auto newSharded = sink.Add(TNewUuid<true, false>::Name());
        newSharded->SetTypeAwareness();
        newSharded->SetPolyArgs(TStringRef(newShardedPolyArgs));

        auto newShardedPrefix = sink.Add(TNewUuid<true, true>::Name());
        newShardedPrefix->SetTypeAwareness();
        newShardedPrefix->SetPolyArgs(TStringRef(newShardedPrefixPolyArgs));

        auto newV7 = sink.Add(TNewV7::Name());
        newV7->SetTypeAwareness();
        newV7->SetPolyArgs(TStringRef(newV7PolyArgs));

        auto newV7At = sink.Add(TNewV7At<false>::Name());
        newV7At->SetTypeAwareness();
        newV7At->SetPolyArgs(TStringRef(newV7AtPolyArgs));
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
            const bool found = TNewUuid<false, false>::DeclareSignature(name, userType, builder, typesOnly)
                || TNewUuid<false, true, false>::DeclareSignature(name, userType, builder, typesOnly)
                || TNewUuid<false, true, true>::DeclareSignature(name, userType, builder, typesOnly)
                || TNewUuid<true, false>::DeclareSignature(name, userType, builder, typesOnly)
                || TNewUuid<true, true, false>::DeclareSignature(name, userType, builder, typesOnly)
                || TNewUuid<true, true, true>::DeclareSignature(name, userType, builder, typesOnly)
                || TNewV7::DeclareSignature(name, userType, builder, typesOnly)
                || TNewV7At<false>::DeclareSignature(name, userType, builder, typesOnly)
                || TNewV7At<true>::DeclareSignature(name, userType, builder, typesOnly);
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
