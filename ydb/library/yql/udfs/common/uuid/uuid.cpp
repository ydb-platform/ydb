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
// For plain random IDs without sort semantics, use RandomUuid() instead.
//
// Assemble bytes in YDB internal (Microsoft GUID) layout and return as Uuid.
// No RFC↔YDB conversion: generators already write sort-order-aware bytes.

using namespace NYql;
using namespace NYql::NUdf;

namespace {

constexpr ui32 MaxDepArgs = 32;

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

TString BuildCallableTypeWithUniversalDeps(ui32 depCount, bool hasPrefix) {
    TStringBuilder sb;
    sb << "[CallableType;[];[];[";
    if (hasPrefix) {
        sb << "[[DataType;Uint64]";
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
    sb << "; {type=" << BuildCallableTypeWithUniversalDeps(depCount, /*hasPrefix=*/false) << "}]";
}

void AppendPrefixPolyArgRule(TStringBuilder& sb, ui32 depCount) {
    sb << "[";
    if (depCount == 0) {
        sb << "{cmd=type;arg=T0;value=[DataType;Uint64]}";
    } else {
        sb << "{cmd=and;value=[{cmd=type;arg=T0;value=[DataType;Uint64]}";
        for (ui32 i = 0; i < depCount; ++i) {
            sb << ";" << BuildDepArgKindsPredicate(TStringBuilder() << "T" << (i + 1));
        }
        sb << "]}";
    }
    sb << "; {type=" << BuildCallableTypeWithUniversalDeps(depCount, /*hasPrefix=*/true) << "}]";
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
        AppendPrefixPolyArgRule(sb, depCount);
    }
    if (!first) {
        sb << ";";
    }
    AppendPrefixPolyArgRule(sb, 0);
    sb << "; [{cmd=error;message=\"" << errorMessage << "\"}; {}]]";
    return sb;
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

// IsSharded=true  → prefix-first layout with second-granularity timestamp.
// IsSharded=false → timestamp-first internal byte layout.
// HasPrefix=true  → caller supplies prefix (e.g. RandomNumber() once per batch);
//                   keys target a single partition range.
template <bool IsSharded, bool HasPrefix>
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
            return MakeUuidValue(valueBuilder, IsSharded, prefix, HasPrefix);
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
