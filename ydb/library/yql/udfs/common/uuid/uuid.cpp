#include "uuid_keygen.h"

#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/public/udf/udf_value_builder.h>

#include <util/system/datetime.h>

#include <vector>

// Uuid UDF: key-friendly UUID generators and RFC helpers.
//
// Returned values are raw 16-byte YDB internal Uuid representation.
// Primary-key helpers (UUIDv8 layouts from the pk_generation RFC):
//   - newV8RowKey: shard spread via 12-bit prefix + time locality within a prefix;
//   - newV8ColumnKey: chronological clustering by creation time (seconds);
//   - newV8RowGroup: batch of row keys sharing a common prefix (Uint64 or Uuid).
// Standard variants:
//   - newV4: random UUID v4 (analogue of RandomUuid());
//   - newV7 / newV7At: RFC 9562 UUID v7 (stored in YDB mixed-endian layout);
//   - extractTs / extractTs64: timestamp from a v7 UUID, NULL otherwise.
//
// Optional dependency arguments [T1, ...] work like RandomUuid(): they control
// when the function is evaluated per row, not the value contents.
// Key generators write sort-order-aware bytes directly (no RFC↔YDB conversion).

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

TString BuildCallableTypeRowGroup(ui32 depCount, EPrefixArgType prefixArg) {
    Y_ENSURE(prefixArg != EPrefixArgType::None);
    const TStringBuf prefixTypeName = prefixArg == EPrefixArgType::Uuid ? "Uuid" : "Uint64";
    TStringBuilder sb;
    sb << "[CallableType;[];[];[[[DataType;" << prefixTypeName << "];[DataType;Uint64]";
    for (ui32 i = 0; i < depCount; ++i) {
        sb << ";[UniversalType]";
    }
    sb << ";[[ListType;[DataType;Uuid]]]]]";
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

void AppendRowGroupPolyArgRule(TStringBuilder& sb, ui32 depCount, EPrefixArgType prefixArg) {
    Y_ENSURE(prefixArg != EPrefixArgType::None);
    const TStringBuf prefixTypeName = prefixArg == EPrefixArgType::Uuid ? "Uuid" : "Uint64";

    sb << "[";
    if (depCount == 0) {
        sb << "{cmd=and;value=["
           << "{cmd=type;arg=T0;value=[DataType;" << prefixTypeName << "]};"
           << "{cmd=type;arg=T1;value=[DataType;Uint64]}"
           << "]}";
    } else {
        sb << "{cmd=and;value=["
           << "{cmd=type;arg=T0;value=[DataType;" << prefixTypeName << "]};"
           << "{cmd=type;arg=T1;value=[DataType;Uint64]}";
        for (ui32 i = 0; i < depCount; ++i) {
            sb << ";" << BuildDepArgKindsPredicate(TStringBuilder() << "T" << (i + 2));
        }
        sb << "]}";
    }
    sb << "; {type=" << BuildCallableTypeRowGroup(depCount, prefixArg) << "}]";
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

TString BuildRowGroupPolyArgs(TStringBuf errorMessage) {
    TStringBuilder sb;
    sb << "[[";
    bool first = true;
    for (ui32 depCount = MaxDepArgs; depCount > 0; --depCount) {
        if (!first) {
            sb << ";";
        }
        first = false;
        AppendRowGroupPolyArgRule(sb, depCount, EPrefixArgType::Uuid);
        sb << ";";
        AppendRowGroupPolyArgRule(sb, depCount, EPrefixArgType::Uint64);
    }
    if (!first) {
        sb << ";";
    }
    AppendRowGroupPolyArgRule(sb, 0, EPrefixArgType::Uuid);
    sb << ";";
    AppendRowGroupPolyArgRule(sb, 0, EPrefixArgType::Uint64);
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

TUnboxedValue MakeUuidFromBytes(
    const IValueBuilder* valueBuilder,
    const std::array<ui8, NKikimr::NUuid::UUID_LEN>& bytes)
{
    return valueBuilder->NewString(TStringRef(
        reinterpret_cast<const char*>(bytes.data()),
        bytes.size()));
}

TUnboxedValue MakeRfcV7UuidValue(const IValueBuilder* valueBuilder, ui64 timestampMs) {
    return MakeUuidFromBytes(valueBuilder, NUuidKeyGen::MakeRfcV7YdbBytes(timestampMs));
}

TUnboxedValue MakeV4UuidValue(const IValueBuilder* valueBuilder) {
    return MakeUuidFromBytes(valueBuilder, NUuidKeyGen::MakeV4UuidBytes());
}

TUnboxedValue MakeRowKeyUuidValue(
    const IValueBuilder* valueBuilder, ui64 prefix, bool hasPrefix)
{
    return MakeUuidFromBytes(
        valueBuilder,
        NUuidKeyGen::MakeRowKeyUuidBytes(prefix, Seconds(), hasPrefix));
}

TUnboxedValue MakeColumnKeyUuidValue(const IValueBuilder* valueBuilder) {
    return MakeUuidFromBytes(
        valueBuilder,
        NUuidKeyGen::MakeColumnKeyUuidBytes(Seconds()));
}

enum class EKeyKind {
    RowKey,
    ColumnKey,
    V4,
};

template <EKeyKind Kind>
class TNewUuid: public TBoxedValue {
public:
    using TTypeAwareMarker = bool;

    explicit TNewUuid(TSourcePosition pos)
        : Pos_(pos)
    {
    }

    static const TStringRef& Name() {
        if constexpr (Kind == EKeyKind::RowKey) {
            static auto name = TStringRef::Of("newV8RowKey");
            return name;
        } else if constexpr (Kind == EKeyKind::ColumnKey) {
            static auto name = TStringRef::Of("newV8ColumnKey");
            return name;
        } else {
            static auto name = TStringRef::Of("newV4");
            return name;
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
        Y_UNUSED(args);
        try {
            if constexpr (Kind == EKeyKind::RowKey) {
                return MakeRowKeyUuidValue(valueBuilder, 0, false);
            } else if constexpr (Kind == EKeyKind::ColumnKey) {
                return MakeColumnKeyUuidValue(valueBuilder);
            } else {
                return MakeV4UuidValue(valueBuilder);
            }
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos_) << " " << e.what()).data());
        }
    }

    TSourcePosition Pos_;
};

template <bool PrefixFromUuid>
class TNewV8RowGroup: public TBoxedValue {
public:
    using TTypeAwareMarker = bool;

    explicit TNewV8RowGroup(TSourcePosition pos)
        : Pos_(pos)
    {
    }

    static const TStringRef& Name() {
        static auto name = TStringRef::Of("newV8RowGroup");
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
        if (argCount < 2) {
            builder.SetError("Expected prefix and count arguments.");
            return true;
        }
        if (IsUuidArgType(*typeHelper, argsTypeInspector.GetElementType(0)) != PrefixFromUuid) {
            return false;
        }

        auto argsBuilder = builder.Args(argCount);
        for (ui32 i = 0; i < argCount; ++i) {
            argsBuilder->Add(argsTypeInspector.GetElementType(i));
        }
        argsBuilder->Done().Returns<TListType<TUuid>>();

        if (!typesOnly) {
            builder.Implementation(new TNewV8RowGroup(GetSourcePosition(builder)));
        }
        return true;
    }

private:
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
        try {
            const ui64 prefix = ReadPrefixArg(args[0], PrefixFromUuid);
            const ui64 count = args[1].Get<ui64>();
            if (count > NUuidKeyGen::MaxRowGroupCount) {
                throw std::runtime_error(TStringBuilder()
                    << "Uuid::newV8RowGroup count must be at most "
                    << NUuidKeyGen::MaxRowGroupCount);
            }

            const ui64 epochSeconds = Seconds();
            std::vector<TUnboxedValue> items;
            items.reserve(count);
            for (ui64 i = 0; i < count; ++i) {
                items.push_back(MakeUuidFromBytes(
                    valueBuilder,
                    NUuidKeyGen::MakeRowKeyUuidBytes(prefix, epochSeconds, true)));
            }
            return valueBuilder->NewList(items.data(), items.size());
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
            // UUID v7 encodes only unix_ts_ms (48-bit millisecond timestamp per RFC 9562).
            // Timestamp/Timestamp64 arguments are microsecond-based, so sub-millisecond
            // digits are truncated here. Consequently extractTs(newV7At(ts)) equals ts
            // only when ts falls on a millisecond boundary; otherwise the result is
            // floor(ts_us / 1000) * 1000.
            const ui64 timestampUs = ReadTimestampMicros(args[0], Timestamp64);
            const ui64 timestampMs = timestampUs / 1000;
            return MakeRfcV7UuidValue(valueBuilder, timestampMs);
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos_) << " " << e.what()).data());
        }
    }

    TSourcePosition Pos_;
};

template <bool Timestamp64>
class TExtractTs: public TBoxedValue {
public:
    using TTypeAwareMarker = bool;

    explicit TExtractTs(TSourcePosition pos)
        : Pos_(pos)
    {
    }

    static const TStringRef& Name() {
        static auto name = Timestamp64
            ? TStringRef::Of("extractTs64")
            : TStringRef::Of("extractTs");
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

        Y_UNUSED(userType);
        if constexpr (Timestamp64) {
            builder.Args(1)->Add<TUuid>().Done().Returns<TOptional<TTimestamp64>>();
        } else {
            builder.Args(1)->Add<TUuid>().Done().Returns<TOptional<TTimestamp>>();
        }

        if (!typesOnly) {
            builder.Implementation(new TExtractTs(GetSourcePosition(builder)));
        }
        return true;
    }

private:
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
        Y_UNUSED(valueBuilder);
        try {
            const auto ref = args[0].AsStringRef();
            if (ref.Size() != NKikimr::NUuid::UUID_LEN) {
                throw std::runtime_error("Expected Uuid value of 16 bytes");
            }
            const auto micros = NUuidKeyGen::ExtractV7TimestampMicrosFromYdbBytes(
                reinterpret_cast<const ui8*>(ref.Data()));
            if (!micros) {
                return TUnboxedValuePod();
            }
            if constexpr (Timestamp64) {
                return TUnboxedValuePod(static_cast<i64>(*micros)).MakeOptional();
            } else {
                return TUnboxedValuePod(*micros).MakeOptional();
            }
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
        static const TString newV8RowKeyPolyArgs = BuildNoPrefixPolyArgs("Unexpected arguments for Uuid::newV8RowKey");
        static const TString newV8ColumnKeyPolyArgs = BuildNoPrefixPolyArgs("Unexpected arguments for Uuid::newV8ColumnKey");
        static const TString newV8RowGroupPolyArgs = BuildRowGroupPolyArgs("Unexpected arguments for Uuid::newV8RowGroup");
        static const TString newV4PolyArgs = BuildNoPrefixPolyArgs("Unexpected arguments for Uuid::newV4");
        static const TString newV7PolyArgs = BuildNoPrefixPolyArgs("Unexpected arguments for Uuid::newV7");
        static const TString newV7AtPolyArgs = BuildTimestampPolyArgs("Unexpected arguments for Uuid::newV7At");

        auto newV8RowKey = sink.Add(TNewUuid<EKeyKind::RowKey>::Name());
        newV8RowKey->SetTypeAwareness();
        newV8RowKey->SetPolyArgs(TStringRef(newV8RowKeyPolyArgs));

        auto newV8ColumnKey = sink.Add(TNewUuid<EKeyKind::ColumnKey>::Name());
        newV8ColumnKey->SetTypeAwareness();
        newV8ColumnKey->SetPolyArgs(TStringRef(newV8ColumnKeyPolyArgs));

        auto newV8RowGroup = sink.Add(TNewV8RowGroup<false>::Name());
        newV8RowGroup->SetTypeAwareness();
        newV8RowGroup->SetPolyArgs(TStringRef(newV8RowGroupPolyArgs));

        auto newV4 = sink.Add(TNewUuid<EKeyKind::V4>::Name());
        newV4->SetTypeAwareness();
        newV4->SetPolyArgs(TStringRef(newV4PolyArgs));

        auto newV7 = sink.Add(TNewV7::Name());
        newV7->SetTypeAwareness();
        newV7->SetPolyArgs(TStringRef(newV7PolyArgs));

        auto newV7At = sink.Add(TNewV7At<false>::Name());
        newV7At->SetTypeAwareness();
        newV7At->SetPolyArgs(TStringRef(newV7AtPolyArgs));

        sink.Add(TExtractTs<false>::Name());
        sink.Add(TExtractTs<true>::Name());
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
            const bool found = TNewUuid<EKeyKind::RowKey>::DeclareSignature(name, userType, builder, typesOnly)
                || TNewUuid<EKeyKind::ColumnKey>::DeclareSignature(name, userType, builder, typesOnly)
                || TNewV8RowGroup<false>::DeclareSignature(name, userType, builder, typesOnly)
                || TNewV8RowGroup<true>::DeclareSignature(name, userType, builder, typesOnly)
                || TNewUuid<EKeyKind::V4>::DeclareSignature(name, userType, builder, typesOnly)
                || TNewV7::DeclareSignature(name, userType, builder, typesOnly)
                || TNewV7At<false>::DeclareSignature(name, userType, builder, typesOnly)
                || TNewV7At<true>::DeclareSignature(name, userType, builder, typesOnly)
                || TExtractTs<false>::DeclareSignature(name, userType, builder, typesOnly)
                || TExtractTs<true>::DeclareSignature(name, userType, builder, typesOnly);
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
