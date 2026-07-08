#include "uuid_keygen.h"

#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/public/udf/udf_value_builder.h>

#include <util/system/datetime.h>

using namespace NYql;
using namespace NYql::NUdf;

namespace {

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

template <bool IsV8>
class TNewUuid: public TBoxedValue {
public:
    explicit TNewUuid(TSourcePosition pos)
        : Pos_(pos)
    {
    }

    static const TStringRef& Name() {
        if constexpr (IsV8) {
            static auto name = TStringRef::Of("newV8");
            return name;
        } else {
            static auto name = TStringRef::Of("newV7");
            return name;
        }
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

        builder.Args(1)
            ->Add(builder.Optional()->Item<ui64>().Build())
            .Done()
            .Returns<TUuid>()
            .OptionalArgs(1)
            .IsStrict();

        if (!typesOnly) {
            builder.Implementation(new TNewUuid(GetSourcePosition(builder)));
        }
        return true;
    }

private:
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
        try {
            const bool hasPrefix = static_cast<bool>(args[0]);
            const ui64 prefix = hasPrefix ? args[0].Get<ui64>() : 0;

            std::array<ui8, NKikimr::NUuid::UUID_LEN> bytes{};
            if constexpr (IsV8) {
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
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos_) << " " << e.what()).data());
        }
    }

    TSourcePosition Pos_;
};

} // namespace

SIMPLE_MODULE(TUuidModule,
              TNewPrefix,
              TNewUuid<false>,
              TNewUuid<true>)

REGISTER_MODULES(TUuidModule)
