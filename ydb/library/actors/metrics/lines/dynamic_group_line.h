#pragma once

#include "../line.h"
#include "compressed_line_storage.h"
#include <util/generic/yexception.h>

namespace NActors {

    enum class EGroupValueType : ui8 { Unsigned, Signed, Decimal, Bool };
    enum class EGroupUpdateMode : ui8 { All, OnChangeAll, OnChangePartial };

    struct TDynamicGroupField {
        TString Name;
        TVector<TLabel> Labels;
        EGroupValueType Type = EGroupValueType::Unsigned;
    };

    // Schema is immutable for the lifetime of a line; the number of participants
    // is chosen at registration. Reconfiguration creates a new line generation.
    struct TDynamicGroupSchema : TLineFrontendOps {
        static constexpr size_t MaxFields = 128;
        const TVector<TDynamicGroupField> Definitions;
        const EGroupUpdateMode Mode;

        TDynamicGroupSchema(TVector<TDynamicGroupField> fields, EGroupUpdateMode mode);
        TDynamicGroupSchema(const TDynamicGroupSchema&) = delete;
        TDynamicGroupSchema& operator=(const TDynamicGroupSchema&) = delete;

    private:
        TVector<TVector<TLineLabelView>> LabelViews;
        TVector<TLineFieldMeta> Metadata;
    };

    struct TDynamicGroupFrontend {
        using TCodec = TCompressedLineStorage<100'000, TIntegerEncoding<>>;
        struct TCache {
            bool Valid = false;
            std::array<ui64, TDynamicGroupSchema::MaxFields> Values = {};
        };
        struct TValueType {
            const TDynamicGroupSchema* Schema;
            std::span<const TLineNumericValue> Values;
            TCache* Cache;
        };
        struct TConfig {
            std::shared_ptr<const TDynamicGroupSchema> Schema;
        };
        static TLineMeta MakeMeta(const TConfig& config) {
            Y_ENSURE(config.Schema);
            TLineMeta meta(config.Schema.get());
            meta.FrontendOwner = config.Schema;
            return meta;
        }
        static bool Append(IMetricLine& line, const TValueType& value) noexcept;
        static void ReadNumericRange(const TLineSnapshot& snapshot, TInstant begin, TInstant end,
            void* opaque, TLineFrontendOps::TInvokeNumericValues invoke);
    };

    // Own the per-writer on-change cache alongside the handle, not in shared
    // schema metadata. Append never allocates and only successful writes update it.
    class TDynamicGroupLine {
    public:
        template<class TSystem>
        static TDynamicGroupLine Create(TSystem* system, TStringBuf name,
                TVector<TDynamicGroupField> fields, EGroupUpdateMode mode = EGroupUpdateMode::All) {
            TDynamicGroupLine result;
            result.Schema = std::make_shared<TDynamicGroupSchema>(std::move(fields), mode);
            result.Line = system->template CreateLine<TDynamicGroupFrontend>(name, {}, {result.Schema});
            return result;
        }
        bool Append(std::span<const TLineNumericValue> values) noexcept {
            return Schema && Line.Append({Schema.get(), values, &Cache});
        }
        explicit operator bool() const noexcept { return bool(Line); }
        ui32 GetLineId() const noexcept { return Line.GetLineId(); }
        size_t FieldCount() const noexcept { return Schema ? Schema->Definitions.size() : 0; }
        void Close() noexcept { Line.Close(); }

    private:
        std::shared_ptr<const TDynamicGroupSchema> Schema;
        TDynamicGroupFrontend::TCache Cache;
        TLine<TDynamicGroupFrontend> Line;
    };

} // namespace NActors
