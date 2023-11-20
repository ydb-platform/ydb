#pragma once

#include "defs.h"
#include "gen.h"
#include <util/generic/variant.h>

namespace NKikimr {
    namespace NIntervalGenerator {

        // an item for uniform interval distribution
        struct TUniformItem {
            const TDuration Min;
            const TDuration Max;

            template<typename TProto>
            TUniformItem(const TProto& x)
                : Min(x.HasMinUs() ? TDuration::MicroSeconds(x.GetMinUs()) : TDuration::MilliSeconds(x.GetMinMs()))
                , Max(x.HasMaxUs() ? TDuration::MicroSeconds(x.GetMaxUs()) : TDuration::MilliSeconds(x.GetMaxMs()))
            {
                const bool a = x.HasMinUs(), b = x.HasMaxUs(), c = x.HasMinMs(), d = x.HasMaxMs();
                Y_ABORT_UNLESS((a && b && !c && !d) || (!a && !b && c && d));
            }

            TDuration Generate() const {
                TDuration range = Max - Min;
                return Min + TDuration::MicroSeconds(TAppData::RandomProvider->GenRand64() % (range.GetValue() + 1));
            }
        };

        // an item for Poisson distrubution
        struct TPoissonItem {
            const double Frequency; // in Hz
            const double Xmin;

            template<typename TProto>
            TPoissonItem(const TProto& x)
                : Frequency(x.GetFrequency())
                , Xmin(exp(-Frequency * (x.GetMaxIntervalMs() * 1e-3)))
            {
                Y_ABORT_UNLESS(x.HasFrequency() && x.HasMaxIntervalMs());
            }

            TDuration Generate() const {
                const double x = Max(Xmin, TAppData::RandomProvider->GenRandReal2());
                return TDuration::Seconds(-log(x) / Frequency);
            }
        };

        struct TItem : public std::variant<TUniformItem, TPoissonItem> {
            using TBase = std::variant<TUniformItem, TPoissonItem>;

            TItem(const NKikimr::TEvLoadTestRequest::TIntervalInfo& x)
                : TBase(CreateVariantFromProtobuf(x))
            {}

            template<typename TProto>
            static TBase CreateVariantFromProtobuf(const TProto& proto) {
                switch (proto.Distribution_case()) {
                    case TProto::kUniform:
                        return TUniformItem(proto.GetUniform());

                    case TProto::kPoisson:
                        return TPoissonItem(proto.GetPoisson());

                    case TProto::DISTRIBUTION_NOT_SET:
                        Y_ABORT("TIntervalInfo.Distribution not set");
                }

                Y_ABORT("unreachable code");
            }

            TDuration Generate() const {
                auto f = [](const auto& item) { return item.Generate(); };
                return std::visit(f, *this);
            }
        };

    } // NIntervalGenerator

    using TIntervalGenerator = TGenerator<NIntervalGenerator::TItem>;

} // NKikimr
