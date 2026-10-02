#include "format.h"

#include "config.h"
#include "metrics.h"

#include <util/string/builder.h>
#include <util/string/printf.h>

namespace NPlan2Svg {

TString FormatDurationMs(ui64 durationMs) {
    TStringBuilder builder;

    if (durationMs && durationMs < 100) {
        builder << durationMs << "ms";
    } else {
        auto seconds = durationMs / 1'000;
        if (seconds >= 60) {
            auto minutes = seconds / 60;
            if (minutes >= 60) {
                auto hours = minutes / 60;
                builder << hours << 'h';
                if (hours < 24) {
                    auto minutes60 = minutes % 60;
                    builder << ' ';
                    if (minutes60 < 10) {
                        builder << '0';
                    }
                    builder << minutes60 << 'm';
                }
            } else {
                auto seconds60 = seconds % 60;
                builder << minutes << "m ";
                if (seconds60 < 10) {
                    builder << '0';
                }
                builder << seconds60 << 's';
            }
        } else {
            auto hundredths = (durationMs % 1'000) / 10;
            builder << seconds << '.';
            if (hundredths < 10) {
                builder << '0';
            }
            builder << hundredths << 's';
        }
    }

    return builder;
}

TString FormatDurationUs(ui64 durationUs) {
    if (durationUs && durationUs < 1000) {
        return TStringBuilder() << durationUs << "us";
    }

    return FormatDurationMs(durationUs / 1000);
}

TString FormatUsage(ui64 usec) {
    return FormatDurationUs(usec);
}

TString FormatIntegerValue(ui64 i, ui32 scale, const TString& suffix) {
    if (i < scale) {
        return Sprintf("%lu%s", i, suffix.c_str());
    }
    for (auto c : "KMGTP") {
        auto pcs = (i % scale) * 100 / scale;
        i /= scale;
        if (i < scale || c == 'P') {
            return Sprintf("%lu.%.2lu%c%s", i, pcs, c, suffix.c_str());
        }
    }
    return "";
}

TString FormatBytes(ui64 bytes) {
    return FormatIntegerValue(bytes, 1024, "B");
}

TString FormatInteger(ui64 bytes) {
    return FormatIntegerValue(bytes);
}

TString FormatTimeMs(ui64 time) {
    time /= 10;
    auto sec = time / 100;
    if (sec >= 3600) {
        auto hours = sec / 3600;
        sec = sec % 3600;
        return Sprintf("%lu:%02lu:%02lu", hours, sec / 60, sec % 60);
    } else if (sec < 10) {
        return Sprintf("0:%02lu.%02lu", sec, time % 100);
    } else {
        return Sprintf("%lu:%02lu", sec / 60, sec % 60);
    }
}

TString FormatTimeAgg(const TAggregation& agg) {
    TStringBuilder result;
    result << FormatTimeMs(agg.Min) << " | " << FormatTimeMs(agg.Avg) << " | " << FormatTimeMs(agg.Max);
    return result;
}

TString FormatMCpu(ui64 mCpu) {
    mCpu /= 10;
    return Sprintf("%lu.%.2lu", mCpu / 100, mCpu % 100);
}

TString FormatTooltip(TStringBuilder& builder, const TString& prefix, TSingleMetric* metric, TString (*format)(ui64), ui64 total) {
    TString result;
    builder << prefix;
    if (metric) {
        result = format(metric->Details.Sum);
        if (!total) {
            total = metric->Summary->Value;
        }
        if (total) {
            builder << ' ' << metric->Details.Sum * 100 / total << "%,";
        }
        if (metric->Details.Count > 1) {
            builder << " \u2211" << result << ", " << format(metric->Details.Min) << " | "
            << format(metric->Details.Avg) << " | " << format(metric->Details.Max);
        } else {
            builder << ' ' << result;
        }
    }
    return result;
}

TString FormatTooltip(TString& tooltip, const TString& prefix, TSingleMetric* metric, TString (*format)(ui64), ui64 total) {
    TStringBuilder builder;
    auto result = FormatTooltip(builder, prefix, metric, format, total);
    if (result) {
        tooltip = builder;
    }
    return result;
}

TString FormatDataFlowTooltip(TStringBuilder& tooltip, const TString& label,
    const std::shared_ptr<TSingleMetric>& bytes,
    const std::shared_ptr<TSingleMetric>& rows,
    ui64 localBytes,
    ui64 chunks,
    const std::shared_ptr<TScalarMetric>& chunkSize)
{
    auto textSum = FormatTooltip(tooltip, label, bytes.get(), FormatBytes);
    if (localBytes && bytes->Details.Sum) {
        tooltip << ", Local " << localBytes * 100 / bytes->Details.Sum << "%, \u2211" << FormatBytes(localBytes);
    }
    if (rows) {
        FormatTooltip(tooltip, ", Rows", rows.get(), FormatInteger);
        if (rows->Details.Sum) {
            tooltip << ", Width " << FormatBytes(bytes->Details.Sum / rows->Details.Sum);
        }
    }
    if (chunks) {
        tooltip << ", Chunks \u2211" << FormatInteger(chunks);
        if (chunkSize) {
            tooltip << " ~ " << FormatBytes(chunkSize->Value);
        }
    }
    return textSum;
}

TString FormatDataFlowRate(const TString& label,
    const std::shared_ptr<TSingleMetric>& bytes,
    const std::shared_ptr<TSingleMetric>& rows)
{
    TStringBuilder title;
    title << label;
    if (auto d = bytes->MaxTime - bytes->MinTime) {
        title << " " << FormatBytes(bytes->Details.Sum * 1000 / d) << "/s";
        if (rows) {
            title << ", Rows " << FormatInteger(rows->Details.Sum * 1000 / d) << "/s";
        }
    }
    return title;
}

} // namespace NPlan2Svg
