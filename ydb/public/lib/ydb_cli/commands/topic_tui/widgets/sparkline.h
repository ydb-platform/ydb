#pragma once

#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/printf.h>
#include <util/string/cast.h>
#include <util/datetime/base.h>

#include <algorithm>

namespace NYdb::NConsoleClient {

// Sparkline characters for different levels (8 levels like Unicode block elements)
// ▁▂▃▄▅▆▇█
inline constexpr const char* SparklineChars[] = {
    " ", "▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"
};

// Render a sparkline from a vector of values
inline ftxui::Element RenderSparkline(const TVector<double>& values, size_t maxWidth = 20) {
    using namespace ftxui;
    
    if (values.empty()) {
        return text(std::string(maxWidth, ' '));
    }
    
    // Find min/max
    double minVal = values[0];
    double maxVal = values[0];
    for (double v : values) {
        minVal = std::min(minVal, v);
        maxVal = std::max(maxVal, v);
    }
    
    double range = maxVal - minVal;
    if (range < 0.0001) {
        range = 1.0;
    }
    
    TString result;
    size_t start = values.size() > maxWidth ? values.size() - maxWidth : 0;
    
    for (size_t i = start; i < values.size(); ++i) {
        double normalized = (values[i] - minVal) / range;
        int level = static_cast<int>(normalized * 8);
        level = std::max(0, std::min(8, level));
        result += SparklineChars[level];
    }
    
    // Pad with spaces if needed
    while (result.size() < maxWidth) {
        result = TString(" ") + result;
    }
    
    return text(std::string(result.c_str())) | color(Color::Green);
}

// Render a horizontal gauge bar
inline ftxui::Element RenderGaugeBar(double ratio, size_t width = 10, bool showPercent = true) {
    using namespace ftxui;
    
    ratio = std::max(0.0, std::min(1.0, ratio));
    int filled = static_cast<int>(ratio * width);
    
    TString bar;
    for (size_t i = 0; i < width; ++i) {
        bar += (static_cast<int>(i) < filled) ? "█" : "░";
    }
    
    Color barColor = Color::Green;
    if (ratio > 0.8) {
        barColor = Color::Red;
    } else if (ratio > 0.5) {
        barColor = Color::Yellow;
    }
    
    Elements parts;
    parts.push_back(text(std::string(bar.c_str())) | color(barColor));
    
    if (showPercent) {
        TString percent = Sprintf(" %3.0f%%", ratio * 100);
        parts.push_back(text(std::string(percent.c_str())));
    }
    
    return hbox(parts);
}

// Format bytes to human-readable
inline TString FormatBytes(ui64 bytes) {
    if (bytes < 1024) {
        return Sprintf("%lu B", bytes);
    } else if (bytes < 1024 * 1024) {
        return Sprintf("%.1f KB", bytes / 1024.0);
    } else if (bytes < 1024 * 1024 * 1024) {
        return Sprintf("%.1f MB", bytes / (1024.0 * 1024));
    } else {
        return Sprintf("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }
}

// Format duration to human-readable
inline TString FormatDuration(TDuration d) {
    if (d.Seconds() < 60) {
        return Sprintf("%lus", d.Seconds());
    } else if (d.Minutes() < 60) {
        return Sprintf("%lum %lus", d.Minutes(), d.Seconds() % 60);
    } else if (d.Hours() < 24) {
        return Sprintf("%luh %lum", d.Hours(), d.Minutes() % 60);
    } else {
        return Sprintf("%lud %luh", d.Hours() / 24, d.Hours() % 24);
    }
}

// Format retention period in HH:MM:SS format (for long durations, shows total hours)
inline TString FormatRetention(TDuration d) {
    ui64 totalSeconds = d.Seconds();
    ui64 hours = totalSeconds / 3600;
    ui64 minutes = (totalSeconds % 3600) / 60;
    ui64 seconds = totalSeconds % 60;
    return Sprintf("%02lu:%02lu:%02lu", hours, minutes, seconds);
}

// Format number with thousands separator
inline TString FormatNumber(ui64 n) {
    TString result = ToString(n);
    if (result.size() <= 3) {
        return result;
    }
    
    TString formatted;
    formatted.reserve(result.size() + result.size() / 3);
    
    size_t firstGroup = result.size() % 3;
    if (firstGroup == 0) {
        firstGroup = 3;
    }
    
    for (size_t i = 0; i < result.size(); ++i) {
        if (i > 0 && (result.size() - i) % 3 == 0) {
            formatted.append(',');
        }
        formatted.append(result[i]);
    }
    return formatted;
}

} // namespace NYdb::NConsoleClient
