#pragma once

#include <cmath>
#include <util/string/printf.h>
#include <util/stream/str.h>
#include <util/generic/set.h>
#include <util/generic/map.h>
#include "data.h"
#include "util.h"

namespace NAnalytics {

struct TAxis {
    TString Name;
    double From;
    double To;
    char Symbol;
    bool Ticks;

    TAxis(const TString& name, double from, double to, char symbol = '+', bool ticks = true)
        : Name(name)
        , From(from)
        , To(to)
        , Symbol(symbol)
        , Ticks(ticks)
    {}

    double Place(double value) const
    {
        return (value - From) / (To - From);
    }

    bool Get(const TRow& row, double& value) const
    {
        return row.Get(Name, value);
    }
};

struct TChart {
    int Width;
    int Height;
    bool Frame;

    TChart(int width, int height, bool frame = true)
        : Width(width)
        , Height(height)
        , Frame(frame)
    {
        if (Width < 2)
            Width = 2;
        if (Height < 2)
            Height = 2;
    }
};

struct TPoint {
    int X;
    int Y;
    char* Pixel;
};

class TScreen {
public:
    TChart Chart;
    TAxis Ox;
    TAxis Oy;
    TVector<char> Screen;
public:
    TScreen(const TChart& chart, const TAxis& ox, const TAxis& oy)
        : Chart(chart)
        , Ox(ox)
        , Oy(oy)
        , Screen(Chart.Width * Chart.Height, ' ')
    {}

    int X(double x) const
    {
        return llrint(Ox.Place(x) * (Chart.Width - 1));
    }

    int Y(double y) const
    {
        return Chart.Height - 1 - llrint(Oy.Place(y) * (Chart.Height - 1));
    }

    TPoint At(double x, double y)
    {
        TPoint pt{X(x), Y(y), nullptr};
        if (Fits(pt)) {
            pt.Pixel = &Screen[pt.Y * Chart.Width + pt.X];
        }
        return pt;
    }

    bool Fits(TPoint pt) const
    {
        return pt.X >= 0 && pt.X < Chart.Width
            && pt.Y >= 0 && pt.Y < Chart.Height;
    }

    TString Str() const
    {
        TStringStream ss;
        size_t i = 0;
        TString lmargin;
        TString x1label;
        TString x2label;
        TString xtick = "-";
        TString y1label;
        TString y2label;
        TString ytick = "|";
        if (Ox.Ticks) {
            x1label = Sprintf("%-7.2le", Ox.From);
            x2label = Sprintf("%-7.2le", Ox.To);
            xtick = "+";
        }
        if (Oy.Ticks) {
            y1label = Sprintf("%-7.2le ", Oy.From);
            y2label = Sprintf("%-7.2le ", Oy.To);
            int sz = Max(y1label.size(), y2label.size());
            y1label = TString(sz - y1label.size(), ' ') + y1label;
            y2label = TString(sz - y2label.size(), ' ') + y2label;
            lmargin = TString(sz, ' ');
            ytick = "+";
        }
        if (Chart.Frame) {
            ss << lmargin << "." << TString(Chart.Width, '-') << ".\n";
        }
        for (int iy = 0; iy < Chart.Height; iy++) {
            if (iy == 0) {
                ss << y2label;
                if (Chart.Frame)
                    ss << ytick;
            } else if (iy == Chart.Height - 1) {
                ss << y1label;
                if (Chart.Frame)
                    ss << ytick;
            } else {
                ss << lmargin;
                if (Chart.Frame)
                    ss << "|";
            }
            for (int ix = 0; ix < Chart.Width; ix++)
                ss << Screen[i++];
            if (Chart.Frame)
                ss << "|";
            ss << "\n";
        }
        if (Chart.Frame) {
            ss << lmargin << "'" << xtick
               << TString(Chart.Width - 2, '-')
               << xtick << "'\n";
        }
        if (Ox.Ticks) {
            ss << lmargin << " " << x1label
               << TString(Max(Chart.Width - int(x1label.size()) - int(x2label.size()), 1), ' ')
               << x2label << "\n";
        }
        return ss.Str();
    }
};

class TLegend {
public:
    TMap<TString, char> Symbols;
    char DefSymbol = '+';
public:
    void Register(const TString& name)
    {
        if (name)
            Symbols[name] = DefSymbol;
    }

    void Build()
    {
        char c = 'A';
        for (auto& kv : Symbols) {
            if (!kv.first)
                continue;
            kv.second = c;
            if (c == '9')
                c = 'a';
            else if (c == 'z')
                c = 'A';
            else if (c == 'Z')
                c = '1';
            else
                c++;
        }
    }

    char Get(const TString& name) const
    {
        auto iter = Symbols.find(name);
        return iter != Symbols.end()? iter->second: DefSymbol;
    }

    TString Str(size_t columns) const
    {
        if (columns == 0)
            columns = 1;
        size_t height = (Symbols.size() + columns - 1) / columns;
        TVector<TString> all;
        TVector<size_t> widths;
        size_t width = 0;
        size_t count = 0;
        for (auto kv : Symbols) {
            TString s = Sprintf("(%ld) %c = %s", count + 1, kv.second, kv.first.data());
            width = Max(width, s.size());
            all.push_back(s);
            if (++count % height == 0) {
                widths.push_back(width);
                width = 0;
            }
        }
        widths.push_back(width);

        TStringStream ss;
        for (size_t row = 0; row < height; ++row) {
            bool first = true;
            for (size_t col = 0; col < widths.size(); col++) {
                size_t idx = col * height + row;
                if (idx < all.size()) {
                    ss << (first? "": "   ") << Sprintf("%-*s", (int)widths[col], all[idx].data());
                    first = false;
                }
            }
            ss << Endl;
        }
        return ss.Str();
    }
};

inline TString PlotAsAsciiArt(const TTable& in, const TChart& chart, const TAxis& ox, const TAxis& oy, bool showLegend = true, size_t columns = 4)
{
    TLegend legend;
    legend.DefSymbol = oy.Symbol;
    for (const TRow& row : in) {
        legend.Register(row.Name);
    }
    legend.Build();

    TScreen screen(chart, ox, oy);
    for (const TRow& row : in) {
        double x, y;
        if (ox.Get(row, x) && oy.Get(row, y)) {
            TPoint pt = screen.At(Finitize(x), Finitize(y));
            if (pt.Pixel) {
                *pt.Pixel = legend.Get(row.Name);
            }
        }
    }
    return screen.Str() + (showLegend? TString("\n") + legend.Str(columns): TString());
}

}
