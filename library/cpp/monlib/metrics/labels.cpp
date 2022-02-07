#include "labels.h"

#include <util/stream/output.h>
#include <util/string/split.h>

static void OutputLabels(IOutputStream& out, const NMonitoring::ILabels& labels) {
    size_t i = 0;
    out << '{';
    for (const auto& label: labels) {
        if (i++ > 0) {
            out << TStringBuf(", ");
        }
        out << label;
    }
    out << '}';
}

template <>
void Out<NMonitoring::ILabelsPtr>(IOutputStream& out, const NMonitoring::ILabelsPtr& labels) {
    OutputLabels(out, *labels);
}

template <>
void Out<NMonitoring::ILabels>(IOutputStream& out, const NMonitoring::ILabels& labels) {
    OutputLabels(out, labels);
}

template <>
void Out<NMonitoring::ILabel>(IOutputStream& out, const NMonitoring::ILabel& labels) {
    out << labels.Name() << "=" << labels.Value();
}

Y_MONLIB_DEFINE_LABELS_OUT(NMonitoring::TLabels);
Y_MONLIB_DEFINE_LABEL_OUT(NMonitoring::TLabel);

namespace NMonitoring {
    bool TryLoadLabelsFromString(TStringBuf sb, ILabels& labels) {
        if (sb.Empty()) {
            return false;
        }

        if (!sb.StartsWith('{') || !sb.EndsWith('}')) {
            return false;
        }

        sb.Skip(1);
        sb.Chop(1);

        if (sb.Empty()) {
            return true;
        }

        bool ok = true;
        TVector<std::pair<TStringBuf, TStringBuf>> rawLabels;
        StringSplitter(sb).SplitBySet(" ,").SkipEmpty().Consume([&] (TStringBuf label) {
            TStringBuf key, value;
            ok &= label.TrySplit('=', key, value);

            if (!ok) {
                return;
            }

            rawLabels.emplace_back(key, value);
        });

        if (!ok) {
            return false;
        }

        for (auto&& [k, v] : rawLabels) {
            labels.Add(k, v);
        }

        return true;
    }

    bool TryLoadLabelsFromString(IInputStream& is, ILabels& labels) {
        TString str = is.ReadAll();
        return TryLoadLabelsFromString(str, labels);
    }

} // namespace NMonitoring
