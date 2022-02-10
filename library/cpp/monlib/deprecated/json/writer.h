#pragma once

#include <library/cpp/json/json_writer.h>

namespace NMonitoring {
    /**
     * Deprecated writer of Solomon JSON format
     * https://wiki.yandex-team.ru/solomon/api/dataformat/json
     *
     * This writer will be deleted soon, so please consider to use
     * high level library library/cpp/monlib/encode which is decoupled from the
     * particular format.
     */
    class TDeprecatedJsonWriter {
    private:
        NJson::TJsonWriter JsonWriter;
        enum EState {
            STATE_ROOT,
            STATE_DOCUMENT,
            STATE_COMMON_LABELS,
            STATE_METRICS,
            STATE_METRIC,
            STATE_LABELS,
        };
        EState State;

    public:
        explicit TDeprecatedJsonWriter(IOutputStream* out);

        void OpenDocument();
        void CloseDocument();

        void OpenCommonLabels();
        void CloseCommonLabels();

        void WriteCommonLabel(TStringBuf name, TStringBuf value);

        void OpenMetrics();
        void CloseMetrics();

        void OpenMetric();
        void CloseMetric();

        void OpenLabels();
        void CloseLabels();

        void WriteLabel(TStringBuf name, TStringBuf value);

        template <typename... T>
        void WriteLabels(T... pairs) {
            OpenLabels();
            WriteLabelsInner(pairs...);
            CloseLabels();
        }

        void WriteModeDeriv();

        void WriteValue(long long value);
        void WriteDoubleValue(double d);

        void WriteTs(ui64 ts);

    private:
        void WriteLabelsInner(TStringBuf name, TStringBuf value) {
            WriteLabel(name, value);
        }

        template <typename... T>
        void WriteLabelsInner(TStringBuf name, TStringBuf value, T... pairs) {
            WriteLabel(name, value);
            WriteLabelsInner(pairs...);
        }

        inline void TransitionState(EState current, EState next);
    };
}
