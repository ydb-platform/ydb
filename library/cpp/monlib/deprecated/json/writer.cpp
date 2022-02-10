#include "writer.h"

namespace NMonitoring {
    TDeprecatedJsonWriter::TDeprecatedJsonWriter(IOutputStream* out)
        : JsonWriter(out, false)
        , State(STATE_ROOT)
    {
    }

    void TDeprecatedJsonWriter::TransitionState(EState current, EState next) {
        if (State != current) {
            ythrow yexception() << "wrong state";
        }
        State = next;
    }

    void TDeprecatedJsonWriter::OpenDocument() {
        TransitionState(STATE_ROOT, STATE_DOCUMENT);
        JsonWriter.OpenMap();
    }

    void TDeprecatedJsonWriter::CloseDocument() {
        TransitionState(STATE_DOCUMENT, STATE_ROOT);
        JsonWriter.CloseMap();
        JsonWriter.Flush();
    }

    void TDeprecatedJsonWriter::OpenCommonLabels() {
        TransitionState(STATE_DOCUMENT, STATE_COMMON_LABELS);
        JsonWriter.Write("commonLabels");
        JsonWriter.OpenMap();
    }

    void TDeprecatedJsonWriter::CloseCommonLabels() {
        TransitionState(STATE_COMMON_LABELS, STATE_DOCUMENT);
        JsonWriter.CloseMap();
    }

    void TDeprecatedJsonWriter::WriteCommonLabel(TStringBuf name, TStringBuf value) {
        TransitionState(STATE_COMMON_LABELS, STATE_COMMON_LABELS);
        JsonWriter.Write(name, value);
    }

    void TDeprecatedJsonWriter::OpenMetrics() {
        TransitionState(STATE_DOCUMENT, STATE_METRICS);
        JsonWriter.Write("sensors");
        JsonWriter.OpenArray();
    }

    void TDeprecatedJsonWriter::CloseMetrics() {
        TransitionState(STATE_METRICS, STATE_DOCUMENT);
        JsonWriter.CloseArray();
    }

    void TDeprecatedJsonWriter::OpenMetric() {
        TransitionState(STATE_METRICS, STATE_METRIC);
        JsonWriter.OpenMap();
    }

    void TDeprecatedJsonWriter::CloseMetric() {
        TransitionState(STATE_METRIC, STATE_METRICS);
        JsonWriter.CloseMap();
    }

    void TDeprecatedJsonWriter::OpenLabels() {
        TransitionState(STATE_METRIC, STATE_LABELS);
        JsonWriter.Write("labels");
        JsonWriter.OpenMap();
    }

    void TDeprecatedJsonWriter::CloseLabels() {
        TransitionState(STATE_LABELS, STATE_METRIC);
        JsonWriter.CloseMap();
    }

    void TDeprecatedJsonWriter::WriteLabel(TStringBuf name, TStringBuf value) {
        TransitionState(STATE_LABELS, STATE_LABELS);
        JsonWriter.Write(name, value);
    }

    void TDeprecatedJsonWriter::WriteModeDeriv() {
        TransitionState(STATE_METRIC, STATE_METRIC);
        JsonWriter.Write("mode", "deriv");
    }

    void TDeprecatedJsonWriter::WriteValue(long long value) {
        TransitionState(STATE_METRIC, STATE_METRIC);
        JsonWriter.Write("value", value);
    }

    void TDeprecatedJsonWriter::WriteDoubleValue(double value) {
        TransitionState(STATE_METRIC, STATE_METRIC);
        JsonWriter.Write("value", value);
    }

    void TDeprecatedJsonWriter::WriteTs(ui64 ts) {
        TransitionState(STATE_METRIC, STATE_METRIC);
        JsonWriter.Write("ts", ts);
    }
}
