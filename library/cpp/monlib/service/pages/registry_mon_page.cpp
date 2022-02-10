#include "registry_mon_page.h"

#include <library/cpp/monlib/encode/text/text.h>
#include <library/cpp/monlib/encode/json/json.h>
#include <library/cpp/monlib/encode/prometheus/prometheus.h>
#include <library/cpp/monlib/encode/spack/spack_v1.h>
#include <library/cpp/monlib/service/format.h>

namespace NMonitoring {
    void TMetricRegistryPage::Output(NMonitoring::IMonHttpRequest& request) {
        const auto formatStr = TStringBuf{request.GetPathInfo()}.RNextTok('/');
        auto& out = request.Output();

        if (!formatStr.empty()) {
            IMetricEncoderPtr encoder;
            TString resp;

            if (formatStr == TStringBuf("json")) {
                resp = HTTPOKJSON;
                encoder = NMonitoring::EncoderJson(&out);
            } else if (formatStr == TStringBuf("spack")) {
                resp = HTTPOKSPACK;
                const auto compression = ParseCompression(request);
                encoder = NMonitoring::EncoderSpackV1(&out, ETimePrecision::SECONDS, compression);
            } else if (formatStr == TStringBuf("prometheus")) {
                resp = HTTPOKPROMETHEUS;
                encoder = NMonitoring::EncoderPrometheus(&out);
            } else {
                ythrow yexception() << "unsupported metric encoding format: " << formatStr;
            }

            out.Write(resp);
            RegistryRawPtr_->Accept(TInstant::Zero(), encoder.Get());

            encoder->Close();
        } else {
            THtmlMonPage::Output(request);
        }
    }

    void TMetricRegistryPage::OutputText(IOutputStream& out, NMonitoring::IMonHttpRequest&) {
        IMetricEncoderPtr encoder = NMonitoring::EncoderText(&out);
        RegistryRawPtr_->Accept(TInstant::Zero(), encoder.Get());
    }

}
