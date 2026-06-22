#include "kqp_rbo_trace_output.h"

#include "../kqp_rbo_context.h"
#include "../html_log/cpp/optimizer_trace_output.h"

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/system/env.h>
#include <util/system/types.h>

#include <yql/essentials/utils/log/log.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

namespace NKikimr::NKqp {

using namespace NYql;

namespace {

std::atomic<ui64> RboTraceSeq = 0;
std::mutex RboTraceMetadataOverrideMutex;

struct TRboTraceMetadataOverride {
    std::optional<std::string> Title;
    std::optional<std::string> QueryText;
};

std::optional<TRboTraceMetadataOverride> RboTraceMetadataOverride;

optimizer_trace::TracePageOptions MakeRboTracePageOptions() {
    optimizer_trace::TracePageOptions options;
    options.compression = optimizer_trace::GenerateOptions::Compression::Brotli;
    options.maxBufferedRules = 20;
    options.maxBufferedPayloadBytes = 8 * 1024 * 1024;
    options.flushOnEverySubmit = false;
    return options;
}

std::string ToStdString(const TString& value) {
    return std::string(value.data(), value.size());
}

std::optional<TRboTraceMetadataOverride> GetRboTraceMetadataOverride() {
    std::lock_guard<std::mutex> lock(RboTraceMetadataOverrideMutex);
    return RboTraceMetadataOverride;
}

std::string BuildRboTraceTitle(const std::optional<TRboTraceMetadataOverride>& metadata) {
    if (metadata && metadata->Title && !metadata->Title->empty()) {
        return *metadata->Title;
    }

    return optimizer_trace::MakeDefaultTraceTitle();
}

optimizer_trace::TraceOutputOptions MakeRboTraceOutputOptions(const TString& output, ui64 traceSeq) {
    optimizer_trace::TraceOutputOptions options;
    options.output = ToStdString(output);
    options.pageOptions = MakeRboTracePageOptions();
    options.fallbackSuffix = std::to_string(traceSeq);
    options.httpTitle = "RBO Trace";
    options.emptyHttpMessage = "RBO trace is not flushed yet. Refreshing...";
    options.httpThreadNamePrefix = "rbo-trace";
    return options;
}

} // anonymous namespace

struct TScopedRboTraceTitleOverride::TImpl {
    explicit TImpl(TString title, std::optional<TString> queryText) {
        std::lock_guard<std::mutex> lock(RboTraceMetadataOverrideMutex);
        Previous_ = RboTraceMetadataOverride;

        TRboTraceMetadataOverride metadata;
        metadata.Title = ToStdString(title);
        if (queryText && !queryText->empty()) {
            metadata.QueryText = ToStdString(*queryText);
        }
        RboTraceMetadataOverride = std::move(metadata);
    }

    ~TImpl() {
        std::lock_guard<std::mutex> lock(RboTraceMetadataOverrideMutex);
        RboTraceMetadataOverride = std::move(Previous_);
    }

private:
    std::optional<TRboTraceMetadataOverride> Previous_;
};

TScopedRboTraceTitleOverride::TScopedRboTraceTitleOverride(TString title)
    : Impl_(std::make_unique<TImpl>(std::move(title), std::nullopt))
{
}

TScopedRboTraceTitleOverride::TScopedRboTraceTitleOverride(TString title, TString queryText)
    : Impl_(std::make_unique<TImpl>(std::move(title), std::move(queryText)))
{
}

TScopedRboTraceTitleOverride::~TScopedRboTraceTitleOverride() = default;

struct TRBOTraceOutput::TImpl {
    explicit TImpl(TRBOContext& ctx) {
        TMaybe<TString> htmlTraceOutput = TryGetEnv("NEW_RBO_LOG");
        if (!htmlTraceOutput.Defined() || htmlTraceOutput->empty()) {
            return;
        }

        TraceSeq = ++RboTraceSeq;
        Output = std::make_unique<optimizer_trace::TraceOutput>(
            MakeRboTraceOutputOptions(*htmlTraceOutput, TraceSeq));
        if (!Output->isOpen()) {
            YQL_CLOG(WARN, CoreDq) << "Failed to prepare new RBO HTML trace log: "
                                   << Output->error();
            return;
        }

        if (Output->httpMode()) {
            YQL_CLOG(WARN, CoreDq) << "Serving new RBO HTML trace log at "
                                   << Output->traceUrl();
        }

        auto metadata = GetRboTraceMetadataOverride();
        auto& trace = Output->trace(BuildRboTraceTitle(metadata));
        ctx.TraceLog.Configure(trace, [
            tracePath = Output->output(),
            traceOutput = Output.get(),
            reportedSubmitFailure = false
        ](optimizer_trace::Trace::Tile& tile) mutable {
            if (!traceOutput) {
                return;
            }

            auto result = traceOutput->submit(tile);
            if (!result && !reportedSubmitFailure) {
                YQL_CLOG(WARN, CoreDq) << "Failed to submit new RBO HTML trace tile to "
                                       << tracePath << ": " << result.error;
                reportedSubmitFailure = true;
            }
        });
        if (metadata && metadata->QueryText && !metadata->QueryText->empty()) {
            ctx.TraceLog.SetQueryText(metadata->QueryText);
        }
    }

    void Flush() {
        if (!Output || !Output->isOpen()) {
            return;
        }

        auto result = Output->flush();
        if (!result) {
            YQL_CLOG(WARN, CoreDq) << "Failed to flush new RBO HTML trace log: "
                                   << result.error;
        }
    }

    ui64 TraceSeq = 0;
    std::unique_ptr<optimizer_trace::TraceOutput> Output;
};

TRBOTraceOutput::TRBOTraceOutput(TRBOContext& ctx)
    : Impl_(std::make_unique<TImpl>(ctx))
{
}

TRBOTraceOutput::~TRBOTraceOutput() = default;
TRBOTraceOutput::TRBOTraceOutput(TRBOTraceOutput&&) noexcept = default;
TRBOTraceOutput& TRBOTraceOutput::operator=(TRBOTraceOutput&&) noexcept = default;

void TRBOTraceOutput::Flush() {
    if (Impl_) {
        Impl_->Flush();
    }
}

} // namespace NKikimr::NKqp
