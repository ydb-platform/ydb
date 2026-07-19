#include "kqp_rbo_trace_output.h"

#include "../kqp_rbo_context.h"
#include "../html_log/cpp/optimizer_trace_output.h"

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/system/env.h>
#include <util/system/getpid.h>
#include <util/system/thread.h>
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
std::mutex RboTraceEnvMutex;
constexpr const char* RboTraceTitleEnv = "NEW_RBO_TRACE_TITLE";
constexpr const char* RboTraceQueryTextEnv = "NEW_RBO_TRACE_QUERY_TEXT";
constexpr const char* RboTraceIdEnv = "NEW_RBO_TRACE_ID";

struct TRboTraceMetadataOverride {
    std::optional<std::string> Title;
    std::optional<std::string> QueryText;
    std::optional<std::string> TraceId;
};

optimizer_trace::TracePageOptions MakeRboTracePageOptions() {
    optimizer_trace::TracePageOptions options;
    options.maxBufferedRules = 20;
    options.maxBufferedPayloadBytes = 8 * 1024 * 1024;
    options.flushOnEverySubmit = false;
    return options;
}

std::string ToStdString(const TString& value) {
    return std::string(value.data(), value.size());
}

std::optional<TRboTraceMetadataOverride> GetRboTraceMetadataOverride() {
    std::lock_guard<std::mutex> lock(RboTraceEnvMutex);

    TRboTraceMetadataOverride metadata;
    bool hasMetadata = false;

    if (const auto title = TryGetEnv(RboTraceTitleEnv); title.Defined() && !title->empty()) {
        metadata.Title = ToStdString(*title);
        hasMetadata = true;
    }
    if (const auto queryText = TryGetEnv(RboTraceQueryTextEnv); queryText.Defined() && !queryText->empty()) {
        metadata.QueryText = ToStdString(*queryText);
        hasMetadata = true;
    }
    if (const auto traceId = TryGetEnv(RboTraceIdEnv); traceId.Defined() && !traceId->empty()) {
        metadata.TraceId = ToStdString(*traceId);
        hasMetadata = true;
    }

    if (!hasMetadata) {
        return std::nullopt;
    }
    return metadata;
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
    explicit TImpl(TString title, std::optional<TString> queryText, std::optional<TString> traceId) {
        std::lock_guard<std::mutex> lock(RboTraceEnvMutex);
        PreviousTitle_ = TryGetEnv(RboTraceTitleEnv);
        PreviousQueryText_ = TryGetEnv(RboTraceQueryTextEnv);
        PreviousTraceId_ = TryGetEnv(RboTraceIdEnv);

        SetEnv(RboTraceTitleEnv, title);
        SetOrUnsetEnv(RboTraceQueryTextEnv, std::move(queryText));
        SetOrUnsetEnv(RboTraceIdEnv, std::move(traceId));
    }

    ~TImpl() {
        std::lock_guard<std::mutex> lock(RboTraceEnvMutex);
        RestoreEnv(RboTraceTitleEnv, PreviousTitle_);
        RestoreEnv(RboTraceQueryTextEnv, PreviousQueryText_);
        RestoreEnv(RboTraceIdEnv, PreviousTraceId_);
    }

private:
    static void SetOrUnsetEnv(const TString& key, std::optional<TString> value) {
        if (value && !value->empty()) {
            SetEnv(key, *value);
        } else {
            UnsetEnv(key);
        }
    }

    static void RestoreEnv(const TString& key, const TMaybe<TString>& value) {
        if (value.Defined()) {
            SetEnv(key, *value);
        } else {
            UnsetEnv(key);
        }
    }

    TMaybe<TString> PreviousTitle_;
    TMaybe<TString> PreviousQueryText_;
    TMaybe<TString> PreviousTraceId_;
};

TScopedRboTraceTitleOverride::TScopedRboTraceTitleOverride(TString title)
    : Impl_(std::make_unique<TImpl>(std::move(title), std::nullopt, std::nullopt))
{
}

TScopedRboTraceTitleOverride::TScopedRboTraceTitleOverride(TString title, TString queryText)
    : Impl_(std::make_unique<TImpl>(std::move(title), std::move(queryText), std::nullopt))
{
}

TScopedRboTraceTitleOverride::TScopedRboTraceTitleOverride(TString title, TString queryText, TString traceId)
    : Impl_(std::make_unique<TImpl>(std::move(title), std::move(queryText), std::move(traceId)))
{
}

TScopedRboTraceTitleOverride::~TScopedRboTraceTitleOverride() = default;

struct TRBOTraceOutputHandle::TImpl {
    explicit TImpl(std::string logName)
        : LogName(std::move(logName))
    {
        TMaybe<TString> htmlTraceOutput = TryGetEnv("NEW_RBO_LOG");
        if (!htmlTraceOutput.Defined() || htmlTraceOutput->empty()) {
            return;
        }

        TraceSeq = ++RboTraceSeq;
        Output = std::make_unique<optimizer_trace::TraceOutput>(
            MakeRboTraceOutputOptions(*htmlTraceOutput, TraceSeq));
        if (!Output->isOpen()) {
            YQL_CLOG(WARN, CoreDq) << "Failed to prepare " << LogName << ": "
                                   << Output->error();
            return;
        }

        if (Output->httpMode()) {
            YQL_CLOG(WARN, CoreDq) << "Serving " << LogName << " at "
                                   << Output->traceUrl();
        }

        auto metadata = GetRboTraceMetadataOverride();
        if (metadata && metadata->TraceId && !metadata->TraceId->empty()) {
            Trace = &Output->trace(BuildRboTraceTitle(metadata), *metadata->TraceId);
        } else {
            Trace = &Output->trace(BuildRboTraceTitle(metadata));
        }
        if (metadata && metadata->QueryText && !metadata->QueryText->empty()) {
            QueryText = metadata->QueryText;
        }
    }

    bool IsOpen() const {
        return Output && Output->isOpen() && Trace;
    }

    optimizer_trace::GenerateResult Submit(optimizer_trace::Trace::Tile& tile) {
        if (!IsOpen()) {
            return optimizer_trace::GenerateResult::failure("RBO trace output is not open");
        }

        return Output->submit(tile);
    }

    void Flush() {
        if (!IsOpen()) {
            return;
        }

        auto result = Output->flush();
        if (!result) {
            YQL_CLOG(WARN, CoreDq) << "Failed to flush " << LogName << ": "
                                   << result.error;
        }
    }

    std::string LogName;
    ui64 TraceSeq = 0;
    std::unique_ptr<optimizer_trace::TraceOutput> Output;
    optimizer_trace::Trace* Trace = nullptr;
    std::optional<std::string> QueryText;
};

TRBOTraceOutputHandle::TRBOTraceOutputHandle(std::string logName)
    : Impl_(std::make_unique<TImpl>(std::move(logName)))
{
}

TRBOTraceOutputHandle::~TRBOTraceOutputHandle() = default;
TRBOTraceOutputHandle::TRBOTraceOutputHandle(TRBOTraceOutputHandle&&) noexcept = default;
TRBOTraceOutputHandle& TRBOTraceOutputHandle::operator=(TRBOTraceOutputHandle&&) noexcept = default;

bool TRBOTraceOutputHandle::IsOpen() const {
    return Impl_ && Impl_->IsOpen();
}

bool TRBOTraceOutputHandle::HttpMode() const {
    return Impl_ && Impl_->Output && Impl_->Output->httpMode();
}

std::string TRBOTraceOutputHandle::TraceUrl() const {
    if (!Impl_ || !Impl_->Output) {
        return {};
    }

    return Impl_->Output->traceUrl();
}

const std::string& TRBOTraceOutputHandle::OutputPath() const {
    static const std::string Empty;
    if (!Impl_ || !Impl_->Output) {
        return Empty;
    }

    return Impl_->Output->output();
}

const std::string& TRBOTraceOutputHandle::Error() const {
    static const std::string Empty;
    if (!Impl_ || !Impl_->Output) {
        return Empty;
    }

    return Impl_->Output->error();
}

const std::optional<std::string>& TRBOTraceOutputHandle::QueryText() const {
    static const std::optional<std::string> Empty;
    if (!Impl_) {
        return Empty;
    }

    return Impl_->QueryText;
}

optimizer_trace::Trace& TRBOTraceOutputHandle::Trace() {
    return *Impl_->Trace;
}

optimizer_trace::GenerateResult TRBOTraceOutputHandle::Submit(optimizer_trace::Trace::Tile& tile) {
    return Impl_->Submit(tile);
}

void TRBOTraceOutputHandle::Flush() {
    if (Impl_) {
        Impl_->Flush();
    }
}

struct TRBOTraceOutput::TImpl {
    explicit TImpl(TRBOContext& ctx)
        : Output("new RBO HTML trace log")
    {
        if (!Output.IsOpen()) {
            return;
        }

        auto& trace = Output.Trace();
        ctx.TraceLog.Configure(trace, [
            tracePath = Output.OutputPath(),
            traceOutput = &Output
        ](optimizer_trace::Trace::Tile& tile) mutable {
            if (!traceOutput) {
                return;
            }

            auto result = traceOutput->Submit(tile);
            if (!result) {
                YQL_CLOG(WARN, CoreDq) << "Failed to submit new RBO HTML trace tile to "
                                       << tracePath << ": " << result.error;
            }
        });
        if (Output.QueryText()) {
            ctx.TraceLog.SetQueryText(Output.QueryText());
        }
    }

    void Flush() {
        Output.Flush();
    }

    TRBOTraceOutputHandle Output;
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

TString MakeRboTraceId() {
    return TStringBuilder()
        << "trace-"
        << GetPID()
        << "-"
        << TThread::CurrentThreadNumericId()
        << "-"
        << RandomNumber<ui64>()
        << "-"
        << RandomNumber<ui64>();
}

} // namespace NKikimr::NKqp
