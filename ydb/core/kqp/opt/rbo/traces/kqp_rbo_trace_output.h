#pragma once

#include "../html_log/cpp/optimizer_trace_output.h"

#include <util/generic/string.h>

#include <memory>
#include <optional>
#include <string>

namespace NKikimr::NKqp {

class TRBOContext;

class TRBOTraceOutputHandle {
public:
    explicit TRBOTraceOutputHandle(std::string logName = "new RBO HTML trace log");
    ~TRBOTraceOutputHandle();

    TRBOTraceOutputHandle(const TRBOTraceOutputHandle&) = delete;
    TRBOTraceOutputHandle& operator=(const TRBOTraceOutputHandle&) = delete;
    TRBOTraceOutputHandle(TRBOTraceOutputHandle&&) noexcept;
    TRBOTraceOutputHandle& operator=(TRBOTraceOutputHandle&&) noexcept;

    bool IsOpen() const;
    bool HttpMode() const;
    std::string TraceUrl() const;
    const std::string& OutputPath() const;
    const std::string& Error() const;
    const std::optional<std::string>& QueryText() const;

    optimizer_trace::Trace& Trace();
    optimizer_trace::GenerateResult Submit(optimizer_trace::Trace::Tile& tile);
    void Flush();

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl_;
};

class TRBOTraceOutput {
public:
    explicit TRBOTraceOutput(TRBOContext& ctx);
    ~TRBOTraceOutput();

    TRBOTraceOutput(const TRBOTraceOutput&) = delete;
    TRBOTraceOutput& operator=(const TRBOTraceOutput&) = delete;
    TRBOTraceOutput(TRBOTraceOutput&&) noexcept;
    TRBOTraceOutput& operator=(TRBOTraceOutput&&) noexcept;

    void Flush();

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl_;
};

class TScopedRboTraceTitleOverride {
public:
    explicit TScopedRboTraceTitleOverride(TString title);
    TScopedRboTraceTitleOverride(TString title, TString queryText);
    TScopedRboTraceTitleOverride(TString title, TString queryText, TString traceId);
    ~TScopedRboTraceTitleOverride();

    TScopedRboTraceTitleOverride(const TScopedRboTraceTitleOverride&) = delete;
    TScopedRboTraceTitleOverride& operator=(const TScopedRboTraceTitleOverride&) = delete;

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl_;
};

TString MakeRboTraceId();

} // namespace NKikimr::NKqp
