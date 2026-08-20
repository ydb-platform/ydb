#pragma once

#include "optimizer_trace.h"

#include <memory>
#include <string>

namespace optimizer_trace {

struct TraceOutputOptions {
    std::string output;
    TracePageOptions pageOptions;
    std::string fallbackSuffix;
    std::string httpTitle = "Optimizer Trace";
    std::string emptyHttpMessage = "Optimizer trace is not flushed yet. Refreshing...";
    std::string httpThreadNamePrefix = "optimizer-trace";
};

class TraceOutput {
public:
    explicit TraceOutput(TraceOutputOptions options);
    ~TraceOutput();

    TraceOutput(const TraceOutput&) = delete;
    TraceOutput& operator=(const TraceOutput&) = delete;
    TraceOutput(TraceOutput&&) noexcept;
    TraceOutput& operator=(TraceOutput&&) noexcept;

    bool isOpen() const;
    bool httpMode() const;
    const std::string& output() const;
    std::string traceUrl() const;
    const std::string& error() const;

    Trace& trace(const std::string& title);
    Trace& trace(const std::string& title, const std::string& id);
    GenerateResult submit(Trace::Tile& tile);
    GenerateResult flush();

private:
    struct Impl;
    std::unique_ptr<Impl> Impl_;
};

std::string MakeDefaultTraceTitle();

} // namespace optimizer_trace
