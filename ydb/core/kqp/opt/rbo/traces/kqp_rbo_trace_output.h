#pragma once

#include <util/generic/string.h>

#include <memory>

namespace NKikimr::NKqp {

class TRBOContext;

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
    ~TScopedRboTraceTitleOverride();

    TScopedRboTraceTitleOverride(const TScopedRboTraceTitleOverride&) = delete;
    TScopedRboTraceTitleOverride& operator=(const TScopedRboTraceTitleOverride&) = delete;

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl_;
};

} // namespace NKikimr::NKqp
