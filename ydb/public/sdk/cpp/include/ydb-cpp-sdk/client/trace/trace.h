#pragma once

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace NYdb::inline Dev::NTrace {

using TAttribute = std::pair<std::string_view, std::string_view>;
using TAttributes = std::initializer_list<TAttribute>;

enum class ESpanKind {
    INTERNAL,
    SERVER,
    CLIENT,
    PRODUCER,
    CONSUMER
};

enum class ESpanStatus {
    Unset,
    Ok,
    Error
};

class IScope {
public:
    virtual ~IScope() = default;
};

class ISpan {
public:
    virtual ~ISpan() = default;
    virtual void End() = 0;
    virtual void SetAttribute(std::string_view key, std::string_view value) = 0;
    virtual void SetAttribute(std::string_view key, int64_t value) = 0;
    virtual void AddEvent(std::string_view name, TAttributes attributes = {}) = 0;

    virtual std::unique_ptr<IScope> Activate() {
        return nullptr;
    }

    virtual void SetStatus(ESpanStatus /*status*/, std::string_view /*description*/ = {}) {}
};

class ITracer {
public:
    virtual ~ITracer() = default;

    virtual std::shared_ptr<ISpan> StartSpan(
        const std::string& name
        , ESpanKind kind = ESpanKind::INTERNAL
    ) = 0;

    virtual std::shared_ptr<ISpan> StartSpan(
        const std::string& name
        , ESpanKind kind
        , ISpan* parent
    ) {
        (void)parent;
        return StartSpan(name, kind);
    }

    virtual std::string GetCurrentTraceparent() const = 0;
};

class ITraceProvider {
public:
    virtual ~ITraceProvider() = default;
    virtual std::shared_ptr<ITracer> GetTracer(const std::string& name) = 0;
};

} // namespace NYdb::NTrace
