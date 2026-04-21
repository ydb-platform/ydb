#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>

namespace NYdb::inline Dev::NTrace {

enum class ESpanKind {
    INTERNAL,
    SERVER,
    CLIENT,
    PRODUCER,
    CONSUMER
};

class IScope {
public:
    virtual ~IScope() = default;
};

class ISpan {
public:
    virtual ~ISpan() = default;
    virtual void End() = 0;
    virtual void SetAttribute(const std::string& key, const std::string& value) = 0;
    virtual void SetAttribute(const std::string& key, int64_t value) = 0;
    virtual void AddEvent(const std::string& name, const std::map<std::string, std::string>& attributes = {}) = 0;
    virtual std::unique_ptr<IScope> Activate() = 0;

    virtual void RecordException(
        const std::string& type,
        const std::string& message,
        const std::string& stacktrace = {}
    ) {
        std::map<std::string, std::string> attrs{
            {"exception.type", type},
            {"exception.message", message},
        };
        if (!stacktrace.empty()) {
            attrs.emplace("exception.stacktrace", stacktrace);
        }
        AddEvent("exception", attrs);
    }
};

class ITracer {
public:
    virtual ~ITracer() = default;
    virtual std::shared_ptr<ISpan> StartSpan(const std::string& name, ESpanKind kind = ESpanKind::INTERNAL) = 0;
};

class ITraceProvider {
public:
    virtual ~ITraceProvider() = default;
    virtual std::shared_ptr<ITracer> GetTracer(const std::string& name) = 0;
};

} // namespace NYdb::NTrace
