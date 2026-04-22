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

class ISpan {
public:
    virtual ~ISpan() = default;
    virtual void End() = 0;
    virtual void SetAttribute(const std::string& key, const std::string& value) = 0;
    virtual void SetAttribute(const std::string& key, int64_t value) = 0;
    virtual void AddEvent(const std::string& name, const std::map<std::string, std::string>& attributes = {}) = 0;
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
