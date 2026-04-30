#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/trace/trace.h>

#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace NYdb::NTests {

struct TFakeEvent {
    std::string Name;
    std::map<std::string, std::string> Attributes;
};

class TFakeScope : public NTrace::IScope {
};

class TFakeSpan : public NTrace::ISpan {
public:
    void End() override {
        std::lock_guard lock(Mutex_);
        Ended_ = true;
    }

    void SetAttribute(const std::string& key, const std::string& value) override {
        std::lock_guard lock(Mutex_);
        StringAttributes_[key] = value;
    }

    void SetAttribute(const std::string& key, int64_t value) override {
        std::lock_guard lock(Mutex_);
        IntAttributes_[key] = value;
    }

    void AddEvent(const std::string& name, const std::map<std::string, std::string>& attributes) override {
        std::lock_guard lock(Mutex_);
        Events_.push_back({name, attributes});
    }

    std::unique_ptr<NTrace::IScope> Activate() override {
        std::lock_guard lock(Mutex_);
        Activated_ = true;
        return std::make_unique<TFakeScope>();
    }

    bool IsEnded() const {
        std::lock_guard lock(Mutex_);
        return Ended_;
    }

    bool IsActivated() const {
        std::lock_guard lock(Mutex_);
        return Activated_;
    }

    std::string GetStringAttribute(const std::string& key) const {
        std::lock_guard lock(Mutex_);
        auto it = StringAttributes_.find(key);
        return it != StringAttributes_.end() ? it->second : "";
    }

    bool HasStringAttribute(const std::string& key) const {
        std::lock_guard lock(Mutex_);
        return StringAttributes_.contains(key);
    }

    int64_t GetIntAttribute(const std::string& key) const {
        std::lock_guard lock(Mutex_);
        auto it = IntAttributes_.find(key);
        return it != IntAttributes_.end() ? it->second : 0;
    }

    bool HasIntAttribute(const std::string& key) const {
        std::lock_guard lock(Mutex_);
        return IntAttributes_.contains(key);
    }

    std::vector<TFakeEvent> GetEvents() const {
        std::lock_guard lock(Mutex_);
        return Events_;
    }

private:
    mutable std::mutex Mutex_;
    bool Ended_ = false;
    bool Activated_ = false;
    std::map<std::string, std::string> StringAttributes_;
    std::map<std::string, int64_t> IntAttributes_;
    std::vector<TFakeEvent> Events_;
};

class TFakeTracer : public NTrace::ITracer {
public:
    std::shared_ptr<NTrace::ISpan> StartSpan(
        const std::string& name,
        NTrace::ESpanKind kind
    ) override {
        return StartSpan(name, kind, /*parent=*/ nullptr);
    }

    std::shared_ptr<NTrace::ISpan> StartSpan(
        const std::string& name,
        NTrace::ESpanKind kind,
        NTrace::ISpan* parent
    ) override {
        auto span = std::make_shared<TFakeSpan>();
        std::lock_guard lock(Mutex_);
        Spans_.push_back({name, kind, span, parent});
        return span;
    }

    struct TSpanRecord {
        std::string Name;
        NTrace::ESpanKind Kind;
        std::shared_ptr<TFakeSpan> Span;
        NTrace::ISpan* Parent = nullptr;
    };

    std::vector<TSpanRecord> GetSpans() const {
        std::lock_guard lock(Mutex_);
        return Spans_;
    }

    std::shared_ptr<TFakeSpan> GetLastSpan() const {
        std::lock_guard lock(Mutex_);
        return Spans_.empty() ? nullptr : Spans_.back().Span;
    }

    TSpanRecord GetLastSpanRecord() const {
        std::lock_guard lock(Mutex_);
        return Spans_.back();
    }

    size_t SpanCount() const {
        std::lock_guard lock(Mutex_);
        return Spans_.size();
    }

private:
    mutable std::mutex Mutex_;
    std::vector<TSpanRecord> Spans_;
};

class TFakeTraceProvider : public NTrace::ITraceProvider {
public:
    std::shared_ptr<NTrace::ITracer> GetTracer(const std::string& name) override {
        std::lock_guard lock(Mutex_);
        auto it = Tracers_.find(name);
        if (it != Tracers_.end()) {
            return it->second;
        }
        auto tracer = std::make_shared<TFakeTracer>();
        Tracers_[name] = tracer;
        return tracer;
    }

    std::shared_ptr<TFakeTracer> GetFakeTracer(const std::string& name) const {
        std::lock_guard lock(Mutex_);
        auto it = Tracers_.find(name);
        return it != Tracers_.end() ? it->second : nullptr;
    }

private:
    mutable std::mutex Mutex_;
    std::map<std::string, std::shared_ptr<TFakeTracer>> Tracers_;
};

} // namespace NYdb::NTests
