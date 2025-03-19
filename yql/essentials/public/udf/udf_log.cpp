#include "udf_log.h"
#include <util/system/mutex.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>

namespace NYql {
namespace NUdf {

namespace {

class TNullLogger : public ILogger {
public:
    TLogComponentId RegisterComponent(const TStringRef& component) final {
        Y_UNUSED(component);
        return 0;
    }

    void SetDefaultLevel(ELogLevel level) final {
        Y_UNUSED(level);
    }

    void SetComponentLevel(TLogComponentId component, ELogLevel level) final {
        Y_UNUSED(component);
        Y_UNUSED(level);
    }

    bool IsActive(TLogComponentId component, ELogLevel level) const final {
        Y_UNUSED(component);
        Y_UNUSED(level);
        return false;
    }

    void Log(TLogComponentId component, ELogLevel level, const TStringRef& message) final {
        Y_UNUSED(component);
        Y_UNUSED(level);
        Y_UNUSED(message);
    }
};

class TSynchronizedLogger : public ILogger {
public:
    TSynchronizedLogger(const TLoggerPtr& inner)
        : Inner_(inner)
    {}

    TLogComponentId RegisterComponent(const TStringRef& component) final {
        with_lock(Mutex_) {
            return Inner_->RegisterComponent(component);
        }
    }

    void SetDefaultLevel(ELogLevel level) final {
        with_lock(Mutex_) {
            Inner_->SetDefaultLevel(level);
        }
    }

    void SetComponentLevel(TLogComponentId component, ELogLevel level) final {
        with_lock(Mutex_) {
            Inner_->SetComponentLevel(component, level);
        }
    }

    bool IsActive(TLogComponentId component, ELogLevel level) const final {
        with_lock(Mutex_) {
            return Inner_->IsActive(component, level);
        }
    }

    void Log(TLogComponentId component, ELogLevel level, const TStringRef& message) final {
        with_lock(Mutex_) {
            Inner_->Log(component, level, message);
        }
    }

private:
    TLoggerPtr Inner_;
    TMutex Mutex_;
};

class TLogger: public ILogger {
public:
    TLogger(TLogProviderFunc func, TMaybe<ELogLevel> filter)
        : Func_(func)
        , Filter_(filter)
    {
    }

    TLogComponentId RegisterComponent(const TStringRef& component) final {
        auto [it, inserted] = Components_.emplace(TString(component), 1 + Components_.size());
        if (inserted) {
            Names_[it->second] = it->first;
        }

        return it->second;
    }

    void SetDefaultLevel(ELogLevel level) final {
        DefLevel_ = level;
    }

    void SetComponentLevel(TLogComponentId component, ELogLevel level) final {
        CompLevels_[component] = level;
    }

    bool IsActive(TLogComponentId component, ELogLevel level) const final {
        if (Filter_ && !IsLogLevelAllowed(level, *Filter_)) {
            return false;
        }

        if (!Names_.contains(component)) {
            return false;
        }

        if (auto it = CompLevels_.find(component); it != CompLevels_.end()) {
            return IsLogLevelAllowed(level, it->second);
        }

        if (DefLevel_) {
            return IsLogLevelAllowed(level, *DefLevel_);
        }

        return true;
    }

    void Log(TLogComponentId component, ELogLevel level, const TStringRef& message) final {
        if (IsActive(component, level)) {
            Func_(Names_[component], level, message);
        }
    }

private:
    const TLogProviderFunc Func_;
    const TMaybe<ELogLevel> Filter_;
    THashMap<TString, TLogComponentId> Components_;
    THashMap<TLogComponentId, TString> Names_;
    TMaybe<ELogLevel> DefLevel_;
    THashMap<TLogComponentId, ELogLevel> CompLevels_;
};

class TLogProvider : public ILogProvider {
public:
    TLogProvider(TLogProviderFunc func, TMaybe<ELogLevel> filter)
        : Func_(func)
        , Filter_(filter)
    {}

    TLoggerPtr MakeLogger() const final {
        return new TLogger(Func_, Filter_);
    }

private:
    const TLogProviderFunc Func_;
    const TMaybe<ELogLevel> Filter_;
};

}

TLoggerPtr MakeNullLogger() {
    return new TNullLogger();
}

TLoggerPtr MakeSynchronizedLogger(const TLoggerPtr& inner) {
    return new TSynchronizedLogger(std::move(inner));
}

#define SWITCH_ENUM_TYPE_TO_STR(name, val) \
    case val: return TStringBuf(#name);

TStringBuf LevelToString(ELogLevel level) {
    switch (static_cast<ui32>(level)) {
        UDF_LOG_LEVEL(SWITCH_ENUM_TYPE_TO_STR)
    }

    return TStringBuf("unknown");
}

#define PARSE_ENUM_TYPE_FROM_STR(name, val) \
    if (#name == str) return static_cast<ELogLevel>(val);

TMaybe<ELogLevel> TryLevelFromString(TStringBuf str) {
    UDF_LOG_LEVEL(PARSE_ENUM_TYPE_FROM_STR)
    return Nothing();
}

TUniquePtr<ILogProvider> MakeLogProvider(TLogProviderFunc func, TMaybe<ELogLevel> filter) {
    return new TLogProvider(func, filter);
}

} // namspace NUdf
} // namspace NYql
