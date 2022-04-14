#pragma once
#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NPersQueue {

class ILogger : public TAtomicRefCount<ILogger> {
public:
    virtual ~ILogger() = default;
    //level = syslog level
    virtual void Log(const TString& msg, const TString& sourceId, const TString& sessionId, int level) = 0;
    virtual bool IsEnabled(int level) const = 0;
};

class TCerrLogger : public ILogger {
public:
    explicit TCerrLogger(int level)
        : Level(level)
    {
    }

    ~TCerrLogger() override = default;

    void Log(const TString& msg, const TString& sourceId, const TString& sessionId, int level) override;

    bool IsEnabled(int level) const override
    {
        return level <= Level;
    }

    int GetLevel() const {
        return Level;
    }

private:
    static TStringBuf LevelToString(int level);

private:
    int Level;
};

}
