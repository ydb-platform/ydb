#include "stream_log_manager.h"

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/yt/logging/plain_text_formatter/formatter.h>

#include <library/cpp/yt/string/raw_formatter.h>

#include <library/cpp/yt/threading/fork_aware_spin_lock.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

class TStreamLogManager
    : public ILogManager
{
public:
    explicit TStreamLogManager(IOutputStream* output)
        : Output_(output)
    { }

    void RegisterStaticAnchor(
        TLoggingAnchor* anchor,
        ::TSourceLocation /*sourceLocation*/,
        TStringBuf /*anchorMessage*/) override
    {
        anchor->Registered = true;
    }

    virtual void UpdateAnchor(TLoggingAnchor* anchor) override
    {
        anchor->Enabled = true;
    }

    virtual void Enqueue(TLogEvent&& event) override
    {
        Buffer_.Reset();
        EventFormatter_.Format(&Buffer_, event);
        *Output_ << Buffer_.GetBuffer() << Endl;
    }

    virtual const TLoggingCategory* GetCategory(TStringBuf categoryName) override
    {
        if (!categoryName) {
            return nullptr;
        }

        auto guard = Guard(SpinLock_);
        auto it = NameToCategory_.find(categoryName);
        if (it == NameToCategory_.end()) {
            auto category = std::make_unique<TLoggingCategory>();
            category->Name = categoryName;
            category->ActualVersion = &Version_;
            category->CurrentVersion = Version_.load();
            it = NameToCategory_.emplace(categoryName, std::move(category)).first;
        }
        return it->second.get();
    }

    virtual void UpdateCategory(TLoggingCategory* /*category*/) override
    { }

    virtual bool GetAbortOnAlert() const override
    {
        return false;
    }

private:
    IOutputStream* const Output_;

    NThreading::TForkAwareSpinLock SpinLock_;
    THashMap<TString, std::unique_ptr<TLoggingCategory>> NameToCategory_;
    std::atomic<int> Version_ = 1;

    TPlainTextEventFormatter EventFormatter_{/*enableSourceLocation*/ false};
    TRawFormatter<MessageBufferSize> Buffer_;
};

std::unique_ptr<ILogManager> CreateStreamLogManager(IOutputStream* output)
{
    return std::make_unique<TStreamLogManager>(output);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
