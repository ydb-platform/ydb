#pragma once

#include <ydb/core/ymq/actor/index_events_processor.h>

namespace NKikimr::NSQS {

class TTestEventsWriter : public IEventsWriterWrapper {
public:
    TTestEventsWriter() = default;
    void Write(const TString& data) override;
    TVector<TString> GetMessages();

    void CloseImpl() override {
    }

private:
    TAdaptiveLock Lock;
    TVector<TString> Messages;
};
using TMockSessionPtr = TIntrusivePtr<TTestEventsWriter>;

} //namespace NKikimr::NSQS
