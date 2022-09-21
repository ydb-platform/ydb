#pragma once

#include <ydb/library/aclib/aclib.h>

#include <ydb/core/protos/config.pb.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NSQS {

class IEventsWriterWrapper : public TAtomicRefCount<IEventsWriterWrapper> {
public:
    virtual void Write(const TString& data) = 0;
    virtual ~IEventsWriterWrapper() {
        Close();
    };

    void Close();

    using TPtr = TIntrusivePtr<IEventsWriterWrapper>;
protected:
    virtual void CloseImpl() = 0;
private:
    bool Closed = false;
};


class IEventsWriterFactory {
public:
    virtual IEventsWriterWrapper::TPtr CreateEventsWriter(const NKikimrConfig::TSqsConfig& config,  const ::NMonitoring::TDynamicCounterPtr& counters) const = 0;
    virtual ~IEventsWriterFactory()
    {}
};

} // namespace NKikimr::NSQS
