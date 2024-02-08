#pragma once

#include "sensor.h"

#include <util/generic/string.h>

#include <library/cpp/yt/memory/ref_counted.h>
#include <library/cpp/yt/memory/intrusive_ptr.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

struct ISensorWriter
{
    virtual ~ISensorWriter() = default;

    virtual void PushTag(TTag tag) = 0;
    virtual void PopTag() = 0;

    virtual void AddGauge(const TString& name, double value) = 0;

    //! AddCounter emits single counter value.
    /*!
     *  #value MUST be monotonically increasing.
     */
    virtual void AddCounter(const TString& name, i64 value) = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! RAII guard for ISensorWriter::PushTag/ISensorWriter::PopTag.
class TWithTagGuard
{
public:
    TWithTagGuard(const TWithTagGuard&) = delete;
    TWithTagGuard(TWithTagGuard&&) = delete;

    [[nodiscard]]
    explicit TWithTagGuard(ISensorWriter* writer);
    // NB: For convenience.
    [[nodiscard]]
    TWithTagGuard(ISensorWriter* writer, TString tagKey, TString tagValue);

    ~TWithTagGuard();

    void AddTag(TTag tag);
    void AddTag(TString tagKey, TString tagValue);

private:
    ISensorWriter* const Writer_;
    int AddedTagCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TSensorBuffer final
    : public ISensorWriter
{
public:
    void PushTag(TTag tag) override;
    void PopTag() override;

    void AddGauge(const TString& name, double value) override;
    void AddCounter(const TString& name, i64 value) override;

    void WriteTo(ISensorWriter* writer);

    const std::vector<std::tuple<TString, TTagList, i64>>& GetCounters() const;
    const std::vector<std::tuple<TString, TTagList, double>>& GetGauges() const;

private:
    TTagList Tags_;

    std::vector<std::tuple<TString, TTagList, i64>> Counters_;
    std::vector<std::tuple<TString, TTagList, double>> Gauges_;
};

////////////////////////////////////////////////////////////////////////////////

struct ISensorProducer
    : virtual public TRefCounted
{
    //! Collect returns set of gauges or counters associated with this producer.
    /*!
     *  Registry keeps track of all (name, tags) pair that were ever returned from
     *  this producer.
     *
     *  Do not use this interface, if set of tags might grow unbound. There is
     *  no way to cleanup removed tags.
     */
    virtual void CollectSensors(ISensorWriter* writer) = 0;
    virtual TIntrusivePtr<TSensorBuffer> GetBuffer();
};

DEFINE_REFCOUNTED_TYPE(ISensorProducer)

////////////////////////////////////////////////////////////////////////////////

class TBufferedProducer
    : public ISensorProducer
{
public:
    void CollectSensors(ISensorWriter* writer) override;
    TIntrusivePtr<TSensorBuffer> GetBuffer() override;

    void Update(TSensorBuffer buffer);
    void Update(const std::function<void(ISensorWriter*)>& callback);

    void SetEnabled(bool enabled);

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    bool Enabled_ = true;
    TIntrusivePtr<TSensorBuffer> Buffer_;
};

DEFINE_REFCOUNTED_TYPE(TBufferedProducer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
