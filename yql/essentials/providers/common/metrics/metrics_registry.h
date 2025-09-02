#pragma once

#include "sensors_group.h"

#include <util/generic/yexception.h>

namespace NYql {

namespace NProto {
class TMetricsRegistrySnapshot;
}

struct IMetricsRegistry;
using IMetricsRegistryPtr = TIntrusivePtr<IMetricsRegistry>;

using TMetricsDecorator = std::function<IMetricsRegistryPtr(
        const IMetricsRegistryPtr& old, const TString& username)>;

//////////////////////////////////////////////////////////////////////////////
// IYqlMetricsRegistry
//////////////////////////////////////////////////////////////////////////////
struct IMetricsRegistry: public TThrRefBase {

    virtual void SetCounter(
        const TString& labelName,
        const TString& labelValue,
        i64 value,
        bool derivative = false) = 0;

    virtual void IncCounter(
            const TString& labelName,
            const TString& labelValue,
            bool derivative = true) = 0;

    virtual void AddCounter(
            const TString& labelName,
            const TString& labelValue,
            i64 value,
            bool derivative = true) = 0;

    // will invalidate all counters
    virtual bool TakeSnapshot(
            NProto::TMetricsRegistrySnapshot* snapshot) const = 0;

    virtual void MergeSnapshot(
            const NProto::TMetricsRegistrySnapshot& snapshot) = 0;

    virtual IMetricsRegistryPtr Personalized(
            const TString& userName) const = 0;

    virtual void Flush() = 0;

    virtual TSensorsGroupPtr GetSensors() {
        ythrow yexception() << "Not implemented";
    }
};


IMetricsRegistryPtr CreateMetricsRegistry(TSensorsGroupPtr sensors);

} // namespace NYql
