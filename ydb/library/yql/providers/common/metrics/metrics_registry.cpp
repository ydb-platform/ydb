#include "metrics_registry.h"

#include <ydb/library/yql/providers/common/metrics/protos/metrics_registry.pb.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/stack.h>


namespace NYql {
namespace {

//////////////////////////////////////////////////////////////////////////////
// TCountersPhotographer
//////////////////////////////////////////////////////////////////////////////
class TCountersPhotographer: public NMonitoring::ICountableConsumer {
public:
    TCountersPhotographer(NProto::TCounterGroup* groupProto, bool invalidate)
        : HasAnyCounters_(false)
        , Invalidate_(invalidate)
    {
        GroupsProto_.push(groupProto);
    }

    bool HasAnyCounters() const {
        return HasAnyCounters_;
    }

private:
    void OnCounter(
            const TString& labelName, const TString& labelValue,
            const TSensorCounter* counter) override
    {
        HasAnyCounters_ = true;

        auto* counterProto = GroupsProto_.top()->AddCounters();
        counterProto->SetDerivative(counter->ForDerivative());
        auto counterVal = counter->Val();
        counterProto->SetValue(counterVal);
        if (Invalidate_) {
            const_cast<TSensorCounter*>(counter)->Sub(counterVal);
        }

        auto* label = counterProto->MutableLabel();
        label->SetName(labelName);
        label->SetValue(labelValue);
    }

    void OnHistogram(const TString& labelName, const TString& labelValue, NMonitoring::IHistogramSnapshotPtr snapshot, bool) override {
        if (Invalidate_) {
            return;
        }
        auto* counterProto = GroupsProto_.top()->AddCounters();
        auto* label = counterProto->MutableLabel();
        label->SetName(labelName);
        label->SetValue(labelValue);

        auto bucketsCount = snapshot->Count();
        for (ui32 i = 0; i < bucketsCount; i += 1) {
            auto upperBound = snapshot->UpperBound(i);
            auto value = snapshot->Value(i);

            auto* bucket = counterProto->AddBucket();
            bucket->SetUpperBound(upperBound);
            bucket->SetValue(value);
        }
    }

    void OnGroupBegin(
            const TString& labelName, const TString& labelValue,
            const TSensorsGroup*) override
    {
        if (labelName.empty() && labelValue.empty()) {
            // root group is alrady present
            return;
        }

        auto* groupProto = GroupsProto_.top()->AddGroups();
        auto* label = groupProto->MutableLabel();
        label->SetName(labelName);
        label->SetValue(labelValue);

        GroupsProto_.push(groupProto);
    }

    void OnGroupEnd(const TString&, const TString&, const TSensorsGroup*) override {
        GroupsProto_.pop();
    }

private:
    TStack<NProto::TCounterGroup*> GroupsProto_;
    bool HasAnyCounters_;
    bool Invalidate_;
};

//////////////////////////////////////////////////////////////////////////////
// TMetricsRegistryImpl
//////////////////////////////////////////////////////////////////////////////
class TMetricsRegistryImpl final: public IMetricsRegistry {
public:
    TMetricsRegistryImpl(
            TSensorsGroupPtr sensors,
            TMaybe<TString> userName)
        : Sensors_(std::move(sensors))
        , UserName_(std::move(userName))
    {
    }

    void SetCounter(
        const TString& labelName,
        const TString& labelValue,
        i64 value,
        bool derivative) override
    {
        if (UserName_) {
            // per user counter
            auto userCnt = GetCounter(labelName, labelValue, UserName_.Get(),
                derivative);
            if (userCnt) {
                *userCnt = value;
            }

            return;
        }

        auto totalCnt = GetCounter(labelName, labelValue, nullptr, derivative);
        if (totalCnt) {
            *totalCnt = value;
        }
    }

    void IncCounter(
            const TString& labelName,
            const TString& labelValue,
            bool derivative) override
    {
        // total aggregate counter
        auto totalCnt = GetCounter(labelName, labelValue, nullptr, derivative);
        if (totalCnt) {
            totalCnt->Inc();
        }

        if (UserName_) {
            // per user counter
            auto userCnt = GetCounter(labelName, labelValue, UserName_.Get(),
                                      derivative);
            if (userCnt) {
                userCnt->Inc();
            }
        }
    }

    void AddCounter(
            const TString& labelName,
            const TString& labelValue,
            i64 value,
            bool derivative) override
    {
        // total aggregate counter
        auto totalCnt = GetCounter(labelName, labelValue, nullptr, derivative);
        if (totalCnt) {
            totalCnt->Add(value);
        }

        if (UserName_) {
            // per user counter
            auto userCnt = GetCounter(labelName, labelValue, UserName_.Get(),
                                      derivative);
            if (userCnt) {
                userCnt->Add(value);
            }
        }
    }

    bool TakeSnapshot(NProto::TMetricsRegistrySnapshot* snapshot) const override {
        bool hasRootGroupBefore = snapshot->HasRootGroup();
        TCountersPhotographer photographer(snapshot->MutableRootGroup(), snapshot->GetDontIncrement() == false);
        Sensors_->Accept(TString(), TString(), photographer);
        if (!photographer.HasAnyCounters() && !hasRootGroupBefore) {
            // remove prematurely allocated group
            snapshot->ClearRootGroup();
            return false;
        }
        return true;
    }

    void MergeSnapshot(const NProto::TMetricsRegistrySnapshot& snapshot) override {
        MergeFromGroupProto(
            snapshot.HasMergeToRoot()
                ? GetSensorsRootGroup().Get()
                : Sensors_.Get(),
            snapshot.GetRootGroup(),
            snapshot.HasDontIncrement()
                ? snapshot.GetDontIncrement()
                : false);
    }

    IMetricsRegistryPtr Personalized(const TString& userName) const override {
        return new TMetricsRegistryImpl(Sensors_, MakeMaybe(userName));
    }

    void Flush() override {
        // do nothing
    }

    TSensorsGroupPtr GetSensors() override {
        return Sensors_.Get();
    }

private:
    TSensorCounterPtr GetCounter(
            const TString& labelName,
            const TString& labelValue,
            const TString* userName,
            bool derivative)
    {
        static const TString USER("user");
        static const TString USER_ABSOLUTE("user_absolute");
        static const TString TOTAL("total");

        const TString& userGroup = derivative ? USER : USER_ABSOLUTE;
        return Sensors_
                ->GetSubgroup(userGroup, userName ? *userName : TOTAL)
                ->GetNamedCounter(labelName, labelValue, derivative);
    }

    void MergeFromGroupProto(
            TSensorsGroup* group, const NProto::TCounterGroup& groupProto, bool asIs)
    {
        for (const auto& counterProto: groupProto.GetCounters()) {
            const auto& label = counterProto.GetLabel();

            if (!counterProto.GetBucket().empty()) {
                NMonitoring::TBucketBounds bounds;
                auto histSnapshot = NMonitoring::TExplicitHistogramSnapshot::New(counterProto.GetBucket().size());
                bounds.reserve(counterProto.GetBucket().size());
                int i = 0;
                for (const auto& b : counterProto.GetBucket()) {
                    if (i < counterProto.GetBucket().size() - 1) {
                        // skip inf
                        bounds.push_back(b.GetUpperBound());
                    }
                    (*histSnapshot)[i].first = b.GetUpperBound();
                    (*histSnapshot)[i].second = b.GetValue();
                    i += 1;
                }

                auto collector = NMonitoring::ExplicitHistogram(bounds).Release();
                auto histogram = group->GetNamedHistogram(
                    label.GetName(), label.GetValue(),
                    THolder(collector));
                Histograms.insert(std::make_pair(histogram, collector));
                Histograms[histogram]->Collect(*histSnapshot);
            } else {
                auto counter = group->GetNamedCounter(
                            label.GetName(), label.GetValue(),
                            counterProto.GetDerivative());
                if (asIs) {
                    *counter = counterProto.GetValue();
                } else {
                    *counter += counterProto.GetValue();
                }
            }
        }

        for (const auto& subGroupProto: groupProto.GetGroups()) {
            const auto& label = subGroupProto.GetLabel();
            auto subGroup = group->GetSubgroup(
                        label.GetName(), label.GetValue());
            MergeFromGroupProto(subGroup.Get(), subGroupProto, asIs);
        }
    }

private:
    TSensorsGroupPtr Sensors_;
    const TMaybe<TString> UserName_;

    THashMap<NMonitoring::THistogramPtr, NMonitoring::IHistogramCollector*> Histograms;
};

} // namespace


IMetricsRegistryPtr CreateMetricsRegistry(TSensorsGroupPtr sensors) {
    return new TMetricsRegistryImpl(std::move(sensors), Nothing());
}

} // namespace NYql
