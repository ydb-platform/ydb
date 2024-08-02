#include "statistics.h"

#include <yt/yt/core/ypath/token.h>
#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/serialize.h>

namespace NYT {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TSummary::TSummary()
{
    Reset();
}

TSummary::TSummary(i64 sum, i64 count, i64 min, i64 max, std::optional<i64> last)
    : Sum_(sum)
    , Count_(count)
    , Min_(min)
    , Max_(max)
    , Last_(last)
{ }

void TSummary::AddSample(i64 sample)
{
    Sum_ += sample;
    Count_ += 1;
    Min_ = std::min(Min_, sample);
    Max_ = std::max(Max_, sample);
    Last_ = sample;
}

void TSummary::Merge(const TSummary& summary)
{
    Sum_ += summary.GetSum();
    Count_ += summary.GetCount();
    Min_ = std::min(Min_, summary.GetMin());
    Max_ = std::max(Max_, summary.GetMax());
    // This method used to aggregate summaries of different objects. Last is intentionally dropped.
    Last_ = std::nullopt;
}

void TSummary::Reset()
{
    Sum_ = 0;
    Count_ = 0;
    Min_ = std::numeric_limits<i64>::max();
    Max_ = std::numeric_limits<i64>::min();
    Last_ = std::nullopt;
}

void TSummary::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Sum_);
    Persist(context, Count_);
    Persist(context, Min_);
    Persist(context, Max_);
    Persist(context, Last_);
}

void Serialize(const TSummary& summary, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("sum").Value(summary.GetSum())
            .Item("count").Value(summary.GetCount())
            .Item("min").Value(summary.GetMin())
            .Item("max").Value(summary.GetMax())
            .OptionalItem("last", summary.GetLast())
        .EndMap();
}

bool TSummary::operator ==(const TSummary& other) const
{
    return
        Sum_ == other.Sum_ &&
        Count_ == other.Count_ &&
        Min_ == other.Min_ &&
        Max_ == other.Max_ &&
        Last_ == other.Last_;
}

////////////////////////////////////////////////////////////////////////////////

TSummary& TStatistics::GetSummary(const NYPath::TYPath& path)
{
    auto [iterator, _] = CheckedEmplaceStatistic(Data_, path, TSummary());
    return iterator->second;
}

void TStatistics::AddSample(const NYPath::TYPath& path, i64 sample)
{
    GetSummary(path).AddSample(sample);
}

void TStatistics::AddSample(const NYPath::TYPath& path, const INodePtr& sample)
{
    ProcessNodeWithCallback(path, sample, [this] (const auto&... args) {
        AddSample(args...);
    });
}

void TStatistics::ReplacePathWithSample(const NYPath::TYPath& path, const i64 sample)
{
    auto& summary = GetSummary(path);
    summary.Reset();
    summary.AddSample(sample);
}

void TStatistics::ReplacePathWithSample(const NYPath::TYPath& path, const INodePtr& sample)
{
    ProcessNodeWithCallback(path, sample, [this] (const auto&... args) {
        ReplacePathWithSample(args...);
    });
}

template <class TCallback>
void TStatistics::ProcessNodeWithCallback(const NYPath::TYPath& path, const NYTree::INodePtr& sample, TCallback callback)
{
    switch (sample->GetType()) {
        case ENodeType::Int64:
            callback(path, sample->AsInt64()->GetValue());
            break;

        case ENodeType::Uint64:
            callback(path, static_cast<i64>(sample->AsUint64()->GetValue()));
            break;

        case ENodeType::Map:
            for (const auto& [key, child] : sample->AsMap()->GetChildren()) {
                if (key.empty()) {
                    THROW_ERROR_EXCEPTION("Statistic cannot have an empty name")
                        << TErrorAttribute("path_prefix", path);
                }
                callback(path + "/" + ToYPathLiteral(key), child);
            }
            break;

        default:
            THROW_ERROR_EXCEPTION(
                "Invalid statistics type: expected map or integral type but found sample of type %Qlv",
                sample->GetType())
                << TErrorAttribute("sample", sample);
    }
}

void TStatistics::Merge(const TStatistics& statistics)
{
    for (const auto& [path, summary] : statistics.Data()) {
        GetSummary(path).Merge(summary);
    }
}

void TStatistics::MergeWithOverride(const TStatistics& statistics)
{
    const auto& otherData = statistics.Data();
    Data_.insert(otherData.begin(), otherData.end());
}

TStatistics::TSummaryRange TStatistics::GetRangeByPrefix(const TString& prefix) const
{
    auto begin = Data().lower_bound(prefix + '/');
    // This will effectively return an iterator to the first path not starting with "`prefix`/".
    auto end = Data().lower_bound(prefix + ('/' + 1));
    return TSummaryRange(begin, end);
}

void TStatistics::RemoveRangeByPrefix(const TString& prefixPath)
{
    auto begin = Data().lower_bound(prefixPath + '/');
    // This will effectively return an iterator to the first path not starting with "`prefix`/".
    auto end = Data().lower_bound(prefixPath + ('/' + 1));
    Data_.erase(begin, end);
}

void TStatistics::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Data_);
}

void Serialize(const TStatistics& statistics, IYsonConsumer* consumer)
{
    using NYT::Serialize;

    if (statistics.GetTimestamp()) {
        consumer->OnBeginAttributes();
        consumer->OnKeyedItem("timestamp");
        NYTree::Serialize(*statistics.GetTimestamp(), consumer);
        consumer->OnEndAttributes();
    }

    SerializeYsonPathsMap<TSummary>(
        statistics.Data(),
        consumer,
        [] (const TSummary& summary, IYsonConsumer* consumer) {
            Serialize(summary, consumer);
        });
}

// Helper function for GetNumericValue.
i64 GetSum(const TSummary& summary)
{
    return summary.GetSum();
}

i64 GetNumericValue(const TStatistics& statistics, const TString& path)
{
    auto value = FindNumericValue(statistics, path);
    if (!value) {
        THROW_ERROR_EXCEPTION("Statistics %v is not present",
            path);
    } else {
        return *value;
    }
}

std::optional<i64> FindNumericValue(const TStatistics& statistics, const TString& path)
{
    auto summary = FindSummary(statistics, path);
    return summary ? std::make_optional(summary->GetSum()) : std::nullopt;
}

std::optional<TSummary> FindSummary(const TStatistics& statistics, const TString& path)
{
    const auto& data = statistics.Data();
    auto iterator = data.lower_bound(path);
    if (iterator != data.end() && iterator->first != path && HasPrefix(iterator->first, path)) {
        THROW_ERROR_EXCEPTION("Invalid statistics type: cannot get summary of %v since it is a map",
            path);
    } else if (iterator == data.end() || iterator->first != path) {
        return std::nullopt;
    } else {
        return iterator->second;
    }
}


////////////////////////////////////////////////////////////////////////////////

class TStatisticsBuildingConsumer
    : public TYsonConsumerBase
    , public IBuildingYsonConsumer<TStatistics>
{
public:
    void OnStringScalar(TStringBuf value) override
    {
        if (!AtAttributes_) {
            THROW_ERROR_EXCEPTION("String scalars are not allowed for statistics");
        }
        Statistics_.SetTimestamp(ConvertTo<TInstant>(value));
    }

    void OnInt64Scalar(i64 value) override
    {
        if (AtAttributes_) {
            THROW_ERROR_EXCEPTION("Timestamp should have string type");
        }
        bool isFieldKnown = true;
        AtSummaryMap_ = true;
        if (LastKey_ == "sum") {
            CurrentSummary_.Sum_ = value;
        } else if (LastKey_ == "count") {
            CurrentSummary_.Count_ = value;
        } else if (LastKey_ == "min") {
            CurrentSummary_.Min_ = value;
        } else if (LastKey_ == "max") {
            CurrentSummary_.Max_ = value;
        } else if (LastKey_ == "last") {
            CurrentSummary_.Last_ = value;
            LastFound_ = true;
        } else {
            isFieldKnown = false;
        }

        if (isFieldKnown) {
            ++FilledSummaryFields_;
        }
    }

    void OnUint64Scalar(ui64 /*value*/) override
    {
        THROW_ERROR_EXCEPTION("Uint64 scalars are not allowed for statistics");
    }

    void OnDoubleScalar(double /*value*/) override
    {
        THROW_ERROR_EXCEPTION("Double scalars are not allowed for statistics");
    }

    void OnBooleanScalar(bool /*value*/) override
    {
        THROW_ERROR_EXCEPTION("Boolean scalars are not allowed for statistics");
    }

    void OnEntity() override
    {
        THROW_ERROR_EXCEPTION("Entities are not allowed for statistics");
    }

    void OnBeginList() override
    {
        THROW_ERROR_EXCEPTION("Lists are not allowed for statistics");
    }

    void OnListItem() override
    {
        THROW_ERROR_EXCEPTION("Lists are not allowed for statistics");
    }

    void OnEndList() override
    {
        THROW_ERROR_EXCEPTION("Lists are not allowed for statistics");
    }

    void OnBeginMap() override
    {
        // If we are here, we are either:
        // * at the root (then do nothing)
        // * at some directory (then the last key was the directory name)
        if (!LastKey_.empty()) {
            DirectoryNameLengths_.push_back(LastKey_.size());
            CurrentPath_.append('/');
            CurrentPath_.append(LastKey_);
            LastKey_.clear();
        } else {
            if (!CurrentPath_.empty()) {
                THROW_ERROR_EXCEPTION("Empty keys are not allowed for statistics");
            }
        }
    }

    void OnKeyedItem(TStringBuf key) override
    {
        if (AtAttributes_) {
            if (key != "timestamp") {
                THROW_ERROR_EXCEPTION("Attributes other than \"timestamp\" are not allowed");
            }
        } else {
            LastKey_ = ToYPathLiteral(key);
        }
    }

    void OnEndMap() override
    {
        if (AtSummaryMap_) {
            int requiredFilledSummaryFields = FilledSummaryFields_ - (LastFound_ ? 1 : 0);
            if (requiredFilledSummaryFields != 4) {
                THROW_ERROR_EXCEPTION("All 4 required summary fields must be filled for statistics, but found %v",
                    requiredFilledSummaryFields);
            }
            if (!LastFound_) {
                CurrentSummary_.Last_ = std::nullopt;
            }
            Statistics_.Data_[CurrentPath_] = CurrentSummary_;
            FilledSummaryFields_ = 0;
            LastFound_ = false;
            AtSummaryMap_ = false;
        }

        if (!CurrentPath_.empty()) {
            // We need to go to the parent.
            CurrentPath_.resize(CurrentPath_.size() - DirectoryNameLengths_.back() - 1);
            DirectoryNameLengths_.pop_back();
        }
    }

    void OnBeginAttributes() override
    {
        if (!CurrentPath_.empty()) {
            THROW_ERROR_EXCEPTION("Attributes are not allowed for statistics");
        }
        AtAttributes_ = true;
    }

    void OnEndAttributes() override
    {
        AtAttributes_ = false;
    }

    TStatistics Finish() override
    {
        return Statistics_;
    }

private:
    TStatistics Statistics_;

    TString CurrentPath_;
    std::vector<int> DirectoryNameLengths_;

    TSummary CurrentSummary_;
    i64 FilledSummaryFields_ = 0;
    bool LastFound_ = false;

    TString LastKey_;

    bool AtSummaryMap_ = false;
    bool AtAttributes_ = false;
};

void CreateBuildingYsonConsumer(std::unique_ptr<IBuildingYsonConsumer<TStatistics>>* buildingConsumer, EYsonType ysonType)
{
    YT_VERIFY(ysonType == EYsonType::Node);
    *buildingConsumer = std::make_unique<TStatisticsBuildingConsumer>();
}

////////////////////////////////////////////////////////////////////////////////

TStatisticsConsumer::TStatisticsConsumer(TSampleHandler sampleHandler)
    : TreeBuilder_(CreateBuilderFromFactory(GetEphemeralNodeFactory()))
    , SampleHandler_(sampleHandler)
{ }

void TStatisticsConsumer::OnMyListItem()
{
    TreeBuilder_->BeginTree();
    Forward(
        TreeBuilder_.get(),
        [this] {
            auto node = TreeBuilder_->EndTree();
            SampleHandler_.Run(node);
        },
        EYsonType::Node);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
