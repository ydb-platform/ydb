#include "kqp_topic.h"
#include <ydb/core/base/path.h>

#define LOG_D(msg) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)

namespace NKikimr::NKqp::NTopic {

//
// TPartition
//
void TPartition::AddRange(const Ydb::Topic::OffsetsRange &range)
{
    AddRangeImpl(range.start(), range.end());
}

void TPartition::SetTabletId(ui64 value)
{
    TabletId_ = value;
}

void TPartition::Merge(const TPartition& rhs)
{
    for (auto& range : rhs.Offsets_) {
        AddRangeImpl(range.first, range.second);
    }
}

void TPartition::AddRangeImpl(ui64 begin, ui64 end)
{
    if (Offsets_.Intersects(begin, end)) {
        ythrow TOffsetsRangeIntersectExpection() << "offset ranges intersect";
    }

    Offsets_.InsertInterval(begin, end);
}

//
// TTopic
//
TPartition& TTopic::AddPartition(ui32 id)
{
    return Partitions_[id];
}

TPartition* TTopic::GetPartition(ui32 id)
{
    auto p = Partitions_.find(id);
    if (p == Partitions_.end()) {
        return nullptr;
    }
    return &p->second;
}

void TTopic::Merge(const TTopic& rhs)
{
    for (auto& [name, partition] : rhs.Partitions_) {
        Partitions_[name].Merge(partition);
    }
}

//
// TOffsetsInfo
//
TTopic& TOffsetsInfo::AddTopic(const TString &path)
{
    return Topics_[path];
}

TTopic *TOffsetsInfo::GetTopic(const TString &path)
{
    auto p = Topics_.find(path);
    if (p == Topics_.end()) {
        return nullptr;
    }
    return &p->second;
}

void TOffsetsInfo::FillSchemeCacheNavigate(NSchemeCache::TSchemeCacheNavigate& navigate) const
{
    for (auto& [topic, _] : Topics_) {
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = NKikimr::SplitPath(topic);
        entry.SyncVersion = true;
        entry.ShowPrivatePath = true;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;

        navigate.ResultSet.push_back(std::move(entry));
    }
}

bool TOffsetsInfo::ProcessSchemeCacheNavigate(const NSchemeCache::TSchemeCacheNavigate::TResultSet& results,
                                              Ydb::StatusIds_StatusCode& status,
                                              TString& message)
{
    Y_VERIFY(results.size() == Topics_.size());

    if (results.empty()) {
        status = Ydb::StatusIds::BAD_REQUEST;
        message = "empty request";
        return false;
    }

    TStringBuilder builder;

    for (auto& result : results) {
        if (result.Kind != NSchemeCache::TSchemeCacheNavigate::KindTopic) {
            builder << "Path '" << JoinPath(result.Path) << "' is not a topic";

            status = Ydb::StatusIds::SCHEME_ERROR;
            message = std::move(builder);

            return false;
        }

        if (result.PQGroupInfo) {
            const NKikimrSchemeOp::TPersQueueGroupDescription& description =
                result.PQGroupInfo->Description;

            TTopic* topic = GetTopic(CanonizePath(result.Path));

            //
            // TODO: если топика нет, то отправить сообщение с ошибкой SCHEME_ERROR
            //
            Y_VERIFY(topic != nullptr);

            for (auto& p : description.GetPartitions()) {
                if (auto partition = topic->GetPartition(p.GetPartitionId()); partition) {
                    LOG_D("topic=" << description.GetName()
                          << ", partition=" << p.GetPartitionId()
                          << ", tabletId=" << p.GetTabletId());

                    partition->SetTabletId(p.GetTabletId());
                }
            }
        } else {
            builder << "The '" << JoinPath(result.Path) << "' topic is missing";

            status = Ydb::StatusIds::SCHEME_ERROR;
            message = std::move(builder);

            return false;
        }
    }

    status = Ydb::StatusIds::SUCCESS;
    message = "";

    return true;
}

void TOffsetsInfo::Merge(const TOffsetsInfo& rhs)
{
    for (auto& [name, topic] : rhs.Topics_) {
        Topics_[name].Merge(topic);
    }
}

}
