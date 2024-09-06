#pragma once
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/data_sharing/common/context/context.h>
#include <ydb/core/tx/columnshard/data_sharing/protos/events.pb.h>
#include <ydb/core/tx/columnshard/columnshard.h>

#include <ydb/library/actors/core/event_pb.h>

namespace NKikimr::NOlap {
class TVersionedIndex;
}

namespace NKikimr::NOlap::NDataSharing {
class TSharedBlobsManager;
class TTaskForTablet;
}

namespace NKikimr::NOlap::NDataSharing::NEvents {

class TPathIdData {
private:
    YDB_READONLY(ui64, PathId, 0);
    YDB_ACCESSOR_DEF(std::vector<TPortionInfo>, Portions);

    TPathIdData() = default;

    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TPathIdData& proto, const TIndexInfo& indexInfo) {
        if (!proto.HasPathId()) {
            return TConclusionStatus::Fail("no path id in proto");
        }
        PathId = proto.GetPathId();
        for (auto&& portionProto : proto.GetPortions()) {
            TConclusion<TPortionInfo> portion = TPortionInfo::BuildFromProto(portionProto, indexInfo);
            if (!portion) {
                return portion.GetError();
            }
            Portions.emplace_back(portion.DetachResult());
        }
        return TConclusionStatus::Success();
    }
public:
    TPathIdData(const ui64 pathId, const std::vector<TPortionInfo>& portions)
        : PathId(pathId)
        , Portions(portions)
    {

    }

    std::vector<TPortionInfo> DetachPortions() {
        return std::move(Portions);
    }
    THashMap<TTabletId, TTaskForTablet> BuildLinkTabletTasks(const std::shared_ptr<IStoragesManager>& storages, const TTabletId selfTabletId,
        const TTransferContext& context, const TVersionedIndex& index);

    void InitPortionIds(ui64* lastPortionId, const std::optional<ui64> pathId = {}) {
        AFL_VERIFY(lastPortionId);
        for (auto&& i : Portions) {
            i.SetPortion(++*lastPortionId);
            if (pathId) {
                i.SetPathId(*pathId);
            }
        }
    }

    void SerializeToProto(NKikimrColumnShardDataSharingProto::TPathIdData& proto) const {
        proto.SetPathId(PathId);
        for (auto&& i : Portions) {
            i.SerializeToProto(*proto.AddPortions());
        }
    };


    static TConclusion<TPathIdData> BuildFromProto(const NKikimrColumnShardDataSharingProto::TPathIdData& proto, const TIndexInfo& indexInfo) {
        TPathIdData result;
        auto resultParsing = result.DeserializeFromProto(proto, indexInfo);
        if (!resultParsing) {
            return resultParsing;
        } else {
            return result;
        }
    }

};

struct TEvSendDataFromSource: public NActors::TEventPB<TEvSendDataFromSource, NKikimrColumnShardDataSharingProto::TEvSendDataFromSource, TEvColumnShard::EvDataSharingSendDataFromSource> {
    TEvSendDataFromSource() = default;

    TEvSendDataFromSource(const TString& sessionId, const ui32 packIdx, const TTabletId sourceTabletId, const THashMap<ui64, TPathIdData>& pathIdData) {
        Record.SetSessionId(sessionId);
        Record.SetPackIdx(packIdx);
        Record.SetSourceTabletId((ui64)sourceTabletId);
        for (auto&& i : pathIdData) {
            i.second.SerializeToProto(*Record.AddPathIdData());
        }
    }
};

struct TEvFinishedFromSource: public NActors::TEventPB<TEvFinishedFromSource, NKikimrColumnShardDataSharingProto::TEvFinishedFromSource, TEvColumnShard::EvDataSharingFinishedFromSource> {
    TEvFinishedFromSource() = default;

    TEvFinishedFromSource(const TString& sessionId, const TTabletId sourceTabletId) {
        Record.SetSessionId(sessionId);
        Record.SetSourceTabletId((ui64)sourceTabletId);
    }
};

}