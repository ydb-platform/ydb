#pragma once

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <ydb/library/yql/ast/yql_expr.h>

#include <ydb/library/actors/core/actorid.h>

#include <variant>

namespace NYql::NDq {

struct TStageId {
    ui64 TxId = 0;
    ui64 StageId = 0;

    TStageId() = default;
    TStageId(ui64 txId, ui64 stageId)
        : TxId(txId)
        , StageId(stageId) {}

    bool operator==(const TStageId& other) const {
        return TxId == other.TxId && StageId == other.StageId;
    }

    bool operator!=(const TStageId& other) const {
        return !(*this == other);
    }

    bool operator<(const TStageId&) = delete;
    bool operator>(const TStageId&) = delete;
    bool operator<=(const TStageId&) = delete;
    bool operator>=(const TStageId&) = delete;

    ui64 Hash() const noexcept {
        auto tuple = std::make_tuple(TxId, StageId);
        return THash<decltype(tuple)>()(tuple);
    }
};

template <class TStageInfoMeta>
struct TStageInfo : private TMoveOnly {
    TStageInfo(ui64 txId, const NNodes::TDqPhyStage& stage, TStageInfoMeta&& meta)
        : Id(txId, stage.Ref().UniqueId())
        , InputsCount(stage.Inputs().Size())
        , Meta(std::move(meta))
    {
        auto stageResultTuple = stage.Ref().GetTypeAnn()->Cast<TTupleExprType>();
        OutputsCount = stageResultTuple->GetSize();
    }

    TStageInfo(const NNodes::TDqPhyStage& stage, TStageInfoMeta&& meta)
        : TStageInfo(0, stage, std::move(meta)) {}

    TStageInfo(const TStageId& id, ui32 inputsCount, ui32 outputsCount, TStageInfoMeta&& meta)
        : Id(id)
        , InputsCount(inputsCount)
        , OutputsCount(outputsCount)
        , Meta(std::move(meta)) {}

    TStageId Id;

    ui32 InputsCount = 0;
    ui32 OutputsCount = 0;

    TVector<ui64> Tasks;
    TStageInfoMeta Meta;

    TString DebugString() const {
        // TODO: Print stage details, including input types and program.
        TStringBuilder result;
        result << "StageInfo: StageId #" << Id
            << ", InputsCount: " << InputsCount
            << ", OutputsCount: " << OutputsCount;
        return result;
    }

};

struct TChannel {
    ui64 Id = 0;
    TStageId SrcStageId;
    ui64 SrcTask = 0;
    ui32 SrcOutputIndex = 0;
    TStageId DstStageId;
    ui64 DstTask = 0;
    ui32 DstInputIndex = 0;
    bool InMemory = true;
    NDqProto::ECheckpointingMode CheckpointingMode = NDqProto::CHECKPOINTING_MODE_DEFAULT;
    NDqProto::EWatermarksMode WatermarksMode = NDqProto::WATERMARKS_MODE_DISABLED;

    TChannel() = default;
};

using TChannelList = TVector<ui64>;

struct TSortColumn {
    std::string Column;
    bool Ascending;

    TSortColumn(const std::string& column, bool ascending)
        : Column(column)
        , Ascending(ascending)
    {}
};

struct TMergeTaskInput {
    TVector<TSortColumn> SortColumns;

    TMergeTaskInput(const TVector<TSortColumn>& sortColumns)
        : SortColumns(sortColumns)
    {}
};

struct TSourceInput {};

// Enum values must match to ConnectionInfo variant alternatives
enum class TTaskInputType {
    UnionAll,
    Merge,
    Source
};

struct TTransform {
    TString Type;

    TString InputType;
    TString OutputType;

    ::google::protobuf::Any Settings;
};

template <class TInputMeta>
struct TTaskInput {
    std::variant<std::monostate, TMergeTaskInput, TSourceInput> ConnectionInfo;
    TChannelList Channels;
    TMaybe<::google::protobuf::Any> SourceSettings;
    TString SourceType;
    NYql::NDqProto::EWatermarksMode WatermarksMode = NYql::NDqProto::EWatermarksMode::WATERMARKS_MODE_DISABLED;
    TInputMeta Meta;
    TMaybe<TTransform> Transform;

    TTaskInputType Type() const {
        return static_cast<TTaskInputType>(ConnectionInfo.index());
    }
};

struct TTaskOutputType {
    enum : ui32 {
        Undefined = 0,
        Map,
        HashPartition,
        Broadcast,
        Effects,
        Sink,
        COMMON_TASK_OUTPUT_TYPE_END
    };
};

template <class TOutputMeta>
struct TTaskOutput {
    ui32 Type = TTaskOutputType::Undefined;
    NYql::NDq::TChannelList Channels;
    TVector<TString> KeyColumns;
    ui32 PartitionsCount = 0;
    TMaybe<::google::protobuf::Any> SinkSettings;
    TString SinkType;
    TOutputMeta Meta;
    TMaybe<TTransform> Transform;
};

template <class TStageInfoMeta, class TTaskMeta, class TInputMeta, class TOutputMeta>
struct TTask {
private:
    YDB_OPT(ui32, MetaId);
    YDB_ACCESSOR(bool, UseLlvm, false);
public:
    using TInputType = TTaskInput<TInputMeta>;
    using TOutputType = TTaskOutput<TOutputMeta>;

    explicit TTask(const TStageInfo<TStageInfoMeta>& stageInfo)
        : StageId(stageInfo.Id)
        , Inputs(stageInfo.InputsCount)
        , Outputs(stageInfo.OutputsCount) {
    }

    ui64 Id = 0;
    TStageId StageId;
    TVector<TInputType> Inputs;
    TVector<TOutputType> Outputs;
    NActors::TActorId ComputeActorId;
    TTaskMeta Meta;
    NDqProto::ECheckpointingMode CheckpointingMode = NDqProto::CHECKPOINTING_MODE_DEFAULT;
    NDqProto::EWatermarksMode WatermarksMode = NDqProto::WATERMARKS_MODE_DISABLED;
};

template <class TGraphMeta, class TStageInfoMeta, class TTaskMeta, class TInputMeta, class TOutputMeta>
class TDqTasksGraph : private TMoveOnly {
public:
    using TStageInfoType = TStageInfo<TStageInfoMeta>;
    using TTaskType = TTask<TStageInfoMeta, TTaskMeta, TInputMeta, TOutputMeta>;

public:
    TDqTasksGraph() = default;

    const TGraphMeta& GetMeta() const {
        return Meta;
    }

    TGraphMeta& GetMeta() {
        return Meta;
    }

    const TChannel& GetChannel(ui64 id) const {
        YQL_ENSURE(id <= Channels.size());
        return Channels[id - 1];
    }

    TChannel& GetChannel(ui64 id) {
        YQL_ENSURE(id <= Channels.size());
        return Channels[id - 1];
    }

    const TVector<TChannel>& GetChannels() const {
        return Channels;
    }

    TVector<TChannel>& GetChannels() {
        return Channels;
    }

    const TStageInfoType& GetStageInfo(const TStageId& id) const {
        auto stageInfo = StagesInfo.FindPtr(id);
        YQL_ENSURE(stageInfo, "no stage #" << id);
        return *stageInfo;
    }

    TStageInfoType& GetStageInfo(const TStageId& id) {
        auto stageInfo = StagesInfo.FindPtr(id);
        YQL_ENSURE(stageInfo, "no stage #" << id);
        return *stageInfo;
    }

    const TStageInfoType& GetStageInfo(ui64 txId, const NNodes::TDqStageBase& stage) const {
        return GetStageInfo(TStageId(txId, stage.Ref().UniqueId()));
    }

    TStageInfoType& GetStageInfo(ui64 txId, const NNodes::TDqStageBase& stage) {
        return GetStageInfo(TStageId(txId, stage.Ref().UniqueId()));
    }

    const TStageInfoType& GetStageInfo(const NNodes::TDqStageBase& stage) const {
        return GetStageInfo(0, stage);
    }

    TStageInfoType& GetStageInfo(const NNodes::TDqStageBase& stage) {
        return GetStageInfo(0, stage);
    }

    const THashMap<TStageId, TStageInfoType>& GetStagesInfo() const {
        return StagesInfo;
    }

    THashMap<TStageId, TStageInfoType>& GetStagesInfo() {
        return StagesInfo;
    }

    const TTaskType& GetTask(ui64 id) const {
        YQL_ENSURE(id <= Tasks.size());
        return Tasks[id - 1];
    }

    TTaskType& GetTask(ui64 id) {
        YQL_ENSURE(id <= Tasks.size());
        return Tasks[id - 1];
    }

    const TVector<TTaskType>& GetTasks() const {
        return Tasks;
    }

    TVector<TTaskType>& GetTasks() {
        return Tasks;
    }

    TChannel& AddChannel() {
        TChannel channel;
        channel.Id = Channels.size() + 1;
        return Channels.emplace_back(channel);
    }

    bool AddStageInfo(TStageInfoType stageInfo) {
        return StagesInfo.emplace(stageInfo.Id, std::move(stageInfo)).second;
    }

    TTaskType& AddTask(TStageInfoType& stageInfo) {
        auto& task = Tasks.emplace_back(stageInfo);
        task.Id = Tasks.size();
        stageInfo.Tasks.push_back(task.Id);
        return task;
    }

    void Clear() {
        StagesInfo.clear();
        Tasks.clear();
        Channels.clear();
    }

private:
    THashMap<TStageId, TStageInfoType> StagesInfo;
    TVector<TTaskType> Tasks;
    TVector<TChannel> Channels;
    TGraphMeta Meta;
};

} // namespace NYql::NDq

template<>
struct THash<NYql::NDq::TStageId> {
    inline size_t operator()(const NYql::NDq::TStageId& id) const {
        return id.Hash();
    }
};

template<>
inline void Out<NYql::NDq::TStageId>(IOutputStream& o, const NYql::NDq::TStageId& id) {
    o << '[' << id.TxId << ',' << id.StageId << ']';
}
