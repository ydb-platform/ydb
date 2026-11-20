#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/control_plane.h>

#include <optional>

namespace NKikimrScheme {
class TEvDescribeSchemeResult;
}

namespace NKikimr::NReplication::NController {

struct TDstCdcSettings {
    TPathId TablePathId;
    Ydb::Table::ChangefeedDescription SourceStream;
    TString Consumer;
    TPathId StreamPathId;
    TPathId TopicPathId;
};

struct TCdcCreation {
    enum class EPath { Table, Stream, Topic };

    TDstCdcSettings Settings;
    ui64 InitialCreateTxId = 0;
    ui64 TableVersion = 0;
    ui64 TablePartitionCount = 0;
    bool ActivateOnDetach = false;
    bool NeedsTopicSetup = false;
    NKikimrSchemeOp::TCreateCdcStream Definition;
    NKikimrPQ::TPQTabletConfig Topic;
    TString TablePath;
    EPath Describe = EPath::Table;
    struct TRefusal {
        NKikimrSchemeOp::TModifyScheme Proposal;
        TString Reason;
    };
    std::optional<TRefusal> Refusal;

    bool Prepare(const NYdb::NTopic::TTopicDescription& topic, const TString& database, TString& error);
    bool CheckTable(const NKikimrScheme::TEvDescribeSchemeResult& record,
        const TPathId& replicationPathId, ui64 schemeShardId, TString& owner, TString& error);
    bool CheckStream(const NKikimrScheme::TEvDescribeSchemeResult& record, ui64 schemeShardId,
        const TString& owner, TString& error);
    bool CheckTopic(const NKikimrScheme::TEvDescribeSchemeResult& record, ui64 schemeShardId,
        const TString& owner, TString& error);
    void FillCreate(NKikimrSchemeOp::TModifyScheme& tx) const;
    void FillTopicSetup(NKikimrSchemeOp::TModifyScheme& tx,
        const NKikimrScheme::TEvDescribeSchemeResult& record) const;
};

}
