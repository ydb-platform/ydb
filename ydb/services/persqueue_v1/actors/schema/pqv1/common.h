#pragma once

#include <ydb/core/persqueue/public/schema/common.h>
#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1 {

NPQ::NSchema::TResult ApplyCreate(
    NKikimrSchemeOp::TModifyScheme& modifyScheme,
    const Ydb::PersQueue::V1::TopicSettings& settings,
    const TString& path,
    const TString& name,
    const TString& database,
    const TString& localDc
);

NPQ::NSchema::TResult ApplyAlter(
    NKikimrSchemeOp::TModifyScheme& modifyScheme,
    const Ydb::PersQueue::V1::TopicSettings& settings,
    const TString& path,
    const TString& name,
    const TString& database,
    const TString& localDc
);

} // namespace NKikimr::NGRpcProxy::V1::NPQv1
