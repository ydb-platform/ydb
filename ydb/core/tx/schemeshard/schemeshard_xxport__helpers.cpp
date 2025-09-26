#include "schemeshard_xxport__helpers.h"

#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

#include <ydb/public/api/protos/ydb_operation.pb.h>

namespace NKikimr::NSchemeShard {

TString GetUid(const Ydb::Operations::OperationParams& operationParams) {
    const auto& labels = operationParams.labels();
    auto it = labels.find("uid");
    if (it != labels.end()) {
        return it->second;
    }
    return {};
}

template <class TInfo>
THolder<TEvSchemeShard::TEvModifySchemeTransaction> MakeModifySchemeTransactionImpl(TSchemeShard* ss, TTxId txId, const TInfo& xxportInfo) {
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), ss->TabletID());
    auto& record = propose->Record;
    record.SetPeerName(xxportInfo.PeerName);
    record.SetSanitizedToken(xxportInfo.SanitizedToken);
    if (xxportInfo.UserSID) {
        record.SetOwner(*xxportInfo.UserSID);
        record.SetUserSID(*xxportInfo.UserSID);
    }
    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> MakeModifySchemeTransaction(TSchemeShard* ss, TTxId txId, const TExportInfo& exportInfo) {
    return MakeModifySchemeTransactionImpl(ss, txId, exportInfo);
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> MakeModifySchemeTransaction(TSchemeShard* ss, TTxId txId, const TImportInfo& importInfo) {
    return MakeModifySchemeTransactionImpl(ss, txId, importInfo);
}

}  // NKikimr::NSchemeShard
