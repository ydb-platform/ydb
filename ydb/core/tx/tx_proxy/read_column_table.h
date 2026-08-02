#pragma once

#include "read_table.h"

#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <util/generic/vector.h>

namespace NKikimr {
namespace NTxProxy {

struct TReadColumnTableParams {
    TReadTableSettings Settings;
    ui64 TxId = 0;
    TTxProxyServices Services;
    TIntrusivePtr<TTxProxyMon> TxProxyMon;
    TTableId TableId;
    TVector<TTableColumnInfo> Columns;
    THolder<TKeyDesc> KeyDesc;
    NSchemeCache::TDomainInfo::TPtr DomainInfo;
    TActorId Parent; // original ReadTable worker; notified on completion
};

IActor* CreateReadColumnTableWorker(TReadColumnTableParams&& params);

} // namespace NTxProxy
} // namespace NKikimr
