#pragma once

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <expected>

namespace NKikimr::NGRpcProxy::V1 {

    class TConsumersAdvancedMonitoringSettings {
    public:
        TConsumersAdvancedMonitoringSettings();
        TConsumersAdvancedMonitoringSettings(TConsumersAdvancedMonitoringSettings&&);
        TConsumersAdvancedMonitoringSettings& operator=(TConsumersAdvancedMonitoringSettings&&);
        ~TConsumersAdvancedMonitoringSettings();

        static std::expected<TConsumersAdvancedMonitoringSettings, TString> FromJson(TStringBuf json);

        bool UpdateConsumerConfig(const TStringBuf name, ::NKikimrPQ::TPQTabletConfig_TConsumer& consumer) const;
        Ydb::StatusIds::StatusCode CheckForUnknownConsumers(TString& error) const;
    private:
        class TImpl;
        THolder<TImpl> Impl_;
    };

}
