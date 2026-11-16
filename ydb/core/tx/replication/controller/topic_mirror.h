#pragma once
#include "replication.h"
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <util/string/cast.h>

namespace NKikimr::NReplication::NController {

struct TTopicMirrorSettings {
    NKikimrPQ::TMirrorPartitionConfig Config;
    TString Error;

    TTopicMirrorSettings() = default;
    TTopicMirrorSettings(const TReplication& replication, const TReplication::ITarget& target) {
        if (target.GetKind() == TReplication::ETargetKind::Topic) {
            Build(replication, target, Error);
        }
    }

private:
    bool Build(const TReplication& replication, const TReplication::ITarget& target, TString& error) {
        const auto& params = replication.GetConfig().GetSrcConnectionParams();
        if (params.HasCaCert()) {
            error = "Custom source CA certificates are not supported by Topic mirroring";
            return false;
        }

        TStringBuf endpoint(params.GetEndpoint());
        TStringBuf host;
        TStringBuf port;
        if (endpoint.StartsWith('[')) {
            const auto close = endpoint.find(']');
            if (close == TStringBuf::npos || close + 1 >= endpoint.size() || endpoint[close + 1] != ':') {
                error = "Invalid source endpoint";
                return false;
            }
            host = endpoint.substr(1, close - 1);
            port = endpoint.substr(close + 2);
        } else {
            const auto colon = endpoint.rfind(':');
            if (colon == TStringBuf::npos) {
                error = "Source endpoint must include a port";
                return false;
            }
            host = endpoint.substr(0, colon);
            port = endpoint.substr(colon + 1);
        }
        ui32 parsedPort = 0;
        if (host.empty() || !TryFromString<ui32>(port, parsedPort) || !parsedPort || parsedPort > 65535) {
            error = "Invalid source endpoint";
            return false;
        }

        Config.SetEndpoint(TString(host));
        Config.SetEndpointPort(parsedPort);
        Config.SetTopic(target.GetSrcPath());
        Config.SetDatabase(params.GetDatabase());
        Config.SetConsumer(target.GetStreamConsumerName());
        Config.SetUseSecureConnection(params.GetEnableSsl());
        Config.SetMirrorConsumerOffsets(true);

        auto& credentials = *Config.MutableCredentials();
        switch (params.GetCredentialsCase()) {
        case NKikimrReplication::TConnectionParams::CREDENTIALS_NOT_SET:
            credentials.SetOauthToken("");
            break;
        case NKikimrReplication::TConnectionParams::kOAuthToken: {
            const auto& token = params.GetOAuthToken();
            if (!token.GetTokenSecretName().empty()) {
                // TODO(KIKIMR-27673, #59067): pass the secret reference after mirrorer support lands.
                // credentials.SetOauthTokenSecretName(token.GetTokenSecretName());
                error = "Topic mirroring secret references require the mirrorer secrets dependency";
                return false;
            } else if (token.HasToken()) {
                credentials.SetOauthToken(token.GetToken());
            } else {
                error = "Source OAuth credentials must contain a token or secret reference";
                return false;
            }
            break;
        }
        default:
            error = "Topic mirroring supports OAuth source credentials only";
            return false;
        }
        return true;
    }
};

}
