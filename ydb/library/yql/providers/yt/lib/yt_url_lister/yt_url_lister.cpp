#include "yt_url_lister.h"

#include <library/cpp/cgiparam/cgiparam.h>

#include <ydb/library/yql/providers/yt/lib/init_yt_api/init.h>
#include <ydb/library/yql/utils/log/log.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <util/generic/guid.h>
#include <util/generic/strbuf.h>
#include <util/string/builder.h>


namespace NYql::NPrivate {

const char Sep = '/';
const TStringBuf Scheme = "yt";

class TYtUrlLister: public IUrlLister {
public:
    TYtUrlLister() = default;

public:
    bool Accept(const THttpURL& url) const override {
        auto rawScheme = url.GetField(NUri::TField::FieldScheme);
        return NUri::EqualNoCase(rawScheme, Scheme);
    }

    TVector<TUrlListEntry> ListUrl(const THttpURL& url, const TString& token) const override {
        InitYtApiOnce();

        TCgiParameters params(url.GetField(NUri::TField::FieldQuery));

        NYT::TCreateClientOptions createOpts;
        if (token) {
            createOpts.Token(token);
        }

        auto host = url.PrintS(NUri::TField::FlagHostPort);

        auto path = params.Has("path")
            ? params.Get("path")
            : TString(TStringBuf(url.GetField(NUri::TField::FieldPath)).Skip(1));

        auto client = NYT::CreateClient(host, createOpts);
        NYT::IClientBasePtr tx = client;

        TString txId = params.Get("transaction_id");
        if (!txId) {
            txId = params.Get("t");
        }

        YQL_LOG(INFO) << "YtUrlLister: host=" << host << ", path='" << path << "', tx=" << txId;

        if (txId) {
            TGUID guid;
            if (!GetGuid(txId, guid)) {
                ythrow yexception() << "Bad transaction ID: " << txId;
            }

            tx = client->AttachTransaction(guid);
        }

        auto composeUrl = [&](auto name) {
            THttpURL url;

            url.Set(NUri::TField::FieldScheme, Scheme);
            url.Set(NUri::TField::FieldHost, host);
            url.Set(NUri::TField::FieldPath, TStringBuilder() << Sep << path << Sep << name);

            if (txId) {
                url.Set(NUri::TField::FieldQuery, TStringBuilder() << "transaction_id=" << txId);
            }

            return url;
        };

        NYT::TListOptions listOpts;
        listOpts.AttributeFilter(
            NYT::TAttributeFilter().Attributes({"type"})
        );

        TVector<TUrlListEntry> entries;

        for (const auto& item: tx->List(path, listOpts)) {
            auto& entry = entries.emplace_back();

            const auto& itemName = item.AsString();
            const auto& itemType = item.GetAttributes()["type"].AsString();

            entry.Name = itemName;
            entry.Url = composeUrl(itemName);
            entry.Type = itemType == "map_node"
                ? EUrlListEntryType::DIRECTORY
                : EUrlListEntryType::FILE;
        }

        return entries;
    }
};

}

namespace NYql {

IUrlListerPtr MakeYtUrlLister() {
    return MakeIntrusive<NPrivate::TYtUrlLister>();
}

}
