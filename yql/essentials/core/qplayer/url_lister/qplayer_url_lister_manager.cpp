#include "qplayer_url_lister_manager.h"

#include <library/cpp/yson/node/node_io.h>

#include <util/string/builder.h>
#include <openssl/sha.h>

namespace {

using namespace NYql;

const TString UrlListerManager_ListUrl = "UrlListerManager_ListUrl";
const TString UrlListerManager_ListUrlRecursive = "UrlListerManager_ListUrlRecursive";

TString MakeHash(const TString& str) {
    SHA256_CTX sha;
    SHA256_Init(&sha);
    SHA256_Update(&sha, str.data(), str.size());
    std::array<unsigned char, SHA256_DIGEST_LENGTH> hash;
    SHA256_Final(hash.data(), &sha);
    return TString((const char*)hash.data(), sizeof(hash));
}

TString SerializeResult(const TVector<TUrlListEntry>& result) {
    auto node = NYT::TNode::CreateList();
    for (auto& e : result) {
        node.Add(
            NYT::TNode::CreateMap()("name", e.Name)("type", int(e.Type))("url", e.Url));
    }
    return NodeToYsonString(node, NYT::NYson::EYsonFormat::Binary);
}

TVector<TUrlListEntry> DeserializeResult(const TString& inp) {
    auto node = NYT::NodeFromYsonString(inp);
    TVector<TUrlListEntry> result;
    result.reserve(node.Size());
    for (const auto& e : node.AsList()) {
        result.emplace_back(TUrlListEntry{
            .Url = e["url"].AsString(),
            .Name = e["name"].AsString(),
            .Type = EUrlListEntryType(e["type"].AsInt64())});
    }
    return result;
}

class TQPlayerUrlListerManager: public IUrlListerManager {
public:
    TQPlayerUrlListerManager(IUrlListerManagerPtr underlying, TQContext qContext)
        : Underlying_(underlying)
        , QContext_(qContext)
    {
    }

    TVector<TUrlListEntry> ListUrl(const TString& url, const TString& tokenName) const override {
        TMaybe<TString> key;
        if (QContext_) {
            key = MakeHash(NodeToCanonicalYsonString(
                // clang-format off
                NYT::TNode()
                        ("url", url)
                        ("token", tokenName),
                // clang-format on
                NYT::NYson::EYsonFormat::Binary));
        }
        if (QContext_.CanRead()) {
            auto val = QContext_.GetReader()->Get({UrlListerManager_ListUrl, *key}).GetValueSync();
            if (!val) {
                ythrow yexception() << "Missing replay data";
            }
            return DeserializeResult(val->Value);
        }
        auto result = Underlying_->ListUrl(url, tokenName);
        if (QContext_.CanWrite()) {
            QContext_.GetWriter()->Put({UrlListerManager_ListUrl, *key}, SerializeResult(result));
        }
        return result;
    }

    TVector<TUrlListEntry> ListUrlRecursive(const TString& url, const TString& tokenName, const TString& separator, ui32 foldersLimit) const override {
        TMaybe<TString> key;
        if (QContext_) {
            key = MakeHash(NodeToCanonicalYsonString(
                // clang-format off
                NYT::TNode()
                    ("url", url)
                    ("token", tokenName)
                    ("separator", separator)
                    ("foldersLimit", foldersLimit),
                // clang-format on
                NYT::NYson::EYsonFormat::Binary));
        }
        if (QContext_.CanRead()) {
            auto val = QContext_.GetReader()->Get({UrlListerManager_ListUrlRecursive, *key}).GetValueSync();
            if (!val) {
                ythrow yexception() << "Missing replay data";
            }
            return DeserializeResult(val->Value);
        }
        auto result = Underlying_->ListUrlRecursive(url, tokenName, separator, foldersLimit);
        if (QContext_.CanWrite()) {
            QContext_.GetWriter()->Put({UrlListerManager_ListUrlRecursive, *key}, SerializeResult(result));
        }
        return result;
    }

    TIntrusivePtr<IUrlListerManager> Clone() const override {
        return MakeIntrusive<TQPlayerUrlListerManager>(Underlying_->Clone(), QContext_);
    }

    void SetCredentials(TCredentials::TPtr credentials) override {
        if (QContext_.CanRead()) {
            return;
        }
        Underlying_->SetCredentials(credentials);
    }

    void SetTokenResolver(std::function<TString(const TString&, const TString&)> tokenResolver) override {
        if (QContext_.CanRead()) {
            return;
        }
        Underlying_->SetTokenResolver(std::move(tokenResolver));
    }

    void SetUrlPreprocessing(IUrlPreprocessing::TPtr urlPreprocessing) override {
        if (QContext_.CanRead()) {
            return;
        }
        Underlying_->SetUrlPreprocessing(urlPreprocessing);
    }

    void SetParameters(const NYT::TNode& parameters) override {
        if (QContext_.CanRead()) {
            return;
        }
        Underlying_->SetParameters(parameters);
    }

private:
    IUrlListerManagerPtr Underlying_;
    const TQContext QContext_;
};
} // namespace

namespace NYql::NCommon {
IUrlListerManagerPtr WrapUrlListerManagerWithQContext(IUrlListerManagerPtr underlying, const TQContext& qContext) {
    return MakeIntrusive<TQPlayerUrlListerManager>(underlying, qContext);
}
} // namespace NYql::NCommon
