#include "yql_qplayer_udf_resolver.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/providers/common/schema/expr/yql_expr_schema.h>
#include <library/cpp/yson/node/node_io.h>

#include <openssl/sha.h>

namespace NYql::NCommon {

namespace {

const TString UdfResolver_GetSystemModulePath = "UdfResolver_GetSystemModulePath";
const TString UdfResolver_LoadMetadataImports = "UdfResolver_LoadMetadataImports";
const TString UdfResolver_LoadMetadata = "UdfResolver_LoadMetadata";
const TString UdfResolver_ContainsModule = "UdfResolver_ContainsModule";

// TODO(vitya-smirnov): Copy-pasted from core/qplayer/url_lister/qplayer_url_lister_manager.
TString MakeHash(const TString& str) {
    SHA256_CTX sha;
    SHA256_Init(&sha);
    SHA256_Update(&sha, str.data(), str.size());
    std::array<unsigned char, SHA256_DIGEST_LENGTH> hash;
    SHA256_Final(hash.data(), &sha);
    return TString((const char*)hash.data(), sizeof(hash));
}

class TResolver: public IUdfResolver {
public:
    TResolver(IUdfResolver::TPtr inner, const TQContext& qContext)
        : Inner_(inner)
        , QContext_(qContext)
    {
    }

    TMaybe<TFilePathWithMd5> GetSystemModulePath(const TStringBuf& moduleName) const final {
        if (QContext_.CanRead()) {
            auto res = QContext_.GetReader()->Get({UdfResolver_GetSystemModulePath, TString(moduleName)}).GetValueSync();
            return MakeMaybe<TFilePathWithMd5>(res ? res->Value : "", "");
        }

        auto res = Inner_->GetSystemModulePath(moduleName);
        if (res && QContext_.CanWrite()) {
            QContext_.GetWriter()->Put({UdfResolver_GetSystemModulePath, TString(moduleName)}, res->Path).GetValueSync();
        }

        return res;
    }

    bool LoadMetadata(const TVector<TImport*>& imports,
                      const TVector<TFunction*>& functions, TExprContext& ctx, NUdf::ELogLevel logLevel, THoldingFileStorage& storage) const final {
        if (QContext_.CanRead()) {
            for (auto& import : imports) {
                auto key = MakeKey(import);
                auto res = QContext_.GetReader()->Get({UdfResolver_LoadMetadataImports, key}).GetValueSync();
                if (!res) {
                    // for compatibility
                    continue;
                }

                LoadValue(import, res->Value);
            }

            for (auto& f : functions) {
                auto key = MakeKey(f);
                auto res = QContext_.GetReader()->Get({UdfResolver_LoadMetadata, key}).GetValueSync();
                if (!res) {
                    ythrow yexception() << "Missing replay data";
                }

                LoadValue(f, res->Value, ctx);
            }

            return true;
        }

        auto res = Inner_->LoadMetadata(imports, functions, ctx, logLevel, storage);
        if (res && QContext_.CanWrite()) {
            for (const auto& import : imports) {
                auto key = MakeKey(import);
                auto value = SaveValue(import);
                QContext_.GetWriter()->Put({UdfResolver_LoadMetadataImports, key}, value).GetValueSync();
            }

            // calculate hash for each function and store it
            for (const auto& f : functions) {
                auto key = MakeKey(f);
                auto value = SaveValue(f);
                QContext_.GetWriter()->Put({UdfResolver_LoadMetadata, key}, value).GetValueSync();
            }
        }

        return res;
    }

    TResolveResult LoadRichMetadata(const TVector<TImport>& imports, NUdf::ELogLevel logLevel, THoldingFileStorage& storage) const final {
        if (QContext_.CanRead()) {
            ythrow yexception() << "Can't replay LoadRichMetadata";
        }

        return Inner_->LoadRichMetadata(imports, logLevel, storage);
    }

    bool ContainsModule(const TStringBuf& moduleName) const final {
        if (QContext_.CanRead()) {
            auto res = QContext_.GetReader()->Get({UdfResolver_ContainsModule, TString(moduleName)}).GetValueSync();
            if (!res) {
                ythrow yexception() << "Missing replay data";
            }

            return res->Value == "1";
        }

        auto ret = Inner_->ContainsModule(moduleName);
        if (QContext_.CanWrite()) {
            QContext_.GetWriter()->Put({UdfResolver_ContainsModule, TString(moduleName)}, ret ? "1" : "0").GetValueSync();
        }

        return ret;
    }

private:
    TString MakeKey(const TImport* import) const {
        // clang-format off
        auto node = NYT::TNode()
            ("FileAlias", NYT::TNode(import->FileAlias))
            ("PosColumn", NYT::TNode(import->Pos.Column))
            ("PosRow", NYT::TNode(import->Pos.Row))
            ("PosFile", NYT::TNode(import->Pos.File));
        // clang-format on

        return MakeHash(NYT::NodeToCanonicalYsonString(node, NYT::NYson::EYsonFormat::Binary));
    }

    TString MakeKey(const TFunction* f) const {
        auto node = NYT::TNode()("Name", NYT::TNode(f->Name));
        if (f->TypeConfig) {
            node("TypeConfig", NYT::TNode(f->TypeConfig));
        }

        if (f->UserType) {
            node("UserType", TypeToYsonNode(f->UserType));
        }

        return MakeHash(NYT::NodeToCanonicalYsonString(node, NYT::NYson::EYsonFormat::Binary));
    }

    TString SaveValue(const TImport* import) const {
        auto node = NYT::TNode::CreateMap();
        if (import->Modules) {
            auto modules = NYT::TNode::CreateList();
            for (const auto& x : *import->Modules) {
                modules.Add(x);
            }

            node("Modules", modules);
        }

        return NYT::NodeToYsonString(node, NYT::NYson::EYsonFormat::Binary);
    }

    TString SaveValue(const TFunction* f) const {
        auto node = NYT::TNode()("NormalizedName", f->NormalizedName)("CallableType", TypeToYsonNode(f->CallableType));
        if (f->NormalizedUserType && f->NormalizedUserType->GetKind() != ETypeAnnotationKind::Void) {
            node("NormalizedUserType", TypeToYsonNode(f->NormalizedUserType));
        }

        if (f->RunConfigType && f->RunConfigType->GetKind() != ETypeAnnotationKind::Void) {
            node("RunConfigType", TypeToYsonNode(f->RunConfigType));
        }

        if (f->SupportsBlocks) {
            node("SupportsBlocks", NYT::TNode(true));
        }

        if (f->IsStrict) {
            node("IsStrict", NYT::TNode(true));
        }

        if (!f->Messages.empty()) {
            auto list = NYT::TNode::CreateList();
            for (const auto& x : f->Messages) {
                list.Add(x);
            }

            node("Messages", list);
        }

        return NYT::NodeToYsonString(node, NYT::NYson::EYsonFormat::Binary);
    }

    void LoadValue(TImport* import, const TString& value) const {
        auto node = NYT::NodeFromYsonString(value);
        if (node.HasKey("Modules")) {
            import->Modules.ConstructInPlace();
            const auto& moduleNodes = node["Modules"].AsList();
            for (const auto& moduleNode : moduleNodes) {
                import->Modules->push_back(moduleNode.AsString());
            }
        }
    }

    void LoadValue(TFunction* f, const TString& value, TExprContext& ctx) const {
        auto node = NYT::NodeFromYsonString(value);
        if (node.HasKey("NormalizedName")) {
            f->NormalizedName = node["NormalizedName"].AsString();
        } else {
            f->NormalizedName = f->Name;
        }

        f->CallableType = ParseTypeFromYson(node["CallableType"], ctx);
        if (node.HasKey("NormalizedUserType")) {
            f->NormalizedUserType = ParseTypeFromYson(node["NormalizedUserType"], ctx);
        }

        if (node.HasKey("RunConfigType")) {
            f->RunConfigType = ParseTypeFromYson(node["RunConfigType"], ctx);
        }

        if (node.HasKey("SupportsBlocks")) {
            f->SupportsBlocks = node["SupportsBlocks"].AsBool();
        }

        if (node.HasKey("IsStrict")) {
            f->IsStrict = node["IsStrict"].AsBool();
        }

        if (node.HasKey("Messages")) {
            for (const auto& x : node["Messages"].AsList()) {
                f->Messages.push_back(x.AsString());
            }
        }
    }

private:
    const IUdfResolver::TPtr Inner_;
    const TQContext QContext_;
};

} // namespace

IUdfResolver::TPtr WrapUdfResolverWithQContext(IUdfResolver::TPtr inner, const TQContext& qContext) {
    return new TResolver(inner, qContext);
}

} // namespace NYql::NCommon
