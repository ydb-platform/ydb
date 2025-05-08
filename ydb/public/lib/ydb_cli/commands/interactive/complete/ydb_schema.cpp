#include "ydb_schema.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>

namespace NYdb::NConsoleClient {

    class TYDBSchema: public NSQLComplete::ISchema {
    public:
        explicit TYDBSchema(TDriver driver, TString database)
            : Driver_(std::move(driver))
            , Database_(std::move(database))
        {
        }

        NThreading::TFuture<NSQLComplete::TListResponse> List(const NSQLComplete::TListRequest& request) const override {
            auto [head, tail] = ParsePath(request.Path);
            return List(head)
                .Apply([nameHint = ToLowerUTF8(tail), request](auto f) {
                    NSQLComplete::TListResponse response;

                    response.Entries = f.ExtractValue();

                    EraseIf(response.Entries, [&](const NSQLComplete::TFolderEntry& entry) {
                        return !entry.Name.StartsWith(nameHint);
                    });

                    EraseIf(response.Entries, [types = std::move(request.Filter.Types)](
                                                  const NSQLComplete::TFolderEntry& entry) {
                        return types && !types->contains(entry.Type);
                    });

                    response.Entries.crop(request.Limit);

                    response.NameHintLength = nameHint.length();
                    return response;
                });
        }

    private:
        NThreading::TFuture<TVector<NSQLComplete::TFolderEntry>> List(TStringBuf head) const {
            auto path = TString(head);
            if (!head.StartsWith('/')) {
                path.prepend("/");
                path.prepend(Database_);
            }

            return NScheme::TSchemeClient(Driver_)
                .ListDirectory(path)
                .Apply([path](NScheme::TAsyncListDirectoryResult f) {
                    NScheme::TListDirectoryResult result = f.ExtractValue();

                    if (!result.IsSuccess()) {
                        result.Out(Cerr);
                        return TVector<NSQLComplete::TFolderEntry>{};
                        // TODO(YQL-19747): Use Swallowing and Logging NameServices
                        // ythrow yexception()
                        //     << "ListDirectory('" << path << "') failed: "
                        //     << result.GetIssues().ToOneLineString();
                    }

                    const std::vector<NScheme::TSchemeEntry>& children = result.GetChildren();

                    TVector<NSQLComplete::TFolderEntry> entries;
                    entries.reserve(children.size());

                    for (size_t i = 0; i < children.size(); ++i) {
                        const auto& child = children[i];

                        NSQLComplete::TFolderEntry entry = {
                            .Type = Convert(child.Type),
                            .Name = TString(child.Name),
                        };

                        entries.emplace_back(std::move(entry));
                    }

                    return entries;
                });
        }

        static TString Convert(NScheme::ESchemeEntryType type) {
            switch (type) {
                case NScheme::ESchemeEntryType::Directory:
                    return "Folder";
                case NScheme::ESchemeEntryType::Table:
                    return "Table";
                case NScheme::ESchemeEntryType::PqGroup:
                    return "PqGroup";
                case NScheme::ESchemeEntryType::SubDomain:
                    return "SubDomain";
                case NScheme::ESchemeEntryType::RtmrVolume:
                    return "RtmrVolume";
                case NScheme::ESchemeEntryType::BlockStoreVolume:
                    return "BlockStoreVolume";
                case NScheme::ESchemeEntryType::CoordinationNode:
                    return "CoordinationNode";
                case NScheme::ESchemeEntryType::ColumnStore:
                    return "ColumnStore";
                case NScheme::ESchemeEntryType::ColumnTable:
                    return "ColumnTable";
                case NScheme::ESchemeEntryType::Sequence:
                    return "Sequence";
                case NScheme::ESchemeEntryType::Replication:
                    return "Replication";
                case NScheme::ESchemeEntryType::Topic:
                    return "Topic";
                case NScheme::ESchemeEntryType::ExternalTable:
                    return "ExternalTable";
                case NScheme::ESchemeEntryType::ExternalDataSource:
                    return "ExternalDataSource";
                case NScheme::ESchemeEntryType::View:
                    return "View";
                case NScheme::ESchemeEntryType::ResourcePool:
                    return "ResourcePool";
                case NScheme::ESchemeEntryType::Unknown:
                default:
                    return "Unknown";
            }
        }

        static std::tuple<TStringBuf, TStringBuf> ParsePath(TString path Y_LIFETIME_BOUND) {
            size_t pos = path.find_last_of('/');
            if (pos == TString::npos) {
                return {"", path};
            }

            TStringBuf head, tail;
            TStringBuf(path).SplitAt(pos + 1, head, tail);
            return {head, tail};
        }

        TDriver Driver_;
        TString Database_;
    };

    NSQLComplete::ISchema::TPtr MakeYDBSchema(TDriver driver, TString database) {
        return new TYDBSchema(std::move(driver), std::move(database));
    }

} // namespace NYdb::NConsoleClient
