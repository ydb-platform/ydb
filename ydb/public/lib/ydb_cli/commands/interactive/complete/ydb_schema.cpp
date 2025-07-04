#include "ydb_schema.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <yql/essentials/sql/v1/complete/name/object/simple/schema.h>

namespace NYdb::NConsoleClient {

    class TYDBSchema: public NSQLComplete::ISimpleSchema {
    public:
        explicit TYDBSchema(TDriver driver, TString database, bool isVerbose)
            : Driver_(std::move(driver))
            , Database_(std::move(database))
            , IsVerbose_(isVerbose)
        {
        }

        NSQLComplete::TSplittedPath Split(TStringBuf path) const override {
            size_t pos = path.find_last_of('/');
            if (pos == TString::npos) {
                return {"", path};
            }

            TStringBuf head, tail;
            TStringBuf(path).SplitAt(pos + 1, head, tail);
            return {head, tail};
        }

        NThreading::TFuture<TVector<NSQLComplete::TFolderEntry>> List(TString folder) const override {
            return NScheme::TSchemeClient(Driver_)
                .ListDirectory(Qualified(folder))
                .Apply([this, folder](auto f) { return this->Convert(folder, f.ExtractValue()); });
        }

        NThreading::TFuture<TMaybe<NSQLComplete::TTableDetails>>
        DescribeTable(const TString& /* cluster */, const TString& path) const override {
            auto promise = NThreading::NewPromise<TMaybe<NSQLComplete::TTableDetails>>();
            NTable::TTableClient(Driver_)
                .GetSession(NTable::TCreateSessionSettings())
                .Apply([this, path, promise](auto f) mutable {
                    static_cast<NTable::TCreateSessionResult>(f.ExtractValue())
                        .GetSession()
                        .DescribeTable(Qualified(path))
                        .Apply([this, path, promise](auto f) mutable {
                            NTable::TDescribeTableResult result = f.ExtractValue();
                            promise.SetValue(Convert(path, std::move(result)));
                        });
                });
            return promise;
        }

    private:
        TString Qualified(TString folder) const {
            if (!folder.StartsWith('/')) {
                folder.prepend('/');
                folder.prepend(Database_);
            }
            return folder;
        }

        TVector<NSQLComplete::TFolderEntry> Convert(TString folder, NScheme::TListDirectoryResult result) const {
            if (!result.IsSuccess()) {
                if (IsVerbose_) {
                    Cerr << "ListDirectory('" << folder << "') failed: "
                         << result.GetIssues().ToOneLineString() << Endl;
                }
                return {};
            }

            return Convert(result.GetChildren());
        }

        static TVector<NSQLComplete::TFolderEntry> Convert(const std::vector<NScheme::TSchemeEntry>& children) {
            TVector<NSQLComplete::TFolderEntry> entries;
            entries.reserve(children.size());
            for (size_t i = 0; i < children.size(); ++i) {
                entries.emplace_back(Convert(children[i]));
            }
            return entries;
        }

        static NSQLComplete::TFolderEntry Convert(const NScheme::TSchemeEntry& entry) {
            return {
                .Type = Convert(entry.Type),
                .Name = TString(entry.Name),
            };
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
                case NScheme::ESchemeEntryType::SysView:
                    return "SysView";
                case NScheme::ESchemeEntryType::Unknown:
                default:
                    return "Unknown";
            }
        }

        TMaybe<NSQLComplete::TTableDetails> Convert(TString path, NTable::TDescribeTableResult result) const {
            if (!result.IsSuccess()) {
                if (IsVerbose_) {
                    Cerr << "DescribeTable('" << path << "') failed: "
                         << result.GetIssues().ToOneLineString() << Endl;
                }
                return Nothing();
            }

            NSQLComplete::TTableDetails details;
            for (TColumn column : result.GetTableDescription().GetColumns()) {
                details.Columns.emplace_back(std::move(column.Name));
            }
            return details;
        }

        TDriver Driver_;
        TString Database_;
        bool IsVerbose_;
    };

    NSQLComplete::ISimpleSchema::TPtr MakeYDBSchema(
        TDriver driver, TString database, bool isVerbose) {
        return new TYDBSchema(std::move(driver), std::move(database), isVerbose);
    }

} // namespace NYdb::NConsoleClient
