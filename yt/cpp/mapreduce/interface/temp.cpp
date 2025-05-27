#include "temp.h"
#include "errors.h"

#include <yt/cpp/mapreduce/interface/config.h>

#include <util/system/yassert.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TTempTable::TTempTable(
    IClientBasePtr client,
    const TString& prefix,
    const TYPath& directory,
    const TCreateOptions& options)
    : Client_(std::move(client))
{
    Name_ = CreateTempTable(Client_, prefix, directory, options);
}

TTempTable::TTempTable(TTempTable::TPrivateConstuctorTag, IClientBasePtr client, TYPath path, const TCreateOptions& options)
    : Client_(std::move(client))
    , Name_(std::move(path))
{
    Client_->Create(Name_, NT_TABLE, options);
}

TTempTable::TTempTable(TTempTable&& sourceTable)
    : Client_(sourceTable.Client_)
    , Name_(sourceTable.Name_)
    , Owns_(sourceTable.Owns_)
{
    sourceTable.Owns_ = false;
}

TTempTable& TTempTable::operator=(TTempTable&& sourceTable)
{
    if (&sourceTable == this) {
        return *this;
    }

    if (Owns_) {
        RemoveTable();
    }

    Client_ = sourceTable.Client_;
    Name_ = sourceTable.Name_;
    Owns_ = sourceTable.Owns_;

    sourceTable.Owns_ = false;

    return *this;
}

TTempTable TTempTable::CreateAutoremovingTable(IClientBasePtr client, TYPath path, const TCreateOptions& options)
{
    return TTempTable(TTempTable::TPrivateConstuctorTag(), std::move(client), std::move(path), options);
}

void TTempTable::RemoveTable()
{
    if (TConfig::Get()->KeepTempTables) {
        return;
    }
    Client_->Remove(Name_, TRemoveOptions().Force(true));
}

TTempTable::~TTempTable()
{
    if (Owns_) {
        try {
            RemoveTable();
        } catch (...) {
        }
    }
}

TString TTempTable::Name() const &
{
    return Name_;
}

TString TTempTable::Release()
{
    Y_ASSERT(Owns_);
    Owns_ = false;
    return Name_;
}

////////////////////////////////////////////////////////////////////////////////

TYPath CreateTempTable(
    const IClientBasePtr& client,
    const TString& prefix,
    const TYPath& directory,
    const TCreateOptions& options)
{
    TYPath result;
    bool createDirectory = false;

    if (directory) {
        result = directory;
    } else {
        result = TConfig::Get()->RemoteTempTablesDirectory;

        // User might override configuration above.
        // Class used to create directory in such cases if it doesn't exist.
        // We keep this behaviour.
        createDirectory = true;
    }

    if (result == DefaultRemoteTempTablesDirectory) {
        result += "/";
        result += client->GetParentClient()->WhoAmI().Login;
        createDirectory = true;
    }

    auto resultDirectory = result;

    result += "/";
    result += prefix;
    result += CreateGuidAsString();

    if (!createDirectory) {
        client->Create(result, NT_TABLE, options);
    } else {
        // Directory we are going to create our table in can be missing.
        // We create it explicitly outside of any transactions,
        // because concurrent processes might want to create the same directory from other transactions.
        //
        // It's unlikely but directory might be removed between directory creation and table creation
        // we retry attempt if it was failed with path resolution error.
        const int maxAttempts = 3;
        for (int i = 0; i < maxAttempts; ++i) {
            client->GetParentClient(/*ignoreGlobalTx=*/ true)->Create(resultDirectory, NT_MAP, TCreateOptions().Recursive(true).IgnoreExisting(true));

            try {
                client->Create(result, NT_TABLE, options);
                break;
            } catch (const TErrorResponse& error) {
                if (error.IsResolveError()) {
                    if (i < maxAttempts - 1) {
                        continue;
                    }
                }
                throw;
            }
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
