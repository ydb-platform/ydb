#pragma once

#include <ydb/public/lib/ydb_cli/common/sys.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

#include <util/folder/path.h>
#include <util/generic/deque.h>

namespace NYdb {

////////////////////////////////////////////////////////////////////////////////
// Traverse a directory in a database in DepthFirst order
////////////////////////////////////////////////////////////////////////////////

struct TSchemeEntryWithPath {
    NScheme::TSchemeEntry Entry;
    // Path relative to TraverseRoot
    TString RelParentPath;

    bool IsListed;

    TSchemeEntryWithPath(const NScheme::TSchemeEntry& entry, const TString& relParentPath, bool isListed)
      : Entry(entry)
      , RelParentPath(relParentPath)
      , IsListed(isListed)
    {}
};

enum class ETraverseType {
    Preordering,
    Postordering,
};

template<ETraverseType Ordering>
class TDbIterator {
private:
    NScheme::TSchemeClient Client;

    TString TraverseRoot;
    TDeque<TSchemeEntryWithPath> NextNodes;

public:
    TDbIterator(TDriver driver, const TString& fullPath)
      : Client(driver)
    {
        NScheme::TListDirectoryResult listResult = Client.ListDirectory(fullPath).GetValueSync();
        Y_ENSURE(listResult.IsSuccess(), "Can't list directory, maybe it doesn't exist, dbPath# "
                << fullPath.Quote());

        if (listResult.GetEntry().Type == NScheme::ESchemeEntryType::Table) {
            TPathSplitUnix parentPath(fullPath);
            parentPath.pop_back();
            TraverseRoot = parentPath.Reconstruct();
            NextNodes.emplace_front(listResult.GetEntry(), "", true);
        } else {
            TraverseRoot = fullPath;
            for (const auto& x : listResult.GetChildren()) {
                NextNodes.emplace_front(x, "", false);
            }

            while (NextNodes && IsSkipped()) {
                NextNodes.pop_front();
            }

            switch (Ordering) {
            case ETraverseType::Preordering: {
                break;
            }
            case ETraverseType::Postordering: {
                while (NextNodes && IsDir() && !IsListed()) {
                    const TString& fullPath = GetFullPath();
                    NScheme::TListDirectoryResult childList = Client.ListDirectory(fullPath).GetValueSync();
                    Y_ENSURE(childList.IsSuccess(), "Can't list directory, maybe it doesn't exist, dbPath# "
                            << fullPath.Quote());
                    NextNodes.front().IsListed = true;

                    const auto& children = childList.GetChildren();
                    if (!children) {
                        break;
                    }
                    const auto& currRelPath = GetRelPath();
                    for (const auto& x : children) {
                        NextNodes.emplace_front(x, currRelPath, false);
                    }

                    while (NextNodes && IsSkipped()) {
                        NextNodes.pop_front();
                    }
                }
                break;
            }
            default:
                Y_ABORT();
            }
        }
    }

    const NScheme::TSchemeEntry *GetCurrentNode() const {
        Y_ENSURE(NextNodes, "Empty TDbIterator dereference");
        return &NextNodes.front().Entry;
    }

    TString GetFullPath() const {
        Y_ENSURE(NextNodes, "Empty TDbIterator dereference");
        TPathSplitUnix path(TraverseRoot);
        path.AppendComponent(NextNodes.front().RelParentPath);
        path.AppendComponent(NextNodes.front().Entry.Name);
        return path.Reconstruct();
    }

    TString GetTraverseRoot() const {
        return TraverseRoot;
    }

    TString GetRelParentPath() const {
        Y_ENSURE(NextNodes, "Empty TDbIterator dereference");
        return NextNodes.front().RelParentPath;
    }

    TString GetRelPath() const {
        Y_ENSURE(NextNodes, "Empty TDbIterator dereference");
        TPathSplitUnix path(NextNodes.front().RelParentPath);
        path.AppendComponent(NextNodes.front().Entry.Name);
        return path.Reconstruct();
    }

    bool IsTable() const {
        return GetCurrentNode()->Type == NScheme::ESchemeEntryType::Table;
    }

    bool IsDir() const {
        return GetCurrentNode()->Type == NScheme::ESchemeEntryType::Directory;
    }

    bool IsListed() const {
        return NextNodes.front().IsListed;
    }

    explicit operator bool() const {
        return bool{NextNodes};
    }

    bool IsSkipped() const {
        return NConsoleClient::IsSystemObject(*GetCurrentNode());
    }

    void Next() {
        switch (Ordering) {
            case ETraverseType::Preordering: {
                if (IsDir()) {
                    NScheme::TListDirectoryResult listResult = Client.ListDirectory(GetFullPath()).GetValueSync();
                    Y_ENSURE(listResult.IsSuccess(), "Can't list directory, maybe it doesn't exist, dbPath# "
                            << GetFullPath().Quote());

                    for (const auto& x : listResult.GetChildren()) {
                        NextNodes.emplace_back(x, GetRelPath(), false);
                    }
                }
                NextNodes.pop_front();

                while (NextNodes && IsSkipped()) {
                    NextNodes.pop_front();
                }
                break;
            }
            case ETraverseType::Postordering: {
                if (!IsDir() || IsListed()) {
                    NextNodes.pop_front();
                }
                while (NextNodes && IsSkipped()) {
                    NextNodes.pop_front();
                }
                if (!NextNodes) {
                    return;
                }

                while (IsDir() && !IsListed()) {
                    const TString& fullPath = GetFullPath();
                    NScheme::TListDirectoryResult listResult = Client.ListDirectory(fullPath).GetValueSync();
                    Y_ENSURE(listResult.IsSuccess(), "Can't list directory, maybe it doesn't exist, dbPath# "
                            << fullPath.Quote());
                    const auto& currRelPath = GetRelPath();
                    NextNodes.front().IsListed = true;
                    for (const auto& x : listResult.GetChildren()) {
                        NextNodes.emplace_front(x, currRelPath, false);
                    }

                    while (NextNodes && IsSkipped()) {
                        NextNodes.pop_front();
                    }
                }
                break;
            }
            default:
                Y_ABORT();
        }
    }
};

}
