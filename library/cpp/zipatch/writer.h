#pragma once

#include <util/folder/path.h>
#include <util/generic/ptr.h>
#include <util/generic/maybe.h>

namespace NZipatch {

class TWriter {
public:
    struct TOrigin {
        TString Path;
        ui64 Revision;

        inline TOrigin(const TString& path, const ui64 revision)
            : Path(path)
            , Revision(revision)
        { }
    };

     TWriter(const TFsPath& path);
    ~TWriter();

    void Finish();

    void SetBaseSvnRevision(ui64 revision);

    void AddRevprop(const TString& prop, const TString& value);

    void Copy(const TString& path, const TOrigin& origin);

    void MkDir(const TString& path);

    void RemoveFile(const TString& path);

    void RemoveTree(const TString& path);

    void StoreFile(const TString& path,
                   const TString& data,
                   const bool execute,
                   const bool symlink,
                   const TMaybe<bool> binaryHint = Nothing(),
                   const TMaybe<bool> encrypted = Nothing());

private:
    class TImpl;
    THolder<TImpl> Impl_;
};

} // namespace NZipatch

