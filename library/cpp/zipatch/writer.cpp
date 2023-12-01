#include "writer.h"

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

#include <util/string/join.h>

#include <contrib/libs/libarchive/libarchive/archive.h>
#include <contrib/libs/libarchive/libarchive/archive_entry.h>

using namespace NJson;

namespace NZipatch {

class TWriter::TImpl {
public:
    TImpl(const TFsPath& path)
        : Actions_(new TJsonValue(JSON_ARRAY))
        , Meta_(new TJsonValue(JSON_MAP))
        , Revprops_(new TJsonValue(JSON_MAP))
        , Archive_(nullptr)
    {
        Archive_ = archive_write_new();
        if (!Archive_) {
            ythrow yexception() << "can't create archive object";
        }
        archive_write_set_format_zip(Archive_);
        archive_write_zip_set_compression_deflate(Archive_);

        if (ARCHIVE_OK != archive_write_open_filename(Archive_, TString(path).c_str())) {
            ythrow yexception() << "can't open archive path = " << path;
        }
    }

    ~TImpl() {
        if (Actions_ || Meta_ || Revprops_) {
            Finish();
        }
        if (Archive_) {
            archive_write_free(Archive_);
        }
    }

    void Finish() {
        if (Actions_) {
            if (Archive_) {
                WriteEntry("actions.json", WriteJson(Actions_.Get(), true, false));
            }

            Actions_.Destroy();
        }

        if (Meta_) {
            if (Archive_) {
                WriteEntry("meta.json", WriteJson(Meta_.Get(), true));
            }

            Meta_.Destroy();
        }

        if (Revprops_) {
            if (Archive_) {
                WriteEntry("revprops.json", WriteJson(Revprops_.Get(), true));
            }

            Revprops_.Destroy();
        }

        if (Archive_) {
            archive_write_close(Archive_);
        }
    }

    void Copy(const TString& path, const TOrigin& origin) {
        Y_ASSERT(origin.Path);
        Y_ASSERT(origin.Revision);

        if (Actions_) {
            TJsonValue item;
            item["type"] = "svn_copy";
            item["path"] = path;
            item["orig_path"] = origin.Path;
            item["orig_revision"] = origin.Revision;
            Actions_->AppendValue(item);
        }
    }

    void MkDir(const TString& path) {
        if (Actions_) {
            TJsonValue item;
            item["type"] = "mkdir";
            item["path"] = path;
            Actions_->AppendValue(item);
        }
    }

    void RemoveFile(const TString& path) {
        if (Actions_) {
            TJsonValue item;
            item["type"] = "remove_file";
            item["path"] = path;
            Actions_->AppendValue(item);
        }
    }

    void RemoveTree(const TString& path) {
        if (Actions_) {
            TJsonValue item;
            item["type"] = "remove_tree";
            item["path"] = path;
            Actions_->AppendValue(item);
        }
    }

    void StoreFile(
        const TString& path,
        const TString& data,
        const bool execute,
        const bool symlink,
        const TMaybe<bool> binaryHint,
        const TMaybe<bool> encrypted)
    {
        if (Actions_) {
            const TString file = Join("/", "files", path);
            TJsonValue item;
            item["type"] = "store_file";
            item["executable"] = execute;
            item["path"] = path;
            item["file"] = file;
            if (binaryHint.Defined()) {
                item["binary_hint"] = *binaryHint;
            }
            if (encrypted.Defined()) {
                item["encrypted"] = *encrypted;
            }
            Actions_->AppendValue(item);
            WriteEntry(file, data, symlink);
        }
    }

    void SetBaseSvnRevision(ui64 revision) {
        if (Meta_) {
            (*Meta_)["base_svn_revision"] = revision;
        }
    }

    void AddRevprop(const TString& prop, const TString& value) {
        if (Revprops_) {
            (*Revprops_)[prop] = value;
        }
    }

private:
    void WriteEntry(
        const TString& path,
        const TString& data,
        const bool symlink = false)
    {
        struct archive_entry* const entry = archive_entry_new();
        // Write header.
        archive_entry_set_pathname(entry, path.c_str());
        archive_entry_set_size(entry, data.size());
        archive_entry_set_filetype(entry, symlink ? AE_IFLNK : AE_IFREG);
        archive_entry_set_perm(entry, 0644);
        if (symlink) {
            archive_entry_set_symlink(entry, data.c_str());
        }
        archive_write_header(Archive_, entry);
        // Write data.
        // If entry is symlink then entry size become zero.
        if (archive_entry_size(entry) > 0) {
            archive_write_data(Archive_, data.data(), data.size());
        }
        archive_entry_free(entry);
    }

private:
    THolder<NJson::TJsonValue> Actions_;
    THolder<NJson::TJsonValue> Meta_;
    THolder<NJson::TJsonValue> Revprops_;
    struct archive* Archive_;
};

TWriter::TWriter(const TFsPath& path)
    : Impl_(new TImpl(path))
{
}

TWriter::~TWriter()
{ }

void TWriter::Finish() {
    Impl_->Finish();
}

void TWriter::SetBaseSvnRevision(ui64 revision) {
    Impl_->SetBaseSvnRevision(revision);
}

void TWriter::AddRevprop(const TString& prop, const TString& value) {
    Impl_->AddRevprop(prop, value);
}

void TWriter::Copy(const TString& path, const TOrigin& origin) {
    Impl_->Copy(path, origin);
}

void TWriter::MkDir(const TString& path) {
    Impl_->MkDir(path);
}

void TWriter::RemoveFile(const TString& path) {
    Impl_->RemoveFile(path);
}

void TWriter::RemoveTree(const TString& path) {
    Impl_->RemoveTree(path);
}

void TWriter::StoreFile(
    const TString& path,
    const TString& data,
    const bool execute,
    const bool symlink,
    const TMaybe<bool> binaryHint,
    const TMaybe<bool> encrypted)
{
    Impl_->StoreFile(path, data, execute, symlink, binaryHint, encrypted);
}

} // namespace NZipatch

