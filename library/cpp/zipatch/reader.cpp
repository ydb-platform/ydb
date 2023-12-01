#include "reader.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>

#include <util/generic/hash.h>
#include <util/memory/tempbuf.h>

#include <contrib/libs/libarchive/libarchive/archive.h>
#include <contrib/libs/libarchive/libarchive/archive_entry.h>

using namespace NJson;

namespace NZipatch {

class TReader::TImpl {

    using TEntry = archive_entry;

public:
    TImpl() {
        if ((Archive_ = archive_read_new()) == nullptr) {
            ythrow yexception() << "can't create archive object";
        }
    }

    TImpl(const TFsPath& path)
        : TImpl()
    {
        archive_read_support_filter_all(Archive_);
        archive_read_support_format_zip(Archive_);

        if (ARCHIVE_OK != archive_read_open_filename(Archive_, TString(path).c_str(), 10240)) {
            ythrow yexception() << "can't open archive path = " << path;
        }

        Read();
    }

    TImpl(const TStringBuf buf)
        : TImpl()
    {
        archive_read_support_filter_all(Archive_);
        archive_read_support_format_zip(Archive_);

        if (ARCHIVE_OK != archive_read_open_memory(Archive_, buf.data(), buf.size())) {
            ythrow yexception() << "can't open in-memory archive";
        }

        Read();
    }

    ~TImpl() {
        for (const auto& item : Files_) {
            archive_entry_free(item.second.first);
        }
        if (Archive_) {
            archive_read_free(Archive_);
        }
    }

    void Enumerate(TOnEvent cb) const {
        for (const auto& item : Actions_) {
            TEvent event;

            event.Action = GetTypeFromString(item["type"].GetStringSafe(TString()));
            event.Path = item["path"].GetStringSafe(TString());
            event.Executable = item["executable"].GetBooleanSafe(false);
            event.Symlink = false;

            if (event.Action == Copy || event.Action == Move) {
                event.Source.Path = item["orig_path"].GetStringSafe(TString());
                event.Source.Revision = item["orig_revision"].GetUIntegerRobust();
            }
            if (event.Action == StoreFile) {
                auto fi = Files_.find(event.Path);
                if (fi == Files_.end()) {
                    ythrow yexception() << "can't find file; path = " << event.Path;
                }

                event.Data = fi->second.second;
                event.Symlink = archive_entry_filetype(fi->second.first) == AE_IFLNK;
            }

            if (event.Path) {
                cb(event);
            }
        }
    }

private:
    EAction GetTypeFromString(const TString& type) const {
        if (type == "store_file") {
            return StoreFile;
        }
        if (type == "mkdir") {
            return MkDir;
        }
        if (type == "remove_file" || type == "remove_tree") {
            return Remove;
        }
        if (type == "svn_copy") {
            return Copy;
        }
        return Unknown;
    }

    void Read() {
        TEntry* current = nullptr;

        while (archive_read_next_header(Archive_, &current) == ARCHIVE_OK) {
            const TStringBuf path(archive_entry_pathname(current));

            if (path == "actions.json") {
                TJsonValue value;
                ReadJsonFastTree(GetData(current), &value, true);

                for (const auto& item : value.GetArraySafe()) {
                    Actions_.push_back(item);
                }
            } else if (AsciiHasPrefix(path, "files/")) {
                TEntry* entry = archive_entry_clone(current);

                Files_.emplace(path.substr(6), std::make_pair(entry, GetData(current)));
            }
        }

        archive_read_close(Archive_);
    }

    TString GetData(TEntry* current) const {
        if (archive_entry_filetype(current) == AE_IFLNK) {
            return archive_entry_symlink(current);
        }

        if (const auto size = archive_entry_size(current)) {
            TTempBuf data(size);

            if (archive_read_data(Archive_, data.Data(), size) != size) {
                ythrow yexception() << "can't read entry";
            }

            return TString(data.Data(), size);
        }

        return TString();
    }

private:
    struct archive* Archive_;
    TVector<TJsonValue> Actions_;
    THashMap<TString, std::pair<TEntry*, TString>> Files_;
};

TReader::TReader(const TFsPath& path)
    : Impl_(new TImpl(path))
{
}

TReader::TReader(const TStringBuf buf)
    : Impl_(new TImpl(buf))
{
}

TReader::~TReader()
{ }

void TReader::Enumerate(TOnEvent cb) const {
    Impl_->Enumerate(cb);
}

} // namespace NZipatch

