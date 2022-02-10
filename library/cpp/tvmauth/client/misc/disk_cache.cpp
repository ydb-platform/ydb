#include "disk_cache.h"

#include <library/cpp/tvmauth/client/logger.h>

#include <contrib/libs/openssl/include/openssl/evp.h>
#include <contrib/libs/openssl/include/openssl/hmac.h>
#include <contrib/libs/openssl/include/openssl/sha.h>

#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/system/fs.h>
#include <util/system/sysstat.h>
#include <util/system/tempfile.h> 

#include <exception>

namespace NTvmAuth {
    static const size_t HASH_SIZE = 32;
    static const size_t TIMESTAMP_SIZE = sizeof(time_t);

    TDiskReader::TDiskReader(const TString& filename, ILogger* logger)
        : Filename_(filename)
        , Logger_(logger)
    {
    }

    bool TDiskReader::Read() {
        TStringStream s;

        try {
            if (!NFs::Exists(Filename_)) {
                if (Logger_) {
                    s << "File '" << Filename_ << "' does not exist";
                    Logger_->Debug(s.Str());
                }
                return false;
            }

            TFile file(Filename_, OpenExisting | RdOnly | Seq);
            file.Flock(LOCK_SH | LOCK_NB);

            TFileInput input(file);
            return ParseData(input.ReadAll());
        } catch (const std::exception& e) {
            if (Logger_) {
                s << "Failed to read '" << Filename_ << "': " << e.what();
                Logger_->Error(s.Str());
            }
        }

        return false;
    }

    bool TDiskReader::ParseData(TStringBuf buf) {
        TStringStream s;

        if (buf.size() <= HASH_SIZE + TIMESTAMP_SIZE) {
            if (Logger_) {
                s << "File '" << Filename_ << "' is too small";
                Logger_->Warning(s.Str());
            }
            return false;
        }

        TStringBuf hash = buf.SubStr(0, HASH_SIZE);
        if (hash != GetHash(buf.Skip(HASH_SIZE))) {
            if (Logger_) {
                s << "Content of '" << Filename_ << "' was incorrectly changed";
                Logger_->Warning(s.Str());
            }
            return false;
        }

        Time_ = TInstant::Seconds(GetTimestamp(buf.substr(0, TIMESTAMP_SIZE)));
        Data_ = buf.Skip(TIMESTAMP_SIZE);

        if (Logger_) {
            s << "File '" << Filename_ << "' was successfully read";
            Logger_->Info(s.Str());
        }
        return true;
    }

    TString TDiskReader::GetHash(TStringBuf data) {
        TString value(EVP_MAX_MD_SIZE, 0);
        unsigned macLen = 0;
        if (!::HMAC(EVP_sha256(),
                    "",
                    0,
                    (unsigned char*)data.data(),
                    data.size(),
                    (unsigned char*)value.data(),
                    &macLen)) {
            return {};
        }

        if (macLen != EVP_MAX_MD_SIZE) {
            value.resize(macLen);
        }

        return value;
    }

    time_t TDiskReader::GetTimestamp(TStringBuf data) {
        time_t time = 0;
        for (int idx = TIMESTAMP_SIZE - 1; idx >= 0; --idx) {
            time <<= 8;
            time |= static_cast<unsigned char>(data.at(idx));
        }
        return time;
    }

    TDiskWriter::TDiskWriter(const TString& filename, ILogger* logger)
        : Filename_(filename)
        , Logger_(logger)
    {
    }

    bool TDiskWriter::Write(TStringBuf data, TInstant now) {
        TStringStream s;

        try {
            {
                if (NFs::Exists(Filename_)) {
                    Chmod(Filename_.c_str(),
                          S_IRUSR | S_IWUSR); // 600
                }

                TFile file(Filename_, CreateAlways | WrOnly | Seq | AWUser | ARUser);
                file.Flock(LOCK_EX | LOCK_NB);

                TFileOutput output(file);
                output << PrepareData(now, data);
            }

            if (Logger_) {
                s << "File '" << Filename_ << "' was successfully written";
                Logger_->Info(s.Str());
            }
            return true;
        } catch (const std::exception& e) {
            if (Logger_) {
                s << "Failed to write '" << Filename_ << "': " << e.what();
                Logger_->Error(s.Str());
            }
        }

        return false;
    }

    TString TDiskWriter::PrepareData(TInstant time, TStringBuf data) {
        TString toHash = WriteTimestamp(time.TimeT()) + data;
        return TDiskReader::GetHash(toHash) + toHash;
    }

    TString TDiskWriter::WriteTimestamp(time_t time) {
        TString res(TIMESTAMP_SIZE, 0);
        for (size_t idx = 0; idx < TIMESTAMP_SIZE; ++idx) {
            res[idx] = time & 0xFF;
            time >>= 8;
        }
        return res;
    }
}
