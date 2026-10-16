#include "format.h"

#include <contrib/libs/zlib/zlib.h>

#include <util/generic/yexception.h>

namespace NKikimr::NBackup::NYt {

    class TFirstKeyProbe::TImpl {
    public:
        TImpl(const TSchema& schema, size_t maxBytes)
            : MaxBytes_(maxBytes)
            , Keys_(schema.KeyColumns)
            , Decoder_(schema, [this](const TDecodedRow& row, TString&) {
                if (Key_.empty()) {
                    TVector<TCell> keys;
                    for (const auto index : Keys_) {
                        keys.push_back(row.Cells[index]);
                    }
                    Key_ = TSerializedCellVec::Serialize(keys);
                }
                // Stop parsing as soon as the first complete row has provided its key.
                return false;
            }, maxBytes)
        {
            if (inflateInit2(&Stream_, 16 + MAX_WBITS) != Z_OK) {
                ythrow yexception() << "Cannot initialize YT gzip first-key probe";
            }
        }

        ~TImpl() {
            inflateEnd(&Stream_);
        }

        bool Feed(TStringBuf compressed, bool last, TString& error) {
            if (HasKey()) {
                return true;
            }
            if (compressed.size() > MaxBytes_ - CompressedBytes_) {
                error = "YT first row exceeds bounded source read";
                return false;
            }
            CompressedBytes_ += compressed.size();
            Stream_.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(compressed.data()));
            Stream_.avail_in = compressed.size();
            bool drain = false;
            while (Stream_.avail_in || drain) {
                char output[64 * 1024];
                Stream_.next_out = reinterpret_cast<Bytef*>(output);
                const size_t capacity = Min<size_t>(sizeof(output), MaxBytes_ - DecodedBytes_);
                if (!capacity) {
                    error = "YT first row exceeds decoded size limit";
                    return false;
                }
                Stream_.avail_out = capacity;
                const auto inputBefore = Stream_.avail_in;
                const int status = inflate(&Stream_, Z_NO_FLUSH);
                const size_t size = capacity - Stream_.avail_out;
                if (size > MaxBytes_ - DecodedBytes_) {
                    error = "YT first row exceeds decoded size limit";
                    return false;
                }
                DecodedBytes_ += size;
                if (status != Z_OK && status != Z_STREAM_END && status != Z_BUF_ERROR) {
                    error = "Invalid YT gzip first row";
                    return false;
                }
                const bool decoded = Decoder_.Feed(TStringBuf(output, size), error);
                if (HasKey()) {
                    error.clear();
                    return true;
                }
                if (!decoded) {
                    return false;
                }
                if (status == Z_STREAM_END) {
                    if (!Decoder_.Finish(error)) {
                        return false;
                    }
                    error = "YT part contains no rows";
                    return false;
                }
                drain = !Stream_.avail_out;
                if (!size && Stream_.avail_in == inputBefore) {
                    break;
                }
            }
            if (last) {
                error = "YT first row gzip stream is truncated";
                return false;
            }
            return true;
        }

        bool HasKey() const {
            return !Key_.empty();
        }

        const TString& GetKey() const {
            return Key_;
        }

    private:
        z_stream Stream_ = {};
        size_t MaxBytes_;
        size_t CompressedBytes_ = 0;
        size_t DecodedBytes_ = 0;
        TVector<ui32> Keys_;
        TRowDecoder Decoder_;
        TString Key_;
    };

    TFirstKeyProbe::TFirstKeyProbe(const TSchema& schema, size_t maxBytes)
        : Impl_(MakeHolder<TImpl>(schema, maxBytes))
    {
    }

    TFirstKeyProbe::~TFirstKeyProbe() = default;

    bool TFirstKeyProbe::Feed(TStringBuf compressed, bool last, TString& error) {
        return Impl_->Feed(compressed, last, error);
    }

    bool TFirstKeyProbe::HasKey() const {
        return Impl_->HasKey();
    }

    const TString& TFirstKeyProbe::GetKey() const {
        return Impl_->GetKey();
    }

} // namespace NKikimr::NBackup::NYt
