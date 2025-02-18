#include "encryption.h"

#include <ydb/core/backup/common/proto/encrypted_file.pb.h>

#include <util/generic/hash.h>
#include <util/generic/yexception.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/string/ascii.h>
#include <util/string/builder.h>
#include <util/string/hex.h>

// byte order functions
#if defined (_win_)
   #include <winsock2.h>
#elif defined (_unix_)
   #include <arpa/inet.h>
#endif

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

namespace NKikimr::NBackup {

namespace {

THashMap<TString, TString> AlgNames = {
    {"aes128gcm",        "AES-128-GCM"},
    {"aes256gcm",        "AES-256-GCM"},
    {"chacha20poly1305", "ChaCha20-Poly1305"},
};

TString NormalizeAlgName(const TString& name) {
    TString result;
    result.reserve(name.size());
    for (char c : name) {
        if (IsAsciiAlpha(c)) {
            result.push_back(AsciiToLower(c));
        } else if (c == '-' || c == '_') {
            // do nothing, allow user to write something like "AES128-GCM" or "AES_128-GCM"
        } else {
            result.push_back(c);
        }
    }
    const auto alg = AlgNames.find(result);
    if (alg == AlgNames.end()) {
        return {};
    } else {
        return alg->second; // normalized name
    }
}

const EVP_CIPHER* GetCipherByName(const TString& cipherName) {
    const EVP_CIPHER* cipher = EVP_get_cipherbyname(cipherName.c_str());
    if (!cipher) {
        throw yexception() << "Failed to get cipher \"" << cipherName << "\" by name";
    }
    return cipher;
}

class TOpenSslObjectFree {
public:
    TOpenSslObjectFree() = default;
    TOpenSslObjectFree(const TOpenSslObjectFree&) = default;
    TOpenSslObjectFree(TOpenSslObjectFree&&) = default;

    void operator()(EVP_CIPHER_CTX* ctx) const {
        EVP_CIPHER_CTX_free(ctx);
    }

    void operator()(BIO* bio) const {
        BIO_free(bio);
    }
};

using TCipherCtxPtr = std::unique_ptr<EVP_CIPHER_CTX, TOpenSslObjectFree>;
using TBioPtr = std::unique_ptr<BIO, TOpenSslObjectFree>;

TString GetLastOpenSslError() {
    TBioPtr bio(BIO_new(BIO_s_mem()));
    ERR_print_errors(bio.get());
    char* buf;
    size_t len = BIO_get_mem_data(bio.get(), &buf);
    TString ret(buf, len);
    return ret;
}

TString GetOpenSslErrorText(int errorCode) {
    TStringBuilder result;
    result << "Code " << errorCode;
    if (TString err = GetLastOpenSslError()) {
        result << ": " << err;
    }
    return std::move(result);
}

uint32_t ToNetworkByteOrder(uint32_t n) {
    return htonl(n);
}

uint16_t ToNetworkByteOrder(uint16_t n) {
    return htons(n);
}

// uint32_t ToHostByteOrder(uint32_t n) {
//     return ntohl(n);
// }

// uint16_t ToHostByteOrder(uint16_t n) {
//     return ntohs(n);
// }

} // anonymous

TEncryptionIV TEncryptionIV::Generate() {
    TEncryptionIV iv;
    iv.IV.resize(SIZE);
    if (int err = RAND_bytes(iv.IV.data(), static_cast<int>(SIZE)); err <= 0) {
        throw yexception() << "Failed to generate IV: " << GetOpenSslErrorText(err);
    }
    return iv;
}

TEncryptionIV TEncryptionIV::Combine(const TEncryptionIV& base, EBackupFileType fileType, uint32_t backupItemNumber, uint32_t shardNumber) {
    if (backupItemNumber >= MAX_BACKUP_ITEM_NUMBER) { // must suit to 3 bytes
        throw yexception() << "Backup item number must be less than " << MAX_BACKUP_ITEM_NUMBER;
    }

    TEncryptionIV iv = base;
    Y_VERIFY(iv.Size() == SIZE); // proper size

    static_assert(FILE_TYPE_SIZE == sizeof(fileType));
    static_assert(sizeof(iv.IV[FILE_TYPE_OFFSET]) == sizeof(fileType));
    iv.IV[FILE_TYPE_OFFSET] ^= static_cast<unsigned char>(fileType);

    static_assert(BACKUP_ITEM_SIZE == sizeof(uint32_t) - 1); // assume in the code here that it is sizeof(uint32_t) - 1
    static_assert(BACKUP_ITEM_OFFSET >= 1);
    *reinterpret_cast<uint32_t*>(&iv.IV[BACKUP_ITEM_OFFSET - 1]) ^= ToNetworkByteOrder(backupItemNumber); // first byte is always zero

    static_assert(SHARD_NUMBER_SIZE == sizeof(uint32_t));
    *reinterpret_cast<uint32_t*>(&iv.IV[SHARD_NUMBER_OFFSET]) ^= ToNetworkByteOrder(shardNumber);
    return iv;
}

TEncryptionIV TEncryptionIV::CombineForChunk(const TEncryptionIV& fileIV, uint32_t chunkNumber) {
    TEncryptionIV iv = fileIV;
    Y_VERIFY(iv.Size() == SIZE); // proper size
    static_assert(CHUNK_NUMBER_SIZE == sizeof(uint32_t));
    *reinterpret_cast<uint32_t*>(&iv.IV[CHUNK_NUMBER_OFFSET]) ^= ToNetworkByteOrder(chunkNumber);
    return iv;
}

TString TEncryptionIV::GetHexString() const {
    if (*this) {
        return HexEncode(IV.data(), IV.size());
    } else {
        return TString();
    }
}

TString TEncryptionIV::GetBinaryString() const {
    if (*this) {
        return TString(reinterpret_cast<const char*>(IV.data()), IV.size());
    } else {
        return TString();
    }
}

class TEncryptedFileSerializer::TImpl {
public:
    TImpl(TString algorithm, TEncryptionKey key, TEncryptionIV iv)
        : Algorithm(std::move(algorithm))
        , NormalizedAlgName(NormalizeAlgName(Algorithm))
        , Key(std::move(key))
        , IV(std::move(iv))
    {
        ResetCtx();
    }

    TBuffer AddBlock(TStringBuf data, bool last) {
        if (!data) {
            throw yexception() << "Empty data block";
        }

        TBuffer buffer;
        ReserveBufferSize(buffer, data, last);
        if (CurrentChunkNumber == 0) {
            WriteHeader(buffer);
            ResetCtxForNextChunk();
        }

        WriteBlock(buffer, data);
        ResetCtxForNextChunk();

        if (last) {
            WriteBlock(buffer, TStringBuf());
            Ctx.reset();
        }

        return buffer;
    }

    void ResetCtx() {
        if (!Algorithm) {
            throw yexception() << "No cipher algorithm specified";
        }

        if (!NormalizedAlgName) {
            throw yexception() << "Unknown cipher algorithm: \"" << Algorithm << "\"";
        }

        const EVP_CIPHER* cipher = GetCipherByName(NormalizedAlgName);

        // Check key length
        const int keyLength = EVP_CIPHER_key_length(cipher);
        if (static_cast<int>(Key.Size()) != keyLength) {
            throw yexception() << "Invalid key length " << Key.Size() << ". Expected: " << keyLength;
        }

        // Check IV length
        const int ivLength = EVP_CIPHER_iv_length(cipher);
        if (static_cast<int>(IV.Size()) != ivLength) {
            throw yexception() << "Invalid IV length " << IV.Size() << ". Expected: " << ivLength;
        }

        Ctx.reset(EVP_CIPHER_CTX_new());
        if (!Ctx) {
            throw yexception() << "Failed to allocate cypher context";
        }

        TEncryptionIV iv = TEncryptionIV::CombineForChunk(IV, CurrentChunkNumber);
        if (int err = EVP_EncryptInit_ex(Ctx.get(), GetCipherByName(NormalizedAlgName), nullptr, Key.Ptr(), iv.Ptr()); err <= 0) {
            throw yexception() << "Failed to init encryption algoritm: " << GetOpenSslErrorText(err);
        }

        // Start calculating MAC from the previous MAC
        if (CurrentChunkNumber > 0) {
            int outSize = 0;
            if (int err = EVP_EncryptUpdate(Ctx.get(), nullptr, &outSize, reinterpret_cast<const unsigned char*>(PreviousMAC), MAC_SIZE); err <= 0) {
                throw yexception() << "Failed to write unencrypted data: " << GetOpenSslErrorText(err);
            }
        }
    }

    void ResetCtxForNextChunk() {
        ++CurrentChunkNumber;
        ResetCtx();
    }

    void ReserveBufferSize(TBuffer& dst, TStringBuf data, bool last) {
        size_t size = data.size() + sizeof(uint32_t) + MAC_SIZE;
        if (CurrentChunkNumber == 0) {
            size += 100; // for header
        }
        if (last) {
            size += sizeof(uint32_t) + MAC_SIZE; // for empty last chunk
        }
        dst.Reserve(size);
    }

    void WriteUnencrypted(TBuffer& dst, const unsigned char* data, size_t size) {
        dst.Append(reinterpret_cast<const char*>(data), size);
        if (size) {
            int outSize = 0;
            if (int err = EVP_EncryptUpdate(Ctx.get(), nullptr, &outSize, data, static_cast<int>(size)); err <= 0) {
                throw yexception() << "Failed to write unencrypted data: " << GetOpenSslErrorText(err);
            }
        }
    }

    void WriteUnencrypted(TBuffer& dst, const char* data, size_t size) {
        WriteUnencrypted(dst, reinterpret_cast<const unsigned char*>(data), size);
    }

    void WriteUnencrypted(TBuffer& dst, TStringBuf data) {
        WriteUnencrypted(dst, data.data(), data.size());
    }

    void WriteEncrypted(TBuffer& dst, const unsigned char* data, size_t size) {
        if (size) {
            if (dst.Avail() < size) {
                dst.Reserve(dst.Size() + size);
            }
            int bufferSize = static_cast<int>(dst.Avail());
            Y_VERIFY(bufferSize >= static_cast<int>(size));

            if (int err = EVP_EncryptUpdate(Ctx.get(), reinterpret_cast<unsigned char*>(dst.Data() + dst.Size()), &bufferSize, data, static_cast<int>(size)); err <= 0) {
                throw yexception() << "Failed to write unencrypted data: " << GetOpenSslErrorText(err);
            }
            dst.Advance(static_cast<size_t>(bufferSize));
        }
    }

    void WriteEncrypted(TBuffer& dst, const char* data, size_t size) {
        WriteEncrypted(dst, reinterpret_cast<const unsigned char*>(data), size);
    }

    void WriteEncrypted(TBuffer& dst, TStringBuf data) {
        WriteEncrypted(dst, data.data(), data.size());
    }

    void FinalizeAndWriteMAC(TBuffer& dst) {
        // Finalize
        int bufferSize = static_cast<int>(dst.Avail());
        if (int err = EVP_EncryptFinal_ex(Ctx.get(), reinterpret_cast<unsigned char*>(dst.Data() + dst.Size()), &bufferSize); err <= 0) {
            throw yexception() << "Failed to finalize encryption: " << GetOpenSslErrorText(err);
        }
        dst.Advance(static_cast<size_t>(bufferSize));

        // Write MAC
        if (int err = EVP_CIPHER_CTX_ctrl(Ctx.get(), EVP_CTRL_AEAD_GET_TAG, MAC_SIZE, PreviousMAC); err <= 0) {
            throw yexception() << "Failed to get MAC: " << GetOpenSslErrorText(err);
        }
        dst.Append(PreviousMAC, MAC_SIZE);
    }

    template <class T>
    void WriteSize(TBuffer& dst, T size) {
        size = ToNetworkByteOrder(size);
        WriteUnencrypted(dst, reinterpret_cast<const unsigned char*>(&size), sizeof(size));
    }

    void WriteHeader(TBuffer& dst) {
        TExportEncryptedFileHeader header;
        header.SetVersion(1);
        header.SetEncryptionAlgorithm(NormalizedAlgName);
        header.SetIV(IV.GetBinaryString());
        TString serialized;
        if (!header.SerializeToString(&serialized)) {
            throw yexception() << "Failed to write header";
        }

        Y_VERIFY(serialized.size() < (2 << 16));
        WriteSize(dst, static_cast<uint16_t>(serialized.size()));
        WriteUnencrypted(dst, serialized);
        FinalizeAndWriteMAC(dst);
    }

    void WriteBlock(TBuffer& dst, TStringBuf data) {
        WriteSize(dst, static_cast<uint32_t>(data.size()));
        WriteEncrypted(dst, data);
        FinalizeAndWriteMAC(dst);
    }

private:
    TString Algorithm;
    TString NormalizedAlgName;
    TEncryptionKey Key;
    TEncryptionIV IV;
    TCipherCtxPtr Ctx;
    uint32_t CurrentChunkNumber = 0; // 0 for header

    static constexpr size_t MAC_SIZE = 16;
    char PreviousMAC[MAC_SIZE] = {};
};

TEncryptedFileSerializer::TEncryptedFileSerializer(TString algorithm, TEncryptionKey key, TEncryptionIV iv)
    : Impl(new TImpl(std::move(algorithm), std::move(key), std::move(iv)))
{
}

TEncryptedFileSerializer::~TEncryptedFileSerializer() = default;

TBuffer TEncryptedFileSerializer::AddBlock(TStringBuf data, bool last) {
    return Impl->AddBlock(data, last);
}

TBuffer TEncryptedFileSerializer::EncryptFile(TString algorithm, TEncryptionKey key, TEncryptionIV iv, TStringBuf data) {
    TEncryptedFileSerializer serializer(std::move(algorithm), std::move(key), std::move(iv));
    return serializer.AddBlock(data, true);
}

} // NKikimr::NBackup
