#include "encryption.h"

#include <ydb/core/backup/common/proto/encrypted_file.pb.h>

#include <util/generic/hash.h>
#include <util/generic/yexception.h>
#include <util/generic/size_literals.h>
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

#include <deque>

namespace NKikimr::NBackup {

namespace {

static constexpr size_t MAC_SIZE = 16;
static constexpr size_t MAX_HEADER_SIZE = 16_KB; // Header does not contain much data
static constexpr size_t MAX_BLOCK_SIZE = 30_MB; // Max block size must always be at least size of table row (~8 MB) serialized into text csv format.

THashMap<TString, TString> AlgNames = {
    {"aes128gcm", "AES-128-GCM"},
    {"aes256gcm", "AES-256-GCM"},
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

uint32_t ToHostByteOrder(uint32_t n) {
    return ntohl(n);
}

uint16_t ToHostByteOrder(uint16_t n) {
    return ntohs(n);
}

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

TEncryptionIV TEncryptionIV::FromBinaryString(const TString& s) {
    TEncryptionIV iv;
    iv.IV.assign(reinterpret_cast<const unsigned char*>(s.data()), reinterpret_cast<const unsigned char*>(s.data() + s.size()));
    return iv;
}

TString TEncryptionKey::GetBinaryString() const {
    if (*this) {
        return TString(reinterpret_cast<const char*>(Key.data()), Key.size());
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
        TBuffer buffer;
        ReserveBufferSize(buffer, data, last);
        if (CurrentChunkNumber == 0) {
            WriteHeader(buffer);
            ResetCtxForNextChunk();
        }

        if (data) {
            WriteBlock(buffer, data);
            ResetCtxForNextChunk();
        }

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
        if (!cipher) {
            throw yexception() << "Algorith \"" << NormalizedAlgName << "\" was not found";
        }

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
        if (int err = EVP_EncryptInit_ex(Ctx.get(), cipher, nullptr, Key.Ptr(), iv.Ptr()); err <= 0) {
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

TBuffer TEncryptedFileSerializer::EncryptFullFile(TString algorithm, TEncryptionKey key, TEncryptionIV iv, TStringBuf data) {
    TEncryptedFileSerializer serializer(std::move(algorithm), std::move(key), std::move(iv));
    return serializer.AddBlock(data, true);
}


class TEncryptedFileDeserializer::TImpl {
public:
    TImpl() = default;
    TImpl(TEncryptionKey key, TEncryptionIV expectedIV)
        : Key(std::move(key))
        , IV(std::move(expectedIV))
    {
    }

    void AddData(TBuffer data, bool last) {
        if (Finished) {
            throw yexception() << "Stream finished";
        }
        Finished = last;

        if (data) {
            InputBytes += data.Size();
            InputData.emplace_back(std::move(data));
        }
    }

    size_t GetAvailableBytes() const {
        return InputBytes - CurrentBufferPos;
    }

    const TBuffer& GetCurrentBuffer() const {
        return InputData.front();
    }

    TBuffer& GetCurrentBuffer() {
        return InputData.front();
    }

    const char* GetCurrentBufferData() const {
        return GetCurrentBuffer().Data() + CurrentBufferPos;
    }

    size_t GetCurrentBufferAvailableBytes() const {
        return GetCurrentBuffer().Size() - CurrentBufferPos;
    }

    void PopCurrentBuffer() {
        InputBytes -= GetCurrentBuffer().Size();
        InputData.pop_front();
        CurrentBufferPos = 0;
    }

    bool ReadUnencrypted(char* data, size_t size, size_t skip = 0, bool addToMACCalculation = true) {
        TStringBuf buf(data, size);
        while (!InputData.empty() && (size || skip)) {
            if (skip) {
                size_t toSkip = Min(skip, GetCurrentBufferAvailableBytes());
                skip -= toSkip;
                CurrentBufferPos += toSkip;
                BytesProcessed += toSkip;
                if (addToMACCalculation) {
                    CalcMACOnNonencryptedData(TStringBuf(GetCurrentBufferData(), toSkip));
                }
                if (!GetCurrentBufferAvailableBytes()) {
                    PopCurrentBuffer();
                    continue;
                }
            }

            size_t toCopy = Min(size, GetCurrentBufferAvailableBytes());
            memcpy(data, GetCurrentBufferData(), toCopy);
            CurrentBufferPos += toCopy;
            size -= toCopy;
            data += toCopy;
            if (!GetCurrentBufferAvailableBytes()) {
                PopCurrentBuffer();
            }
        }
        if (addToMACCalculation) {
            CalcMACOnNonencryptedData(buf);
        }
        BytesProcessed += buf.size();
        return size == 0 && skip == 0;
    }

    bool ReadEncrypted(char* data, size_t size, size_t skip = 0) {
        while (!InputData.empty() && (size || skip)) {
            if (skip) {
                size_t toSkip = Min(skip, GetCurrentBufferAvailableBytes());
                skip -= toSkip;
                CurrentBufferPos += toSkip;
                BytesProcessed += toSkip;
                if (!GetCurrentBufferAvailableBytes()) {
                    PopCurrentBuffer();
                    continue;
                }
            }

            size_t toDecrypt = Min(size, GetCurrentBufferAvailableBytes());
            int outSize = static_cast<int>(toDecrypt);
            if (int err = EVP_DecryptUpdate(Ctx.get(), reinterpret_cast<unsigned char*>(data), &outSize, reinterpret_cast<const unsigned char*>(GetCurrentBufferData()), toDecrypt); err <= 0) {
                ThrowFileIsCorrupted();
            }
            Y_VERIFY(static_cast<size_t>(outSize) == toDecrypt);
            CurrentBufferPos += static_cast<size_t>(outSize);
            size -= static_cast<size_t>(outSize);
            data += static_cast<size_t>(outSize);
            BytesProcessed += toDecrypt;
            if (!GetCurrentBufferAvailableBytes()) {
                PopCurrentBuffer();
            }
        }
        return size == 0 && skip == 0;
    }

    bool ReadEncrypted(TBuffer& dst, size_t size, size_t skip = 0) {
        dst.Reserve(dst.Size() + size);
        if (ReadEncrypted(dst.Data() + dst.Size(), size, skip)) {
            dst.Advance(size);
            return true;
        }
        return false;
    }

    // Reads, but does not move position.
    // Used to read sizes and then check if data is available.
    bool Peek(char* data, size_t size) {
        if (InputData.empty()) {
            return false;
        }
        size_t outPos = 0;
        size_t inPos = CurrentBufferPos;
        for (size_t i = 0; i < InputData.size(); ++i) {
            const char* curBufferData = InputData[i].Data();
            size_t curBufferSize = InputData[i].Size();
            while (outPos < size && inPos < curBufferSize) {
                data[outPos++] = curBufferData[inPos++];
            }
            if (outPos == size) {
                return true;
            }
            inPos = 0;
        }
        return false;
    }

    bool Peek(uint16_t& n) {
        if (Peek(reinterpret_cast<char*>(&n), sizeof(n))) {
            n = ToHostByteOrder(n);
            return true;
        }
        return false;
    }

    bool Peek(uint32_t& n) {
        if (Peek(reinterpret_cast<char*>(&n), sizeof(n))) {
            n = ToHostByteOrder(n);
            return true;
        }
        return false;
    }

    bool TryReadHeader() {
        uint16_t headerSize;
        if (!Peek(headerSize)) {
            return false;
        }
        if (headerSize > MAX_HEADER_SIZE) {
            ThrowFileIsCorrupted();
        }
        if (GetAvailableBytes() < sizeof(headerSize) + headerSize + MAC_SIZE) {
            if (Finished) {
                ThrowFileIsCorrupted();
            }
            return false;
        }
        std::string serializedHeader;
        serializedHeader.resize(headerSize);
        if (!ReadUnencrypted(serializedHeader.data(), headerSize, sizeof(headerSize) /* skip */, false)) {
            throw yexception() << "Failed to read header"; // Failed, but we have checked that we can read
        }

        TExportEncryptedFileHeader header;
        if (!header.ParseFromString(serializedHeader)) {
            ThrowFileIsCorrupted();
        }
        if (header.GetVersion() != 1) {
            ThrowFileIsCorrupted();
        }

        TEncryptionIV iv = TEncryptionIV::FromBinaryString(header.GetIV());
        if (IV && iv != IV) {
            ThrowFileIsCorrupted();
        }
        if (iv.Size() != TEncryptionIV::SIZE) {
            ThrowFileIsCorrupted();
        }
        IV = std::move(iv);
        NormalizedAlgName = NormalizeAlgName(header.GetEncryptionAlgorithm());
        if (!NormalizedAlgName) {
            ThrowFileIsCorrupted();
        }

        ResetCtx();

        // Check header MAC
        {
            CalcMACOnNonencryptedData(headerSize);
            CalcMACOnNonencryptedData(serializedHeader);
            TBuffer buf;
            FinalizeAndCheckMAC(buf);
        }

        HeaderWasRead = true;

        ResetCtxForNextChunk();
        return true;
    }

    void ResetCtx() {
        const EVP_CIPHER* cipher = GetCipherByName(NormalizedAlgName);
        if (!cipher) {
            ThrowFileIsCorrupted();
        }

        // Check key length
        const int keyLength = EVP_CIPHER_key_length(cipher);
        if (static_cast<int>(Key.Size()) != keyLength) {
            throw yexception() << "Invalid key length " << Key.Size() << ". Expected: " << keyLength;
        }

        // Check IV length
        const int ivLength = EVP_CIPHER_iv_length(cipher);
        if (static_cast<int>(IV.Size()) != ivLength) {
            ThrowFileIsCorrupted();
        }

        Ctx.reset(EVP_CIPHER_CTX_new());
        if (!Ctx) {
            throw yexception() << "Failed to allocate cypher context";
        }

        TEncryptionIV iv = TEncryptionIV::CombineForChunk(IV, CurrentChunkNumber);
        if (int err = EVP_DecryptInit_ex(Ctx.get(), cipher, nullptr, Key.Ptr(), iv.Ptr()); err <= 0) {
            throw yexception() << "Failed to init decryption algoritm: " << GetOpenSslErrorText(err);
        }

        // Start calculating MAC from the previous MAC
        if (CurrentChunkNumber > 0) {
            CalcMACOnNonencryptedData(TStringBuf(PreviousMAC, MAC_SIZE));
        }
    }

    void ResetCtxForNextChunk() {
        ++CurrentChunkNumber;
        ResetCtx();
    }

    void CalcMACOnNonencryptedData(TStringBuf data) {
        int outSize = 0;
        if (int err = EVP_DecryptUpdate(Ctx.get(), nullptr, &outSize, reinterpret_cast<const unsigned char*>(data.data()), data.size()); err <= 0) {
            ThrowFileIsCorrupted();
        }
    }

    void CalcMACOnNonencryptedData(uint16_t size) {
        size = ToNetworkByteOrder(size);
        TStringBuf data(reinterpret_cast<const char*>(&size), sizeof(size));
        CalcMACOnNonencryptedData(data);
    }

    void CalcMACOnNonencryptedData(uint32_t size) {
        size = ToNetworkByteOrder(size);
        TStringBuf data(reinterpret_cast<const char*>(&size), sizeof(size));
        CalcMACOnNonencryptedData(data);
    }

    void ReadMAC() {
        if (!ReadUnencrypted(PreviousMAC, MAC_SIZE, 0, false)) {
            ThrowFileIsCorrupted();
        }
        if (int err = EVP_CIPHER_CTX_ctrl(Ctx.get(), EVP_CTRL_AEAD_SET_TAG, static_cast<int>(MAC_SIZE), (void*)PreviousMAC); err <= 0) {
            ThrowFileIsCorrupted();
        }
    }

    void FinalizeAndCheckMAC(TBuffer& dst) {
        ReadMAC();
        int outLen = dst.Avail();
        if (int err = EVP_DecryptFinal_ex(Ctx.get(), reinterpret_cast<unsigned char*>(dst.Data() + dst.Size()), &outLen); err <= 0) {
            ThrowFileIsCorrupted();
        }
        dst.Advance(outLen);
    }

    bool TryReadBlock(TBuffer& dst) {
        if (DecryptedLastBlock) {
            return false;
        }

        uint32_t size;
        if (!Peek(size)) {
            return false;
        }
        if (size > MAX_BLOCK_SIZE) {
            ThrowFileIsCorrupted();
        }
        if (GetAvailableBytes() < size + sizeof(size) + MAC_SIZE) {
            if (Finished) {
                ThrowFileIsCorrupted();
            }
            return false;
        }
        CalcMACOnNonencryptedData(size);
        if (!ReadEncrypted(dst, size, sizeof(size))) {
            ThrowFileIsCorrupted();
        }
        FinalizeAndCheckMAC(dst);
        if (!size) {
            DecryptedLastBlock = true;
            Ctx.reset();
        } else {
            ResetCtxForNextChunk();
        }
        return true;
    }

    TMaybe<TBuffer> GetNextBlock() {
        if (!HeaderWasRead && !TryReadHeader()) {
            return Nothing();
        }
        if (DecryptedLastBlock) {
            if (GetAvailableBytes()) {
                ThrowFileIsCorrupted();
            }
            return Nothing();
        }
        TBuffer dst;
        dst.Reserve(GetAvailableBytes());
        while (TryReadBlock(dst));
        if (DecryptedLastBlock && GetAvailableBytes()) {
            ThrowFileIsCorrupted();
        }
        if (Finished && !DecryptedLastBlock) {
            ThrowFileIsCorrupted();
        }
        if (dst || Finished && CurrentChunkNumber == 1) { // If the only empty chunk, then return empty data
            return std::move(dst);
        } else {
            return Nothing();
        }
    }

    static void ThrowFileIsCorrupted() {
        throw yexception() << "File is corrupted";
    }

    TString GetState() const {
        TEncryptedFileDeserializerState state;
        state.SetEncryptionAlgorithm(NormalizedAlgName);
        state.SetKey(Key.GetBinaryString());
        state.SetIV(IV.GetBinaryString());
        state.SetCurrentChunkNumber(CurrentChunkNumber);
        state.SetBytesProcessed(BytesProcessed);
        state.SetPreviousMAC(TString(PreviousMAC, MAC_SIZE));
        state.SetHeaderWasRead(HeaderWasRead);
        state.SetDecryptedLastBlock(DecryptedLastBlock);
        state.SetFinished(Finished);

        TString result;
        if (!state.SerializeToString(&result)) {
            throw yexception() << "Failed to save state";
        }
        return result;
    }

    void RestoreFromState(const TString& serializedState) {
        TEncryptedFileDeserializerState state;
        if (!state.ParseFromString(serializedState)) {
            throw yexception() << "Failed to restore state";
        }
        NormalizedAlgName = state.GetEncryptionAlgorithm();
        Key = TEncryptionKey(state.GetKey());
        IV = TEncryptionIV::FromBinaryString(state.GetIV());
        CurrentChunkNumber = state.GetCurrentChunkNumber();
        BytesProcessed = state.GetBytesProcessed();
        memcpy(PreviousMAC, state.GetPreviousMAC().data(), Min(MAC_SIZE, state.GetPreviousMAC().size()));
        HeaderWasRead = state.GetHeaderWasRead();
        DecryptedLastBlock = state.GetDecryptedLastBlock();
        Finished = state.GetFinished();

        if (HeaderWasRead) {
            ResetCtx();
        }
    }

    TEncryptionIV GetIV() const {
        return IV;
    }

    size_t GetProcessedInputBytes() const {
        return BytesProcessed;
    }

private:
    TEncryptionKey Key;
    TEncryptionIV IV;
    TString NormalizedAlgName;
    TCipherCtxPtr Ctx;
    uint32_t CurrentChunkNumber = 0; // 0 for header

    std::deque<TBuffer> InputData;
    size_t InputBytes = 0;
    size_t BytesProcessed = 0;
    size_t CurrentBufferPos = 0;
    bool Finished = false;
    bool DecryptedLastBlock = false;
    bool HeaderWasRead = false;
    char PreviousMAC[MAC_SIZE] = {};
};

TEncryptedFileDeserializer::TEncryptedFileDeserializer(TEncryptionKey key)
    : Impl(new TImpl(std::move(key), TEncryptionIV()))
{
}

TEncryptedFileDeserializer::TEncryptedFileDeserializer(TEncryptionKey key, TEncryptionIV expectedIV)
    : Impl(new TImpl(std::move(key), std::move(expectedIV)))
{
}

TEncryptedFileDeserializer::TEncryptedFileDeserializer()
    : Impl(new TImpl())
{
}

TEncryptedFileDeserializer::~TEncryptedFileDeserializer() = default;

void TEncryptedFileDeserializer::AddData(TBuffer data, bool last) {
    Impl->AddData(std::move(data), last);
}

TMaybe<TBuffer> TEncryptedFileDeserializer::GetNextBlock() {
    return Impl->GetNextBlock();
}

TString TEncryptedFileDeserializer::GetState() const {
    return Impl->GetState();
}

TEncryptedFileDeserializer TEncryptedFileDeserializer::RestoreFromState(const TString& state) {
    TEncryptedFileDeserializer deserializer;
    deserializer.Impl->RestoreFromState(state);
    return deserializer;
}

std::pair<TBuffer, TEncryptionIV> TEncryptedFileDeserializer::DecryptFullFile(TEncryptionKey key, TBuffer data) {
    TEncryptedFileDeserializer deserializer(std::move(key));
    deserializer.AddData(std::move(data), true);

    TMaybe<TBuffer> result = deserializer.GetNextBlock();
    Y_VERIFY(result);
    return {std::move(*result), deserializer.GetIV()};
}

TBuffer TEncryptedFileDeserializer::DecryptFullFile(TEncryptionKey key, TEncryptionIV expectedIV, TBuffer data) {
    TEncryptedFileDeserializer deserializer(std::move(key), std::move(expectedIV));
    deserializer.AddData(std::move(data), true);

    TMaybe<TBuffer> result = deserializer.GetNextBlock();
    Y_VERIFY(result);
    return std::move(*result);
}

TEncryptionIV TEncryptedFileDeserializer::GetIV() const {
    return Impl->GetIV();
}

size_t TEncryptedFileDeserializer::GetProcessedInputBytes() const {
    return Impl->GetProcessedInputBytes();
}

} // NKikimr::NBackup
