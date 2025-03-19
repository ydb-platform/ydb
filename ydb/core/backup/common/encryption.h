#pragma once

#include <ydb/library/yverify_stream/yverify_stream.h>

#include <util/generic/buffer.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

#include <vector>

namespace NKikimr::NBackup {

// Backup file type.
// Must be different for all files in one backup item folder.
// Must be one byte size.
enum class EBackupFileType : unsigned char {
    // All items
    Metadata = 0,
    Permissions = 1,

    // Table
    TableSchema = 10,
    TableData = 11,

    // Topic
    TopicDescription = 10,
    TopicCreate = 11,

    // Coordination node
    CoordinationNodeCreate = 10,
    CoordinationNodeCreateRateLimiter = 11,

    // Incomplete
    Incomplete = 10,
    IncompleteCsv = 11,

    // Directory
    DirectoryEmpty = 10,

    // View
    ViewCreate = 10,

    // Database
    Database = 10,

    // User
    UserCreate = 10,

    // Group
    GroupCreate = 10,
    GroupAlter = 11,

    // Replication
    AsyncReplicationCreate = 10,

    // External data source
    ExternalDataSourceCreate = 10,

    // External table
    ExternalTableCreate = 10,
};

struct TEncryptionIV {
    TEncryptionIV() = default; // Uninitialized IV
    TEncryptionIV(const TEncryptionIV&) = default;
    TEncryptionIV(TEncryptionIV&&) = default;

    TEncryptionIV& operator=(const TEncryptionIV&) = default;
    TEncryptionIV& operator=(TEncryptionIV&&) = default;

    // Generate new random IV
    static TEncryptionIV Generate();

    // Combine IV for backup item file
    // base: base IV for the whole backup
    // backupItemNumber: unique backup item number within backup
    // shardNumber (only for sharded backup items such as tables): datashard number
    static TEncryptionIV Combine(const TEncryptionIV& base, EBackupFileType fileType, uint32_t backupItemNumber, uint32_t shardNumber);

    // Combine IV for specific chunk
    // fileIV: IV for backup item file got by Combine() function
    static TEncryptionIV CombineForChunk(const TEncryptionIV& fileIV, uint32_t chunkNumber);

    static TEncryptionIV FromBinaryString(const TString& s);

    operator bool() const {
        return !IV.empty();
    }

    bool operator!() const {
        return IV.empty();
    }

    bool operator==(const TEncryptionIV& iv) const {
        return IV == iv.IV;
    }

    bool operator!=(const TEncryptionIV& iv) const {
        return IV != iv.IV;
    }

    size_t Size() const {
        return IV.size();
    }

    const unsigned char* Ptr() const {
        Y_VERIFY(!IV.empty());
        return &IV[0];
    }

    TString GetHexString() const;
    TString GetBinaryString() const;

    std::vector<unsigned char> IV;

    // Proper size for ciphers used in backups
    static constexpr size_t SIZE = 12;

    static constexpr size_t FILE_TYPE_OFFSET = 0;
    static constexpr size_t FILE_TYPE_SIZE = 1;

    static constexpr size_t BACKUP_ITEM_OFFSET = FILE_TYPE_OFFSET + FILE_TYPE_SIZE;
    static constexpr size_t BACKUP_ITEM_SIZE = 3;
    static constexpr uint32_t MAX_BACKUP_ITEM_NUMBER = (1 << (8 * BACKUP_ITEM_SIZE));

    static constexpr size_t SHARD_NUMBER_OFFSET = BACKUP_ITEM_OFFSET + BACKUP_ITEM_SIZE;
    static constexpr size_t SHARD_NUMBER_SIZE = 4;

    static constexpr size_t CHUNK_NUMBER_OFFSET = SHARD_NUMBER_OFFSET + SHARD_NUMBER_SIZE;
    static constexpr size_t CHUNK_NUMBER_SIZE = 4;

    static_assert(CHUNK_NUMBER_OFFSET + CHUNK_NUMBER_SIZE == SIZE);
};

struct TEncryptionKey {
    TEncryptionKey() = default; // Uninitialized
    TEncryptionKey(const TEncryptionKey&) = default;
    TEncryptionKey(TEncryptionKey&&) = default;
    explicit TEncryptionKey(const TString& bytes)
        : Key(reinterpret_cast<const unsigned char*>(bytes.data()), reinterpret_cast<const unsigned char*>(bytes.data() + bytes.size()))
    {
    }

    TEncryptionKey& operator=(const TEncryptionKey&) = default;
    TEncryptionKey& operator=(TEncryptionKey&&) = default;

    operator bool() const {
        return !Key.empty();
    }

    bool operator!() const {
        return Key.empty();
    }

    size_t Size() const {
        return Key.size();
    }

    const unsigned char* Ptr() const {
        Y_VERIFY(!Key.empty());
        return &Key[0];
    }

    TString GetBinaryString() const;

    std::vector<unsigned char> Key;
};

// Class that writes encrypted file
// Has streaming interface
class TEncryptedFileSerializer {
public:
    TEncryptedFileSerializer(TEncryptedFileSerializer&&) = default;
    TEncryptedFileSerializer(TString algorithm, TEncryptionKey key, TEncryptionIV iv);
    ~TEncryptedFileSerializer();

    // Streaming interface
    // File consists of blocks that contain MAC
    // Block size should not be too big
    // because whole block must be read before usage.
    TBuffer AddBlock(TStringBuf data, bool last);

    // Helper that serializes the whole file at one time
    static TBuffer EncryptFullFile(TString algorithm, TEncryptionKey key, TEncryptionIV iv, TStringBuf data);

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;
};

// Class that reads encrypted file
// Has streaming interface
class TEncryptedFileDeserializer {
    TEncryptedFileDeserializer();

public:
    TEncryptedFileDeserializer(TEncryptedFileDeserializer&&) = default;
    TEncryptedFileDeserializer(TEncryptionKey key); // Decrypt file with key. Take IV from file header.
    TEncryptedFileDeserializer(TEncryptionKey key, TEncryptionIV expectedIV); // Decrypt file with key. Check that IV in header is equal to expectedIV
    ~TEncryptedFileDeserializer();

    // Adds buffer with input data.
    void AddData(TBuffer data, bool last);

    // Decrypts next block from previously added data.
    // Throws in case of error.
    // Returns Nothing if not enough data added.
    // Returns buffer with data in normal case.
    TMaybe<TBuffer> GetNextBlock();

    // Store state
    TString GetState() const;

    // Restore from state
    // State includes secret key
    static TEncryptedFileDeserializer RestoreFromState(const TString& state);

    // Get file IV.
    // Must be called after data added enough for the file header.
    TEncryptionIV GetIV() const;

    // Get input bytes read
    size_t GetProcessedInputBytes() const;

    // Helper that deserializes the whole file at one time
    static std::pair<TBuffer, TEncryptionIV> DecryptFullFile(TEncryptionKey key, TBuffer data);
    static TBuffer DecryptFullFile(TEncryptionKey key, TEncryptionIV expectedIV, TBuffer data);

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;
};

} // NKikimr::NBackup
