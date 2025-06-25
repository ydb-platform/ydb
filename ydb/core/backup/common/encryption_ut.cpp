#include "encryption.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

// byte order functions
#if defined (_win_)
   #include <winsock2.h>
#elif defined (_unix_)
   #include <arpa/inet.h>
#endif

namespace NKikimr::NBackup {

const TEncryptionKey Key16("Cool random key!");
const TEncryptionKey Key32("Key is big enough to be 32 bytes");
const TString Algorithms[] = {
    "AES-128-GCM",
    "AES-256-GCM",
    "ChaCha20-Poly1305",
};

const TEncryptionKey& SelectKey(const TString& alg) {
    return alg == "AES-128-GCM" ? Key16 : Key32;
}

Y_UNIT_TEST_SUITE(EncryptedFileSerializerTest) {
    Y_UNIT_TEST(SerializeWholeFileAtATime) {
        TEncryptionIV iv = TEncryptionIV::Generate();
        TBuffer fileData = TEncryptedFileSerializer::EncryptFullFile("aes-128_gcm", Key16, iv, "short data file");
        TBuffer data = TEncryptedFileDeserializer::DecryptFullFile(Key16, iv, fileData);
        TString dataStr;
        data.AsString(dataStr);
        UNIT_ASSERT_STRINGS_EQUAL(dataStr, "short data file");
    }

    Y_UNIT_TEST(WrongParametersForSerializer) {
        TEncryptionIV iv = TEncryptionIV::Generate();
        UNIT_ASSERT_EXCEPTION_CONTAINS(TEncryptedFileSerializer("", Key16, iv), yexception, "No cipher algorithm specified");

        UNIT_ASSERT_EXCEPTION_CONTAINS(TEncryptedFileSerializer("EAS-256_gcm", Key16, iv), yexception, "Unknown cipher algorithm: \"EAS-256_gcm\"");

        UNIT_ASSERT_EXCEPTION_CONTAINS(TEncryptedFileSerializer("chacha20_poly1305", Key16, iv), yexception, "Invalid key length 16. Expected: 32");

        TEncryptionIV badIv = iv;
        badIv.IV.push_back(42);
        UNIT_ASSERT_EXCEPTION_CONTAINS(TEncryptedFileSerializer("aes_256_gcm", Key32, badIv), yexception, "Invalid IV length 13. Expected: 12");
    }

    Y_UNIT_TEST(WrongParametersForDeserializer) {
        TEncryptionIV iv = TEncryptionIV::Generate();
        TBuffer testData = TEncryptedFileSerializer::EncryptFullFile("chacha-20-poly1305", Key32, iv, "test data");
        UNIT_ASSERT_EXCEPTION_CONTAINS(TEncryptedFileDeserializer::DecryptFullFile(Key16, testData), yexception, "Invalid key length 16. Expected: 32");
        UNIT_ASSERT_EXCEPTION_CONTAINS(TEncryptedFileDeserializer::DecryptFullFile(Key16, TEncryptionIV::Generate(), testData), yexception, "File is corrupted");
    }

    Y_UNIT_TEST(SplitOnBlocks) {
        const TStringBuf text = "YDB is a fault-tolerant distributed SQL DBMS. YDB provides high availability, horizontal scalability, strict consistency, and ACID transaction support. Queries are made using an SQL dialect (YQL).";
        const TEncryptionIV iv = TEncryptionIV::Generate();
        const size_t splits[] = {0, 1, 2, 3, text.size() / 2, text.size() / 2 + 1};
        for (size_t splitOnWrite : splits) {
            // Write
            TEncryptedFileSerializer serializer("Chacha-20-poly-1305", Key32, iv);
            TBuffer serializedFile;
            if (splitOnWrite) {
                for (const char* fragmentStart = text.begin(); fragmentStart < text.end(); fragmentStart += splitOnWrite) {
                    TStringBuf fragment(fragmentStart, Min(fragmentStart + splitOnWrite, text.end()));
                    TBuffer block = serializer.AddBlock(fragment, false);
                    serializedFile.Append(block.Begin(), block.End());
                }
            } else {
                serializedFile = serializer.AddBlock(text, false);
            }
            TBuffer lastBlock = serializer.AddBlock(TStringBuf(), true);
            serializedFile.Append(lastBlock.Begin(), lastBlock.End());

            // Read with different splits
            for (size_t splitOnRead : splits) {
                for (size_t blocksToAddBeforeRead : splits) {
                    if (!blocksToAddBeforeRead) {
                        continue;
                    }
                    TBuffer deserializedFile;
                    TEncryptedFileDeserializer deserializer(Key32, iv);
                    auto tryAddBlock = [&]() -> bool {
                        if (TMaybe<TBuffer> block = deserializer.GetNextBlock()) {
                            deserializedFile.Append(block->Begin(), block->End());
                            return true;
                        }
                        return false;
                    };
                    if (splitOnRead) {
                        size_t addDataCalls = blocksToAddBeforeRead;
                        for (const char* fragmentStart = serializedFile.Data(); fragmentStart < serializedFile.Data() + serializedFile.Size(); fragmentStart += splitOnRead) {
                            deserializer.AddData(TBuffer(fragmentStart, Min<size_t>(splitOnRead, serializedFile.Data() + serializedFile.Size() - fragmentStart)), false);
                            if (--addDataCalls == 0) {
                                tryAddBlock();
                                addDataCalls = blocksToAddBeforeRead;
                            }
                        }
                    } else {
                        deserializer.AddData(serializedFile, false);
                        if (blocksToAddBeforeRead == 1) {
                            tryAddBlock();
                        }
                    }
                    deserializer.AddData(TBuffer(), true);
                    tryAddBlock();

                    TStringBuf resultText(deserializedFile.Data(), deserializedFile.Size());
                    UNIT_ASSERT_STRINGS_EQUAL(text, resultText);

                    UNIT_ASSERT_VALUES_EQUAL(serializedFile.Size(), deserializer.GetProcessedInputBytes());
                }
            }
        }
    }

    Y_UNIT_TEST(EmptyFile) {
        TEncryptionIV iv = TEncryptionIV::Generate();
        TBuffer fileData = TEncryptedFileSerializer::EncryptFullFile("aes-128_gcm", Key16, iv, TStringBuf());

        auto [buffer, headerIV] = TEncryptedFileDeserializer::DecryptFullFile(Key16, fileData);
        UNIT_ASSERT_VALUES_EQUAL(buffer.Size(), 0);
        UNIT_ASSERT_EQUAL(iv, headerIV);

        {
            TEncryptedFileDeserializer deserializer(Key16);
            deserializer.AddData(fileData, true);
            auto maybeBuffer = deserializer.GetNextBlock();
            UNIT_ASSERT(maybeBuffer);
            UNIT_ASSERT_VALUES_EQUAL(maybeBuffer->Size(), 0);
            // Calls after last block got
            UNIT_ASSERT(!deserializer.GetNextBlock());
            UNIT_ASSERT(!deserializer.GetNextBlock());
        }

        // Add empty data twice
        {
            TEncryptedFileSerializer serializer("AES-256-GCM", Key32, iv);
            TBuffer fileData;
            {
                TBuffer part = serializer.AddBlock("", false);
                fileData.Append(part.Data(), part.Size());
            }
            {
                TBuffer part = serializer.AddBlock("", false);
                UNIT_ASSERT_C(part.Size() == 0, part.Size());
            }
            {
                TBuffer part = serializer.AddBlock("", true);
                fileData.Append(part.Data(), part.Size());
            }
            TEncryptedFileDeserializer deserializer(Key32);
            deserializer.AddData(fileData, true);
            auto maybeBuffer = deserializer.GetNextBlock();
            UNIT_ASSERT(maybeBuffer);
            UNIT_ASSERT_VALUES_EQUAL(maybeBuffer->Size(), 0);
            UNIT_ASSERT(!deserializer.GetNextBlock());
        }
    }

    Y_UNIT_TEST(ReadPartial) {
        for (const TString& alg : Algorithms) {
            TEncryptionIV iv = TEncryptionIV::Generate();
            TBuffer fileData = TEncryptedFileSerializer::EncryptFullFile(alg, SelectKey(alg), iv, "encrypted text");

            TBuffer allButLastBlock(fileData.Data(), fileData.Size() - 20);
            TBuffer lastBlock(fileData.Data() + fileData.Size() - 20, 20);

            TEncryptedFileDeserializer deserializer(SelectKey(alg));
            deserializer.AddData(allButLastBlock, false);
            auto maybeBuffer = deserializer.GetNextBlock();
            UNIT_ASSERT(maybeBuffer);
            UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(maybeBuffer->Data(), maybeBuffer->Size()), "encrypted text"); // partial result

            deserializer.AddData(lastBlock, true);
            UNIT_ASSERT(!deserializer.GetNextBlock());
        }
    }

    Y_UNIT_TEST(DeleteLastByte) {
        TEncryptionIV iv = TEncryptionIV::Generate();
        TBuffer fileData = TEncryptedFileSerializer::EncryptFullFile("aes-256_gcm", Key32, iv, "short data file");
        fileData.Resize(fileData.Size() - 1);
        UNIT_ASSERT_EXCEPTION_CONTAINS(TEncryptedFileDeserializer::DecryptFullFile(Key32, fileData), yexception, "File is corrupted");
    }

    Y_UNIT_TEST(AddByte) {
        TEncryptionIV iv = TEncryptionIV::Generate();
        TBuffer fileData = TEncryptedFileSerializer::EncryptFullFile("aes-256_gcm", Key32, iv, "short data file");
        fileData.Resize(fileData.Size() + 1);
        UNIT_ASSERT_EXCEPTION_CONTAINS(TEncryptedFileDeserializer::DecryptFullFile(Key32, fileData), yexception, "File is corrupted");

        fileData.Resize(fileData.Size() - 1); // return back
        TEncryptedFileDeserializer deserializer(Key32);
        deserializer.AddData(fileData, false);
        auto maybeBuffer = deserializer.GetNextBlock();
        UNIT_ASSERT(maybeBuffer);
        UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(maybeBuffer->Data(), maybeBuffer->Size()), "short data file"); // partial result

        TBuffer additionalByte;
        additionalByte.Resize(1);
        deserializer.AddData(additionalByte, true);
        UNIT_ASSERT_EXCEPTION_CONTAINS(deserializer.GetNextBlock(), yexception, "File is corrupted");
    }

    Y_UNIT_TEST(RemoveLastBlock) {
        for (const TString& alg : Algorithms) {
            TEncryptionIV iv = TEncryptionIV::Generate();
            TBuffer fileData = TEncryptedFileSerializer::EncryptFullFile(alg, SelectKey(alg), iv, "encrypted text");

            fileData.Resize(fileData.Size() - 20); // remove last block
            TEncryptedFileDeserializer deserializer(SelectKey(alg));
            deserializer.AddData(fileData, false);
            auto maybeBuffer = deserializer.GetNextBlock();
            UNIT_ASSERT(maybeBuffer);
            UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(maybeBuffer->Data(), maybeBuffer->Size()), "encrypted text"); // partial result

            TBuffer empty;
            deserializer.AddData(empty, true);
            UNIT_ASSERT_EXCEPTION_CONTAINS(deserializer.GetNextBlock(), yexception, "File is corrupted");
        }
    }

    Y_UNIT_TEST(ChangeAnyByte) {
        for (const TString& alg : Algorithms) {
            TEncryptionIV iv = TEncryptionIV::Generate();
            TBuffer fileData = TEncryptedFileSerializer::EncryptFullFile(alg, SelectKey(alg), iv, "test text");
            for (size_t i = 0; i < fileData.Size(); ++i) {
                TBuffer modified = fileData;
                modified.Data()[i] ^= 1;
                UNIT_ASSERT_EXCEPTION_CONTAINS(TEncryptedFileDeserializer::DecryptFullFile(SelectKey(alg), modified), yexception, "File is corrupted");
            }
        }
    }

    Y_UNIT_TEST(BigHeaderSize) {
        TEncryptionIV iv = TEncryptionIV::Generate();
        TBuffer fileData = TEncryptedFileSerializer::EncryptFullFile("AES-128-GCM", Key16, iv, "test text");
        // Make header big
        fileData.Data()[0] = char(-1);
        UNIT_ASSERT_EXCEPTION_CONTAINS(TEncryptedFileDeserializer::DecryptFullFile(Key16, fileData), yexception, "File is corrupted");

        {
            TEncryptedFileDeserializer deserializer(Key16);
            deserializer.AddData(fileData, false);
            UNIT_ASSERT_EXCEPTION_CONTAINS(deserializer.GetNextBlock(), yexception, "File is corrupted");
        }

        // buffer only with size
        TBuffer bigHeaderSizeWrittenOnly(2);
        uint16_t bigHeader = 20_KB;
        bigHeaderSizeWrittenOnly.Data()[0] = *(reinterpret_cast<const char*>(&bigHeader) + 1);
        bigHeaderSizeWrittenOnly.Data()[1] = *reinterpret_cast<const char*>(&bigHeader);
        bigHeaderSizeWrittenOnly.Advance(2);
        {
            TEncryptedFileDeserializer deserializer(Key16);
            deserializer.AddData(bigHeaderSizeWrittenOnly, false);
            UNIT_ASSERT_EXCEPTION_CONTAINS(deserializer.GetNextBlock(), yexception, "File is corrupted");
        }
    }

    Y_UNIT_TEST(BigBlockSize) {
        TEncryptionIV iv = TEncryptionIV::Generate();
        TStringBuf srcText = "File with too big block size";
        TBuffer fileData = TEncryptedFileSerializer::EncryptFullFile("AES-256-GCM", Key32, iv, srcText);
        size_t blockSizePos = fileData.Size() - 20 /* last block with MAC */ - 16 /* MAC */ - srcText.size() - 4 /* sizeof(size) */;
        UNIT_ASSERT_LT(blockSizePos, fileData.Size() - 4);
        uint32_t* size = reinterpret_cast<uint32_t*>(fileData.Data() + blockSizePos);
        *size = htonl(51_MB);
        fileData.Resize(fileData.Size() + 4);
        {
            TEncryptedFileDeserializer deserializer(Key32);
            deserializer.AddData(fileData, false);
            UNIT_ASSERT_EXCEPTION_CONTAINS(deserializer.GetNextBlock(), yexception, "File is corrupted");
        }
    }

    Y_UNIT_TEST(RestoreFromState) {
        TString blocks[] = {
            "Come crawling faster",
            "Obey your master",
            "Your life burns faster",
            "Obey your master",
        };
        TEncryptionIV iv = TEncryptionIV::Generate();
        TEncryptedFileSerializer serializer("Chacha20-Poly1305", Key32, iv);
        TEncryptedFileDeserializer deserializer(Key32);
        TString state = deserializer.GetState();
        const size_t blocksSize = sizeof(blocks) / sizeof(blocks[0]);
        for (size_t i = 0; i < blocksSize; ++i) {
            const TString& block = blocks[i];
            const bool last = i == blocksSize - 1;
            TBuffer buffer = serializer.AddBlock(block, last);
            TEncryptedFileDeserializer restored = TEncryptedFileDeserializer::RestoreFromState(state);
            restored.AddData(std::move(buffer), last);
            TMaybe<TBuffer> decryptedBlock = restored.GetNextBlock();
            UNIT_ASSERT(decryptedBlock);
            UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(decryptedBlock->Data(), decryptedBlock->Size()), block);
            state = restored.GetState();
        }

        TEncryptedFileDeserializer restored = TEncryptedFileDeserializer::RestoreFromState(state);
        UNIT_ASSERT(!restored.GetNextBlock());
        UNIT_ASSERT_EXCEPTION_CONTAINS(restored.AddData(TBuffer("data", 4), true), yexception, "Stream finished");
    }

    Y_UNIT_TEST(IVSerialization) {
        TEncryptionIV iv = TEncryptionIV::Generate();
        UNIT_ASSERT_STRINGS_EQUAL(TEncryptionIV::FromHexString(iv.GetHexString()).GetHexString(), iv.GetHexString());
        UNIT_ASSERT_EQUAL(TEncryptionIV::FromHexString(iv.GetHexString()), iv);
        UNIT_ASSERT_EQUAL(TEncryptionIV::FromBinaryString(iv.GetBinaryString()), iv);
    }
}

} // namespace NKikimr::NBackup
