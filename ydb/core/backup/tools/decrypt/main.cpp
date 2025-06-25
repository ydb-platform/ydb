#include <ydb/core/backup/common/encryption.h>

#include <library/cpp/getopt/opt.h>

#include <util/generic/size_literals.h>
#include <util/stream/file.h>

struct TOptions {
    NKikimr::NBackup::TEncryptionKey Key;
    TString KeyFile;
    TString InputFile;

public:
    TOptions(int argc, const char* argv[]) {
        try {
            ParseOptions(argc, argv);
        } catch (const std::exception&) {
            Cerr << "Failed to get options: " << CurrentExceptionMessage() << Endl;
            exit(1);
        }
    }

private:
    void ParseOptions(int argc, const char** argv) {
        NLastGetopt::TOpts opts;
        opts.SetTitle("Backup file decryption tool");
        opts.SetFreeArgsNum(0);
        opts.AddHelpOption('h');
        opts.AddVersionOption();

        opts.AddLongOption('k', "encryption-key-file", "Encryption key file")
            .RequiredArgument("KEY")
            .StoreResult(&KeyFile);

        opts.AddLongOption('i', "input-file", "Input file")
            .RequiredArgument("PATH")
            .StoreResult(&InputFile);

        NLastGetopt::TOptsParseResult res(&opts, argc, argv);

        if (KeyFile) {
            TFileInput keyFile(KeyFile);
            TString keyData = keyFile.ReadAll();
            Key = NKikimr::NBackup::TEncryptionKey(keyData);
        } else {
            Cerr << "No encryption key file provided" << Endl;
            exit(1);
        }
    }
};


int main(int argc, const char* argv[]) {
    TOptions options(argc, argv);
    try {
        NKikimr::NBackup::TEncryptedFileDeserializer deserializer(options.Key);
        std::optional<TFileInput> inputFile;
        IInputStream* in = &Cin;
        if (options.InputFile) {
            inputFile.emplace(options.InputFile);
            in = &*inputFile;
        }
        char buffer[4_MB];
        while (size_t bytes = in->Read(buffer, sizeof(buffer))) {
            deserializer.AddData(TBuffer(buffer, bytes), false);
            if (TMaybe<TBuffer> block = deserializer.GetNextBlock()) {
                Cout.Write(block->Data(), block->Size());
            }
        }
        deserializer.AddData(TBuffer(), true);
        if (TMaybe<TBuffer> block = deserializer.GetNextBlock()) {
            Cout.Write(block->Data(), block->Size());
        }
        Cerr << "IV: " << deserializer.GetIV().GetHexString() << Endl;
        return 0;
    } catch (const std::exception& ex) {
        Cerr << "Error: " << ex.what() << Endl;
        return 1;
    }
}
