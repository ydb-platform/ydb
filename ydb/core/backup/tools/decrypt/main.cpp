#include <ydb/core/backup/common/encryption.h>

#include <library/cpp/getopt/opt.h>

#include <util/generic/size_literals.h>
#include <util/stream/file.h>

struct TOptions {
    NKikimr::NBackup::TEncryptionKey Key;
    TString KeyFile;
    TString InputFile;
    TString OutputFile;
    bool NoOutput = false;
    bool Verbose = false;

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

        opts.AddLongOption('o', "output-file", "Write decrypted data to this file instead of stdout")
            .RequiredArgument("PATH")
            .StoreResult(&OutputFile);

        opts.AddLongOption("no-output", "Do not write decrypted data, only check the file structure")
            .NoArgument()
            .SetFlag(&NoOutput);

        opts.AddLongOption('v', "verbose", "Print offset and size of every block")
            .NoArgument()
            .SetFlag(&Verbose);

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

struct TProgress {
    ui64 BlocksRead = 0;
    ui64 ProcessedBytes = 0; // encrypted bytes consumed, i.e. offset of the next block
    ui64 InputBytes = 0;     // encrypted bytes read from the input
    ui64 DecryptedBytes = 0;

    void Print(IOutputStream& out) const {
        out << "Blocks read: " << BlocksRead << Endl;
        out << "Decrypted bytes: " << DecryptedBytes << Endl;
        out << "Encrypted bytes processed: " << ProcessedBytes << Endl;
        out << "Encrypted bytes read from input: " << InputBytes << Endl;
    }
};

int main(int argc, const char* argv[]) {
    TOptions options(argc, argv);
    TProgress progress;

    if (options.NoOutput && options.OutputFile) {
        Cerr << "Error: --output-file cannot be used together with --no-output" << Endl;
        return 1;
    }

    std::optional<TFileOutput> outputFile;
    IOutputStream* out = &Cout;
    std::optional<NKikimr::NBackup::TEncryptedFileDeserializer> deserializer;

    try {
        if (!options.NoOutput && options.OutputFile) {
            outputFile.emplace(options.OutputFile);
            out = &*outputFile;
        }

        deserializer.emplace(options.Key);

        auto printIV = [&]() {
            if (const auto iv = deserializer->GetIV()) {
                Cerr << "IV: " << iv.GetHexString() << Endl;
            }
        };

        auto drainBlocks = [&]() {
            for (;;) {
                const ui64 blockStart = deserializer->GetProcessedInputBytes();
                TMaybe<TBuffer> block = deserializer->GetNextBlock();
                progress.ProcessedBytes = deserializer->GetProcessedInputBytes();
                if (!block) {
                    break;
                }

                ++progress.BlocksRead;
                progress.DecryptedBytes += block->Size();

                if (options.Verbose) {
                    Cerr << "Block " << progress.BlocksRead
                        << ": offset " << blockStart
                        << ", encrypted size " << (progress.ProcessedBytes - blockStart)
                        << ", decrypted size " << block->Size() << Endl;
                }

                if (!options.NoOutput) {
                    out->Write(block->Data(), block->Size());
                }
            }
        };

        std::optional<TFileInput> inputFile;
        IInputStream* in = &Cin;
        if (options.InputFile) {
            inputFile.emplace(options.InputFile);
            in = &*inputFile;
        }

        char buffer[4_MB];
        while (size_t bytes = in->Read(buffer, sizeof(buffer))) {
            progress.InputBytes += bytes;
            deserializer->AddData(TBuffer(buffer, bytes), false);
            drainBlocks();
        }
        deserializer->AddData(TBuffer(), true);
        drainBlocks();

        if (!options.NoOutput) {
            out->Finish();
        }

        printIV();
        progress.Print(Cerr);
        return 0;
    } catch (const std::exception& ex) {
        Cerr << "Error: " << ex.what() << Endl;
        if (deserializer) {
            progress.ProcessedBytes = deserializer->GetProcessedInputBytes();
            Cerr << Endl << "Decryption stopped at encrypted-file offset " << progress.ProcessedBytes
                << ": the block starting there could not be read" << Endl;
            if (const auto iv = deserializer->GetIV()) {
                Cerr << "IV: " << iv.GetHexString() << Endl;
            }
        }
        progress.Print(Cerr);
        return 1;
    }
}
