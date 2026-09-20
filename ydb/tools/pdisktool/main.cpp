#include <ydb/tools/pdisktool/lib/commands.h>
#include <ydb/core/blobstorage/crypto/default.h>

namespace NKikimr {
namespace NPDisk {
extern const ui64 YdbDefaultPDiskSequence = 0x7e5700007e570000;
}
}

int main(int argc, char** argv) {
    if (argc < 2) {
        NKikimr::NPDiskTool::PrintUsage(argv[0]);
        return 1;
    }
    return NKikimr::NPDiskTool::RunCommand(argv[1], argc - 1, argv + 1);
}
