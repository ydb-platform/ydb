#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>

#include <util/stream/output.h>
#include <util/generic/yexception.h>

int main(int argc, char** argv) {
    if (argc != 2) {
        Cerr << "Usage: " << argv[0] << " <path>" << Endl;
        return 1;
    }

    try {
        NKikimr::ObliterateDisk(argv[1]);
        return 0;
    } catch (const yexception& e) {
        Cerr << "Error: " << e.what() << Endl;
        return 1;
    }
}
