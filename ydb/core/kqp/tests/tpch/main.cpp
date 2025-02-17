#include "commands.h"

#include <ydb/public/lib/ydb_cli/common/common.h>

int main(int argc, char** argv) {
    try {
        return NYdb::NTpch::NewTpchClient(argc, argv);
    }
    catch (const NYdb::NConsoleClient::TMisuseException& e) {
        Cerr << e.what() << Endl;
        Cerr << "Try \"--help\" option for more info." << Endl;
        return 1;
    }
    catch (const yexception& e) {
        Cerr << e.what() << Endl;
        return 1;
    }
}
