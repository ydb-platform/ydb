#include <ydb/apps/ydb/commands/ydb_cloud_root.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>

int main(int argc, char **argv) {
    try {
        return NYdb::NConsoleClient::NewYCloudClient(argc, argv);
    }
    catch (const NYdb::NConsoleClient::TMisuseException& e) {
        Cerr << e.what() << Endl;
        Cerr << "Try \"--help\" option for more info." << Endl;
        return EXIT_FAILURE;
    }
    catch (const NYdb::NConsoleClient::TYdbErrorException& e) {
        Cerr << e;
        return EXIT_FAILURE;
    }
    catch (const yexception& e) {
        Cerr << e.what() << Endl;
        return EXIT_FAILURE;
    }
}
