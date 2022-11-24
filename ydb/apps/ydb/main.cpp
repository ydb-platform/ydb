#include <ydb/public/lib/ydb_cli/commands/ydb_service_topic.h>
#include <ydb/apps/ydb/commands/ydb_cloud_root.h>

TVector<NYdb::NPersQueue::ECodec> NYdb::NConsoleClient::InitAllowedCodecs() {
    return TVector<NYdb::NPersQueue::ECodec>{
            NYdb::NPersQueue::ECodec::RAW,
            NYdb::NPersQueue::ECodec::ZSTD,
            NYdb::NPersQueue::ECodec::GZIP,
    };
}

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
