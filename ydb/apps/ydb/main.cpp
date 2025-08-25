#include <ydb/apps/ydb/commands/ydb_root.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_topic.h>

TVector<NYdb::NTopic::ECodec> NYdb::NConsoleClient::InitAllowedCodecs() {
    return TVector<NYdb::NTopic::ECodec>{
            NYdb::NTopic::ECodec::RAW,
            NYdb::NTopic::ECodec::ZSTD,
            NYdb::NTopic::ECodec::GZIP,
    };
}

int main(int argc, char **argv) {
    try {
        return NYdb::NConsoleClient::NewYdbClient(argc, argv);
    }
    catch (const NYdb::NStatusHelpers::TYdbErrorException& e) {
        Cerr << e;
        return EXIT_FAILURE;
    }
    catch (const yexception& e) {
        Cerr << e.what() << Endl;
        return EXIT_FAILURE;
    }
}
