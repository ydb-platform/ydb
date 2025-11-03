#include <ydb/library/yaml_config/yaml_config.h>
#include <ydb/library/yaml_config/yaml_config_parser.h>
#include <ydb/library/fyamlcpp/fyamlcpp.h>

#include <fstream>

using namespace NKikimr;

int main(int argc, char **argv) {
    if (argc != 4) {
        Cerr << "usage: " << argv[0] << " <input_yaml> <tenant> <output_yaml>" << Endl;
        return 1;
    }

    auto inputFile = argv[1];
    auto tenant = argv[2];
    auto outputFile = argv[3];

    std::ifstream inputStream(inputFile);
    std::ofstream outputStream(outputFile);

    if (!inputStream.is_open() || !outputStream.is_open()) {
        Cerr << "Can't open file " << (inputStream.is_open() ? outputFile : inputFile) << Endl;
        return 1;
    }

    std::stringstream inputBuf;
    TStringStream outputBuf;

    inputBuf << inputStream.rdbuf();
    inputStream.close();

    auto doc = NFyaml::TDocument::Parse(inputBuf.str());
    auto [resolved, node] = NYamlConfig::Resolve(
        doc,
        {
            NYamlConfig::TNamedLabel{"tenant", tenant},
        }
    );

    outputBuf << node;

    outputStream << outputBuf.Str();

    if (outputStream.flush().fail()) {
        Cerr << "Can't write to file " << outputFile << Endl;
        return 1;
    }

    outputStream.close();

    return 0;
}
