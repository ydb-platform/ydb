#include <map>

#include <google/protobuf/compiler/code_generator.h>
#include <google/protobuf/compiler/plugin.h>
#include <google/protobuf/descriptor.h>

#include <ydb/public/lib/protobuf/scoped_file_printer.h>

namespace NKikimr::NProtobuf {

class TBaseMessageGenerator {
public:
    using TVars = std::map<TString, TString>;

    explicit TBaseMessageGenerator(const google::protobuf::Descriptor* message, google::protobuf::compiler::OutputDirectory* output)
        : Message(message)
        , Header(output, HeaderFileName(message), ClassScope(message))
        , HeaderIncludes(output, HeaderFileName(message), IncludesScope())
        , Vars({
            {"class", ClassName(message)},
            {"fqMessageClass", FullyQualifiedClassName(Message)},
        })
    {}

protected:
    const google::protobuf::Descriptor* Message;
    TScopedFilePrinter Header;
    TScopedFilePrinter HeaderIncludes;
    TVars Vars;
};

} // namespace NKikimr::NProtobuf
