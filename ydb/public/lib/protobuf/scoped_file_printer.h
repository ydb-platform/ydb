#include "helpers.h"

#include <google/protobuf/compiler/code_generator.h>
#include <google/protobuf/io/printer.h>
#include <google/protobuf/io/zero_copy_stream.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NKikimr::NProtobuf {

class TScopedFilePrinter
    : public IPrinter
{
public:
    explicit TScopedFilePrinter(google::protobuf::compiler::OutputDirectory* output, const TString& fileName, const TString& scope)
        : Output(output)
        , FileName(fileName)
        , Scope(scope)
    {
    }

    google::protobuf::io::Printer* operator->() override {
        if (!Printer) {
            Stream.Reset(Output->OpenForInsert(FileName, Scope));
            Printer.ConstructInPlace(Stream.Get(), '$');
        }

        return Printer.Get();
    }

protected:
    google::protobuf::compiler::OutputDirectory* Output;
    const TString FileName;
    const TString Scope;

    THolder<google::protobuf::io::ZeroCopyOutputStream> Stream;
    TMaybe<google::protobuf::io::Printer> Printer;
}; // TScopedFilePrinter

} // namespace NKikimr::NProtobuf
