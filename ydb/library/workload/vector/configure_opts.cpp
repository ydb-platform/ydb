#include "configure_opts.h"

#include <ydb/public/lib/ydb_cli/common/colors.h>

namespace NYdbWorkload::NVector {

void ConfigureTableOpts(NLastGetopt::TOpts& opts, TTableOpts* tableOpts) {
    opts.AddLongOption( "table", "Table name")
        .RequiredArgument("NAME")
        .DefaultValue(tableOpts->Name)
        .StoreResult(&tableOpts->Name);
}

void ConfigureVectorOpts(NLastGetopt::TOpts& opts, TVectorOpts* vectorOpts) {
    NColorizer::TColors colors = NYdb::NConsoleClient::AutoColors(Cout);

    {
        const TVector<TString> vectorTypes = {"float", "int8", "uint8", "bit"};

        TStringBuilder builder;
        builder << "Type of vectors. Available options: ";
        bool printComma = false;
        for (const TString& vectorType : vectorTypes) {
            if (printComma) {
                builder << ", ";
            } else {
                printComma = true;
            }
            builder << colors.BoldColor() << vectorType << colors.OldColor();
        }

        opts.AddLongOption("vector-type", builder)
            .DefaultValue(vectorOpts->VectorType)
            .StoreResult(&vectorOpts->VectorType);
    }

    opts.AddLongOption("vector-dimension", "Vector dimension. Size of embedding vectors")
        .DefaultValue(vectorOpts->VectorDimension)
        .StoreResult(&vectorOpts->VectorDimension);
}

}
