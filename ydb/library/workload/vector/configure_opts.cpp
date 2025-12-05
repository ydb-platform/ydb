#include "configure_opts.h"

#include <ydb/public/lib/ydb_cli/common/colors.h>

namespace NYdbWorkload::NVector {

void ConfigureTableOpts(NLastGetopt::TOpts& opts, TTableOpts* tableOpts) {
    opts.AddLongOption( "table", "Table name")
        .RequiredArgument("NAME")
        .DefaultValue(tableOpts->Name)
        .StoreResult(&tableOpts->Name);
}

void ConfigureTablePartitioningOpts(NLastGetopt::TOpts& opts, TTablePartitioningOpts* partitioningOpts) {
    opts.AddLongOption("table-min-partitions", "Table min partitions count (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT)")
        .RequiredArgument("INT")
        .DefaultValue(partitioningOpts->MinPartitions)
        .StoreResult(&partitioningOpts->MinPartitions);

    opts.AddLongOption("table-partition-size", "Table max partition size (AUTO_PARTITIONING_PARTITION_SIZE_MB)")
        .RequiredArgument("INT")
        .DefaultValue(partitioningOpts->PartitionSize)
        .StoreResult(&partitioningOpts->PartitionSize);

    opts.AddLongOption("table-auto-partitioning-by-size", "Table automatic partitioning by load (AUTO_PARTITIONING_BY_LOAD)")
        .RequiredArgument("BOOL")
        .DefaultValue(partitioningOpts->AutoPartitioningBySize)
        .StoreResult(&partitioningOpts->AutoPartitioningBySize);

    opts.AddLongOption("table-auto-partitioning-by-load", "Table automatic partitioning by load (AUTO_PARTITIONING_BY_LOAD)")
        .RequiredArgument("BOOL")
        .DefaultValue(partitioningOpts->AutoPartitioningByLoad)
        .StoreResult(&partitioningOpts->AutoPartitioningByLoad);
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
