#include "file_client.h"

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NApi {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TFileReadRange::Register(TRegistrar registrar)
{
    registrar.Parameter("begin", &TThis::Begin)
        .Default(0)
        .GreaterThanOrEqual(0);
    registrar.Parameter("end", &TThis::End)
        .Default()
        .GreaterThanOrEqual(0);
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TFilePartition& partition, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .DoIf(static_cast<bool>(partition.Cookie), [&] (TFluentMap fluent) {
                auto ysonString = NYson::ConvertToYsonString(partition.Cookie);
                fluent.Item("cookie").Value(ysonString.AsStringBuf());
            })
            .Item("length").Value(partition.Length)
        .EndMap();
}

void Serialize(const TFilePartitions& partitions, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("partitions").Value(partitions.Partitions)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
