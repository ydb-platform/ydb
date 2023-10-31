#pragma once

#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>

#include <library/cpp/tdigest/tdigest.h>

using namespace NYql;
using namespace NUdf;

namespace {
    extern const char DigestResourceName[] = "Stat.TDigestResource";

    typedef TBoxedResource<TDigest, DigestResourceName> TDigestResource;
    typedef TRefCountedPtr<TDigestResource> TDigestResourcePtr;

    SIMPLE_UDF_WITH_OPTIONAL_ARGS(TTDigest_Create, TResource<DigestResourceName>(double, TOptional<double>, TOptional<double>), 2) {
        Y_UNUSED(valueBuilder);
        const double delta = args[1].GetOrDefault<double>(0.01);
        const double K = args[2].GetOrDefault<double>(25.0);
        if (delta == 0 || K / delta < 1) {
            UdfTerminate((TStringBuilder() << GetPos() << " Invalid combination of delta/K values").data());
        }

        return TUnboxedValuePod(new TDigestResource(delta, K, args[0].Get<double>()));
    }

    SIMPLE_STRICT_UDF(TTDigest_AddValue, TResource<DigestResourceName>(TResource<DigestResourceName>, double)) {
        Y_UNUSED(valueBuilder);
        TDigestResource::Validate(args[0]);
        TDigestResource* resource = static_cast<TDigestResource*>(args[0].AsBoxed().Get());
        resource->Get()->AddValue(args[1].Get<double>());
        return TUnboxedValuePod(resource);
    }

    SIMPLE_STRICT_UDF(TTDigest_GetPercentile, double(TResource<DigestResourceName>, double)) {
        Y_UNUSED(valueBuilder);
        TDigestResource::Validate(args[0]);
        return TUnboxedValuePod(static_cast<TDigestResource*>(args[0].AsBoxed().Get())->Get()->GetPercentile(args[1].Get<double>()));
    }

    SIMPLE_STRICT_UDF(TTDigest_Serialize, char*(TResource<DigestResourceName>)) {
        TDigestResource::Validate(args[0]);
        return valueBuilder->NewString(static_cast<TDigestResource*>(args[0].AsBoxed().Get())->Get()->Serialize());
    }

    SIMPLE_UDF(TTDigest_Deserialize, TResource<DigestResourceName>(char*)) {
        Y_UNUSED(valueBuilder);
        return TUnboxedValuePod(new TDigestResource(TString(args[0].AsStringRef())));
    }

    SIMPLE_STRICT_UDF(TTDigest_Merge, TResource<DigestResourceName>(TResource<DigestResourceName>, TResource<DigestResourceName>)) {
        Y_UNUSED(valueBuilder);
        TDigestResource::Validate(args[0]);
        TDigestResource::Validate(args[1]);
        return TUnboxedValuePod(new TDigestResource(static_cast<TDigestResource*>(args[0].AsBoxed().Get())->Get(), static_cast<TDigestResource*>(args[1].AsBoxed().Get())->Get()));
    }

    /*
 *
 * TODO: Memory tracking
 *
 *
 *
 */

    SIMPLE_MODULE(TStatModule,
                  TTDigest_Create,
                  TTDigest_AddValue,
                  TTDigest_GetPercentile,
                  TTDigest_Serialize,
                  TTDigest_Deserialize,
                  TTDigest_Merge)

}
