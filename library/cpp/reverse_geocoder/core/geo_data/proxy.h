#pragma once

#include "geo_data.h"
#include "map.h"

#include <util/generic/ptr.h>
#include <util/system/filemap.h>

namespace NReverseGeocoder {
    class IGeoDataProxy {
    public:
        virtual const IGeoData* GeoData() const = 0;

        virtual ~IGeoDataProxy() {
        }
    };

    using TGeoDataProxyPtr = THolder<IGeoDataProxy>;

    class TGeoDataMapProxy: public IGeoDataProxy, public TNonCopyable {
    public:
        explicit TGeoDataMapProxy(const char* path)
            : MemFile_(path)
        {
            MemFile_.Map(0, MemFile_.Length());
            GeoData_ = TGeoDataMap((const char*)MemFile_.Ptr(), MemFile_.MappedSize());
        }

        const IGeoData* GeoData() const override {
            return &GeoData_;
        }

    private:
        TFileMap MemFile_;
        TGeoDataMap GeoData_;
    };

    class TGeoDataWrapper: public IGeoDataProxy, public TNonCopyable {
    public:
        explicit TGeoDataWrapper(const IGeoData& g)
            : GeoData_(&g)
        {
        }

        const IGeoData* GeoData() const override {
            return GeoData_;
        }

    private:
        const IGeoData* GeoData_;
    };

    class TGeoDataRawProxy: public IGeoDataProxy, public TNonCopyable {
    public:
        TGeoDataRawProxy(const char* data, size_t dataSize)
            : GeoData_(data, dataSize)
        {
        }

        const IGeoData* GeoData() const override {
            return &GeoData_;
        }

    private:
        TGeoDataMap GeoData_;
    };

}
