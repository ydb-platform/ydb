#pragma once

#include "geo_data.h"

#include <library/cpp/reverse_geocoder/library/block_allocator.h>

#include <util/memory/blob.h>

namespace NReverseGeocoder {
    class TGeoDataMap: public IGeoData, public TNonCopyable {
#define GEO_BASE_DEF_VAR(TVar, Var)    \
public:                                \
    const TVar& Var() const override { \
        return Var##_;                 \
    }                                  \
                                       \
private:                               \
    TVar Var##_;

#define GEO_BASE_DEF_ARR(TArr, Arr)        \
public:                                    \
    const TArr* Arr() const override {     \
        return Arr##_;                     \
    }                                      \
    TNumber Arr##Number() const override { \
        return Arr##Number_;               \
    }                                      \
                                           \
private:                                   \
    TNumber Arr##Number_;                  \
    const TArr* Arr##_;

        GEO_BASE_DEF_GEO_DATA

#undef GEO_BASE_DEF_VAR
#undef GEO_BASE_DEF_ARR

    public:
        TGeoDataMap();

        static void SerializeToFile(const TString& path, const IGeoData& data);

        static TBlob SerializeToBlob(const IGeoData& data);

        TGeoDataMap(const IGeoData& data, TBlockAllocator* allocator);

        TGeoDataMap(const char* data, size_t size)
            : TGeoDataMap()
        {
            Data_ = data;
            Size_ = size;
            Remap();
        }

        TGeoDataMap(TGeoDataMap&& dat)
            : TGeoDataMap()
        {
            DoSwap(Data_, dat.Data_);
            DoSwap(Size_, dat.Size_);
            Remap();
            dat.Remap();
        }

        TGeoDataMap& operator=(TGeoDataMap&& dat) {
            DoSwap(Data_, dat.Data_);
            DoSwap(Size_, dat.Size_);
            Remap();
            dat.Remap();
            return *this;
        }

        const char* Data() const {
            return Data_;
        }

        size_t Size() const {
            return Size_;
        }

    private:
        void Init();

        void Remap();

        const char* Data_;
        size_t Size_;
    };

}
