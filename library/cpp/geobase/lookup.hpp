#pragma once

#include <geobase/include/lookup.hpp>
#include <geobase/include/lookup_wrapper.hpp>
#include <geobase/include/structs.hpp>

namespace NGeobase {
    using TInitTraits = NImpl::TLookup::TInitTraits;

    class TLookup: public NImpl::TLookup {
    public:
        using parent = NImpl::TLookup;

        explicit TLookup(const std::string& datafile, const TInitTraits traits = {})
            : parent(datafile, traits)
        {
        }
        explicit TLookup(const TInitTraits traits)
            : parent(traits)
        {
        }
        explicit TLookup(const void* pData, size_t len)
            : parent(pData, len)
        {
        }

        ~TLookup() {
        }
    };

    using TRegion = NImpl::TRegion;
    using TGeolocation = NImpl::TGeolocation;
    using TLinguistics = NImpl::TLinguistics;
    using TGeoPoint = NImpl::TGeoPoint;

    using TLookupWrapper = NImpl::TLookupWrapper;

    using TId = NImpl::Id;
    using TIdsList = NImpl::IdsList;
    using TRegionsList = NImpl::TRegionsList;

    using TIpBasicTraits = NImpl::TIpBasicTraits;
    using TIpTraits = NImpl::TIpTraits;
}
