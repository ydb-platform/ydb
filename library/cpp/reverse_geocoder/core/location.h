#pragma once

namespace NReverseGeocoder {
    struct TLocation {
        double Lon;
        double Lat;

        TLocation()
            : Lon(0)
            , Lat(0)
        {
        }

        TLocation(double lon, double lat)
            : Lon(lon)
            , Lat(lat)
        {
        }
    };

}
