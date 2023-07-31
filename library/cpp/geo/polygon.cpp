#include "polygon.h"
namespace NGeo {
    TMaybe<TGeoPolygon> TGeoPolygon::TryParse(TStringBuf s, TStringBuf llDelimiter, TStringBuf pointsDelimiter) {
        TVector<TGeoPoint> points;

        for (const auto& pointString : StringSplitter(s).SplitByString(pointsDelimiter).SkipEmpty()) {
            auto curPoint = TGeoPoint::TryParse(pointString.Token(), llDelimiter);
            if (!curPoint) {
                return {};
            }
            points.push_back(*curPoint);
        }

        if (points.size() < 3) {
            return {};
        }

        return TGeoPolygon(points);
    }

    TGeoPolygon TGeoPolygon::Parse(TStringBuf s, TStringBuf llDelimiter, TStringBuf pointsDelimiter) {
        auto res = TGeoPolygon::TryParse(s, llDelimiter, pointsDelimiter);
        if (!res) {
            ythrow yexception() << "Can't parse polygon from input string: " << s;
        }
        return *res;
    }
} // namespace NGeo
