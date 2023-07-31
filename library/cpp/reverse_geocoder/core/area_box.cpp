#include "area_box.h"

using namespace NReverseGeocoder;

TRef NReverseGeocoder::LookupAreaBox(const TPoint& point) {
    const TRef boxX = (point.X - NAreaBox::LowerX) / NAreaBox::DeltaX;
    const TRef boxY = (point.Y - NAreaBox::LowerY) / NAreaBox::DeltaY;
    return boxX * NAreaBox::NumberY + boxY;
}
