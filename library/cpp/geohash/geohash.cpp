#include "geohash.h"

#include <util/generic/xrange.h>

namespace {
    using TNeighbourDescriptors = NGeoHash::TNeighbours<TMaybe<NGeoHash::TGeoHashDescriptor>>;
    const auto directions = GetEnumAllValues<NGeoHash::EDirection>();

    const auto doubleEps = std::numeric_limits<double>::epsilon();

    const NGeoHash::TBoundingBoxLL& GetGlobalBBox() {
        static const NGeoHash::TBoundingBoxLL globalLimits({-180, -90}, {180, 90});
        return globalLimits;
    }

    const TStringBuf base32EncodeTable = "0123456789bcdefghjkmnpqrstuvwxyz";

    const ui64 base32DecodeMask = 0x1F;
    constexpr int base32DecodeTableSize = 128;

    using TBase32DecodeTable = std::array<TMaybe<i8>, base32DecodeTableSize>;

    TBase32DecodeTable MakeBase32DecodeTable() {
        TBase32DecodeTable result;
        result.fill(Nothing());
        for (auto i : xrange(base32EncodeTable.size())) {
            result[base32EncodeTable[i]] = i;
        }
        return result;
    }

    const TBase32DecodeTable base32DecodeTable = MakeBase32DecodeTable();
}

namespace NGeoHash {
    static const ui8 maxSteps = 62;
    static const ui8 maxPrecision = TGeoHashDescriptor::StepsToPrecision(maxSteps); // 12

    static const TNeighbours<std::pair<i8, i8>> neighborBitMoves = {
        {1, 0}, // NORTH
        {1, 1},
        {0, 1},
        {-1, 1},
        {-1, 0},
        {-1, -1},
        {0, -1},
        {1, -1},
    };

    ui8 TGeoHashDescriptor::StepsToPrecision(ui8 steps) {
        return steps / StepsPerPrecisionUnit;
    }

    ui8 TGeoHashDescriptor::PrecisionToSteps(ui8 precision) {
        return precision * StepsPerPrecisionUnit;
    }

    /* Steps interleave starting from lon so for 5 steps 3 are lon-steps and 2 are lat-steps.
     * Thus there are ceil(step/2) lon-steps and floor(step/2) lat-steps */
    std::pair<ui8, ui8> TGeoHashDescriptor::LatLonSteps() const {
        return std::make_pair<ui8, ui8>(Steps / 2, (Steps + 1) / 2);
    }

    struct TMagicNumber {
        ui64 Mask;
        ui8 Shift;
    };

    /* Interleave lower bits of x and y, so the bits of x
     * are in the even positions and bits from y in the odd.
     * e.g. Interleave64(0b101, 0b110) => 0b111001
     * From: https://graphics.stanford.edu/~seander/bithacks.html#InterleaveBMN
     */
    ui64 TGeoHashDescriptor::Interleave64(ui32 x, ui32 y) {
        // attention: magic numbers
        constexpr TMagicNumber mortonMagicNumbers[] = {
            {0x0000FFFF0000FFFF, 16},
            {0x00FF00FF00FF00FF, 8},
            {0x0F0F0F0F0F0F0F0F, 4},
            {0x3333333333333333, 2},
            {0x5555555555555555, 1}};

        ui64 x64 = x;
        ui64 y64 = y;

        for (const auto& magicNumber : mortonMagicNumbers) {
            x64 = (x64 | (x64 << magicNumber.Shift)) & magicNumber.Mask;
            y64 = (y64 | (y64 << magicNumber.Shift)) & magicNumber.Mask;
        }
        return x64 | (y64 << 1);
    }

    /* Reverse the interleave process
     * Deinterleave64(0b111001) => 0b101110
     * derived from http://stackoverflow.com/questions/4909263 */
    std::pair<ui32, ui32> TGeoHashDescriptor::Deinterleave64(ui64 z) {
        constexpr TMagicNumber demortonMagicNumbers[] = {
            {0x5555555555555555ULL, 0},
            {0x3333333333333333ULL, 1},
            {0x0F0F0F0F0F0F0F0FULL, 2},
            {0x00FF00FF00FF00FFULL, 4},
            {0x0000FFFF0000FFFFULL, 8},
            {0x00000000FFFFFFFFULL, 16}};

        ui64 x = z;
        ui64 y = z >> 1;

        for (const auto& magicNumber : demortonMagicNumbers) {
            x = (x | (x >> magicNumber.Shift)) & magicNumber.Mask;
            y = (y | (y >> magicNumber.Shift)) & magicNumber.Mask;
        }

        return std::make_pair(x, y);
    }

    std::pair<ui32, ui32> TGeoHashDescriptor::LatLonBits() const {
        auto deinterleaved = Deinterleave64(Bits);

        if (Steps % 2) {
            DoSwap(deinterleaved.first, deinterleaved.second);
        }
        return deinterleaved;
    }

    void TGeoHashDescriptor::SetLatLonBits(ui32 latBits, ui32 lonBits) {
        if (Steps % 2) {
            Bits = Interleave64(lonBits, latBits);
        } else {
            Bits = Interleave64(latBits, lonBits);
        }
    }

    void TGeoHashDescriptor::InitFromLatLon(double latitude, double longitude, const TBoundingBoxLL& limits, ui8 steps) {
        Steps = steps;
        if (Steps > maxSteps) {
            ythrow yexception() << "Invalid steps: available values: 0.." << ::ToString(maxSteps);
        }

        if (limits.Width() < doubleEps || limits.Height() < doubleEps) {
            ythrow yexception() << "Invalid limits: min/max for one of coordinates are equal";
        }

        if (latitude < limits.GetMinY() || latitude > limits.GetMaxY() || longitude < limits.GetMinX() || longitude > limits.GetMaxX()) {
            ythrow yexception() << "Invalid point (" << latitude << ", " << longitude << "): outside of limits";
        }

        double lat01 = (latitude - limits.GetMinY()) / limits.Height();
        double lon01 = (longitude - limits.GetMinX()) / limits.Width();

        auto llSteps = LatLonSteps();

        /* convert to fixed point based on the step size */
        lat01 *= (1 << llSteps.first);
        lon01 *= (1 << llSteps.second);

        /* If lon_steps > lat_step, last bit is lon-bit, otherwise last bit is lat-bit*/
        SetLatLonBits(lat01, lon01);
    }

    TGeoHashDescriptor::TGeoHashDescriptor(double latitude, double longitude, const TBoundingBoxLL& limits, ui8 steps) {
        InitFromLatLon(latitude, longitude, limits, steps);
    }

    TGeoHashDescriptor::TGeoHashDescriptor(double latitude, double longitude, ui8 steps) {
        InitFromLatLon(latitude, longitude, GetGlobalBBox(), steps);
    }

    TGeoHashDescriptor::TGeoHashDescriptor(const NGeo::TPointLL& point, const TBoundingBoxLL& limits, ui8 steps) {
        InitFromLatLon(point.Lat(), point.Lon(), limits, steps);
    }

    TGeoHashDescriptor::TGeoHashDescriptor(const NGeo::TPointLL& point, ui8 steps) {
        InitFromLatLon(point.Lat(), point.Lon(), GetGlobalBBox(), steps);
    }

    TGeoHashDescriptor::TGeoHashDescriptor(const TString& hashString) {
        if (hashString.size() > maxPrecision) {
            ythrow yexception() << "hashString is too long: max length is " << ::ToString(maxPrecision);
        }

        Bits = 0;
        for (auto c : hashString) {
            Bits <<= StepsPerPrecisionUnit;
            Y_ENSURE(c >= 0);
            const auto decodedChar = base32DecodeTable[c];
            Y_ENSURE(decodedChar.Defined());
            Bits |= decodedChar.GetRef();
        }

        Steps = PrecisionToSteps(hashString.size());
    }

    ui64 TGeoHashDescriptor::GetBits() const {
        return Bits;
    }

    ui8 TGeoHashDescriptor::GetSteps() const {
        return Steps;
    }

    TString TGeoHashDescriptor::ToString() const {
        auto precision = StepsToPrecision(Steps);

        TStringStream stream;

        auto bits = Bits;
        auto activeSteps = PrecisionToSteps(precision);

        bits >>= (Steps - activeSteps);
        for (auto i : xrange(precision)) {
            auto ix = (bits >> (StepsPerPrecisionUnit * ((precision - i - 1)))) & base32DecodeMask;
            stream << base32EncodeTable[ix];
        }

        return stream.Str();
    }

    TBoundingBoxLL TGeoHashDescriptor::ToBoundingBox(const TBoundingBoxLL& limits) const {
        auto llBits = LatLonBits();
        auto llSteps = LatLonSteps();

        double latMultiplier = limits.Height() / (1ull << llSteps.first);
        double lonMultiplier = limits.Width() / (1ull << llSteps.second);

        return {
            {
                limits.GetMinX() + lonMultiplier * llBits.second,
                limits.GetMinY() + latMultiplier * llBits.first,
            },
            {
                limits.GetMinX() + lonMultiplier * (llBits.second + 1),
                limits.GetMinY() + latMultiplier * (llBits.first + 1),
            }};
    }

    TBoundingBoxLL TGeoHashDescriptor::ToBoundingBox() const {
        return ToBoundingBox(GetGlobalBBox());
    }

    NGeo::TPointLL TGeoHashDescriptor::ToPoint(const TBoundingBoxLL& limits) const {
        auto boundingBox = ToBoundingBox(limits);
        return {
            boundingBox.GetMinX() + boundingBox.Width() / 2,
            boundingBox.GetMinY() + boundingBox.Height() / 2};
    }

    NGeo::TPointLL TGeoHashDescriptor::ToPoint() const {
        return ToPoint(GetGlobalBBox());
    }

    TMaybe<TGeoHashDescriptor> TGeoHashDescriptor::GetNeighbour(EDirection direction) const {
        TGeoHashDescriptor result(0, Steps);
        auto llBits = LatLonBits();
        auto llSteps = LatLonSteps();
        std::pair<i8, i8> bitMove = neighborBitMoves[direction];

        auto newLatBits = llBits.first + bitMove.first;
        auto newLonBits = llBits.second + bitMove.second;

        // Overflow in lat means polar, so return Nothing
        if (newLatBits >> llSteps.first != 0) {
            return Nothing();
        }

        // Overflow in lon means 180-meridian, so just remove overflowed bits
        newLonBits &= ((1 << llSteps.second) - 1);
        result.SetLatLonBits(newLatBits, newLonBits);
        return result;
    }

    TNeighbourDescriptors TGeoHashDescriptor::GetNeighbours() const {
        TNeighbourDescriptors result;
        auto llBits = LatLonBits();
        auto llSteps = LatLonSteps();
        std::pair<i8, i8> bitMove;

        for (auto direction : directions) {
            bitMove = neighborBitMoves[direction];

            auto newLatBits = llBits.first + bitMove.first;
            auto newLonBits = llBits.second + bitMove.second;

            // Overflow in lat means polar, so put Nothing
            if (newLatBits >> llSteps.first != 0) {
                result[direction] = Nothing();
            } else {
                result[direction] = TGeoHashDescriptor(0, Steps);
                // Overflow in lon means 180-meridian, so just remove overflowed bits
                newLonBits &= ((1 << llSteps.second) - 1);
                result[direction]->SetLatLonBits(newLatBits, newLonBits);
            }
        }

        return result;
    }

    TVector<TGeoHashDescriptor> TGeoHashDescriptor::GetChildren(ui8 steps = StepsPerPrecisionUnit) const {
        TVector<TGeoHashDescriptor> children(Reserve(1 << steps));
        ui8 childrenSteps = steps + Steps;
        auto parentBits = Bits << steps;
        if (childrenSteps > maxSteps) {
            ythrow yexception() << "Resulting geohash steps are too big, available values: 0.." << ::ToString(maxSteps);
        }
        for (auto residue : xrange(1 << steps)) {
            children.emplace_back(parentBits | residue, childrenSteps);
        }
        return children;
    }

    /* Functions */

    ui64 Encode(double latitude, double longitude, ui8 precision) {
        auto descr = TGeoHashDescriptor(
            latitude, longitude, TGeoHashDescriptor::PrecisionToSteps(precision));
        return descr.GetBits();
    }
    ui64 Encode(const NGeo::TPointLL& point, ui8 precision) {
        return TGeoHashDescriptor(
                   point, TGeoHashDescriptor::PrecisionToSteps(precision))
            .GetBits();
    }

    TString EncodeToString(double latitude, double longitude, ui8 precision) {
        return TGeoHashDescriptor(
                   latitude, longitude, TGeoHashDescriptor::PrecisionToSteps(precision))
            .ToString();
    }
    TString EncodeToString(const NGeo::TPointLL& point, ui8 precision) {
        return TGeoHashDescriptor(
                   point, TGeoHashDescriptor::PrecisionToSteps(precision))
            .ToString();
    }

    NGeo::TPointLL DecodeToPoint(const TString& hashString) {
        return TGeoHashDescriptor(hashString).ToPoint();
    }
    NGeo::TPointLL DecodeToPoint(ui64 hash, ui8 precision) {
        return TGeoHashDescriptor(hash, TGeoHashDescriptor::PrecisionToSteps(precision)).ToPoint();
    }

    TBoundingBoxLL DecodeToBoundingBox(const TString& hashString) {
        return TGeoHashDescriptor(hashString).ToBoundingBox();
    }

    TBoundingBoxLL DecodeToBoundingBox(ui64 hash, ui8 precision) {
        return TGeoHashDescriptor(hash, TGeoHashDescriptor::PrecisionToSteps(precision)).ToBoundingBox();
    }

    TMaybe<ui64> GetNeighbour(ui64 hash, EDirection direction, ui8 precision) {
        auto neighbour = TGeoHashDescriptor(
                             hash, TGeoHashDescriptor::PrecisionToSteps(precision))
                             .GetNeighbour(direction);

        if (neighbour.Defined()) {
            return neighbour->GetBits();
        } else {
            return Nothing();
        }
    }

    TMaybe<TString> GetNeighbour(const TString& hashString, EDirection direction) {
        auto neighbour = TGeoHashDescriptor(hashString).GetNeighbour(direction);
        if (neighbour.Defined()) {
            return neighbour->ToString();
        } else {
            return Nothing();
        }
    }

    TGeoHashBitsNeighbours GetNeighbours(ui64 hash, ui8 precision) {
        TGeoHashBitsNeighbours result;

        auto neighbours = TGeoHashDescriptor(
                              hash, TGeoHashDescriptor::PrecisionToSteps(precision))
                              .GetNeighbours();

        for (auto direction : directions) {
            if (neighbours[direction].Defined()) {
                result[direction] = neighbours[direction]->GetBits();
            } else {
                result[direction] = Nothing();
            }
        }

        return result;
    }

    TGeoHashStringNeighbours GetNeighbours(const TString& hashString) {
        TGeoHashStringNeighbours result;

        auto neighbours = TGeoHashDescriptor(
                              hashString)
                              .GetNeighbours();

        for (auto direction : directions) {
            if (neighbours[direction].Defined()) {
                result[direction] = neighbours[direction]->ToString();
            } else {
                result[direction] = Nothing();
            }
        }
        return result;
    }

    TVector<TString> GetChildren(const TString& hashString) {
        TVector<TString> result(Reserve(base32EncodeTable.size()));

        for (auto ch : base32EncodeTable) {
            result.push_back(hashString + ch);
        }
        return result;
    }
}
