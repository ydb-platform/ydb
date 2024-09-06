#include "change_exchange.h"

namespace NKikimr::NChangeExchange {

/// TEvEnqueueRecords
TEvChangeExchange::TEvEnqueueRecords::TEvEnqueueRecords(const TVector<TRecordInfo>& records)
    : Records(records)
{
}

TEvChangeExchange::TEvEnqueueRecords::TEvEnqueueRecords(TVector<TRecordInfo>&& records)
    : Records(std::move(records))
{
}

TString TEvChangeExchange::TEvEnqueueRecords::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " Records [" << JoinSeq(",", Records) << "]"
    << " }";
}

TEvChangeExchange::TEvEnqueueRecords::TRecordInfo::TRecordInfo(ui64 order, const TPathId& pathId, ui64 bodySize)
    : Order(order)
    , PathId(pathId)
    , BodySize(bodySize)
{
}

void TEvChangeExchange::TEvEnqueueRecords::TRecordInfo::Out(IOutputStream& out) const {
    out << "{"
        << " Order: " << Order
        << " PathId: " << PathId
        << " BodySize: " << BodySize
    << " }";
}

/// TEvRequestRecords
TEvChangeExchange::TEvRequestRecords::TEvRequestRecords(const TVector<TRecordInfo>& records)
    : Records(records)
{
}

TEvChangeExchange::TEvRequestRecords::TEvRequestRecords(TVector<TRecordInfo>&& records)
    : Records(std::move(records))
{
}

TString TEvChangeExchange::TEvRequestRecords::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " Records [" << JoinSeq(",", Records) << "]"
    << " }";
}

TEvChangeExchange::TEvRequestRecords::TRecordInfo::TRecordInfo(ui64 order, ui64 bodySize)
    : Order(order)
    , BodySize(bodySize)
{
}

bool TEvChangeExchange::TEvRequestRecords::TRecordInfo::operator<(const TRecordInfo& rhs) const {
    return Order < rhs.Order;
}

void TEvChangeExchange::TEvRequestRecords::TRecordInfo::Out(IOutputStream& out) const {
    out << "{"
        << " Order: " << Order
        << " BodySize: " << BodySize
    << " }";
}

/// TEvRemoveRecords
TEvChangeExchange::TEvRemoveRecords::TEvRemoveRecords(const TVector<ui64>& records)
    : Records(records)
{
}

TEvChangeExchange::TEvRemoveRecords::TEvRemoveRecords(TVector<ui64>&& records)
    : Records(std::move(records))
{
}

TString TEvChangeExchange::TEvRemoveRecords::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " Records [" << JoinSeq(",", Records) << "]"
    << " }";
}

// TEvRecords
TEvChangeExchange::TEvRecords::TEvRecords(TChangeRecordVector&& records)
    : Records(std::move(records))
{
}

TString TEvChangeExchange::TEvRecords::ToString() const {
    auto visitor = [&](auto& records){
        return TStringBuilder() << ToStringHeader() << " {"
            << " Records " << reinterpret_cast<IChangeRecordContainer*>(records.get())->Out()
        << " }";
    };

    return std::visit(visitor, Records);
}

/// TEvForgetRecords
TEvChangeExchange::TEvForgetRecords::TEvForgetRecords(const TVector<ui64>& records)
    : Records(records)
{
}

TEvChangeExchange::TEvForgetRecords::TEvForgetRecords(TVector<ui64>&& records)
    : Records(std::move(records))
{
}

TString TEvChangeExchange::TEvForgetRecords::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " Records [" << JoinSeq(",", Records) << "]"
    << " }";
}

}
