#include "bill_record.h"

#include <library/cpp/json/json_writer.h>

#include <util/generic/size_literals.h>
#include <util/string/cast.h>

namespace NKikimr {

NJson::TJsonMap TBillRecord::TUsage::ToJson() const {
    return NJson::TJsonMap{
        {"type", ::ToString(Type_)},
        {"unit", ::ToString(Unit_)},
        {"quantity", Quantity_},
        {"start", Start_.Seconds()},
        {"finish", Finish_.Seconds()},
    };
}

TString TBillRecord::TUsage::ToString() const {
    return NJson::WriteJson(ToJson(), false);
}

NJson::TJsonMap TBillRecord::ToJson() const {
    auto json = NJson::TJsonMap{
        {"version", Version_},
        {"id", Id_},
        {"schema", Schema_},
        {"cloud_id", CloudId_},
        {"folder_id", FolderId_},
        {"resource_id", ResourceId_},
        {"source_id", SourceId_},
        {"source_wt", SourceWt_.Seconds()},
        {"tags", Tags_},
        {"usage", Usage_.ToJson()},
    };

    if (Labels_.IsMap() && Labels_.GetMap()) {
        json["labels"] = Labels_;
    }

    return json;
}

TString TBillRecord::ToString() const {
    TStringStream out;

    auto json = ToJson();
    NJson::WriteJson(&out, &json, false, false, false);

    out << Endl;
    return out.Str();
}

TBillRecord::TUsage TBillRecord::RequestUnits(ui64 quantity, TInstant start, TInstant finish) {
    return TUsage()
        .Type(TUsage::EType::Delta)
        .Unit(TUsage::EUnit::RequestUnit)
        .Quantity(quantity)
        .Start(start)
        .Finish(finish);
}

TBillRecord::TUsage TBillRecord::RequestUnits(ui64 quantity, TInstant now) {
    return RequestUnits(quantity, now, now);
}

}   // NKikimr
