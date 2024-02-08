#include "interconnect.h"
#include <util/digest/murmur.h>
#include <google/protobuf/text_format.h>

namespace NActors {

    TNodeLocation::TNodeLocation(const NActorsInterconnect::TNodeLocation& location) {
        const NProtoBuf::Descriptor *descriptor = NActorsInterconnect::TNodeLocation::descriptor();
        const NActorsInterconnect::TNodeLocation *locp = &location;
        NActorsInterconnect::TNodeLocation temp; // for legacy location case

        // WalleConfig compatibility section
        if (locp->HasBody()) {
            if (locp == &location) {
                temp.CopyFrom(*locp);
                locp = &temp;
            }
            temp.SetUnit(::ToString(temp.GetBody()));
            temp.ClearBody();
        }

        // legacy value processing
        if (locp->HasDataCenterNum() || locp->HasRoomNum() || locp->HasRackNum() || locp->HasBodyNum()) {
            if (locp == &location) {
                temp.CopyFrom(*locp);
                locp = &temp;
            }
            LegacyValue = TLegacyValue{temp.GetDataCenterNum(), temp.GetRoomNum(), temp.GetRackNum(), temp.GetBodyNum()};
            temp.ClearDataCenterNum();
            temp.ClearRoomNum();
            temp.ClearRackNum();
            temp.ClearBodyNum();

            const NProtoBuf::Reflection *reflection = temp.GetReflection();
            bool fieldsFromNewFormat = false;
            for (int i = 0, count = descriptor->field_count(); !fieldsFromNewFormat && i < count; ++i) {
                fieldsFromNewFormat |= reflection->HasField(temp, descriptor->field(i));
            }
            if (!fieldsFromNewFormat) {
                const auto& v = LegacyValue->DataCenter;
                const char *p = reinterpret_cast<const char*>(&v);
                temp.SetDataCenter(TString(p, strnlen(p, sizeof(ui32))));
                temp.SetModule(::ToString(LegacyValue->Room));
                temp.SetRack(::ToString(LegacyValue->Rack));
                temp.SetUnit(::ToString(LegacyValue->Body));
            }
        }

        auto makeString = [&] {
            NProtoBuf::TextFormat::Printer p;
            p.SetSingleLineMode(true);
            TString s;
            p.PrintToString(*locp, &s);
            return s;
        };

        // modern format parsing
        const NProtoBuf::Reflection *reflection = locp->GetReflection();
        for (int i = 0, count = descriptor->field_count(); i < count; ++i) {
            const NProtoBuf::FieldDescriptor *field = descriptor->field(i);
            if (reflection->HasField(*locp, field)) {
                Y_ABORT_UNLESS(field->type() == NProtoBuf::FieldDescriptor::TYPE_STRING, "Location# %s", makeString().data());
                Items.emplace_back(TKeys::E(field->number()), reflection->GetString(*locp, field));
            }
        }
        const NProtoBuf::UnknownFieldSet& unknown = locp->unknown_fields();
        for (int i = 0, count = unknown.field_count(); i < count; ++i) {
            const NProtoBuf::UnknownField& field = unknown.field(i);
            Y_ABORT_UNLESS(field.type() == NProtoBuf::UnknownField::TYPE_LENGTH_DELIMITED, "Location# %s", makeString().data());
            Items.emplace_back(TKeys::E(field.number()), field.length_delimited());
        }
        std::sort(Items.begin(), Items.end());
    }

    TNodeLocation::TNodeLocation(TFromSerialized, const TString& s)
        : TNodeLocation(ParseLocation(s))
    {}

    TNodeLocation::TNodeLocation(const TString& DataCenter, const TString& Module, const TString& Rack, const TString& Unit) {
        if (DataCenter) Items.emplace_back(TKeys::DataCenter, DataCenter);
        if (Module)     Items.emplace_back(TKeys::Module, Module);
        if (Rack)       Items.emplace_back(TKeys::Rack, Rack);
        if (Unit)       Items.emplace_back(TKeys::Unit, Unit);
    }

    NActorsInterconnect::TNodeLocation TNodeLocation::ParseLocation(const TString& s) {
        NActorsInterconnect::TNodeLocation res;
        const bool success = res.ParseFromString(s);
        Y_ABORT_UNLESS(success);
        return res;
    }

    TString TNodeLocation::ToStringUpTo(TKeys::E upToKey) const {
        const NProtoBuf::Descriptor *descriptor = NActorsInterconnect::TNodeLocation::descriptor();

        TStringBuilder res;
        for (const auto& [key, value] : Items) {
            if (upToKey < key) {
                break;
            }
            TString name;
            if (const NProtoBuf::FieldDescriptor *field = descriptor->FindFieldByNumber(key)) {
                name = field->options().GetExtension(NActorsInterconnect::PrintName);
            } else {
                name = ::ToString(int(key));
            }
            if (key != upToKey) {
                res << name << "=" << value << "/";
            } else {
                res << value;
            }
        }
        return res;
    }

    void TNodeLocation::Serialize(NActorsInterconnect::TNodeLocation *pb, bool compatibleWithOlderVersions) const {
        const NProtoBuf::Descriptor *descriptor = NActorsInterconnect::TNodeLocation::descriptor();
        const NProtoBuf::Reflection *reflection = pb->GetReflection();
        NProtoBuf::UnknownFieldSet *unknown = pb->mutable_unknown_fields();
        for (const auto& [key, value] : Items) {
            if (const NProtoBuf::FieldDescriptor *field = descriptor->FindFieldByNumber(key)) {
                reflection->SetString(pb, field, value);
            } else {
                unknown->AddLengthDelimited(key)->assign(value);
            }
        }
        if (compatibleWithOlderVersions) {
            GetLegacyValue().Serialize(pb);
        }
    }

    TString TNodeLocation::GetSerializedLocation() const {
        NActorsInterconnect::TNodeLocation pb;
        Serialize(&pb, false);
        TString s;
        const bool success = pb.SerializeToString(&s);
        Y_ABORT_UNLESS(success);
        return s;
    }

    TNodeLocation::TLegacyValue TNodeLocation::GetLegacyValue() const {
        if (LegacyValue) {
            return *LegacyValue;
        }

        ui32 dataCenterId = 0, moduleId = 0, rackId = 0, unitId = 0;

        for (const auto& [key, value] : Items) {
            switch (key) {
                case TKeys::DataCenter:
                    memcpy(&dataCenterId, value.data(), Min<size_t>(sizeof(dataCenterId), value.length()));
                    break;

                case TKeys::Module: {
                    const bool success = TryFromString(value, moduleId);
                    Y_ABORT_UNLESS(success);
                    break;
                }

                case TKeys::Rack:
                    // hacky way to obtain numeric id by a rack name
                    if (!TryFromString(value, rackId)) {
                        rackId = MurmurHash<ui32>(value.data(), value.length());
                    }
                    break;

                case TKeys::Unit: {
                    const bool success = TryFromString(value, unitId);
                    Y_ABORT_UNLESS(success);
                    break;
                }

                default:
                    Y_ABORT("unexpected legacy key# %d", key);
            }
        }

        return {dataCenterId, moduleId, rackId, unitId};
    }

} // NActors
