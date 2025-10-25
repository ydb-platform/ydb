#include "sector_map.h"

#include <ydb/library/pdisk_io/protos/sector_map.pb.h>

namespace NKikimr::NPDisk {

void TSectorMap::LoadFromFile(const TString& path) {
    TString raw = TFileInput(path).ReadAll();
    NKikimrPDisk::TSectorMapSnapshot snap;
    bool success = snap.ParseFromString(raw);
    Y_VERIFY_S(success, path);

    Map.reserve(snap.SectorsSize());
    for (auto& s : snap.GetSectors()) {
        Y_VERIFY_S(s.GetCompressionType() == NKikimrPDisk::ECompression::LZ4, path);
        Map[s.GetOffset()] = s.GetData();
    }
}

void TSectorMap::StoreToFile(const TString& path) {
    NKikimrPDisk::TSectorMapSnapshot snap;
    for (auto& [offset, dataVar] : Map) {
        NKikimrPDisk::TSectorData *sd = snap.AddSectors();
        sd->SetOffset(offset);
        if (std::holds_alternative<TString>(dataVar)) {
            sd->SetData(std::get<TString>(dataVar));
        } else {
            sd->SetData(std::get<std::pair<TString, TString>>(dataVar).second);
        }
        sd->SetCompressionType(NKikimrPDisk::ECompression::LZ4);
    }

    TString raw;
    Y_PROTOBUF_SUPPRESS_NODISCARD snap.SerializeToString(&raw);
    TFileOutput(path).Write(raw);
}

}
