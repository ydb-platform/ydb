#pragma once
#include <ydb/core/protos/tx_columnshard.pb.h>

namespace NKikimr::NEvWrite {
enum class EModificationType {
    Upsert,
    Insert,
    Update,
    Replace,
    Delete
};

}

namespace NKikimr {

template <class T>
class TEnumOperator {
public:
};

template <>
class TEnumOperator<NEvWrite::EModificationType> {
public:
    using TProto = NKikimrTxColumnShard::TEvWrite::EModificationType;
    static TProto SerializeToProto(const NEvWrite::EModificationType value) {
        switch (value) {
            case NEvWrite::EModificationType::Upsert:
                return NKikimrTxColumnShard::TEvWrite::UPSERT;
            case NEvWrite::EModificationType::Insert:
                return NKikimrTxColumnShard::TEvWrite::INSERT;
            case NEvWrite::EModificationType::Delete:
                return NKikimrTxColumnShard::TEvWrite::DELETE;
            case NEvWrite::EModificationType::Replace:
                return NKikimrTxColumnShard::TEvWrite::REPLACE;
            case NEvWrite::EModificationType::Update:
                return NKikimrTxColumnShard::TEvWrite::UPDATE;
        }
    }

    static NEvWrite::EModificationType DeserializeFromProto(const NKikimrTxColumnShard::TEvWrite::EModificationType value) {
        switch (value) {
            case NKikimrTxColumnShard::TEvWrite::UPSERT:
                return NEvWrite::EModificationType::Upsert;
            case NKikimrTxColumnShard::TEvWrite::INSERT:
                return NEvWrite::EModificationType::Insert;
            case NKikimrTxColumnShard::TEvWrite::UPDATE:
                return NEvWrite::EModificationType::Update;
            case NKikimrTxColumnShard::TEvWrite::DELETE:
                return NEvWrite::EModificationType::Delete;
            case NKikimrTxColumnShard::TEvWrite::REPLACE:
                return NEvWrite::EModificationType::Replace;
        }
    }
};

}
