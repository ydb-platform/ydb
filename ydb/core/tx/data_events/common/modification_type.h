#pragma once
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/protos/data_events.pb.h>

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

    static bool NeedSchemaRestore(const NEvWrite::EModificationType value) {
        switch (value) {
            case NEvWrite::EModificationType::Upsert:
            case NEvWrite::EModificationType::Delete:
            case NEvWrite::EModificationType::Update:
                return false;
            case NEvWrite::EModificationType::Insert:
            case NEvWrite::EModificationType::Replace:
                return true;
        }
    }

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

    static std::optional<NEvWrite::EModificationType> DeserializeFromProto(const NKikimrDataEvents::TEvWrite::TOperation::EOperationType value) {
        switch (value) {
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UNSPECIFIED:
                return {};
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT:
                return NEvWrite::EModificationType::Upsert;
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT:
                return NEvWrite::EModificationType::Insert;
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE:
                return NEvWrite::EModificationType::Update;
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE:
                return NEvWrite::EModificationType::Delete;
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE:
                return NEvWrite::EModificationType::Replace;
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
