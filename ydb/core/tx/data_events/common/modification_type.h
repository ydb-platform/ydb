#pragma once
#include "util/generic/yexception.h"
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/protos/data_events.pb.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr::NEvWrite {
enum class EModificationType {
    Upsert,
    Insert,
    Update,
    Replace,
    Delete,
    Increment
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

    static bool NeedDefaultForNotInitializedColumns(const NEvWrite::EModificationType value) {
        switch (value) {
            case NEvWrite::EModificationType::Upsert:
            case NEvWrite::EModificationType::Delete:
            case NEvWrite::EModificationType::Update:
            case NEvWrite::EModificationType::Increment:
                return false;
            case NEvWrite::EModificationType::Insert:
            case NEvWrite::EModificationType::Replace:
                return true;
        }
    }

    static bool NeedSchemaRestore(const NEvWrite::EModificationType value) {
        switch (value) {
            case NEvWrite::EModificationType::Upsert:
            case NEvWrite::EModificationType::Delete:
            case NEvWrite::EModificationType::Update:
            case NEvWrite::EModificationType::Increment:
                return false;
            case NEvWrite::EModificationType::Insert:
            case NEvWrite::EModificationType::Replace:
                return true;
        }
    }

    static NKikimrDataEvents::TEvWrite::TOperation::EOperationType SerializeToWriteProto(const NEvWrite::EModificationType value) {
        switch (value) {
            case NEvWrite::EModificationType::Upsert:
                return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT;
            case NEvWrite::EModificationType::Insert:
                return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT;
            case NEvWrite::EModificationType::Delete:
                return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE;
            case NEvWrite::EModificationType::Replace:
                return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE;
            case NEvWrite::EModificationType::Update:
                return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE;
            case NEvWrite::EModificationType::Increment:
                return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INCREMENT;
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
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INCREMENT:
                return NEvWrite::EModificationType::Increment;
        }
    }

    static TProto SerializeToProto(const NEvWrite::EModificationType value) {
        switch (value) {
            case NEvWrite::EModificationType::Upsert:
                return NKikimrTxColumnShard::TEvWrite::OPERATION_UPSERT;
            case NEvWrite::EModificationType::Insert:
                return NKikimrTxColumnShard::TEvWrite::OPERATION_INSERT;
            case NEvWrite::EModificationType::Delete:
                return NKikimrTxColumnShard::TEvWrite::OPERATION_DELETE;
            case NEvWrite::EModificationType::Replace:
                return NKikimrTxColumnShard::TEvWrite::OPERATION_REPLACE;
            case NEvWrite::EModificationType::Update:
                return NKikimrTxColumnShard::TEvWrite::OPERATION_UPDATE;
            case NEvWrite::EModificationType::Increment:
                Y_ENSURE(false);
        }
    }

    static NEvWrite::EModificationType DeserializeFromProto(const NKikimrTxColumnShard::TEvWrite::EModificationType value) {
        switch (value) {
            case NKikimrTxColumnShard::TEvWrite::OPERATION_UPSERT:
                return NEvWrite::EModificationType::Upsert;
            case NKikimrTxColumnShard::TEvWrite::OPERATION_INSERT:
                return NEvWrite::EModificationType::Insert;
            case NKikimrTxColumnShard::TEvWrite::OPERATION_UPDATE:
                return NEvWrite::EModificationType::Update;
            case NKikimrTxColumnShard::TEvWrite::OPERATION_DELETE:
                return NEvWrite::EModificationType::Delete;
            case NKikimrTxColumnShard::TEvWrite::OPERATION_REPLACE:
                return NEvWrite::EModificationType::Replace;
        }
    }
};

}
