#pragma once

#include "abstract.h"

#include <ydb/library/accessor/accessor.h>

namespace NKikimrSchemeOp {
    class TFSSettings;
} // namespace NKikimrSchemeOp

namespace Ydb::Export {
    class ExportToFsSettings;
} // namespace Ydb::Export

namespace Ydb::Import {
    class ImportFromFsSettings;
} // namespace Ydb::Import

namespace NKikimr::NWrappers::NExternalStorage {

class TFsExternalStorageConfig: public IExternalStorageConfig {
private:
    YDB_READONLY_DEF(TString, BasePath);

protected:
    virtual TString DoGetStorageId() const override;
    virtual IExternalStorageOperator::TPtr DoConstructStorageOperator(bool verbose) const override;

public:
    TFsExternalStorageConfig(const NKikimrSchemeOp::TFSSettings& settings);
    TFsExternalStorageConfig(const Ydb::Export::ExportToFsSettings& settings);
    TFsExternalStorageConfig(const Ydb::Import::ImportFromFsSettings& settings);
};

} // NKikimr::NWrappers::NExternalStorage
