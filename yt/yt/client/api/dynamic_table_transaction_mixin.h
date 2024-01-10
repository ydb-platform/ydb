#pragma once

#include "transaction.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

class TDynamicTableTransactionMixin
    : public virtual ITransaction
    , public virtual IDynamicTableTransaction
{
public:
    void WriteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TUnversionedRow> rows,
        const TModifyRowsOptions& options = {},
        NTableClient::ELockType lockType = NTableClient::ELockType::Exclusive) override;
    void WriteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TVersionedRow> rows,
        const TModifyRowsOptions& options = {}) override;

    void DeleteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TLegacyKey> keys,
        const TModifyRowsOptions& options = {}) override;

    void LockRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TLegacyKey> keys,
        NTableClient::TLockMask lockMask) override;
    void LockRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TLegacyKey> keys,
        NTableClient::ELockType lockType = NTableClient::ELockType::SharedStrong) override;
    void LockRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TLegacyKey> keys,
        const std::vector<TString>& locks,
        NTableClient::ELockType lockType = NTableClient::ELockType::SharedStrong) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
