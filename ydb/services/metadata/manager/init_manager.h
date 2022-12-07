#pragma once
#include <ydb/services/metadata/abstract/manager.h>
#include <ydb/services/metadata/manager/common.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NMetadata {

class TInitManagerBase: public NMetadata::IOperationsManager {
private:
    NThreading::TFuture<TObjectOperatorResult> BuildProcessingError() const;
protected:
    virtual NThreading::TFuture<TObjectOperatorResult> DoCreateObject(
        const NYql::TCreateObjectSettings& settings, const ui32 nodeId,
        IOperationsManager::TPtr manager, const TModificationContext& context) const override final;
    virtual NThreading::TFuture<TObjectOperatorResult> DoAlterObject(
        const NYql::TAlterObjectSettings& settings, const ui32 nodeId,
        IOperationsManager::TPtr manager, const TModificationContext& context) const override final;
    virtual NThreading::TFuture<TObjectOperatorResult> DoDropObject(
        const NYql::TDropObjectSettings& settings, const ui32 nodeId,
        IOperationsManager::TPtr manager, const TModificationContext& context) const override final;
public:
    virtual TString GetTablePath() const override final {
        Y_VERIFY(false);
        return "FAIL";
    }
};

}
