#pragma once
#include <ydb/core/tx/columnshard/tx_reader/abstract.h>

namespace NKikimr::NColumnShard {
class TColumnShard;

}

namespace NKikimr::NColumnShard::NLoading {

class ITxShardInitReader: public ITxReader {
private:
    using TBase = ITxReader;

protected:
    TColumnShard* Self = nullptr;

public:
    ITxShardInitReader(const TString& name, TColumnShard* shard)
        : TBase(name)
        , Self(shard) {
    }
};

class TTxControllerInitializer: public ITxShardInitReader {
private:
    using TBase = ITxShardInitReader;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

public:
    using TBase::TBase;
};

class TOperationsManagerInitializer: public ITxShardInitReader {
private:
    using TBase = ITxShardInitReader;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

public:
    using TBase::TBase;
};

class TStoragesManagerInitializer: public ITxShardInitReader {
private:
    using TBase = ITxShardInitReader;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

public:
    using TBase::TBase;
};

class TDBLocksInitializer: public ITxShardInitReader {
private:
    using TBase = ITxShardInitReader;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

public:
    using TBase::TBase;
};

class TBackgroundSessionsInitializer: public ITxShardInitReader {
private:
    using TBase = ITxShardInitReader;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) override {
        return true;
    }

public:
    using TBase::TBase;
};

class TSharingSessionsInitializer: public ITxShardInitReader {
private:
    using TBase = ITxShardInitReader;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) override {
        return true;
    }

public:
    using TBase::TBase;
};

class TInFlightReadsInitializer: public ITxShardInitReader {
private:
    using TBase = ITxShardInitReader;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) override {
        return true;
    }

public:
    using TBase::TBase;
};

class TSpecialValuesInitializer: public ITxShardInitReader {
private:
    using TBase = ITxShardInitReader;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

public:
    using TBase::TBase;
};

class TTablesManagerInitializer: public ITxShardInitReader {
private:
    using TBase = ITxShardInitReader;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    virtual std::shared_ptr<ITxReader> BuildNextReaderAfterLoad() override;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

public:
    using TBase::TBase;
};

class TTiersManagerInitializer: public ITxShardInitReader {
private:
    using TBase = ITxShardInitReader;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

public:
    using TBase::TBase;
};

}   // namespace NKikimr::NColumnShard::NLoading
