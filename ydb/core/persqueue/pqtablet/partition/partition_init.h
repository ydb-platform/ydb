#pragma once

#include <ydb/core/persqueue/pqtablet/blob/header.h>
#include <ydb/core/persqueue/public/utils.h>

#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/library/persqueue/counter_time_keeper/counter_time_keeper.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/set.h>


namespace NKikimr::NPQ {

class TInitializerStep;
class TPartition;
class TPartitionId;

struct TInitializionContext {
    std::optional<ui64> StartOffset;
    std::optional<ui64> EndOffset;
    std::deque<TString> DeletedKeys;
};


/**
 * This class execute independent steps of parttition actor initialization.
 * Each initialization step makes its own decision whether to perform it or not.
 */
class TInitializer {
    friend TInitializerStep;

public:
    TInitializer(TPartition* partition);

    void Execute(const TActorContext& ctx);

    bool Handle(STFUNC_SIG);

protected:
    void Next(const TActorContext& ctx);
    void Done(const TActorContext& ctx);

private:
    void DoNext(const TActorContext& ctx);

    TString LogPrefix() const;

    TPartition* Partition;

    bool InProgress;

    TVector<THolder<TInitializerStep>> Steps;
    std::vector<THolder<TInitializerStep>>::iterator CurrentStep;
    TInitializionContext Ctx;
};

/**
 * Its is independent initialization step.
 * Step begin a execution when method Execute called and ends it after metheod Done called.
 */
class TInitializerStep {
public:
    TInitializerStep(TInitializer* initializer, TString name, bool skipNewPartition);
    virtual ~TInitializerStep() = default;

    virtual void Execute(const TActorContext& ctx) = 0;
    virtual bool Handle(STFUNC_SIG);

    void RestartTablet(const std::string_view message) const;

    TPartition* Partition() const;
    const TPartitionId& PartitionId() const;
    const TString& TopicName() const;
    TInitializionContext& GetContext();

    const TString Name;
    const bool SkipNewPartition;

protected:
    void Done(const TActorContext& ctx);

    TString LogPrefix() const;

private:
    TInitializer* Initializer;
};


class TBaseKVStep: public TInitializerStep {
public:
    TBaseKVStep(TInitializer* initializer, TString name, bool skipNewPartition);

    bool Handle(STFUNC_SIG) override;
    virtual void Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx) = 0;
};


//
// Initialization steps
//

class TInitConfigStep: public TBaseKVStep {
public:
    TInitConfigStep(TInitializer* initializer);

    void Execute(const TActorContext& ctx) override;
    void Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx) override;
};

class TInitInternalFieldsStep: public TInitializerStep {
public:
    TInitInternalFieldsStep(TInitializer* initializer);

    void Execute(const TActorContext& ctx) override;
};

class TInitDiskStatusStep: public TBaseKVStep {
public:
    TInitDiskStatusStep(TInitializer* initializer);

    void Execute(const TActorContext& ctx) override;
    void Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx) override;
};

class TInitMetaStep: public TBaseKVStep {
    friend class TPartitionTestWrapper;
public:
    TInitMetaStep(TInitializer* initializer);

    void Execute(const TActorContext& ctx) override;
    void Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx) override;
private:
    void LoadMeta(const NKikimrClient::TResponse& kvResponse);
};

class TInitInfoRangeStep: public TBaseKVStep {
public:
    TInitInfoRangeStep(TInitializer* initializer);

    void Execute(const TActorContext& ctx) override;
    void Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx) override;

    void PostProcessing(const TActorContext& ctx);
};

class TInitDataRangeStep: public TBaseKVStep {
public:
    TInitDataRangeStep(TInitializer* initializer);

    void Execute(const TActorContext& ctx) override;
    void Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx) override;

private:
    void FillBlobsMetaData(const TActorContext& ctx);
    void FormHeadAndProceed();

    TVector<NKikimrClient::TKeyValueResponse::TReadRangeResult> Ranges;
};

class TDeleteKeysStep: public TBaseKVStep {
public:
    TDeleteKeysStep(TInitializer* initializer);

    void Execute(const TActorContext& ctx) override;
    void Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx) override;
};

class TInitMessageDeduplicatorStep: public TBaseKVStep {
public:
    TInitMessageDeduplicatorStep(TInitializer* initializer);

    void Execute(const TActorContext& ctx) override;
    void Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx) override;
};

class TInitDataStep: public TBaseKVStep {
public:
    TInitDataStep(TInitializer* initializer);

    void Execute(const TActorContext& ctx) override;
    void Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx) override;
};

class TInitEndWriteTimestampStep: public TInitializerStep {
public:
    TInitEndWriteTimestampStep(TInitializer* initializer);

    void Execute(const TActorContext& ctx) override;
};

class TInitFieldsStep: public TInitializerStep {
public:
    TInitFieldsStep(TInitializer* initializer);

    void Execute(const TActorContext& ctx) override;
};

} // NKikimr::NPQ
