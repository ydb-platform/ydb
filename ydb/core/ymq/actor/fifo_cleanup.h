#pragma once 
#include "defs.h" 
#include <ydb/core/ymq/actor/events.h>
#include <ydb/core/protos/services.pb.h>
 
#include <library/cpp/actors/core/actor.h>
 
namespace NKikimr::NSQS { 
 
class TCleanupActor : public TActorBootstrapped<TCleanupActor> { 
public: 
    enum class ECleanupType { 
        Deduplication, 
        Reads, 
    }; 
 
    TCleanupActor(const TQueuePath& queuePath, const TActorId& queueLeader, ECleanupType cleanupType);
    ~TCleanupActor(); 
 
    void Bootstrap(); 
 
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_CLEANUP_BACKGROUND_ACTOR;
    } 
 
private: 
    TDuration RandomCleanupPeriod(); 
 
    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev); 
    void HandlePoisonPill(TEvPoisonPill::TPtr&); 
    void HandleWakeup(); 
 
    void RunCleanupQuery(); 
 
    EQueryId GetCleanupQueryId() const; 
 
private: 
    STATEFN(StateFunc); 
 
private: 
    const TQueuePath QueuePath_; 
    const TString RequestId_; 
    const TActorId QueueLeader_;
    const ECleanupType CleanupType; 
    TString KeyRangeStart; 
}; 
 
} // namespace NKikimr::NSQS 
