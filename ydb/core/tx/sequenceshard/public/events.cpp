#include "events.h"

namespace NKikimr::NSequenceShard::TEvSequenceShard {

TEvMarkSchemeShardPipe::TEvMarkSchemeShardPipe(ui64 schemeShardId, ui64 generation, ui64 round) {
    Record.SetSchemeShardId(schemeShardId);
    Record.SetGeneration(generation);
    Record.SetRound(round);
}

void TEvCreateSequence::SetPathId(const TPathId& pathId) {
    auto* p = Record.MutablePathId();
    p->SetOwnerId(pathId.OwnerId);
    p->SetLocalId(pathId.LocalPathId);
}

TPathId TEvCreateSequence::GetPathId() const {
    const auto& p = Record.GetPathId();
    return TPathId(p.GetOwnerId(), p.GetLocalId());
}

TEvCreateSequence::TBuilder&& TEvCreateSequence::TBuilder::SetMinValue(i64 minValue) && {
    Msg->Record.SetMinValue(minValue);
    return std::move(*this);
}

TEvCreateSequence::TBuilder&& TEvCreateSequence::TBuilder::SetMaxValue(i64 maxValue) && {
    Msg->Record.SetMaxValue(maxValue);
    return std::move(*this);
}

TEvCreateSequence::TBuilder&& TEvCreateSequence::TBuilder::SetStartValue(i64 startValue) && {
    Msg->Record.SetStartValue(startValue);
    return std::move(*this);
}

TEvCreateSequence::TBuilder&& TEvCreateSequence::TBuilder::SetCache(ui64 cache) && {
    Msg->Record.SetCache(cache);
    return std::move(*this);
}

TEvCreateSequence::TBuilder&& TEvCreateSequence::TBuilder::SetIncrement(i64 increment) && {
    Msg->Record.SetIncrement(increment);
    return std::move(*this);
}

TEvCreateSequence::TBuilder&& TEvCreateSequence::TBuilder::SetCycle(bool cycle) && {
    Msg->Record.SetCycle(cycle);
    return std::move(*this);
}

TEvCreateSequence::TBuilder&& TEvCreateSequence::TBuilder::SetFrozen(bool frozen) && {
    Msg->Record.SetFrozen(frozen);
    return std::move(*this);
}

TEvCreateSequenceResult::TEvCreateSequenceResult(EStatus status, ui64 origin) {
    Record.SetStatus(status);
    Record.SetOrigin(origin);
}

TEvAllocateSequence::TEvAllocateSequence(const TPathId& pathId, ui64 cache) {
    SetPathId(pathId);
    Record.SetCache(cache);
}

void TEvAllocateSequence::SetPathId(const TPathId& pathId) {
    auto* p = Record.MutablePathId();
    p->SetOwnerId(pathId.OwnerId);
    p->SetLocalId(pathId.LocalPathId);
}

TPathId TEvAllocateSequence::GetPathId() const {
    const auto& p = Record.GetPathId();
    return TPathId(p.GetOwnerId(), p.GetLocalId());
}

TEvAllocateSequenceResult::TEvAllocateSequenceResult(EStatus status, ui64 origin) {
    Record.SetStatus(status);
    Record.SetOrigin(origin);
}

void TEvDropSequence::SetPathId(const TPathId& pathId) {
    auto* p = Record.MutablePathId();
    p->SetOwnerId(pathId.OwnerId);
    p->SetLocalId(pathId.LocalPathId);
}

TPathId TEvDropSequence::GetPathId() const {
    const auto& p = Record.GetPathId();
    return TPathId(p.GetOwnerId(), p.GetLocalId());
}

TEvDropSequenceResult::TEvDropSequenceResult(EStatus status, ui64 origin) {
    Record.SetStatus(status);
    Record.SetOrigin(origin);
}

void TEvUpdateSequence::SetPathId(const TPathId& pathId) {
    auto* p = Record.MutablePathId();
    p->SetOwnerId(pathId.OwnerId);
    p->SetLocalId(pathId.LocalPathId);
}

TPathId TEvUpdateSequence::GetPathId() const {
    const auto& p = Record.GetPathId();
    return TPathId(p.GetOwnerId(), p.GetLocalId());
}

TEvUpdateSequence::TBuilder&& TEvUpdateSequence::TBuilder::SetMinValue(i64 minValue) && {
    Msg->Record.SetMinValue(minValue);
    return std::move(*this);
}

TEvUpdateSequence::TBuilder&& TEvUpdateSequence::TBuilder::SetMaxValue(i64 maxValue) && {
    Msg->Record.SetMaxValue(maxValue);
    return std::move(*this);
}

TEvUpdateSequence::TBuilder&& TEvUpdateSequence::TBuilder::SetStartValue(i64 startValue) && {
    Msg->Record.SetStartValue(startValue);
    return std::move(*this);
}

TEvUpdateSequence::TBuilder&& TEvUpdateSequence::TBuilder::SetNextValue(i64 nextValue) && {
    Msg->Record.SetNextValue(nextValue);
    return std::move(*this);
}

TEvUpdateSequence::TBuilder&& TEvUpdateSequence::TBuilder::SetNextUsed(bool nextUsed) && {
    Msg->Record.SetNextUsed(nextUsed);
    return std::move(*this);
}

TEvUpdateSequence::TBuilder&& TEvUpdateSequence::TBuilder::SetCache(ui64 cache) && {
    Msg->Record.SetCache(cache);
    return std::move(*this);
}

TEvUpdateSequence::TBuilder&& TEvUpdateSequence::TBuilder::SetIncrement(i64 increment) && {
    Msg->Record.SetIncrement(increment);
    return std::move(*this);
}

TEvUpdateSequence::TBuilder&& TEvUpdateSequence::TBuilder::SetCycle(bool cycle) && {
    Msg->Record.SetCycle(cycle);
    return std::move(*this);
}

TEvUpdateSequenceResult::TEvUpdateSequenceResult(EStatus status, ui64 origin) {
    Record.SetStatus(status);
    Record.SetOrigin(origin);
}

void TEvFreezeSequence::SetPathId(const TPathId& pathId) {
    auto* p = Record.MutablePathId();
    p->SetOwnerId(pathId.OwnerId);
    p->SetLocalId(pathId.LocalPathId);
}

TPathId TEvFreezeSequence::GetPathId() const {
    const auto& p = Record.GetPathId();
    return TPathId(p.GetOwnerId(), p.GetLocalId());
}

TEvFreezeSequenceResult::TEvFreezeSequenceResult(EStatus status, ui64 origin) {
    Record.SetStatus(status);
    Record.SetOrigin(origin);
}

void TEvRestoreSequence::SetPathId(const TPathId& pathId) {
    auto* p = Record.MutablePathId();
    p->SetOwnerId(pathId.OwnerId);
    p->SetLocalId(pathId.LocalPathId);
}

TPathId TEvRestoreSequence::GetPathId() const {
    const auto& p = Record.GetPathId();
    return TPathId(p.GetOwnerId(), p.GetLocalId());
}

void TEvRestoreSequence::InitFrom(const NKikimrTxSequenceShard::TEvFreezeSequenceResult& record) {
    Record.SetMinValue(record.GetMinValue());
    Record.SetMaxValue(record.GetMaxValue());
    Record.SetStartValue(record.GetStartValue());
    Record.SetNextValue(record.GetNextValue());
    Record.SetNextUsed(record.GetNextUsed());
    Record.SetCache(record.GetCache());
    Record.SetIncrement(record.GetIncrement());
    Record.SetCycle(record.GetCycle());
}

void TEvRestoreSequence::InitFrom(const NKikimrTxSequenceShard::TEvGetSequenceResult& record) {
    Record.SetMinValue(record.GetMinValue());
    Record.SetMaxValue(record.GetMaxValue());
    Record.SetStartValue(record.GetStartValue());
    Record.SetNextValue(record.GetNextValue());
    Record.SetNextUsed(record.GetNextUsed());
    Record.SetCache(record.GetCache());
    Record.SetIncrement(record.GetIncrement());
    Record.SetCycle(record.GetCycle());
}

TEvRestoreSequenceResult::TEvRestoreSequenceResult(EStatus status, ui64 origin) {
    Record.SetStatus(status);
    Record.SetOrigin(origin);
}

TEvRedirectSequence::TEvRedirectSequence(const TPathId& pathId, ui64 redirectTo) {
    SetPathId(pathId);
    Record.SetRedirectTo(redirectTo);
}

void TEvRedirectSequence::SetPathId(const TPathId& pathId) {
    auto* p = Record.MutablePathId();
    p->SetOwnerId(pathId.OwnerId);
    p->SetLocalId(pathId.LocalPathId);
}

TPathId TEvRedirectSequence::GetPathId() const {
    const auto& p = Record.GetPathId();
    return TPathId(p.GetOwnerId(), p.GetLocalId());
}

TEvRedirectSequenceResult::TEvRedirectSequenceResult(EStatus status, ui64 origin) {
    Record.SetStatus(status);
    Record.SetOrigin(origin);
}

void TEvGetSequence::SetPathId(const TPathId& pathId) {
    auto* p = Record.MutablePathId();
    p->SetOwnerId(pathId.OwnerId);
    p->SetLocalId(pathId.LocalPathId);
}

TPathId TEvGetSequence::GetPathId() const {
    const auto& p = Record.GetPathId();
    return TPathId(p.GetOwnerId(), p.GetLocalId());
}

TEvGetSequenceResult::TEvGetSequenceResult(EStatus status, ui64 origin) {
    Record.SetStatus(status);
    Record.SetOrigin(origin);
}

void TEvAdvanceSequence::SetPathId(const TPathId& pathId) {
    auto* p = Record.MutablePathId();
    p->SetOwnerId(pathId.OwnerId);
    p->SetLocalId(pathId.LocalPathId);
}

TPathId TEvAdvanceSequence::GetPathId() const {
    const auto& p = Record.GetPathId();
    return TPathId(p.GetOwnerId(), p.GetLocalId());
}

TEvAdvanceSequence::TBuilder&& TEvAdvanceSequence::TBuilder::SetObserved(i64 observedMin, i64 observedMax) && {
    Msg->Record.SetObservedMin(observedMin);
    Msg->Record.SetObservedMax(observedMax);
    return std::move(*this);
}

TEvAdvanceSequenceResult::TEvAdvanceSequenceResult(EStatus status, ui64 origin) {
    Record.SetStatus(status);
    Record.SetOrigin(origin);
}

} // namespace NKikimr::NSequenceShard::TEvSequenceShard
