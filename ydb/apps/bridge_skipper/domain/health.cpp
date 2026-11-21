#include "health.h"

#include <ydb/public/api/protos/ydb_monitoring.pb.h>

#include <util/string/builder.h>

#include <algorithm>

namespace NBridgeSkipper {
namespace {

using TIssue = Ydb::Monitoring::IssueLog;
using TStatus = Ydb::Monitoring::StatusFlag;
using TIssuesById = std::map<TString, const TIssue*>;

bool IsGroup(const TIssue& issue) {
    return issue.type() == "BRIDGE_GROUP" || issue.type() == "STORAGE_GROUP";
}

bool IsFallbackMarker(const TIssue& issue) {
    return issue.type() == "STORAGE" && issue.status() == TStatus::RED
        && issue.message() == "System tablet BSC didn't provide information";
}

// A present decisive reason makes the part RED. Otherwise a missing reason, as
// left by truncation, or an ambiguous one makes it Incomplete. Otherwise it
// replicates if at least one reason does.
EPart ClassifyBscPart(const TIssue& group, const TIssuesById& issues) {
    bool replication = false;
    bool incomplete = false;
    // Returns false for a decisive reason.
    const auto inspectVDisk = [&](const TString& id) {
        const auto it = issues.find(id);
        if (it == issues.end()) {
            incomplete = true;
            return true;
        }
        const auto* issue = it->second;
        if (!issue) {
            // An ambiguous id proves nothing, like a reason missing after truncation.
            incomplete = true;
            return true;
        }
        if (issue->type() != "VDISK") {
            return false;
        }
        // The server counts only BLUE, RED and GREY disks as failed; throttling
        // during resync is ORANGE.
        if (issue->status() == TStatus::GREEN || issue->status() == TStatus::YELLOW
            || issue->status() == TStatus::ORANGE)
        {
            return true;
        }
        if (issue->status() == TStatus::BLUE && issue->message() == "Replication in progress") {
            replication = true;
            return true;
        }
        return false;
    };
    for (const auto& id : group.reason()) {
        const auto it = issues.find(id);
        if (it != issues.end() && it->second && it->second->type() == "BRIDGE_GROUP" && it->second->reason_size() != 0) {
            // An attributed logical STORAGE_GROUP can inherit a BRIDGE_GROUP's
            // pile. Inspect its VDisk reasons once; never traverse an open graph.
            for (const auto& childId : it->second->reason()) {
                if (!inspectVDisk(childId)) {
                    return EPart::RedPart;
                }
            }
        } else if (!inspectVDisk(id)) {
            return EPart::RedPart;
        }
    }
    if (incomplete) {
        return EPart::Incomplete;
    }
    // No reasons prove replication.
    return replication ? EPart::ReplicatingPart : EPart::RedPart;
}

// Static whiteboard fallback must retain a complete VDisk reason chain.
// Replication-only, absent, ambiguous and GREY reasons are not failure proof.
EPart ClassifyFallbackPart(const TIssue& group, const TIssuesById& issues) {
    if (group.type() != "STORAGE_GROUP" || group.reason().empty()) {
        return EPart::Incomplete;
    }
    bool failed = false;
    for (const auto& id : group.reason()) {
        const auto it = issues.find(id);
        if (it == issues.end() || !it->second || it->second->type() != "VDISK") {
            return EPart::Incomplete;
        }
        const auto& disk = *it->second;
        switch (disk.status()) {
            case TStatus::RED:
                failed = true;
                break;
            case TStatus::BLUE:
                if (disk.message() != "Replication in progress") {
                    return EPart::Incomplete;
                }
                break;
            case TStatus::GREEN:
            case TStatus::YELLOW:
            case TStatus::ORANGE:
                break;
            default:
                return EPart::Incomplete;
        }
    }
    return failed ? EPart::FallbackRed : EPart::Incomplete;
}

// The group is already RED; every contributing reason must prove device I/O
// failure. One device error mixed with transport loss or replication need not
// cross the group's erasure threshold and cannot override a live node quorum.
bool HasStorageFailure(const TIssue& group, const TIssuesById& issues) {
    const auto find = [&](const TString& id) -> const TIssue* {
        const auto it = issues.find(id);
        return it == issues.end() ? nullptr : it->second;
    };
    if (group.type() != "STORAGE_GROUP" || group.reason().empty() || find(group.id()) != &group) {
        return false;
    }
    const auto& pile = group.location().storage().pool().group().pile().name();
    const auto samePile = [&](const TIssue& issue) {
        const auto& location = issue.location().storage().pool().group();
        return location.has_pile() && location.pile().name() == pile;
    };
    for (const auto& id : group.reason()) {
        const auto* disk = find(id);
        if (!disk || disk->type() != "VDISK" || disk->status() != TStatus::RED
            || disk->reason().empty() || !samePile(*disk) || !disk->location().storage().node().id())
        {
            return false;
        }
        for (const auto& reason : disk->reason()) {
            const auto* pdisk = find(reason);
            if (!pdisk || pdisk->type() != "PDISK" || pdisk->status() != TStatus::RED
                || pdisk->message() != "PDisk state is DeviceIoError" || !pdisk->reason().empty()
                || !samePile(*pdisk)
                || pdisk->location().storage().node().id() != disk->location().storage().node().id())
            {
                return false;
            }
            const auto& devices = pdisk->location().storage().pool().group().vdisk().pdisk();
            if (devices.size() != 1 || devices.Get(0).id().empty()) {
                return false;
            }
        }
    }
    return true;
}

int Rank(EPart part) {
    return part == EPart::ReplicatingPart ? 0 : part == EPart::Incomplete ? 1 : 2;
}

void AddPart(TSelfCheckClassification& result, const TString& pile, EPart part) {
    const auto [it, inserted] = result.Parts.emplace(pile, part);
    if (!inserted && Rank(part) > Rank(it->second)) {
        it->second = part;
    }
}

void AddNote(TSelfCheckClassification& result, const TIssue& issue) {
    if (result.Notes.size() >= MaxSelfCheckNotes) {
        ++result.OmittedNotes;
        return;
    }
    TStringBuilder note;
    note << issue.type() << ' ' << Ydb::Monitoring::StatusFlag::Status_Name(issue.status());
    const auto& compute = issue.location().compute();
    if (compute.state_storage().has_pile()) {
        note << " pile=" << compute.state_storage().pile().name();
    } else if (compute.has_pile()) {
        note << " pile=" << compute.pile().name();
    }
    note << ": " << issue.message();
    result.Notes.push_back(std::move(note));
}

bool IsFresh(TMonotonic sentAt, TMonotonic now, const THealthParams& params) {
    return now - sentAt < params.EvidenceTtl;
}

bool IsEndpointValid(const TClusterView& view, const TEndpointState& endpoint) {
    return endpoint.Identity == EEndpointIdentity::Verified && endpoint.Current && view.Piles.contains(endpoint.Pile);
}

const TEndpointState* FindEndpoint(const THealthReplies& replies, const TString& name) {
    const auto it = std::find_if(replies.Endpoints.begin(), replies.Endpoints.end(), [&](const auto& endpoint) {
        return endpoint.Endpoint == name;
    });
    return it == replies.Endpoints.end() ? nullptr : &*it;
}

std::optional<EPart> Part(const TSelfCheckReply& reply, const TString& pile) {
    const auto it = reply.Classification.Parts.find(pile);
    return it == reply.Classification.Parts.end() ? std::nullopt : std::optional(it->second);
}

} // namespace

TSelfCheckClassification ClassifySelfCheck(const Ydb::Monitoring::SelfCheckResult& result) {
    TSelfCheckClassification classification;
    classification.SelfCheckResult = TString(Ydb::Monitoring::SelfCheck::Result_Name(result.self_check_result()));
    TIssuesById issues;
    for (const auto& issue : result.issue_log()) {
        const auto [it, inserted] = issues.emplace(issue.id(), &issue);
        if (!inserted) {
            it->second = nullptr;
        }
        if (IsFallbackMarker(issue)) {
            classification.Mode = ESelfCheckMode::Fallback;
        } else if (issue.type() == "STORAGE" || issue.type() == "BRIDGE_PILE" || issue.type().StartsWith("PILE_")) {
            AddNote(classification, issue);
        }
        if (issue.type() == "HEALTHCHECK_STATUS") {
            classification.Truncated |= issue.status() == TStatus::RED;
            classification.SizeWarning |= issue.status() == TStatus::YELLOW || issue.status() == TStatus::ORANGE;
        }
    }
    for (const auto& issue : result.issue_log()) {
        if (!IsGroup(issue) || (issue.status() != TStatus::RED && issue.status() != TStatus::GREY)) {
            continue;
        }
        const auto& group = issue.location().storage().pool().group();
        if (issue.status() == TStatus::GREY) {
            // Fallback "no data" is unknown, neither healthy nor failed (Ov 4.2.1).
            if (group.has_pile()) {
                AddPart(classification, group.pile().name(), EPart::Incomplete);
            }
            continue;
        }
        if (!group.has_pile()) {
            ++classification.UnnamedRed;
            continue;
        }
        const auto part = classification.Mode == ESelfCheckMode::Fallback
            ? ClassifyFallbackPart(issue, issues)
            : ClassifyBscPart(issue, issues);
        AddPart(classification, group.pile().name(), part);
        if (classification.Mode == ESelfCheckMode::Fallback && !classification.Truncated && part == EPart::FallbackRed
            && HasStorageFailure(issue, issues))
        {
            classification.StorageFailures.insert(group.pile().name());
        }
    }
    return classification;
}

bool IsConnectivityValid(
    const TClusterView& view,
    const TEndpointState& endpoint,
    const TConnectivity& report,
    TMonotonic now,
    const THealthParams& params)
{
    return IsEndpointValid(view, endpoint) && report.Generation == view.Generation
        && IsFresh(report.SentAt, now, params);
}

std::optional<bool> IsParticipating(
    const TString& pile,
    EPileState state,
    const std::vector<TConnectivityReply>& validReports)
{
    if (state == EPileState::Primary || state == EPileState::Promoted || state == EPileState::Synchronized) {
        return true;
    }
    if (state != EPileState::NotSynchronized) {
        return false;
    }
    std::optional<bool> participating;
    for (const auto& reply : validReports) {
        const auto& report = reply.Report;
        const auto it = report.Piles.find(pile);
        if (it != report.Piles.end() && (it->second.InConfigQuorum || !report.BindingInProgress)) {
            participating = participating.value_or(false) || it->second.InConfigQuorum;
        }
    }
    return participating;
}

std::optional<bool> EvaluatedParticipation(
    const TString& pile,
    EPileState state,
    const std::vector<TConnectivityReply>& validReports,
    const TPileParticipation& evaluated)
{
    const auto found = evaluated.find(pile);
    return found == evaluated.end() ? IsParticipating(pile, state, validReports) : found->second.Value;
}

THealthEvaluation EvaluatePiles(
    const TClusterView& view,
    const THealthReplies& replies,
    TMonotonic now,
    const THealthParams& params,
    THealthState& state)
{
    THealthEvaluation evaluation;
    std::optional<TString> primary;
    std::optional<TString> promoted;
    for (const auto& [pile, pileState] : view.Piles) {
        if (pileState == EPileState::Primary) {
            primary = pile;
        } else if (pileState == EPileState::Promoted) {
            promoted = pile;
        }
    }
    if (state.Primary != primary || state.Promoted != promoted) {
        if (state.Primary || state.Promoted) {
            state.RolesChangedAt = now;
        }
        state.CleanSince.clear();
        state.Primary = primary;
        state.Promoted = promoted;
    }

    if (state.Generation != view.Generation) {
        state.Generation = view.Generation;
        state.GenerationChangedAt = now;
    }
    auto fallbackFrom = state.GenerationChangedAt;
    if (state.RolesChangedAt) {
        fallbackFrom = std::max(fallbackFrom, *state.RolesChangedAt + params.BscWarmup);
    }

    std::vector<const TSelfCheckReply*> bsc;
    std::vector<const TSelfCheckReply*> fallback;
    for (const auto& reply : replies.SelfChecks) {
        const auto* endpoint = FindEndpoint(replies, reply.Endpoint);
        // Identity comes from the latest reply's pile (IsEndpointValid). A
        // reply without a pile keeps the endpoint's identity but is not evidence.
        if (!endpoint || !IsEndpointValid(view, *endpoint) || !reply.ResponderPile
            || !IsFresh(reply.SentAt, now, params))
        {
            continue;
        }
        (reply.Classification.Mode == ESelfCheckMode::Bsc ? bsc : fallback).push_back(&reply);
        evaluation.ValidUntil = std::min(evaluation.ValidUntil, reply.SentAt + params.EvidenceTtl);
    }
    // A fresh fallback reply from the BSC host says BSC is unavailable now.
    const auto& bscHost = promoted ? promoted : primary;
    bool hostFallback = false;
    for (const auto* reply : fallback) {
        hostFallback |= FindEndpoint(replies, reply->Endpoint)->Pile == bscHost;
    }
    // The first BSC replies after a restart can be clean by omission (Ov 3.2.5).
    bool modeSwitch = false;
    for (const auto* reply : bsc) {
        const auto it = state.EndpointModes.find(reply->Endpoint);
        modeSwitch |= it != state.EndpointModes.end() && it->second == ESelfCheckMode::Fallback;
    }
    if (modeSwitch) {
        state.CleanSince.clear();
    }
    evaluation.BscAvailable = !bsc.empty();
    for (const auto& reply : replies.Connectivity) {
        const auto* endpoint = FindEndpoint(replies, reply.Endpoint);
        if (endpoint && IsConnectivityValid(view, *endpoint, reply.Report, now, params)) {
            evaluation.Connectivity.push_back(reply);
            evaluation.ValidUntil = std::min(evaluation.ValidUntil, reply.Report.SentAt + params.EvidenceTtl);
        }
    }

    std::set<TString> keptPiles;
    for (const auto& [pile, pileState] : view.Piles) {
        auto participation = IsParticipating(pile, pileState, evaluation.Connectivity);
        TMonotonic participationAt = pileState == EPileState::NotSynchronized ? TMonotonic{} : view.SentAt;
        if (pileState == EPileState::NotSynchronized && participation) {
            for (const auto& reply : evaluation.Connectivity) {
                const auto& report = reply.Report;
                const auto item = report.Piles.find(pile);
                if (item != report.Piles.end() && item->second.InConfigQuorum == *participation
                    && (*participation || !report.BindingInProgress))
                {
                    participationAt = std::max(participationAt, report.SentAt);
                }
            }
        }
        // B7: BSC assesses only stage-2 parts, so a part for a NOT_SYNCHRONIZED pile proves stage 2.
        if (pileState == EPileState::NotSynchronized) {
            for (const auto* reply : bsc) {
                if (Part(*reply, pile)) {
                    if (!participation.value_or(false)) {
                        participation = true;
                        participationAt = reply->SentAt;
                    } else {
                        participationAt = std::max(participationAt, reply->SentAt);
                    }
                }
            }
        }
        evaluation.Participation[pile] = {participation, participationAt};
        // Unknown stage-2 participation is participating with an Unknown verdict.
        const bool participating = participation.value_or(true);
        if (participating) {
            evaluation.Participating.insert(pile);
        } else if (pileState != EPileState::Suspended) {
            continue;
        }
        auto& verdict = evaluation.Piles[pile];
        bool outsideFallback = false;
        bool allFallbackRed = true;
        bool allStorageFailed = true;
        TMonotonic newestFallback;
        for (const auto* reply : fallback) {
            const auto& responder = FindEndpoint(replies, reply->Endpoint)->Pile;
            verdict.FallbackRed |= Part(*reply, pile).has_value();
            verdict.OwnFallback |= responder == pile && reply->ResponderPile == pile
                && !reply->Classification.Truncated;
            if (responder != pile && reply->SentAt >= fallbackFrom) {
                outsideFallback = true;
                allFallbackRed &= reply->ResponderPile == responder && !reply->Classification.Truncated
                    && Part(*reply, pile) == EPart::FallbackRed;
                allStorageFailed &= reply->Classification.StorageFailures.contains(pile);
                newestFallback = std::max(newestFallback, reply->SentAt);
            }
        }
        bool outside = false;
        bool outsideLost = true;
        TMonotonic newestLost;
        for (const auto& reply : evaluation.Connectivity) {
            const auto& report = reply.Report;
            const auto it = report.Piles.find(pile);
            if (report.VantagePile == pile) {
                verdict.SelfQuorum |= it != report.Piles.end() && it->second.HasNodeQuorum;
            } else if (!report.BindingInProgress) {
                outside = true;
                outsideLost &= it != report.Piles.end() && it->second.InConfigQuorum && !it->second.HasNodeQuorum;
                newestLost = std::max(newestLost, report.SentAt);
            }
        }

        bool allRed = !bsc.empty();
        bool allRedOrReplicating = !bsc.empty();
        bool anyReplicating = false;
        bool clean = !bsc.empty() && !hostFallback && participation.value_or(false);
        TMonotonic newestBsc;
        for (const auto* reply : bsc) {
            const auto part = Part(*reply, pile);
            allRed &= part == EPart::RedPart;
            allRedOrReplicating &= part == EPart::RedPart || part == EPart::ReplicatingPart;
            anyReplicating |= part == EPart::ReplicatingPart;
            clean &= !part && !reply->Classification.Truncated;
            newestBsc = std::max(newestBsc, reply->SentAt);
        }
        // Cached replies from the previous BSC placement can still establish a
        // negative verdict, but cannot seed or advance the new clean window.
        clean &= !state.RolesChangedAt || newestBsc >= *state.RolesChangedAt;
        if (clean) {
            keptPiles.insert(pile);
            const auto [it, inserted] = state.CleanSince.try_emplace(pile);
            auto& window = it->second;
            if (inserted || newestBsc - window.NewestSentAt > params.EvidenceTtl) {
                window = {newestBsc, newestBsc};
            } else {
                window.NewestSentAt = std::max(window.NewestSentAt, newestBsc);
            }
        } else if (bsc.empty() && fallback.empty() && participation.value_or(false)) {
            keptPiles.insert(pile);
        } else {
            state.CleanSince.erase(pile);
        }
        if (!participation) {
            verdict.Reasons.push_back("participation unknown");
        } else if (participating && allRed) {
            verdict.Verdict = EPileVerdict::Failed;
            verdict.NewestEvidenceSentAt = newestBsc;
            verdict.Reasons.push_back("Every BSC-mode reply reports a RED group part");
        } else if (bscHost && pile == *bscHost && bsc.empty() && outsideFallback && allFallbackRed
            && (!verdict.SelfQuorum || allStorageFailed))
        {
            // A retained settled report can keep Lost evidence frozen while
            // newer reports are binding. Prefer the independently qualified
            // fallback failure so fresh samples can advance its confirmation.
            verdict.Verdict = EPileVerdict::Failed;
            verdict.FailureEvidence = allStorageFailed ? EFailureEvidence::Storage : EFailureEvidence::Fallback;
            verdict.NewestEvidenceSentAt = newestFallback;
            verdict.Reasons.push_back(allStorageFailed
                ? "Every current outside fallback reply proves a RED static group caused by PDisk DeviceIoError"
                : "Every current outside fallback reply reports a decisive RED static group part");
        } else if (outside && outsideLost && !verdict.SelfQuorum) {
            verdict.Verdict = EPileVerdict::Lost;
            verdict.NewestEvidenceSentAt = newestLost;
            verdict.Reasons.push_back("Outside reports lack node quorum and no inside report claims quorum");
        } else if (participating && allRedOrReplicating && anyReplicating) {
            verdict.Verdict = EPileVerdict::Replicating;
            verdict.NewestEvidenceSentAt = newestBsc;
            verdict.Reasons.push_back("BSC-mode group parts are RED or replication-only");
        } else if (clean && state.CleanSince.at(pile).NewestSentAt - state.CleanSince.at(pile).Since >= params.BscWarmup) {
            verdict.Verdict = EPileVerdict::Healthy;
            verdict.NewestEvidenceSentAt = newestBsc;
            verdict.Reasons.push_back("BSC-mode replies are continuously clean after warm-up");
        } else {
            verdict.NewestEvidenceSentAt = newestBsc;
            verdict.Reasons.push_back(pileState == EPileState::Suspended
                ? "Suspended pile has no conclusive Lost evidence"
                : clean ? "BSC cleanliness warm-up is incomplete" : "No conclusive BSC or connectivity verdict");
        }
    }
    for (auto it = state.CleanSince.begin(); it != state.CleanSince.end();) {
        if (!keptPiles.contains(it->first)) {
            it = state.CleanSince.erase(it);
        } else {
            ++it;
        }
    }
    for (const auto* reply : bsc) {
        state.EndpointModes[reply->Endpoint] = ESelfCheckMode::Bsc;
    }
    for (const auto* reply : fallback) {
        state.EndpointModes[reply->Endpoint] = ESelfCheckMode::Fallback;
    }
    return evaluation;
}

} // namespace NBridgeSkipper
