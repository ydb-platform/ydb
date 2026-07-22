#include "schemeshard__init_tx.h"

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

bool TSchemeShard::TTxInit::CreateScheme(TTransactionContext &txc) {
        if (!txc.DB.GetScheme().IsEmpty())
            return false;

        NIceDb::TNiceDb(txc.DB).Materialize<Schema>();

        return true;
    }

void TSchemeShard::TTxInit::CollectObjectsToClean() {
        THashSet<TPathId> underOperation;
        for (auto& item : Self->TxInFlight) {
            const TTxState& txState = item.second;
            underOperation.insert(txState.TargetPathId);
            if (txState.SourcePathId) {
                underOperation.insert(txState.SourcePathId);
            }
        }

        TablesToClean.clear();
        for (auto& tItem : Self->Tables) {
            TPathId pathId = tItem.first;
            TPathElement::TPtr path = Self->PathsById.at(pathId);

            if (path->IsTable() && path->Dropped() && !underOperation.contains(pathId)) {
                TablesToClean.push_back(pathId);
            }
        }

        BlockStoreVolumesToClean.clear();
        for (auto& xpair : Self->BlockStoreVolumes) {
            TPathId pathId = xpair.first;
            TPathElement::TPtr path = Self->PathsById.at(pathId);

            if (path->IsBlockStoreVolume() && path->Dropped() && !underOperation.contains(pathId)) {
                BlockStoreVolumesToClean.push_back(pathId);
            }
        }

        for (const auto& item : Self->PathsById) {
            if (item.second->DbRefCount == 0 && item.second->Dropped()) {
                Y_DEBUG_ABORT_UNLESS(!underOperation.contains(item.first));
                Self->CleanDroppedPathsCandidates.insert(item.first);
            }
        }

        for (const auto& item : Self->SubDomains) {
            if (!item.second->GetInternalShards().empty()) {
                continue;
            }
            auto path = Self->PathsById.at(item.first);
            if (path->DbRefCount == 1 && path->AllChildrenCount == 0 && path->Dropped()) {
                Y_DEBUG_ABORT_UNLESS(!underOperation.contains(item.first));
                Self->CleanDroppedSubDomainsCandidates.insert(item.first);
            }
        }
    }

TPathElement::TPtr TSchemeShard::TTxInit::MakePathElement(const TPathRec& rec) const {
        TPathId pathId = std::get<0>(rec);
        TPathId parentPathId = std::get<1>(rec);

        TString name = std::get<2>(rec);
        TString owner = std::get<3>(rec);

        TPathId domainId = Self->RootPathId();
        if (pathId != Self->RootPathId()) {
            Y_VERIFY_S(Self->PathsById.contains(parentPathId), "Parent path not found"
                           << ", pathId: " << pathId
                           << ", parentPathId: " << parentPathId);
            auto parent = Self->PathsById.at(parentPathId);
            if (parent->IsDomainRoot()) {
                domainId = parentPathId;
            } else {
                domainId = parent->DomainPathId;
            }
        }

        TPathElement::TPtr path = new TPathElement(pathId, parentPathId, domainId, name, owner);

        TString tempDirOwnerActorId_Deprecated;
        std::tie(
            std::ignore, //pathId
            std::ignore, //parentPathId
            std::ignore, //name
            std::ignore, //owner
            path->PathType,
            path->StepCreated,
            path->CreateTxId,
            path->StepDropped,
            path->DropTxId,
            path->ACL,
            path->LastTxId,
            path->DirAlterVersion,
            path->UserAttrs->AlterVersion,
            path->ACLVersion,
            tempDirOwnerActorId_Deprecated,
            path->TempDirOwnerActorId) = rec;

        path->PathState = TPathElement::EPathState::EPathStateNoChanges;
        if (path->StepDropped) {
            path->PathState = TPathElement::EPathState::EPathStateNotExist;
        }

        if (!path->TempDirOwnerActorId) {
            path->TempDirOwnerActorId.Parse(tempDirOwnerActorId_Deprecated.c_str(), tempDirOwnerActorId_Deprecated.size());
        }

        return path;
    }

TTxType TSchemeShard::TTxInit::GetTxType() const { return TXTYPE_INIT; }

bool TSchemeShard::TTxInit::Execute(TTransactionContext &txc, const TActorContext &ctx) {
        try {
            bool newScheme = CreateScheme(txc);
            if (newScheme)
                return true;
            return ReadEverything(txc, ctx);
        } catch (const TNotReadyTabletException &) {
            return false;
        } catch (const TSchemeErrorTabletException &ex) {
            Y_ABORT("there must be no leaked scheme error exceptions: %s", ex.what());
        } catch (const std::exception& ex) {
            Y_ABORT("there must be no leaked exceptions: %s", ex.what());
        } catch (...) {
            Y_ABORT("there must be no leaked exceptions");
        }
    }

void TSchemeShard::TTxInit::Complete(const TActorContext &ctx) {
        if (Broken) {
            return;
        }

        auto delayPublications = OnComplete.ExtractPublicationsToSchemeBoard(); //there no Populator exist jet
        for (auto& [txId, pathIds] : Publications) {
            std::move(pathIds.begin(), pathIds.end(), std::back_inserter(delayPublications[txId]));
        }

        OnComplete.ApplyOnComplete(Self, ctx);

        if (!Self->IsSchemeShardConfigured()) {
            if (Self->IsDomainSchemeShard) { // self initiation
                Self->Execute(Self->CreateTxInitRoot(), ctx);
            } else { // wait initiation msg
                Self->SignalTabletActive(ctx);
                Self->Become(&TSelf::StateConfigure);
            }
            return;
        }

        // flatten
        TVector<TPathId> cdcStreamScansToResume;
        for (auto& [_, v] : CdcStreamScansToResume) {
            std::move(v.begin(), v.end(), std::back_inserter(cdcStreamScansToResume));
        }

        Self->ActivateAfterInitialization(ctx, {
            .DelayPublications = std::move(delayPublications),
            .ExportIds = ExportsToResume,
            .ImportsIds = ImportsToResume,
            .CdcStreamScans = std::move(cdcStreamScansToResume),
            .TablesToClean = std::move(TablesToClean),
            .BlockStoreVolumesToClean = std::move(BlockStoreVolumesToClean),
            .RestoreTablesToUnmark = std::move(RestoreTablesToUnmark),
            .IncrementalBackupIds = std::move(IncrementalBackupsToResume),
            .FullBackupIds = std::move(FullBackupsToResume),
        });

        Self->ScheduleForcedCompactionProgress(ctx);
    }

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxInit() {
    return new TTxInit(this);
}

}}
