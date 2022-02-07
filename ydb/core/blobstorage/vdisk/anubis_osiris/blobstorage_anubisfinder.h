#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TAnubisCandidates
    // A portion of candidates for removing from the Hull Database.
    // We find candidates iteratively, limiting number of candidates at
    // each quauntum, so we provide Finished and Pos fields, to understand
    // if got to the end and which position to start next quantum.
    ////////////////////////////////////////////////////////////////////////////
    struct TAnubisCandidates {
        static constexpr unsigned MaxCandidates = 1000u;

        TLogoBlobID Pos;                    // we stopped at this position
        TVector<TLogoBlobID> Candidates;    // suspected logoblobs

        // Statistics
        TInstant StartTime;
        TInstant BarriersTime;
        TInstant FinishTime;

        void AddCandidate(const TLogoBlobID &id) {
            Candidates.push_back(id);
        }

        bool Enough() const {
            return Candidates.size() >= MaxCandidates;
        }

        void SetPos(const TLogoBlobID &id) {
            Pos = id;
        }

        bool Finished() const {
            return Candidates.empty();
        }

        void Output(IOutputStream &str) const {
            str << "{Pos# " << Pos << " Candidates# [";
            bool prev = false;
            for (const auto &x : Candidates) {
                if (prev) {
                    str << " ";
                } else {
                    prev = true;
                }
                str << x;
            }
            str << "] BarriersDuration# " << (BarriersTime - StartTime)
            << " FinderDuration# " << (FinishTime - BarriersTime) << "}";
        }

        TString ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvAnubisCandidates
    ////////////////////////////////////////////////////////////////////////////
    struct TEvAnubisCandidates :
        public TEventLocal<TEvAnubisCandidates, TEvBlobStorage::EvAnubisCandidates>
    {
        TAnubisCandidates Candidates;

        TEvAnubisCandidates(TAnubisCandidates &&c)
            : Candidates(std::move(c))
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // CreateAnubisCandidatesFinder
    // Creates an actor to find Anubis Candidates. Subject to run in batch pool.
    ////////////////////////////////////////////////////////////////////////////
    IActor *CreateAnubisCandidatesFinder(
                const TIntrusivePtr<THullCtx> &hullCtx,
                const TActorId &parentId,
                const TLogoBlobID &pos,
                TLogoBlobsSnapshot &&logoBlobsSnap,
                TBarriersSnapshot &&barriersSnap);

} // NKikimr
