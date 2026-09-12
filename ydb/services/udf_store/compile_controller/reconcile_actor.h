#pragma once

#include <ydb/library/actors/core/actor.h>

#include <util/generic/string.h>

namespace NKikimr::NUdfStore {

//! Reads the keys of every finished artifact of one platform out of
//! `artifacts/{cpu_spec}` and answers with TEvControllerPrivate::
//! TEvReconcileResult. Runs as a child of the controller so that a slow
//! metadata read never sits on the tablet's own mailbox.
NActors::IActor* CreateReconcileActor(
    const NActors::TActorId& replyTo,
    const TString& cpuSpec,
    const TString& artifactTablePath);

} // namespace NKikimr::NUdfStore
