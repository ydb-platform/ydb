#pragma once

#include <util/generic/string.h>

namespace NYql {

// works on Linux only

// assume we have enough capabilities to do so: CAP_SETUID, CAP_SETGID
void BecomeUser(const TString& username, const TString& groupname);

// should be called by root (more specifically caps required: CAP_SETPCAP)
// special ambient capabilities will be set up: CAP_SETUID, CAP_SETGID, CAP_KILL
// they will be preserved by fork and exec*
void TurnOnBecomeUserAmbientCaps();

// forget ambient capabilities and ensure we cannot setuid to root
void TurnOffBecomeUserAbility();

// dump to stderr current secirity context incluing uid/guid/caps
void DumpCaps(const TString& title);

// subscribe child process on receiving signal on parent process death (particularly on parent thread exit)
void SendSignalOnParentThreadExit(int signo);

}
