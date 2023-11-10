#include "become_user.h"
#ifndef _linux_
namespace NYql {

void BecomeUser(const TString& username, const TString& groupname) {
    Y_UNUSED(username);
    Y_UNUSED(groupname);
}

void TurnOnBecomeUserAmbientCaps() {
}

void TurnOffBecomeUserAbility() {
}

void DumpCaps(const TString& title) {
    Y_UNUSED(title);
}

void SendSignalOnParentThreadExit(int signo)
{
    Y_UNUSED(signo);
}

}
#endif
