#include "become_user.h"

#ifdef _linux_
#include <ydb/library/yql/utils/sys/linux_version.h>

#include <util/generic/yexception.h>
#include <util/system/user.h>

#include <memory>
#include <vector>
#include <errno.h>

#include <grp.h>
#include <pwd.h>
#include <unistd.h>

#include <sys/prctl.h>
#include <contrib/libs/libcap/include/sys/capability.h>
#include <contrib/libs/libcap/include/sys/securebits.h>

// strange, but sometimes we have to specify values manually
#define PR_CAP_AMBIENT 47
#define PR_CAP_AMBIENT_IS_SET 1
#define PR_CAP_AMBIENT_RAISE 2
#define PR_CAP_AMBIENT_LOWER 3
#define PR_CAP_AMBIENT_CLEAR_ALL 4

namespace NYql {

namespace {

void SetCapFlag(cap_t caps, cap_flag_t flag, cap_value_t value) {
    if (cap_set_flag(caps, flag, 1, &value, CAP_SET) < 0) {
        throw TSystemError() << "cap_set_flag() failed, flag = " << static_cast<int>(flag) << ", value = " << value;
    }
}

void SetCapFlags(cap_t caps, cap_value_t value) {
    SetCapFlag(caps, CAP_EFFECTIVE, value);
    SetCapFlag(caps, CAP_PERMITTED, value);
    SetCapFlag(caps, CAP_INHERITABLE, value);
}

void ClearAmbientCapFlags() {
    // from man: PR_CAP_AMBIENT (since Linux 4.3)
    if (IsLinuxKernelBelow4_3()) {
        return;
    }

    if (prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_CLEAR_ALL, 0, 0, 0) < 0) {
        throw TSystemError() << "prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_CLEAR_ALL, ....) failed";
    }
}

void SetAmbientCapFlag(cap_value_t value) {
    if (IsLinuxKernelBelow4_3()) {
        return;
    }

    if (prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_RAISE, value, 0, 0) < 0) {
        throw TSystemError() << "prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_RAISE, ....) failed, value = " << value;
    }
}

void SetCapFlagsVector(const std::vector<cap_value_t>& flags) {
    cap_t caps = cap_init();
    std::unique_ptr<std::remove_reference_t<decltype(*caps)>, decltype(&cap_free)> capsHolder(caps, &cap_free);

    if (!caps) {
        throw TSystemError() << "cap_init() failed";
    }

    cap_clear(caps);

    for (auto f : flags) {
        SetCapFlags(caps, f);
    }

    if (cap_set_proc(caps) < 0) {
        throw TSystemError() << "cap_set_proc() failed";
    }

    ClearAmbientCapFlags();
    for (auto f : flags) {
        SetAmbientCapFlag(f);
    }
}

void EnsureCapFlagsVectorCannotBeRaised(const std::vector<cap_value_t>& flags) {
    for (auto f : flags) {
        try {
            // one-by-one
            SetCapFlagsVector({ f });
        } catch (const TSystemError&) {
            continue;
        }

        throw yexception() << "Cap flag " << f << " raised unexpectedly";
    }
}

void DoBecomeUser(const char* username, const char* groupname) {
    errno = 0;
    passwd* pw = getpwnam(username);
    if (pw == nullptr) {
        if (errno == 0) {
            ythrow yexception() << "unknown user: " << username;
        } else {
            ythrow TSystemError() << "can't get user info";
        }
    }

    if (groupname == nullptr || strlen(groupname) == 0) {
        groupname = username;
    }

    errno = 0;
    group* gr = getgrnam(groupname);
    if (gr == nullptr) {
        if (errno == 0) {
            ythrow yexception() << "unknown group: " << groupname;
        } else {
            ythrow TSystemError() << "can't get group info";
        }
    }

    if (setgid(gr->gr_gid) == -1) {
        ythrow TSystemError() << "can't change process group";
    }

    if (initgroups(username, gr->gr_gid) == -1) {
        ythrow TSystemError() << "can't initgroups";
    }

    if (setuid(pw->pw_uid) == -1) {
        ythrow TSystemError() << "can't change process user";
    }

    if (prctl(PR_SET_DUMPABLE, 1, 0, 0, 0) == -1) {
        ythrow TSystemError() << "can't set dumpable flag for a process";
    }
}

}

void BecomeUser(const TString& username, const TString& groupname) {
    DoBecomeUser(username.data(), groupname.data());
}

void TurnOnBecomeUserAmbientCaps() {
    SetCapFlagsVector({ CAP_SETUID, CAP_SETGID, CAP_SETPCAP, CAP_KILL });
    if (prctl(PR_SET_SECUREBITS, SECBIT_NO_SETUID_FIXUP | SECBIT_NO_SETUID_FIXUP_LOCKED, 0, 0, 0) == -1) {
        ythrow TSystemError() << "can't set secure bits for a process";
    }
}

void TurnOffBecomeUserAbility() {
    ClearAmbientCapFlags();
    SetCapFlagsVector({});
    EnsureCapFlagsVectorCannotBeRaised({ CAP_SETUID, CAP_SETGID, CAP_SETPCAP, CAP_KILL });

    // ensure we cannot get root access back
    if (setuid(0) != -1) {
        ythrow TSystemError() << "unexpected switch to root in TurnOffBecomeUserAbility";
    }
}

void DumpCaps(const TString& title) {
    cap_t caps = cap_get_proc();
    std::unique_ptr<std::remove_reference_t<decltype(*caps)>, decltype(&cap_free)> capsHolder(caps, &cap_free);

    ssize_t size;
    char* capsText = cap_to_text(caps, &size);
    Cerr << title << ": current user: " << GetUsername() << ", proc caps: " << capsText << Endl;

    cap_free(capsText);
}

void SendSignalOnParentThreadExit(int signo)
{
    if (::prctl(PR_SET_PDEATHSIG, signo) == -1) {
        ythrow TSystemError() << "Cannot set signal " << strsignal(signo) << " for parent death using prctl";
    }
}

}

#endif
