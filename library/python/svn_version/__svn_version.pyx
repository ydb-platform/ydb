import future.utils as fu

cdef extern from "library/cpp/svnversion/svnversion.h":
    cdef const char* GetVCS() except +;
    cdef const char* GetProgramSvnVersion() except +;
    cdef int GetProgramSvnRevision() except +;
    cdef int GetArcadiaLastChangeNum() except +;
    cdef const char* GetProgramCommitId() except +;
    cdef const char* GetProgramHash() except +;
    cdef const char* GetBranch() except +;
    cdef const char* GetTag() except +;
    cdef int GetArcadiaPatchNumber() except +;
    cdef int GetProgramBuildTimestamp() except +;

def svn_version():
    return fu.bytes_to_native_str(GetProgramSvnVersion())

def svn_revision():
    return GetProgramSvnRevision()

def svn_last_revision():
    return GetArcadiaLastChangeNum()

def commit_id():
    return fu.bytes_to_native_str(GetProgramCommitId())

def hash():
    return fu.bytes_to_native_str(GetProgramHash())

def svn_branch():
    return fu.bytes_to_native_str(GetBranch())

def svn_tag():
    return fu.bytes_to_native_str(GetTag())

def svn_timestamp():
    return GetProgramBuildTimestamp()

def patch_number():
    return GetArcadiaPatchNumber()

def vcs():
    return fu.bytes_to_native_str(GetVCS())
