import library.python.svn_version as sv


def test_simple():
    assert sv.svn_version()
    assert isinstance(sv.svn_version(), str)

    assert sv.vcs()
    assert isinstance(sv.vcs(), str)

    # svn_revision() will be -1 on non-trunk commits via arc
    # svn revision of 0 technically may exist, but practiacally it will never appear here
    assert sv.svn_revision() >= 0 or (sv.vcs() != "svn" and sv.svn_revision() == -1)
    assert isinstance(sv.svn_revision(), int)

    # svn_last_revision() will be equal to zero on non-trunk commits
    assert sv.svn_last_revision() >= 0 or (sv.vcs() != "svn" and sv.svn_last_revision() == -1)
    assert isinstance(sv.svn_last_revision(), int)

    assert sv.commit_id()
    assert isinstance(sv.commit_id(), str)
    assert len(sv.commit_id()) > 0
    assert isinstance(sv.hash(), str)
    assert isinstance(sv.patch_number(), int)
