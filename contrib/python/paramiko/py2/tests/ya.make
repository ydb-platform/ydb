PY2TEST()

FORK_TESTS()
SIZE(MEDIUM)

TEST_SRCS(
    __init__.py
    conftest.py
    loop.py
    stub_sftp.py
    test_auth.py
    test_buffered_pipe.py
    test_channelfile.py
    test_client.py
    test_config.py
    test_file.py
    test_gssapi.py
    test_hostkeys.py
    test_kex.py
    test_kex_gss.py
    test_message.py
    test_packetizer.py
    test_pkey.py
    test_proxy.py
    test_sftp.py
    test_sftp_big.py
    test_ssh_exception.py
    test_ssh_gss.py
    test_transport.py
    test_util.py
    util.py
)

PEERDIR(
    contrib/python/paramiko
    contrib/python/mock
    contrib/python/invoke
)

NO_LINT()

END()
