import smtplib

import pytest

try:  # python 3
    from email.mime.text import MIMEText
except ImportError:  # python 2?
    from email.MIMEText import MIMEText

from pytest_localserver import plugin


smtp = pytest.importorskip("pytest_localserver.smtp")


def send_plain_email(to, from_, subject, txt, server=("localhost", 25)):
    """
    Sends a simple plain text message via SMTP.
    """
    if type(to) in (tuple, list):
        to = ", ".join(to)

    # Create a text/plain message
    msg = MIMEText(txt)
    msg["Subject"] = subject
    msg["From"] = from_
    msg["To"] = to

    host, port = server[:2]
    server = smtplib.SMTP(host, port)
    server.set_debuglevel(1)
    server.sendmail(from_, to, msg.as_string())
    server.quit()


# define test fixture here again in order to run tests without having to
# install the plugin anew every single time
smtpserver = plugin.smtpserver


def test_smtpserver_funcarg(smtpserver):
    assert isinstance(smtpserver, smtp.Server)
    assert smtpserver.is_alive()
    assert smtpserver.accepting and smtpserver.addr


def test_smtpserver_addr(smtpserver):
    host, port = smtpserver.addr
    assert isinstance(host, str)
    assert isinstance(port, int)
    assert port > 0


def test_server_is_killed(smtpserver):
    assert smtpserver.is_alive()
    smtpserver.stop()
    assert not smtpserver.is_alive()


def test_server_is_deleted(smtpserver):
    assert smtpserver.is_alive()
    smtpserver.__del__()  # need to call magic method here!
    assert not smtpserver.is_alive()


def test_smtpserver_has_empty_outbox_at_startup(smtpserver):
    assert len(smtpserver.outbox) == 0


def test_send_email(smtpserver):
    # send one e-mail
    send_plain_email(
        "alice@example.com",
        "webmaster@example.com",
        "Your e-mail is getting there",
        "Seems like this test actually works!",
        smtpserver.addr,
    )
    msg = smtpserver.outbox[-1]
    assert msg["To"] == "alice@example.com"
    assert msg["From"] == "webmaster@example.com"
    assert msg["Subject"] == "Your e-mail is getting there"
    assert msg.details.rcpttos == ["alice@example.com"]
    assert msg.details.peer
    assert msg.details.mailfrom

    # send another e-mail
    send_plain_email(
        "bob@example.com",
        "webmaster@example.com",
        "His e-mail too",
        "Seems like this test actually works!",
        smtpserver.addr,
    )

    msg = smtpserver.outbox[-1]
    assert msg["To"] == "bob@example.com"
    assert msg["From"] == "webmaster@example.com"
    assert msg["Subject"] == "His e-mail too"

    # two mails are now in outbox
    assert len(smtpserver.outbox) == 2
