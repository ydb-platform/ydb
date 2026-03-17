"""Tests for SSH connection support."""

import pytest

from aiodocker.exceptions import DockerError


def test_ssh_connector_import_error() -> None:
    """Test SSH connector raises DockerError when asyncssh not available."""
    # Mock missing asyncssh
    import aiodocker.ssh

    original_asyncssh = aiodocker.ssh.asyncssh
    aiodocker.ssh.asyncssh = None  # type: ignore[assignment]

    try:
        from aiodocker.ssh import SSHConnector

        with pytest.raises(DockerError, match="asyncssh is required"):
            SSHConnector("ssh://user@host")
    finally:
        aiodocker.ssh.asyncssh = original_asyncssh


def test_ssh_connector_invalid_url_scheme() -> None:
    """Test SSH connector rejects invalid URL schemes."""
    with pytest.raises(DockerError, match="Invalid SSH URL scheme"):
        from aiodocker.ssh import SSHConnector

        SSHConnector("http://user@host")


def test_ssh_connector_missing_hostname() -> None:
    """Test SSH connector requires hostname."""
    with pytest.raises(DockerError, match="SSH URL must include hostname"):
        from aiodocker.ssh import SSHConnector

        SSHConnector("ssh://user@")


def test_ssh_connector_invalid_port() -> None:
    """Test SSH connector validates port range."""
    with pytest.raises(ValueError, match="Port out of range"):
        from aiodocker.ssh import SSHConnector

        SSHConnector("ssh://user@host:70000")

    with pytest.raises(ValueError, match="Port could not be cast to integer"):
        from aiodocker.ssh import SSHConnector

        SSHConnector("ssh://user@host:-1")
