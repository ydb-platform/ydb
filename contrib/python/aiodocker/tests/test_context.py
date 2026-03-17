from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from aiodocker.docker import Docker, DockerContextEndpoint
from aiodocker.exceptions import DockerContextInvalidError, DockerContextTLSError


class TestGetContextDirName:
    """Tests for Docker._get_context_dir_name()."""

    def test_computes_sha256_hash(self) -> None:
        """Context directory name should be SHA256 hash of context name."""
        context_name = "mycontext"
        expected = hashlib.sha256(context_name.encode("utf-8")).hexdigest()
        assert Docker._get_context_dir_name(context_name) == expected

    def test_known_hash_value(self) -> None:
        """Verify against a known hash value."""
        # echo -n "production" | sha256sum
        assert Docker._get_context_dir_name("production") == (
            "ab8e18ef4ebebeddc0b3152ce9c9006e14fc05242e3fc9ce32246ea6a9543074"
        )

    def test_empty_string(self) -> None:
        """Empty context name should still produce a valid hash."""
        # echo -n "" | sha256sum
        assert Docker._get_context_dir_name("") == (
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )

    def test_unicode_context_name(self) -> None:
        """Unicode context names should be encoded as UTF-8."""
        context_name = "контекст"  # Russian for "context"
        expected = hashlib.sha256(context_name.encode("utf-8")).hexdigest()
        assert Docker._get_context_dir_name(context_name) == expected


class TestGetDockerContextEndpoint:
    """Tests for Docker._get_docker_context_endpoint()."""

    def test_returns_none_when_no_config(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Should return None when ~/.docker/config.json doesn't exist."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.delenv("DOCKER_CONTEXT", raising=False)
        assert Docker._get_docker_context_endpoint() is None

    def test_returns_none_for_default_context(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Should return None when currentContext is 'default'."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.delenv("DOCKER_CONTEXT", raising=False)

        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        config_path = docker_dir / "config.json"
        config_path.write_text(json.dumps({"currentContext": "default"}))

        assert Docker._get_docker_context_endpoint() is None

    def test_returns_none_when_no_current_context(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Should return None when config.json has no currentContext."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.delenv("DOCKER_CONTEXT", raising=False)

        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        config_path = docker_dir / "config.json"
        config_path.write_text(json.dumps({}))

        assert Docker._get_docker_context_endpoint() is None

    def test_reads_context_from_config_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Should read context from config.json and return endpoint."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.delenv("DOCKER_CONTEXT", raising=False)

        # Create config.json with currentContext
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        config_path = docker_dir / "config.json"
        config_path.write_text(json.dumps({"currentContext": "mycontext"}))

        # Create context meta.json
        context_hash = Docker._get_context_dir_name("mycontext")
        meta_dir = docker_dir / "contexts" / "meta" / context_hash
        meta_dir.mkdir(parents=True)
        meta_path = meta_dir / "meta.json"
        meta_path.write_text(
            json.dumps({
                "Name": "mycontext",
                "Endpoints": {"docker": {"Host": "tcp://192.168.1.100:2375"}},
            })
        )

        endpoint = Docker._get_docker_context_endpoint()
        assert endpoint is not None
        assert endpoint.host == "tcp://192.168.1.100:2375"
        assert endpoint.skip_tls_verify is False
        assert endpoint.tls_ca is None
        assert endpoint.tls_cert is None
        assert endpoint.tls_key is None

    def test_docker_context_env_takes_precedence(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """DOCKER_CONTEXT env var should override config.json."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("DOCKER_CONTEXT", "envcontext")

        # Create config.json with different currentContext
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        config_path = docker_dir / "config.json"
        config_path.write_text(json.dumps({"currentContext": "configcontext"}))

        # Create meta.json for envcontext (from env var)
        env_context_hash = Docker._get_context_dir_name("envcontext")
        meta_dir = docker_dir / "contexts" / "meta" / env_context_hash
        meta_dir.mkdir(parents=True)
        meta_path = meta_dir / "meta.json"
        meta_path.write_text(
            json.dumps({
                "Name": "envcontext",
                "Endpoints": {"docker": {"Host": "unix:///custom/docker.sock"}},
            })
        )

        # Create meta.json for configcontext (should NOT be used)
        config_context_hash = Docker._get_context_dir_name("configcontext")
        meta_dir2 = docker_dir / "contexts" / "meta" / config_context_hash
        meta_dir2.mkdir(parents=True)
        meta_path2 = meta_dir2 / "meta.json"
        meta_path2.write_text(
            json.dumps({
                "Name": "configcontext",
                "Endpoints": {"docker": {"Host": "tcp://wrong-host:2375"}},
            })
        )

        # Should use envcontext, not configcontext
        endpoint = Docker._get_docker_context_endpoint()
        assert endpoint is not None
        assert endpoint.host == "unix:///custom/docker.sock"

    def test_docker_context_env_default_returns_none(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """DOCKER_CONTEXT=default should return None."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("DOCKER_CONTEXT", "default")

        assert Docker._get_docker_context_endpoint() is None

    def test_raises_error_when_context_not_found(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Should raise DockerContextInvalidError when context directory doesn't exist."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("DOCKER_CONTEXT", "nonexistent")

        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()

        with pytest.raises(DockerContextInvalidError) as exc_info:
            Docker._get_docker_context_endpoint()
        assert exc_info.value.context_name == "nonexistent"
        assert "not found" in exc_info.value.message

    def test_raises_error_on_invalid_json_config(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Should raise DockerContextInvalidError when config.json contains invalid JSON."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.delenv("DOCKER_CONTEXT", raising=False)

        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        config_path = docker_dir / "config.json"
        config_path.write_text("not valid json {{{")

        with pytest.raises(DockerContextInvalidError) as exc_info:
            Docker._get_docker_context_endpoint()
        assert "Invalid JSON in Docker config file" in exc_info.value.message

    def test_raises_error_on_invalid_json_meta(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Should raise DockerContextInvalidError when meta.json contains invalid JSON."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("DOCKER_CONTEXT", "badmeta")

        docker_dir = tmp_path / ".docker"
        context_hash = Docker._get_context_dir_name("badmeta")
        meta_dir = docker_dir / "contexts" / "meta" / context_hash
        meta_dir.mkdir(parents=True)
        meta_path = meta_dir / "meta.json"
        meta_path.write_text("invalid json")

        with pytest.raises(DockerContextInvalidError) as exc_info:
            Docker._get_docker_context_endpoint()
        assert exc_info.value.context_name == "badmeta"
        assert "Invalid JSON in context metadata file" in exc_info.value.message

    def test_raises_error_on_missing_endpoints_key(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Should raise DockerContextInvalidError when meta.json is missing Endpoints."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("DOCKER_CONTEXT", "noendpoints")

        docker_dir = tmp_path / ".docker"
        context_hash = Docker._get_context_dir_name("noendpoints")
        meta_dir = docker_dir / "contexts" / "meta" / context_hash
        meta_dir.mkdir(parents=True)
        meta_path = meta_dir / "meta.json"
        meta_path.write_text(json.dumps({"Name": "noendpoints"}))

        with pytest.raises(DockerContextInvalidError) as exc_info:
            Docker._get_docker_context_endpoint()
        assert exc_info.value.context_name == "noendpoints"
        assert "Missing required field" in exc_info.value.message

    def test_raises_error_on_missing_docker_endpoint(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Should raise DockerContextInvalidError when meta.json has no docker endpoint."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("DOCKER_CONTEXT", "nodocker")

        docker_dir = tmp_path / ".docker"
        context_hash = Docker._get_context_dir_name("nodocker")
        meta_dir = docker_dir / "contexts" / "meta" / context_hash
        meta_dir.mkdir(parents=True)
        meta_path = meta_dir / "meta.json"
        meta_path.write_text(
            json.dumps({"Name": "nodocker", "Endpoints": {"other": {"Host": "foo"}}})
        )

        with pytest.raises(DockerContextInvalidError) as exc_info:
            Docker._get_docker_context_endpoint()
        assert exc_info.value.context_name == "nodocker"
        assert "Missing required field" in exc_info.value.message

    def test_raises_error_on_missing_host_field(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Should raise DockerContextInvalidError when docker endpoint has no Host."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("DOCKER_CONTEXT", "nohost")

        docker_dir = tmp_path / ".docker"
        context_hash = Docker._get_context_dir_name("nohost")
        meta_dir = docker_dir / "contexts" / "meta" / context_hash
        meta_dir.mkdir(parents=True)
        meta_path = meta_dir / "meta.json"
        meta_path.write_text(
            json.dumps({
                "Name": "nohost",
                "Endpoints": {"docker": {"SkipTLSVerify": True}},
            })
        )

        with pytest.raises(DockerContextInvalidError) as exc_info:
            Docker._get_docker_context_endpoint()
        assert exc_info.value.context_name == "nohost"
        assert "Host" in exc_info.value.message

    def test_handles_unix_socket_host(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Should correctly return Unix socket paths."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("DOCKER_CONTEXT", "unixctx")

        docker_dir = tmp_path / ".docker"
        context_hash = Docker._get_context_dir_name("unixctx")
        meta_dir = docker_dir / "contexts" / "meta" / context_hash
        meta_dir.mkdir(parents=True)
        meta_path = meta_dir / "meta.json"
        meta_path.write_text(
            json.dumps({
                "Name": "unixctx",
                "Endpoints": {"docker": {"Host": "unix:///var/run/docker.sock"}},
            })
        )

        endpoint = Docker._get_docker_context_endpoint()
        assert endpoint is not None
        assert endpoint.host == "unix:///var/run/docker.sock"

    def test_handles_npipe_host(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Should correctly return Windows named pipe paths."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("DOCKER_CONTEXT", "winctx")

        docker_dir = tmp_path / ".docker"
        context_hash = Docker._get_context_dir_name("winctx")
        meta_dir = docker_dir / "contexts" / "meta" / context_hash
        meta_dir.mkdir(parents=True)
        meta_path = meta_dir / "meta.json"
        meta_path.write_text(
            json.dumps({
                "Name": "winctx",
                "Endpoints": {"docker": {"Host": "npipe:////./pipe/docker_engine"}},
            })
        )

        endpoint = Docker._get_docker_context_endpoint()
        assert endpoint is not None
        assert endpoint.host == "npipe:////./pipe/docker_engine"

    def test_reads_skip_tls_verify(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Should read SkipTLSVerify from endpoint config."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("DOCKER_CONTEXT", "tlsctx")

        docker_dir = tmp_path / ".docker"
        context_hash = Docker._get_context_dir_name("tlsctx")
        meta_dir = docker_dir / "contexts" / "meta" / context_hash
        meta_dir.mkdir(parents=True)
        meta_path = meta_dir / "meta.json"
        meta_path.write_text(
            json.dumps({
                "Name": "tlsctx",
                "Endpoints": {
                    "docker": {
                        "Host": "tcp://secure-host:2376",
                        "SkipTLSVerify": True,
                    }
                },
            })
        )

        endpoint = Docker._get_docker_context_endpoint()
        assert endpoint is not None
        assert endpoint.host == "tcp://secure-host:2376"
        assert endpoint.skip_tls_verify is True


class TestLoadContextTLS:
    """Tests for Docker._load_context_tls()."""

    def test_returns_none_when_dir_not_exists(self, tmp_path: Path) -> None:
        """Should return None values when TLS directory doesn't exist."""
        tls_dir = tmp_path / "nonexistent"
        ca, cert, key = Docker._load_context_tls(tls_dir)
        assert ca is None
        assert cert is None
        assert key is None

    def test_loads_ca_cert(self, tmp_path: Path) -> None:
        """Should load CA certificate when present."""
        tls_dir = tmp_path / "tls"
        tls_dir.mkdir()
        ca_content = b"-----BEGIN CERTIFICATE-----\nCA CERT\n-----END CERTIFICATE-----"
        (tls_dir / "ca.pem").write_bytes(ca_content)

        ca, cert, key = Docker._load_context_tls(tls_dir)
        assert ca == ca_content
        assert cert is None
        assert key is None

    def test_loads_client_cert_and_key(self, tmp_path: Path) -> None:
        """Should load client certificate and key when present."""
        tls_dir = tmp_path / "tls"
        tls_dir.mkdir()
        cert_content = (
            b"-----BEGIN CERTIFICATE-----\nCLIENT CERT\n-----END CERTIFICATE-----"
        )
        key_content = b"-----BEGIN RSA PRIVATE KEY-----\nPRIVATE KEY\n-----END RSA PRIVATE KEY-----"
        (tls_dir / "cert.pem").write_bytes(cert_content)
        (tls_dir / "key.pem").write_bytes(key_content)

        ca, cert, key = Docker._load_context_tls(tls_dir)
        assert ca is None
        assert cert == cert_content
        assert key == key_content

    def test_loads_all_tls_files(self, tmp_path: Path) -> None:
        """Should load all TLS files when present."""
        tls_dir = tmp_path / "tls"
        tls_dir.mkdir()
        ca_content = b"CA"
        cert_content = b"CERT"
        key_content = b"KEY"
        (tls_dir / "ca.pem").write_bytes(ca_content)
        (tls_dir / "cert.pem").write_bytes(cert_content)
        (tls_dir / "key.pem").write_bytes(key_content)

        ca, cert, key = Docker._load_context_tls(tls_dir)
        assert ca == ca_content
        assert cert == cert_content
        assert key == key_content

    def test_raises_error_on_unreadable_ca_file(self, tmp_path: Path) -> None:
        """Should raise DockerContextTLSError when CA file exists but cannot be read."""
        tls_dir = tmp_path / "tls"
        tls_dir.mkdir()
        ca_path = tls_dir / "ca.pem"
        ca_path.write_bytes(b"CA")
        # Make the file unreadable
        ca_path.chmod(0o000)

        try:
            with pytest.raises(DockerContextTLSError) as exc_info:
                Docker._load_context_tls(tls_dir, context_name="testctx")
            assert "Failed to read CA certificate" in exc_info.value.message
            assert exc_info.value.context_name == "testctx"
        finally:
            # Restore permissions for cleanup
            ca_path.chmod(0o644)

    def test_raises_error_on_unreadable_cert_file(self, tmp_path: Path) -> None:
        """Should raise DockerContextTLSError when cert file exists but cannot be read."""
        tls_dir = tmp_path / "tls"
        tls_dir.mkdir()
        cert_path = tls_dir / "cert.pem"
        cert_path.write_bytes(b"CERT")
        # Make the file unreadable
        cert_path.chmod(0o000)

        try:
            with pytest.raises(DockerContextTLSError) as exc_info:
                Docker._load_context_tls(tls_dir, context_name="testctx")
            assert "Failed to read client certificate" in exc_info.value.message
            assert exc_info.value.context_name == "testctx"
        finally:
            # Restore permissions for cleanup
            cert_path.chmod(0o644)

    def test_raises_error_on_unreadable_key_file(self, tmp_path: Path) -> None:
        """Should raise DockerContextTLSError when key file exists but cannot be read."""
        tls_dir = tmp_path / "tls"
        tls_dir.mkdir()
        key_path = tls_dir / "key.pem"
        key_path.write_bytes(b"KEY")
        # Make the file unreadable
        key_path.chmod(0o000)

        try:
            with pytest.raises(DockerContextTLSError) as exc_info:
                Docker._load_context_tls(tls_dir, context_name="testctx")
            assert "Failed to read private key" in exc_info.value.message
            assert exc_info.value.context_name == "testctx"
        finally:
            # Restore permissions for cleanup
            key_path.chmod(0o644)


class TestGetDockerContextEndpointWithTLS:
    """Tests for Docker._get_docker_context_endpoint() with TLS files."""

    def test_loads_tls_files_from_context(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Should load TLS files when present in context directory."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("DOCKER_CONTEXT", "securectx")

        docker_dir = tmp_path / ".docker"
        context_hash = Docker._get_context_dir_name("securectx")

        # Create meta.json
        meta_dir = docker_dir / "contexts" / "meta" / context_hash
        meta_dir.mkdir(parents=True)
        meta_path = meta_dir / "meta.json"
        meta_path.write_text(
            json.dumps({
                "Name": "securectx",
                "Endpoints": {"docker": {"Host": "tcp://secure:2376"}},
            })
        )

        # Create TLS files
        tls_dir = docker_dir / "contexts" / "tls" / context_hash / "docker"
        tls_dir.mkdir(parents=True)
        ca_content = b"CA DATA"
        cert_content = b"CERT DATA"
        key_content = b"KEY DATA"
        (tls_dir / "ca.pem").write_bytes(ca_content)
        (tls_dir / "cert.pem").write_bytes(cert_content)
        (tls_dir / "key.pem").write_bytes(key_content)

        endpoint = Docker._get_docker_context_endpoint()
        assert endpoint is not None
        assert endpoint.host == "tcp://secure:2376"
        assert endpoint.context_name == "securectx"
        assert endpoint.tls_ca == ca_content
        assert endpoint.tls_cert == cert_content
        assert endpoint.tls_key == key_content
        assert endpoint.has_tls is True

    def test_has_tls_false_without_certs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """has_tls should be False when no TLS files present."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("DOCKER_CONTEXT", "nontlsctx")

        docker_dir = tmp_path / ".docker"
        context_hash = Docker._get_context_dir_name("nontlsctx")

        # Create meta.json only (no TLS files)
        meta_dir = docker_dir / "contexts" / "meta" / context_hash
        meta_dir.mkdir(parents=True)
        meta_path = meta_dir / "meta.json"
        meta_path.write_text(
            json.dumps({
                "Name": "nontlsctx",
                "Endpoints": {"docker": {"Host": "tcp://insecure:2375"}},
            })
        )

        endpoint = Docker._get_docker_context_endpoint()
        assert endpoint is not None
        assert endpoint.has_tls is False


class TestDockerContextEndpoint:
    """Tests for DockerContextEndpoint dataclass."""

    def test_has_tls_with_ca(self) -> None:
        """has_tls should be True when CA is present."""
        endpoint = DockerContextEndpoint(host="tcp://host:2376", tls_ca=b"CA")
        assert endpoint.has_tls is True

    def test_has_tls_with_cert(self) -> None:
        """has_tls should be True when cert is present."""
        endpoint = DockerContextEndpoint(host="tcp://host:2376", tls_cert=b"CERT")
        assert endpoint.has_tls is True

    def test_has_tls_false_without_any(self) -> None:
        """has_tls should be False when no TLS data."""
        endpoint = DockerContextEndpoint(host="tcp://host:2376")
        assert endpoint.has_tls is False

    def test_default_skip_tls_verify(self) -> None:
        """skip_tls_verify should default to False."""
        endpoint = DockerContextEndpoint(host="tcp://host:2376")
        assert endpoint.skip_tls_verify is False

    def test_default_context_name(self) -> None:
        """context_name should default to None."""
        endpoint = DockerContextEndpoint(host="tcp://host:2376")
        assert endpoint.context_name is None

    def test_context_name_set(self) -> None:
        """context_name should be set when provided."""
        endpoint = DockerContextEndpoint(host="tcp://host:2376", context_name="myctx")
        assert endpoint.context_name == "myctx"


class TestDockerConstructorContextParameter:
    """Tests for Docker.__init__() context parameter."""

    async def test_context_parameter_loads_named_context(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Docker(context='mycontext') should load the specified context."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.delenv("DOCKER_HOST", raising=False)
        monkeypatch.delenv("DOCKER_CONTEXT", raising=False)

        # Create context files
        docker_dir = tmp_path / ".docker"
        context_hash = Docker._get_context_dir_name("mycontext")
        meta_dir = docker_dir / "contexts" / "meta" / context_hash
        meta_dir.mkdir(parents=True)
        meta_path = meta_dir / "meta.json"
        meta_path.write_text(
            json.dumps({
                "Name": "mycontext",
                "Endpoints": {"docker": {"Host": "tcp://context-host:2375"}},
            })
        )

        # Create Docker client with context parameter
        docker = Docker(context="mycontext")
        assert docker.docker_host == "tcp://context-host:2375"
        await docker.close()

    async def test_context_parameter_overrides_docker_context_env(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """context parameter should take precedence over DOCKER_CONTEXT env var."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.delenv("DOCKER_HOST", raising=False)
        monkeypatch.setenv("DOCKER_CONTEXT", "envcontext")

        docker_dir = tmp_path / ".docker"

        # Create context from parameter
        param_context_hash = Docker._get_context_dir_name("paramcontext")
        meta_dir1 = docker_dir / "contexts" / "meta" / param_context_hash
        meta_dir1.mkdir(parents=True)
        (meta_dir1 / "meta.json").write_text(
            json.dumps({
                "Name": "paramcontext",
                "Endpoints": {"docker": {"Host": "tcp://param-host:2375"}},
            })
        )

        # Create context from env var (should NOT be used)
        env_context_hash = Docker._get_context_dir_name("envcontext")
        meta_dir2 = docker_dir / "contexts" / "meta" / env_context_hash
        meta_dir2.mkdir(parents=True)
        (meta_dir2 / "meta.json").write_text(
            json.dumps({
                "Name": "envcontext",
                "Endpoints": {"docker": {"Host": "tcp://env-host:2375"}},
            })
        )

        # Should use paramcontext, not envcontext
        docker = Docker(context="paramcontext")
        assert docker.docker_host == "tcp://param-host:2375"
        await docker.close()

    async def test_context_parameter_overrides_docker_host_env(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """context parameter should take precedence over DOCKER_HOST env var."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("DOCKER_HOST", "tcp://env-host:2375")

        docker_dir = tmp_path / ".docker"
        context_hash = Docker._get_context_dir_name("mycontext")
        meta_dir = docker_dir / "contexts" / "meta" / context_hash
        meta_dir.mkdir(parents=True)
        (meta_dir / "meta.json").write_text(
            json.dumps({
                "Name": "mycontext",
                "Endpoints": {"docker": {"Host": "tcp://context-host:2375"}},
            })
        )

        # context parameter should win over DOCKER_HOST
        docker = Docker(context="mycontext")
        assert docker.docker_host == "tcp://context-host:2375"
        await docker.close()

    async def test_url_parameter_overrides_context_parameter(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """url parameter should take precedence over context parameter."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.delenv("DOCKER_HOST", raising=False)

        docker_dir = tmp_path / ".docker"
        context_hash = Docker._get_context_dir_name("mycontext")
        meta_dir = docker_dir / "contexts" / "meta" / context_hash
        meta_dir.mkdir(parents=True)
        (meta_dir / "meta.json").write_text(
            json.dumps({
                "Name": "mycontext",
                "Endpoints": {"docker": {"Host": "tcp://context-host:2375"}},
            })
        )

        # url parameter should win
        docker = Docker(url="tcp://url-host:2375", context="mycontext")
        assert docker.docker_host == "tcp://url-host:2375"
        await docker.close()

    async def test_context_parameter_with_default_value(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """context='default' should be treated as no context (fall back to DOCKER_HOST)."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("DOCKER_HOST", "tcp://fallback-host:2375")
        monkeypatch.delenv("DOCKER_CONTEXT", raising=False)

        # context='default' should be ignored, falling back to DOCKER_HOST
        docker = Docker(context="default")
        assert docker.docker_host == "tcp://fallback-host:2375"
        await docker.close()
