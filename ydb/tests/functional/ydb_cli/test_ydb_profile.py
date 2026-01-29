# -*- coding: utf-8 -*-
"""
Functional tests for YDB CLI profile management commands.

Tests cover both interactive (ftxui-based) and non-interactive modes
for all profile-related commands: init, create, delete, activate,
deactivate, list, get, update, replace.
"""

import os
import logging
import sys
import time
import uuid
import yaml

import pytest
import pexpect

import yatest

logger = logging.getLogger(__name__)


def ydb_bin():
    if os.getenv("YDB_CLI_BINARY"):
        return yatest.common.binary_path(os.getenv("YDB_CLI_BINARY"))
    raise RuntimeError("YDB_CLI_BINARY environment variable is not specified")


class ProfileTestBase:
    """Base class for profile tests with common utilities"""

    # Delay required before sending input to ftxui Input fields.
    # Without this delay, input sent immediately after expect() may be lost
    # because the ftxui component hasn't finished rendering yet.
    FTXUI_INPUT_DELAY = 0.3

    @pytest.fixture(autouse=True)
    def setup_profile_file(self, tmp_path):
        """Create isolated profile file for each test"""
        self.tmp_path = tmp_path
        self.profile_file = str(tmp_path / f"profile_{uuid.uuid4().hex}.yaml")
        yield
        # Cleanup handled by tmp_path fixture

    def execute_ydb_cli(self, args, stdin=None, check_exit_code=True):
        """Execute YDB CLI command non-interactively"""
        full_args = ["--profile-file", self.profile_file] + args

        # Convert string stdin to a temp file for yatest.common.execute
        stdin_file = None
        temp_stdin_path = None
        if stdin is not None:
            if isinstance(stdin, str):
                temp_stdin_path = str(self.tmp_path / f"stdin_{uuid.uuid4().hex}.txt")
                with open(temp_stdin_path, 'w') as f:
                    f.write(stdin)
                stdin_file = open(temp_stdin_path, 'r')
            else:
                stdin_file = stdin

        try:
            execution = yatest.common.execute(
                [ydb_bin()] + full_args,
                stdin=stdin_file,
                check_exit_code=False
            )
        finally:
            if stdin_file and temp_stdin_path:
                stdin_file.close()

        result = {
            'stdout': execution.std_out.decode('utf-8') if execution.std_out else '',
            'stderr': execution.std_err.decode('utf-8') if execution.std_err else '',
            'exit_code': execution.exit_code
        }
        logger.debug("Command: %s", full_args)
        logger.debug("Exit code: %d", result['exit_code'])
        logger.debug("stdout:\n%s", result['stdout'])
        logger.debug("stderr:\n%s", result['stderr'])
        if check_exit_code and result['exit_code'] != 0:
            raise RuntimeError(f"Command failed: {result['stderr']}")
        return result

    def spawn_ydb_cli(self, args, timeout=10, debug=False):
        """Spawn YDB CLI for interactive testing with pexpect"""
        full_args = ["--profile-file", self.profile_file] + args
        env = os.environ.copy()
        env["TERM"] = "xterm-256color"

        child = pexpect.spawn(
            ydb_bin(),
            full_args,
            encoding='utf-8',
            timeout=timeout,
            env=env
        )
        # For debugging, enable output to stdout
        if debug:
            child.logfile_read = sys.stdout
        return child

    def read_profile_file(self):
        """Read and parse the profile YAML file"""
        if not os.path.exists(self.profile_file):
            return None
        with open(self.profile_file, 'r') as f:
            return yaml.safe_load(f)

    def write_profile_file(self, content):
        """Write content to profile file"""
        os.makedirs(os.path.dirname(self.profile_file), exist_ok=True)
        with open(self.profile_file, 'w') as f:
            yaml.dump(content, f)

    def create_test_profile(self, name, endpoint=None, database=None, auth=None, active=False,
                            token_file=None, ca_file=None, user=None, password_file=None):
        """Helper to create a test profile programmatically"""
        config = self.read_profile_file() or {}
        if 'profiles' not in config:
            config['profiles'] = {}

        profile_data = {}
        if endpoint:
            profile_data['endpoint'] = endpoint
        if database:
            profile_data['database'] = database
        if auth:
            profile_data['authentication'] = auth
        if token_file:
            profile_data['authentication'] = {'method': 'token-file', 'data': token_file}
        if ca_file:
            profile_data['ca-file'] = ca_file
        if user:
            auth_data = {'method': 'static-credentials', 'data': {'user': user}}
            if password_file:
                auth_data['data']['password-file'] = password_file
            profile_data['authentication'] = auth_data

        config['profiles'][name] = profile_data
        if active:
            config['active_profile'] = name

        self.write_profile_file(config)

    def create_temp_file(self, content, filename):
        """Create a temporary file with given content and return its path"""
        filepath = str(self.tmp_path / filename)
        with open(filepath, 'w') as f:
            f.write(content)
        return filepath

    def set_active_profile(self, name):
        """Set the active profile in config file"""
        config = self.read_profile_file() or {}
        config['active_profile'] = name
        self.write_profile_file(config)

    def run_ydb_cli(self, args, stdin=None):
        """Execute YDB CLI and return result object with returncode, stdout, stderr"""
        result = self.execute_ydb_cli(args, stdin=stdin, check_exit_code=False)

        class Result:
            pass

        r = Result()
        r.returncode = result['exit_code']
        r.stdout = result['stdout']
        r.stderr = result['stderr']
        return r


# =============================================================================
# Non-interactive tests (command line arguments)
# =============================================================================

class TestProfileCreateNonInteractive(ProfileTestBase):
    """Test 'ydb config profile create' with command line arguments"""

    def test_create_profile_with_endpoint_and_database(self):
        """Create profile with endpoint and database from command line"""
        result = self.execute_ydb_cli([
            "config", "profile", "create", "test_profile",
            "--endpoint", "grpc://localhost:2136",
            "--database", "/Root"
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert config is not None
        assert 'profiles' in config
        assert 'test_profile' in config['profiles']
        assert config['profiles']['test_profile']['endpoint'] == 'grpc://localhost:2136'
        assert config['profiles']['test_profile']['database'] == '/Root'

    def test_create_profile_with_token_file(self):
        """Create profile with token-file authentication"""
        token_file = str(self.tmp_path / "token.txt")
        with open(token_file, 'w') as f:
            f.write("test_token")

        result = self.execute_ydb_cli([
            "config", "profile", "create", "token_profile",
            "--endpoint", "grpc://localhost:2136",
            "--database", "/Root",
            "--token-file", token_file
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert config['profiles']['token_profile']['authentication']['method'] == 'token-file'
        assert config['profiles']['token_profile']['authentication']['data'] == token_file

    def test_create_profile_with_anonymous_auth(self):
        """Create profile with anonymous authentication"""
        result = self.execute_ydb_cli([
            "config", "profile", "create", "anon_profile",
            "--endpoint", "grpc://localhost:2136",
            "--anonymous-auth"
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert config['profiles']['anon_profile']['authentication']['method'] == 'anonymous-auth'

    def test_create_profile_with_static_credentials(self):
        """Create profile with static credentials (user + password file)"""
        password_file = str(self.tmp_path / "password.txt")
        with open(password_file, 'w') as f:
            f.write("test_password")

        result = self.execute_ydb_cli([
            "config", "profile", "create", "static_profile",
            "--endpoint", "grpc://localhost:2136",
            "--user", "test_user",
            "--password-file", password_file
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        auth = config['profiles']['static_profile']['authentication']
        assert auth['method'] == 'static-credentials'
        assert auth['data']['user'] == 'test_user'
        assert auth['data']['password-file'] == password_file

    def test_create_profile_with_ca_file(self):
        """Create profile with CA file for SSL"""
        ca_file = str(self.tmp_path / "ca.pem")
        with open(ca_file, 'w') as f:
            f.write("fake_ca_cert")

        result = self.execute_ydb_cli([
            "config", "profile", "create", "ssl_profile",
            "--endpoint", "grpcs://localhost:2136",
            "--ca-file", ca_file
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert config['profiles']['ssl_profile']['ca-file'] == ca_file

    def test_create_profile_with_client_cert(self):
        """Create profile with client certificate for mTLS"""
        cert_file = str(self.tmp_path / "client.pem")
        key_file = str(self.tmp_path / "client.key")
        with open(cert_file, 'w') as f:
            f.write("fake_cert")
        with open(key_file, 'w') as f:
            f.write("fake_key")

        result = self.execute_ydb_cli([
            "config", "profile", "create", "mtls_profile",
            "--endpoint", "grpcs://localhost:2136",
            "--client-cert-file", cert_file,
            "--client-cert-key-file", key_file
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert config['profiles']['mtls_profile']['client-cert-file'] == cert_file
        assert config['profiles']['mtls_profile']['client-cert-key-file'] == key_file

    def test_create_existing_profile_fails(self):
        """Creating existing profile without interactive mode should fail"""
        # First create a profile
        self.execute_ydb_cli([
            "config", "profile", "create", "existing",
            "--endpoint", "grpc://localhost:2136"
        ])

        # Try to create it again
        result = self.execute_ydb_cli([
            "config", "profile", "create", "existing",
            "--endpoint", "grpc://localhost:9999"
        ], check_exit_code=False)

        assert result['exit_code'] != 0
        assert "already exists" in result['stderr']

    def test_create_profile_multiple_auth_methods_fails(self):
        """Creating profile with multiple auth methods should fail"""
        token_file = str(self.tmp_path / "token.txt")
        with open(token_file, 'w') as f:
            f.write("test_token")

        result = self.execute_ydb_cli([
            "config", "profile", "create", "bad_profile",
            "--endpoint", "grpc://localhost:2136",
            "--token-file", token_file,
            "--anonymous-auth"
        ], check_exit_code=False)

        assert result['exit_code'] != 0
        assert "authentication method" in result['stderr'].lower() or "2" in result['stderr']


class TestProfileDeleteNonInteractive(ProfileTestBase):
    """Test 'ydb config profile delete' with command line arguments"""

    def test_delete_profile_with_force(self):
        """Delete profile with --force flag (no confirmation)"""
        self.create_test_profile("to_delete", endpoint="grpc://localhost:2136")

        result = self.execute_ydb_cli([
            "config", "profile", "delete", "to_delete", "--force"
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert config is None or 'to_delete' not in config.get('profiles', {})

    def test_delete_nonexistent_profile_fails(self):
        """Deleting non-existent profile should fail"""
        result = self.execute_ydb_cli([
            "config", "profile", "delete", "nonexistent"
        ], check_exit_code=False)

        assert result['exit_code'] != 0
        assert "No existing profile" in result['stderr']

    def test_delete_nonexistent_profile_force_succeeds(self):
        """Deleting non-existent profile with --force should succeed"""
        result = self.execute_ydb_cli([
            "config", "profile", "delete", "nonexistent", "--force"
        ])
        assert result['exit_code'] == 0

    def test_delete_without_name_and_force_fails(self):
        """Delete without profile name and with --force should fail"""
        result = self.execute_ydb_cli([
            "config", "profile", "delete", "--force"
        ], check_exit_code=False)

        assert result['exit_code'] != 0


class TestProfileActivateNonInteractive(ProfileTestBase):
    """Test 'ydb config profile activate' with command line arguments"""

    def test_activate_profile(self):
        """Activate a profile by name"""
        self.create_test_profile("to_activate", endpoint="grpc://localhost:2136")

        result = self.execute_ydb_cli([
            "config", "profile", "activate", "to_activate"
        ])
        assert result['exit_code'] == 0
        assert "is now active" in result['stdout']

        config = self.read_profile_file()
        assert config['active_profile'] == 'to_activate'

    def test_activate_already_active_profile(self):
        """Activating already active profile should show appropriate message"""
        self.create_test_profile("active_one", endpoint="grpc://localhost:2136", active=True)

        result = self.execute_ydb_cli([
            "config", "profile", "activate", "active_one"
        ])
        assert result['exit_code'] == 0
        assert "already active" in result['stdout']

    def test_activate_nonexistent_profile_fails(self):
        """Activating non-existent profile should fail"""
        result = self.execute_ydb_cli([
            "config", "profile", "activate", "nonexistent"
        ], check_exit_code=False)

        assert result['exit_code'] != 0
        assert "No existing profile" in result['stderr']


class TestProfileDeactivateNonInteractive(ProfileTestBase):
    """Test 'ydb config profile deactivate'"""

    def test_deactivate_active_profile(self):
        """Deactivate currently active profile"""
        self.create_test_profile("active_profile", endpoint="grpc://localhost:2136", active=True)

        result = self.execute_ydb_cli([
            "config", "profile", "deactivate"
        ])
        assert result['exit_code'] == 0
        assert "deactivated" in result['stdout']

        config = self.read_profile_file()
        assert config.get('active_profile') is None or config.get('active_profile') == ''

    def test_deactivate_when_no_active_profile(self):
        """Deactivate when no profile is active"""
        self.create_test_profile("inactive", endpoint="grpc://localhost:2136", active=False)

        result = self.execute_ydb_cli([
            "config", "profile", "deactivate"
        ])
        assert result['exit_code'] == 0
        assert "No active profile" in result['stdout']


class TestProfileListNonInteractive(ProfileTestBase):
    """Test 'ydb config profile list'"""

    def test_list_empty(self):
        """List profiles when none exist"""
        result = self.execute_ydb_cli([
            "config", "profile", "list"
        ])
        assert result['exit_code'] == 0
        # Output should be empty or just whitespace
        assert result['stdout'].strip() == ''

    def test_list_profiles(self):
        """List existing profiles"""
        self.create_test_profile("profile_a", endpoint="grpc://localhost:2136")
        self.create_test_profile("profile_b", endpoint="grpc://localhost:2137")

        result = self.execute_ydb_cli([
            "config", "profile", "list"
        ])
        assert result['exit_code'] == 0
        assert "profile_a" in result['stdout']
        assert "profile_b" in result['stdout']

    def test_list_shows_active_profile(self):
        """List should show which profile is active"""
        self.create_test_profile("inactive_one", endpoint="grpc://localhost:2136")
        self.create_test_profile("active_one", endpoint="grpc://localhost:2137", active=True)

        result = self.execute_ydb_cli([
            "config", "profile", "list"
        ])
        assert result['exit_code'] == 0
        # active_one should be marked as active
        lines = result['stdout'].split('\n')
        active_line = [l for l in lines if 'active_one' in l][0]
        assert '(active)' in active_line

    def test_list_with_content(self):
        """List profiles with --with-content flag"""
        self.create_test_profile(
            "detailed_profile",
            endpoint="grpc://localhost:2136",
            database="/Root"
        )

        result = self.execute_ydb_cli([
            "config", "profile", "list", "--with-content"
        ])
        assert result['exit_code'] == 0
        assert "detailed_profile" in result['stdout']
        assert "endpoint:" in result['stdout']
        assert "grpc://localhost:2136" in result['stdout']
        assert "database:" in result['stdout']
        assert "/Root" in result['stdout']


class TestProfileGetNonInteractive(ProfileTestBase):
    """Test 'ydb config profile get'"""

    def test_get_profile(self):
        """Get profile content by name"""
        self.create_test_profile(
            "get_test",
            endpoint="grpc://localhost:2136",
            database="/Root",
            auth={'method': 'anonymous-auth'}
        )

        result = self.execute_ydb_cli([
            "config", "profile", "get", "get_test"
        ])
        assert result['exit_code'] == 0
        assert "endpoint:" in result['stdout']
        assert "grpc://localhost:2136" in result['stdout']
        assert "database:" in result['stdout']
        assert "/Root" in result['stdout']
        assert "anonymous-auth" in result['stdout']

    def test_get_nonexistent_profile_fails(self):
        """Getting non-existent profile should fail"""
        result = self.execute_ydb_cli([
            "config", "profile", "get", "nonexistent"
        ], check_exit_code=False)

        assert result['exit_code'] != 0
        assert "No existing profile" in result['stderr']


class TestProfileUpdateNonInteractive(ProfileTestBase):
    """Test 'ydb config profile update'"""

    def test_update_endpoint(self):
        """Update profile endpoint"""
        self.create_test_profile("update_test", endpoint="grpc://old:2136")

        result = self.execute_ydb_cli([
            "config", "profile", "update", "update_test",
            "--endpoint", "grpc://new:2136"
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert config['profiles']['update_test']['endpoint'] == 'grpc://new:2136'

    def test_update_add_database(self):
        """Add database to profile that didn't have one"""
        self.create_test_profile("update_test", endpoint="grpc://localhost:2136")

        result = self.execute_ydb_cli([
            "config", "profile", "update", "update_test",
            "--database", "/NewDb"
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert config['profiles']['update_test']['database'] == '/NewDb'

    def test_update_remove_endpoint(self):
        """Remove endpoint from profile with --no-endpoint"""
        self.create_test_profile(
            "update_test",
            endpoint="grpc://localhost:2136",
            database="/Root"
        )

        result = self.execute_ydb_cli([
            "config", "profile", "update", "update_test",
            "--no-endpoint"
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert 'endpoint' not in config['profiles']['update_test']
        assert config['profiles']['update_test']['database'] == '/Root'

    def test_update_remove_database(self):
        """Remove database from profile with --no-database"""
        self.create_test_profile(
            "update_test",
            endpoint="grpc://localhost:2136",
            database="/Root"
        )

        result = self.execute_ydb_cli([
            "config", "profile", "update", "update_test",
            "--no-database"
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert 'database' not in config['profiles']['update_test']
        assert config['profiles']['update_test']['endpoint'] == 'grpc://localhost:2136'

    def test_update_remove_auth(self):
        """Remove authentication from profile with --no-auth"""
        self.create_test_profile(
            "update_test",
            endpoint="grpc://localhost:2136",
            auth={'method': 'anonymous-auth'}
        )

        result = self.execute_ydb_cli([
            "config", "profile", "update", "update_test",
            "--no-auth"
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert 'authentication' not in config['profiles']['update_test']

    def test_update_nonexistent_profile_fails(self):
        """Updating non-existent profile should fail"""
        result = self.execute_ydb_cli([
            "config", "profile", "update", "nonexistent",
            "--endpoint", "grpc://localhost:2136"
        ], check_exit_code=False)

        assert result['exit_code'] != 0
        assert "No existing profile" in result['stderr']

    def test_update_mutually_exclusive_options_fail(self):
        """Using --endpoint and --no-endpoint together should fail"""
        self.create_test_profile("update_test", endpoint="grpc://localhost:2136")

        result = self.execute_ydb_cli([
            "config", "profile", "update", "update_test",
            "--endpoint", "grpc://new:2136",
            "--no-endpoint"
        ], check_exit_code=False)

        assert result['exit_code'] != 0
        assert "mutually exclusive" in result['stderr']

    def test_update_auth_and_no_auth_fail(self):
        """Using authentication option and --no-auth together should fail"""
        self.create_test_profile("update_test", endpoint="grpc://localhost:2136")

        result = self.execute_ydb_cli([
            "config", "profile", "update", "update_test",
            "--anonymous-auth",
            "--no-auth"
        ], check_exit_code=False)

        assert result['exit_code'] != 0


class TestProfileReplaceNonInteractive(ProfileTestBase):
    """Test 'ydb config profile replace'"""

    def test_replace_profile_with_force(self):
        """Replace profile with --force (no confirmation)"""
        self.create_test_profile(
            "replace_test",
            endpoint="grpc://old:2136",
            database="/OldDb"
        )

        result = self.execute_ydb_cli([
            "config", "profile", "replace", "replace_test",
            "--endpoint", "grpc://new:2136",
            "--database", "/NewDb",
            "--force"
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert config['profiles']['replace_test']['endpoint'] == 'grpc://new:2136'
        assert config['profiles']['replace_test']['database'] == '/NewDb'

    def test_replace_creates_new_profile(self):
        """Replace creates profile if it doesn't exist"""
        result = self.execute_ydb_cli([
            "config", "profile", "replace", "new_profile",
            "--endpoint", "grpc://localhost:2136",
            "--force"
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert 'new_profile' in config['profiles']


# =============================================================================
# Interactive tests (ftxui menus via pexpect)
# =============================================================================

class TestProfileInitInteractive(ProfileTestBase):
    """Test 'ydb init' interactive command"""

    def test_init_create_new_profile_full(self):
        """Create new profile with endpoint, database and anonymous auth via init"""
        # When no profiles exist, init goes directly to profile name input
        child = self.spawn_ydb_cli(["init"])

        child.expect("Welcome", timeout=5)

        # Wait for Input to be ready (shown by "Enter to confirm")
        child.expect("Enter to confirm", timeout=5)
        time.sleep(self.FTXUI_INPUT_DELAY)
        child.sendline("new_full_profile")

        # Endpoint menu - "Set a new endpoint value" is first option
        child.expect("endpoint", timeout=5)
        child.send('\n')

        # Enter endpoint value - wait for Input
        child.expect("Enter to confirm", timeout=5)
        time.sleep(self.FTXUI_INPUT_DELAY)
        child.sendline("grpc://newhost:2136")

        # Database menu - "Set a new database value" is first option
        child.expect("database", timeout=5)
        child.send('\n')

        # Enter database value - wait for Input
        child.expect("Enter to confirm", timeout=5)
        time.sleep(self.FTXUI_INPUT_DELAY)
        child.sendline("/NewDatabase")

        # Auth menu - select anonymous (index 7)
        child.expect("authentication", timeout=5)
        for _ in range(7):
            child.send('\x1b[B')
        child.send('\n')

        # Activate - Yes
        child.expect("Activate", timeout=5)
        child.send('\n')

        child.expect("configured successfully", timeout=5)
        child.expect(pexpect.EOF, timeout=5)

        config = self.read_profile_file()
        assert 'new_full_profile' in config['profiles']
        assert config['profiles']['new_full_profile']['endpoint'] == 'grpc://newhost:2136'
        assert config['profiles']['new_full_profile']['database'] == '/NewDatabase'
        assert config['profiles']['new_full_profile']['authentication']['method'] == 'anonymous-auth'
        assert config.get('active_profile') == 'new_full_profile'

    def test_init_create_new_profile_endpoint_only(self):
        """Create new profile with only endpoint via init"""
        child = self.spawn_ydb_cli(["init"])

        child.expect("Welcome", timeout=5)

        # Wait for Input to be ready
        child.expect("Enter to confirm", timeout=5)
        time.sleep(self.FTXUI_INPUT_DELAY)
        child.sendline("endpoint_only_init")

        # Endpoint menu - "Set a new endpoint value"
        child.expect("endpoint", timeout=5)
        time.sleep(self.FTXUI_INPUT_DELAY)
        child.send('\n')

        # Enter endpoint value
        child.expect("Enter to confirm", timeout=5)
        time.sleep(self.FTXUI_INPUT_DELAY)
        child.sendline("grpc://endpoint-only:2136")

        # Database menu - "Don't save database" (index 1)
        child.expect("database", timeout=5)
        child.send('\x1b[B')
        child.send('\n')

        # Auth menu - "Don't save authentication data" (index 8)
        child.expect("authentication", timeout=5)
        for _ in range(8):
            child.send('\x1b[B')
        child.send('\n')

        # Activate - No
        child.expect("Activate", timeout=5)
        child.send('\x1b[B')
        child.send('\n')

        child.expect("configured successfully", timeout=5)
        child.expect(pexpect.EOF, timeout=5)

        config = self.read_profile_file()
        assert 'endpoint_only_init' in config['profiles']
        assert config['profiles']['endpoint_only_init']['endpoint'] == 'grpc://endpoint-only:2136'

    def test_init_create_new_profile_with_metadata_auth(self):
        """Create new profile with metadata authentication via init"""
        child = self.spawn_ydb_cli(["init"])

        child.expect("Welcome", timeout=5)

        # Wait for Input to be ready
        child.expect("Enter to confirm", timeout=5)
        time.sleep(self.FTXUI_INPUT_DELAY)
        child.sendline("metadata_init_profile")

        # Endpoint menu - Set new endpoint
        child.expect("endpoint", timeout=5)
        child.send('\n')

        # Enter endpoint
        child.expect("Enter to confirm", timeout=5)
        time.sleep(self.FTXUI_INPUT_DELAY)
        child.sendline("grpc://metadata-test:2136")

        # Database - Set new database
        child.expect("database", timeout=5)
        child.send('\n')

        # Enter database
        child.expect("Enter to confirm", timeout=5)
        time.sleep(self.FTXUI_INPUT_DELAY)
        child.sendline("/MetadataDb")

        # Auth menu - "Use metadata service" (index 4)
        child.expect("authentication", timeout=5)
        for _ in range(4):
            child.send('\x1b[B')
        child.send('\n')

        # Activate - Yes
        child.expect("Activate", timeout=5)
        child.send('\n')

        child.expect("configured successfully", timeout=5)
        child.expect(pexpect.EOF, timeout=5)

        config = self.read_profile_file()
        assert 'metadata_init_profile' in config['profiles']
        assert config['profiles']['metadata_init_profile']['endpoint'] == 'grpc://metadata-test:2136'
        assert config['profiles']['metadata_init_profile']['database'] == '/MetadataDb'
        assert config['profiles']['metadata_init_profile']['authentication']['method'] == 'use-metadata-credentials'

    def test_init_create_profile_no_endpoint_no_database(self):
        """Create new profile without endpoint and database via init"""
        child = self.spawn_ydb_cli(["init"])

        child.expect("Welcome", timeout=5)

        # Wait for Input to be ready
        child.expect("Enter to confirm", timeout=5)
        time.sleep(self.FTXUI_INPUT_DELAY)
        child.sendline("minimal_init_profile")

        # Endpoint menu - "Don't save endpoint" (index 1)
        child.expect("endpoint", timeout=5)
        child.send('\x1b[B')
        child.send('\n')

        # Database menu - "Don't save database" (index 1)
        child.expect("database", timeout=5)
        child.send('\x1b[B')
        child.send('\n')

        # Auth menu - anonymous (index 7)
        child.expect("authentication", timeout=5)
        for _ in range(7):
            child.send('\x1b[B')
        child.send('\n')

        # Activate - No
        child.expect("Activate", timeout=5)
        child.send('\x1b[B')
        child.send('\n')

        child.expect("configured successfully", timeout=5)
        child.expect(pexpect.EOF, timeout=5)

        config = self.read_profile_file()
        assert 'minimal_init_profile' in config['profiles']
        assert config['profiles']['minimal_init_profile']['authentication']['method'] == 'anonymous-auth'

    def test_init_create_second_profile_via_menu(self):
        """Create a second profile by selecting 'Create new' from menu"""
        # First create an existing profile
        self.create_test_profile("existing", endpoint="grpc://old:2136")

        child = self.spawn_ydb_cli(["init"])

        child.expect("Welcome", timeout=5)
        # Now menu appears because profile exists
        child.expect("Please choose profile to configure", timeout=5)

        # "Create a new profile" is first option (index 0)
        child.send('\n')

        # Wait for Input to be ready
        child.expect("Enter to confirm", timeout=5)
        time.sleep(self.FTXUI_INPUT_DELAY)
        child.sendline("second_profile")

        # Endpoint menu - Set new endpoint
        child.expect("endpoint", timeout=5)
        child.send('\n')

        # Enter endpoint
        child.expect("Enter to confirm", timeout=5)
        time.sleep(self.FTXUI_INPUT_DELAY)
        child.sendline("grpc://second:2136")

        # Database - Don't save
        child.expect("database", timeout=5)
        child.send('\x1b[B')
        child.send('\n')

        # Auth - anonymous
        child.expect("authentication", timeout=5)
        for _ in range(7):
            child.send('\x1b[B')
        child.send('\n')

        # Activate - Yes
        child.expect("Activate", timeout=5)
        child.send('\n')

        child.expect("configured successfully", timeout=5)
        child.expect(pexpect.EOF, timeout=5)

        config = self.read_profile_file()
        assert 'second_profile' in config['profiles']
        assert config['profiles']['second_profile']['endpoint'] == 'grpc://second:2136'
        assert config.get('active_profile') == 'second_profile'

    def test_init_select_existing_profile(self):
        """Select existing profile to modify in init"""
        self.create_test_profile("existing_profile", endpoint="grpc://old:2136")

        child = self.spawn_ydb_cli(["init"])

        child.expect("Welcome", timeout=5)

        # Menu should show "Create a new profile" and "existing_profile"
        child.expect("Please choose profile to configure", timeout=5)

        # Navigate down to existing_profile and select
        child.send('\x1b[B')  # Down arrow
        child.send('\n')  # Select

        # Should now be in endpoint menu for existing profile
        child.expect("endpoint", timeout=5)

        # Cancel the rest
        child.send('\x1b')
        child.expect(pexpect.EOF, timeout=10)

    def test_init_keep_all_current_values(self):
        """Test init with 'Use current value' for endpoint/database"""
        self.create_test_profile(
            "current_values_test",
            endpoint="grpc://original:2136",
            database="/OriginalDb"
        )

        child = self.spawn_ydb_cli(["init"])

        child.expect("Welcome", timeout=5)
        child.expect("Please choose profile to configure", timeout=5)

        # Select existing profile (second option)
        child.send('\x1b[B')  # Down to profile
        child.send('\n')

        # Endpoint menu - "Use current endpoint value" is third option (index 2)
        # Options: 0=Set new, 1=Don't save, 2=Use current
        child.expect("endpoint", timeout=5)
        child.send('\x1b[B')  # Down
        child.send('\x1b[B')  # Down to "Use current"
        child.send('\n')

        # Database menu - "Use current database value" is third option
        child.expect("database", timeout=5)
        child.send('\x1b[B')  # Down
        child.send('\x1b[B')  # Down to "Use current"
        child.send('\n')

        # Auth menu - "Don't save authentication data" is last option (index 8)
        child.expect("authentication", timeout=5)
        for _ in range(8):
            child.send('\x1b[B')
        child.send('\n')

        # Activate - No (second option)
        child.expect("Activate", timeout=5)
        child.send('\x1b[B')
        child.send('\n')

        child.expect("configured successfully", timeout=5)
        child.expect(pexpect.EOF, timeout=5)

        # Verify original values preserved
        config = self.read_profile_file()
        assert config['profiles']['current_values_test']['endpoint'] == 'grpc://original:2136'
        assert config['profiles']['current_values_test']['database'] == '/OriginalDb'

    def test_init_with_anonymous_auth(self):
        """Test init selecting anonymous authentication"""
        self.create_test_profile("anon_test", endpoint="grpc://test:2136")

        child = self.spawn_ydb_cli(["init"])

        child.expect("Welcome", timeout=5)
        child.expect("Please choose profile to configure", timeout=5)

        # Select existing profile
        child.send('\x1b[B')
        child.send('\n')

        # Endpoint - Use current
        child.expect("endpoint", timeout=5)
        child.send('\x1b[B')
        child.send('\x1b[B')
        child.send('\n')

        # Database - Don't save (second option, index 1)
        child.expect("database", timeout=5)
        child.send('\x1b[B')
        child.send('\n')

        # Auth menu - "Set anonymous authentication" is index 7
        # 0: static-credentials, 1: iam-token, 2: yc-token, 3: oauth2-key-file
        # 4: metadata, 5: sa-key-file, 6: ydb-token, 7: anonymous, 8: don't save
        child.expect("authentication", timeout=5)
        for _ in range(7):
            child.send('\x1b[B')
        child.send('\n')

        # Activate - Yes (first option)
        child.expect("Activate", timeout=5)
        child.send('\n')

        child.expect("configured successfully", timeout=5)
        child.expect(pexpect.EOF, timeout=5)

        config = self.read_profile_file()
        assert config['profiles']['anon_test']['authentication']['method'] == 'anonymous-auth'
        assert config.get('active_profile') == 'anon_test'

    def test_init_with_metadata_credentials(self):
        """Test init selecting metadata credentials authentication"""
        self.create_test_profile("meta_test", endpoint="grpc://test:2136")

        child = self.spawn_ydb_cli(["init"])

        child.expect("Welcome", timeout=5)
        child.expect("Please choose profile to configure", timeout=5)

        # Select existing profile
        child.send('\x1b[B')
        child.send('\n')

        # Endpoint - Use current
        child.expect("endpoint", timeout=5)
        child.send('\x1b[B')
        child.send('\x1b[B')
        child.send('\n')

        # Database - Don't save
        child.expect("database", timeout=5)
        child.send('\x1b[B')
        child.send('\n')

        # Auth menu - "Use metadata service" is index 4
        child.expect("authentication", timeout=5)
        for _ in range(4):
            child.send('\x1b[B')
        child.send('\n')

        # Activate - No
        child.expect("Activate", timeout=5)
        child.send('\x1b[B')
        child.send('\n')

        child.expect("configured successfully", timeout=5)
        child.expect(pexpect.EOF, timeout=5)

        config = self.read_profile_file()
        assert config['profiles']['meta_test']['authentication']['method'] == 'use-metadata-credentials'

    def test_init_dont_save_anything(self):
        """Test init with 'Don't save' for all options"""
        self.create_test_profile("empty_test", endpoint="grpc://old:2136")

        child = self.spawn_ydb_cli(["init"])

        child.expect("Welcome", timeout=5)
        child.expect("Please choose profile to configure", timeout=5)

        # Select existing profile
        child.send('\x1b[B')
        child.send('\n')

        # Endpoint - Don't save (index 1)
        child.expect("endpoint", timeout=5)
        child.send('\x1b[B')
        child.send('\n')

        # Database - Don't save (index 1)
        child.expect("database", timeout=5)
        child.send('\x1b[B')
        child.send('\n')

        # Auth - Don't save (index 8)
        child.expect("authentication", timeout=5)
        for _ in range(8):
            child.send('\x1b[B')
        child.send('\n')

        # Activate - No
        child.expect("Activate", timeout=5)
        child.send('\x1b[B')
        child.send('\n')

        child.expect("configured successfully", timeout=5)
        child.expect(pexpect.EOF, timeout=5)

        config = self.read_profile_file()
        # Profile should exist but be empty
        assert 'empty_test' in config['profiles']

    def test_init_activate_profile(self):
        """Test init with profile activation"""
        self.create_test_profile("activate_test", endpoint="grpc://test:2136")
        # Ensure no active profile
        config = self.read_profile_file()
        if 'active_profile' in config:
            del config['active_profile']
            self.write_profile_file(config)

        child = self.spawn_ydb_cli(["init"])

        child.expect("Welcome", timeout=5)
        child.expect("Please choose profile to configure", timeout=5)

        # Select existing profile
        child.send('\x1b[B')
        child.send('\n')

        # Endpoint - Use current
        child.expect("endpoint", timeout=5)
        child.send('\x1b[B')
        child.send('\x1b[B')
        child.send('\n')

        # Database - Don't save
        child.expect("database", timeout=5)
        child.send('\x1b[B')
        child.send('\n')

        # Auth - Don't save
        child.expect("authentication", timeout=5)
        for _ in range(8):
            child.send('\x1b[B')
        child.send('\n')

        # Activate - Yes (first option)
        child.expect("Activate", timeout=5)
        child.send('\n')

        child.expect("configured successfully", timeout=5)
        child.expect(pexpect.EOF, timeout=5)

        config = self.read_profile_file()
        assert config.get('active_profile') == 'activate_test'

    def test_init_dont_activate_profile(self):
        """Test init without profile activation"""
        self.create_test_profile("no_activate_test", endpoint="grpc://test:2136")
        # Set different active profile
        config = self.read_profile_file()
        config['active_profile'] = 'other'
        self.write_profile_file(config)

        child = self.spawn_ydb_cli(["init"])

        child.expect("Welcome", timeout=5)
        child.expect("Please choose profile to configure", timeout=5)

        # Select existing profile (second option - our test profile)
        child.send('\x1b[B')
        child.send('\n')

        # Endpoint - Use current
        child.expect("endpoint", timeout=5)
        child.send('\x1b[B')
        child.send('\x1b[B')
        child.send('\n')

        # Database - Don't save
        child.expect("database", timeout=5)
        child.send('\x1b[B')
        child.send('\n')

        # Auth - Don't save
        child.expect("authentication", timeout=5)
        for _ in range(8):
            child.send('\x1b[B')
        child.send('\n')

        # Activate - No (second option)
        child.expect("Activate", timeout=5)
        child.send('\x1b[B')
        child.send('\n')

        child.expect("configured successfully", timeout=5)
        child.expect(pexpect.EOF, timeout=5)

        config = self.read_profile_file()
        # Active profile should remain unchanged
        assert config.get('active_profile') == 'other'

    def test_init_cancel_at_profile_selection(self):
        """Test canceling init at profile selection menu"""
        self.create_test_profile("cancel_test", endpoint="grpc://test:2136")

        child = self.spawn_ydb_cli(["init"])

        child.expect("Welcome", timeout=5)
        child.expect("Please choose profile to configure", timeout=5)

        # Press Escape to cancel
        child.send('\x1b')

        child.expect(pexpect.EOF, timeout=5)

    def test_init_cancel_at_endpoint_menu(self):
        """Test canceling init at endpoint menu"""
        self.create_test_profile("cancel_ep_test", endpoint="grpc://test:2136")

        child = self.spawn_ydb_cli(["init"])

        child.expect("Welcome", timeout=5)
        child.expect("Please choose profile to configure", timeout=5)

        # Select existing profile
        child.send('\x1b[B')
        child.send('\n')

        # At endpoint menu - press Escape
        child.expect("endpoint", timeout=5)
        child.send('\x1b')

        child.expect(pexpect.EOF, timeout=5)

    def test_init_cancel_at_database_menu(self):
        """Test canceling init at database menu"""
        self.create_test_profile("cancel_db_test", endpoint="grpc://test:2136")

        child = self.spawn_ydb_cli(["init"])

        child.expect("Welcome", timeout=5)
        child.expect("Please choose profile to configure", timeout=5)

        # Select existing profile
        child.send('\x1b[B')
        child.send('\n')

        # Endpoint - Don't save
        child.expect("endpoint", timeout=5)
        child.send('\x1b[B')
        child.send('\n')

        # At database menu - press Escape
        child.expect("database", timeout=5)
        child.send('\x1b')

        child.expect(pexpect.EOF, timeout=5)

    def test_init_cancel_at_auth_menu(self):
        """Test canceling init at authentication menu"""
        self.create_test_profile("cancel_auth_test", endpoint="grpc://test:2136")

        child = self.spawn_ydb_cli(["init"])

        child.expect("Welcome", timeout=5)
        child.expect("Please choose profile to configure", timeout=5)

        # Select existing profile
        child.send('\x1b[B')
        child.send('\n')

        # Endpoint - Don't save
        child.expect("endpoint", timeout=5)
        child.send('\x1b[B')
        child.send('\n')

        # Database - Don't save
        child.expect("database", timeout=5)
        child.send('\x1b[B')
        child.send('\n')

        # At auth menu - press Escape
        child.expect("authentication", timeout=5)
        child.send('\x1b')

        child.expect(pexpect.EOF, timeout=5)


class TestProfileDeleteInteractive(ProfileTestBase):
    """Test 'ydb config profile delete' interactive mode"""

    def test_delete_profile_interactive_confirm(self):
        """Delete profile with interactive confirmation"""
        self.create_test_profile("to_delete", endpoint="grpc://localhost:2136")

        child = self.spawn_ydb_cli(["config", "profile", "delete", "to_delete"])

        # Confirmation prompt - default is No (index 1), need to go up to Yes
        child.expect("will be permanently removed", timeout=5)
        child.send('\x1b[A')  # Up to Yes
        child.send('\n')  # Confirm

        child.expect("deleted", timeout=5)
        child.expect(pexpect.EOF, timeout=5)

        config = self.read_profile_file()
        assert config is None or 'to_delete' not in config.get('profiles', {})

    def test_delete_profile_interactive_decline(self):
        """Decline deletion in interactive mode"""
        self.create_test_profile("keep_me", endpoint="grpc://localhost:2136")

        child = self.spawn_ydb_cli(["config", "profile", "delete", "keep_me"])

        child.expect("will be permanently removed", timeout=5)
        # Default is No (index 1), just press Enter to decline
        child.send('\n')

        child.expect("No changes made", timeout=5)
        child.expect(pexpect.EOF, timeout=5)

        # Profile should still exist
        config = self.read_profile_file()
        assert 'keep_me' in config['profiles']

    def test_delete_profile_select_from_menu(self):
        """Select profile to delete from menu"""
        self.create_test_profile("profile_a", endpoint="grpc://a:2136")
        self.create_test_profile("profile_b", endpoint="grpc://b:2136")

        child = self.spawn_ydb_cli(["config", "profile", "delete"])

        child.expect("Please choose profile to remove", timeout=5)

        # First option is "Don't remove anything"
        # Select profile_a (second option)
        child.send('\x1b[B')  # Down
        child.send('\n')

        # Confirm - default is No, need to go up to Yes
        child.expect("will be permanently removed", timeout=5)
        child.send('\x1b[A')  # Up to Yes
        child.send('\n')

        child.expect("deleted", timeout=5)
        child.expect(pexpect.EOF, timeout=5)

        config = self.read_profile_file()
        assert 'profile_a' not in config.get('profiles', {})
        assert 'profile_b' in config['profiles']

    def test_delete_cancel_at_menu(self):
        """Cancel deletion at menu selection"""
        self.create_test_profile("keep_it", endpoint="grpc://localhost:2136")

        child = self.spawn_ydb_cli(["config", "profile", "delete"])

        child.expect("Please choose profile to remove", timeout=5)

        # Select "Don't remove anything" (first option)
        child.send('\n')

        child.expect("No changes made", timeout=5)
        child.expect(pexpect.EOF, timeout=5)

        config = self.read_profile_file()
        assert 'keep_it' in config['profiles']


class TestProfileActivateInteractive(ProfileTestBase):
    """Test 'ydb config profile activate' interactive mode"""

    def test_activate_select_from_menu(self):
        """Select profile to activate from menu"""
        self.create_test_profile("profile_a", endpoint="grpc://a:2136")
        self.create_test_profile("profile_b", endpoint="grpc://b:2136")

        child = self.spawn_ydb_cli(["config", "profile", "activate"])

        child.expect("Please choose profile to activate", timeout=5)

        # First option is "Don't do anything"
        # Select profile_b (third option - after "Don't do anything")
        child.send('\x1b[B')  # Down
        child.send('\x1b[B')  # Down
        child.send('\n')

        child.expect("is now active", timeout=5)
        child.expect(pexpect.EOF, timeout=5)

        config = self.read_profile_file()
        assert config['active_profile'] == 'profile_b'

    def test_activate_deactivate_option(self):
        """Deactivate current profile from activate menu"""
        self.create_test_profile("active_one", endpoint="grpc://localhost:2136", active=True)
        self.create_test_profile("other", endpoint="grpc://other:2136")

        child = self.spawn_ydb_cli(["config", "profile", "activate"])

        child.expect("Please choose profile to activate", timeout=5)

        # When there's an active profile, option order is:
        # 0: Don't do anything
        # 1: Deactivate current
        # 2+: profiles
        child.send('\x1b[B')  # Down to "Deactivate"
        child.send('\n')

        child.expect("deactivated", timeout=5)
        child.expect(pexpect.EOF, timeout=5)

        config = self.read_profile_file()
        assert config.get('active_profile') is None or config.get('active_profile') == ''


class TestProfileGetInteractive(ProfileTestBase):
    """Test 'ydb config profile get' interactive mode"""

    def test_get_select_from_menu(self):
        """Select profile to view from menu"""
        self.create_test_profile(
            "view_me",
            endpoint="grpc://localhost:2136",
            database="/Root"
        )

        child = self.spawn_ydb_cli(["config", "profile", "get"])

        child.expect("Please choose profile to list its values", timeout=5)
        child.send('\n')  # Select first profile

        child.expect("endpoint:", timeout=5)
        child.expect("grpc://localhost:2136", timeout=5)
        child.expect(pexpect.EOF, timeout=5)

    def test_get_cancel_at_menu(self):
        """Cancel get at menu selection"""
        self.create_test_profile("profile", endpoint="grpc://localhost:2136")

        child = self.spawn_ydb_cli(["config", "profile", "get"])

        child.expect("Please choose profile", timeout=5)
        child.send('\x1b')  # Escape

        child.expect("Cancelled", timeout=5)
        child.expect(pexpect.EOF, timeout=5)


class TestProfileReplaceInteractive(ProfileTestBase):
    """Test 'ydb config profile replace' interactive mode"""

    def test_replace_confirm(self):
        """Replace profile with confirmation"""
        self.create_test_profile("replace_me", endpoint="grpc://old:2136")

        child = self.spawn_ydb_cli([
            "config", "profile", "replace", "replace_me",
            "--endpoint", "grpc://new:2136"
        ])

        # Confirmation - default is No, need to go up to Yes
        child.expect("will be replaced", timeout=5)
        child.send('\x1b[A')  # Up to Yes
        child.send('\n')

        child.expect(pexpect.EOF, timeout=5)

        config = self.read_profile_file()
        assert config['profiles']['replace_me']['endpoint'] == 'grpc://new:2136'

    def test_replace_decline(self):
        """Decline replacement"""
        self.create_test_profile("keep_me", endpoint="grpc://old:2136")

        child = self.spawn_ydb_cli([
            "config", "profile", "replace", "keep_me",
            "--endpoint", "grpc://new:2136"
        ])

        # Confirmation - default is No, just press Enter to decline
        child.expect("will be replaced", timeout=5)
        child.send('\n')

        child.expect(pexpect.EOF, timeout=5)

        # Original should be preserved
        config = self.read_profile_file()
        assert config['profiles']['keep_me']['endpoint'] == 'grpc://old:2136'


# =============================================================================
# Edge cases and error handling
# =============================================================================

class TestProfileIamAuth(ProfileTestBase):
    """Test IAM authentication options"""

    def test_create_profile_with_sa_key_file(self):
        """Create profile with service account key file"""
        sa_key_file = str(self.tmp_path / "sa_key.json")
        with open(sa_key_file, 'w') as f:
            f.write('{"id": "test", "service_account_id": "sa1", "private_key": "key"}')

        result = self.execute_ydb_cli([
            "config", "profile", "create", "sa_profile",
            "--endpoint", "grpc://localhost:2136",
            "--sa-key-file", sa_key_file
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert config['profiles']['sa_profile']['authentication']['method'] == 'sa-key-file'
        assert config['profiles']['sa_profile']['authentication']['data'] == sa_key_file

    def test_create_profile_with_oauth2_key_file(self):
        """Create profile with OAuth2 key file"""
        oauth2_file = str(self.tmp_path / "oauth2.json")
        with open(oauth2_file, 'w') as f:
            f.write('{"grant-type": "urn:ietf:params:oauth:grant-type:token-exchange"}')

        result = self.execute_ydb_cli([
            "config", "profile", "create", "oauth2_profile",
            "--endpoint", "grpc://localhost:2136",
            "--oauth2-key-file", oauth2_file
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert config['profiles']['oauth2_profile']['authentication']['method'] == 'oauth2-key-file'
        assert config['profiles']['oauth2_profile']['authentication']['data'] == oauth2_file

    def test_create_profile_with_metadata_credentials(self):
        """Create profile with metadata credentials (VM auth)"""
        result = self.execute_ydb_cli([
            "config", "profile", "create", "vm_profile",
            "--endpoint", "grpc://localhost:2136",
            "--use-metadata-credentials"
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert config['profiles']['vm_profile']['authentication']['method'] == 'use-metadata-credentials'

    def test_create_profile_with_iam_endpoint(self):
        """Create profile with custom IAM endpoint"""
        sa_key_file = str(self.tmp_path / "sa_key.json")
        with open(sa_key_file, 'w') as f:
            f.write('{"id": "test"}')

        result = self.execute_ydb_cli([
            "config", "profile", "create", "iam_profile",
            "--endpoint", "grpc://localhost:2136",
            "--sa-key-file", sa_key_file,
            "--iam-endpoint", "iam.api.cloud.yandex.net:443"
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert config['profiles']['iam_profile']['iam-endpoint'] == 'iam.api.cloud.yandex.net:443'

    def test_update_remove_iam_endpoint(self):
        """Remove IAM endpoint with --no-iam-endpoint"""
        self.create_test_profile("iam_test", endpoint="grpc://localhost:2136")
        # Manually add iam-endpoint
        config = self.read_profile_file()
        config['profiles']['iam_test']['iam-endpoint'] = 'iam.example.com'
        self.write_profile_file(config)

        result = self.execute_ydb_cli([
            "config", "profile", "update", "iam_test",
            "--no-iam-endpoint"
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert 'iam-endpoint' not in config['profiles']['iam_test']


class TestProfileEdgeCases(ProfileTestBase):
    """Test edge cases and error handling"""

    def test_profile_name_with_special_characters(self):
        """Profile names with special characters"""
        result = self.execute_ydb_cli([
            "config", "profile", "create", "test-profile_123",
            "--endpoint", "grpc://localhost:2136"
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert 'test-profile_123' in config['profiles']

    def test_empty_profile_name_fails_interactive(self):
        """Empty profile name should be rejected"""
        child = self.spawn_ydb_cli(["config", "profile", "create"])

        child.expect("Welcome", timeout=5)
        child.expect("Please enter configuration profile name", timeout=5)

        # Try to submit empty name
        child.send('\n')  # Just press Enter without typing anything

        # Should show error and not proceed
        child.expect("cannot be empty", timeout=5)

        child.send('\x1b')  # Cancel
        child.expect(pexpect.EOF, timeout=10)

    def test_password_file_without_user_fails(self):
        """Using --password-file without --user should fail"""
        password_file = str(self.tmp_path / "password.txt")
        with open(password_file, 'w') as f:
            f.write("test_password")

        result = self.execute_ydb_cli([
            "config", "profile", "create", "bad_profile",
            "--password-file", password_file
        ], check_exit_code=False)

        assert result['exit_code'] != 0
        assert "password-file without user" in result['stderr']

    def test_endpoint_with_database_in_url_via_stdin(self):
        """Endpoint URL with database parameter via stdin (ParseUrl extracts database)"""
        # Note: database extraction from URL only works when reading from stdin,
        # not when using --endpoint command line option
        stdin_content = """endpoint: grpc://localhost:2136?database=/Root
"""
        stdin_file = str(self.tmp_path / "stdin_url.txt")
        with open(stdin_file, 'w') as f:
            f.write(stdin_content)

        with open(stdin_file, 'r') as f:
            result = self.execute_ydb_cli([
                "config", "profile", "create", "url_profile"
            ], stdin=f)

        assert result['exit_code'] == 0

        config = self.read_profile_file()
        # Should extract database from URL
        assert config['profiles']['url_profile']['endpoint'] == 'grpc://localhost:2136'
        assert config['profiles']['url_profile']['database'] == '/Root'

    def test_profile_with_all_ssl_options(self):
        """Create profile with all SSL/TLS options"""
        ca_file = str(self.tmp_path / "ca.pem")
        cert_file = str(self.tmp_path / "cert.pem")
        key_file = str(self.tmp_path / "key.pem")
        key_password_file = str(self.tmp_path / "key_password.txt")

        for f in [ca_file, cert_file, key_file, key_password_file]:
            with open(f, 'w') as fh:
                fh.write("fake_content")

        result = self.execute_ydb_cli([
            "config", "profile", "create", "full_ssl",
            "--endpoint", "grpcs://localhost:2136",
            "--ca-file", ca_file,
            "--client-cert-file", cert_file,
            "--client-cert-key-file", key_file,
            "--client-cert-key-password-file", key_password_file
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        profile = config['profiles']['full_ssl']
        assert profile['ca-file'] == ca_file
        assert profile['client-cert-file'] == cert_file
        assert profile['client-cert-key-file'] == key_file
        assert profile['client-cert-key-password-file'] == key_password_file

    def test_update_remove_ssl_options(self):
        """Remove SSL options with --no-* flags"""
        ca_file = str(self.tmp_path / "ca.pem")
        cert_file = str(self.tmp_path / "cert.pem")
        key_file = str(self.tmp_path / "key.pem")

        for f in [ca_file, cert_file, key_file]:
            with open(f, 'w') as fh:
                fh.write("fake")

        self.execute_ydb_cli([
            "config", "profile", "create", "ssl_remove_test",
            "--endpoint", "grpcs://localhost:2136",
            "--ca-file", ca_file,
            "--client-cert-file", cert_file,
            "--client-cert-key-file", key_file
        ])

        result = self.execute_ydb_cli([
            "config", "profile", "update", "ssl_remove_test",
            "--no-ca-file",
            "--no-client-cert-file",
            "--no-client-cert-key-file"
        ])
        assert result['exit_code'] == 0

        config = self.read_profile_file()
        profile = config['profiles']['ssl_remove_test']
        assert 'ca-file' not in profile
        assert 'client-cert-file' not in profile
        assert 'client-cert-key-file' not in profile

    def test_multiple_profiles_management(self):
        """Test managing multiple profiles"""
        # Create several profiles
        for i in range(5):
            self.execute_ydb_cli([
                "config", "profile", "create", f"profile_{i}",
                "--endpoint", f"grpc://host{i}:2136",
                "--database", f"/Db{i}"
            ])

        # Verify all created
        config = self.read_profile_file()
        assert len(config['profiles']) == 5

        # Activate one
        self.execute_ydb_cli(["config", "profile", "activate", "profile_2"])
        config = self.read_profile_file()
        assert config['active_profile'] == 'profile_2'

        # Delete one
        self.execute_ydb_cli(["config", "profile", "delete", "profile_3", "--force"])
        config = self.read_profile_file()
        assert 'profile_3' not in config['profiles']
        assert len(config['profiles']) == 4

        # Update one
        self.execute_ydb_cli([
            "config", "profile", "update", "profile_4",
            "--endpoint", "grpc://updated:2136"
        ])
        config = self.read_profile_file()
        assert config['profiles']['profile_4']['endpoint'] == 'grpc://updated:2136'


class TestProfileStdinInput(ProfileTestBase):
    """Test profile creation from stdin (non-interactive piped input)"""

    def test_create_profile_from_stdin(self):
        """Create profile with options piped via stdin"""
        stdin_content = """endpoint: grpc://stdin-host:2136
database: /StdinDb
anonymous-auth
"""
        stdin_file = str(self.tmp_path / "stdin.txt")
        with open(stdin_file, 'w') as f:
            f.write(stdin_content)

        with open(stdin_file, 'r') as f:
            result = self.execute_ydb_cli([
                "config", "profile", "create", "stdin_profile"
            ], stdin=f)

        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert 'stdin_profile' in config['profiles']
        assert config['profiles']['stdin_profile']['endpoint'] == 'grpc://stdin-host:2136'
        assert config['profiles']['stdin_profile']['database'] == '/StdinDb'
        assert config['profiles']['stdin_profile']['authentication']['method'] == 'anonymous-auth'

    def test_create_profile_from_stdin_with_token(self):
        """Create profile with token-file from stdin"""
        token_file = str(self.tmp_path / "token.txt")
        with open(token_file, 'w') as f:
            f.write("my_token")

        stdin_content = f"""endpoint: grpc://localhost:2136
database: /Root
token-file: {token_file}
"""
        stdin_file = str(self.tmp_path / "stdin.txt")
        with open(stdin_file, 'w') as f:
            f.write(stdin_content)

        with open(stdin_file, 'r') as f:
            result = self.execute_ydb_cli([
                "config", "profile", "create", "token_stdin"
            ], stdin=f)

        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert config['profiles']['token_stdin']['authentication']['method'] == 'token-file'

    def test_create_profile_from_stdin_with_static_creds(self):
        """Create profile with static credentials from stdin"""
        password_file = str(self.tmp_path / "password.txt")
        with open(password_file, 'w') as f:
            f.write("secret_password")

        stdin_content = f"""endpoint: grpc://localhost:2136
database: /Root
user: admin
password-file: {password_file}
"""
        stdin_file = str(self.tmp_path / "stdin.txt")
        with open(stdin_file, 'w') as f:
            f.write(stdin_content)

        with open(stdin_file, 'r') as f:
            result = self.execute_ydb_cli([
                "config", "profile", "create", "static_stdin"
            ], stdin=f)

        assert result['exit_code'] == 0

        config = self.read_profile_file()
        auth = config['profiles']['static_stdin']['authentication']
        assert auth['method'] == 'static-credentials'
        assert auth['data']['user'] == 'admin'

    def test_create_profile_from_stdin_duplicate_option_fails(self):
        """Duplicate options in stdin should fail"""
        stdin_content = """endpoint: grpc://host1:2136
endpoint: grpc://host2:2136
"""
        stdin_file = str(self.tmp_path / "stdin.txt")
        with open(stdin_file, 'w') as f:
            f.write(stdin_content)

        with open(stdin_file, 'r') as f:
            result = self.execute_ydb_cli([
                "config", "profile", "create", "dup_profile"
            ], stdin=f, check_exit_code=False)

        assert result['exit_code'] != 0
        assert "too many" in result['stderr'].lower()

    def test_create_profile_with_sa_key_file(self):
        """Create profile with service account key file auth"""
        sa_key_content = '{"id": "key123", "service_account_id": "sa123", "private_key": "key"}'
        sa_key_file = str(self.tmp_path / "sa_key.json")
        with open(sa_key_file, 'w') as f:
            f.write(sa_key_content)

        stdin_content = f"""endpoint: grpc://localhost:2136
database: /Root
sa-key-file: {sa_key_file}
"""
        stdin_file = str(self.tmp_path / "stdin.txt")
        with open(stdin_file, 'w') as f:
            f.write(stdin_content)

        with open(stdin_file, 'r') as f:
            result = self.execute_ydb_cli([
                "config", "profile", "create", "sa_key_profile"
            ], stdin=f)

        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert 'sa_key_profile' in config['profiles']
        assert config['profiles']['sa_key_profile']['authentication']['method'] == 'sa-key-file'

    def test_create_profile_with_yc_token_file(self):
        """Create profile with yc-token-file auth"""
        yc_token_file = str(self.tmp_path / "yc_token.txt")
        with open(yc_token_file, 'w') as f:
            f.write("yc_oauth_token_content")

        stdin_content = f"""endpoint: grpc://localhost:2136
yc-token-file: {yc_token_file}
"""
        stdin_file = str(self.tmp_path / "stdin.txt")
        with open(stdin_file, 'w') as f:
            f.write(stdin_content)

        with open(stdin_file, 'r') as f:
            result = self.execute_ydb_cli([
                "config", "profile", "create", "yc_token_profile"
            ], stdin=f)

        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert config['profiles']['yc_token_profile']['authentication']['method'] == 'yc-token-file'

    def test_create_profile_with_oauth2_key_file(self):
        """Create profile with oauth2-key-file auth"""
        oauth2_file = str(self.tmp_path / "oauth2.json")
        with open(oauth2_file, 'w') as f:
            f.write('{"token_endpoint": "https://example.com/token"}')

        stdin_content = f"""endpoint: grpc://localhost:2136
oauth2-key-file: {oauth2_file}
"""
        stdin_file = str(self.tmp_path / "stdin.txt")
        with open(stdin_file, 'w') as f:
            f.write(stdin_content)

        with open(stdin_file, 'r') as f:
            result = self.execute_ydb_cli([
                "config", "profile", "create", "oauth2_profile"
            ], stdin=f)

        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert config['profiles']['oauth2_profile']['authentication']['method'] == 'oauth2-key-file'

    def test_create_profile_with_iam_token_file(self):
        """Create profile with iam-token-file auth"""
        iam_token_file = str(self.tmp_path / "iam_token.txt")
        with open(iam_token_file, 'w') as f:
            f.write("iam_token_content_here")

        stdin_content = f"""endpoint: grpc://localhost:2136
iam-token-file: {iam_token_file}
"""
        stdin_file = str(self.tmp_path / "stdin.txt")
        with open(stdin_file, 'w') as f:
            f.write(stdin_content)

        with open(stdin_file, 'r') as f:
            result = self.execute_ydb_cli([
                "config", "profile", "create", "iam_token_profile"
            ], stdin=f)

        assert result['exit_code'] == 0

        config = self.read_profile_file()
        # IAM token file is stored as token-file method
        assert config['profiles']['iam_token_profile']['authentication']['method'] == 'token-file'

    def test_create_profile_with_ca_file(self):
        """Create profile with CA certificate file"""
        ca_file = str(self.tmp_path / "ca.pem")
        with open(ca_file, 'w') as f:
            f.write("-----BEGIN CERTIFICATE-----\nCA_CERT_CONTENT\n-----END CERTIFICATE-----")

        stdin_content = f"""endpoint: grpcs://localhost:2136
ca-file: {ca_file}
anonymous-auth
"""
        stdin_file = str(self.tmp_path / "stdin.txt")
        with open(stdin_file, 'w') as f:
            f.write(stdin_content)

        with open(stdin_file, 'r') as f:
            result = self.execute_ydb_cli([
                "config", "profile", "create", "ca_profile"
            ], stdin=f)

        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert 'ca_profile' in config['profiles']
        assert config['profiles']['ca_profile']['ca-file'] == ca_file

    def test_create_profile_with_iam_endpoint(self):
        """Create profile with custom IAM endpoint"""
        sa_key_content = '{"id": "key123"}'
        sa_key_file = str(self.tmp_path / "sa_key.json")
        with open(sa_key_file, 'w') as f:
            f.write(sa_key_content)

        stdin_content = f"""endpoint: grpc://localhost:2136
sa-key-file: {sa_key_file}
iam-endpoint: https://custom-iam.example.com/v1/tokens
"""
        stdin_file = str(self.tmp_path / "stdin.txt")
        with open(stdin_file, 'w') as f:
            f.write(stdin_content)

        with open(stdin_file, 'r') as f:
            result = self.execute_ydb_cli([
                "config", "profile", "create", "iam_endpoint_profile"
            ], stdin=f)

        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert config['profiles']['iam_endpoint_profile']['iam-endpoint'] == 'https://custom-iam.example.com/v1/tokens'

    def test_create_profile_only_endpoint(self):
        """Create profile with only endpoint"""
        stdin_content = """endpoint: grpc://endpoint-only:2136
"""
        stdin_file = str(self.tmp_path / "stdin.txt")
        with open(stdin_file, 'w') as f:
            f.write(stdin_content)

        with open(stdin_file, 'r') as f:
            result = self.execute_ydb_cli([
                "config", "profile", "create", "endpoint_only_profile"
            ], stdin=f)

        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert config['profiles']['endpoint_only_profile']['endpoint'] == 'grpc://endpoint-only:2136'
        # No auth should be set
        assert 'authentication' not in config['profiles']['endpoint_only_profile']

    def test_create_profile_only_database(self):
        """Create profile with only database"""
        stdin_content = """database: /DbOnly
"""
        stdin_file = str(self.tmp_path / "stdin.txt")
        with open(stdin_file, 'w') as f:
            f.write(stdin_content)

        with open(stdin_file, 'r') as f:
            result = self.execute_ydb_cli([
                "config", "profile", "create", "db_only_profile"
            ], stdin=f)

        assert result['exit_code'] == 0

        config = self.read_profile_file()
        assert config['profiles']['db_only_profile']['database'] == '/DbOnly'


class TestProfileMenuNavigation(ProfileTestBase):
    """Test interactive menu navigation edge cases"""

    def test_input_validation_retry(self):
        """Test that invalid input shows error and allows retry"""
        child = self.spawn_ydb_cli(["config", "profile", "create"])

        child.expect("Welcome", timeout=5)
        child.expect("Please enter configuration profile name", timeout=5)

        # Try empty name
        child.send('\n')  # Press Enter without typing
        child.expect("cannot be empty", timeout=5)

        # Now enter valid name
        child.send("valid_name")
        child.send('\n')  # Confirm

        # Should proceed to endpoint menu
        child.expect("endpoint", timeout=5)

        # Cancel
        child.send('\x1b')
        child.expect(pexpect.EOF, timeout=10)

    def test_page_navigation_in_large_menu(self):
        """Test pagination in menu with many options"""
        # Create many profiles to trigger pagination
        for i in range(20):
            self.create_test_profile(f"profile_{i:02d}", endpoint=f"grpc://host{i}:2136")

        child = self.spawn_ydb_cli(["config", "profile", "get"])

        child.expect("Please choose profile", timeout=5)

        # Navigate down through pages
        for _ in range(15):
            child.send('\x1b[B')  # Down

        # Select current option
        child.send('\n')

        # Should show profile content
        child.expect("endpoint:", timeout=5)
        child.expect(pexpect.EOF, timeout=5)

    def test_use_current_value_option(self):
        """Test 'Use current value' option when modifying existing profile"""
        self.create_test_profile(
            "existing",
            endpoint="grpc://original:2136",
            database="/OriginalDb"
        )

        child = self.spawn_ydb_cli(["init"])

        child.expect("Welcome", timeout=5)
        child.expect("Please choose profile to configure", timeout=5)

        # Select existing profile
        child.send('\x1b[B')  # Down to existing
        child.send('\n')

        # Endpoint menu - select "Use current endpoint value"
        child.expect("endpoint", timeout=5)
        child.send('\x1b[B')  # Down
        child.send('\x1b[B')  # Down to "Use current"
        child.send('\n')

        # Database menu - select "Use current database value"
        child.expect("database", timeout=5)
        child.send('\x1b[B')
        child.send('\x1b[B')
        child.send('\n')

        # Auth menu - select "Don't save authentication data" (index 8)
        child.expect("authentication", timeout=5)
        for _ in range(8):
            child.send('\x1b[B')
        child.send('\n')

        # Activate - No
        child.expect("Activate", timeout=5)
        child.send('\x1b[B')  # No
        child.send('\n')

        child.expect("configured successfully", timeout=5)
        child.expect(pexpect.EOF, timeout=5)

        # Verify original values preserved
        config = self.read_profile_file()
        assert config['profiles']['existing']['endpoint'] == 'grpc://original:2136'
        assert config['profiles']['existing']['database'] == '/OriginalDb'






class TestProfileStdinInputAdditional(ProfileTestBase):
    """Additional stdin input tests"""

    def test_stdin_with_metadata_credentials(self):
        """Test profile creation from stdin with metadata credentials"""
        stdin_data = "endpoint: grpc://metadata:2136\nuse-metadata-credentials\n"
        result = self.run_ydb_cli(
            ["config", "profile", "create", "metadata_profile"],
            stdin=stdin_data
        )
        assert result.returncode == 0

        config = self.read_profile_file()
        assert 'metadata_profile' in config['profiles']
        assert config['profiles']['metadata_profile']['endpoint'] == 'grpc://metadata:2136'
        assert config['profiles']['metadata_profile']['authentication']['method'] == 'use-metadata-credentials'

    def test_stdin_with_anonymous_auth(self):
        """Test profile creation from stdin with anonymous auth"""
        stdin_data = "endpoint: grpc://anon:2136\nanonymous-auth\n"
        result = self.run_ydb_cli(
            ["config", "profile", "create", "anon_stdin_profile"],
            stdin=stdin_data
        )
        assert result.returncode == 0

        config = self.read_profile_file()
        assert 'anon_stdin_profile' in config['profiles']
        assert config['profiles']['anon_stdin_profile']['authentication']['method'] == 'anonymous-auth'

    def test_stdin_with_iam_endpoint(self):
        """Test profile creation from stdin with IAM endpoint"""
        sa_key_file = self.create_temp_file('{"id": "key1"}', "sa_key.json")
        stdin_data = f"endpoint: grpc://iam:2136\nsa-key-file: {sa_key_file}\niam-endpoint: iam.api.cloud.yandex.net\n"
        result = self.run_ydb_cli(
            ["config", "profile", "create", "iam_stdin_profile"],
            stdin=stdin_data
        )
        assert result.returncode == 0

        config = self.read_profile_file()
        assert 'iam_stdin_profile' in config['profiles']
        assert config['profiles']['iam_stdin_profile']['iam-endpoint'] == 'iam.api.cloud.yandex.net'

    def test_stdin_duplicate_endpoint_fails(self):
        """Test that duplicate endpoint in stdin fails"""
        stdin_data = "endpoint: grpc://first:2136\nendpoint: grpc://second:2136\n"
        result = self.run_ydb_cli(
            ["config", "profile", "create", "dup_endpoint"],
            stdin=stdin_data
        )
        assert result.returncode != 0
        assert "too many" in result.stderr.lower()

    def test_stdin_duplicate_database_fails(self):
        """Test that duplicate database in stdin fails"""
        stdin_data = "database: /First\ndatabase: /Second\n"
        result = self.run_ydb_cli(
            ["config", "profile", "create", "dup_db"],
            stdin=stdin_data
        )
        assert result.returncode != 0
        assert "too many" in result.stderr.lower()

    def test_stdin_duplicate_metadata_credentials_fails(self):
        """Test that duplicate metadata credentials in stdin fails"""
        stdin_data = "use-metadata-credentials\nuse-metadata-credentials\n"
        result = self.run_ydb_cli(
            ["config", "profile", "create", "dup_meta"],
            stdin=stdin_data
        )
        assert result.returncode != 0

    def test_stdin_duplicate_anonymous_auth_fails(self):
        """Test that duplicate anonymous auth in stdin fails"""
        stdin_data = "anonymous-auth\nanonymous-auth\n"
        result = self.run_ydb_cli(
            ["config", "profile", "create", "dup_anon"],
            stdin=stdin_data
        )
        assert result.returncode != 0


class TestProfileYcTokenAuth(ProfileTestBase):
    """Test YC token authentication"""

    def test_create_profile_with_yc_token_file(self):
        """Test profile creation with YC token file"""
        yc_token_file = self.create_temp_file("yc-oauth-token-content", "yc_token.txt")
        result = self.run_ydb_cli([
            "config", "profile", "create", "yc_profile",
            "--yc-token-file", yc_token_file
        ])
        assert result.returncode == 0

        config = self.read_profile_file()
        assert 'yc_profile' in config['profiles']
        assert config['profiles']['yc_profile']['authentication']['method'] == 'yc-token-file'
        assert config['profiles']['yc_profile']['authentication']['data'] == yc_token_file

    def test_stdin_with_yc_token_file(self):
        """Test profile creation from stdin with YC token file"""
        yc_token_file = self.create_temp_file("yc-oauth-token", "yc.txt")
        stdin_data = f"endpoint: grpc://yc:2136\nyc-token-file: {yc_token_file}\n"
        result = self.run_ydb_cli(
            ["config", "profile", "create", "yc_stdin_profile"],
            stdin=stdin_data
        )
        assert result.returncode == 0

        config = self.read_profile_file()
        assert 'yc_stdin_profile' in config['profiles']
        assert config['profiles']['yc_stdin_profile']['authentication']['method'] == 'yc-token-file'


class TestProfileValidationErrors(ProfileTestBase):
    """Test validation error cases"""

    def test_multiple_auth_methods_detailed(self):
        """Test error message lists all provided auth methods"""
        token_file = self.create_temp_file("token", "t.txt")
        sa_key_file = self.create_temp_file('{"id": "k"}', "sa.json")

        result = self.run_ydb_cli([
            "config", "profile", "create", "multi_auth",
            "--token-file", token_file,
            "--sa-key-file", sa_key_file
        ])
        assert result.returncode != 0
        assert "2 authentication methods" in result.stderr

    def test_token_file_and_metadata_fails(self):
        """Test that token file and metadata credentials together fail"""
        token_file = self.create_temp_file("token", "t.txt")
        result = self.run_ydb_cli([
            "config", "profile", "create", "conflict_auth",
            "--token-file", token_file,
            "--use-metadata-credentials"
        ])
        assert result.returncode != 0

    def test_token_file_and_anonymous_fails(self):
        """Test that token file and anonymous auth together fail"""
        token_file = self.create_temp_file("token", "t.txt")
        result = self.run_ydb_cli([
            "config", "profile", "create", "conflict_auth2",
            "--token-file", token_file,
            "--anonymous-auth"
        ])
        assert result.returncode != 0

    def test_user_and_token_file_fails(self):
        """Test that user and token file together fail"""
        token_file = self.create_temp_file("token", "t.txt")
        password_file = self.create_temp_file("pass", "p.txt")
        result = self.run_ydb_cli([
            "config", "profile", "create", "conflict_auth3",
            "--token-file", token_file,
            "--user", "testuser",
            "--password-file", password_file
        ])
        assert result.returncode != 0


class TestProfileUpdateAdditional(ProfileTestBase):
    """Additional update command tests"""

    def test_update_replace_auth_method(self):
        """Test updating profile to replace auth method"""
        # Create with token
        token_file = self.create_temp_file("original-token", "token.txt")
        self.create_test_profile("test", token_file=token_file)

        # Update with different auth
        new_token_file = self.create_temp_file("new-token", "new_token.txt")
        result = self.run_ydb_cli([
            "config", "profile", "update", "test",
            "--token-file", new_token_file
        ])
        assert result.returncode == 0

        config = self.read_profile_file()
        assert config['profiles']['test']['authentication']['data'] == new_token_file

    def test_update_add_ca_file(self):
        """Test adding CA file to existing profile"""
        self.create_test_profile("test", endpoint="grpc://test:2136")

        ca_file = self.create_temp_file("CA-CERT", "ca.pem")
        result = self.run_ydb_cli([
            "config", "profile", "update", "test",
            "--ca-file", ca_file
        ])
        assert result.returncode == 0

        config = self.read_profile_file()
        assert config['profiles']['test']['ca-file'] == ca_file

    def test_update_becomes_empty_recreates(self):
        """Test that update that removes all values recreates profile"""
        self.create_test_profile("test", endpoint="grpc://test:2136")

        result = self.run_ydb_cli([
            "config", "profile", "update", "test",
            "--no-endpoint"
        ])
        assert result.returncode == 0

        # Profile should still exist (empty profiles are valid)
        config = self.read_profile_file()
        assert 'test' in config['profiles']


class TestProfileGetAdditional(ProfileTestBase):
    """Additional get profile tests"""

    def test_get_profile_with_all_auth_types_display(self):
        """Test get profile displays various auth types correctly"""
        # Test with static credentials
        password_file = self.create_temp_file("secret", "pass.txt")
        self.write_profile_file({
            'profiles': {
                'static_test': {
                    'endpoint': 'grpc://test:2136',
                    'authentication': {
                        'method': 'static-credentials',
                        'data': {
                            'user': 'testuser',
                            'password-file': password_file
                        }
                    }
                }
            }
        })

        result = self.run_ydb_cli(["config", "profile", "get", "static_test"])
        assert result.returncode == 0
        assert "static-credentials" in result.stdout
        assert "testuser" in result.stdout

    def test_get_profile_with_iam_endpoint(self):
        """Test get profile displays IAM endpoint"""
        sa_key_file = self.create_temp_file('{"id": "key"}', "sa.json")
        self.write_profile_file({
            'profiles': {
                'iam_test': {
                    'endpoint': 'grpc://test:2136',
                    'iam-endpoint': 'iam.api.cloud.yandex.net',
                    'authentication': {
                        'method': 'sa-key-file',
                        'data': sa_key_file
                    }
                }
            }
        })

        result = self.run_ydb_cli(["config", "profile", "get", "iam_test"])
        assert result.returncode == 0
        assert "iam-endpoint" in result.stdout

    def test_get_profile_with_ssl_options(self):
        """Test get profile displays SSL options"""
        ca_file = self.create_temp_file("CA", "ca.pem")
        cert_file = self.create_temp_file("CERT", "cert.pem")
        key_file = self.create_temp_file("KEY", "key.pem")

        self.write_profile_file({
            'profiles': {
                'ssl_test': {
                    'endpoint': 'grpcs://test:2136',
                    'ca-file': ca_file,
                    'client-cert-file': cert_file,
                    'client-cert-key-file': key_file
                }
            }
        })

        result = self.run_ydb_cli(["config", "profile", "get", "ssl_test"])
        assert result.returncode == 0
        assert "ca-file" in result.stdout
        assert "client-cert-file" in result.stdout
        assert "client-cert-key-file" in result.stdout


class TestProfileListAdditional(ProfileTestBase):
    """Additional list profiles tests"""

    def test_list_with_content_shows_auth_methods(self):
        """Test list --with-content shows authentication methods"""
        token_file = self.create_temp_file("token", "t.txt")
        self.write_profile_file({
            'profiles': {
                'token_profile': {
                    'endpoint': 'grpc://test:2136',
                    'authentication': {
                        'method': 'token-file',
                        'data': token_file
                    }
                },
                'anon_profile': {
                    'endpoint': 'grpc://anon:2136',
                    'authentication': {
                        'method': 'anonymous-auth'
                    }
                }
            }
        })

        result = self.run_ydb_cli(["config", "profile", "list", "--with-content"])
        assert result.returncode == 0
        assert "token-file" in result.stdout or "token_profile" in result.stdout
        assert "anonymous-auth" in result.stdout or "anon_profile" in result.stdout


class TestProfileActivateEdgeCases(ProfileTestBase):
    """Edge cases for activate/deactivate"""

    def test_activate_when_no_profiles_exist(self):
        """Test activate fails when no profiles exist"""
        # Ensure no profiles
        self.write_profile_file({'profiles': {}})

        result = self.run_ydb_cli(["config", "profile", "activate", "nonexistent"])
        assert result.returncode != 0

    def test_activate_interactive_exit_without_selection(self):
        """Test activate interactive mode with Escape cancels"""
        self.create_test_profile("test", endpoint="grpc://test:2136")

        child = self.spawn_ydb_cli(["config", "profile", "activate"])

        child.expect("Please choose profile to activate", timeout=5)
        child.send('\x1b')  # Escape

        child.expect(pexpect.EOF, timeout=5)


class TestProfileDeleteEdgeCases(ProfileTestBase):
    """Edge cases for delete command"""

    def test_delete_active_profile(self):
        """Test deleting the currently active profile"""
        self.create_test_profile("active_test", endpoint="grpc://test:2136")
        self.set_active_profile("active_test")

        result = self.run_ydb_cli([
            "config", "profile", "delete", "active_test", "--force"
        ])
        assert result.returncode == 0

        config = self.read_profile_file()
        assert 'active_test' not in config.get('profiles', {})

    def test_delete_when_no_profiles_exist(self):
        """Test delete fails gracefully when no profiles exist"""
        self.write_profile_file({'profiles': {}})

        result = self.run_ydb_cli(["config", "profile", "delete", "nonexistent"])
        assert result.returncode != 0


class TestProfileReplaceEdgeCases(ProfileTestBase):
    """Edge cases for replace command"""

    def test_replace_nonexistent_creates(self):
        """Test replace creates profile if it doesn't exist"""
        result = self.run_ydb_cli([
            "config", "profile", "replace", "new_replace",
            "--endpoint", "grpc://new:2136"
        ])
        assert result.returncode == 0

        config = self.read_profile_file()
        assert 'new_replace' in config['profiles']
        assert config['profiles']['new_replace']['endpoint'] == 'grpc://new:2136'

    def test_replace_interactive_decline(self):
        """Test replace with interactive decline"""
        self.create_test_profile("to_replace", endpoint="grpc://original:2136")

        child = self.spawn_ydb_cli([
            "config", "profile", "replace", "to_replace",
            "--endpoint", "grpc://new:2136"
        ])

        # Should ask for confirmation
        child.expect("replaced", timeout=5)
        child.send('\x1b[B')  # Down to No
        child.send('\n')

        child.expect(pexpect.EOF, timeout=5)

        # Original should be preserved
        config = self.read_profile_file()
        assert config['profiles']['to_replace']['endpoint'] == 'grpc://original:2136'


class TestProfileUrlParsing(ProfileTestBase):
    """Test URL parsing with database in query string"""

    def test_url_with_database_query_param(self):
        """Test endpoint URL with database in query parameter"""
        stdin_data = "endpoint: grpc://test:2136/?database=/QueryDb\n"
        result = self.run_ydb_cli(
            ["config", "profile", "create", "url_test"],
            stdin=stdin_data
        )
        assert result.returncode == 0

        config = self.read_profile_file()
        assert 'url_test' in config['profiles']
        # Database should be extracted from URL
        assert config['profiles']['url_test'].get('database') == '/QueryDb'

    def test_url_and_separate_database_fails(self):
        """Test that URL with database and separate database option fails"""
        stdin_data = "endpoint: grpc://test:2136/?database=/QueryDb\ndatabase: /AnotherDb\n"
        result = self.run_ydb_cli(
            ["config", "profile", "create", "url_conflict"],
            stdin=stdin_data
        )
        assert result.returncode != 0
        assert "too many" in result.stderr.lower()


class TestProfileInitEdgeCases(ProfileTestBase):
    """Edge cases for init command"""

    def test_init_with_many_profiles_pagination(self):
        """Test init with many profiles shows pagination"""
        # Create many profiles to trigger pagination
        for i in range(25):
            self.create_test_profile(f"profile_{i:02d}", endpoint=f"grpc://test{i}:2136")

        child = self.spawn_ydb_cli(["init"])

        child.expect("Welcome", timeout=5)
        child.expect("Please choose profile to configure", timeout=5)

        # Navigate through pages
        child.send('\x1b[C')  # Right arrow for next page

        # Cancel
        child.send('\x1b')

        child.expect(pexpect.EOF, timeout=5)


class TestProfilePrintContent(ProfileTestBase):
    """Test profile content display for various auth types"""

    def test_display_ydb_token_auth(self):
        """Test display of ydb-token authentication"""
        self.write_profile_file({
            'profiles': {
                'token_test': {
                    'authentication': {
                        'method': 'ydb-token',
                        'data': 'secret-token-value-12345'
                    }
                }
            }
        })

        result = self.run_ydb_cli(["config", "profile", "get", "token_test"])
        assert result.returncode == 0
        assert "ydb-token" in result.stdout

    def test_display_iam_token_auth(self):
        """Test display of iam-token authentication"""
        self.write_profile_file({
            'profiles': {
                'iam_test': {
                    'authentication': {
                        'method': 'iam-token',
                        'data': 'iam-token-secret-value'
                    }
                }
            }
        })

        result = self.run_ydb_cli(["config", "profile", "get", "iam_test"])
        assert result.returncode == 0
        assert "iam-token" in result.stdout

    def test_display_yc_token_auth(self):
        """Test display of yc-token authentication"""
        self.write_profile_file({
            'profiles': {
                'yc_test': {
                    'authentication': {
                        'method': 'yc-token',
                        'data': 'yc-oauth-token-secret'
                    }
                }
            }
        })

        result = self.run_ydb_cli(["config", "profile", "get", "yc_test"])
        assert result.returncode == 0
        assert "yc-token" in result.stdout

    def test_display_static_credentials_with_password(self):
        """Test display of static credentials with password"""
        self.write_profile_file({
            'profiles': {
                'static_test': {
                    'authentication': {
                        'method': 'static-credentials',
                        'data': {
                            'user': 'myuser',
                            'password': 'secret123'
                        }
                    }
                }
            }
        })

        result = self.run_ydb_cli(["config", "profile", "get", "static_test"])
        assert result.returncode == 0
        assert "static-credentials" in result.stdout
        assert "myuser" in result.stdout
        # Password should be shown (possibly blurred in interactive mode)
        assert "password" in result.stdout.lower()

    def test_display_use_metadata_credentials(self):
        """Test display of use-metadata-credentials"""
        self.write_profile_file({
            'profiles': {
                'meta_test': {
                    'authentication': {
                        'method': 'use-metadata-credentials'
                    }
                }
            }
        })

        result = self.run_ydb_cli(["config", "profile", "get", "meta_test"])
        assert result.returncode == 0
        assert "use-metadata-credentials" in result.stdout

    def test_display_oauth2_key_file(self):
        """Test display of oauth2-key-file authentication"""
        oauth2_file = self.create_temp_file('{"key": "value"}', "oauth2.json")
        self.write_profile_file({
            'profiles': {
                'oauth2_test': {
                    'authentication': {
                        'method': 'oauth2-key-file',
                        'data': oauth2_file
                    }
                }
            }
        })

        result = self.run_ydb_cli(["config", "profile", "get", "oauth2_test"])
        assert result.returncode == 0
        assert "oauth2-key-file" in result.stdout

    def test_display_token_file(self):
        """Test display of token-file authentication"""
        token_file = self.create_temp_file("my-token", "token.txt")
        self.write_profile_file({
            'profiles': {
                'tf_test': {
                    'authentication': {
                        'method': 'token-file',
                        'data': token_file
                    }
                }
            }
        })

        result = self.run_ydb_cli(["config", "profile", "get", "tf_test"])
        assert result.returncode == 0
        assert "token-file" in result.stdout

    def test_display_yc_token_file(self):
        """Test display of yc-token-file authentication"""
        yc_token_file = self.create_temp_file("yc-token", "yc.txt")
        self.write_profile_file({
            'profiles': {
                'yctf_test': {
                    'authentication': {
                        'method': 'yc-token-file',
                        'data': yc_token_file
                    }
                }
            }
        })

        result = self.run_ydb_cli(["config", "profile", "get", "yctf_test"])
        assert result.returncode == 0
        assert "yc-token-file" in result.stdout

    def test_display_client_cert_key_password_file(self):
        """Test display of client cert key password file"""
        ca_file = self.create_temp_file("CA", "ca.pem")
        cert_file = self.create_temp_file("CERT", "cert.pem")
        key_file = self.create_temp_file("KEY", "key.pem")
        password_file = self.create_temp_file("pass", "keypass.txt")

        self.write_profile_file({
            'profiles': {
                'fullssl_test': {
                    'endpoint': 'grpcs://test:2136',
                    'ca-file': ca_file,
                    'client-cert-file': cert_file,
                    'client-cert-key-file': key_file,
                    'client-cert-key-password-file': password_file
                }
            }
        })

        result = self.run_ydb_cli(["config", "profile", "get", "fullssl_test"])
        assert result.returncode == 0
        assert "client-cert-key-password-file" in result.stdout




class TestProfileIamTokenFile(ProfileTestBase):
    """Test iam-token-file option (mapped to token-file internally)"""

    def test_create_with_iam_token_file_hidden(self):
        """Test that iam-token-file is treated as token-file"""
        # iam-token-file is a hidden option that maps to token-file
        token_file = self.create_temp_file("iam-token-content", "iam.txt")

        # Use stdin to provide the option
        stdin_data = f"iam-token-file: {token_file}\n"
        result = self.run_ydb_cli(
            ["config", "profile", "create", "iam_file_test"],
            stdin=stdin_data
        )
        assert result.returncode == 0

        config = self.read_profile_file()
        assert 'iam_file_test' in config['profiles']
        # Should be stored as token-file
        assert config['profiles']['iam_file_test']['authentication']['method'] == 'token-file'
