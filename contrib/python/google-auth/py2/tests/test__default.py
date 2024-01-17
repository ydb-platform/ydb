# Copyright 2016 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os

import mock
import pytest

from google.auth import _default
from google.auth import app_engine
from google.auth import aws
from google.auth import compute_engine
from google.auth import credentials
from google.auth import environment_vars
from google.auth import exceptions
from google.auth import external_account
from google.auth import identity_pool
from google.oauth2 import service_account
import google.oauth2.credentials


import yatest.common
DATA_DIR = os.path.join(yatest.common.test_source_path(), "data")
AUTHORIZED_USER_FILE = os.path.join(DATA_DIR, "authorized_user.json")

with open(AUTHORIZED_USER_FILE) as fh:
    AUTHORIZED_USER_FILE_DATA = json.load(fh)

AUTHORIZED_USER_CLOUD_SDK_FILE = os.path.join(
    DATA_DIR, "authorized_user_cloud_sdk.json"
)

AUTHORIZED_USER_CLOUD_SDK_WITH_QUOTA_PROJECT_ID_FILE = os.path.join(
    DATA_DIR, "authorized_user_cloud_sdk_with_quota_project_id.json"
)

SERVICE_ACCOUNT_FILE = os.path.join(DATA_DIR, "service_account.json")

CLIENT_SECRETS_FILE = os.path.join(DATA_DIR, "client_secrets.json")

with open(SERVICE_ACCOUNT_FILE) as fh:
    SERVICE_ACCOUNT_FILE_DATA = json.load(fh)

SUBJECT_TOKEN_TEXT_FILE = os.path.join(DATA_DIR, "external_subject_token.txt")
TOKEN_URL = "https://sts.googleapis.com/v1/token"
AUDIENCE = "//iam.googleapis.com/projects/123456/locations/global/workloadIdentityPools/POOL_ID/providers/PROVIDER_ID"
REGION_URL = "http://169.254.169.254/latest/meta-data/placement/availability-zone"
SECURITY_CREDS_URL = "http://169.254.169.254/latest/meta-data/iam/security-credentials"
CRED_VERIFICATION_URL = (
    "https://sts.{region}.amazonaws.com?Action=GetCallerIdentity&Version=2011-06-15"
)
IDENTITY_POOL_DATA = {
    "type": "external_account",
    "audience": AUDIENCE,
    "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
    "token_url": TOKEN_URL,
    "credential_source": {"file": SUBJECT_TOKEN_TEXT_FILE},
}
AWS_DATA = {
    "type": "external_account",
    "audience": AUDIENCE,
    "subject_token_type": "urn:ietf:params:aws:token-type:aws4_request",
    "token_url": TOKEN_URL,
    "credential_source": {
        "environment_id": "aws1",
        "region_url": REGION_URL,
        "url": SECURITY_CREDS_URL,
        "regional_cred_verification_url": CRED_VERIFICATION_URL,
    },
}

MOCK_CREDENTIALS = mock.Mock(spec=credentials.CredentialsWithQuotaProject)
MOCK_CREDENTIALS.with_quota_project.return_value = MOCK_CREDENTIALS


def get_project_id_side_effect(self, request=None):
    # If no scopes are set, this will always return None.
    if not self.scopes:
        return None
    return mock.sentinel.project_id


LOAD_FILE_PATCH = mock.patch(
    "google.auth._default.load_credentials_from_file",
    return_value=(MOCK_CREDENTIALS, mock.sentinel.project_id),
    autospec=True,
)
EXTERNAL_ACCOUNT_GET_PROJECT_ID_PATCH = mock.patch.object(
    external_account.Credentials,
    "get_project_id",
    side_effect=get_project_id_side_effect,
    autospec=True,
)


def test_load_credentials_from_missing_file():
    with pytest.raises(exceptions.DefaultCredentialsError) as excinfo:
        _default.load_credentials_from_file("")

    assert excinfo.match(r"not found")


def test_load_credentials_from_file_invalid_json(tmpdir):
    jsonfile = tmpdir.join("invalid.json")
    jsonfile.write("{")

    with pytest.raises(exceptions.DefaultCredentialsError) as excinfo:
        _default.load_credentials_from_file(str(jsonfile))

    assert excinfo.match(r"not a valid json file")


def test_load_credentials_from_file_invalid_type(tmpdir):
    jsonfile = tmpdir.join("invalid.json")
    jsonfile.write(json.dumps({"type": "not-a-real-type"}))

    with pytest.raises(exceptions.DefaultCredentialsError) as excinfo:
        _default.load_credentials_from_file(str(jsonfile))

    assert excinfo.match(r"does not have a valid type")


def test_load_credentials_from_file_authorized_user():
    credentials, project_id = _default.load_credentials_from_file(AUTHORIZED_USER_FILE)
    assert isinstance(credentials, google.oauth2.credentials.Credentials)
    assert project_id is None


def test_load_credentials_from_file_no_type(tmpdir):
    # use the client_secrets.json, which is valid json but not a
    # loadable credentials type
    with pytest.raises(exceptions.DefaultCredentialsError) as excinfo:
        _default.load_credentials_from_file(CLIENT_SECRETS_FILE)

    assert excinfo.match(r"does not have a valid type")
    assert excinfo.match(r"Type is None")


def test_load_credentials_from_file_authorized_user_bad_format(tmpdir):
    filename = tmpdir.join("authorized_user_bad.json")
    filename.write(json.dumps({"type": "authorized_user"}))

    with pytest.raises(exceptions.DefaultCredentialsError) as excinfo:
        _default.load_credentials_from_file(str(filename))

    assert excinfo.match(r"Failed to load authorized user")
    assert excinfo.match(r"missing fields")


def test_load_credentials_from_file_authorized_user_cloud_sdk():
    with pytest.warns(UserWarning, match="Cloud SDK"):
        credentials, project_id = _default.load_credentials_from_file(
            AUTHORIZED_USER_CLOUD_SDK_FILE
        )
    assert isinstance(credentials, google.oauth2.credentials.Credentials)
    assert project_id is None

    # No warning if the json file has quota project id.
    credentials, project_id = _default.load_credentials_from_file(
        AUTHORIZED_USER_CLOUD_SDK_WITH_QUOTA_PROJECT_ID_FILE
    )
    assert isinstance(credentials, google.oauth2.credentials.Credentials)
    assert project_id is None


def test_load_credentials_from_file_authorized_user_cloud_sdk_with_scopes():
    with pytest.warns(UserWarning, match="Cloud SDK"):
        credentials, project_id = _default.load_credentials_from_file(
            AUTHORIZED_USER_CLOUD_SDK_FILE,
            scopes=["https://www.google.com/calendar/feeds"],
        )
    assert isinstance(credentials, google.oauth2.credentials.Credentials)
    assert project_id is None
    assert credentials.scopes == ["https://www.google.com/calendar/feeds"]


def test_load_credentials_from_file_authorized_user_cloud_sdk_with_quota_project():
    credentials, project_id = _default.load_credentials_from_file(
        AUTHORIZED_USER_CLOUD_SDK_FILE, quota_project_id="project-foo"
    )

    assert isinstance(credentials, google.oauth2.credentials.Credentials)
    assert project_id is None
    assert credentials.quota_project_id == "project-foo"


def test_load_credentials_from_file_service_account():
    credentials, project_id = _default.load_credentials_from_file(SERVICE_ACCOUNT_FILE)
    assert isinstance(credentials, service_account.Credentials)
    assert project_id == SERVICE_ACCOUNT_FILE_DATA["project_id"]


def test_load_credentials_from_file_service_account_with_scopes():
    credentials, project_id = _default.load_credentials_from_file(
        SERVICE_ACCOUNT_FILE, scopes=["https://www.google.com/calendar/feeds"]
    )
    assert isinstance(credentials, service_account.Credentials)
    assert project_id == SERVICE_ACCOUNT_FILE_DATA["project_id"]
    assert credentials.scopes == ["https://www.google.com/calendar/feeds"]


def test_load_credentials_from_file_service_account_with_quota_project():
    credentials, project_id = _default.load_credentials_from_file(
        SERVICE_ACCOUNT_FILE, quota_project_id="project-foo"
    )
    assert isinstance(credentials, service_account.Credentials)
    assert project_id == SERVICE_ACCOUNT_FILE_DATA["project_id"]
    assert credentials.quota_project_id == "project-foo"


def test_load_credentials_from_file_service_account_bad_format(tmpdir):
    filename = tmpdir.join("serivce_account_bad.json")
    filename.write(json.dumps({"type": "service_account"}))

    with pytest.raises(exceptions.DefaultCredentialsError) as excinfo:
        _default.load_credentials_from_file(str(filename))

    assert excinfo.match(r"Failed to load service account")
    assert excinfo.match(r"missing fields")


@EXTERNAL_ACCOUNT_GET_PROJECT_ID_PATCH
def test_load_credentials_from_file_external_account_identity_pool(
    get_project_id, tmpdir
):
    config_file = tmpdir.join("config.json")
    config_file.write(json.dumps(IDENTITY_POOL_DATA))
    credentials, project_id = _default.load_credentials_from_file(str(config_file))

    assert isinstance(credentials, identity_pool.Credentials)
    # Since no scopes are specified, the project ID cannot be determined.
    assert project_id is None
    assert get_project_id.called


@EXTERNAL_ACCOUNT_GET_PROJECT_ID_PATCH
def test_load_credentials_from_file_external_account_aws(get_project_id, tmpdir):
    config_file = tmpdir.join("config.json")
    config_file.write(json.dumps(AWS_DATA))
    credentials, project_id = _default.load_credentials_from_file(str(config_file))

    assert isinstance(credentials, aws.Credentials)
    # Since no scopes are specified, the project ID cannot be determined.
    assert project_id is None
    assert get_project_id.called


@EXTERNAL_ACCOUNT_GET_PROJECT_ID_PATCH
def test_load_credentials_from_file_external_account_with_user_and_default_scopes(
    get_project_id, tmpdir
):
    config_file = tmpdir.join("config.json")
    config_file.write(json.dumps(IDENTITY_POOL_DATA))
    credentials, project_id = _default.load_credentials_from_file(
        str(config_file),
        scopes=["https://www.google.com/calendar/feeds"],
        default_scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    assert isinstance(credentials, identity_pool.Credentials)
    # Since scopes are specified, the project ID can be determined.
    assert project_id is mock.sentinel.project_id
    assert credentials.scopes == ["https://www.google.com/calendar/feeds"]
    assert credentials.default_scopes == [
        "https://www.googleapis.com/auth/cloud-platform"
    ]


@EXTERNAL_ACCOUNT_GET_PROJECT_ID_PATCH
def test_load_credentials_from_file_external_account_with_quota_project(
    get_project_id, tmpdir
):
    config_file = tmpdir.join("config.json")
    config_file.write(json.dumps(IDENTITY_POOL_DATA))
    credentials, project_id = _default.load_credentials_from_file(
        str(config_file), quota_project_id="project-foo"
    )

    assert isinstance(credentials, identity_pool.Credentials)
    # Since no scopes are specified, the project ID cannot be determined.
    assert project_id is None
    assert credentials.quota_project_id == "project-foo"


def test_load_credentials_from_file_external_account_bad_format(tmpdir):
    filename = tmpdir.join("external_account_bad.json")
    filename.write(json.dumps({"type": "external_account"}))

    with pytest.raises(exceptions.DefaultCredentialsError) as excinfo:
        _default.load_credentials_from_file(str(filename))

    assert excinfo.match(
        "Failed to load external account credentials from {}".format(str(filename))
    )


@EXTERNAL_ACCOUNT_GET_PROJECT_ID_PATCH
def test_load_credentials_from_file_external_account_explicit_request(
    get_project_id, tmpdir
):
    config_file = tmpdir.join("config.json")
    config_file.write(json.dumps(IDENTITY_POOL_DATA))
    credentials, project_id = _default.load_credentials_from_file(
        str(config_file),
        request=mock.sentinel.request,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    assert isinstance(credentials, identity_pool.Credentials)
    # Since scopes are specified, the project ID can be determined.
    assert project_id is mock.sentinel.project_id
    get_project_id.assert_called_with(credentials, request=mock.sentinel.request)


@mock.patch.dict(os.environ, {}, clear=True)
def test__get_explicit_environ_credentials_no_env():
    assert _default._get_explicit_environ_credentials() == (None, None)


@LOAD_FILE_PATCH
def test__get_explicit_environ_credentials(load, monkeypatch):
    monkeypatch.setenv(environment_vars.CREDENTIALS, "filename")

    credentials, project_id = _default._get_explicit_environ_credentials()

    assert credentials is MOCK_CREDENTIALS
    assert project_id is mock.sentinel.project_id
    load.assert_called_with("filename")


@LOAD_FILE_PATCH
def test__get_explicit_environ_credentials_no_project_id(load, monkeypatch):
    load.return_value = MOCK_CREDENTIALS, None
    monkeypatch.setenv(environment_vars.CREDENTIALS, "filename")

    credentials, project_id = _default._get_explicit_environ_credentials()

    assert credentials is MOCK_CREDENTIALS
    assert project_id is None


@mock.patch(
    "google.auth._cloud_sdk.get_application_default_credentials_path", autospec=True
)
@mock.patch("google.auth._default._get_gcloud_sdk_credentials", autospec=True)
def test__get_explicit_environ_credentials_fallback_to_gcloud(
    get_gcloud_creds, get_adc_path, monkeypatch
):
    # Set explicit credentials path to cloud sdk credentials path.
    get_adc_path.return_value = "filename"
    monkeypatch.setenv(environment_vars.CREDENTIALS, "filename")

    _default._get_explicit_environ_credentials()

    # Check we fall back to cloud sdk flow since explicit credentials path is
    # cloud sdk credentials path
    get_gcloud_creds.assert_called_once()


@LOAD_FILE_PATCH
@mock.patch(
    "google.auth._cloud_sdk.get_application_default_credentials_path", autospec=True
)
def test__get_gcloud_sdk_credentials(get_adc_path, load):
    get_adc_path.return_value = SERVICE_ACCOUNT_FILE

    credentials, project_id = _default._get_gcloud_sdk_credentials()

    assert credentials is MOCK_CREDENTIALS
    assert project_id is mock.sentinel.project_id
    load.assert_called_with(SERVICE_ACCOUNT_FILE)


@mock.patch(
    "google.auth._cloud_sdk.get_application_default_credentials_path", autospec=True
)
def test__get_gcloud_sdk_credentials_non_existent(get_adc_path, tmpdir):
    non_existent = tmpdir.join("non-existent")
    get_adc_path.return_value = str(non_existent)

    credentials, project_id = _default._get_gcloud_sdk_credentials()

    assert credentials is None
    assert project_id is None


@mock.patch(
    "google.auth._cloud_sdk.get_project_id",
    return_value=mock.sentinel.project_id,
    autospec=True,
)
@mock.patch("os.path.isfile", return_value=True, autospec=True)
@LOAD_FILE_PATCH
def test__get_gcloud_sdk_credentials_project_id(load, unused_isfile, get_project_id):
    # Don't return a project ID from load file, make the function check
    # the Cloud SDK project.
    load.return_value = MOCK_CREDENTIALS, None

    credentials, project_id = _default._get_gcloud_sdk_credentials()

    assert credentials == MOCK_CREDENTIALS
    assert project_id == mock.sentinel.project_id
    assert get_project_id.called


@mock.patch("google.auth._cloud_sdk.get_project_id", return_value=None, autospec=True)
@mock.patch("os.path.isfile", return_value=True)
@LOAD_FILE_PATCH
def test__get_gcloud_sdk_credentials_no_project_id(load, unused_isfile, get_project_id):
    # Don't return a project ID from load file, make the function check
    # the Cloud SDK project.
    load.return_value = MOCK_CREDENTIALS, None

    credentials, project_id = _default._get_gcloud_sdk_credentials()

    assert credentials == MOCK_CREDENTIALS
    assert project_id is None
    assert get_project_id.called


class _AppIdentityModule(object):
    """The interface of the App Idenity app engine module.
    See https://cloud.google.com/appengine/docs/standard/python/refdocs\
    /google.appengine.api.app_identity.app_identity
    """

    def get_application_id(self):
        raise NotImplementedError()


@pytest.fixture
def app_identity(monkeypatch):
    """Mocks the app_identity module for google.auth.app_engine."""
    app_identity_module = mock.create_autospec(_AppIdentityModule, instance=True)
    monkeypatch.setattr(app_engine, "app_identity", app_identity_module)
    yield app_identity_module


@mock.patch.dict(os.environ)
def test__get_gae_credentials_gen1(app_identity):
    os.environ[environment_vars.LEGACY_APPENGINE_RUNTIME] = "python27"
    app_identity.get_application_id.return_value = mock.sentinel.project

    credentials, project_id = _default._get_gae_credentials()

    assert isinstance(credentials, app_engine.Credentials)
    assert project_id == mock.sentinel.project


@mock.patch.dict(os.environ)
def test__get_gae_credentials_gen2():
    os.environ["GAE_RUNTIME"] = "python37"
    credentials, project_id = _default._get_gae_credentials()
    assert credentials is None
    assert project_id is None


@mock.patch.dict(os.environ)
def test__get_gae_credentials_gen2_backwards_compat():
    # compat helpers may copy GAE_RUNTIME to APPENGINE_RUNTIME
    # for backwards compatibility with code that relies on it
    os.environ[environment_vars.LEGACY_APPENGINE_RUNTIME] = "python37"
    os.environ["GAE_RUNTIME"] = "python37"
    credentials, project_id = _default._get_gae_credentials()
    assert credentials is None
    assert project_id is None


def test__get_gae_credentials_env_unset():
    assert environment_vars.LEGACY_APPENGINE_RUNTIME not in os.environ
    assert "GAE_RUNTIME" not in os.environ
    credentials, project_id = _default._get_gae_credentials()
    assert credentials is None
    assert project_id is None


@mock.patch.dict(os.environ)
def test__get_gae_credentials_no_app_engine():
    # test both with and without LEGACY_APPENGINE_RUNTIME setting
    assert environment_vars.LEGACY_APPENGINE_RUNTIME not in os.environ

    import sys

    with mock.patch.dict(sys.modules, {"google.auth.app_engine": None}):
        credentials, project_id = _default._get_gae_credentials()
        assert credentials is None
        assert project_id is None

        os.environ[environment_vars.LEGACY_APPENGINE_RUNTIME] = "python27"
        credentials, project_id = _default._get_gae_credentials()
        assert credentials is None
        assert project_id is None


@mock.patch.dict(os.environ)
@mock.patch.object(app_engine, "app_identity", new=None)
def test__get_gae_credentials_no_apis():
    # test both with and without LEGACY_APPENGINE_RUNTIME setting
    assert environment_vars.LEGACY_APPENGINE_RUNTIME not in os.environ

    credentials, project_id = _default._get_gae_credentials()
    assert credentials is None
    assert project_id is None

    os.environ[environment_vars.LEGACY_APPENGINE_RUNTIME] = "python27"
    credentials, project_id = _default._get_gae_credentials()
    assert credentials is None
    assert project_id is None


@mock.patch(
    "google.auth.compute_engine._metadata.ping", return_value=True, autospec=True
)
@mock.patch(
    "google.auth.compute_engine._metadata.get_project_id",
    return_value="example-project",
    autospec=True,
)
def test__get_gce_credentials(unused_get, unused_ping):
    credentials, project_id = _default._get_gce_credentials()

    assert isinstance(credentials, compute_engine.Credentials)
    assert project_id == "example-project"


@mock.patch(
    "google.auth.compute_engine._metadata.ping", return_value=False, autospec=True
)
def test__get_gce_credentials_no_ping(unused_ping):
    credentials, project_id = _default._get_gce_credentials()

    assert credentials is None
    assert project_id is None


@mock.patch(
    "google.auth.compute_engine._metadata.ping", return_value=True, autospec=True
)
@mock.patch(
    "google.auth.compute_engine._metadata.get_project_id",
    side_effect=exceptions.TransportError(),
    autospec=True,
)
def test__get_gce_credentials_no_project_id(unused_get, unused_ping):
    credentials, project_id = _default._get_gce_credentials()

    assert isinstance(credentials, compute_engine.Credentials)
    assert project_id is None


def test__get_gce_credentials_no_compute_engine():
    import sys

    with mock.patch.dict("sys.modules"):
        sys.modules["google.auth.compute_engine"] = None
        credentials, project_id = _default._get_gce_credentials()
        assert credentials is None
        assert project_id is None


@mock.patch(
    "google.auth.compute_engine._metadata.ping", return_value=False, autospec=True
)
def test__get_gce_credentials_explicit_request(ping):
    _default._get_gce_credentials(mock.sentinel.request)
    ping.assert_called_with(request=mock.sentinel.request)


@mock.patch(
    "google.auth._default._get_explicit_environ_credentials",
    return_value=(MOCK_CREDENTIALS, mock.sentinel.project_id),
    autospec=True,
)
def test_default_early_out(unused_get):
    assert _default.default() == (MOCK_CREDENTIALS, mock.sentinel.project_id)


@mock.patch(
    "google.auth._default._get_explicit_environ_credentials",
    return_value=(MOCK_CREDENTIALS, mock.sentinel.project_id),
    autospec=True,
)
def test_default_explict_project_id(unused_get, monkeypatch):
    monkeypatch.setenv(environment_vars.PROJECT, "explicit-env")
    assert _default.default() == (MOCK_CREDENTIALS, "explicit-env")


@mock.patch(
    "google.auth._default._get_explicit_environ_credentials",
    return_value=(MOCK_CREDENTIALS, mock.sentinel.project_id),
    autospec=True,
)
def test_default_explict_legacy_project_id(unused_get, monkeypatch):
    monkeypatch.setenv(environment_vars.LEGACY_PROJECT, "explicit-env")
    assert _default.default() == (MOCK_CREDENTIALS, "explicit-env")


@mock.patch("logging.Logger.warning", autospec=True)
@mock.patch(
    "google.auth._default._get_explicit_environ_credentials",
    return_value=(MOCK_CREDENTIALS, None),
    autospec=True,
)
@mock.patch(
    "google.auth._default._get_gcloud_sdk_credentials",
    return_value=(MOCK_CREDENTIALS, None),
    autospec=True,
)
@mock.patch(
    "google.auth._default._get_gae_credentials",
    return_value=(MOCK_CREDENTIALS, None),
    autospec=True,
)
@mock.patch(
    "google.auth._default._get_gce_credentials",
    return_value=(MOCK_CREDENTIALS, None),
    autospec=True,
)
def test_default_without_project_id(
    unused_gce, unused_gae, unused_sdk, unused_explicit, logger_warning
):
    assert _default.default() == (MOCK_CREDENTIALS, None)
    logger_warning.assert_called_with(mock.ANY, mock.ANY, mock.ANY)


@mock.patch(
    "google.auth._default._get_explicit_environ_credentials",
    return_value=(None, None),
    autospec=True,
)
@mock.patch(
    "google.auth._default._get_gcloud_sdk_credentials",
    return_value=(None, None),
    autospec=True,
)
@mock.patch(
    "google.auth._default._get_gae_credentials",
    return_value=(None, None),
    autospec=True,
)
@mock.patch(
    "google.auth._default._get_gce_credentials",
    return_value=(None, None),
    autospec=True,
)
def test_default_fail(unused_gce, unused_gae, unused_sdk, unused_explicit):
    with pytest.raises(exceptions.DefaultCredentialsError):
        assert _default.default()


@mock.patch(
    "google.auth._default._get_explicit_environ_credentials",
    return_value=(MOCK_CREDENTIALS, mock.sentinel.project_id),
    autospec=True,
)
@mock.patch(
    "google.auth.credentials.with_scopes_if_required",
    return_value=MOCK_CREDENTIALS,
    autospec=True,
)
def test_default_scoped(with_scopes, unused_get):
    scopes = ["one", "two"]

    credentials, project_id = _default.default(scopes=scopes)

    assert credentials == with_scopes.return_value
    assert project_id == mock.sentinel.project_id
    with_scopes.assert_called_once_with(MOCK_CREDENTIALS, scopes, default_scopes=None)


@mock.patch(
    "google.auth._default._get_explicit_environ_credentials",
    return_value=(MOCK_CREDENTIALS, mock.sentinel.project_id),
    autospec=True,
)
def test_default_quota_project(with_quota_project):
    credentials, project_id = _default.default(quota_project_id="project-foo")

    MOCK_CREDENTIALS.with_quota_project.assert_called_once_with("project-foo")
    assert project_id == mock.sentinel.project_id


@mock.patch(
    "google.auth._default._get_explicit_environ_credentials",
    return_value=(MOCK_CREDENTIALS, mock.sentinel.project_id),
    autospec=True,
)
def test_default_no_app_engine_compute_engine_module(unused_get):
    """
    google.auth.compute_engine and google.auth.app_engine are both optional
    to allow not including them when using this package. This verifies
    that default fails gracefully if these modules are absent
    """
    import sys

    with mock.patch.dict("sys.modules"):
        sys.modules["google.auth.compute_engine"] = None
        sys.modules["google.auth.app_engine"] = None
        assert _default.default() == (MOCK_CREDENTIALS, mock.sentinel.project_id)


@EXTERNAL_ACCOUNT_GET_PROJECT_ID_PATCH
def test_default_environ_external_credentials(get_project_id, monkeypatch, tmpdir):
    config_file = tmpdir.join("config.json")
    config_file.write(json.dumps(IDENTITY_POOL_DATA))
    monkeypatch.setenv(environment_vars.CREDENTIALS, str(config_file))

    credentials, project_id = _default.default()

    assert isinstance(credentials, identity_pool.Credentials)
    # Without scopes, project ID cannot be determined.
    assert project_id is None


@EXTERNAL_ACCOUNT_GET_PROJECT_ID_PATCH
def test_default_environ_external_credentials_with_user_and_default_scopes_and_quota_project_id(
    get_project_id, monkeypatch, tmpdir
):
    config_file = tmpdir.join("config.json")
    config_file.write(json.dumps(IDENTITY_POOL_DATA))
    monkeypatch.setenv(environment_vars.CREDENTIALS, str(config_file))

    credentials, project_id = _default.default(
        scopes=["https://www.google.com/calendar/feeds"],
        default_scopes=["https://www.googleapis.com/auth/cloud-platform"],
        quota_project_id="project-foo",
    )

    assert isinstance(credentials, identity_pool.Credentials)
    assert project_id is mock.sentinel.project_id
    assert credentials.quota_project_id == "project-foo"
    assert credentials.scopes == ["https://www.google.com/calendar/feeds"]
    assert credentials.default_scopes == [
        "https://www.googleapis.com/auth/cloud-platform"
    ]


@EXTERNAL_ACCOUNT_GET_PROJECT_ID_PATCH
def test_default_environ_external_credentials_explicit_request_with_scopes(
    get_project_id, monkeypatch, tmpdir
):
    config_file = tmpdir.join("config.json")
    config_file.write(json.dumps(IDENTITY_POOL_DATA))
    monkeypatch.setenv(environment_vars.CREDENTIALS, str(config_file))

    credentials, project_id = _default.default(
        request=mock.sentinel.request,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    assert isinstance(credentials, identity_pool.Credentials)
    assert project_id is mock.sentinel.project_id
    # default() will initialize new credentials via with_scopes_if_required
    # and potentially with_quota_project.
    # As a result the caller of get_project_id() will not match the returned
    # credentials.
    get_project_id.assert_called_with(mock.ANY, request=mock.sentinel.request)


def test_default_environ_external_credentials_bad_format(monkeypatch, tmpdir):
    filename = tmpdir.join("external_account_bad.json")
    filename.write(json.dumps({"type": "external_account"}))
    monkeypatch.setenv(environment_vars.CREDENTIALS, str(filename))

    with pytest.raises(exceptions.DefaultCredentialsError) as excinfo:
        _default.default()

    assert excinfo.match(
        "Failed to load external account credentials from {}".format(str(filename))
    )
