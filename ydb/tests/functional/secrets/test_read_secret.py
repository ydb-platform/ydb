import grpc
import pytest

from ydb.public.api.grpc import ydb_secret_v1_pb2_grpc
from ydb.public.api.protos import ydb_secret_pb2, ydb_status_codes_pb2
from ydb.tests.oss.ydb_sdk_import import ydb

ADMIN = "root@builtin"
USER = "reader@builtin"
DESCRIBE_SCHEMA = "ydb.granular.describe_schema"

# TODO(vlad-serikov): After rebasing onto the EnableReadSecrets change, enable the flag for these tests
# and add a case with it disabled: even ADMIN must get grpc.StatusCode.FAILED_PRECONDITION.

CLUSTER_CONFIG = dict(
    default_clusteradmin=ADMIN,
    extra_feature_flags=["enable_schema_secrets"],
    extra_grpc_services=["secret"],
)


def read_secret(stub, database, path, token):
    metadata = [("x-ydb-database", database)]
    if token:
        metadata.append(("x-ydb-auth-ticket", token))
    return stub.ReadSecret(ydb_secret_pb2.ReadSecretRequest(path=path), metadata=metadata, timeout=10).operation


@pytest.fixture
def stub(ydb_endpoint):
    with grpc.insecure_channel(ydb_endpoint) as channel:
        yield ydb_secret_v1_pb2_grpc.SecretServiceStub(channel)


def create_secret_with_private_acl(driver, path, value, owner):
    with ydb.QuerySessionPool(driver) as pool:
        pool.execute_with_retries(f"CREATE SECRET `{path}` WITH (value = '{value}')")
    permissions = ydb.ModifyPermissionsSettings().change_owner(owner).clear_permissions()
    permissions.to_pb().interrupt_inheritance = True
    driver.scheme_client.modify_permissions(path, permissions)


def test_read_requires_cluster_administrator(stub, ydb_database, ydb_client):
    driver = ydb_client(ydb_database, credentials=ydb.AuthTokenCredentials(ADMIN))
    driver.scheme_client.modify_permissions(
        ydb_database,
        ydb.ModifyPermissionsSettings().grant_permissions(
            USER,
            ["ydb.generic.full"],
        ),
    )
    path = f"{ydb_database}/secret"
    with ydb.QuerySessionPool(driver) as pool:
        pool.execute_with_retries(f"CREATE SECRET `{path}` WITH (value = 'test-value')")
    driver.scheme_client.modify_permissions(path, ydb.ModifyPermissionsSettings().change_owner(USER))

    for token in (USER, None):
        for target in (path, f"{ydb_database}/missing"):
            operation = read_secret(stub, ydb_database, target, token)
            assert operation.ready and operation.status == ydb_status_codes_pb2.StatusIds.UNAUTHORIZED, operation.issues
            assert any("cluster administrator" in issue.message for issue in operation.issues), operation.issues
            assert not operation.HasField("result")


def test_admin_read_rejects_missing_secret_and_directory(stub, ydb_database, ydb_client):
    driver = ydb_client(ydb_database, credentials=ydb.AuthTokenCredentials(ADMIN))
    directory = f"{ydb_database}/directory"
    driver.scheme_client.make_directory(directory)
    for path in (f"{ydb_database}/missing", directory, "/Root/another_database/secret"):
        operation = read_secret(stub, ydb_database, path, ADMIN)
        assert operation.status == ydb_status_codes_pb2.StatusIds.SCHEME_ERROR, operation.issues
        assert not operation.HasField("result")


@pytest.mark.parametrize("value", ["test-value", ""])
def test_administrator_requires_describe_schema(stub, ydb_database, ydb_client, value):
    driver = ydb_client(ydb_database, credentials=ydb.AuthTokenCredentials(ADMIN))
    path = f"{ydb_database}/secret"
    create_secret_with_private_acl(driver, path, value, USER)
    denied = read_secret(stub, ydb_database, path, ADMIN)
    assert denied.ready and denied.status == ydb_status_codes_pb2.StatusIds.UNAUTHORIZED, denied.issues
    assert not denied.HasField("result")

    driver.scheme_client.modify_permissions(
        path, ydb.ModifyPermissionsSettings().grant_permissions(ADMIN, [DESCRIBE_SCHEMA])
    )
    operation = read_secret(stub, ydb_database, path, ADMIN)
    assert operation.ready and operation.status == ydb_status_codes_pb2.StatusIds.SUCCESS, operation.issues
    result = ydb_secret_pb2.ReadSecretResult()
    assert operation.result.Unpack(result)
    assert result.value == value.encode()


def test_describe_schema_is_checked_per_secret_and_after_revoke(stub, ydb_database, ydb_client):
    driver = ydb_client(ydb_database, credentials=ydb.AuthTokenCredentials(ADMIN))
    first, second = f"{ydb_database}/first", f"{ydb_database}/second"
    create_secret_with_private_acl(driver, first, "first-value", USER)
    create_secret_with_private_acl(driver, second, "second-value", USER)
    driver.scheme_client.modify_permissions(
        first, ydb.ModifyPermissionsSettings().grant_permissions(ADMIN, [DESCRIBE_SCHEMA])
    )
    driver.scheme_client.modify_permissions(
        second, ydb.ModifyPermissionsSettings().grant_permissions(ADMIN, ["ydb.granular.select_row"])
    )

    allowed = read_secret(stub, ydb_database, first, ADMIN)
    assert allowed.ready and allowed.status == ydb_status_codes_pb2.StatusIds.SUCCESS, allowed.issues
    result = ydb_secret_pb2.ReadSecretResult()
    assert allowed.result.Unpack(result)
    assert result.value == b"first-value"

    denied = read_secret(stub, ydb_database, second, ADMIN)
    assert denied.ready and denied.status == ydb_status_codes_pb2.StatusIds.UNAUTHORIZED, denied.issues
    assert not denied.HasField("result")

    driver.scheme_client.modify_permissions(
        first, ydb.ModifyPermissionsSettings().revoke_permissions(ADMIN, [DESCRIBE_SCHEMA])
    )
    after_revoke = read_secret(stub, ydb_database, first, ADMIN)
    assert after_revoke.ready
    assert after_revoke.status == ydb_status_codes_pb2.StatusIds.UNAUTHORIZED, after_revoke.issues
    assert not after_revoke.HasField("result")


def test_non_admin_with_describe_schema_can_only_describe_secret(stub, ydb_database, ydb_client):
    driver = ydb_client(ydb_database, credentials=ydb.AuthTokenCredentials(ADMIN))
    path = f"{ydb_database}/secret"
    create_secret_with_private_acl(driver, path, "test-value", ADMIN)
    driver.scheme_client.modify_permissions(
        ydb_database, ydb.ModifyPermissionsSettings().grant_permissions(USER, ["ydb.database.connect"])
    )
    driver.scheme_client.modify_permissions(
        path, ydb.ModifyPermissionsSettings().grant_permissions(USER, [DESCRIBE_SCHEMA])
    )

    description = stub.DescribeSecret(
        ydb_secret_pb2.DescribeSecretRequest(path=path),
        metadata=[("x-ydb-database", ydb_database), ("x-ydb-auth-ticket", USER)],
        timeout=10,
    ).operation
    assert description.ready and description.status == ydb_status_codes_pb2.StatusIds.SUCCESS, description.issues
    denied = read_secret(stub, ydb_database, path, USER)
    assert denied.ready and denied.status == ydb_status_codes_pb2.StatusIds.UNAUTHORIZED, denied.issues
    assert not denied.HasField("result")
