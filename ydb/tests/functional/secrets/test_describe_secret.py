import grpc
import pytest

from ydb.public.api.grpc import ydb_secret_v1_pb2_grpc
from ydb.public.api.protos import ydb_scheme_pb2, ydb_secret_pb2, ydb_status_codes_pb2
from ydb.tests.oss.ydb_sdk_import import ydb

ADMIN = "root@builtin"
USER = "reader@builtin"
CONNECT = "ydb.database.connect"
CREATE = "ydb.database.create"
DROP = "ydb.database.drop"
DESCRIBE = "ydb.granular.describe_schema"
Status = ydb_status_codes_pb2.StatusIds

CLUSTER_CONFIG = dict(
    default_clusteradmin=ADMIN,
    extra_feature_flags=["enable_schema_secrets", "enable_read_secret_rpc_handler"],
    extra_grpc_services=["secret"],
)


@pytest.fixture
def stub(ydb_endpoint):
    with grpc.insecure_channel(ydb_endpoint) as channel:
        yield ydb_secret_v1_pb2_grpc.SecretServiceStub(channel)


def describe_secret(stub, database, path, user=USER):
    return stub.DescribeSecret(
        ydb_secret_pb2.DescribeSecretRequest(path=path),
        metadata=(("x-ydb-database", database), ("x-ydb-auth-ticket", user)),
        timeout=10,
    ).operation


@pytest.fixture
def driver(ydb_client, ydb_database):
    admin = ydb_client(ydb_database, credentials=ydb.AuthTokenCredentials(ADMIN))
    admin.wait(timeout=10)
    return admin


def create_secret(driver, path, owner):
    with ydb.QuerySessionPool(driver) as pool:
        pool.execute_with_retries(f"CREATE SECRET `{path}` WITH (value = 'test-value')")
    acl = ydb.ModifyPermissionsSettings().change_owner(owner).clear_permissions()
    acl.to_pb().interrupt_inheritance = True
    driver.scheme_client.modify_permissions(path, acl)


@pytest.mark.parametrize("revoked", [CREATE, DROP])
def test_describe_without_describe_schema_requires_both_database_permissions(stub, driver, ydb_database, revoked):
    path = f"{ydb_database}/secret"
    create_secret(driver, path, ADMIN)
    driver.scheme_client.modify_permissions(
        ydb_database, ydb.ModifyPermissionsSettings().grant_permissions(USER, [CONNECT, CREATE, DROP])
    )

    allowed = describe_secret(stub, ydb_database, path)
    assert allowed.ready and allowed.status == Status.SUCCESS, allowed.issues
    assert not allowed.issues
    result = ydb_secret_pb2.DescribeSecretResult()
    assert allowed.result.Unpack(result)
    assert result.self.name == "secret"
    assert result.self.owner == ADMIN
    assert result.self.type == ydb_scheme_pb2.Entry.SECRET
    assert result.version == 0

    driver.scheme_client.modify_permissions(
        ydb_database, ydb.ModifyPermissionsSettings().revoke_permissions(USER, [revoked])
    )
    driver.scheme_client.modify_permissions(
        path, ydb.ModifyPermissionsSettings().grant_permissions(USER, [CREATE, DROP])
    )
    for target, expected_status in ((path, Status.UNAUTHORIZED), (f"{ydb_database}/missing", Status.SCHEME_ERROR)):
        operation = describe_secret(stub, ydb_database, target)
        assert operation.ready and operation.status == expected_status, operation.issues
        assert not operation.HasField("result")


def test_describe_with_ownership_or_describe_schema(stub, driver, ydb_client, ydb_database):
    path = f"{ydb_database}/secret"
    create_secret(driver, path, USER)
    driver.scheme_client.modify_permissions(
        ydb_database, ydb.ModifyPermissionsSettings().grant_permissions(USER, [CONNECT])
    )
    allowed = describe_secret(stub, ydb_database, path)
    assert allowed.ready and allowed.status == Status.SUCCESS, allowed.issues
    result = ydb_secret_pb2.DescribeSecretResult()
    assert allowed.result.Unpack(result)
    assert result.self.owner == USER

    owner = ydb_client(ydb_database, credentials=ydb.AuthTokenCredentials(USER))
    owner.wait(timeout=10)
    owner.scheme_client.modify_permissions(path, ydb.ModifyPermissionsSettings().change_owner(ADMIN))
    denied = describe_secret(stub, ydb_database, path)
    assert denied.ready and denied.status == Status.UNAUTHORIZED, denied.issues
    assert not denied.HasField("result")

    driver.scheme_client.modify_permissions(path, ydb.ModifyPermissionsSettings().grant_permissions(USER, [DESCRIBE]))
    allowed = describe_secret(stub, ydb_database, path)
    assert allowed.ready and allowed.status == Status.SUCCESS, allowed.issues
    assert allowed.result.Unpack(result)
    assert result.self.owner == ADMIN


def test_describe_scopes_database_permissions_and_ownership(
    stub, driver, ydb_client, ydb_database, ydb_database_module_scope
):
    other = ydb_client(ydb_database_module_scope, credentials=ydb.AuthTokenCredentials(ADMIN))
    other.wait(timeout=10)
    path = f"{ydb_database_module_scope}/secret"
    create_secret(other, path, USER)
    driver.scheme_client.modify_permissions(
        ydb_database, ydb.ModifyPermissionsSettings().grant_permissions(USER, [CONNECT, CREATE, DROP])
    )

    denied = describe_secret(stub, ydb_database, path)
    assert denied.ready and denied.status == Status.SCHEME_ERROR, denied.issues
    assert not denied.HasField("result")


@pytest.mark.parametrize(
    "ydb_cluster_configuration",
    [
        dict(
            CLUSTER_CONFIG,
            extra_feature_flags=["enable_schema_secrets"],
            disabled_feature_flags=["enable_read_secret_rpc_handler"],
        )
    ],
    scope="module",
    ids=["flag-off"],
)
def test_describe_uses_describe_schema_when_flag_is_off(stub, driver, ydb_database):
    path = f"{ydb_database}/secret"
    create_secret(driver, path, ADMIN)
    driver.scheme_client.modify_permissions(
        ydb_database, ydb.ModifyPermissionsSettings().grant_permissions(USER, [CONNECT, CREATE, DROP])
    )
    denied = describe_secret(stub, ydb_database, path)
    assert denied.ready and denied.status == Status.UNAUTHORIZED, denied.issues
    assert not denied.HasField("result")

    driver.scheme_client.modify_permissions(
        ydb_database, ydb.ModifyPermissionsSettings().revoke_permissions(USER, [CREATE, DROP])
    )
    driver.scheme_client.modify_permissions(path, ydb.ModifyPermissionsSettings().grant_permissions(USER, [DESCRIBE]))
    allowed = describe_secret(stub, ydb_database, path)
    assert allowed.ready and allowed.status == Status.SUCCESS, allowed.issues
    result = ydb_secret_pb2.DescribeSecretResult()
    assert allowed.result.Unpack(result)
    assert result.self.owner == ADMIN
    assert result.version == 0
