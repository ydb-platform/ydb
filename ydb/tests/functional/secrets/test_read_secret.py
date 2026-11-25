import grpc
import pytest

from ydb.public.api.grpc import ydb_secret_v1_pb2_grpc
from ydb.public.api.protos import ydb_secret_pb2, ydb_status_codes_pb2
from ydb.tests.oss.ydb_sdk_import import ydb

ADMIN = "root@builtin"
USER = "reader@builtin"
DESCRIBE_SCHEMA = "ydb.granular.describe_schema"
DATABASE_USE = "ydb.generic.use"
DATABASE_FULL = "ydb.generic.full"

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


@pytest.fixture
def driver(ydb_client, ydb_database):
    admin_driver = ydb_client(ydb_database, credentials=ydb.AuthTokenCredentials(ADMIN))
    admin_driver.wait(timeout=10)
    return admin_driver


def create_secret_with_private_acl(driver, path, value, owner):
    with ydb.QuerySessionPool(driver) as pool:
        pool.execute_with_retries(f"CREATE SECRET `{path}` WITH (value = '{value}')")
    permissions = ydb.ModifyPermissionsSettings().change_owner(owner).clear_permissions()
    permissions.to_pb().interrupt_inheritance = True
    driver.scheme_client.modify_permissions(path, permissions)


def test_read_requires_full_on_database_root(stub, ydb_database, driver):
    driver.scheme_client.modify_permissions(
        ydb_database,
        ydb.ModifyPermissionsSettings().grant_permissions(USER, [DATABASE_USE]),
    )
    path = f"{ydb_database}/secret"
    with ydb.QuerySessionPool(driver) as pool:
        pool.execute_with_retries(f"CREATE SECRET `{path}` WITH (value = 'test-value')")
    driver.scheme_client.modify_permissions(path, ydb.ModifyPermissionsSettings().change_owner(USER))

    for token in (USER, None):
        for target in (path, f"{ydb_database}/missing"):
            operation = read_secret(stub, ydb_database, target, token)
            assert operation.ready and operation.status == ydb_status_codes_pb2.StatusIds.UNAUTHORIZED, operation.issues
            assert not operation.HasField("result")


def test_cluster_administrator_without_database_full_cannot_read(stub, ydb_database, driver):
    path = f"{ydb_database}/secret"
    create_secret_with_private_acl(driver, path, "test-value", ADMIN)
    root_acl = ydb.ModifyPermissionsSettings().clear_permissions().change_owner(USER)
    # The ydb_database fixture drops the database as ADMIN after this test.
    root_acl.grant_permissions(ADMIN, ["ydb.database.connect", "ydb.database.drop", DESCRIBE_SCHEMA])
    root_acl.to_pb().interrupt_inheritance = True
    driver.scheme_client.modify_permissions(ydb_database, root_acl)

    denied = read_secret(stub, ydb_database, path, ADMIN)
    assert denied.ready and denied.status == ydb_status_codes_pb2.StatusIds.UNAUTHORIZED, denied.issues
    assert any("FULL on the database root" in issue.message for issue in denied.issues), denied.issues
    assert not denied.HasField("result")


def test_admin_read_rejects_missing_secret_and_directory(stub, ydb_database, driver):
    directory = f"{ydb_database}/directory"
    driver.scheme_client.make_directory(directory)
    for path in (f"{ydb_database}/missing", directory):
        operation = read_secret(stub, ydb_database, path, ADMIN)
        assert operation.status == ydb_status_codes_pb2.StatusIds.SCHEME_ERROR, operation.issues
        assert not operation.HasField("result")

    path = f"{directory}/secret"
    create_secret_with_private_acl(driver, path, "test-value", ADMIN)
    operation = read_secret(stub, directory, path, ADMIN)
    assert operation.status == ydb_status_codes_pb2.StatusIds.BAD_REQUEST, operation.issues
    assert not operation.HasField("result")


@pytest.mark.parametrize("value", ["test-value", ""])
def test_database_full_reads_secret_without_describe_schema(stub, ydb_database, driver, value):
    path = f"{ydb_database}/secret"
    create_secret_with_private_acl(driver, path, value, ADMIN)
    driver.scheme_client.modify_permissions(
        ydb_database, ydb.ModifyPermissionsSettings().grant_permissions(USER, [DATABASE_FULL])
    )

    description = stub.DescribeSecret(
        ydb_secret_pb2.DescribeSecretRequest(path=path),
        metadata=[("x-ydb-database", ydb_database), ("x-ydb-auth-ticket", USER)],
        timeout=10,
    ).operation
    assert description.ready and description.status == ydb_status_codes_pb2.StatusIds.UNAUTHORIZED, description.issues
    operation = read_secret(stub, ydb_database, path, USER)
    assert operation.ready and operation.status == ydb_status_codes_pb2.StatusIds.SUCCESS, operation.issues
    result = ydb_secret_pb2.ReadSecretResult()
    assert operation.result.Unpack(result)
    assert result.value == value.encode()


def test_database_full_is_scoped_to_one_database(stub, ydb_database, ydb_database_module_scope, ydb_client, driver):
    other_database = ydb_database_module_scope
    other_driver = ydb_client(other_database, credentials=ydb.AuthTokenCredentials(ADMIN))
    other_driver.wait(timeout=10)
    path = f"{other_database}/secret"
    create_secret_with_private_acl(other_driver, path, "other-database-value", ADMIN)
    driver.scheme_client.modify_permissions(
        ydb_database, ydb.ModifyPermissionsSettings().grant_permissions(USER, [DATABASE_FULL])
    )

    denied = read_secret(stub, ydb_database, path, USER)
    assert denied.ready and denied.status == ydb_status_codes_pb2.StatusIds.SCHEME_ERROR, denied.issues
    assert not denied.HasField("result")


def test_read_rechecks_effective_database_full_after_revoke(stub, ydb_database, driver):
    path = f"{ydb_database}/secret"
    create_secret_with_private_acl(driver, path, "test-value", ADMIN)
    driver.scheme_client.modify_permissions(
        ydb_database, ydb.ModifyPermissionsSettings().grant_permissions(USER, [DATABASE_USE])
    )
    driver.scheme_client.modify_permissions(
        path, ydb.ModifyPermissionsSettings().grant_permissions(USER, [DESCRIBE_SCHEMA])
    )

    driver.scheme_client.modify_permissions(
        ydb_database, ydb.ModifyPermissionsSettings().grant_permissions(USER, ["ydb.generic.manage"])
    )
    allowed = read_secret(stub, ydb_database, path, USER)
    assert allowed.ready and allowed.status == ydb_status_codes_pb2.StatusIds.SUCCESS, allowed.issues
    driver.scheme_client.modify_permissions(
        ydb_database, ydb.ModifyPermissionsSettings().revoke_permissions(USER, ["ydb.generic.manage"])
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
