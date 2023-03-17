import enum

from . import _apis

from . import settings_impl as s_impl

# Workaround for good IDE and universal for runtime
# noinspection PyUnreachableCode
if False:
    from ._grpc.v4.protos import ydb_import_pb2
    from ._grpc.v4 import ydb_import_v1_pb2_grpc
else:
    from ._grpc.common.protos import ydb_import_pb2
    from ._grpc.common import ydb_import_v1_pb2_grpc


from . import operation

_ImportFromS3 = "ImportFromS3"
_progresses = {}


@enum.unique
class ImportProgress(enum.IntEnum):
    UNSPECIFIED = 0
    PREPARING = 1
    TRANSFER_DATA = 2
    BUILD_INDEXES = 3
    DONE = 4
    CANCELLATION = 5
    CANCELLED = 6


def _initialize_progresses():
    for key, value in ydb_import_pb2.ImportProgress.Progress.items():
        _progresses[value] = getattr(ImportProgress, key[len("PROGRESS_") :])


_initialize_progresses()


class ImportFromS3Operation(operation.Operation):
    def __init__(self, rpc_state, response, driver):
        super(ImportFromS3Operation, self).__init__(rpc_state, response, driver)
        metadata = ydb_import_pb2.ImportFromS3Metadata()
        response.operation.metadata.Unpack(metadata)
        self.progress = _progresses.get(metadata.progress)

    def __str__(self):
        return "ImportFromS3Operation<id: %s, progress: %s>" % (
            self.id,
            self.progress.name,
        )

    def __repr__(self):
        return self.__str__()


class ImportFromS3Settings(s_impl.BaseRequestSettings):
    def __init__(self):
        super(ImportFromS3Settings, self).__init__()
        self.items = []
        self.bucket = None
        self.endpoint = None
        self.scheme = 2
        self.uid = None
        self.access_key = None
        self.secret_key = None
        self.number_of_retries = 0

    def with_scheme(self, scheme):
        self.scheme = scheme
        return self

    def with_bucket(self, bucket):
        self.bucket = bucket
        return self

    def with_endpoint(self, endpoint):
        self.endpoint = endpoint
        return self

    def with_access_key(self, access_key):
        self.access_key = access_key
        return self

    def with_uid(self, uid):
        self.uid = uid
        return self

    def with_secret_key(self, secret_key):
        self.secret_key = secret_key
        return self

    def with_number_of_retries(self, number_of_retries):
        self.number_of_retries = number_of_retries
        return self

    def with_source_and_destination(self, source_path, destination_prefix):
        return self.with_item((source_path, destination_prefix))

    def with_item(self, item):
        self.items.append(item)
        return self

    def with_items(self, *items):
        self.items.extend(items)
        return self


def _get_operation_request(operation_id):
    request = _apis.ydb_operation.GetOperationRequest(id=operation_id)
    return request


def _import_from_s3_request_factory(settings):
    request = ydb_import_pb2.ImportFromS3Request(
        settings=ydb_import_pb2.ImportFromS3Settings(
            endpoint=settings.endpoint,
            bucket=settings.bucket,
            access_key=settings.access_key,
            secret_key=settings.secret_key,
            scheme=settings.scheme,
        )
    )

    if settings.uid is not None:
        request.operation_params.labels["uid"] = settings.uid

    if settings.number_of_retries > 0:
        request.settings.number_of_retries = settings.number_of_retries

    for source, destination in settings.items:
        request.settings.items.add(
            source_prefix=source,
            destination_path=destination,
        )

    return request


class ImportClient(object):
    def __init__(self, driver):
        self._driver = driver

    def get_import_from_s3_operation(self, operation_id, settings=None):
        return self._driver(
            _get_operation_request(operation_id),
            _apis.OperationService.Stub,
            _apis.OperationService.GetOperation,
            ImportFromS3Operation,
            settings,
            (self._driver,),
        )

    def import_from_s3(self, settings):
        return self._driver(
            _import_from_s3_request_factory(settings),
            ydb_import_v1_pb2_grpc.ImportServiceStub,
            _ImportFromS3,
            ImportFromS3Operation,
            settings,
            (self._driver,),
        )
