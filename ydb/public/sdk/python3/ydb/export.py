import enum

from . import _apis

from . import settings_impl as s_impl

# Workaround for good IDE and universal for runtime
# noinspection PyUnreachableCode
if False:
    from ._grpc.v4.protos import ydb_export_pb2
    from ._grpc.v4 import ydb_export_v1_pb2_grpc
else:
    from ._grpc.common.protos import ydb_export_pb2
    from ._grpc.common import ydb_export_v1_pb2_grpc

from . import operation

_ExportToYt = "ExportToYt"
_ExportToS3 = "ExportToS3"
_progresses = {}


@enum.unique
class ExportProgress(enum.IntEnum):
    UNSPECIFIED = 0
    PREPARING = 1
    TRANSFER_DATA = 2
    DONE = 3
    CANCELLATION = 4
    CANCELLED = 5


def _initialize_progresses():
    for key, value in ydb_export_pb2.ExportProgress.Progress.items():
        _progresses[value] = getattr(ExportProgress, key[len("PROGRESS_") :])


_initialize_progresses()


class ExportToYTOperation(operation.Operation):
    def __init__(self, rpc_state, response, driver):
        super(ExportToYTOperation, self).__init__(rpc_state, response, driver)
        metadata = ydb_export_pb2.ExportToYtMetadata()
        response.operation.metadata.Unpack(metadata)
        self.progress = _progresses.get(metadata.progress)
        self.items_progress = metadata.items_progress

    def __str__(self):
        return "ExportToYTOperation<id: %s, progress: %s>" % (
            self.id,
            self.progress.name,
        )

    def __repr__(self):
        return self.__str__()


class ExportToS3Operation(operation.Operation):
    def __init__(self, rpc_state, response, driver):
        super(ExportToS3Operation, self).__init__(rpc_state, response, driver)
        metadata = ydb_export_pb2.ExportToS3Metadata()
        response.operation.metadata.Unpack(metadata)
        self.progress = _progresses.get(metadata.progress)
        self.items_progress = metadata.items_progress

    def __str__(self):
        return "ExportToS3Operation<id: %s, progress: %s>" % (
            self.id,
            self.progress.name,
        )

    def __repr__(self):
        return self.__str__()


class ExportToYTSettings(s_impl.BaseRequestSettings):
    def __init__(self):
        super(ExportToYTSettings, self).__init__()
        self.items = []
        self.number_of_retries = 0
        self.token = None
        self.host = None
        self.port = None
        self.uid = None

    def with_port(self, port):
        self.port = port
        return self

    def with_host(self, host):
        self.host = host
        return self

    def with_uid(self, uid):
        self.uid = uid
        return self

    def with_token(self, token):
        self.token = token
        return self

    def with_item(self, item):
        """
        :param: A source & destination tuple to export.
        """
        self.items.append(item)
        return self

    def with_source_and_destination(self, source_path, destination_path):
        return self.with_item((source_path, destination_path))

    def with_number_of_retries(self, number_of_retries):
        self.number_of_retries = number_of_retries
        return self

    def with_items(self, *items):
        self.items.extend(items)
        return self


class ExportToS3Settings(s_impl.BaseRequestSettings):
    def __init__(self):
        super(ExportToS3Settings, self).__init__()
        self.items = []
        self.bucket = None
        self.endpoint = None
        self.scheme = 2
        self.uid = None
        self.access_key = None
        self.secret_key = None
        self.number_of_retries = 0
        self.storage_class = None
        self.export_compression = None

    def with_scheme(self, scheme):
        self.scheme = scheme
        return self

    def with_storage_class(self, storage_class):
        self.storage_class = storage_class
        return self

    def with_export_compression(self, compression):
        self.export_compression = compression
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


def _export_to_yt_request_factory(settings):
    request = ydb_export_pb2.ExportToYtRequest(
        settings=ydb_export_pb2.ExportToYtSettings(
            host=settings.host, token=settings.token
        )
    )

    if settings.number_of_retries > 0:
        request.settings.number_of_retries = settings.number_of_retries

    if settings.port:
        request.settings.port = settings.port

    for source_path, destination_path in settings.items:
        request.settings.items.add(
            source_path=source_path, destination_path=destination_path
        )

    return request


def _get_operation_request(operation_id):
    request = _apis.ydb_operation.GetOperationRequest(id=operation_id)
    return request


def _export_to_s3_request_factory(settings):
    request = ydb_export_pb2.ExportToS3Request(
        settings=ydb_export_pb2.ExportToS3Settings(
            endpoint=settings.endpoint,
            bucket=settings.bucket,
            access_key=settings.access_key,
            secret_key=settings.secret_key,
            scheme=settings.scheme,
            storage_class=settings.storage_class,
        )
    )

    if settings.uid is not None:
        request.operation_params.labels["uid"] = settings.uid

    if settings.number_of_retries > 0:
        request.settings.number_of_retries = settings.number_of_retries

    if settings.export_compression is not None:
        request.settings.compression = settings.export_compression

    for source_path, destination_prefix in settings.items:
        request.settings.items.add(
            source_path=source_path,
            destination_prefix=destination_prefix,
        )

    return request


class ExportClient(object):
    def __init__(self, driver):
        self._driver = driver

    def get_export_to_s3_operation(self, operation_id, settings=None):
        return self._driver(
            _get_operation_request(operation_id),
            _apis.OperationService.Stub,
            _apis.OperationService.GetOperation,
            ExportToS3Operation,
            settings,
            (self._driver,),
        )

    def export_to_s3(self, settings):
        return self._driver(
            _export_to_s3_request_factory(settings),
            ydb_export_v1_pb2_grpc.ExportServiceStub,
            _ExportToS3,
            ExportToS3Operation,
            settings,
            (self._driver,),
        )

    def export_to_yt(self, settings):
        return self._driver(
            _export_to_yt_request_factory(settings),
            ydb_export_v1_pb2_grpc.ExportServiceStub,
            _ExportToYt,
            ExportToYTOperation,
            settings,
            (self._driver,),
        )

    def async_export_to_yt(self, settings):
        return self._driver.future(
            _export_to_yt_request_factory(settings),
            ydb_export_v1_pb2_grpc.ExportServiceStub,
            _ExportToYt,
            ExportToYTOperation,
            settings,
            (self._driver,),
        )
