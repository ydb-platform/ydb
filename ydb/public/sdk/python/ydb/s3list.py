# -*- coding: utf-8 -*-
from ydb.public.api.grpc.draft import ydb_s3_internal_v1_pb2_grpc
from ydb.public.api.protos import ydb_s3_internal_pb2
from . import convert, issues, types


_S3Listing = "S3Listing"


def _prepare_tuple_typed_value(vl):
    if vl is None:
        return None

    tpb = types.TupleType()
    for _ in vl:
        tpb.add_element(types.OptionalType(types.PrimitiveType.Utf8))

    return convert.to_typed_value_from_native(tpb.proto, vl)


def _s3_listing_request_factory(
    table_name,
    key_prefix,
    path_column_prefix,
    path_column_delimiter,
    max_keys,
    columns_to_return=None,
    start_after_key_suffix=None,
):
    columns_to_return = [] if columns_to_return is None else columns_to_return
    start_after_key_suffix = (
        [] if start_after_key_suffix is None else start_after_key_suffix
    )
    return ydb_s3_internal_pb2.S3ListingRequest(
        table_name=table_name,
        key_prefix=_prepare_tuple_typed_value(key_prefix),
        path_column_prefix=path_column_prefix,
        path_column_delimiter=path_column_delimiter,
        max_keys=max_keys + 1,
        columns_to_return=columns_to_return,
        start_after_key_suffix=_prepare_tuple_typed_value(start_after_key_suffix),
    )


class S3ListingResult(object):
    __slots__ = (
        "is_truncated",
        "continue_after",
        "path_column_delimiter",
        "common_prefixes",
        "contents",
    )

    def __init__(
        self,
        is_truncated,
        continue_after,
        path_column_delimiter,
        common_prefixes,
        contents,
    ):
        self.is_truncated = is_truncated
        self.continue_after = continue_after
        self.path_column_delimiter = path_column_delimiter
        self.common_prefixes = common_prefixes
        self.contents = contents


def _wrap_s3_listing_response(rpc_state, response, path_column_delimiter, max_keys):
    issues._process_response(response.operation)
    message = ydb_s3_internal_pb2.S3ListingResult()
    response.operation.result.Unpack(message)
    common_prefixes_rs = convert.ResultSet.from_message(message.common_prefixes)
    common_prefixes = [row[0] for row in common_prefixes_rs.rows]
    contents = convert.ResultSet.from_message(message.contents).rows
    key_suffix_size = max(1, message.key_suffix_size)

    continue_after = None
    is_truncated = False
    if len(contents) + len(common_prefixes) > max_keys:
        is_truncated = True

        if len(contents) > 0 and len(common_prefixes) > 0:
            if contents[-1][0] < common_prefixes[-1]:
                common_prefixes.pop()
            else:
                contents.pop()
        elif len(contents) > 0:
            contents.pop()
        elif len(common_prefixes) > 0:
            common_prefixes.pop()

        use_contents_for_continue_after = True
        if len(contents) > 0 and len(common_prefixes) > 0:
            use_contents_for_continue_after = contents[-1][0] > common_prefixes[-1]
        elif len(common_prefixes) > 0:
            use_contents_for_continue_after = False

        if use_contents_for_continue_after:
            continue_after = contents[-1][:key_suffix_size]
        else:
            continue_after = [common_prefixes[-1]]

    return S3ListingResult(
        is_truncated=is_truncated,
        continue_after=continue_after,
        path_column_delimiter=path_column_delimiter,
        common_prefixes=common_prefixes,
        contents=contents,
    )


class S3InternalClient(object):
    def __init__(self, driver):
        self._driver = driver

    @staticmethod
    def _handle(
        callee,
        table_name,
        key_prefix,
        path_column_prefix,
        path_column_delimiter,
        max_keys,
        columns_to_return=None,
        start_after_key_suffix=None,
        settings=None,
    ):
        return callee(
            _s3_listing_request_factory(
                table_name,
                key_prefix,
                path_column_prefix,
                path_column_delimiter,
                max_keys,
                columns_to_return,
                start_after_key_suffix,
            ),
            ydb_s3_internal_v1_pb2_grpc.S3InternalServiceStub,
            _S3Listing,
            _wrap_s3_listing_response,
            settings,
            (
                path_column_delimiter,
                max_keys,
            ),
        )

    def s3_listing(
        self,
        table_name,
        key_prefix,
        path_column_prefix,
        path_column_delimiter,
        max_keys,
        columns_to_return=None,
        start_after_key_suffix=None,
        settings=None,
    ):
        return self._handle(
            self._driver,
            table_name,
            key_prefix,
            path_column_prefix,
            path_column_delimiter,
            max_keys,
            columns_to_return,
            start_after_key_suffix,
            settings,
        )

    def async_s3_listing(
        self,
        table_name,
        key_prefix,
        path_column_prefix,
        path_column_delimiter,
        max_keys,
        columns_to_return=None,
        start_after_key_suffix=None,
        settings=None,
    ):
        return self._handle(
            self._driver.future,
            table_name,
            key_prefix,
            path_column_prefix,
            path_column_delimiter,
            max_keys,
            columns_to_return,
            start_after_key_suffix,
            settings,
        )
