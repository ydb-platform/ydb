import typing
import logging
from storage import Storage
from models import (
    ReservationCreateResponse,
    ReservationCreateRequest,
    ReservationCancelRequest,
    ReservationCancelResponse,
)


class Controller(object):

    __slots__ = ("_storage",)

    def __init__(self, storage: Storage):
        self._storage = storage

    def _find_available_table_id(
        self, request: ReservationCreateRequest
    ) -> typing.Optional[int]:
        table_ids = set(self._storage.list_table_ids(cnt=request.cnt))
        reserved_table_ids = set(
            self._storage.find_reserved_table_ids(cnt=request.cnt, dt=request.dt)
        )
        for table_id in table_ids.difference(reserved_table_ids):
            return table_id

    def maybe_create_reservation(
        self, request: ReservationCreateRequest
    ) -> ReservationCreateResponse:
        table_id = self._find_available_table_id(request)
        if table_id is None:
            logging.warning("reservation failed")
            return ReservationCreateResponse(success=False)
        try:
            self._storage.save_reservation(
                dt=request.dt,
                table_id=table_id,
                cnt=request.cnt,
                description=request.description,
                phone=request.phone,
            )
            logging.warning(f"reservation {request.phone} {request.dt} succeeded")
            return ReservationCreateResponse(success=True, table_id=table_id)
        except Exception as e:
            logging.warning(f"failed to reserve a table due to {repr(e)}")
            return ReservationCreateResponse(success=False)

    def maybe_cancel_reservation(
        self, request: ReservationCancelRequest
    ) -> ReservationCancelResponse:
        try:
            self._storage.delete_reservation(phone=request.phone, dt=request.dt)
            logging.warning(f"reservation {request.phone} {request.dt} cancelled")
            return ReservationCancelResponse(success=True)
        except Exception as e:
            logging.warning(
                f"failed to cancel reservation for {request.phone} "
                f"{request.dt} due to {repr(e)}"
            )
            return ReservationCancelResponse(success=False)
