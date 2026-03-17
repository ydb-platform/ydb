import binascii
import json

from celery import maybe_signature, states
from celery.backends.base import BaseDictBackend, get_current_task
from celery.exceptions import ChordError
from celery.result import GroupResult, allow_join_result, result_from_tuple
from celery.utils.log import get_logger
from celery.utils.serialization import b64decode, b64encode
from django.db import connection, router, transaction
from django.db.models.functions import Now
from django.db.utils import InterfaceError
from kombu.exceptions import DecodeError

from ..models import ChordCounter
from ..models import GroupResult as GroupResultModel
from ..models import TaskResult

EXCEPTIONS_TO_CATCH = (InterfaceError,)

try:
    from psycopg2 import InterfaceError as Psycopg2InterfaceError
    EXCEPTIONS_TO_CATCH += (Psycopg2InterfaceError,)
except ImportError:
    pass

logger = get_logger(__name__)


class DatabaseBackend(BaseDictBackend):
    """The Django database backend, using models to store task state."""

    TaskModel = TaskResult
    GroupModel = GroupResultModel
    subpolling_interval = 0.5

    def exception_safe_to_retry(self, exc):
        """Check if an exception is safe to retry.

        Backends have to overload this method with correct predicates
        dealing with their exceptions.

        By default no exception is safe to retry, it's up to
        backend implementation to define which exceptions are safe.

        For Celery / django-celery-results, retry Django / Psycopg2
        InterfaceErrors, like "Connection already closed", with new connection.

        Set result_backend_always_retry to True in order to enable retries.
        """
        for exc_type in EXCEPTIONS_TO_CATCH:
            if isinstance(exc, exc_type):
                # Only called if InterfaceError occurs and always_retry is True
                connection.close()
                return True
        return False

    def _get_extended_properties(self, request, traceback):
        extended_props = {
            'periodic_task_name': None,
            'task_args': None,
            'task_kwargs': None,
            'task_name': None,
            'traceback': None,
            'worker': None,
        }
        if request and self.app.conf.find_value_for_key('extended', 'result'):

            if getattr(request, 'argsrepr', None) is not None:
                # task protocol 2
                task_args = request.argsrepr
            else:
                # task protocol 1
                task_args = getattr(request, 'args', None)

            if getattr(request, 'kwargsrepr', None) is not None:
                # task protocol 2
                task_kwargs = request.kwargsrepr
            else:
                # task protocol 1
                task_kwargs = getattr(request, 'kwargs', None)

            # Encode input arguments
            if task_args is not None:
                _, _, task_args = self.encode_content(task_args)

            if task_kwargs is not None:
                _, _, task_kwargs = self.encode_content(task_kwargs)

            periodic_task_name = getattr(request, 'periodic_task_name', None)

            extended_props.update({
                'periodic_task_name': periodic_task_name,
                'task_args': task_args,
                'task_kwargs': task_kwargs,
                'task_name': getattr(request, 'task', None),
                'traceback': traceback,
                'worker': getattr(request, 'hostname', None),
            })

        return extended_props

    def _get_meta_from_request(self, request=None):
        """
        Use the request or get_current_task to evaluate the `meta` attribute.

        With this, is possible to assign arbitrary data in request.meta to be
        retrieve and stored on the TaskResult.
        """
        request = request or getattr(get_current_task(), "request", None)
        return getattr(request, "meta", {})

    def _store_result(
            self,
            task_id,
            result,
            status,
            traceback=None,
            request=None,
            using=None
    ):
        """Store return value and status of an executed task."""
        content_type, content_encoding, result = self.encode_content(result)

        meta = {
            **self._get_meta_from_request(request),
            "children": self.current_task_children(request),
        }
        _, _, encoded_meta = self.encode_content(
            meta,
        )

        task_props = {
            'content_encoding': content_encoding,
            'content_type': content_type,
            'meta': encoded_meta,
            'result': result,
            'status': status,
            'task_id': task_id,
            'traceback': traceback,
            'using': using,
        }

        task_props.update(
            self._get_extended_properties(request, traceback)
        )

        if status == states.STARTED:
            task_props['date_started'] = Now()

        self.TaskModel._default_manager.store_result(**task_props)
        return result

    def _get_task_meta_for(self, task_id):
        """Get task metadata for a task by id."""
        obj = self.TaskModel._default_manager.get_task(task_id)
        res = obj.as_dict()
        meta = self.decode_content(obj, res.pop('meta', None)) or {}
        result = self.decode_content(obj, res.get('result'))

        task_args = res.get('task_args')
        task_kwargs = res.get('task_kwargs')
        try:
            task_args = self.decode_content(obj, task_args)
            task_kwargs = self.decode_content(obj, task_kwargs)
        except (DecodeError, binascii.Error):
            pass

        # the right names are args/kwargs, not task_args/task_kwargs,
        # keep both for backward compatibility
        res.update(
            meta,
            result=result,
            task_args=task_args,
            task_kwargs=task_kwargs,
            args=task_args,
            kwargs=task_kwargs,
        )
        return self.meta_from_decoded(res)

    def encode_content(self, data):
        content_type, content_encoding, content = self._encode(data)
        if content_encoding == 'binary':
            content = b64encode(content)
        return content_type, content_encoding, content

    def decode_content(self, obj, content):
        if content:
            if obj.content_encoding == 'binary':
                content = b64decode(content)
            return self.decode(content)

    def _forget(self, task_id):
        try:
            self.TaskModel._default_manager.get(task_id=task_id).delete()
        except self.TaskModel.DoesNotExist:
            pass

    def cleanup(self):
        """Delete expired metadata."""
        if not self.expires:
            return
        self.TaskModel._default_manager.delete_expired(self.expires)
        self.GroupModel._default_manager.delete_expired(self.expires)

    def _restore_group(self, group_id):
        """return result value for a group by id."""
        group_result = self.GroupModel._default_manager.get_group(group_id)

        if group_result:
            res = group_result.as_dict()
            decoded_result = self.decode_content(group_result, res["result"])
            res["result"] = None
            if decoded_result:
                res["result"] = result_from_tuple(decoded_result, app=self.app)
            return res

    def _save_group(self, group_id, group_result):
        """Store return value of group"""
        content_type, content_encoding, result = self.encode_content(
            group_result.as_tuple()
        )
        self.GroupModel._default_manager.store_group_result(
            content_type, content_encoding, group_id, result
        )
        return group_result

    def _delete_group(self, group_id):
        try:
            self.GroupModel._default_manager.get_group(group_id).delete()
        except self.TaskModel.DoesNotExist:
            pass

    def apply_chord(self, header_result_args, body, **kwargs):
        """Add a ChordCounter with the expected number of results"""
        if not isinstance(header_result_args, GroupResult):
            # Celery 5.1 provides the GroupResult args
            header_result = self.app.GroupResult(*header_result_args)
        else:
            # celery <5.1 will pass a GroupResult object
            header_result = header_result_args
        results = [r.as_tuple() for r in header_result]
        chord_size = body.get("chord_size", None) or len(results)
        data = json.dumps(results)
        ChordCounter.objects.create(
            group_id=header_result.id, sub_tasks=data, count=chord_size
        )

    def on_chord_part_return(self, request, state, result, **kwargs):
        """Called on finishing each part of a Chord header"""
        tid, gid = request.id, request.group
        if not gid or not tid:
            return
        call_callback = False
        with transaction.atomic(using=router.db_for_write(ChordCounter)):
            # We need to know if `count` hits 0.
            # wrap the update in a transaction
            # with a `select_for_update` lock to prevent race conditions.
            # SELECT FOR UPDATE is not supported on all databases
            try:
                chord_counter = (
                    ChordCounter.objects.select_for_update()
                    .get(group_id=gid)
                )
            except ChordCounter.DoesNotExist:
                logger.warning("Can't find ChordCounter for Group %s", gid)
                return
            chord_counter.count -= 1
            if chord_counter.count != 0:
                chord_counter.save(update_fields=["count"])
            else:
                # Last task in the chord header has finished
                call_callback = True
                chord_counter.delete()

        if call_callback:
            deps = chord_counter.group_result(app=self.app)
            if deps.ready():
                callback = maybe_signature(request.chord, app=self.app)
                trigger_callback(
                    app=self.app,
                    callback=callback,
                    group_result=deps
                )


def trigger_callback(app, callback, group_result):
    """Add the callback to the queue or mark the callback as failed
    Implementation borrowed from `celery.app.builtins.unlock_chord`
    """
    if group_result.supports_native_join:
        j = group_result.join_native
    else:
        j = group_result.join

    try:
        with allow_join_result():
            ret = j(timeout=app.conf.result_chord_join_timeout, propagate=True)
    except Exception as exc:  # pylint: disable=broad-except
        try:
            culprit = next(group_result._failed_join_report())
            reason = f"Dependency {culprit.id} raised {exc!r}"
        except StopIteration:
            reason = repr(exc)
        logger.exception("Chord %r raised: %r", group_result.id, exc)
        app.backend.chord_error_from_stack(callback, ChordError(reason))
    else:
        try:
            callback.delay(ret)
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("Chord %r raised: %r", group_result.id, exc)
            app.backend.chord_error_from_stack(
                callback, exc=ChordError(f"Callback error: {exc!r}")
            )
