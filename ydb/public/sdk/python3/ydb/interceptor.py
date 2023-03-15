# -*- coding: utf-8 -*-
import grpc
from concurrent import futures
from grpc._cython import cygrpc
from grpc._channel import _handle_event, _EMPTY_FLAGS


def _event_handler(state, response_deserializer):
    def handle_event(event):
        with state.condition:
            callbacks = _handle_event(event, state, response_deserializer)
            state.condition.notify_all()
            done = not state.due
        for callback in callbacks:
            callback()

        if getattr(state, "on_event_handler_callback", None) is not None:
            state.on_event_handler_callback(state)

        return done and state.fork_epoch >= cygrpc.get_fork_epoch()

    return handle_event


def on_event_callback(future, it, response_wrapper):
    def _callback(state):
        with state.condition:
            if state.response is not None:
                response = state.response
                state.response = None
                if not future.done():
                    try:
                        future.set_result(response_wrapper(response))
                    except Exception as e:
                        future.set_exception(e)
            elif cygrpc.OperationType.receive_message not in state.due:
                if state.code is grpc.StatusCode.OK:
                    if not future.done():
                        future.set_exception(StopIteration())
                elif state.code is not None:
                    if not future.done():
                        future.set_exception(it)

    return _callback


def operate_async_stream_call(it, wrapper):
    future = futures.Future()
    callback = on_event_callback(future, it, wrapper)

    with it._state.condition:
        if it._state.code is None:
            it._state.on_event_handler_callback = callback
            operating = it._call.operate(
                (cygrpc.ReceiveMessageOperation(_EMPTY_FLAGS),),
                _event_handler(it._state, it._response_deserializer),
            )
            if operating:
                it._state.due.add(cygrpc.OperationType.receive_message)
        elif it._state.code is grpc.StatusCode.OK:
            future.set_exception(StopIteration())
        else:
            future.set_exception(it)
    return future


def monkey_patch_event_handler():
    grpc._channel._event_handler = _event_handler
