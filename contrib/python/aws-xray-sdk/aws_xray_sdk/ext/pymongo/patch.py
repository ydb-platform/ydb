# Copyright © 2018 Clarity Movement Co. All rights reserved.
from pymongo import monitoring
from aws_xray_sdk.core import xray_recorder


class XrayCommandListener(monitoring.CommandListener):
    """
    A listener that traces all pymongo db commands to AWS Xray.
    Creates a subsegment for each mongo db conmmand.

    name: 'mydb@127.0.0.1:27017'
    records all available information provided by pymongo,
    except for `command` and `reply`. They may contain business secrets.
    If you insist to record them, specify `record_full_documents=True`.
    """

    def __init__(self, record_full_documents):
        super().__init__()
        self.record_full_documents = record_full_documents

    def started(self, event):
        host, port = event.connection_id
        host_and_port_str = f'{host}:{port}'

        subsegment = xray_recorder.begin_subsegment(
            f'{event.database_name}@{host_and_port_str}', 'remote')
        subsegment.put_annotation('mongodb_command_name', event.command_name)
        subsegment.put_annotation('mongodb_connection_id', host_and_port_str)
        subsegment.put_annotation('mongodb_database_name', event.database_name)
        subsegment.put_annotation('mongodb_operation_id', event.operation_id)
        subsegment.put_annotation('mongodb_request_id', event.request_id)
        if self.record_full_documents:
            subsegment.put_metadata('mongodb_command', event.command)

    def succeeded(self, event):
        subsegment = xray_recorder.current_subsegment()
        subsegment.put_annotation('mongodb_duration_micros', event.duration_micros)
        if self.record_full_documents:
            subsegment.put_metadata('mongodb_reply', event.reply)
        xray_recorder.end_subsegment()

    def failed(self, event):
        subsegment = xray_recorder.current_subsegment()
        subsegment.add_fault_flag()
        subsegment.put_annotation('mongodb_duration_micros', event.duration_micros)
        subsegment.put_metadata('failure', event.failure)
        xray_recorder.end_subsegment()


def patch(record_full_documents=False):
    # ensure `patch()` is idempotent
    if hasattr(monitoring, '_xray_enabled'):
        return
    setattr(monitoring, '_xray_enabled', True)
    monitoring.register(XrayCommandListener(record_full_documents))
