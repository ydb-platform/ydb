from collections import OrderedDict
from contextlib import contextmanager
from allure_commons._core import plugin_manager
from allure_commons.model2 import TestResultContainer
from allure_commons.model2 import TestResult
from allure_commons.model2 import Attachment, ATTACHMENT_PATTERN
from allure_commons.model2 import TestStepResult
from allure_commons.model2 import ExecutableItem
from allure_commons.model2 import TestBeforeResult
from allure_commons.model2 import TestAfterResult
from allure_commons.utils import uuid4
from allure_commons.utils import now
from allure_commons.types import AttachmentType


class AllureLifecycle:
    def __init__(self):
        self._items = OrderedDict()

    def _get_item(self, uuid=None, item_type=None):
        uuid = uuid or self._last_item_uuid(item_type=item_type)
        return self._items.get(uuid)

    def _pop_item(self, uuid=None, item_type=None):
        uuid = uuid or self._last_item_uuid(item_type=item_type)
        return self._items.pop(uuid, None)

    def _last_item_uuid(self, item_type=None):
        for uuid in reversed(self._items):
            item = self._items.get(uuid)
            if item_type is None:
                return uuid
            elif isinstance(item, item_type):
                return uuid

    @contextmanager
    def schedule_test_case(self, uuid=None):
        test_result = TestResult()
        test_result.uuid = uuid or uuid4()
        self._items[test_result.uuid] = test_result
        yield test_result

    @contextmanager
    def update_test_case(self, uuid=None):
        yield self._get_item(uuid=uuid, item_type=TestResult)

    def write_test_case(self, uuid=None):
        test_result = self._pop_item(uuid=uuid, item_type=TestResult)
        if test_result:
            plugin_manager.hook.report_result(result=test_result)

    @contextmanager
    def start_step(self, parent_uuid=None, uuid=None):
        parent = self._get_item(uuid=parent_uuid, item_type=ExecutableItem)
        step = TestStepResult()
        step.start = now()
        parent.steps.append(step)
        self._items[uuid or uuid4()] = step
        yield step

    @contextmanager
    def update_step(self, uuid=None):
        yield self._get_item(uuid=uuid, item_type=TestStepResult)

    def stop_step(self, uuid=None):
        step = self._pop_item(uuid=uuid, item_type=TestStepResult)
        if step and not step.stop:
            step.stop = now()

    @contextmanager
    def start_container(self, uuid=None):
        container = TestResultContainer(uuid=uuid or uuid4())
        self._items[container.uuid] = container
        yield container

    def containers(self):
        for item in self._items.values():
            if isinstance(item, TestResultContainer):
                yield item

    @contextmanager
    def update_container(self, uuid=None):
        yield self._get_item(uuid=uuid, item_type=TestResultContainer)

    def write_container(self, uuid=None):
        container = self._pop_item(uuid=uuid, item_type=TestResultContainer)
        if container and (container.befores or container.afters):
            plugin_manager.hook.report_container(container=container)

    @contextmanager
    def start_before_fixture(self, parent_uuid=None, uuid=None):
        fixture = TestBeforeResult()
        parent = self._get_item(uuid=parent_uuid, item_type=TestResultContainer)
        if parent:
            parent.befores.append(fixture)
        self._items[uuid or uuid4()] = fixture
        yield fixture

    @contextmanager
    def update_before_fixture(self, uuid=None):
        yield self._get_item(uuid=uuid, item_type=TestBeforeResult)

    def stop_before_fixture(self, uuid=None):
        fixture = self._pop_item(uuid=uuid, item_type=TestBeforeResult)
        if fixture and not fixture.stop:
            fixture.stop = now()

    @contextmanager
    def start_after_fixture(self, parent_uuid=None, uuid=None):
        fixture = TestAfterResult()
        parent = self._get_item(uuid=parent_uuid, item_type=TestResultContainer)
        if parent:
            parent.afters.append(fixture)
        self._items[uuid or uuid4()] = fixture
        yield fixture

    @contextmanager
    def update_after_fixture(self, uuid=None):
        yield self._get_item(uuid=uuid, item_type=TestAfterResult)

    def stop_after_fixture(self, uuid=None):
        fixture = self._pop_item(uuid=uuid, item_type=TestAfterResult)
        if fixture and not fixture.stop:
            fixture.stop = now()

    def _attach(self, uuid, name=None, attachment_type=None, extension=None, parent_uuid=None):
        mime_type = attachment_type
        extension = extension if extension else 'attach'

        if type(attachment_type) is AttachmentType:
            extension = attachment_type.extension
            mime_type = attachment_type.mime_type

        file_name = ATTACHMENT_PATTERN.format(prefix=uuid, ext=extension)
        attachment = Attachment(source=file_name, name=name, type=mime_type)
        last_uuid = parent_uuid if parent_uuid else self._last_item_uuid(ExecutableItem)
        self._items[last_uuid].attachments.append(attachment)

        return file_name

    def attach_file(self, uuid, source, name=None, attachment_type=None, extension=None, parent_uuid=None):
        file_name = self._attach(uuid, name=name, attachment_type=attachment_type,
                                 extension=extension, parent_uuid=parent_uuid)
        plugin_manager.hook.report_attached_file(source=source, file_name=file_name)

    def attach_data(self, uuid, body, name=None, attachment_type=None, extension=None, parent_uuid=None):
        file_name = self._attach(uuid, name=name, attachment_type=attachment_type,
                                 extension=extension, parent_uuid=parent_uuid)
        plugin_manager.hook.report_attached_data(body=body, file_name=file_name)
