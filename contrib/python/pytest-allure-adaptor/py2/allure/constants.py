'''
Various constants extracted from schema.

Created on Oct 15, 2013

@author: pupssman
'''
from enum import Enum


class Status(object):
    FAILED = 'failed'
    BROKEN = 'broken'
    PASSED = 'passed'
    CANCELED = 'canceled'
    PENDING = 'pending'


class Label(object):
    DEFAULT = 'allure_label'
    FEATURE = 'feature'
    STORY = 'story'
    SEVERITY = 'severity'
    ISSUE = 'issue'
    TESTCASE = 'testId'
    THREAD = 'thread'
    HOST = 'host'
    FRAMEWORK = 'framework'
    LANGUAGE = 'language'


class Severity(object):
    BLOCKER = 'blocker'
    CRITICAL = 'critical'
    NORMAL = 'normal'
    MINOR = 'minor'
    TRIVIAL = 'trivial'


class AttachmentType(Enum):

    def __init__(self, mime_type, extension):
        self.mime_type = mime_type
        self.extension = extension

    TEXT = ("text/plain", "txt")
    HTML = ("application/html", "html")
    XML = ("application/xml", "xml")
    PNG = ("image/png", "png")
    JPG = ("image/jpg", "jpg")
    JSON = ("application/json", "json")
    OTHER = ("other", "other")


ALLURE_NAMESPACE = "urn:model.allure.qatools.yandex.ru"
COMMON_NAMESPACE = "urn:model.commons.qatools.yandex.ru"
FAILED_STATUSES = [Status.FAILED, Status.BROKEN]
SKIPPED_STATUSES = [Status.CANCELED, Status.PENDING]
