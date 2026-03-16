import xml.etree.ElementTree as ET
from ..datetime_helpers import parse_datetime


class JobItem(object):
    def __init__(self, id_, job_type, progress, created_at, started_at=None, completed_at=None, finish_code=0):
        self._id = id_
        self._type = job_type
        self._progress = progress
        self._created_at = created_at
        self._started_at = started_at
        self._completed_at = completed_at
        self._finish_code = finish_code

    @property
    def id(self):
        return self._id

    @property
    def type(self):
        return self._type

    @property
    def progress(self):
        return self._progress

    @property
    def created_at(self):
        return self._created_at

    @property
    def started_at(self):
        return self._started_at

    @property
    def completed_at(self):
        return self._completed_at

    @property
    def finish_code(self):
        return self._finish_code

    def __repr__(self):
        return "<Job#{_id} {_type} created_at({_created_at}) started_at({_started_at}) completed_at({_completed_at})" \
               " progress ({_progress}) finish_code({_finish_code})>".format(**self.__dict__)

    @classmethod
    def from_response(cls, xml, ns):
        parsed_response = ET.fromstring(xml)
        all_tasks_xml = parsed_response.findall(
            './/t:job', namespaces=ns)

        all_tasks = [JobItem._parse_element(x, ns) for x in all_tasks_xml]

        return all_tasks

    @classmethod
    def _parse_element(cls, element, ns):
        id_ = element.get('id', None)
        type_ = element.get('type', None)
        progress = element.get('progress', None)
        created_at = parse_datetime(element.get('createdAt', None))
        started_at = parse_datetime(element.get('startedAt', None))
        completed_at = parse_datetime(element.get('completedAt', None))
        finish_code = element.get('finishCode', -1)
        return cls(id_, type_, progress, created_at, started_at, completed_at, finish_code)


class BackgroundJobItem(object):
    class Status:
        Pending = "Pending"
        InProgress = "InProgress"
        Success = "Success"
        Failed = "Failed"
        Cancelled = "Cancelled"

    def __init__(self, id_, created_at, priority, job_type, status, title=None, subtitle=None, started_at=None,
                 ended_at=None):
        self._id = id_
        self._type = job_type
        self._status = status
        self._created_at = created_at
        self._started_at = started_at
        self._ended_at = ended_at
        self._priority = priority
        self._title = title
        self._subtitle = subtitle

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        """For API consistency - all other resource endpoints have a name attribute which is used to display what
        they are.  Alias title as name to allow consistent handling of resources in the list sample."""
        return self._title

    @property
    def status(self):
        return self._status

    @property
    def type(self):
        return self._type

    @property
    def created_at(self):
        return self._created_at

    @property
    def started_at(self):
        return self._started_at

    @property
    def ended_at(self):
        return self._ended_at

    @property
    def title(self):
        return self._title

    @property
    def subtitle(self):
        return self._subtitle

    @property
    def priority(self):
        return self._priority

    @classmethod
    def from_response(cls, xml, ns):
        parsed_response = ET.fromstring(xml)
        all_tasks_xml = parsed_response.findall(
            './/t:backgroundJob', namespaces=ns)
        return [cls._parse_element(x, ns) for x in all_tasks_xml]

    @classmethod
    def _parse_element(cls, element, ns):
        id_ = element.get('id', None)
        type_ = element.get('jobType', None)
        status = element.get('status', None)
        created_at = parse_datetime(element.get('createdAt', None))
        started_at = parse_datetime(element.get('startedAt', None))
        ended_at = parse_datetime(element.get('endedAt', None))
        priority = element.get('priority', None)
        title = element.get('title', None)
        subtitle = element.get('subtitle', None)

        return cls(id_, created_at, priority, type_, status, title, subtitle, started_at, ended_at)
