from dataclasses import dataclass
import re

from .common import LocalizedString, DEFAULT_LANG
from .functions import TaskFunction

task_id_pattern = re.compile('[a-z0-9_-]+')


@dataclass
class TaskSpec:
    id: str
    function: TaskFunction
    name: LocalizedString
    description: LocalizedString
    instruction: LocalizedString

    def __post_init__(self):
        if not task_id_pattern.fullmatch(self.id):
            raise ValueError(f'id does not match pattern {task_id_pattern.pattern}')
        LocalizedString.convert_dataclass(self)
        assert DEFAULT_LANG in self.name.lang_to_text, f'please provide name for {DEFAULT_LANG} language'
