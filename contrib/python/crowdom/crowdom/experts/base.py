import enum

from .. import base


class ExpertCase(enum.Enum):
    TASK_VERIFICATION = 'task-verification'
    LABELING_QUALITY_VERIFICATION = 'labeling-quality-verification'

    @property
    def label(self) -> base.LocalizedString:
        return base.LocalizedString(
            {
                self.TASK_VERIFICATION: {
                    'EN': 'Task verification',
                    'RU': 'Проверка заданий',
                },
                self.LABELING_QUALITY_VERIFICATION: {
                    'EN': 'Labeling quality verification',
                    'RU': 'Проверка качества разметки',
                },
            }[self]
        )
