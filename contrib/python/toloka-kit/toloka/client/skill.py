__all__ = ['Skill']
import datetime
from typing import Dict

from .owner import Owner
from .primitives.base import BaseTolokaObject
from ..util._codegen import attribute

LangIso639 = str


class Skill(BaseTolokaObject):
    """Some characteristic of a Toloker described by a number from 0 to 100.

    A `Skill` describes some characteristic, for example the percentage of correct responses.
    Skills can be used to filter Tolokers.

    Skill values are assigned to Tolokers by quality control rules, or manually. The values are accessed via the [UserSkill](toloka.client.user_skill.UserSkill.md) class.

    Learn more about the [Toloker skills](https://toloka.ai/docs/guide/nav/).

    Attributes:
        name: The skill name.
        public_name: The skill name visible to Tolokers. The name can be provided in several languages.
        public_requester_description: The skill description visible to Tolokers. The description can be provided in several languages.
        private_comment: Comments visible to a requester.
        hidden: Visibility of the skill values to Tolokers:
            * `True` — Tolokers don't see the information about the skill.
            * `False` — Tolokers see the name and the value of the assigned skill.

            Default value: `True`.
        skill_ttl_hours: A lifetime in hours. If the skill value assigned to a Toloker is not updated for `skill_ttl_hours`, the skill is removed from a Toloker's profile.
        training: Whether the skill is related to a training pool:
            * `True` — The skill value is set after completing a training pool.
            * `False` — The skill isn't related to a training pool.
        owner: The skill owner.
        id: The skill ID. Read-only field.
        created: The UTC date and time when the skill was created. Read-only field.

    Example:
        Creating a new skill if it doesn't exist.

        >>> segmentation_skill = next(toloka_client.get_skills(name='Road signs detection'), None)
        >>> if segmentation_skill:
        >>>     print(f'Skill exists. ID: {segmentation_skill.id}')
        >>> else:
        >>>     segmentation_skill = toloka_client.create_skill(
        >>>         name='Road signs detection',
        >>>         public_requester_description={
        >>>             'EN': 'The quality of selecting road signs on images',
        >>>             'RU': 'Качество выделения дорожных знаков на фотографиях',
        >>>         },
        >>>     )
        >>>     print(f'Skill created. ID: {segmentation_skill.id}')
        ...
    """

    name: str

    private_comment: str
    hidden: bool
    skill_ttl_hours: int
    training: bool

    public_name: Dict[LangIso639, str]
    public_requester_description: Dict[LangIso639, str]
    owner: Owner

    # Readonly
    id: str = attribute(readonly=True)
    created: datetime.datetime = attribute(readonly=True)
