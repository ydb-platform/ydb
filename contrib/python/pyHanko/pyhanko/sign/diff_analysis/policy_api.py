from dataclasses import dataclass
from enum import unique
from typing import Optional, Set, Union

from pyhanko.pdf_utils.misc import OrderedEnum
from pyhanko.pdf_utils.reader import HistoricalResolver, PdfFileReader
from pyhanko.sign.fields import FieldMDPSpec, MDPPerm

__all__ = [
    'ModificationLevel',
    'SuspiciousModification',
    'DiffResult',
    'DiffPolicy',
]


@unique
class ModificationLevel(OrderedEnum):
    """
    Records the (semantic) modification level of a document.

    Compare :class:`~.pyhanko.sign.fields.MDPPerm`, which records the document
    modification policy associated with a particular signature, as opposed
    to the empirical judgment indicated by this enum.
    """

    NONE = 0
    """
    The document was not modified at all (i.e. it is byte-for-byte unchanged).
    """

    LTA_UPDATES = 1
    """
    The only updates are of the type that would be allowed as part of
    signature long term archival (LTA) processing.
    That is to say, updates to the document security store or new document
    time stamps. For the purposes of evaluating whether a document has been
    modified in the sense defined in the PAdES and ISO 32000-2 standards,
    these updates do not count.
    Adding form fields is permissible at this level, but only if they are
    signature fields. This is necessary for proper document timestamp support.
    """

    FORM_FILLING = 2
    """
    The only updates are extra signatures and updates to form field values or
    their appearance streams, in addition to the previous levels.
    """

    ANNOTATIONS = 3
    """
    In addition to the previous levels, manipulating annotations is also allowed
    at this level.

    .. note::
        This level is currently unused by the default diff policy, and
        modifications to annotations other than those permitted to fill in forms
        are treated as suspicious.
    """

    OTHER = 4
    """
    The document has been modified in ways that aren't on the validator's
    whitelist. This always invalidates the corresponding signature, irrespective
    of cryptographical integrity or ``/DocMDP`` settings.
    """


class SuspiciousModification(ValueError):
    """Error indicating a suspicious modification"""

    pass


@dataclass(frozen=True)
class DiffResult:
    """
    Encodes the result of a difference analysis on two revisions.

    Returned by :meth:`.DiffPolicy.apply`.
    """

    modification_level: ModificationLevel
    """
    The strictest modification level at which all changes pass muster.
    """

    changed_form_fields: Set[str]
    """
    Set containing the names of all changed form fields.

    .. note::
        For the purposes of this parameter, a change is defined as any
        :class:`.FormUpdate` where :attr:`.FormUpdate.valid_when_locked`
        is ``False``.
    """


class DiffPolicy:
    """
    Analyse the differences between two revisions.
    """

    def apply(
        self,
        old: HistoricalResolver,
        new: HistoricalResolver,
        field_mdp_spec: Optional[FieldMDPSpec] = None,
        doc_mdp: Optional[MDPPerm] = None,
    ) -> DiffResult:
        """
        Execute the policy on a pair of revisions, with the MDP values provided.
        :class:`.SuspiciousModification` exceptions should be propagated.

        :param old:
            The older, base revision.
        :param new:
            The newer revision.
        :param field_mdp_spec:
            The field MDP spec that's currently active.
        :param doc_mdp:
            The DocMDP spec that's currently active.
        :return:
            A :class:`.DiffResult` object summarising the policy's judgment.
        """
        raise NotImplementedError

    def review_file(
        self,
        reader: PdfFileReader,
        base_revision: Union[int, HistoricalResolver],
        field_mdp_spec: Optional[FieldMDPSpec] = None,
        doc_mdp: Optional[MDPPerm] = None,
    ) -> Union[DiffResult, SuspiciousModification]:
        """
        Compare the current state of a file to an earlier version,
        with the MDP values provided.
        :class:`.SuspiciousModification` exceptions should be propagated.

        If there are multiple revisions between the base revision and the
        current one, the precise manner in which the review is conducted
        is left up to the implementing class. In particular,
        subclasses may choose to review each intermediate revision individually,
        or handle them all at once.

        :param reader:
            PDF reader representing the current state of the file.
        :param base_revision:
            The older, base revision. You can choose between providing it as a
            revision index, or a :class:`.HistoricalResolver` instance.
        :param field_mdp_spec:
            The field MDP spec that's currently active.
        :param doc_mdp:
            The DocMDP spec that's currently active.
        :return:
            A :class:`.DiffResult` object summarising the policy's judgment.
        """
        raise NotImplementedError
