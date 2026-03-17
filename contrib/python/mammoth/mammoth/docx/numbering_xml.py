import cobble

from ..documents import numbering_level
from .styles_xml import Styles


def read_numbering_xml_element(element, styles):
    abstract_nums = _read_abstract_nums(element)
    nums = _read_nums(element)
    return Numbering(abstract_nums=abstract_nums, nums=nums, styles=styles)


def _read_abstract_nums(element):
    abstract_num_elements = element.find_children("w:abstractNum")
    return dict(map(_read_abstract_num, abstract_num_elements))


def _read_abstract_num(element):
    abstract_num_id = element.attributes.get("w:abstractNumId")
    levels = _read_abstract_num_levels(element)
    num_style_link = element.find_child_or_null("w:numStyleLink").attributes.get("w:val")
    return abstract_num_id, _AbstractNum(levels=levels, num_style_link=num_style_link)


@cobble.data
class _AbstractNum(object):
    levels = cobble.field()
    num_style_link = cobble.field()


@cobble.data
class _AbstractNumLevel(object):
    level_index = cobble.field()
    is_ordered = cobble.field()
    paragraph_style_id = cobble.field()


def _read_abstract_num_levels(element):
    levels = {}

    # Some malformed documents define numbering levels without an index, and
    # reference the numbering using a w:numPr element without a w:ilvl child.
    # To handle such cases, we assume a level of 0 as a fallback.
    level_without_index = None

    for level_element in element.find_children("w:lvl"):
        level = _read_abstract_num_level(level_element)
        if level.level_index is None:
            level.level_index = "0"
            level_without_index = level
        else:
            levels[level.level_index] = level

    if level_without_index is not None and level_without_index.level_index not in levels:
        levels[level_without_index.level_index] = level_without_index

    return levels


def _read_abstract_num_level(element):
    level_index = element.attributes.get("w:ilvl")
    num_fmt = element.find_child_or_null("w:numFmt").attributes.get("w:val")
    is_ordered = num_fmt != "bullet"
    paragraph_style_id = element.find_child_or_null("w:pStyle").attributes.get("w:val")
    return _AbstractNumLevel(
        level_index=level_index,
        is_ordered=is_ordered,
        paragraph_style_id=paragraph_style_id,
    )


def _read_nums(element):
    num_elements = element.find_children("w:num")
    return dict(
        _read_num(num_element)
        for num_element in num_elements
    )


def _read_num(element):
    num_id = element.attributes.get("w:numId")
    abstract_num_id = element.find_child_or_null("w:abstractNumId").attributes["w:val"]
    return num_id, _Num(abstract_num_id=abstract_num_id)


@cobble.data
class _Num(object):
    abstract_num_id = cobble.field()


class Numbering(object):
    def __init__(self, abstract_nums, nums, styles):
        self._abstract_nums = abstract_nums
        self._levels_by_paragraph_style_id = dict(
            (level.paragraph_style_id, self._to_numbering_level(level))
            for abstract_num in abstract_nums.values()
            for level in abstract_num.levels.values()
            if level.paragraph_style_id is not None
        )
        self._nums = nums
        self._styles = styles

    def find_level(self, num_id, level):
        num = self._nums.get(num_id)
        if num is None:
            return None
        else:
            abstract_num = self._abstract_nums.get(num.abstract_num_id)
            if abstract_num is None:
                return None
            elif abstract_num.num_style_link is None:
                return self._to_numbering_level(abstract_num.levels.get(level))
            else:
                style = self._styles.find_numbering_style_by_id(abstract_num.num_style_link)
                return self.find_level(style.num_id, level)

    def find_level_by_paragraph_style_id(self, style_id):
        return self._levels_by_paragraph_style_id.get(style_id)

    def _to_numbering_level(self, abstract_num_level):
        if abstract_num_level is None:
            return None
        else:
            return numbering_level(
                level_index=abstract_num_level.level_index,
                is_ordered=abstract_num_level.is_ordered,
            )


Numbering.EMPTY = Numbering(abstract_nums={}, nums={}, styles=Styles.EMPTY)
