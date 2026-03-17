import logging
from datetime import datetime
from typing import Any, Dict, Optional, Union

import tzlocal

from pyhanko.pdf_utils import generic, misc

from . import model

__all__ = ['update_info_dict', 'view_from_info_dict']

logger = logging.getLogger(__name__)


def _write_meta_string(
    dictionary: generic.DictionaryObject,
    key: str,
    meta_str: model.MetaString,
    existing_only: bool,
) -> bool:
    if isinstance(meta_str, misc.StringWithLanguage):
        # don't bother with embedding language codes in strings,
        # too few people implement escapes anyway, so meh
        string = meta_str.value
    elif isinstance(meta_str, str):
        string = meta_str
    else:
        return False

    pdf_str = generic.TextStringObject(string)
    try:
        old_value = dictionary[key]
        mod = old_value != pdf_str
    except KeyError:
        mod = not existing_only
    if mod:
        dictionary[key] = pdf_str
    return mod


def _write_meta_date(
    dictionary: generic.DictionaryObject,
    key: str,
    meta_date: Union[datetime, str, None],
    existing_only: bool,
) -> bool:
    if isinstance(meta_date, datetime):
        value = meta_date
    elif meta_date == 'now':
        value = datetime.now(tz=tzlocal.get_localzone())
    else:
        return False

    if not existing_only or key in dictionary:
        dictionary[key] = generic.pdf_date(value)
        return True
    else:
        return False


def update_info_dict(
    meta: model.DocumentMetadata,
    info: generic.DictionaryObject,
    only_update_existing: bool = False,
) -> bool:
    mod = _write_meta_date(
        info, "/ModDate", meta.last_modified, existing_only=only_update_existing
    )
    producer = model.VENDOR
    try:
        producer_string = info['/Producer']
        if producer not in producer_string:
            producer_string = generic.TextStringObject(
                f"{producer_string}; {producer}"
            )
            mod = True
    except (KeyError, TypeError):
        producer_string = generic.TextStringObject(producer)
        mod = True
    # always override this
    info['/Producer'] = producer_string

    if meta.xmp_unmanaged:
        return mod

    mod |= _write_meta_string(
        info, "/Title", meta.title, existing_only=only_update_existing
    )
    mod |= _write_meta_string(
        info, "/Author", meta.author, existing_only=only_update_existing
    )
    mod |= _write_meta_string(
        info, "/Subject", meta.subject, existing_only=only_update_existing
    )
    mod |= _write_meta_string(
        info, "/Creator", meta.creator, existing_only=only_update_existing
    )
    mod |= _write_meta_date(
        info, "/CreationDate", meta.created, existing_only=only_update_existing
    )

    if meta.keywords:
        info['/Keywords'] = generic.TextStringObject(','.join(meta.keywords))
        mod = True

    return mod


def _read_date_from_dict(
    info_dict: generic.DictionaryObject, key: str, strict: bool
) -> Optional[datetime]:
    try:
        date_str = info_dict[key]
    except KeyError:
        return None

    try:
        if isinstance(date_str, generic.TextStringObject):
            return generic.parse_pdf_date(date_str, strict=strict)
    except misc.PdfReadError:
        pass

    logger.warning(
        "Key %s in info dict has value %s, which is not a valid date string",
        key,
        repr(date_str),
    )
    return None


def view_from_info_dict(
    info_dict: generic.DictionaryObject, strict: bool = True
) -> model.DocumentMetadata:
    kwargs: Dict[str, Any] = {}
    for s_entry in ('title', 'author', 'subject', 'creator'):
        try:
            kwargs[s_entry] = str(info_dict[f"/{s_entry.title()}"])
        except KeyError:
            pass

    creation_date = _read_date_from_dict(
        info_dict, '/CreationDate', strict=strict
    )
    if creation_date is not None:
        kwargs['created'] = creation_date

    mod_date = _read_date_from_dict(info_dict, '/ModDate', strict=strict)
    if mod_date is not None:
        kwargs['last_modified'] = mod_date

    if '/Keywords' in info_dict:
        kwargs['keywords'] = str(info_dict['/Keywords']).split(',')

    return model.DocumentMetadata(**kwargs)
