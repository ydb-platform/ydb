import re
from typing import Optional, Iterable, List, Dict, Tuple

from .imap_utf7 import utf7_decode
from .consts import MailBoxFolderStatusOptions
from .utils import check_command_status, pairs_to_dict, encode_folder, StrOrBytes
from .errors import MailboxFolderStatusValueError, MailboxFolderSelectError, MailboxFolderCreateError, \
    MailboxFolderRenameError, MailboxFolderDeleteError, MailboxFolderStatusError, MailboxFolderSubscribeError


class FolderInfo:
    """
    Mailbox folder info
        name: str - folder name
        delim: str - delimiter, a character used to delimit levels of hierarchy in a mailbox name
        flags: (str,) - folder flags
    A 'NIL' delimiter means that no hierarchy exists, the name is a "flat" name.
    """
    __slots__ = 'name', 'delim', 'flags'

    def __init__(self, name: str, delim: str, flags: Tuple[str, ...]) -> None:
        self.name = name
        self.delim = delim
        self.flags = flags

    def __repr__(self):
        return f"{self.__class__.__name__}(name={repr(self.name)}, delim={repr(self.delim)}, flags={repr(self.flags)})"

    def __eq__(self, other):
        return all(getattr(self, i) == getattr(other, i) for i in self.__slots__)


class MailBoxFolderManager:
    """Operations with mailbox folders"""

    def __init__(self, mailbox) -> None:
        self.mailbox = mailbox
        self._current_folder = None

    def set(self, folder: StrOrBytes, readonly: bool = False) -> tuple:
        """Select current folder"""
        result = self.mailbox.client.select(encode_folder(folder), readonly)
        check_command_status(result, MailboxFolderSelectError)
        self._current_folder = folder
        return result

    def exists(self, folder: str) -> bool:
        """Checks whether a folder exists on the server."""
        return len(self.list('', folder)) > 0

    def create(self, folder: StrOrBytes) -> tuple:
        """
        Create folder on the server.
        Use email box delimiter to separate folders. Example for "|" delimiter: "folder|sub folder"
        """
        result = self.mailbox.client._simple_command('CREATE', encode_folder(folder))
        check_command_status(result, MailboxFolderCreateError)
        return result

    def get(self) -> Optional[str]:
        """
        Get current folder
        :return:
            None - if folder is not selected
            str - if folder is selected
        """
        return self._current_folder

    def rename(self, old_name: StrOrBytes, new_name: StrOrBytes) -> tuple:
        """Rename folder from old_name to new_name"""
        result = self.mailbox.client._simple_command(
            'RENAME', encode_folder(old_name), encode_folder(new_name))
        check_command_status(result, MailboxFolderRenameError)
        return result

    def delete(self, folder: StrOrBytes) -> tuple:
        """Delete folder"""
        result = self.mailbox.client._simple_command('DELETE', encode_folder(folder))
        check_command_status(result, MailboxFolderDeleteError)
        return result

    def status(self, folder: Optional[StrOrBytes] = None, options: Optional[Iterable[str]] = None) -> Dict[str, int]:
        """
        Get the status of a folder
        :param folder: mailbox folder, current folder if None
        :param options: [str] with values from MailBoxFolderStatusOptions.all | None - for get all options
        :return: dict with available options keys
            example: {'MESSAGES': 41, 'RECENT': 0, 'UIDNEXT': 11996, 'UIDVALIDITY': 1, 'UNSEEN': 5}
        """
        command = 'STATUS'
        if folder is None:
            folder = self.get()
        if not options:
            options = tuple(MailBoxFolderStatusOptions.all)
        for opt in options:
            if opt not in MailBoxFolderStatusOptions.all:
                raise MailboxFolderStatusValueError(str(opt))
        status_result = self.mailbox.client._simple_command(
            command, encode_folder(folder), f'({" ".join(options)})')
        check_command_status(status_result, MailboxFolderStatusError)
        result = self.mailbox.client._untagged_response(status_result[0], status_result[1], command)
        check_command_status(result, MailboxFolderStatusError)
        status_data = [i for i in result[1] if type(i) is bytes][0]  # may contain tuples with encoded names
        values = status_data.decode().split('(')[-1].split(')')[0].split(' ')
        return {k: int(v) for k, v in pairs_to_dict(values).items() if str(v).isdigit()}

    def list(self, folder: StrOrBytes = '', search_args: str = '*', subscribed_only: bool = False) -> List[FolderInfo]:
        """
        Get a listing of folders on the server
        :param folder: mailbox folder, if empty - get from root
        :param search_args: search arguments, is case-sensitive mailbox name with possible wildcards
            * is a wildcard, and matches zero or more characters at this position
            % is similar to * but it does not match a hierarchy delimiter
        :param subscribed_only: bool - get only subscribed folders
        :return: [FolderInfo]
        """
        folder_item_re = re.compile(r'\((?P<flags>[\S ]*?)\) (?P<delim>[\S]+) (?P<name>.+)')
        command = 'LSUB' if subscribed_only else 'LIST'
        typ, data = self.mailbox.client._simple_command(
            command, encode_folder(folder), encode_folder(search_args))
        typ, data = self.mailbox.client._untagged_response(typ, data, command)
        result = []
        for folder_item in data:
            if not folder_item:
                continue
            if type(folder_item) is bytes:
                folder_match = re.search(folder_item_re, utf7_decode(folder_item))
                if not folder_match:
                    continue
                folder_dict = folder_match.groupdict()
                name = folder_dict['name']
                if name.startswith('"') and name.endswith('"'):
                    name = name[1:-1]
            elif type(folder_item) is tuple:
                # when name has " or \ chars
                folder_match = re.search(folder_item_re, utf7_decode(folder_item[0]))
                if not folder_match:
                    continue
                folder_dict = folder_match.groupdict()
                name = utf7_decode(folder_item[1])
            else:
                continue
            result.append(FolderInfo(
                name=name.replace('\\"', '"'),
                delim=folder_dict['delim'].replace('"', ''),
                flags=tuple(folder_dict['flags'].split())  # noqa,
            ))
        return result

    def subscribe(self, folder: StrOrBytes, value: bool) -> tuple:
        """subscribe/unsubscribe to folder"""
        method = self.mailbox.client.subscribe if value else self.mailbox.client.unsubscribe
        result = method(encode_folder(folder))
        check_command_status(result, MailboxFolderSubscribeError)
        return result
