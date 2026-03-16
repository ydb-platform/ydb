from pathlib import Path
from typing import Iterable, List

from library.python.resource import resfs_files, resfs_read


def extract_locales(names: Iterable[str], into: str = 'locales') -> List[str]:
    """Позволяет изъять файлы локализации (.mo), впаянные внутрь исполняемых аркадиныйх файлов,
    и положить в файловую систему, чтобы gettext могла до них добраться.

    Тем самым появляется возможность использования встроенной в Django системы локализации.

    Результатом выполнения является список путей, который можно передать
    в настройку Django LOCALE_PATHS.

    :param names: Имена локалей, файлы для которых требуется достать. Например: ru, fr
    :param into: Директория, куда требуется положить изъятые файлы.

    """
    locale_paths = []

    path_locales_root = Path(into).absolute()

    for path_resfs in resfs_files():

        if 'django.mo' not in path_resfs:
            continue

        _, pack, app, _, name, _, _ = path_resfs.rsplit('/', 6)

        if name not in names:
            continue

        dir_fs_locale = path_locales_root / pack / app / 'locale'
        dir_fs_messages = dir_fs_locale / name / 'LC_MESSAGES'
        dir_fs_messages.mkdir(parents=True, exist_ok=True)

        path_fs = dir_fs_messages / 'django.mo'
        path_fs.write_bytes(resfs_read(path_resfs))

        locale_paths.append(str(dir_fs_locale))

    return locale_paths
