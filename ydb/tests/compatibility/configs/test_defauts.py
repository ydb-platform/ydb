from __future__ import annotations
import json
import yatest.common


class Differ:
    def __init__(self, old_version: str, new_version: str):
        self.errors: list[tuple[str, str]] = []
        self.old_version = old_version
        self.new_version = new_version

    def __compare_field(self, old, new, path: list[str]) -> None:
        if type(old) is not type(new):
            self.errors.append(('.'.join(path), f'types differ, in {self.old_version} was {type(old)}, but in {self.new_version} is {type(new)}'))
        if isinstance(old, list):
            for i in range(len(old)):
                new_path = path + [str(i)]
                if i < len(new):
                    self.__compare_field(old[i], new[i], new_path)
                else:
                    self.errors.append(('.'.join(new_path), f'field was in {self.old_version}, but is deleted in {self.new_version}'))
        elif isinstance(old, dict):
            for key in sorted(old.keys()):
                old_field = old[key]
                new_field = new.get(key)
                new_path = path + [key]
                if new_field is None:
                    self.errors.append(('.'.join(new_path), f'field was in {self.old_version}, but is deleted in {self.new_version}'))
                    continue
                old_id = old_field.get('id')
                new_id = new_field.get('id')
                if old_id is not None and new_id is not None:
                    if old_id != new_id:
                        self.errors.append(('.'.join(new_path), f'ids differ, in {self.old_version} was {old_id}, but in {self.new_version} is {new_id}'))
                else:
                    if old_id is None:
                        self.errors.append(('.'.join(new_path), f'no id for field in {self.old_version}'))
                    if new_id is None:
                        self.errors.append(('.'.join(new_path), f'no id for field in {self.new_version}'))
                old_value = old_field.get('default-value')
                new_value = new_field.get('default-value')
                if old_value is not None and new_value is not None:
                    self.__compare_field(old_value, new_value, new_path)
                else:
                    if old_value is None:
                        self.errors.append(('.'.join(new_path), f'no value for field in {self.old_version}'))
                    if new_value is None:
                        self.errors.append(('.'.join(new_path), f'no value for field in {self.new_version}'))
        else:
            if old != new:
                if isinstance(old, bool) and len(path) > 0 and path[0] == 'FeatureFlags':
                    if old:
                        self.errors.append(('.'.join(path), f'feature flag was swithed on in {self.old_version}, bat is switched off in {self.new_version}'))

    def compare_versions(self) -> str:
        with open(yatest.common.binary_path(f'ydb/tests/library/compatibility/configs/{self.old_version}')) as f:
            old = json.load(f).get('proto')
        assert old is not None

        with open(yatest.common.binary_path(f'ydb/tests/library/compatibility/configs/{self.new_version}')) as f:
            new = json.load(f).get('proto')
        assert new is not None

        self.__compare_field(old, new, [])
        return '\n'.join([f'{path}: {msg}' for path, msg in self.errors])


def test_stable_25_1__main():
    return Differ('stable-25-1', 'current').compare_versions()


def test_stable_25_1_3__main():
    return Differ('stable-25-1-3', 'current').compare_versions()
