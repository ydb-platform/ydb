from typing import overload
from typing_extensions import Self

from defusedxml.ElementTree import fromstring

from tableauserverclient.models.property_decorators import property_is_boolean


class ExtensionsServer:
    def __init__(self) -> None:
        self._enabled: bool | None = None
        self._block_list: list[str] | None = None

    @property
    def enabled(self) -> bool | None:
        """Indicates whether the extensions server is enabled."""
        return self._enabled

    @enabled.setter
    @property_is_boolean
    def enabled(self, value: bool | None) -> None:
        self._enabled = value

    @property
    def block_list(self) -> list[str] | None:
        """List of blocked extensions."""
        return self._block_list

    @block_list.setter
    def block_list(self, value: list[str] | None) -> None:
        self._block_list = value

    @classmethod
    def from_response(cls: type[Self], response, ns) -> Self:
        xml = fromstring(response)
        obj = cls()
        element = xml.find(".//t:extensionsServerSettings", namespaces=ns)
        if element is None:
            raise ValueError("Missing extensionsServerSettings element in response")

        if (enabled_element := element.find("./t:extensionsGloballyEnabled", namespaces=ns)) is not None:
            obj.enabled = string_to_bool(enabled_element.text)
        obj.block_list = [e.text for e in element.findall("./t:blockList", namespaces=ns) if e.text is not None]

        return obj


class SafeExtension:
    def __init__(
        self, url: str | None = None, full_data_allowed: bool | None = None, prompt_needed: bool | None = None
    ) -> None:
        self.url = url
        self._full_data_allowed = full_data_allowed
        self._prompt_needed = prompt_needed

    @property
    def full_data_allowed(self) -> bool | None:
        return self._full_data_allowed

    @full_data_allowed.setter
    @property_is_boolean
    def full_data_allowed(self, value: bool | None) -> None:
        self._full_data_allowed = value

    @property
    def prompt_needed(self) -> bool | None:
        return self._prompt_needed

    @prompt_needed.setter
    @property_is_boolean
    def prompt_needed(self, value: bool | None) -> None:
        self._prompt_needed = value


class ExtensionsSiteSettings:
    def __init__(self) -> None:
        self._enabled: bool | None = None
        self._use_default_setting: bool | None = None
        self.safe_list: list[SafeExtension] | None = None
        self._allow_trusted: bool | None = None
        self._include_tableau_built: bool | None = None
        self._include_partner_built: bool | None = None
        self._include_sandboxed: bool | None = None

    @property
    def enabled(self) -> bool | None:
        return self._enabled

    @enabled.setter
    @property_is_boolean
    def enabled(self, value: bool | None) -> None:
        self._enabled = value

    @property
    def use_default_setting(self) -> bool | None:
        return self._use_default_setting

    @use_default_setting.setter
    @property_is_boolean
    def use_default_setting(self, value: bool | None) -> None:
        self._use_default_setting = value

    @property
    def allow_trusted(self) -> bool | None:
        return self._allow_trusted

    @allow_trusted.setter
    @property_is_boolean
    def allow_trusted(self, value: bool | None) -> None:
        self._allow_trusted = value

    @property
    def include_tableau_built(self) -> bool | None:
        return self._include_tableau_built

    @include_tableau_built.setter
    @property_is_boolean
    def include_tableau_built(self, value: bool | None) -> None:
        self._include_tableau_built = value

    @property
    def include_partner_built(self) -> bool | None:
        return self._include_partner_built

    @include_partner_built.setter
    @property_is_boolean
    def include_partner_built(self, value: bool | None) -> None:
        self._include_partner_built = value

    @property
    def include_sandboxed(self) -> bool | None:
        return self._include_sandboxed

    @include_sandboxed.setter
    @property_is_boolean
    def include_sandboxed(self, value: bool | None) -> None:
        self._include_sandboxed = value

    @classmethod
    def from_response(cls: type[Self], response, ns) -> Self:
        xml = fromstring(response)
        element = xml.find(".//t:extensionsSiteSettings", namespaces=ns)
        obj = cls()
        if element is None:
            raise ValueError("Missing extensionsSiteSettings element in response")

        if (enabled_element := element.find("./t:extensionsEnabled", namespaces=ns)) is not None:
            obj.enabled = string_to_bool(enabled_element.text)
        if (default_settings_element := element.find("./t:useDefaultSetting", namespaces=ns)) is not None:
            obj.use_default_setting = string_to_bool(default_settings_element.text)
        if (allow_trusted_element := element.find("./t:allowTrusted", namespaces=ns)) is not None:
            obj.allow_trusted = string_to_bool(allow_trusted_element.text)
        if (include_tableau_built_element := element.find("./t:includeTableauBuilt", namespaces=ns)) is not None:
            obj.include_tableau_built = string_to_bool(include_tableau_built_element.text)
        if (include_partner_built_element := element.find("./t:includePartnerBuilt", namespaces=ns)) is not None:
            obj.include_partner_built = string_to_bool(include_partner_built_element.text)
        if (include_sandboxed_element := element.find("./t:includeSandboxed", namespaces=ns)) is not None:
            obj.include_sandboxed = string_to_bool(include_sandboxed_element.text)

        safe_list = []
        for safe_extension_element in element.findall("./t:safeList", namespaces=ns):
            url = safe_extension_element.find("./t:url", namespaces=ns)
            full_data_allowed = safe_extension_element.find("./t:fullDataAllowed", namespaces=ns)
            prompt_needed = safe_extension_element.find("./t:promptNeeded", namespaces=ns)

            safe_extension = SafeExtension(
                url=url.text if url is not None else None,
                full_data_allowed=string_to_bool(full_data_allowed.text) if full_data_allowed is not None else None,
                prompt_needed=string_to_bool(prompt_needed.text) if prompt_needed is not None else None,
            )
            safe_list.append(safe_extension)

        obj.safe_list = safe_list
        return obj


@overload
def string_to_bool(s: str) -> bool: ...


@overload
def string_to_bool(s: None) -> None: ...


def string_to_bool(s):
    return s.lower() == "true" if s is not None else None
