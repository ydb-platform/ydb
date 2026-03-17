__all__ = [
    'User',
]

from typing import List

from .primitives.base import BaseTolokaObject


class User(BaseTolokaObject):
    """Information about a Toloker.

    Some information is got from the Toloker's profile, other is obtained from the Toloker's device.

    Country and language are returned as [country codes](https://toloka.ai/docs/api/regions/).

    Attributes:
        id: The Toloker's ID.
        country: The country set in the profile.
        languages: A list of languages specified in the profile.
        adult_allowed: Whether or not the Toloker agreed to complete tasks which contain adult content.
        attributes: Information obtained from the device.
    """

    class Attributes(BaseTolokaObject):
        """Information obtained from the Toloker's device.

        Attributes:
            country_by_phone: The country determined based on the phone number.
            country_by_ip: The country determined based on the IP address.
            client_type: The client which the Toloker uses to access the platform:
                * `TOLOKA_APP` — A mobile application.
                * `BROWSER` — A browser.
            user_agent_type: The user agent type which the client application uses to identify itself:
                * `BROWSER` — The desktop browser user agent.
                * `MOBILE_BROWSER` — The mobile browser user agent.
                * `OTHER` — User agents which could not be identified as either desktop or mobile browsers.
                    Normally, the Toloka mobile application identifies itself as `OTHER`.
            device_category: The category of the device:
                * `PERSONAL_COMPUTER` — A personal computer.
                * `SMARTPHONE` — A smartphone.
                * `TABLET` — A tablet device.
                * `WEARABLE_COMPUTER` — A wearable device, such as a smart watch.
            os_family: The operating system family installed on the device:
                * `ANDROID` — Android mobile operating system based on a modified version of the Linux kernel, designed primarily for touchscreen mobile devices.
                * `BLACKBERRY` — BlackBerry OS mobile operating system developed by BlackBerry Limited for its BlackBerry smartphone devices.
                * `BSD` — An operating system based on Research Unix, developed and distributed by the CSRG, and its open-source descendants like FreeBSD, OpenBSD, NetBSD, and DragonFly BSD.
                * `IOS` — iOS mobile operating system developed by Apple Inc. exclusively for its mobile devices.
                * `LINUX` — A family of open-source Unix-like operating systems based on the Linux kernel.
                * `MAC_OS` — Classic Mac OS operating system developed by Apple Inc. before 2001 for the Macintosh family of personal computers.
                * `OS_X` — macOS operating system developed by Apple Inc. since 2001 for Mac computers.
                * `WINDOWS` — Microsoft Windows operating system developed and marketed by Microsoft for personal computers.
            os_version: The version of the OS.
                The version consists of major and minor version numbers and is represented as a single floating point number, for example, `14.4`.
            os_version_major: The major version of the OS.
            os_version_minor: The minor version of the OS.
            os_version_bugfix: The build number or the bugfix version of the OS.

        """
        country_by_phone: str
        country_by_ip: str
        client_type: str
        user_agent_type: str
        device_category: str
        os_family: str
        os_version: float
        os_version_major: int
        os_version_minor: int
        os_version_bugfix: int

    id: str
    country: str
    languages: List[str]
    adult_allowed: bool
    attributes: Attributes
