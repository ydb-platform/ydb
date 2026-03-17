"""Localization for the Hijri month and day names."""

from typing import ClassVar, Dict, List, Type

_locale_map: Dict[str, Type["Locale"]] = {}


def get_locale(name: str) -> "Locale":
    """Return an appropriate :obj:`Locale` corresponding to a locale name.

    Args:
        name: name of the locale.
    """

    language_tag = name.lower()[:2]
    locale_cls = _locale_map.get(language_tag)

    if locale_cls is None:
        raise ValueError(f"Unsupported language: {language_tag}")

    return locale_cls()


class Locale:
    """A Locale object represents locale-specific data and functionality."""

    language_tag: ClassVar[str]
    month_names: ClassVar[List[str]] = []
    gregorian_month_names: ClassVar[List[str]] = []
    day_names: ClassVar[List[str]] = []
    notation: ClassVar[str]
    gregorian_notation: ClassVar[str]

    def __init_subclass__(cls) -> None:
        if cls.language_tag in _locale_map:
            raise LookupError(f"Duplicated language tag: {cls.language_tag}")
        _locale_map[cls.language_tag] = cls

    def month_name(self, month: int) -> str:
        """Return the month name for a specified Hijri month of the year.

        Args:
            month: month of year, in range 1-12.
        """

        return self.month_names[month - 1]

    def gregorian_month_name(self, month: int) -> str:
        """Return the month name for a specified Gregorian month of the year.

        Args:
            month: month of year, in range 1-12.
        """

        return self.gregorian_month_names[month - 1]

    def day_name(self, day: int) -> str:
        """Return the day name for a specified day of the week.

        Args:
            day: day of week, where Monday is 1 and Sunday is 7.
        """

        return self.day_names[day - 1]


class EnglishLocale(Locale):
    """An English Locale object represents English locale-specific data and
    functionality.
    """

    language_tag = "en"
    month_names = [
        "Muharram",
        "Safar",
        "Rabi’ al-Awwal",
        "Rabi’ al-Thani",
        "Jumada al-Ula",
        "Jumada al-Akhirah",
        "Rajab",
        "Sha’ban",
        "Ramadhan",
        "Shawwal",
        "Dhu al-Qi’dah",
        "Dhu al-Hijjah",
    ]
    gregorian_month_names = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]
    day_names = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    notation = "AH"
    gregorian_notation = "CE"


class ArabicLocale(Locale):
    """An Arabic Locale object represents Arabic locale-specific data and
    functionality.
    """

    language_tag = "ar"
    month_names = [
        "محرم",
        "صفر",
        "ربيع الأول",
        "ربيع الثاني",
        "جمادى الأولى",
        "جمادى الآخرة",
        "رجب",
        "شعبان",
        "رمضان",
        "شوال",
        "ذو القعدة",
        "ذو الحجة",
    ]
    gregorian_month_names = [
        "يناير",
        "فبراير",
        "مارس",
        "أبريل",
        "مايو",
        "يونيو",
        "يوليو",
        "أغسطس",
        "سبتمبر",
        "أكتوبر",
        "نوفمبر",
        "ديسمبر",
    ]
    day_names = [
        "الإثنين",
        "الثلاثاء",
        "الأربعاء",
        "الخميس",
        "الجمعة",
        "السبت",
        "الأحد",
    ]
    notation = "هـ"
    gregorian_notation = "م"


class BengaliLocale(Locale):
    """A Bengali Locale object represents Bengali locale-specific data and
    functionality.
    """

    language_tag = "bn"
    month_names = [
        "মুহাররম",
        "সফর",
        "রবিউল আউয়াল",
        "রবিউস সানী",
        "জুমাদাল উলা",
        "জুমাদাস সানী",
        "রজব",
        "শাবান",
        "রমজান",
        "শাওয়াল",
        "জিলক্বদ",
        "জিলহজ",
    ]
    gregorian_month_names = [
        "জানুয়ারি",
        "ফেব্রুয়ারি",
        "মার্চ",
        "এপ্রিল",
        "মে",
        "জুন",
        "ঞ্জুলাই",
        "আগস্ট",
        "সেপ্টেম্বর",
        "অক্টোবর",
        "নভেম্বর",
        "ডিসেম্বর",
    ]
    day_names = [
        "সোমবার",
        "মঙ্গলবার",
        "বুধবার",
        "বৃহস্পতিবার",
        "শুক্রবার",
        "শনিবার",
        "রবিবার",
    ]
    notation = "হিজরি"
    gregorian_notation = "খ্রিস্টাব্দ"
