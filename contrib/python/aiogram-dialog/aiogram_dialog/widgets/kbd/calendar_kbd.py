from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any,
    Protocol,
    TypedDict,
    TypeVar,
)

from aiogram.types import CallbackQuery, InlineKeyboardButton

from aiogram_dialog.api.entities import ChatEvent
from aiogram_dialog.api.internal import RawKeyboard, StyleWidget, TextWidget
from aiogram_dialog.api.protocols import DialogManager, DialogProtocol
from aiogram_dialog.widgets.common import ManagedWidget, WhenCondition
from aiogram_dialog.widgets.style import EMPTY_STYLE
from aiogram_dialog.widgets.text import Format
from aiogram_dialog.widgets.widget_event import (
    WidgetEventProcessor,
    ensure_event_processor,
)
from .base import Keyboard

EPOCH = date(1970, 1, 1)

CALLBACK_NEXT_MONTH = "+"
CALLBACK_PREV_MONTH = "-"
CALLBACK_NEXT_YEAR = "+Y"
CALLBACK_PREV_YEAR = "-Y"
CALLBACK_NEXT_YEARS_PAGE = "+YY"
CALLBACK_PREV_YEARS_PAGE = "-YY"
CALLBACK_SCOPE_MONTHS = "M"
CALLBACK_SCOPE_YEARS = "Y"

CALLBACK_PREFIX_MONTH = "MONTH"
CALLBACK_PREFIX_YEAR = "YEAR"

ZOOM_OUT_TEXT = Format("🔍")

PREV_YEARS_PAGE_TEXT = Format("<< {date:%Y}")
NEXT_YEARS_PAGE_TEXT = Format("{date:%Y} >>")
THIS_YEAR_TEXT = Format("[ {date:%Y} ]")
YEAR_TEXT = Format("{date:%Y}")
PREV_YEAR_TEXT = Format("<< {date:%Y}")
NEXT_YEAR_TEXT = Format("{date:%Y} >>")
MONTHS_HEADER_TEXT = Format("🗓 {date:%Y}")
THIS_MONTH_TEXT = Format("[ {date:%B} ]")
MONTH_TEXT = Format("{date:%B}")
DATE_TEXT = Format("{date:%d}")
TODAY_TEXT = Format("[ {date:%d} ]")
WEEK_DAY_TEXT = Format("{date:%a}")
PREV_MONTH_TEXT = Format("<< {date:%B %Y}")
NEXT_MONTH_TEXT = Format("{date:%B %Y} >>")
DAYS_HEADER_TEXT = Format("🗓 {date:%B %Y}")

BEARING_DATE = date(2018, 1, 1)


def empty_button():
    return InlineKeyboardButton(text=" ", callback_data="")


class CalendarScope(Enum):
    DAYS = "DAYS"
    MONTHS = "MONTHS"
    YEARS = "YEARS"


def raw_from_date(d: date) -> int:
    diff = d - EPOCH
    return int(diff.total_seconds())


def date_from_raw(raw_date: int) -> date:
    return EPOCH + timedelta(seconds=raw_date)


def month_begin(offset: date):
    return offset.replace(day=1)


def next_month_begin(offset: date):
    return month_begin(month_begin(offset) + timedelta(days=31))


def prev_month_begin(offset: date):
    return month_begin(month_begin(offset) - timedelta(days=1))


def get_today(tz: timezone):
    return datetime.now(tz).date()


class CalendarData(TypedDict):
    current_scope: str
    current_offset: str


class OnDateSelected(Protocol):
    async def __call__(
            self,
            event: ChatEvent,
            widget: ManagedCalendar,
            dialog_manager: DialogManager,
            date: date,
            /,
    ) -> Any:
        raise NotImplementedError


@dataclass(frozen=True)
class CalendarUserConfig:
    firstweekday: int | None = None
    timezone: timezone | None = None
    min_date: date | None = None
    max_date: date | None = None
    month_columns: int | None = None
    years_per_page: int | None = None
    years_columns: int | None = None


T = TypeVar("T")


def _coalesce(a: T | None, b: T) -> T:
    if a is None:
        return b
    return a


@dataclass(frozen=True)
class CalendarConfig:
    firstweekday: int = 0
    timezone: timezone = datetime.now().astimezone().tzinfo
    min_date: date = date(1900, 1, 1)
    max_date: date = date(2100, 12, 31)
    month_columns: int = 3
    years_per_page: int = 20
    years_columns: int = 5

    def merge(self, other: CalendarUserConfig) -> CalendarConfig:
        return CalendarConfig(
            firstweekday=_coalesce(other.firstweekday, self.firstweekday),
            timezone=_coalesce(other.timezone, self.timezone),
            min_date=_coalesce(other.min_date, self.min_date),
            max_date=_coalesce(other.max_date, self.max_date),
            month_columns=_coalesce(other.month_columns, self.month_columns),
            years_per_page=_coalesce(other.years_per_page,
                                     self.years_per_page),
            years_columns=_coalesce(other.years_columns, self.years_columns),
        )


class CalendarScopeView(Protocol):
    """
    Calendar widget part.

    Used to represent keyboard for one of calendar scopes.
    """

    async def render(
            self,
            config: CalendarConfig,
            offset: date,
            data: dict,
            manager: DialogManager,
    ) -> list[list[InlineKeyboardButton]]:
        """
        Render keyboard for current scope.

        :param config: configuration related to current user
        :param offset: current offset from which we show dates
        :param data: data received from window getter
        :param manager: dialog manager instance
        :return: rendered keyboard
        """


CallbackGenerator = Callable[[str], str]


class CalendarDaysView(CalendarScopeView):
    def __init__(
            self,
            callback_generator: CallbackGenerator,
            date_text: TextWidget = DATE_TEXT,
            today_text: TextWidget = TODAY_TEXT,
            weekday_text: TextWidget = WEEK_DAY_TEXT,
            header_text: TextWidget = DAYS_HEADER_TEXT,
            zoom_out_text: TextWidget = ZOOM_OUT_TEXT,
            next_month_text: TextWidget = NEXT_MONTH_TEXT,
            prev_month_text: TextWidget = PREV_MONTH_TEXT,
            date_style: StyleWidget = EMPTY_STYLE,
            today_style: StyleWidget = EMPTY_STYLE,
            weekday_style: StyleWidget = EMPTY_STYLE,
            header_style: StyleWidget = EMPTY_STYLE,
            zoom_out_style: StyleWidget = EMPTY_STYLE,
            next_month_style: StyleWidget = EMPTY_STYLE,
            prev_month_style: StyleWidget = EMPTY_STYLE,
    ):
        self.callback_generator = callback_generator

        self.zoom_out_text = zoom_out_text
        self.next_month_text = next_month_text
        self.prev_month_text = prev_month_text
        self.date_text = date_text
        self.today_text = today_text
        self.weekday_text = weekday_text
        self.header_text = header_text

        self.date_style = date_style
        self.today_style = today_style
        self.weekday_style = weekday_style
        self.header_style = header_style
        self.zoom_out_style = zoom_out_style
        self.next_month_style = next_month_style
        self.prev_month_style = prev_month_style

    async def _render_date_button(
            self,
            selected_date: date,
            today: date,
            data: dict,
            manager: DialogManager,
    ) -> InlineKeyboardButton:
        current_data = {
            "date": selected_date,
            "data": data,
        }
        if selected_date == today:
            text = self.today_text
            style = self.today_style
        else:
            text = self.date_text
            style = self.date_style

        raw_date = raw_from_date(selected_date)

        return InlineKeyboardButton(
            text=await text.render_text(
                current_data, manager,
            ),
            callback_data=self.callback_generator(str(raw_date)),
            style=await style.render_style(current_data, manager),
            icon_custom_emoji_id=await style.render_emoji(
                current_data, manager,
            ),
        )

    async def _render_days(
            self,
            config: CalendarConfig,
            offset: date,
            data: dict,
            manager: DialogManager,
    ) -> list[list[InlineKeyboardButton]]:
        keyboard = []
        # align beginning
        start_date = offset.replace(day=1)  # month beginning
        min_date = max(config.min_date, start_date)
        days_since_week_start = start_date.weekday() - config.firstweekday
        if days_since_week_start < 0:
            days_since_week_start += 7
        start_date -= timedelta(days=days_since_week_start)
        end_date = next_month_begin(offset) - timedelta(days=1)
        # align ending
        max_date = min(config.max_date, end_date)
        days_since_week_start = end_date.weekday() - config.firstweekday
        days_till_week_end = (6 - days_since_week_start) % 7
        end_date += timedelta(days=days_till_week_end)
        # add days
        today = get_today(config.timezone)
        for offset in range(0, (end_date - start_date).days, 7):  # noqa: PLR1704
            row = []
            for row_offset in range(7):
                days_offset = timedelta(days=(offset + row_offset))
                current_date = start_date + days_offset
                if min_date <= current_date <= max_date:
                    row.append(await self._render_date_button(
                        current_date, today, data, manager,
                    ))
                else:
                    row.append(empty_button())
            keyboard.append(row)
        return keyboard

    async def _render_week_header(
            self,
            config: CalendarConfig,
            data: dict,
            manager: DialogManager,
    ) -> list[InlineKeyboardButton]:
        week_range = range(config.firstweekday, config.firstweekday + 7)
        header = []
        for week_day in week_range:
            week_day = week_day % 7 + 1
            data = {
                "week_day": week_day,
                "date": BEARING_DATE.replace(day=week_day),
                "data": data,
            }
            header.append(InlineKeyboardButton(
                text=await self.weekday_text.render_text(data, manager),
                callback_data="",
                style=await self.weekday_style.render_style(data, manager),
                icon_custom_emoji_id=await self.weekday_style.render_emoji(
                    data, manager,
                ),
            ))
        return header

    async def _render_pager(
            self,
            config: CalendarConfig,
            offset: date,
            data: dict,
            manager: DialogManager,
    ) -> list[InlineKeyboardButton]:
        curr_month = offset.month
        next_month = (curr_month % 12) + 1
        prev_month = (curr_month - 2) % 12 + 1
        prev_end = month_begin(offset) - timedelta(1)
        prev_begin = month_begin(prev_end)
        next_begin = next_month_begin(offset)
        if (
                prev_end < config.min_date and
                next_begin > config.max_date
        ):
            return []

        prev_month_data = {
            "month": prev_month,
            "date": prev_begin,
            "data": data,
        }
        curr_month_data = {
            "month": curr_month,
            "date": BEARING_DATE.replace(month=curr_month),
            "data": data,
        }
        next_month_data = {
            "month": next_month,
            "date": next_begin,
            "data": data,
        }
        if prev_end < config.min_date:
            prev_button = empty_button()
        else:
            prev_button = InlineKeyboardButton(
                text=await self.prev_month_text.render_text(
                    prev_month_data, manager,
                ),
                callback_data=self.callback_generator(CALLBACK_PREV_MONTH),
                style=await self.prev_month_style.render_style(
                    prev_month_data, manager,
                ),
                icon_custom_emoji_id=await self.prev_month_style.render_emoji(
                    prev_month_data, manager,
                ),
            )
        zoom_button = InlineKeyboardButton(
            text=await self.zoom_out_text.render_text(
                curr_month_data, manager,
            ),
            callback_data=self.callback_generator(CALLBACK_SCOPE_MONTHS),
            style=await self.zoom_out_style.render_style(
                curr_month_data, manager,
            ),
            icon_custom_emoji_id=await self.zoom_out_style.render_emoji(
                curr_month_data, manager,
            ),
        )
        if next_begin > config.max_date:
            next_button = empty_button()
        else:
            next_button = InlineKeyboardButton(
                text=await self.next_month_text.render_text(
                    next_month_data, manager,
                ),
                callback_data=self.callback_generator(CALLBACK_NEXT_MONTH),
                style=await self.next_month_style.render_style(
                    next_month_data, manager,
                ),
                icon_custom_emoji_id=await self.next_month_style.render_emoji(
                    next_month_data, manager,
                ),
            )

        return [prev_button, zoom_button, next_button]

    async def _render_header(
            self,
            config: CalendarConfig,
            offset: date,
            data: dict,
            manager: DialogManager,
    ) -> list[InlineKeyboardButton]:
        data = {
            "date": offset,
            "data": data,
        }
        return [InlineKeyboardButton(
            text=await self.header_text.render_text(data, manager),
            callback_data=self.callback_generator(CALLBACK_SCOPE_MONTHS),
            style=await self.header_style.render_style(
                data, manager,
            ),
            icon_custom_emoji_id=await self.header_style.render_emoji(
                data, manager,
            ),
        )]

    async def render(
            self,
            config: CalendarConfig,
            offset: date,
            data: dict,
            manager: DialogManager,
    ) -> list[list[InlineKeyboardButton]]:
        return [
            await self._render_header(config, offset, data, manager),
            await self._render_week_header(config, data, manager),
            *await self._render_days(config, offset, data, manager),
            await self._render_pager(config, offset, data, manager),
        ]


class CalendarMonthView(CalendarScopeView):
    def __init__(
            self,
            callback_generator: CallbackGenerator,
            month_text: TextWidget = MONTH_TEXT,
            this_month_text: TextWidget = THIS_MONTH_TEXT,
            header_text: TextWidget = MONTHS_HEADER_TEXT,
            zoom_out_text: TextWidget = ZOOM_OUT_TEXT,
            next_year_text: TextWidget = NEXT_YEAR_TEXT,
            prev_year_text: TextWidget = PREV_YEAR_TEXT,
            month_style: StyleWidget = EMPTY_STYLE,
            this_month_style: StyleWidget = EMPTY_STYLE,
            header_style: StyleWidget = EMPTY_STYLE,
            zoom_out_style: StyleWidget = EMPTY_STYLE,
            next_year_style: StyleWidget = EMPTY_STYLE,
            prev_year_style: StyleWidget = EMPTY_STYLE,
    ):
        self.callback_generator = callback_generator

        self.month_text = month_text
        self.this_month_text = this_month_text
        self.header_text = header_text
        self.zoom_out_text = zoom_out_text
        self.next_year_text = next_year_text
        self.prev_year_text = prev_year_text

        self.month_style = month_style
        self.this_month_style = this_month_style
        self.header_style = header_style
        self.zoom_out_style = zoom_out_style
        self.next_year_style = next_year_style
        self.prev_year_style = prev_year_style

    async def _render_pager(
            self,
            config: CalendarConfig,
            offset: date,
            data: dict,
            manager: DialogManager,
    ) -> list[InlineKeyboardButton]:
        curr_year = offset.year
        next_year = curr_year + 1
        prev_year = curr_year - 1

        if curr_year not in range(
                config.min_date.year, config.max_date.year,
        ):
            return []

        prev_year_data = {
            "year": prev_year,
            "date": max(
                BEARING_DATE.replace(year=prev_year),
                config.min_date,
            ),
            "data": data,
        }
        curr_year_data = {
            "year": curr_year,
            "date": BEARING_DATE.replace(year=curr_year),
            "data": data,
        }
        next_year_data = {
            "year": next_year,
            "date": min(
                BEARING_DATE.replace(year=next_year),
                config.max_date,
            ),
            "data": data,
        }
        if prev_year < config.min_date.year:
            prev_button = empty_button()
        else:
            prev_button = InlineKeyboardButton(
                text=await self.prev_year_text.render_text(
                    prev_year_data, manager,
                ),
                callback_data=self.callback_generator(CALLBACK_PREV_YEAR),
                style=await self.prev_year_style.render_style(
                    prev_year_data, manager,
                ),
                icon_custom_emoji_id=await self.prev_year_style.render_emoji(
                    prev_year_data, manager,
                ),
            )
        if next_year > config.max_date.year:
            next_button = empty_button()
        else:
            next_button = InlineKeyboardButton(
                text=await self.next_year_text.render_text(
                    next_year_data, manager,
                ),
                callback_data=self.callback_generator(CALLBACK_NEXT_YEAR),
                style=await self.next_year_style.render_style(
                    next_year_data, manager,
                ),
                icon_custom_emoji_id=await self.next_year_style.render_emoji(
                    next_year_data, manager,
                ),
            )
        zoom_button = InlineKeyboardButton(
            text=await self.zoom_out_text.render_text(
                curr_year_data, manager,
            ),
            callback_data=self.callback_generator(CALLBACK_SCOPE_YEARS),
            style=await self.zoom_out_style.render_style(
                curr_year_data, manager,
            ),
            icon_custom_emoji_id=await self.zoom_out_style.render_emoji(
                curr_year_data, manager,
            ),
        )
        return [prev_button, zoom_button, next_button]

    def _is_month_allowed(
            self, config: CalendarConfig, offset: date, month: int,
    ) -> bool:
        start = date(offset.year, month, 1)
        end = next_month_begin(start) - timedelta(days=1)
        return end >= config.min_date and start <= config.max_date

    async def _render_month_button(
            self,
            month: int,
            this_month: int,
            data: dict,
            offset: date,
            config: CalendarConfig,
            manager: DialogManager,
    ) -> InlineKeyboardButton:
        if not self._is_month_allowed(config, offset, month):
            return empty_button()

        month_data = {
            "month": month,
            "date": BEARING_DATE.replace(month=month),
            "data": data,
        }
        if month == this_month:
            text = self.this_month_text
            style = self.this_month_style
        else:
            text = self.month_text
            style = self.month_style

        return InlineKeyboardButton(
            text=await text.render_text(
                month_data, manager,
            ),
            callback_data=self.callback_generator(
                f"{CALLBACK_PREFIX_MONTH}{month}",
            ),
            style=await style.render_style(
                month_data, manager,
            ),
            icon_custom_emoji_id=await style.render_emoji(
                month_data, manager,
            ),
        )

    async def _render_months(
            self,
            config: CalendarConfig,
            offset: date,
            data: dict,
            manager: DialogManager,
    ) -> list[list[InlineKeyboardButton]]:
        keyboard = []
        today = get_today(config.timezone)
        if offset.year == today.year:
            this_month = today.month
        else:
            this_month = -1
        for row in range(1, 13, config.month_columns):
            keyboard_row = []
            for column in range(config.month_columns):
                month = row + column
                keyboard_row.append(await self._render_month_button(
                    month, this_month, data, offset, config, manager,
                ))
            keyboard.append(keyboard_row)
        return keyboard

    async def _render_header(
            self, config, offset, data, manager,
    ) -> list[InlineKeyboardButton]:
        data = {
            "date": offset,
            "data": data,
        }
        return [InlineKeyboardButton(
            text=await self.header_text.render_text(data, manager),
            callback_data=self.callback_generator(CALLBACK_SCOPE_YEARS),
            style=await self.header_style.render_style(
                data, manager,
            ),
            icon_custom_emoji_id=await self.header_style.render_emoji(
                data, manager,
            ),
        )]

    async def render(
            self,
            config: CalendarConfig,
            offset: date,
            data: dict,
            manager: DialogManager,
    ) -> list[list[InlineKeyboardButton]]:
        return [
            await self._render_header(config, offset, data, manager),
            *await self._render_months(config, offset, data, manager),
            await self._render_pager(config, offset, data, manager),
        ]


class CalendarYearsView(CalendarScopeView):
    def __init__(
            self,
            callback_generator: CallbackGenerator,
            year_text: TextWidget = YEAR_TEXT,
            this_year_text: TextWidget = THIS_YEAR_TEXT,
            next_page_text: TextWidget = NEXT_YEARS_PAGE_TEXT,
            prev_page_text: TextWidget = PREV_YEARS_PAGE_TEXT,
            year_style: StyleWidget = EMPTY_STYLE,
            this_year_style: StyleWidget = EMPTY_STYLE,
            next_page_style: StyleWidget = EMPTY_STYLE,
            prev_page_style: StyleWidget = EMPTY_STYLE,
    ):
        self.callback_generator = callback_generator

        self.year_text = year_text
        self.this_year_text = this_year_text
        self.next_page_text = next_page_text
        self.prev_page_text = prev_page_text

        self.year_style = year_style
        self.this_year_style = this_year_style
        self.next_page_style = next_page_style
        self.prev_page_style = prev_page_style

    async def _render_pager(
            self,
            config: CalendarConfig,
            offset: date,
            data: dict,
            manager: DialogManager,
    ) -> list[InlineKeyboardButton]:
        curr_year = offset.year
        next_year = curr_year + config.years_per_page
        prev_year = curr_year - config.years_per_page

        prev_year_data = {
            "year": prev_year,
            "date": max(
                BEARING_DATE.replace(year=prev_year),
                config.min_date,
            ),
            "data": data,
        }
        next_year_data = {
            "year": next_year,
            "date": min(
                BEARING_DATE.replace(year=next_year),
                config.max_date,
            ),
            "data": data,
        }
        if curr_year <= config.min_date.year:
            prev_button = empty_button()
        else:
            prev_button = InlineKeyboardButton(
                text=await self.prev_page_text.render_text(
                    prev_year_data, manager,
                ),
                callback_data=self.callback_generator(
                    CALLBACK_PREV_YEARS_PAGE,
                ),
                style=await self.prev_page_style.render_style(
                    prev_year_data, manager,
                ),
                icon_custom_emoji_id=await self.prev_page_style.render_emoji(
                    prev_year_data, manager,
                ),
            )
        if next_year > config.max_date.year:
            next_button = empty_button()
        else:
            next_button = InlineKeyboardButton(
                text=await self.next_page_text.render_text(
                    next_year_data, manager,
                ),
                callback_data=self.callback_generator(
                    CALLBACK_NEXT_YEARS_PAGE,
                ),
                style=await self.next_page_style.render_style(
                    next_year_data, manager,
                ),
                icon_custom_emoji_id=await self.next_page_style.render_emoji(
                    next_year_data, manager,
                ),
            )

        if prev_button == next_button == empty_button():
            return []
        return [prev_button, next_button]

    def _is_year_allowed(self, config: CalendarConfig, year: int) -> bool:
        return config.min_date.year <= year <= config.max_date.year

    async def _render_year_button(
            self,
            year: int,
            this_year: int,
            data: dict,
            config: CalendarConfig,
            manager: DialogManager,
    ) -> InlineKeyboardButton:
        if not self._is_year_allowed(config, year):
            return empty_button()
        if year == this_year:
            text = self.this_year_text
            style = self.this_year_style
        else:
            text = self.year_text
            style = self.year_style

        year_data = {
            "year": year,
            "date": BEARING_DATE.replace(year=year),
            "data": data,
        }
        return InlineKeyboardButton(
            text=await text.render_text(
                year_data, manager,
            ),
            callback_data=self.callback_generator(
                f"{CALLBACK_PREFIX_YEAR}{year}",
            ),
            style=await style.render_style(
                year_data, manager,
            ),
            icon_custom_emoji_id=await style.render_emoji(
                year_data, manager,
            ),
        )

    async def _render_years(
            self,
            config: CalendarConfig,
            offset: date,
            data: dict,
            manager: DialogManager,
    ) -> list[list[InlineKeyboardButton]]:
        keyboard = []
        this_year = get_today(config.timezone).year
        years_columns = config.years_columns
        years_per_page = config.years_per_page

        for row in range(0, years_per_page, years_columns):
            keyboard_row = []
            for column in range(years_columns):
                curr_year = offset.year + row + column
                keyboard_row.append(await self._render_year_button(
                    curr_year, this_year, data, config, manager,
                ))
            keyboard.append(keyboard_row)
        return keyboard

    async def render(
            self,
            config: CalendarConfig,
            offset: date,
            data: dict,
            manager: DialogManager,
    ) -> list[list[InlineKeyboardButton]]:
        return [
            *await self._render_years(config, offset, data, manager),
            await self._render_pager(config, offset, data, manager),
        ]


class Calendar(Keyboard):
    """
    Calendar widget.

    Used to render keyboard for date selection.
    """

    def __init__(
            self,
            id: str,
            on_click: OnDateSelected | WidgetEventProcessor | None = None,
            config: CalendarConfig | None = None,
            when: WhenCondition = None,
    ) -> None:
        """
        Init calendar widget.

        :param id: ID of widget
        :param on_click: Function to handle date selection
        :param config: Calendar configuration
        :param when: Condition when to show widget
        """
        super().__init__(id=id, when=when)
        self.on_click = ensure_event_processor(on_click)
        if config is None:
            config = CalendarConfig()
        self.config = config
        self.views = self._init_views()
        self._handlers = {
            CALLBACK_NEXT_MONTH: self._handle_next_month,
            CALLBACK_PREV_MONTH: self._handle_prev_month,
            CALLBACK_NEXT_YEAR: self._handle_next_year,
            CALLBACK_PREV_YEAR: self._handle_prev_year,
            CALLBACK_NEXT_YEARS_PAGE: self._handle_next_years_page,
            CALLBACK_PREV_YEARS_PAGE: self._handle_prev_years_page,
            CALLBACK_SCOPE_MONTHS: self._handle_scope_months,
            CALLBACK_SCOPE_YEARS: self._handle_scope_years,
        }

    def _init_views(self) -> dict[CalendarScope, CalendarScopeView]:
        """
        Calendar scopes view initializer.

        Override this method customize how calendar is rendered.
        You can either set Text widgets for buttons in default views or
        create own implementation of views
        """
        return {
            CalendarScope.DAYS: CalendarDaysView(self._item_callback_data),
            CalendarScope.MONTHS: CalendarMonthView(self._item_callback_data),
            CalendarScope.YEARS: CalendarYearsView(self._item_callback_data),
        }

    async def _get_user_config(
            self,
            data: dict,
            manager: DialogManager,
    ) -> CalendarUserConfig:
        """
        User related config getter.

        Override this method to customize how user config is retrieved.

        :param data: data from window getter
        :param manager: dialog manager instance
        :return:
        """
        return CalendarUserConfig()

    async def _render_keyboard(
            self,
            data,
            manager: DialogManager,
    ) -> RawKeyboard:
        scope = self.get_scope(manager)
        view = self.views[scope]
        offset = self.get_offset(manager)
        config = self.config.merge(await self._get_user_config(data, manager))
        if offset is None:
            offset = get_today(config.timezone)
            self.set_offset(offset, manager)
        return await view.render(config, offset, data, manager)

    def get_scope(self, manager: DialogManager) -> CalendarScope:
        calendar_data: CalendarData = self.get_widget_data(manager, {})
        current_scope = calendar_data.get("current_scope")
        if not current_scope:
            return CalendarScope.DAYS
        try:
            return CalendarScope(current_scope)
        except ValueError:
            # LOG
            return CalendarScope.DAYS

    def get_offset(self, manager: DialogManager) -> date | None:
        calendar_data: CalendarData = self.get_widget_data(manager, {})
        current_offset = calendar_data.get("current_offset")
        if current_offset is None:
            return None
        return date.fromisoformat(current_offset)

    def set_offset(self, new_offset: date,
                   manager: DialogManager) -> None:
        data = self.get_widget_data(manager, {})
        data["current_offset"] = new_offset.isoformat()

    def set_scope(self, new_scope: CalendarScope,
                  manager: DialogManager) -> None:
        data = self.get_widget_data(manager, {})
        data["current_scope"] = new_scope.value

    def managed(self, manager: DialogManager) -> ManagedCalendar:
        return ManagedCalendar(self, manager)

    async def _handle_scope_months(
            self, data: str, manager: DialogManager,
    ) -> None:
        self.set_scope(CalendarScope.MONTHS, manager)

    async def _handle_scope_years(
            self, data: str, manager: DialogManager,
    ) -> None:
        self.set_scope(CalendarScope.YEARS, manager)

    async def _handle_prev_month(
            self, data: str, manager: DialogManager,
    ) -> None:
        offset = self.get_offset(manager)
        offset = month_begin(month_begin(offset) - timedelta(days=1))
        self.set_offset(offset, manager)

    async def _handle_next_month(
            self, data: str, manager: DialogManager,
    ) -> None:
        offset = self.get_offset(manager)
        offset = next_month_begin(offset)
        self.set_offset(offset, manager)

    async def _handle_prev_year(
            self, data: str, manager: DialogManager,
    ) -> None:
        offset = self.get_offset(manager)
        offset = offset.replace(offset.year - 1)
        self.set_offset(offset, manager)

    async def _handle_next_year(
            self, data: str, manager: DialogManager,
    ) -> None:
        offset = self.get_offset(manager)
        offset = offset.replace(offset.year + 1)
        self.set_offset(offset, manager)

    async def _handle_prev_years_page(
            self, data: str, manager: DialogManager,
    ) -> None:
        offset = self.get_offset(manager)
        offset = offset.replace(offset.year - self.config.years_per_page)
        self.set_offset(offset, manager)

    async def _handle_next_years_page(
            self, data: str, manager: DialogManager,
    ) -> None:
        offset = self.get_offset(manager)
        offset = offset.replace(offset.year + self.config.years_per_page)
        self.set_offset(offset, manager)

    async def _handle_click_month(
            self, data: str, manager: DialogManager,
    ) -> None:
        offset = self.get_offset(manager)
        month = int(data[len(CALLBACK_PREFIX_MONTH):])
        offset = date(offset.year, month, 1)
        self.set_offset(offset, manager)
        self.set_scope(CalendarScope.DAYS, manager)

    async def _handle_click_year(
            self, data: str, manager: DialogManager,
    ) -> None:
        year = int(data[len(CALLBACK_PREFIX_YEAR):])
        offset = date(year, 1, 1)
        self.set_offset(offset, manager)
        self.set_scope(CalendarScope.MONTHS, manager)

    async def _handle_click_date(
            self, data: str, manager: DialogManager,
    ) -> None:
        await self.on_click.process_event(
            manager.event,
            self.managed(manager),
            manager,
            date_from_raw(int(data)),
        )

    async def _process_item_callback(
            self,
            callback: CallbackQuery,
            data: str,
            dialog: DialogProtocol,
            manager: DialogManager,
    ) -> bool:
        if data in self._handlers:
            handler = self._handlers[data]
        elif data.startswith(CALLBACK_PREFIX_MONTH):
            handler = self._handle_click_month
        elif data.startswith(CALLBACK_PREFIX_YEAR):
            handler = self._handle_click_year
        else:
            handler = self._handle_click_date
        await handler(data, manager)
        return True


class ManagedCalendar(ManagedWidget[Calendar]):
    def get_scope(self) -> CalendarScope:
        """Get current scope showing in calendar."""
        return self.widget.get_scope(self.manager)

    def get_offset(self) -> date | None:
        """Get current offset from which calendar is shown."""
        return self.widget.get_offset(self.manager)

    def set_offset(
            self, new_offset: date,
    ) -> None:
        """Set current offset for calendar paging."""
        return self.widget.set_offset(new_offset, self.manager)

    def set_scope(
            self, new_scope: CalendarScope,
    ) -> None:
        """Set current scope showing in calendar."""
        return self.widget.set_scope(new_scope, self.manager)
