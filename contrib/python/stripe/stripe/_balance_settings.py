# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._singleton_api_resource import SingletonAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from typing import ClassVar, Dict, List, Optional, cast
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params._balance_settings_modify_params import (
        BalanceSettingsModifyParams,
    )
    from stripe.params._balance_settings_retrieve_params import (
        BalanceSettingsRetrieveParams,
    )


class BalanceSettings(
    SingletonAPIResource["BalanceSettings"],
    UpdateableAPIResource["BalanceSettings"],
):
    """
    Options for customizing account balances and payout settings for a Stripe platform's connected accounts.
    """

    OBJECT_NAME: ClassVar[Literal["balance_settings"]] = "balance_settings"

    class Payments(StripeObject):
        class Payouts(StripeObject):
            class Schedule(StripeObject):
                interval: Optional[
                    Literal["daily", "manual", "monthly", "weekly"]
                ]
                """
                How frequently funds will be paid out. One of `manual` (payouts only created via API call), `daily`, `weekly`, or `monthly`.
                """
                monthly_payout_days: Optional[List[int]]
                """
                The day of the month funds will be paid out. Only shown if `interval` is monthly. Payouts scheduled between the 29th and 31st of the month are sent on the last day of shorter months.
                """
                weekly_payout_days: Optional[
                    List[
                        Literal[
                            "friday",
                            "monday",
                            "thursday",
                            "tuesday",
                            "wednesday",
                        ]
                    ]
                ]
                """
                The days of the week when available funds are paid out, specified as an array, for example, [`monday`, `tuesday`]. Only shown if `interval` is weekly.
                """

            minimum_balance_by_currency: Optional[Dict[str, int]]
            """
            The minimum balance amount to retain per currency after automatic payouts. Only funds that exceed these amounts are paid out. Learn more about the [minimum balances for automatic payouts](https://docs.stripe.com/payouts/minimum-balances-for-automatic-payouts).
            """
            schedule: Optional[Schedule]
            """
            Details on when funds from charges are available, and when they are paid out to an external account. See our [Setting Bank and Debit Card Payouts](https://docs.stripe.com/connect/bank-transfers#payout-information) documentation for details.
            """
            statement_descriptor: Optional[str]
            """
            The text that appears on the bank account statement for payouts. If not set, this defaults to the platform's bank descriptor as set in the Dashboard.
            """
            status: Literal["disabled", "enabled"]
            """
            Whether the funds in this account can be paid out.
            """
            _inner_class_types = {"schedule": Schedule}

        class SettlementTiming(StripeObject):
            delay_days: int
            """
            The number of days charge funds are held before becoming available.
            """
            delay_days_override: Optional[int]
            """
            The number of days charge funds are held before becoming available. If present, overrides the default, or minimum available, for the account.
            """

        debit_negative_balances: Optional[bool]
        """
        A Boolean indicating if Stripe should try to reclaim negative balances from an attached bank account. See [Understanding Connect account balances](https://docs.stripe.com/connect/account-balances) for details. The default value is `false` when [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `application`, which includes Custom accounts, otherwise `true`.
        """
        payouts: Optional[Payouts]
        """
        Settings specific to the account's payouts.
        """
        settlement_timing: SettlementTiming
        _inner_class_types = {
            "payouts": Payouts,
            "settlement_timing": SettlementTiming,
        }

    object: Literal["balance_settings"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    payments: Payments

    @classmethod
    def modify(
        cls, **params: Unpack["BalanceSettingsModifyParams"]
    ) -> "BalanceSettings":
        """
        Updates balance settings for a given connected account.
         Related guide: [Making API calls for connected accounts](https://docs.stripe.com/connect/authentication)
        """
        return cast(
            "BalanceSettings",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, **params: Unpack["BalanceSettingsModifyParams"]
    ) -> "BalanceSettings":
        """
        Updates balance settings for a given connected account.
         Related guide: [Making API calls for connected accounts](https://docs.stripe.com/connect/authentication)
        """
        return cast(
            "BalanceSettings",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, **params: Unpack["BalanceSettingsRetrieveParams"]
    ) -> "BalanceSettings":
        """
        Retrieves balance settings for a given connected account.
         Related guide: [Making API calls for connected accounts](https://docs.stripe.com/connect/authentication)
        """
        instance = cls(None, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, **params: Unpack["BalanceSettingsRetrieveParams"]
    ) -> "BalanceSettings":
        """
        Retrieves balance settings for a given connected account.
         Related guide: [Making API calls for connected accounts](https://docs.stripe.com/connect/authentication)
        """
        instance = cls(None, **params)
        await instance.refresh_async()
        return instance

    @classmethod
    def class_url(cls):
        return "/v1/balance_settings"

    _inner_class_types = {"payments": Payments}
