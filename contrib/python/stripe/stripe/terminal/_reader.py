# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._deletable_api_resource import DeletableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._test_helpers import APIResourceTestHelpers
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, List, Optional, cast, overload
from typing_extensions import Literal, Type, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._charge import Charge
    from stripe._payment_intent import PaymentIntent
    from stripe._payment_method import PaymentMethod
    from stripe._refund import Refund
    from stripe._setup_intent import SetupIntent
    from stripe.params.terminal._reader_cancel_action_params import (
        ReaderCancelActionParams,
    )
    from stripe.params.terminal._reader_collect_inputs_params import (
        ReaderCollectInputsParams,
    )
    from stripe.params.terminal._reader_collect_payment_method_params import (
        ReaderCollectPaymentMethodParams,
    )
    from stripe.params.terminal._reader_confirm_payment_intent_params import (
        ReaderConfirmPaymentIntentParams,
    )
    from stripe.params.terminal._reader_create_params import ReaderCreateParams
    from stripe.params.terminal._reader_delete_params import ReaderDeleteParams
    from stripe.params.terminal._reader_list_params import ReaderListParams
    from stripe.params.terminal._reader_modify_params import ReaderModifyParams
    from stripe.params.terminal._reader_present_payment_method_params import (
        ReaderPresentPaymentMethodParams,
    )
    from stripe.params.terminal._reader_process_payment_intent_params import (
        ReaderProcessPaymentIntentParams,
    )
    from stripe.params.terminal._reader_process_setup_intent_params import (
        ReaderProcessSetupIntentParams,
    )
    from stripe.params.terminal._reader_refund_payment_params import (
        ReaderRefundPaymentParams,
    )
    from stripe.params.terminal._reader_retrieve_params import (
        ReaderRetrieveParams,
    )
    from stripe.params.terminal._reader_set_reader_display_params import (
        ReaderSetReaderDisplayParams,
    )
    from stripe.params.terminal._reader_succeed_input_collection_params import (
        ReaderSucceedInputCollectionParams,
    )
    from stripe.params.terminal._reader_timeout_input_collection_params import (
        ReaderTimeoutInputCollectionParams,
    )
    from stripe.terminal._location import Location


class Reader(
    CreateableAPIResource["Reader"],
    DeletableAPIResource["Reader"],
    ListableAPIResource["Reader"],
    UpdateableAPIResource["Reader"],
):
    """
    A Reader represents a physical device for accepting payment details.

    Related guide: [Connecting to a reader](https://docs.stripe.com/terminal/payments/connect-reader)
    """

    OBJECT_NAME: ClassVar[Literal["terminal.reader"]] = "terminal.reader"

    class Action(StripeObject):
        class CollectInputs(StripeObject):
            class Input(StripeObject):
                class CustomText(StripeObject):
                    description: Optional[str]
                    """
                    Customize the default description for this input
                    """
                    skip_button: Optional[str]
                    """
                    Customize the default label for this input's skip button
                    """
                    submit_button: Optional[str]
                    """
                    Customize the default label for this input's submit button
                    """
                    title: Optional[str]
                    """
                    Customize the default title for this input
                    """

                class Email(StripeObject):
                    value: Optional[str]
                    """
                    The collected email address
                    """

                class Numeric(StripeObject):
                    value: Optional[str]
                    """
                    The collected number
                    """

                class Phone(StripeObject):
                    value: Optional[str]
                    """
                    The collected phone number
                    """

                class Selection(StripeObject):
                    class Choice(StripeObject):
                        id: Optional[str]
                        """
                        The identifier for the selected choice. Maximum 50 characters.
                        """
                        style: Optional[Literal["primary", "secondary"]]
                        """
                        The button style for the choice. Can be `primary` or `secondary`.
                        """
                        text: str
                        """
                        The text to be selected. Maximum 30 characters.
                        """

                    choices: List[Choice]
                    """
                    List of possible choices to be selected
                    """
                    id: Optional[str]
                    """
                    The id of the selected choice
                    """
                    text: Optional[str]
                    """
                    The text of the selected choice
                    """
                    _inner_class_types = {"choices": Choice}

                class Signature(StripeObject):
                    value: Optional[str]
                    """
                    The File ID of a collected signature image
                    """

                class Text(StripeObject):
                    value: Optional[str]
                    """
                    The collected text value
                    """

                class Toggle(StripeObject):
                    default_value: Optional[Literal["disabled", "enabled"]]
                    """
                    The toggle's default value. Can be `enabled` or `disabled`.
                    """
                    description: Optional[str]
                    """
                    The toggle's description text. Maximum 50 characters.
                    """
                    title: Optional[str]
                    """
                    The toggle's title text. Maximum 50 characters.
                    """
                    value: Optional[Literal["disabled", "enabled"]]
                    """
                    The toggle's collected value. Can be `enabled` or `disabled`.
                    """

                custom_text: Optional[CustomText]
                """
                Default text of input being collected.
                """
                email: Optional[Email]
                """
                Information about a email being collected using a reader
                """
                numeric: Optional[Numeric]
                """
                Information about a number being collected using a reader
                """
                phone: Optional[Phone]
                """
                Information about a phone number being collected using a reader
                """
                required: Optional[bool]
                """
                Indicate that this input is required, disabling the skip button.
                """
                selection: Optional[Selection]
                """
                Information about a selection being collected using a reader
                """
                signature: Optional[Signature]
                """
                Information about a signature being collected using a reader
                """
                skipped: Optional[bool]
                """
                Indicate that this input was skipped by the user.
                """
                text: Optional[Text]
                """
                Information about text being collected using a reader
                """
                toggles: Optional[List[Toggle]]
                """
                List of toggles being collected. Values are present if collection is complete.
                """
                type: Literal[
                    "email",
                    "numeric",
                    "phone",
                    "selection",
                    "signature",
                    "text",
                ]
                """
                Type of input being collected.
                """
                _inner_class_types = {
                    "custom_text": CustomText,
                    "email": Email,
                    "numeric": Numeric,
                    "phone": Phone,
                    "selection": Selection,
                    "signature": Signature,
                    "text": Text,
                    "toggles": Toggle,
                }

            inputs: List[Input]
            """
            List of inputs to be collected.
            """
            metadata: Optional[Dict[str, str]]
            """
            Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
            """
            _inner_class_types = {"inputs": Input}

        class CollectPaymentMethod(StripeObject):
            class CollectConfig(StripeObject):
                class Tipping(StripeObject):
                    amount_eligible: Optional[int]
                    """
                    Amount used to calculate tip suggestions on tipping selection screen for this transaction. Must be a positive integer in the smallest currency unit (e.g., 100 cents to represent $1.00 or 100 to represent ¥100, a zero-decimal currency).
                    """

                enable_customer_cancellation: Optional[bool]
                """
                Enable customer-initiated cancellation when processing this payment.
                """
                skip_tipping: Optional[bool]
                """
                Override showing a tipping selection screen on this transaction.
                """
                tipping: Optional[Tipping]
                """
                Represents a per-transaction tipping configuration
                """
                _inner_class_types = {"tipping": Tipping}

            collect_config: Optional[CollectConfig]
            """
            Represents a per-transaction override of a reader configuration
            """
            payment_intent: ExpandableField["PaymentIntent"]
            """
            Most recent PaymentIntent processed by the reader.
            """
            payment_method: Optional["PaymentMethod"]
            """
            PaymentMethod objects represent your customer's payment instruments.
            You can use them with [PaymentIntents](https://docs.stripe.com/payments/payment-intents) to collect payments or save them to
            Customer objects to store instrument details for future payments.

            Related guides: [Payment Methods](https://docs.stripe.com/payments/payment-methods) and [More Payment Scenarios](https://docs.stripe.com/payments/more-payment-scenarios).
            """
            _inner_class_types = {"collect_config": CollectConfig}

        class ConfirmPaymentIntent(StripeObject):
            class ConfirmConfig(StripeObject):
                return_url: Optional[str]
                """
                If the customer doesn't abandon authenticating the payment, they're redirected to this URL after completion.
                """

            confirm_config: Optional[ConfirmConfig]
            """
            Represents a per-transaction override of a reader configuration
            """
            payment_intent: ExpandableField["PaymentIntent"]
            """
            Most recent PaymentIntent processed by the reader.
            """
            _inner_class_types = {"confirm_config": ConfirmConfig}

        class ProcessPaymentIntent(StripeObject):
            class ProcessConfig(StripeObject):
                class Tipping(StripeObject):
                    amount_eligible: Optional[int]
                    """
                    Amount used to calculate tip suggestions on tipping selection screen for this transaction. Must be a positive integer in the smallest currency unit (e.g., 100 cents to represent $1.00 or 100 to represent ¥100, a zero-decimal currency).
                    """

                enable_customer_cancellation: Optional[bool]
                """
                Enable customer-initiated cancellation when processing this payment.
                """
                return_url: Optional[str]
                """
                If the customer doesn't abandon authenticating the payment, they're redirected to this URL after completion.
                """
                skip_tipping: Optional[bool]
                """
                Override showing a tipping selection screen on this transaction.
                """
                tipping: Optional[Tipping]
                """
                Represents a per-transaction tipping configuration
                """
                _inner_class_types = {"tipping": Tipping}

            payment_intent: ExpandableField["PaymentIntent"]
            """
            Most recent PaymentIntent processed by the reader.
            """
            process_config: Optional[ProcessConfig]
            """
            Represents a per-transaction override of a reader configuration
            """
            _inner_class_types = {"process_config": ProcessConfig}

        class ProcessSetupIntent(StripeObject):
            class ProcessConfig(StripeObject):
                enable_customer_cancellation: Optional[bool]
                """
                Enable customer-initiated cancellation when processing this SetupIntent.
                """

            generated_card: Optional[str]
            """
            ID of a card PaymentMethod generated from the card_present PaymentMethod that may be attached to a Customer for future transactions. Only present if it was possible to generate a card PaymentMethod.
            """
            process_config: Optional[ProcessConfig]
            """
            Represents a per-setup override of a reader configuration
            """
            setup_intent: ExpandableField["SetupIntent"]
            """
            Most recent SetupIntent processed by the reader.
            """
            _inner_class_types = {"process_config": ProcessConfig}

        class RefundPayment(StripeObject):
            class RefundPaymentConfig(StripeObject):
                enable_customer_cancellation: Optional[bool]
                """
                Enable customer-initiated cancellation when refunding this payment.
                """

            amount: Optional[int]
            """
            The amount being refunded.
            """
            charge: Optional[ExpandableField["Charge"]]
            """
            Charge that is being refunded.
            """
            metadata: Optional[Dict[str, str]]
            """
            Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
            """
            payment_intent: Optional[ExpandableField["PaymentIntent"]]
            """
            Payment intent that is being refunded.
            """
            reason: Optional[
                Literal["duplicate", "fraudulent", "requested_by_customer"]
            ]
            """
            The reason for the refund.
            """
            refund: Optional[ExpandableField["Refund"]]
            """
            Unique identifier for the refund object.
            """
            refund_application_fee: Optional[bool]
            """
            Boolean indicating whether the application fee should be refunded when refunding this charge. If a full charge refund is given, the full application fee will be refunded. Otherwise, the application fee will be refunded in an amount proportional to the amount of the charge refunded. An application fee can be refunded only by the application that created the charge.
            """
            refund_payment_config: Optional[RefundPaymentConfig]
            """
            Represents a per-transaction override of a reader configuration
            """
            reverse_transfer: Optional[bool]
            """
            Boolean indicating whether the transfer should be reversed when refunding this charge. The transfer will be reversed proportionally to the amount being refunded (either the entire or partial amount). A transfer can be reversed only by the application that created the charge.
            """
            _inner_class_types = {"refund_payment_config": RefundPaymentConfig}

        class SetReaderDisplay(StripeObject):
            class Cart(StripeObject):
                class LineItem(StripeObject):
                    amount: int
                    """
                    The amount of the line item. A positive integer in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
                    """
                    description: str
                    """
                    Description of the line item.
                    """
                    quantity: int
                    """
                    The quantity of the line item.
                    """

                currency: str
                """
                Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
                """
                line_items: List[LineItem]
                """
                List of line items in the cart.
                """
                tax: Optional[int]
                """
                Tax amount for the entire cart. A positive integer in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
                """
                total: int
                """
                Total amount for the entire cart, including tax. A positive integer in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
                """
                _inner_class_types = {"line_items": LineItem}

            cart: Optional[Cart]
            """
            Cart object to be displayed by the reader, including line items, amounts, and currency.
            """
            type: Literal["cart"]
            """
            Type of information to be displayed by the reader. Only `cart` is currently supported.
            """
            _inner_class_types = {"cart": Cart}

        collect_inputs: Optional[CollectInputs]
        """
        Represents a reader action to collect customer inputs
        """
        collect_payment_method: Optional[CollectPaymentMethod]
        """
        Represents a reader action to collect a payment method
        """
        confirm_payment_intent: Optional[ConfirmPaymentIntent]
        """
        Represents a reader action to confirm a payment
        """
        failure_code: Optional[str]
        """
        Failure code, only set if status is `failed`.
        """
        failure_message: Optional[str]
        """
        Detailed failure message, only set if status is `failed`.
        """
        process_payment_intent: Optional[ProcessPaymentIntent]
        """
        Represents a reader action to process a payment intent
        """
        process_setup_intent: Optional[ProcessSetupIntent]
        """
        Represents a reader action to process a setup intent
        """
        refund_payment: Optional[RefundPayment]
        """
        Represents a reader action to refund a payment
        """
        set_reader_display: Optional[SetReaderDisplay]
        """
        Represents a reader action to set the reader display
        """
        status: Literal["failed", "in_progress", "succeeded"]
        """
        Status of the action performed by the reader.
        """
        type: Literal[
            "collect_inputs",
            "collect_payment_method",
            "confirm_payment_intent",
            "process_payment_intent",
            "process_setup_intent",
            "refund_payment",
            "set_reader_display",
        ]
        """
        Type of action performed by the reader.
        """
        _inner_class_types = {
            "collect_inputs": CollectInputs,
            "collect_payment_method": CollectPaymentMethod,
            "confirm_payment_intent": ConfirmPaymentIntent,
            "process_payment_intent": ProcessPaymentIntent,
            "process_setup_intent": ProcessSetupIntent,
            "refund_payment": RefundPayment,
            "set_reader_display": SetReaderDisplay,
        }

    action: Optional[Action]
    """
    The most recent action performed by the reader.
    """
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    device_sw_version: Optional[str]
    """
    The current software version of the reader.
    """
    device_type: Literal[
        "bbpos_chipper2x",
        "bbpos_wisepad3",
        "bbpos_wisepos_e",
        "mobile_phone_reader",
        "simulated_stripe_s700",
        "simulated_stripe_s710",
        "simulated_wisepos_e",
        "stripe_m2",
        "stripe_s700",
        "stripe_s710",
        "verifone_P400",
    ]
    """
    Device type of the reader.
    """
    id: str
    """
    Unique identifier for the object.
    """
    ip_address: Optional[str]
    """
    The local IP address of the reader.
    """
    label: str
    """
    Custom label given to the reader for easier identification.
    """
    last_seen_at: Optional[int]
    """
    The last time this reader reported to Stripe backend. Timestamp is measured in milliseconds since the Unix epoch. Unlike most other Stripe timestamp fields which use seconds, this field uses milliseconds.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    location: Optional[ExpandableField["Location"]]
    """
    The location identifier of the reader.
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["terminal.reader"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    serial_number: str
    """
    Serial number of the reader.
    """
    status: Optional[Literal["offline", "online"]]
    """
    The networking status of the reader. We do not recommend using this field in flows that may block taking payments.
    """

    @classmethod
    def _cls_cancel_action(
        cls, reader: str, **params: Unpack["ReaderCancelActionParams"]
    ) -> "Reader":
        """
        Cancels the current reader action. See [Programmatic Cancellation](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven#programmatic-cancellation) for more details.
        """
        return cast(
            "Reader",
            cls._static_request(
                "post",
                "/v1/terminal/readers/{reader}/cancel_action".format(
                    reader=sanitize_id(reader)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def cancel_action(
        reader: str, **params: Unpack["ReaderCancelActionParams"]
    ) -> "Reader":
        """
        Cancels the current reader action. See [Programmatic Cancellation](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven#programmatic-cancellation) for more details.
        """
        ...

    @overload
    def cancel_action(
        self, **params: Unpack["ReaderCancelActionParams"]
    ) -> "Reader":
        """
        Cancels the current reader action. See [Programmatic Cancellation](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven#programmatic-cancellation) for more details.
        """
        ...

    @class_method_variant("_cls_cancel_action")
    def cancel_action(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ReaderCancelActionParams"]
    ) -> "Reader":
        """
        Cancels the current reader action. See [Programmatic Cancellation](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven#programmatic-cancellation) for more details.
        """
        return cast(
            "Reader",
            self._request(
                "post",
                "/v1/terminal/readers/{reader}/cancel_action".format(
                    reader=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_cancel_action_async(
        cls, reader: str, **params: Unpack["ReaderCancelActionParams"]
    ) -> "Reader":
        """
        Cancels the current reader action. See [Programmatic Cancellation](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven#programmatic-cancellation) for more details.
        """
        return cast(
            "Reader",
            await cls._static_request_async(
                "post",
                "/v1/terminal/readers/{reader}/cancel_action".format(
                    reader=sanitize_id(reader)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def cancel_action_async(
        reader: str, **params: Unpack["ReaderCancelActionParams"]
    ) -> "Reader":
        """
        Cancels the current reader action. See [Programmatic Cancellation](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven#programmatic-cancellation) for more details.
        """
        ...

    @overload
    async def cancel_action_async(
        self, **params: Unpack["ReaderCancelActionParams"]
    ) -> "Reader":
        """
        Cancels the current reader action. See [Programmatic Cancellation](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven#programmatic-cancellation) for more details.
        """
        ...

    @class_method_variant("_cls_cancel_action_async")
    async def cancel_action_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ReaderCancelActionParams"]
    ) -> "Reader":
        """
        Cancels the current reader action. See [Programmatic Cancellation](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven#programmatic-cancellation) for more details.
        """
        return cast(
            "Reader",
            await self._request_async(
                "post",
                "/v1/terminal/readers/{reader}/cancel_action".format(
                    reader=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_collect_inputs(
        cls, reader: str, **params: Unpack["ReaderCollectInputsParams"]
    ) -> "Reader":
        """
        Initiates an [input collection flow](https://docs.stripe.com/docs/terminal/features/collect-inputs) on a Reader to display input forms and collect information from your customers.
        """
        return cast(
            "Reader",
            cls._static_request(
                "post",
                "/v1/terminal/readers/{reader}/collect_inputs".format(
                    reader=sanitize_id(reader)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def collect_inputs(
        reader: str, **params: Unpack["ReaderCollectInputsParams"]
    ) -> "Reader":
        """
        Initiates an [input collection flow](https://docs.stripe.com/docs/terminal/features/collect-inputs) on a Reader to display input forms and collect information from your customers.
        """
        ...

    @overload
    def collect_inputs(
        self, **params: Unpack["ReaderCollectInputsParams"]
    ) -> "Reader":
        """
        Initiates an [input collection flow](https://docs.stripe.com/docs/terminal/features/collect-inputs) on a Reader to display input forms and collect information from your customers.
        """
        ...

    @class_method_variant("_cls_collect_inputs")
    def collect_inputs(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ReaderCollectInputsParams"]
    ) -> "Reader":
        """
        Initiates an [input collection flow](https://docs.stripe.com/docs/terminal/features/collect-inputs) on a Reader to display input forms and collect information from your customers.
        """
        return cast(
            "Reader",
            self._request(
                "post",
                "/v1/terminal/readers/{reader}/collect_inputs".format(
                    reader=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_collect_inputs_async(
        cls, reader: str, **params: Unpack["ReaderCollectInputsParams"]
    ) -> "Reader":
        """
        Initiates an [input collection flow](https://docs.stripe.com/docs/terminal/features/collect-inputs) on a Reader to display input forms and collect information from your customers.
        """
        return cast(
            "Reader",
            await cls._static_request_async(
                "post",
                "/v1/terminal/readers/{reader}/collect_inputs".format(
                    reader=sanitize_id(reader)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def collect_inputs_async(
        reader: str, **params: Unpack["ReaderCollectInputsParams"]
    ) -> "Reader":
        """
        Initiates an [input collection flow](https://docs.stripe.com/docs/terminal/features/collect-inputs) on a Reader to display input forms and collect information from your customers.
        """
        ...

    @overload
    async def collect_inputs_async(
        self, **params: Unpack["ReaderCollectInputsParams"]
    ) -> "Reader":
        """
        Initiates an [input collection flow](https://docs.stripe.com/docs/terminal/features/collect-inputs) on a Reader to display input forms and collect information from your customers.
        """
        ...

    @class_method_variant("_cls_collect_inputs_async")
    async def collect_inputs_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ReaderCollectInputsParams"]
    ) -> "Reader":
        """
        Initiates an [input collection flow](https://docs.stripe.com/docs/terminal/features/collect-inputs) on a Reader to display input forms and collect information from your customers.
        """
        return cast(
            "Reader",
            await self._request_async(
                "post",
                "/v1/terminal/readers/{reader}/collect_inputs".format(
                    reader=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_collect_payment_method(
        cls, reader: str, **params: Unpack["ReaderCollectPaymentMethodParams"]
    ) -> "Reader":
        """
        Initiates a payment flow on a Reader and updates the PaymentIntent with card details before manual confirmation. See [Collecting a Payment method](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=inspect#collect-a-paymentmethod) for more details.
        """
        return cast(
            "Reader",
            cls._static_request(
                "post",
                "/v1/terminal/readers/{reader}/collect_payment_method".format(
                    reader=sanitize_id(reader)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def collect_payment_method(
        reader: str, **params: Unpack["ReaderCollectPaymentMethodParams"]
    ) -> "Reader":
        """
        Initiates a payment flow on a Reader and updates the PaymentIntent with card details before manual confirmation. See [Collecting a Payment method](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=inspect#collect-a-paymentmethod) for more details.
        """
        ...

    @overload
    def collect_payment_method(
        self, **params: Unpack["ReaderCollectPaymentMethodParams"]
    ) -> "Reader":
        """
        Initiates a payment flow on a Reader and updates the PaymentIntent with card details before manual confirmation. See [Collecting a Payment method](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=inspect#collect-a-paymentmethod) for more details.
        """
        ...

    @class_method_variant("_cls_collect_payment_method")
    def collect_payment_method(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ReaderCollectPaymentMethodParams"]
    ) -> "Reader":
        """
        Initiates a payment flow on a Reader and updates the PaymentIntent with card details before manual confirmation. See [Collecting a Payment method](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=inspect#collect-a-paymentmethod) for more details.
        """
        return cast(
            "Reader",
            self._request(
                "post",
                "/v1/terminal/readers/{reader}/collect_payment_method".format(
                    reader=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_collect_payment_method_async(
        cls, reader: str, **params: Unpack["ReaderCollectPaymentMethodParams"]
    ) -> "Reader":
        """
        Initiates a payment flow on a Reader and updates the PaymentIntent with card details before manual confirmation. See [Collecting a Payment method](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=inspect#collect-a-paymentmethod) for more details.
        """
        return cast(
            "Reader",
            await cls._static_request_async(
                "post",
                "/v1/terminal/readers/{reader}/collect_payment_method".format(
                    reader=sanitize_id(reader)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def collect_payment_method_async(
        reader: str, **params: Unpack["ReaderCollectPaymentMethodParams"]
    ) -> "Reader":
        """
        Initiates a payment flow on a Reader and updates the PaymentIntent with card details before manual confirmation. See [Collecting a Payment method](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=inspect#collect-a-paymentmethod) for more details.
        """
        ...

    @overload
    async def collect_payment_method_async(
        self, **params: Unpack["ReaderCollectPaymentMethodParams"]
    ) -> "Reader":
        """
        Initiates a payment flow on a Reader and updates the PaymentIntent with card details before manual confirmation. See [Collecting a Payment method](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=inspect#collect-a-paymentmethod) for more details.
        """
        ...

    @class_method_variant("_cls_collect_payment_method_async")
    async def collect_payment_method_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ReaderCollectPaymentMethodParams"]
    ) -> "Reader":
        """
        Initiates a payment flow on a Reader and updates the PaymentIntent with card details before manual confirmation. See [Collecting a Payment method](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=inspect#collect-a-paymentmethod) for more details.
        """
        return cast(
            "Reader",
            await self._request_async(
                "post",
                "/v1/terminal/readers/{reader}/collect_payment_method".format(
                    reader=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_confirm_payment_intent(
        cls, reader: str, **params: Unpack["ReaderConfirmPaymentIntentParams"]
    ) -> "Reader":
        """
        Finalizes a payment on a Reader. See [Confirming a Payment](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=inspect#confirm-the-paymentintent) for more details.
        """
        return cast(
            "Reader",
            cls._static_request(
                "post",
                "/v1/terminal/readers/{reader}/confirm_payment_intent".format(
                    reader=sanitize_id(reader)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def confirm_payment_intent(
        reader: str, **params: Unpack["ReaderConfirmPaymentIntentParams"]
    ) -> "Reader":
        """
        Finalizes a payment on a Reader. See [Confirming a Payment](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=inspect#confirm-the-paymentintent) for more details.
        """
        ...

    @overload
    def confirm_payment_intent(
        self, **params: Unpack["ReaderConfirmPaymentIntentParams"]
    ) -> "Reader":
        """
        Finalizes a payment on a Reader. See [Confirming a Payment](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=inspect#confirm-the-paymentintent) for more details.
        """
        ...

    @class_method_variant("_cls_confirm_payment_intent")
    def confirm_payment_intent(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ReaderConfirmPaymentIntentParams"]
    ) -> "Reader":
        """
        Finalizes a payment on a Reader. See [Confirming a Payment](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=inspect#confirm-the-paymentintent) for more details.
        """
        return cast(
            "Reader",
            self._request(
                "post",
                "/v1/terminal/readers/{reader}/confirm_payment_intent".format(
                    reader=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_confirm_payment_intent_async(
        cls, reader: str, **params: Unpack["ReaderConfirmPaymentIntentParams"]
    ) -> "Reader":
        """
        Finalizes a payment on a Reader. See [Confirming a Payment](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=inspect#confirm-the-paymentintent) for more details.
        """
        return cast(
            "Reader",
            await cls._static_request_async(
                "post",
                "/v1/terminal/readers/{reader}/confirm_payment_intent".format(
                    reader=sanitize_id(reader)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def confirm_payment_intent_async(
        reader: str, **params: Unpack["ReaderConfirmPaymentIntentParams"]
    ) -> "Reader":
        """
        Finalizes a payment on a Reader. See [Confirming a Payment](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=inspect#confirm-the-paymentintent) for more details.
        """
        ...

    @overload
    async def confirm_payment_intent_async(
        self, **params: Unpack["ReaderConfirmPaymentIntentParams"]
    ) -> "Reader":
        """
        Finalizes a payment on a Reader. See [Confirming a Payment](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=inspect#confirm-the-paymentintent) for more details.
        """
        ...

    @class_method_variant("_cls_confirm_payment_intent_async")
    async def confirm_payment_intent_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ReaderConfirmPaymentIntentParams"]
    ) -> "Reader":
        """
        Finalizes a payment on a Reader. See [Confirming a Payment](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=inspect#confirm-the-paymentintent) for more details.
        """
        return cast(
            "Reader",
            await self._request_async(
                "post",
                "/v1/terminal/readers/{reader}/confirm_payment_intent".format(
                    reader=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def create(cls, **params: Unpack["ReaderCreateParams"]) -> "Reader":
        """
        Creates a new Reader object.
        """
        return cast(
            "Reader",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["ReaderCreateParams"]
    ) -> "Reader":
        """
        Creates a new Reader object.
        """
        return cast(
            "Reader",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def _cls_delete(
        cls, sid: str, **params: Unpack["ReaderDeleteParams"]
    ) -> "Reader":
        """
        Deletes a Reader object.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "Reader",
            cls._static_request(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    def delete(sid: str, **params: Unpack["ReaderDeleteParams"]) -> "Reader":
        """
        Deletes a Reader object.
        """
        ...

    @overload
    def delete(self, **params: Unpack["ReaderDeleteParams"]) -> "Reader":
        """
        Deletes a Reader object.
        """
        ...

    @class_method_variant("_cls_delete")
    def delete(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ReaderDeleteParams"]
    ) -> "Reader":
        """
        Deletes a Reader object.
        """
        return self._request_and_refresh(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    async def _cls_delete_async(
        cls, sid: str, **params: Unpack["ReaderDeleteParams"]
    ) -> "Reader":
        """
        Deletes a Reader object.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "Reader",
            await cls._static_request_async(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def delete_async(
        sid: str, **params: Unpack["ReaderDeleteParams"]
    ) -> "Reader":
        """
        Deletes a Reader object.
        """
        ...

    @overload
    async def delete_async(
        self, **params: Unpack["ReaderDeleteParams"]
    ) -> "Reader":
        """
        Deletes a Reader object.
        """
        ...

    @class_method_variant("_cls_delete_async")
    async def delete_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ReaderDeleteParams"]
    ) -> "Reader":
        """
        Deletes a Reader object.
        """
        return await self._request_and_refresh_async(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    def list(
        cls, **params: Unpack["ReaderListParams"]
    ) -> ListObject["Reader"]:
        """
        Returns a list of Reader objects.
        """
        result = cls._static_request(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    async def list_async(
        cls, **params: Unpack["ReaderListParams"]
    ) -> ListObject["Reader"]:
        """
        Returns a list of Reader objects.
        """
        result = await cls._static_request_async(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    def modify(
        cls, id: str, **params: Unpack["ReaderModifyParams"]
    ) -> "Reader":
        """
        Updates a Reader object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Reader",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["ReaderModifyParams"]
    ) -> "Reader":
        """
        Updates a Reader object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Reader",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def _cls_process_payment_intent(
        cls, reader: str, **params: Unpack["ReaderProcessPaymentIntentParams"]
    ) -> "Reader":
        """
        Initiates a payment flow on a Reader. See [process the payment](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=immediately#process-payment) for more details.
        """
        return cast(
            "Reader",
            cls._static_request(
                "post",
                "/v1/terminal/readers/{reader}/process_payment_intent".format(
                    reader=sanitize_id(reader)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def process_payment_intent(
        reader: str, **params: Unpack["ReaderProcessPaymentIntentParams"]
    ) -> "Reader":
        """
        Initiates a payment flow on a Reader. See [process the payment](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=immediately#process-payment) for more details.
        """
        ...

    @overload
    def process_payment_intent(
        self, **params: Unpack["ReaderProcessPaymentIntentParams"]
    ) -> "Reader":
        """
        Initiates a payment flow on a Reader. See [process the payment](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=immediately#process-payment) for more details.
        """
        ...

    @class_method_variant("_cls_process_payment_intent")
    def process_payment_intent(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ReaderProcessPaymentIntentParams"]
    ) -> "Reader":
        """
        Initiates a payment flow on a Reader. See [process the payment](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=immediately#process-payment) for more details.
        """
        return cast(
            "Reader",
            self._request(
                "post",
                "/v1/terminal/readers/{reader}/process_payment_intent".format(
                    reader=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_process_payment_intent_async(
        cls, reader: str, **params: Unpack["ReaderProcessPaymentIntentParams"]
    ) -> "Reader":
        """
        Initiates a payment flow on a Reader. See [process the payment](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=immediately#process-payment) for more details.
        """
        return cast(
            "Reader",
            await cls._static_request_async(
                "post",
                "/v1/terminal/readers/{reader}/process_payment_intent".format(
                    reader=sanitize_id(reader)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def process_payment_intent_async(
        reader: str, **params: Unpack["ReaderProcessPaymentIntentParams"]
    ) -> "Reader":
        """
        Initiates a payment flow on a Reader. See [process the payment](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=immediately#process-payment) for more details.
        """
        ...

    @overload
    async def process_payment_intent_async(
        self, **params: Unpack["ReaderProcessPaymentIntentParams"]
    ) -> "Reader":
        """
        Initiates a payment flow on a Reader. See [process the payment](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=immediately#process-payment) for more details.
        """
        ...

    @class_method_variant("_cls_process_payment_intent_async")
    async def process_payment_intent_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ReaderProcessPaymentIntentParams"]
    ) -> "Reader":
        """
        Initiates a payment flow on a Reader. See [process the payment](https://docs.stripe.com/docs/terminal/payments/collect-card-payment?terminal-sdk-platform=server-driven&process=immediately#process-payment) for more details.
        """
        return cast(
            "Reader",
            await self._request_async(
                "post",
                "/v1/terminal/readers/{reader}/process_payment_intent".format(
                    reader=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_process_setup_intent(
        cls, reader: str, **params: Unpack["ReaderProcessSetupIntentParams"]
    ) -> "Reader":
        """
        Initiates a SetupIntent flow on a Reader. See [Save directly without charging](https://docs.stripe.com/docs/terminal/features/saving-payment-details/save-directly) for more details.
        """
        return cast(
            "Reader",
            cls._static_request(
                "post",
                "/v1/terminal/readers/{reader}/process_setup_intent".format(
                    reader=sanitize_id(reader)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def process_setup_intent(
        reader: str, **params: Unpack["ReaderProcessSetupIntentParams"]
    ) -> "Reader":
        """
        Initiates a SetupIntent flow on a Reader. See [Save directly without charging](https://docs.stripe.com/docs/terminal/features/saving-payment-details/save-directly) for more details.
        """
        ...

    @overload
    def process_setup_intent(
        self, **params: Unpack["ReaderProcessSetupIntentParams"]
    ) -> "Reader":
        """
        Initiates a SetupIntent flow on a Reader. See [Save directly without charging](https://docs.stripe.com/docs/terminal/features/saving-payment-details/save-directly) for more details.
        """
        ...

    @class_method_variant("_cls_process_setup_intent")
    def process_setup_intent(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ReaderProcessSetupIntentParams"]
    ) -> "Reader":
        """
        Initiates a SetupIntent flow on a Reader. See [Save directly without charging](https://docs.stripe.com/docs/terminal/features/saving-payment-details/save-directly) for more details.
        """
        return cast(
            "Reader",
            self._request(
                "post",
                "/v1/terminal/readers/{reader}/process_setup_intent".format(
                    reader=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_process_setup_intent_async(
        cls, reader: str, **params: Unpack["ReaderProcessSetupIntentParams"]
    ) -> "Reader":
        """
        Initiates a SetupIntent flow on a Reader. See [Save directly without charging](https://docs.stripe.com/docs/terminal/features/saving-payment-details/save-directly) for more details.
        """
        return cast(
            "Reader",
            await cls._static_request_async(
                "post",
                "/v1/terminal/readers/{reader}/process_setup_intent".format(
                    reader=sanitize_id(reader)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def process_setup_intent_async(
        reader: str, **params: Unpack["ReaderProcessSetupIntentParams"]
    ) -> "Reader":
        """
        Initiates a SetupIntent flow on a Reader. See [Save directly without charging](https://docs.stripe.com/docs/terminal/features/saving-payment-details/save-directly) for more details.
        """
        ...

    @overload
    async def process_setup_intent_async(
        self, **params: Unpack["ReaderProcessSetupIntentParams"]
    ) -> "Reader":
        """
        Initiates a SetupIntent flow on a Reader. See [Save directly without charging](https://docs.stripe.com/docs/terminal/features/saving-payment-details/save-directly) for more details.
        """
        ...

    @class_method_variant("_cls_process_setup_intent_async")
    async def process_setup_intent_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ReaderProcessSetupIntentParams"]
    ) -> "Reader":
        """
        Initiates a SetupIntent flow on a Reader. See [Save directly without charging](https://docs.stripe.com/docs/terminal/features/saving-payment-details/save-directly) for more details.
        """
        return cast(
            "Reader",
            await self._request_async(
                "post",
                "/v1/terminal/readers/{reader}/process_setup_intent".format(
                    reader=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_refund_payment(
        cls, reader: str, **params: Unpack["ReaderRefundPaymentParams"]
    ) -> "Reader":
        """
        Initiates an in-person refund on a Reader. See [Refund an Interac Payment](https://docs.stripe.com/docs/terminal/payments/regional?integration-country=CA#refund-an-interac-payment) for more details.
        """
        return cast(
            "Reader",
            cls._static_request(
                "post",
                "/v1/terminal/readers/{reader}/refund_payment".format(
                    reader=sanitize_id(reader)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def refund_payment(
        reader: str, **params: Unpack["ReaderRefundPaymentParams"]
    ) -> "Reader":
        """
        Initiates an in-person refund on a Reader. See [Refund an Interac Payment](https://docs.stripe.com/docs/terminal/payments/regional?integration-country=CA#refund-an-interac-payment) for more details.
        """
        ...

    @overload
    def refund_payment(
        self, **params: Unpack["ReaderRefundPaymentParams"]
    ) -> "Reader":
        """
        Initiates an in-person refund on a Reader. See [Refund an Interac Payment](https://docs.stripe.com/docs/terminal/payments/regional?integration-country=CA#refund-an-interac-payment) for more details.
        """
        ...

    @class_method_variant("_cls_refund_payment")
    def refund_payment(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ReaderRefundPaymentParams"]
    ) -> "Reader":
        """
        Initiates an in-person refund on a Reader. See [Refund an Interac Payment](https://docs.stripe.com/docs/terminal/payments/regional?integration-country=CA#refund-an-interac-payment) for more details.
        """
        return cast(
            "Reader",
            self._request(
                "post",
                "/v1/terminal/readers/{reader}/refund_payment".format(
                    reader=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_refund_payment_async(
        cls, reader: str, **params: Unpack["ReaderRefundPaymentParams"]
    ) -> "Reader":
        """
        Initiates an in-person refund on a Reader. See [Refund an Interac Payment](https://docs.stripe.com/docs/terminal/payments/regional?integration-country=CA#refund-an-interac-payment) for more details.
        """
        return cast(
            "Reader",
            await cls._static_request_async(
                "post",
                "/v1/terminal/readers/{reader}/refund_payment".format(
                    reader=sanitize_id(reader)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def refund_payment_async(
        reader: str, **params: Unpack["ReaderRefundPaymentParams"]
    ) -> "Reader":
        """
        Initiates an in-person refund on a Reader. See [Refund an Interac Payment](https://docs.stripe.com/docs/terminal/payments/regional?integration-country=CA#refund-an-interac-payment) for more details.
        """
        ...

    @overload
    async def refund_payment_async(
        self, **params: Unpack["ReaderRefundPaymentParams"]
    ) -> "Reader":
        """
        Initiates an in-person refund on a Reader. See [Refund an Interac Payment](https://docs.stripe.com/docs/terminal/payments/regional?integration-country=CA#refund-an-interac-payment) for more details.
        """
        ...

    @class_method_variant("_cls_refund_payment_async")
    async def refund_payment_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ReaderRefundPaymentParams"]
    ) -> "Reader":
        """
        Initiates an in-person refund on a Reader. See [Refund an Interac Payment](https://docs.stripe.com/docs/terminal/payments/regional?integration-country=CA#refund-an-interac-payment) for more details.
        """
        return cast(
            "Reader",
            await self._request_async(
                "post",
                "/v1/terminal/readers/{reader}/refund_payment".format(
                    reader=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["ReaderRetrieveParams"]
    ) -> "Reader":
        """
        Retrieves a Reader object.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["ReaderRetrieveParams"]
    ) -> "Reader":
        """
        Retrieves a Reader object.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    @classmethod
    def _cls_set_reader_display(
        cls, reader: str, **params: Unpack["ReaderSetReaderDisplayParams"]
    ) -> "Reader":
        """
        Sets the reader display to show [cart details](https://docs.stripe.com/docs/terminal/features/display).
        """
        return cast(
            "Reader",
            cls._static_request(
                "post",
                "/v1/terminal/readers/{reader}/set_reader_display".format(
                    reader=sanitize_id(reader)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def set_reader_display(
        reader: str, **params: Unpack["ReaderSetReaderDisplayParams"]
    ) -> "Reader":
        """
        Sets the reader display to show [cart details](https://docs.stripe.com/docs/terminal/features/display).
        """
        ...

    @overload
    def set_reader_display(
        self, **params: Unpack["ReaderSetReaderDisplayParams"]
    ) -> "Reader":
        """
        Sets the reader display to show [cart details](https://docs.stripe.com/docs/terminal/features/display).
        """
        ...

    @class_method_variant("_cls_set_reader_display")
    def set_reader_display(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ReaderSetReaderDisplayParams"]
    ) -> "Reader":
        """
        Sets the reader display to show [cart details](https://docs.stripe.com/docs/terminal/features/display).
        """
        return cast(
            "Reader",
            self._request(
                "post",
                "/v1/terminal/readers/{reader}/set_reader_display".format(
                    reader=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_set_reader_display_async(
        cls, reader: str, **params: Unpack["ReaderSetReaderDisplayParams"]
    ) -> "Reader":
        """
        Sets the reader display to show [cart details](https://docs.stripe.com/docs/terminal/features/display).
        """
        return cast(
            "Reader",
            await cls._static_request_async(
                "post",
                "/v1/terminal/readers/{reader}/set_reader_display".format(
                    reader=sanitize_id(reader)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def set_reader_display_async(
        reader: str, **params: Unpack["ReaderSetReaderDisplayParams"]
    ) -> "Reader":
        """
        Sets the reader display to show [cart details](https://docs.stripe.com/docs/terminal/features/display).
        """
        ...

    @overload
    async def set_reader_display_async(
        self, **params: Unpack["ReaderSetReaderDisplayParams"]
    ) -> "Reader":
        """
        Sets the reader display to show [cart details](https://docs.stripe.com/docs/terminal/features/display).
        """
        ...

    @class_method_variant("_cls_set_reader_display_async")
    async def set_reader_display_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ReaderSetReaderDisplayParams"]
    ) -> "Reader":
        """
        Sets the reader display to show [cart details](https://docs.stripe.com/docs/terminal/features/display).
        """
        return cast(
            "Reader",
            await self._request_async(
                "post",
                "/v1/terminal/readers/{reader}/set_reader_display".format(
                    reader=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    class TestHelpers(APIResourceTestHelpers["Reader"]):
        _resource_cls: Type["Reader"]

        @classmethod
        def _cls_present_payment_method(
            cls,
            reader: str,
            **params: Unpack["ReaderPresentPaymentMethodParams"],
        ) -> "Reader":
            """
            Presents a payment method on a simulated reader. Can be used to simulate accepting a payment, saving a card or refunding a transaction.
            """
            return cast(
                "Reader",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/terminal/readers/{reader}/present_payment_method".format(
                        reader=sanitize_id(reader)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def present_payment_method(
            reader: str, **params: Unpack["ReaderPresentPaymentMethodParams"]
        ) -> "Reader":
            """
            Presents a payment method on a simulated reader. Can be used to simulate accepting a payment, saving a card or refunding a transaction.
            """
            ...

        @overload
        def present_payment_method(
            self, **params: Unpack["ReaderPresentPaymentMethodParams"]
        ) -> "Reader":
            """
            Presents a payment method on a simulated reader. Can be used to simulate accepting a payment, saving a card or refunding a transaction.
            """
            ...

        @class_method_variant("_cls_present_payment_method")
        def present_payment_method(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["ReaderPresentPaymentMethodParams"]
        ) -> "Reader":
            """
            Presents a payment method on a simulated reader. Can be used to simulate accepting a payment, saving a card or refunding a transaction.
            """
            return cast(
                "Reader",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/terminal/readers/{reader}/present_payment_method".format(
                        reader=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_present_payment_method_async(
            cls,
            reader: str,
            **params: Unpack["ReaderPresentPaymentMethodParams"],
        ) -> "Reader":
            """
            Presents a payment method on a simulated reader. Can be used to simulate accepting a payment, saving a card or refunding a transaction.
            """
            return cast(
                "Reader",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/terminal/readers/{reader}/present_payment_method".format(
                        reader=sanitize_id(reader)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def present_payment_method_async(
            reader: str, **params: Unpack["ReaderPresentPaymentMethodParams"]
        ) -> "Reader":
            """
            Presents a payment method on a simulated reader. Can be used to simulate accepting a payment, saving a card or refunding a transaction.
            """
            ...

        @overload
        async def present_payment_method_async(
            self, **params: Unpack["ReaderPresentPaymentMethodParams"]
        ) -> "Reader":
            """
            Presents a payment method on a simulated reader. Can be used to simulate accepting a payment, saving a card or refunding a transaction.
            """
            ...

        @class_method_variant("_cls_present_payment_method_async")
        async def present_payment_method_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["ReaderPresentPaymentMethodParams"]
        ) -> "Reader":
            """
            Presents a payment method on a simulated reader. Can be used to simulate accepting a payment, saving a card or refunding a transaction.
            """
            return cast(
                "Reader",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/terminal/readers/{reader}/present_payment_method".format(
                        reader=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        def _cls_succeed_input_collection(
            cls,
            reader: str,
            **params: Unpack["ReaderSucceedInputCollectionParams"],
        ) -> "Reader":
            """
            Use this endpoint to trigger a successful input collection on a simulated reader.
            """
            return cast(
                "Reader",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/terminal/readers/{reader}/succeed_input_collection".format(
                        reader=sanitize_id(reader)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def succeed_input_collection(
            reader: str, **params: Unpack["ReaderSucceedInputCollectionParams"]
        ) -> "Reader":
            """
            Use this endpoint to trigger a successful input collection on a simulated reader.
            """
            ...

        @overload
        def succeed_input_collection(
            self, **params: Unpack["ReaderSucceedInputCollectionParams"]
        ) -> "Reader":
            """
            Use this endpoint to trigger a successful input collection on a simulated reader.
            """
            ...

        @class_method_variant("_cls_succeed_input_collection")
        def succeed_input_collection(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["ReaderSucceedInputCollectionParams"]
        ) -> "Reader":
            """
            Use this endpoint to trigger a successful input collection on a simulated reader.
            """
            return cast(
                "Reader",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/terminal/readers/{reader}/succeed_input_collection".format(
                        reader=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_succeed_input_collection_async(
            cls,
            reader: str,
            **params: Unpack["ReaderSucceedInputCollectionParams"],
        ) -> "Reader":
            """
            Use this endpoint to trigger a successful input collection on a simulated reader.
            """
            return cast(
                "Reader",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/terminal/readers/{reader}/succeed_input_collection".format(
                        reader=sanitize_id(reader)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def succeed_input_collection_async(
            reader: str, **params: Unpack["ReaderSucceedInputCollectionParams"]
        ) -> "Reader":
            """
            Use this endpoint to trigger a successful input collection on a simulated reader.
            """
            ...

        @overload
        async def succeed_input_collection_async(
            self, **params: Unpack["ReaderSucceedInputCollectionParams"]
        ) -> "Reader":
            """
            Use this endpoint to trigger a successful input collection on a simulated reader.
            """
            ...

        @class_method_variant("_cls_succeed_input_collection_async")
        async def succeed_input_collection_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["ReaderSucceedInputCollectionParams"]
        ) -> "Reader":
            """
            Use this endpoint to trigger a successful input collection on a simulated reader.
            """
            return cast(
                "Reader",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/terminal/readers/{reader}/succeed_input_collection".format(
                        reader=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        def _cls_timeout_input_collection(
            cls,
            reader: str,
            **params: Unpack["ReaderTimeoutInputCollectionParams"],
        ) -> "Reader":
            """
            Use this endpoint to complete an input collection with a timeout error on a simulated reader.
            """
            return cast(
                "Reader",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/terminal/readers/{reader}/timeout_input_collection".format(
                        reader=sanitize_id(reader)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def timeout_input_collection(
            reader: str, **params: Unpack["ReaderTimeoutInputCollectionParams"]
        ) -> "Reader":
            """
            Use this endpoint to complete an input collection with a timeout error on a simulated reader.
            """
            ...

        @overload
        def timeout_input_collection(
            self, **params: Unpack["ReaderTimeoutInputCollectionParams"]
        ) -> "Reader":
            """
            Use this endpoint to complete an input collection with a timeout error on a simulated reader.
            """
            ...

        @class_method_variant("_cls_timeout_input_collection")
        def timeout_input_collection(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["ReaderTimeoutInputCollectionParams"]
        ) -> "Reader":
            """
            Use this endpoint to complete an input collection with a timeout error on a simulated reader.
            """
            return cast(
                "Reader",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/terminal/readers/{reader}/timeout_input_collection".format(
                        reader=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_timeout_input_collection_async(
            cls,
            reader: str,
            **params: Unpack["ReaderTimeoutInputCollectionParams"],
        ) -> "Reader":
            """
            Use this endpoint to complete an input collection with a timeout error on a simulated reader.
            """
            return cast(
                "Reader",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/terminal/readers/{reader}/timeout_input_collection".format(
                        reader=sanitize_id(reader)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def timeout_input_collection_async(
            reader: str, **params: Unpack["ReaderTimeoutInputCollectionParams"]
        ) -> "Reader":
            """
            Use this endpoint to complete an input collection with a timeout error on a simulated reader.
            """
            ...

        @overload
        async def timeout_input_collection_async(
            self, **params: Unpack["ReaderTimeoutInputCollectionParams"]
        ) -> "Reader":
            """
            Use this endpoint to complete an input collection with a timeout error on a simulated reader.
            """
            ...

        @class_method_variant("_cls_timeout_input_collection_async")
        async def timeout_input_collection_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["ReaderTimeoutInputCollectionParams"]
        ) -> "Reader":
            """
            Use this endpoint to complete an input collection with a timeout error on a simulated reader.
            """
            return cast(
                "Reader",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/terminal/readers/{reader}/timeout_input_collection".format(
                        reader=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

    @property
    def test_helpers(self):
        return self.TestHelpers(self)

    _inner_class_types = {"action": Action}


Reader.TestHelpers._resource_cls = Reader
