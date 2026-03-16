from typing import Optional
from typing_extensions import TYPE_CHECKING
from stripe._util import merge_dicts
from stripe._stripe_object import StripeObject
from stripe._api_mode import ApiMode

if TYPE_CHECKING:
    from stripe._payment_intent import PaymentIntent
    from stripe._setup_intent import SetupIntent
    from stripe._source import Source
    from stripe._payment_method import PaymentMethod


class ErrorObject(StripeObject):
    charge: Optional[str]
    code: Optional[str]
    decline_code: Optional[str]
    doc_url: Optional[str]
    message: Optional[str]
    param: Optional[str]
    payment_intent: Optional["PaymentIntent"]
    payment_method: Optional["PaymentMethod"]
    setup_intent: Optional["SetupIntent"]
    source: Optional["Source"]
    type: str

    def refresh_from(
        self,
        values,
        api_key=None,
        partial=False,
        stripe_version=None,
        stripe_account=None,
        last_response=None,
        *,
        api_mode: ApiMode = "V1",
    ):
        return self._refresh_from(
            values=values,
            partial=partial,
            last_response=last_response,
            requestor=self._requestor._new_requestor_with_options(
                {
                    "api_key": api_key,
                    "stripe_version": stripe_version,
                    "stripe_account": stripe_account,
                }
            ),
            api_mode=api_mode,
        )

    def _refresh_from(
        self,
        *,
        values,
        partial=False,
        last_response=None,
        requestor,
        api_mode: ApiMode,
    ) -> None:
        # Unlike most other API resources, the API will omit attributes in
        # error objects when they have a null value. We manually set default
        # values here to facilitate generic error handling.
        values = merge_dicts(
            {
                "charge": None,
                "code": None,
                "decline_code": None,
                "doc_url": None,
                "message": None,
                "param": None,
                "payment_intent": None,
                "payment_method": None,
                "setup_intent": None,
                "source": None,
                "type": None,
            },
            values,
        )
        return super(ErrorObject, self)._refresh_from(
            values=values,
            partial=partial,
            last_response=last_response,
            requestor=requestor,
            api_mode=api_mode,
        )


class OAuthErrorObject(StripeObject):
    def refresh_from(
        self,
        values,
        api_key=None,
        partial=False,
        stripe_version=None,
        stripe_account=None,
        last_response=None,
        *,
        api_mode: ApiMode = "V1",
    ):
        return self._refresh_from(
            values=values,
            partial=partial,
            last_response=last_response,
            requestor=self._requestor._new_requestor_with_options(
                {
                    "api_key": api_key,
                    "stripe_version": stripe_version,
                    "stripe_account": stripe_account,
                }
            ),
            api_mode=api_mode,
        )

    def _refresh_from(
        self,
        *,
        values,
        partial=False,
        last_response=None,
        requestor,
        api_mode: ApiMode,
    ) -> None:
        # Unlike most other API resources, the API will omit attributes in
        # error objects when they have a null value. We manually set default
        # values here to facilitate generic error handling.
        values = merge_dicts(
            {"error": None, "error_description": None}, values
        )
        return super(OAuthErrorObject, self)._refresh_from(
            values=values,
            partial=partial,
            last_response=last_response,
            requestor=requestor,
            api_mode=api_mode,
        )
