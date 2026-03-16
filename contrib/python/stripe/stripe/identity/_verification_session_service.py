# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.identity._verification_session import VerificationSession
    from stripe.params.identity._verification_session_cancel_params import (
        VerificationSessionCancelParams,
    )
    from stripe.params.identity._verification_session_create_params import (
        VerificationSessionCreateParams,
    )
    from stripe.params.identity._verification_session_list_params import (
        VerificationSessionListParams,
    )
    from stripe.params.identity._verification_session_redact_params import (
        VerificationSessionRedactParams,
    )
    from stripe.params.identity._verification_session_retrieve_params import (
        VerificationSessionRetrieveParams,
    )
    from stripe.params.identity._verification_session_update_params import (
        VerificationSessionUpdateParams,
    )


class VerificationSessionService(StripeService):
    def list(
        self,
        params: Optional["VerificationSessionListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[VerificationSession]":
        """
        Returns a list of VerificationSessions
        """
        return cast(
            "ListObject[VerificationSession]",
            self._request(
                "get",
                "/v1/identity/verification_sessions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["VerificationSessionListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[VerificationSession]":
        """
        Returns a list of VerificationSessions
        """
        return cast(
            "ListObject[VerificationSession]",
            await self._request_async(
                "get",
                "/v1/identity/verification_sessions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: Optional["VerificationSessionCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "VerificationSession":
        """
        Creates a VerificationSession object.

        After the VerificationSession is created, display a verification modal using the session client_secret or send your users to the session's url.

        If your API key is in test mode, verification checks won't actually process, though everything else will occur as if in live mode.

        Related guide: [Verify your users' identity documents](https://docs.stripe.com/docs/identity/verify-identity-documents)
        """
        return cast(
            "VerificationSession",
            self._request(
                "post",
                "/v1/identity/verification_sessions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["VerificationSessionCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "VerificationSession":
        """
        Creates a VerificationSession object.

        After the VerificationSession is created, display a verification modal using the session client_secret or send your users to the session's url.

        If your API key is in test mode, verification checks won't actually process, though everything else will occur as if in live mode.

        Related guide: [Verify your users' identity documents](https://docs.stripe.com/docs/identity/verify-identity-documents)
        """
        return cast(
            "VerificationSession",
            await self._request_async(
                "post",
                "/v1/identity/verification_sessions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        session: str,
        params: Optional["VerificationSessionRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "VerificationSession":
        """
        Retrieves the details of a VerificationSession that was previously created.

        When the session status is requires_input, you can use this method to retrieve a valid
        client_secret or url to allow re-submission.
        """
        return cast(
            "VerificationSession",
            self._request(
                "get",
                "/v1/identity/verification_sessions/{session}".format(
                    session=sanitize_id(session),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        session: str,
        params: Optional["VerificationSessionRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "VerificationSession":
        """
        Retrieves the details of a VerificationSession that was previously created.

        When the session status is requires_input, you can use this method to retrieve a valid
        client_secret or url to allow re-submission.
        """
        return cast(
            "VerificationSession",
            await self._request_async(
                "get",
                "/v1/identity/verification_sessions/{session}".format(
                    session=sanitize_id(session),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        session: str,
        params: Optional["VerificationSessionUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "VerificationSession":
        """
        Updates a VerificationSession object.

        When the session status is requires_input, you can use this method to update the
        verification check and options.
        """
        return cast(
            "VerificationSession",
            self._request(
                "post",
                "/v1/identity/verification_sessions/{session}".format(
                    session=sanitize_id(session),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        session: str,
        params: Optional["VerificationSessionUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "VerificationSession":
        """
        Updates a VerificationSession object.

        When the session status is requires_input, you can use this method to update the
        verification check and options.
        """
        return cast(
            "VerificationSession",
            await self._request_async(
                "post",
                "/v1/identity/verification_sessions/{session}".format(
                    session=sanitize_id(session),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def cancel(
        self,
        session: str,
        params: Optional["VerificationSessionCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "VerificationSession":
        """
        A VerificationSession object can be canceled when it is in requires_input [status](https://docs.stripe.com/docs/identity/how-sessions-work).

        Once canceled, future submission attempts are disabled. This cannot be undone. [Learn more](https://docs.stripe.com/docs/identity/verification-sessions#cancel).
        """
        return cast(
            "VerificationSession",
            self._request(
                "post",
                "/v1/identity/verification_sessions/{session}/cancel".format(
                    session=sanitize_id(session),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def cancel_async(
        self,
        session: str,
        params: Optional["VerificationSessionCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "VerificationSession":
        """
        A VerificationSession object can be canceled when it is in requires_input [status](https://docs.stripe.com/docs/identity/how-sessions-work).

        Once canceled, future submission attempts are disabled. This cannot be undone. [Learn more](https://docs.stripe.com/docs/identity/verification-sessions#cancel).
        """
        return cast(
            "VerificationSession",
            await self._request_async(
                "post",
                "/v1/identity/verification_sessions/{session}/cancel".format(
                    session=sanitize_id(session),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def redact(
        self,
        session: str,
        params: Optional["VerificationSessionRedactParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "VerificationSession":
        """
        Redact a VerificationSession to remove all collected information from Stripe. This will redact
        the VerificationSession and all objects related to it, including VerificationReports, Events,
        request logs, etc.

        A VerificationSession object can be redacted when it is in requires_input or verified
        [status](https://docs.stripe.com/docs/identity/how-sessions-work). Redacting a VerificationSession in requires_action
        state will automatically cancel it.

        The redaction process may take up to four days. When the redaction process is in progress, the
        VerificationSession's redaction.status field will be set to processing; when the process is
        finished, it will change to redacted and an identity.verification_session.redacted event
        will be emitted.

        Redaction is irreversible. Redacted objects are still accessible in the Stripe API, but all the
        fields that contain personal data will be replaced by the string [redacted] or a similar
        placeholder. The metadata field will also be erased. Redacted objects cannot be updated or
        used for any purpose.

        [Learn more](https://docs.stripe.com/docs/identity/verification-sessions#redact).
        """
        return cast(
            "VerificationSession",
            self._request(
                "post",
                "/v1/identity/verification_sessions/{session}/redact".format(
                    session=sanitize_id(session),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def redact_async(
        self,
        session: str,
        params: Optional["VerificationSessionRedactParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "VerificationSession":
        """
        Redact a VerificationSession to remove all collected information from Stripe. This will redact
        the VerificationSession and all objects related to it, including VerificationReports, Events,
        request logs, etc.

        A VerificationSession object can be redacted when it is in requires_input or verified
        [status](https://docs.stripe.com/docs/identity/how-sessions-work). Redacting a VerificationSession in requires_action
        state will automatically cancel it.

        The redaction process may take up to four days. When the redaction process is in progress, the
        VerificationSession's redaction.status field will be set to processing; when the process is
        finished, it will change to redacted and an identity.verification_session.redacted event
        will be emitted.

        Redaction is irreversible. Redacted objects are still accessible in the Stripe API, but all the
        fields that contain personal data will be replaced by the string [redacted] or a similar
        placeholder. The metadata field will also be erased. Redacted objects cannot be updated or
        used for any purpose.

        [Learn more](https://docs.stripe.com/docs/identity/verification-sessions#redact).
        """
        return cast(
            "VerificationSession",
            await self._request_async(
                "post",
                "/v1/identity/verification_sessions/{session}/redact".format(
                    session=sanitize_id(session),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
