# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from typing import ClassVar, Optional, cast
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.apps._secret_create_params import SecretCreateParams
    from stripe.params.apps._secret_delete_where_params import (
        SecretDeleteWhereParams,
    )
    from stripe.params.apps._secret_find_params import SecretFindParams
    from stripe.params.apps._secret_list_params import SecretListParams


class Secret(CreateableAPIResource["Secret"], ListableAPIResource["Secret"]):
    """
    Secret Store is an API that allows Stripe Apps developers to securely persist secrets for use by UI Extensions and app backends.

    The primary resource in Secret Store is a `secret`. Other apps can't view secrets created by an app. Additionally, secrets are scoped to provide further permission control.

    All Dashboard users and the app backend share `account` scoped secrets. Use the `account` scope for secrets that don't change per-user, like a third-party API key.

    A `user` scoped secret is accessible by the app backend and one specific Dashboard user. Use the `user` scope for per-user secrets like per-user OAuth tokens, where different users might have different permissions.

    Related guide: [Store data between page reloads](https://docs.stripe.com/stripe-apps/store-auth-data-custom-objects)
    """

    OBJECT_NAME: ClassVar[Literal["apps.secret"]] = "apps.secret"

    class Scope(StripeObject):
        type: Literal["account", "user"]
        """
        The secret scope type.
        """
        user: Optional[str]
        """
        The user ID, if type is set to "user"
        """

    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    deleted: Optional[bool]
    """
    If true, indicates that this secret has been deleted
    """
    expires_at: Optional[int]
    """
    The Unix timestamp for the expiry time of the secret, after which the secret deletes.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    name: str
    """
    A name for the secret that's unique within the scope.
    """
    object: Literal["apps.secret"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    payload: Optional[str]
    """
    The plaintext secret value to be stored.
    """
    scope: Scope

    @classmethod
    def create(cls, **params: Unpack["SecretCreateParams"]) -> "Secret":
        """
        Create or replace a secret in the secret store.
        """
        return cast(
            "Secret",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["SecretCreateParams"]
    ) -> "Secret":
        """
        Create or replace a secret in the secret store.
        """
        return cast(
            "Secret",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def delete_where(
        cls, **params: Unpack["SecretDeleteWhereParams"]
    ) -> "Secret":
        """
        Deletes a secret from the secret store by name and scope.
        """
        return cast(
            "Secret",
            cls._static_request(
                "post",
                "/v1/apps/secrets/delete",
                params=params,
            ),
        )

    @classmethod
    async def delete_where_async(
        cls, **params: Unpack["SecretDeleteWhereParams"]
    ) -> "Secret":
        """
        Deletes a secret from the secret store by name and scope.
        """
        return cast(
            "Secret",
            await cls._static_request_async(
                "post",
                "/v1/apps/secrets/delete",
                params=params,
            ),
        )

    @classmethod
    def find(cls, **params: Unpack["SecretFindParams"]) -> "Secret":
        """
        Finds a secret in the secret store by name and scope.
        """
        return cast(
            "Secret",
            cls._static_request(
                "get",
                "/v1/apps/secrets/find",
                params=params,
            ),
        )

    @classmethod
    async def find_async(
        cls, **params: Unpack["SecretFindParams"]
    ) -> "Secret":
        """
        Finds a secret in the secret store by name and scope.
        """
        return cast(
            "Secret",
            await cls._static_request_async(
                "get",
                "/v1/apps/secrets/find",
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["SecretListParams"]
    ) -> ListObject["Secret"]:
        """
        List all secrets stored on the given scope.
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
        cls, **params: Unpack["SecretListParams"]
    ) -> ListObject["Secret"]:
        """
        List all secrets stored on the given scope.
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

    _inner_class_types = {"scope": Scope}
