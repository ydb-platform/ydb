# -*- coding: utf-8 -*-

from typing_extensions import TYPE_CHECKING
from stripe.v2.core._event import (
    UnknownEventNotification as UnknownEventNotification,
)


# The beginning of the section generated from our OpenAPI spec
from importlib import import_module

if TYPE_CHECKING:
    from stripe.events._event_classes import (
        ALL_EVENT_NOTIFICATIONS as ALL_EVENT_NOTIFICATIONS,
    )
    from stripe.events._v1_billing_meter_error_report_triggered_event import (
        V1BillingMeterErrorReportTriggeredEvent as V1BillingMeterErrorReportTriggeredEvent,
        V1BillingMeterErrorReportTriggeredEventNotification as V1BillingMeterErrorReportTriggeredEventNotification,
    )
    from stripe.events._v1_billing_meter_no_meter_found_event import (
        V1BillingMeterNoMeterFoundEvent as V1BillingMeterNoMeterFoundEvent,
        V1BillingMeterNoMeterFoundEventNotification as V1BillingMeterNoMeterFoundEventNotification,
    )
    from stripe.events._v2_core_account_closed_event import (
        V2CoreAccountClosedEvent as V2CoreAccountClosedEvent,
        V2CoreAccountClosedEventNotification as V2CoreAccountClosedEventNotification,
    )
    from stripe.events._v2_core_account_created_event import (
        V2CoreAccountCreatedEvent as V2CoreAccountCreatedEvent,
        V2CoreAccountCreatedEventNotification as V2CoreAccountCreatedEventNotification,
    )
    from stripe.events._v2_core_account_including_configuration_customer_capability_status_updated_event import (
        V2CoreAccountIncludingConfigurationCustomerCapabilityStatusUpdatedEvent as V2CoreAccountIncludingConfigurationCustomerCapabilityStatusUpdatedEvent,
        V2CoreAccountIncludingConfigurationCustomerCapabilityStatusUpdatedEventNotification as V2CoreAccountIncludingConfigurationCustomerCapabilityStatusUpdatedEventNotification,
    )
    from stripe.events._v2_core_account_including_configuration_customer_updated_event import (
        V2CoreAccountIncludingConfigurationCustomerUpdatedEvent as V2CoreAccountIncludingConfigurationCustomerUpdatedEvent,
        V2CoreAccountIncludingConfigurationCustomerUpdatedEventNotification as V2CoreAccountIncludingConfigurationCustomerUpdatedEventNotification,
    )
    from stripe.events._v2_core_account_including_configuration_merchant_capability_status_updated_event import (
        V2CoreAccountIncludingConfigurationMerchantCapabilityStatusUpdatedEvent as V2CoreAccountIncludingConfigurationMerchantCapabilityStatusUpdatedEvent,
        V2CoreAccountIncludingConfigurationMerchantCapabilityStatusUpdatedEventNotification as V2CoreAccountIncludingConfigurationMerchantCapabilityStatusUpdatedEventNotification,
    )
    from stripe.events._v2_core_account_including_configuration_merchant_updated_event import (
        V2CoreAccountIncludingConfigurationMerchantUpdatedEvent as V2CoreAccountIncludingConfigurationMerchantUpdatedEvent,
        V2CoreAccountIncludingConfigurationMerchantUpdatedEventNotification as V2CoreAccountIncludingConfigurationMerchantUpdatedEventNotification,
    )
    from stripe.events._v2_core_account_including_configuration_recipient_capability_status_updated_event import (
        V2CoreAccountIncludingConfigurationRecipientCapabilityStatusUpdatedEvent as V2CoreAccountIncludingConfigurationRecipientCapabilityStatusUpdatedEvent,
        V2CoreAccountIncludingConfigurationRecipientCapabilityStatusUpdatedEventNotification as V2CoreAccountIncludingConfigurationRecipientCapabilityStatusUpdatedEventNotification,
    )
    from stripe.events._v2_core_account_including_configuration_recipient_updated_event import (
        V2CoreAccountIncludingConfigurationRecipientUpdatedEvent as V2CoreAccountIncludingConfigurationRecipientUpdatedEvent,
        V2CoreAccountIncludingConfigurationRecipientUpdatedEventNotification as V2CoreAccountIncludingConfigurationRecipientUpdatedEventNotification,
    )
    from stripe.events._v2_core_account_including_defaults_updated_event import (
        V2CoreAccountIncludingDefaultsUpdatedEvent as V2CoreAccountIncludingDefaultsUpdatedEvent,
        V2CoreAccountIncludingDefaultsUpdatedEventNotification as V2CoreAccountIncludingDefaultsUpdatedEventNotification,
    )
    from stripe.events._v2_core_account_including_future_requirements_updated_event import (
        V2CoreAccountIncludingFutureRequirementsUpdatedEvent as V2CoreAccountIncludingFutureRequirementsUpdatedEvent,
        V2CoreAccountIncludingFutureRequirementsUpdatedEventNotification as V2CoreAccountIncludingFutureRequirementsUpdatedEventNotification,
    )
    from stripe.events._v2_core_account_including_identity_updated_event import (
        V2CoreAccountIncludingIdentityUpdatedEvent as V2CoreAccountIncludingIdentityUpdatedEvent,
        V2CoreAccountIncludingIdentityUpdatedEventNotification as V2CoreAccountIncludingIdentityUpdatedEventNotification,
    )
    from stripe.events._v2_core_account_including_requirements_updated_event import (
        V2CoreAccountIncludingRequirementsUpdatedEvent as V2CoreAccountIncludingRequirementsUpdatedEvent,
        V2CoreAccountIncludingRequirementsUpdatedEventNotification as V2CoreAccountIncludingRequirementsUpdatedEventNotification,
    )
    from stripe.events._v2_core_account_link_returned_event import (
        V2CoreAccountLinkReturnedEvent as V2CoreAccountLinkReturnedEvent,
        V2CoreAccountLinkReturnedEventNotification as V2CoreAccountLinkReturnedEventNotification,
    )
    from stripe.events._v2_core_account_person_created_event import (
        V2CoreAccountPersonCreatedEvent as V2CoreAccountPersonCreatedEvent,
        V2CoreAccountPersonCreatedEventNotification as V2CoreAccountPersonCreatedEventNotification,
    )
    from stripe.events._v2_core_account_person_deleted_event import (
        V2CoreAccountPersonDeletedEvent as V2CoreAccountPersonDeletedEvent,
        V2CoreAccountPersonDeletedEventNotification as V2CoreAccountPersonDeletedEventNotification,
    )
    from stripe.events._v2_core_account_person_updated_event import (
        V2CoreAccountPersonUpdatedEvent as V2CoreAccountPersonUpdatedEvent,
        V2CoreAccountPersonUpdatedEventNotification as V2CoreAccountPersonUpdatedEventNotification,
    )
    from stripe.events._v2_core_account_updated_event import (
        V2CoreAccountUpdatedEvent as V2CoreAccountUpdatedEvent,
        V2CoreAccountUpdatedEventNotification as V2CoreAccountUpdatedEventNotification,
    )
    from stripe.events._v2_core_event_destination_ping_event import (
        V2CoreEventDestinationPingEvent as V2CoreEventDestinationPingEvent,
        V2CoreEventDestinationPingEventNotification as V2CoreEventDestinationPingEventNotification,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "ALL_EVENT_NOTIFICATIONS": ("stripe.events._event_classes", False),
    "V1BillingMeterErrorReportTriggeredEvent": (
        "stripe.events._v1_billing_meter_error_report_triggered_event",
        False,
    ),
    "V1BillingMeterErrorReportTriggeredEventNotification": (
        "stripe.events._v1_billing_meter_error_report_triggered_event",
        False,
    ),
    "V1BillingMeterNoMeterFoundEvent": (
        "stripe.events._v1_billing_meter_no_meter_found_event",
        False,
    ),
    "V1BillingMeterNoMeterFoundEventNotification": (
        "stripe.events._v1_billing_meter_no_meter_found_event",
        False,
    ),
    "V2CoreAccountClosedEvent": (
        "stripe.events._v2_core_account_closed_event",
        False,
    ),
    "V2CoreAccountClosedEventNotification": (
        "stripe.events._v2_core_account_closed_event",
        False,
    ),
    "V2CoreAccountCreatedEvent": (
        "stripe.events._v2_core_account_created_event",
        False,
    ),
    "V2CoreAccountCreatedEventNotification": (
        "stripe.events._v2_core_account_created_event",
        False,
    ),
    "V2CoreAccountIncludingConfigurationCustomerCapabilityStatusUpdatedEvent": (
        "stripe.events._v2_core_account_including_configuration_customer_capability_status_updated_event",
        False,
    ),
    "V2CoreAccountIncludingConfigurationCustomerCapabilityStatusUpdatedEventNotification": (
        "stripe.events._v2_core_account_including_configuration_customer_capability_status_updated_event",
        False,
    ),
    "V2CoreAccountIncludingConfigurationCustomerUpdatedEvent": (
        "stripe.events._v2_core_account_including_configuration_customer_updated_event",
        False,
    ),
    "V2CoreAccountIncludingConfigurationCustomerUpdatedEventNotification": (
        "stripe.events._v2_core_account_including_configuration_customer_updated_event",
        False,
    ),
    "V2CoreAccountIncludingConfigurationMerchantCapabilityStatusUpdatedEvent": (
        "stripe.events._v2_core_account_including_configuration_merchant_capability_status_updated_event",
        False,
    ),
    "V2CoreAccountIncludingConfigurationMerchantCapabilityStatusUpdatedEventNotification": (
        "stripe.events._v2_core_account_including_configuration_merchant_capability_status_updated_event",
        False,
    ),
    "V2CoreAccountIncludingConfigurationMerchantUpdatedEvent": (
        "stripe.events._v2_core_account_including_configuration_merchant_updated_event",
        False,
    ),
    "V2CoreAccountIncludingConfigurationMerchantUpdatedEventNotification": (
        "stripe.events._v2_core_account_including_configuration_merchant_updated_event",
        False,
    ),
    "V2CoreAccountIncludingConfigurationRecipientCapabilityStatusUpdatedEvent": (
        "stripe.events._v2_core_account_including_configuration_recipient_capability_status_updated_event",
        False,
    ),
    "V2CoreAccountIncludingConfigurationRecipientCapabilityStatusUpdatedEventNotification": (
        "stripe.events._v2_core_account_including_configuration_recipient_capability_status_updated_event",
        False,
    ),
    "V2CoreAccountIncludingConfigurationRecipientUpdatedEvent": (
        "stripe.events._v2_core_account_including_configuration_recipient_updated_event",
        False,
    ),
    "V2CoreAccountIncludingConfigurationRecipientUpdatedEventNotification": (
        "stripe.events._v2_core_account_including_configuration_recipient_updated_event",
        False,
    ),
    "V2CoreAccountIncludingDefaultsUpdatedEvent": (
        "stripe.events._v2_core_account_including_defaults_updated_event",
        False,
    ),
    "V2CoreAccountIncludingDefaultsUpdatedEventNotification": (
        "stripe.events._v2_core_account_including_defaults_updated_event",
        False,
    ),
    "V2CoreAccountIncludingFutureRequirementsUpdatedEvent": (
        "stripe.events._v2_core_account_including_future_requirements_updated_event",
        False,
    ),
    "V2CoreAccountIncludingFutureRequirementsUpdatedEventNotification": (
        "stripe.events._v2_core_account_including_future_requirements_updated_event",
        False,
    ),
    "V2CoreAccountIncludingIdentityUpdatedEvent": (
        "stripe.events._v2_core_account_including_identity_updated_event",
        False,
    ),
    "V2CoreAccountIncludingIdentityUpdatedEventNotification": (
        "stripe.events._v2_core_account_including_identity_updated_event",
        False,
    ),
    "V2CoreAccountIncludingRequirementsUpdatedEvent": (
        "stripe.events._v2_core_account_including_requirements_updated_event",
        False,
    ),
    "V2CoreAccountIncludingRequirementsUpdatedEventNotification": (
        "stripe.events._v2_core_account_including_requirements_updated_event",
        False,
    ),
    "V2CoreAccountLinkReturnedEvent": (
        "stripe.events._v2_core_account_link_returned_event",
        False,
    ),
    "V2CoreAccountLinkReturnedEventNotification": (
        "stripe.events._v2_core_account_link_returned_event",
        False,
    ),
    "V2CoreAccountPersonCreatedEvent": (
        "stripe.events._v2_core_account_person_created_event",
        False,
    ),
    "V2CoreAccountPersonCreatedEventNotification": (
        "stripe.events._v2_core_account_person_created_event",
        False,
    ),
    "V2CoreAccountPersonDeletedEvent": (
        "stripe.events._v2_core_account_person_deleted_event",
        False,
    ),
    "V2CoreAccountPersonDeletedEventNotification": (
        "stripe.events._v2_core_account_person_deleted_event",
        False,
    ),
    "V2CoreAccountPersonUpdatedEvent": (
        "stripe.events._v2_core_account_person_updated_event",
        False,
    ),
    "V2CoreAccountPersonUpdatedEventNotification": (
        "stripe.events._v2_core_account_person_updated_event",
        False,
    ),
    "V2CoreAccountUpdatedEvent": (
        "stripe.events._v2_core_account_updated_event",
        False,
    ),
    "V2CoreAccountUpdatedEventNotification": (
        "stripe.events._v2_core_account_updated_event",
        False,
    ),
    "V2CoreEventDestinationPingEvent": (
        "stripe.events._v2_core_event_destination_ping_event",
        False,
    ),
    "V2CoreEventDestinationPingEventNotification": (
        "stripe.events._v2_core_event_destination_ping_event",
        False,
    ),
}
if not TYPE_CHECKING:

    def __getattr__(name):
        try:
            target, is_submodule = _import_map[name]
            module = import_module(target)
            if is_submodule:
                return module

            return getattr(
                module,
                name,
            )
        except KeyError:
            raise AttributeError()

# The end of the section generated from our OpenAPI spec
