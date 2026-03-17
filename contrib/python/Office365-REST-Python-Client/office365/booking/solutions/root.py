from office365.backuprestore.root import BackupRestoreRoot
from office365.booking.business.collection import BookingBusinessCollection
from office365.booking.currency import BookingCurrency
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class SolutionsRoot(Entity):
    """The entry point for Microsoft Bookings, virtual event, and business scenario APIs."""

    @property
    def booking_businesses(self):
        # type: () -> BookingBusinessCollection
        """Get a collection of bookingBusiness objects that has been created for the tenant."""
        return self.properties.get(
            "bookingBusinesses",
            BookingBusinessCollection(
                self.context, ResourcePath("bookingBusinesses", self.resource_path)
            ),
        )

    @property
    def booking_currencies(self):
        """Get a list of bookingCurrency objects available to a Microsoft Bookings business"""
        return self.properties.get(
            "bookingCurrencies",
            EntityCollection(
                self.context,
                BookingCurrency,
                ResourcePath("bookingCurrencies", self.resource_path),
            ),
        )

    @property
    def backup_restore(self):
        # type: () -> BackupRestoreRoot
        """Get a Microsoft 365 Backup Storage service in a tenant"""
        return self.properties.get(
            "backupRestore",
            BackupRestoreRoot(
                self.context, ResourcePath("backupRestore", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "bookingBusinesses": self.booking_businesses,
                "bookingCurrencies": self.booking_currencies,
                "backupRestore": self.backup_restore,
            }
            default_value = property_mapping.get(name, None)
        return super(SolutionsRoot, self).get_property(name, default_value)
