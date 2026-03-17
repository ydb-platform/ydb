from django.contrib import admin

from .models import Summary, Configuration, Workers, Maintenance
from .realms import SummaryAdmin, ConfigurationAdmin, WorkersAdmin, MaintenanceAdmin

admin.site.register(Summary, SummaryAdmin)
admin.site.register(Configuration, ConfigurationAdmin)
admin.site.register(Workers, WorkersAdmin)
admin.site.register(Maintenance, MaintenanceAdmin)
