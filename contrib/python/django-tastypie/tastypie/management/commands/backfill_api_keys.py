from django.core.management.base import BaseCommand
from tastypie.compat import get_user_model
from tastypie.models import ApiKey


class Command(BaseCommand):
    help = "Goes through all users and adds API keys for any that don't have one."

    def handle(self, **options):
        "Goes through all users and adds API keys for any that don't have one."
        self.verbosity = int(options.get('verbosity', 1))

        User = get_user_model()
        for user in User.objects.all().iterator():
            try:
                api_key = ApiKey.objects.get(user=user)

                if not api_key.key:
                    # Autogenerate the key.
                    api_key.save()

                    if self.verbosity >= 1:
                        print(u"Generated a new key for '%s'" % user.username)
            except ApiKey.DoesNotExist:
                api_key = ApiKey.objects.create(user=user)

                if self.verbosity >= 1:
                    print(u"Created a new key for '%s'" % user.username)
