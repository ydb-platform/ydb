from django.core.exceptions import PermissionDenied
from django.utils.encoding import force_str
from django.utils.html import escape


class LookupChannel:
    """
    Subclass this, setting the model and implementing methods to taste.

    Attributes::

        model (Model): The Django Model that this lookup channel will search for.
        plugin_options (dict): Options passed to jQuery UI plugin that are specific to this channel.
        min_length (int): Minimum number of characters user types before a search is initiated.

            This is passed to the jQuery plugin_options.
            It is used in jQuery's UI when filtering results from its own cache.

            It is also used in the django view to prevent expensive database queries.
            Large datasets can choke if they search too often with small queries.
            Better to demand at least 2 or 3 characters.
    """

    model = None
    plugin_options = {}
    min_length = 1

    def get_query(self, q, request):
        """
        Return a QuerySet searching for the query string `q`.

        Note that you may return any iterable so you can return a list or even
        use yield and turn this method into a generator.

        Args:
            q (str, unicode): The query string to search for.
            request (Request): This can be used to customize the search by User
                or to use additional GET variables.

        Returns:
            (QuerySet, list, generator): iterable of related_models

        """
        kwargs = {f"{self.search_field}__icontains": q}
        return self.model.objects.filter(**kwargs).order_by(self.search_field)

    def get_result(self, obj):
        """
        The text result of autocompleting the entered query.

        For a partial string that the user typed in, each matched result is
        here converted to the fully completed text.

        This is currently displayed only for a moment in the text field after
        the user has selected the item.
        Then the item is displayed in the item_display deck and the text field
        is cleared.

        Args:
            obj (Model):
        Returns:
            str: The object as string

        """
        return escape(force_str(obj))

    def format_match(self, obj):
        """
        (HTML) Format item for displaying in the dropdown.

        Args:
            obj (Model):
        Returns:
            str: formatted string, may contain HTML.

        """
        return escape(force_str(obj))

    def format_item_display(self, obj):
        """
        (HTML) format item for displaying item in the selected deck area.

        Args:
            obj (Model):
        Returns:
            str: formatted string, may contain HTML.

        """
        return escape(force_str(obj))

    def get_objects(self, ids):
        """
        This is used to retrieve the currently selected objects for either ManyToMany or ForeignKey.

        Args:
            ids (list): list of primary keys
        Returns:
            list: list of Model objects

        """
        # Inherited models have a OneToOneField (rather than eg AutoField)
        if getattr(self.model._meta.pk, "remote_field", False):
            # Use the type of the field being referenced (2.0+)
            pk_type = self.model._meta.pk.remote_field.field.to_python
        elif getattr(self.model._meta.pk, "rel", False):
            # Use the type of the field being referenced
            pk_type = self.model._meta.pk.rel.field.to_python
        else:
            pk_type = self.model._meta.pk.to_python

        # Return objects in the same order as passed in here
        ids = [pk_type(pk) for pk in ids]
        things = self.model.objects.in_bulk(ids)
        return [things[aid] for aid in ids if aid in things]

    def can_add(self, user, other_model):
        """
        Check if the user has permission to add a ForeignKey or M2M model.

        This enables the green popup + on the widget.
        Default implentation is the standard django permission check.

        Args:
            user (User)
            other_model (Model): the ForeignKey or M2M model to check if the User can add.

        Returns:
            bool

        """
        from django.contrib.contenttypes.models import ContentType

        ctype = ContentType.objects.get_for_model(other_model)
        return user.has_perm(f"{ctype.app_label}.add_{ctype.model}")

    def check_auth(self, request):
        """
        By default only request.user.is_staff have access.

        This ensures that nobody can get your data by simply knowing the lookup URL.

        This is called from the ajax_lookup view.

        Public facing forms (outside of the Admin) should implement this to
        allow non-staff to use this LookupChannel.

        Args:
            request (Request)

        Raises:
            PermissionDenied

        """
        if not request.user.is_staff:
            raise PermissionDenied
