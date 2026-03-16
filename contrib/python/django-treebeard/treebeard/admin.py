"""Django admin support for treebeard"""

import sys

from django.contrib import admin, messages
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.http import HttpResponse, HttpResponseBadRequest
from django.urls import path
from django.utils.encoding import force_str
from django.utils.translation import gettext_lazy as _

from treebeard.al_tree import AL_Node
from treebeard.exceptions import InvalidMoveToDescendant, InvalidPosition, MissingNodeOrderBy, PathOverflow


def check_empty_dict(GET_dict):
    """
    Returns True if the GET query string contains no values, but it can contain
    empty keys.
    This is better than doing not bool(request.GET) as an empty key will return True
    """
    for k, v in GET_dict.items():
        # Don't disable on p(age) or 'all' GET param
        if v and (k not in ["p", "all"]):
            return False
    return True


class TreeAdmin(admin.ModelAdmin):
    """Django Admin class for treebeard."""

    change_list_template = "admin/tree_change_list.html"

    def get_queryset(self, request):
        if issubclass(self.model, AL_Node):
            # AL Trees return a list instead of a QuerySet for .get_tree()
            # So we're returning the regular .get_queryset cause we will use
            # the old admin
            return super().get_queryset(request)

        # We deliberately don't use `get_tree()` here because we want the specific
        # model for inherited models. This assumes that all implementations
        # return the queryset in DFS order (except AL_Node which is handled above).
        return self.model.objects.all()

    def changelist_view(self, request, extra_context=None):
        if issubclass(self.model, AL_Node):
            # For AL trees, use the old admin display
            self.change_list_template = "admin/tree_list.html"

        if extra_context is None:
            extra_context = {}

        extra_context["has_change_permission"] = self.has_change_permission(request)
        extra_context["filtered"] = not check_empty_dict(request.GET)
        return super().changelist_view(request, extra_context)

    def _changeform_view(self, *args, **kwargs):
        # Because Treebeard frequently needs to modify many objects in a tree when one node
        # is added/updated, the normal behaviour of relying on `commit=False` to create
        # unsaved objects before validating inlines etc doesn't work: Treebeard has already
        # made database changes to prepare to insert/move a node.
        # For this reason, if the form has error
        response = super()._changeform_view(*args, **kwargs)
        if getattr(response, "context_data", {}).get("errors", None):
            # There was an error somewhere, likely in an inline, so we'll need to roll back
            transaction.set_rollback(True)
        return response

    def get_urls(self):
        """
        Adds a url to move nodes to this admin
        """
        urls = super().get_urls()
        from django.views.i18n import JavaScriptCatalog

        jsi18n_url = path("jsi18n/", JavaScriptCatalog.as_view(packages=["treebeard"]), name="javascript-catalog")

        new_urls = [
            path(
                "move/",
                self.admin_site.admin_view(self.move_node),
            ),
            jsi18n_url,
        ]
        return new_urls + urls

    def get_node(self, node_id):
        return self.model.objects.get(pk=node_id)

    def try_to_move_node(self, as_child, node, pos, request, target):
        try:
            node.move(target, pos=pos)
            # Call the save method on the (reloaded) node in order to trigger
            # possible signal handlers etc.
            node = self.get_node(node.pk)
            node.save()
        except (MissingNodeOrderBy, PathOverflow, InvalidMoveToDescendant, InvalidPosition):
            e = sys.exc_info()[1]
            # An error was raised while trying to move the node, then set an
            # error message and return 400, this will cause a reload on the
            # client to show the message
            messages.error(request, _("Exception raised while moving node: %s") % _(force_str(e)))
            return HttpResponseBadRequest("Exception raised during move")
        if as_child:
            msg = _('Moved node "%(node)s" as child of "%(other)s"')
        else:
            msg = _('Moved node "%(node)s" as sibling of "%(other)s"')
        messages.info(request, msg % {"node": node, "other": target})
        return HttpResponse("OK")

    def move_node(self, request):
        try:
            node_id = request.POST["node_id"]
            target_id = request.POST["sibling_id"]
            as_child = bool(int(request.POST.get("as_child", 0)))
        except (KeyError, ValueError):
            # Some parameters were missing return a BadRequest
            return HttpResponseBadRequest("Malformed POST params")

        node = self.get_node(node_id)

        if not self.has_change_permission(request, node):
            # The JS will trigger a page reload on error. This message will be displayed after reload.
            messages.error(request, _("You do not have permission to change this object."))
            raise PermissionDenied

        target = self.get_node(target_id)
        is_sorted = True if node.node_order_by else False

        pos = {
            (True, True): "sorted-child",
            (True, False): "last-child",
            (False, True): "sorted-sibling",
            (False, False): "left",
        }[as_child, is_sorted]
        return self.try_to_move_node(as_child, node, pos, request, target)


def admin_factory(form_class):
    """Dynamically build a TreeAdmin subclass for the given form class.

    :param form_class:
    :return: A TreeAdmin subclass.
    """
    return type(form_class.__name__ + "Admin", (TreeAdmin,), dict(form=form_class))
