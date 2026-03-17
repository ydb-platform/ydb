from flask import current_app
from flask_debugtoolbar.panels import DebugPanel
from jinja2 import ChoiceLoader, PackageLoader

from flask_mongoengine import operation_tracker

package_loader = PackageLoader("flask_mongoengine", "templates")


def _maybe_patch_jinja_loader(jinja_env):
    """Patch the jinja_env loader to include flaskext.mongoengine
    templates folder if necessary.
    """
    if not isinstance(jinja_env.loader, ChoiceLoader):
        jinja_env.loader = ChoiceLoader([jinja_env.loader, package_loader])
    elif package_loader not in jinja_env.loader.loaders:
        jinja_env.loader.loaders.append(package_loader)


class MongoDebugPanel(DebugPanel):
    """Panel that shows information about MongoDB operations (including stack)

    Adapted from https://github.com/hmarr/django-debug-toolbar-mongo
    """

    name = "MongoDB"
    has_content = True

    def __init__(self, *args, **kwargs):
        super(MongoDebugPanel, self).__init__(*args, **kwargs)
        _maybe_patch_jinja_loader(self.jinja_env)
        operation_tracker.install_tracker()

    def process_request(self, request):
        operation_tracker.reset()

    def nav_title(self):
        return "MongoDB"

    def nav_subtitle(self):
        attrs = ["queries", "inserts", "updates", "removes"]
        ops = sum(
            sum((1 for o in getattr(operation_tracker, a) if not o["internal"]))
            for a in attrs
        )
        total_time = sum(
            sum(o["time"] for o in getattr(operation_tracker, a)) for a in attrs
        )
        return "{0} operations in {1:.2f}ms".format(ops, total_time)

    def title(self):
        return "MongoDB Operations"

    def url(self):
        return ""

    def content(self):
        context = self.context.copy()
        context["queries"] = operation_tracker.queries
        context["inserts"] = operation_tracker.inserts
        context["updates"] = operation_tracker.updates
        context["removes"] = operation_tracker.removes
        context["slow_query_limit"] = current_app.config.get(
            "MONGO_DEBUG_PANEL_SLOW_QUERY_LIMIT", 100
        )
        return self.render("panels/mongo-panel.html", context)
