# -*- coding: utf-8; mode: django -*-
import graphviz
from optparse import make_option
from itertools import chain

from django.core.management.base import BaseCommand
try:
    from django.utils.encoding import force_text
    _requires_system_checks = True
except ImportError:  # Django >= 4.0
    from django.utils.encoding import force_str as force_text
    from django.core.management.base import ALL_CHECKS
    _requires_system_checks = ALL_CHECKS

from django_fsm import FSMFieldMixin, GET_STATE, RETURN_VALUE

try:
    from django.db.models import get_apps, get_app, get_models, get_model

    NEW_META_API = False
except ImportError:
    from django.apps import apps

    NEW_META_API = True

from django import VERSION

HAS_ARGPARSE = VERSION >= (1, 10)


def all_fsm_fields_data(model):
    if NEW_META_API:
        return [(field, model) for field in model._meta.get_fields() if isinstance(field, FSMFieldMixin)]
    else:
        return [(field, model) for field in model._meta.fields if isinstance(field, FSMFieldMixin)]


def node_name(field, state):
    opts = field.model._meta
    return "%s.%s.%s.%s" % (opts.app_label, opts.verbose_name.replace(" ", "_"), field.name, state)


def node_label(field, state):
    if type(state) == int or (type(state) == bool and hasattr(field, "choices")):
        return force_text(dict(field.choices).get(state))
    else:
        return state


def generate_dot(fields_data):
    result = graphviz.Digraph()

    for field, model in fields_data:
        sources, targets, edges, any_targets, any_except_targets = set(), set(), set(), set(), set()

        # dump nodes and edges
        for transition in field.get_all_transitions(model):
            if transition.source == "*":
                any_targets.add((transition.target, transition.name))
            elif transition.source == "+":
                any_except_targets.add((transition.target, transition.name))
            else:
                _targets = (
                    (state for state in transition.target.allowed_states)
                    if isinstance(transition.target, (GET_STATE, RETURN_VALUE))
                    else (transition.target,)
                )
                source_name_pair = (
                    ((state, node_name(field, state)) for state in transition.source.allowed_states)
                    if isinstance(transition.source, (GET_STATE, RETURN_VALUE))
                    else ((transition.source, node_name(field, transition.source)),)
                )
                for source, source_name in source_name_pair:
                    if transition.on_error:
                        on_error_name = node_name(field, transition.on_error)
                        targets.add((on_error_name, node_label(field, transition.on_error)))
                        edges.add((source_name, on_error_name, (("style", "dotted"),)))
                    for target in _targets:
                        add_transition(source, target, transition.name, source_name, field, sources, targets, edges)

        targets.update(
            set((node_name(field, target), node_label(field, target)) for target, _ in chain(any_targets, any_except_targets))
        )
        for target, name in any_targets:
            target_name = node_name(field, target)
            all_nodes = sources | targets
            for source_name, label in all_nodes:
                sources.add((source_name, label))
                edges.add((source_name, target_name, (("label", name),)))

        for target, name in any_except_targets:
            target_name = node_name(field, target)
            all_nodes = sources | targets
            all_nodes.remove(((target_name, node_label(field, target))))
            for source_name, label in all_nodes:
                sources.add((source_name, label))
                edges.add((source_name, target_name, (("label", name),)))

        # construct subgraph
        opts = field.model._meta
        subgraph = graphviz.Digraph(
            name="cluster_%s_%s_%s" % (opts.app_label, opts.object_name, field.name),
            graph_attr={"label": "%s.%s.%s" % (opts.app_label, opts.object_name, field.name)},
        )

        final_states = targets - sources
        for name, label in final_states:
            subgraph.node(name, label=str(label), shape="doublecircle")
        for name, label in (sources | targets) - final_states:
            subgraph.node(name, label=str(label), shape="circle")
            if field.default:  # Adding initial state notation
                if label == field.default:
                    initial_name = node_name(field, "_initial")
                    subgraph.node(name=initial_name, label="", shape="point")
                    subgraph.edge(initial_name, name)
        for source_name, target_name, attrs in edges:
            subgraph.edge(source_name, target_name, **dict(attrs))

        result.subgraph(subgraph)

    return result


def add_transition(transition_source, transition_target, transition_name, source_name, field, sources, targets, edges):
    target_name = node_name(field, transition_target)
    sources.add((source_name, node_label(field, transition_source)))
    targets.add((target_name, node_label(field, transition_target)))
    edges.add((source_name, target_name, (("label", transition_name),)))


def get_graphviz_layouts():
    try:
        import graphviz

        return graphviz.backend.ENGINES
    except Exception:
        return {"sfdp", "circo", "twopi", "dot", "neato", "fdp", "osage", "patchwork"}


class Command(BaseCommand):
    requires_system_checks = _requires_system_checks

    if not HAS_ARGPARSE:
        option_list = BaseCommand.option_list + (
            make_option(
                "--output",
                "-o",
                action="store",
                dest="outputfile",
                help=(
                    "Render output file. Type of output dependent on file extensions. " "Use png or jpg to render graph to image."
                ),
            ),
            # NOQA
            make_option(
                "--layout",
                "-l",
                action="store",
                dest="layout",
                default="dot",
                help=("Layout to be used by GraphViz for visualization. " "Layouts: %s." % " ".join(get_graphviz_layouts())),
            ),
        )
        args = "[appname[.model[.field]]]"
    else:

        def add_arguments(self, parser):
            parser.add_argument(
                "--output",
                "-o",
                action="store",
                dest="outputfile",
                help=(
                    "Render output file. Type of output dependent on file extensions. " "Use png or jpg to render graph to image."
                ),
            )
            parser.add_argument(
                "--layout",
                "-l",
                action="store",
                dest="layout",
                default="dot",
                help=("Layout to be used by GraphViz for visualization. " "Layouts: %s." % " ".join(get_graphviz_layouts())),
            )
            parser.add_argument("args", nargs="*", help=("[appname[.model[.field]]]"))

    help = "Creates a GraphViz dot file with transitions for selected fields"

    def render_output(self, graph, **options):
        filename, format = options["outputfile"].rsplit(".", 1)

        graph.engine = options["layout"]
        graph.format = format
        graph.render(filename)

    def handle(self, *args, **options):
        fields_data = []
        if len(args) != 0:
            for arg in args:
                field_spec = arg.split(".")

                if len(field_spec) == 1:
                    if NEW_META_API:
                        app = apps.get_app(field_spec[0])
                        models = apps.get_models(app)
                    else:
                        app = get_app(field_spec[0])
                        models = get_models(app)
                    for model in models:
                        fields_data += all_fsm_fields_data(model)
                elif len(field_spec) == 2:
                    if NEW_META_API:
                        model = apps.get_model(field_spec[0], field_spec[1])
                    else:
                        model = get_model(field_spec[0], field_spec[1])
                    fields_data += all_fsm_fields_data(model)
                elif len(field_spec) == 3:
                    if NEW_META_API:
                        model = apps.get_model(field_spec[0], field_spec[1])
                    else:
                        model = get_model(field_spec[0], field_spec[1])
                    fields_data += all_fsm_fields_data(model)
        else:
            if NEW_META_API:
                for model in apps.get_models():
                    fields_data += all_fsm_fields_data(model)
            else:
                for app in get_apps():
                    for model in get_models(app):
                        fields_data += all_fsm_fields_data(model)

        dotdata = generate_dot(fields_data)

        if options["outputfile"]:
            self.render_output(dotdata, **options)
        else:
            print(dotdata)
