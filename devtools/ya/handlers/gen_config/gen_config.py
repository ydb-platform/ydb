from __future__ import absolute_import
import sys
import itertools
import collections
import json

import toml

import core.yarg
import six

toml_encoder = toml.TomlEncoder()

NOT_SET = -1


def is_jsonable(x):
    try:
        json.dumps(x)
        return True
    except (TypeError, OverflowError):
        return False


def iter_ya_options(root_handler, all_opts=False):
    # prefer setvalue over setconstvalue to provide correct description and default value
    hook_prefer_order = [core.yarg.SetValueHook, core.yarg.SetConstValueHook]

    def get_hook_priority(hook, consumer):
        if isinstance(consumer, core.yarg.ConfigConsumer):
            return -1

        htype = type(hook)
        if htype in hook_prefer_order:
            pos = hook_prefer_order.index(htype)
        else:
            pos = len(hook_prefer_order)
        return len(hook_prefer_order) - pos

    for handler_name, command in six.iteritems(root_handler.sub_handlers):
        if not command.visible:
            continue

        top_handler = command.command()
        handlers = collections.deque([([handler_name], top_handler)])

        while handlers:
            trace, handler = handlers.pop()

            sub_handlers = handler.sub_handlers or {}
            for sub_name in sorted(sub_handlers):
                handlers.append((trace + [sub_name], sub_handlers[sub_name]))

            options = getattr(handler, "options", None)
            if not options:
                continue

            if all_opts:
                for opt in options:
                    for name in vars(opt).keys():
                        default = getattr(opt, name)
                        if is_jsonable(default):
                            yield name, "", default, "", "", ""
                continue

            top_consumer = options.consumer()
            if isinstance(top_consumer, core.yarg.Compound):
                consumers = top_consumer.parts
            else:
                consumers = [top_consumer]

            # pairwise config option with command line name if it's possible
            params = {}
            for consumer in consumers:
                if not isinstance(consumer, (core.yarg.ArgConsumer, core.yarg.ConfigConsumer)):
                    continue

                hook = consumer.hook
                if not hook:
                    continue

                opt_name = getattr(hook, 'name', None)
                if not opt_name or opt_name.startswith("_"):
                    continue

                if opt_name not in params:
                    params[opt_name] = {
                        'configurable': False,
                    }

                entry = params[opt_name]
                priority = get_hook_priority(hook, consumer)

                entry['configurable'] |= isinstance(consumer, core.yarg.ConfigConsumer)
                if priority > entry.get('_priority', -2):
                    entry['cmdline_names'] = get_arg_consumer_names(consumer) or entry.get('cmdline_names')
                    entry['trace'] = trace or entry.get('trace')
                    entry['description'] = getattr(consumer, "help", "") or entry.get('description')
                    entry['group'] = getattr(consumer, "group", "") or entry.get('group', 0)
                    # default value might be empty list or dict
                    if getattr(options, opt_name, None) is None:
                        entry['default'] = entry.get('default')
                    else:
                        entry['default'] = getattr(options, opt_name)
                    entry['_priority'] = priority

            for name, x in params.items():
                if x['configurable']:
                    yield name, x['cmdline_names'], x['default'], x['group'], x['description'], x['trace']


def get_arg_consumer_names(consumer):
    if isinstance(consumer, core.yarg.ArgConsumer):
        return list([_f for _f in [consumer.short_name, consumer.long_name] if _f])
    return []


def split(iterable, func):
    d1 = []
    d2 = []
    for entry in iterable:
        if func(entry):
            d1.append(entry)
        else:
            d2.append(entry)
    return d1, d2


def get_comment(entry):
    parts = []
    if entry['desc']:
        parts.append(entry['desc'])
    if entry['cmdline_names']:
        parts.append("({})".format(", ".join(entry['cmdline_names'])))
    return " ".join(parts)


def compress_keys(data, result, trace=None):
    trace = trace or []
    for key, val in six.iteritems(data):
        if isinstance(val, dict):
            result[".".join(trace + [key])] = val
            compress_keys(val, result, trace + [key])


def dump_options(entries, output, config, section="", print_comment=True):
    if section:
        output.append("[{}]".format(section))
    for entry in sorted(entries, key=lambda x: x['name']):
        parts = []
        if print_comment:
            comment = get_comment(entry)
            if comment:
                parts.append("# {}".format(comment))
        name = entry['name']
        if name in config and config[name] != entry['default']:
            parts.append("{} = {}".format(name, toml_encoder.dump_value(config[name])))
        else:
            parts.append("# {} = {}".format(name, toml_encoder.dump_value(entry['default'])))

        output.append("\n".join(parts))


def dump_subgroups(subgroups, output, config, section="", print_comment=True):
    def get_section(name):
        return ".".join([_f for _f in [section, name] if _f])

    for entry in subgroups:
        parts = []
        if print_comment:
            comment = get_comment(entry)
            if comment:
                parts.append("# {}".format(comment))

        name = entry['name']
        if name in config and config[name] != entry['default']:
            parts.append("[{}]".format(get_section(name)))
            for key, val in config[name].items():
                parts.append("{} = {}".format(key, toml_encoder.dump_value(val)))
        else:
            parts.append("# [{}]".format(get_section(name)))
            for key, val in entry['default'].items():
                parts.append("# {} = {}".format(key, toml_encoder.dump_value(val)))

        output.append("\n".join(parts))


def dump_config(options, handler_map, user_config):
    def sort_func(x):
        return getattr(x['group'], 'index', NOT_SET)

    def get_group_title(name):
        return "{} {} {}".format("=" * 10, name, "=" * (65 - len(name)))

    blocks = [
        "# Save config to the junk/{USER}/ya.conf or to the ~/.ya/ya.conf\n# For more info see https://docs.yandex-team.ru/yatool/commands/gen_config"
    ]

    # dump all options
    subgroups = []
    data = sorted(options.values(), key=sort_func)
    for group_index, entries in itertools.groupby(data, sort_func):
        entries = list(entries)

        if group_index != NOT_SET:
            group_name = entries[0]['group'].name
            if group_name:
                blocks.append("# " + get_group_title(group_name))

        subs, opts = split(entries, lambda x: isinstance(x['default'], dict))
        dump_options(opts, blocks, user_config)
        # move all subgroup options out of the default section
        # otherwise every option defined after section will be related to the section
        subgroups += subs

    if subgroups:
        blocks.append("# " + get_group_title("Various table options"))
        blocks.append("# Uncomment table name with parameters")
        dump_subgroups(subgroups, blocks, user_config)

    # Save user redefined opts for specific handlers
    specific_opts = {}
    user_handler_map = {}
    compress_keys(user_config, user_handler_map)
    for section, keys in six.iteritems(user_handler_map):
        if section not in handler_map:
            continue
        entries = []
        for optname in keys:
            if optname in handler_map[section]:
                entries.append(options[optname])
        if entries:
            specific_opts[section] = entries

    if specific_opts:
        blocks.append("# " + get_group_title("Redefined options for specific handlers"))
        for section, entries in sorted(specific_opts.items(), key=lambda x: x[0]):
            subs, opts = split(entries, lambda x: isinstance(x['default'], dict))
            if opts:
                dump_options(opts, blocks, user_handler_map[section], section, print_comment=False)
            if subgroups:
                dump_subgroups(subs, blocks, user_handler_map[section], section, print_comment=False)

    return "\n#\n".join(blocks)


def generate_config(root_handler, output=None, dump_defaults=None):
    # Don't load global config files to avoid penetration of the global options into user config
    config_files = core.yarg.get_config_files(global_config=False)
    user_config = core.yarg.load_config(config_files)

    options = {}
    handler_map = {}
    for name, cmdline_names, default, group, desc, trace in iter_ya_options(root_handler, dump_defaults):
        if name not in options:
            options[name] = {
                'cmdline_names': cmdline_names,
                'default': default,
                'desc': desc,
                'group': group,
                'name': name,
            }
        else:
            entry = options[name]
            entry['desc'] = entry['desc'] or desc
            entry['group'] = entry['group'] or group

        target = ".".join(trace)
        if target not in handler_map:
            handler_map[target] = []
        handler_map[target].append(name)

    if dump_defaults:
        json.dump({k: v['default'] for k, v in options.items()}, sys.stdout, indent=2)
        return

    data = dump_config(options, handler_map, user_config)
    if output:
        with open(output, 'w') as afile:
            afile.write(data)
    else:
        sys.stdout.write(data + '\n')
