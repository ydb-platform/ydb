import json
import logging
import argparse

import webstruct.loaders
import webstruct.webannotator

DEFAULT_ENTITIES = [
    'ORG', 'TEL', 'FAX', 'HOURS',
    'STREET', 'CITY', 'STATE', 'ZIPCODE', 'COUNTRY',
    'EMAIL', 'PER', 'FUNC', 'SUBJ'
]


def nodes_difference(l, r):
    if l.tag != r.tag:
        return {'tag': '"{0}" != "{1}"'.format(l.tag, r.tag)}

    l_attrib = [(k, l.attrib[k]) for k in l.attrib]
    l_attrib.sort(key=lambda x: x[0])
    r_attrib = [(k, r.attrib[k]) for k in r.attrib]
    r_attrib.sort(key=lambda x: x[0])

    idx = 0
    while idx < len(l_attrib) and idx < len(r_attrib):
        l_attr = l_attrib[idx]
        r_attr = r_attrib[idx]
        idx = idx + 1

        if l_attr != r_attr:
            return {'attributes': '"{0}" != "{1}"'.format(l_attr, r_attr)}

    if idx < len(l_attrib):
        return {'attributes': "{0} != None".format(l_attrib[idx])}

    if idx < len(r_attrib):
        return {'attributes': "None != {0}".format(r_attrib[idx])}

    l_text = ''
    if l.text:
        l.text = l.text.strip()

    r_text = ''
    if r.text:
        r.text = r.text.strip()

    if l_text != r_text:
        return {'text': "{0} != {1}".format(l_text, r_text)}

    l_tail = ''
    if l.tail:
        l.tail = l.tail.strip()

    r_tail = ''
    if r.tail:
        r.tail = r.tail.strip()

    if l_tail != r_tail:
        return {'tail': "{0} != {1}".format(l_tail, r_tail)}

    if len(l) != len(r):
        return {'children count': "{0} != {1}".format(len(l), len(r))}

    return None


def node_path(node):
    ret = ''
    current = node
    while current is not None:
        parent = current.getparent()
        idx = 0
        if parent:
            idx = parent.index(current)
        step = '{0}:{1}'.format(idx, current.tag)
        ret = step + '/' + ret
        current = parent

    return ret


def tree_difference(l, r):
    stack = [(l, r)]
    while stack:
        l_node, r_node = stack.pop(0)
        diff = nodes_difference(l_node, r_node)

        if diff:
            return {"l":    node_path(l_node),
                    "r":    node_path(r_node),
                    "diff": diff}

        for idx, l_child in enumerate(l_node):
            stack.append((l_child, r_node[idx]))

    return None


def main():
    cmdline = argparse.ArgumentParser(description=('utility to verify '
                                                   'annotation conversion '
                                                   'from GATE format '
                                                   'to WebAnnotator format'))
    cmdline.add_argument('--GATE',
                         help='path to file annotated in GATE format',
                         type=str,
                         required=True)
    cmdline.add_argument('--WebAnnotator',
                         help='path to file annotated in WebAnnotator format',
                         type=str,
                         required=True)
    cmdline.add_argument('--entity',
                         help='enitity type to verify against',
                         type=str,
                         action='append',
                         required=False)
    cmdline.add_argument('--loglevel',
                         help='logging level',
                         type=str,
                         default='INFO')
    args = cmdline.parse_args()

    logging.basicConfig(level=getattr(logging, args.loglevel.upper()),
                        format=('%(asctime)s [%(levelname)s] '
                                '%(pathname)s:%(lineno)d %(message)s'))

    if args.entity:
        entities = args.entity
    else:
        entities = DEFAULT_ENTITIES

    logging.debug('Known entities %s', entities)

    gate = webstruct.loaders.GateLoader(known_entities=entities)
    wa = webstruct.loaders.WebAnnotatorLoader(known_entities=entities)

    tokenizer = webstruct.HtmlTokenizer(tagset=entities)
    with open(args.GATE, 'rb') as reader:
        data = reader.read()
        gate_tree = gate.loadbytes(data)
        gate_tokens, gate_annotations = tokenizer.tokenize_single(gate_tree)

    with open(args.WebAnnotator, 'rb') as reader:
        data = reader.read()
        wa_tree = wa.loadbytes(data)
        wa_tokens, wa_annotations = tokenizer.tokenize_single(wa_tree)

    is_diff = False
    tree_diff = tree_difference(gate_tree, wa_tree)
    if tree_diff:
        logging.error('tree differs %s', json.dumps(tree_diff))
        is_diff = True

    annot_diff = list()
    for idx, (gate_a, wa_a) in enumerate(zip(gate_annotations,
                                             wa_annotations)):
        if gate_a == wa_a:
            continue

        annot_diff.append({'idx':    idx,
                           'gate_a': gate_a,
                           'wa_a':   wa_a})

    if annot_diff:
        logging.error('annotation differs %s', json.dumps(annot_diff))
        is_diff = True

    return is_diff is False

if __name__ == "__main__":
    main()
