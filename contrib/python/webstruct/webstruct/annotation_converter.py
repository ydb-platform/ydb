import logging
import argparse

import webstruct.loaders
from webstruct.webannotator import EntityColors, to_webannotator


def main():
    cmdline = argparse.ArgumentParser(description=('utility '
                                                   'to convert annotations '
                                                   'from GATE format to '
                                                   'WebAnnotator format'))
    cmdline.add_argument('--GATE',
                         help='path to file annotated in GATE format',
                         type=str,
                         required=True)
    cmdline.add_argument('--sample',
                         help=('path to file annotated in WebAnnotator format '
                               'for colors and entities transfer'),
                         type=str,
                         required=True)
    cmdline.add_argument('--WebAnnotator',
                         help='path to result file in WebAnnotator format',
                         type=str,
                         required=True)
    cmdline.add_argument('--loglevel',
                         help='logging level',
                         type=str,
                         default='INFO')
    args = cmdline.parse_args()

    logging.basicConfig(level=getattr(logging, args.loglevel.upper()),
                        format=('%(asctime)s [%(levelname)s]'
                                '%(pathname)s:%(lineno)d %(message)s'))
    with open(args.sample, 'rb') as sample_reader:
        colors = EntityColors.from_htmlbytes(sample_reader.read())
        entities = [typ for typ in colors]

    logging.debug('Current entities %s', entities)
    logging.debug('Current colors %s', colors)

    gate = webstruct.loaders.GateLoader(known_entities=entities)
    tokenizer = webstruct.HtmlTokenizer(tagset=entities)
    with open(args.GATE, 'rb') as reader:
        data = reader.read()
        tree = gate.loadbytes(data)
        tokens, annotations = tokenizer.tokenize_single(tree)
        tree = to_webannotator(tree, entity_colors=colors)
        with open(args.WebAnnotator, 'wb') as writer:
            tree.write(writer, method='html', pretty_print=True)

if __name__ == "__main__":
    main()
