import os.path
import glob
import timeit
import functools

import webstruct.webannotator
import webstruct.html_tokenizer

def load_trees(tokenizer, trees):
    for tree in trees:
        tokenizer.tokenize_single(tree)

def main():
    path = os.path.join(os.path.dirname(__file__) ,
                        ".." ,
                        "webstruct_data",
                        "corpus/business_pages/wa/*.html")

    paths = sorted(glob.glob(path))

    with open(paths[0], 'rb') as sample_reader:
        colors = webstruct.webannotator.EntityColors.from_htmlbytes(sample_reader.read())
        entities = [typ for typ in colors]

    loader = webstruct.WebAnnotatorLoader(known_entities=entities)

    trees = [loader.load(p) for p in paths]
    tokenizer = webstruct.html_tokenizer.HtmlTokenizer()
    print(timeit.timeit(functools.partial(load_trees, tokenizer, trees),
                        setup='gc.enable()',
                        number=3))

if __name__ == "__main__":
    main()
