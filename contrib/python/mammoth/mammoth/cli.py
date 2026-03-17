import argparse
import io
import os
import shutil
import sys

import mammoth
from . import writers


def main():
    args = _parse_args()
    
    if args.style_map is None:
        style_map = None
    else:
        with open(args.style_map) as style_map_fileobj:
            style_map = style_map_fileobj.read()
    
    with open(args.path, "rb") as docx_fileobj:
        if args.output_dir is None:
            convert_image = None
            output_path = args.output
        else:
            convert_image = mammoth.images.img_element(ImageWriter(args.output_dir))
            output_filename = "{0}.html".format(os.path.basename(args.path).rpartition(".")[0])
            output_path = os.path.join(args.output_dir, output_filename)
        
        result = mammoth.convert(
            docx_fileobj,
            style_map=style_map,
            convert_image=convert_image,
            output_format=args.output_format,
        )
        for message in result.messages:
            sys.stderr.write(message.message)
            sys.stderr.write("\n")
        
        _write_output(output_path, result.value)


class ImageWriter(object):
    def __init__(self, output_dir):
        self._output_dir = output_dir
        self._image_number = 1
        
    def __call__(self, element):
        extension = element.content_type.partition("/")[2]
        image_filename = "{0}.{1}".format(self._image_number, extension)
        with open(os.path.join(self._output_dir, image_filename), "wb") as image_dest:
            with element.open() as image_source:
                shutil.copyfileobj(image_source, image_dest)
        
        self._image_number += 1
        
        return {"src": image_filename}


def _write_output(path, contents):
    if path is None:
        if sys.version_info[0] <= 2:
            stdout = sys.stdout
        else:
            stdout = sys.stdout.buffer

        stdout.write(contents.encode("utf-8"))
        stdout.flush()
    else:
        with io.open(path, "w", encoding="utf-8") as fileobj:
            fileobj.write(contents)


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "path",
        metavar="docx-path",
        help="Path to the .docx file to convert.")
    
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument(
        "output",
        nargs="?",
        metavar="output-path",
        help="Output path for the generated document. Images will be stored inline in the output document. Output is written to stdout if not set.")
    output_group.add_argument(
        "--output-dir",
        help="Output directory for generated HTML and images. Images will be stored in separate files. Mutually exclusive with output-path.")
    
    parser.add_argument(
        "--output-format",
        required=False,
        choices=writers.formats(),
        help="Output format.")
    parser.add_argument(
        "--style-map",
        required=False,
        help="File containg a style map.")
    return parser.parse_args()


if __name__ == "__main__":
    main()

