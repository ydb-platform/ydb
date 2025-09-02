import json
import os
import argparse
import shutil

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--link', required=True)
    parser.add_argument('--tag', required=True)
    parser.add_argument('--output-image-path', required=True)
    parser.add_argument('--output-info-path', required=True)
    parser.add_argument('--preloaded-path', required=True)

    return parser.parse_args()


def main(args):
    # assume image was downloaded by runner
    assert os.path.exists(args.preloaded_path)

    output_dir = os.path.dirname(args.output_image_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    shutil.copyfile(args.preloaded_path, args.output_image_path)
    info = {
        'link': args.link,
        'tag': args.tag,
    }
    with open(args.output_info_path, 'w') as f:
        f.write(json.dumps(info))

if __name__ == '__main__':
    args = parse_args()
    exit(main(args))
