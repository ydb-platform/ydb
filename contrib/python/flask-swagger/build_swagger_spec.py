import os
import sys
import argparse
import json
import pkg_resources
from flask_swagger import swagger

sys.path.append(os.getcwd())

parser = argparse.ArgumentParser()
parser.add_argument('app', help='the flask app to swaggerify')
parser.add_argument('--template', help='template spec to start with, before any other options or processing')
parser.add_argument('--out-dir', default=None, help='the directory to output to')
parser.add_argument('--definitions', default=None, help='json definitions file')
parser.add_argument('--host', default=None)
parser.add_argument('--base-path', default=None)
parser.add_argument('--version', default=None, help='Specify a spec version')

args = parser.parse_args()

def run():
    app = pkg_resources.EntryPoint.parse("x=%s" % args.app).load(False)

    # load the base template
    template = None
    if args.template is not None:
        with open(args.template, 'r') as f:
            template = json.loads(f.read())

        # overwrite template with specified arguments
        if args.definitions is not None:
            with open(args.definitions, 'r') as f:
                rawdefs = json.loads(f.read())
                if 'definitions' in rawdefs:
                    rawdefs = rawdefs['definitions']
                for d in rawdefs.keys():
                    template['definitions'][d] = rawdefs[d]

    spec = swagger(app, template=template)
    if args.host is not None:
        spec['host'] = args.host
    if args.base_path is not None:
        spec['basePath'] = args.base_path
    if args.version is not None:
        spec['info']['version'] = args.version
    if args.out_dir is None:
        print(json.dumps(spec, indent=4))
    else:
        with open("%s/swagger.json" % args.out_dir, 'w') as f:
            f.write(json.dumps(spec, indent=4))
            f.close()

run()

