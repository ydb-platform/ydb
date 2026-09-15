import sys
import argparse
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('--branchcompare', action='store_true', help='Compare benchmarks of current branch with main. Provide an optional --filter')
parser.add_argument("-b", '--branch', type=str, default="main", required=False)
parser.add_argument('--benchcompare', type=str, help='Compare simd bench with stdsort methods. Requires one of qsort, qselect, partialsort, argsort or argselect')
parser.add_argument("-f", '--filter', type=str, required=False)
parser.add_argument("-r", '--repeat', type=int, required=False)
args = parser.parse_args()

if len(sys.argv) == 1:
        parser.error("requires one of --benchcompare or --branchcompare")

filterb = ""
if args.filter is not None:
    filterb = args.filter
repeatnum = 1
if args.repeat is not None:
    repeatnum = args.repeat

if args.benchcompare:
    baseline = ""
    contender = ""
    if "ippsort" in args.benchcompare:
        baseline = "ippsort.*" + filterb
        contender = "simdsort.*" + filterb
    elif "ippargsort" in args.benchcompare:
        baseline = "ippargsort.*" + filterb
        contender = "simd_ordern_argsort.*" + filterb
    elif "vqsort" in args.benchcompare:
        baseline = "vqsort.*" + filterb
        contender = "simdsort.*" + filterb
    elif "qsort" in args.benchcompare:
        baseline = "scalarsort.*" + filterb
        contender = "simdsort.*" + filterb
    elif "select" in args.benchcompare:
        baseline = "scalarqselect.*" + filterb
        contender = "simdqselect.*" + filterb
    elif "partial" in args.benchcompare:
        baseline = "scalarpartialsort.*" + filterb
        contender = "simdpartialsort.*" + filterb
    elif "argsort" in args.benchcompare:
        baseline = "scalarargsort.*" + filterb
        contender = "simdargsort.*" + filterb
    elif "keyvalue" in args.benchcompare:
        baseline = "scalarkvsort.*" + filterb
        contender = "simdkvsort.*" + filterb
    elif "objsort" in args.benchcompare:
        baseline = "scalarobjsort.*" + filterb
        contender = "simdobjsort.*" + filterb
    else:
        parser.print_help(sys.stderr)
        parser.error("ERROR: Unknown argument '%s'" % args.benchcompare)
    rc = subprocess.check_call("./scripts/bench-compare.sh '%s' '%s' '%d'" % (baseline, contender, repeatnum), shell=True)

if args.branchcompare:
    branch = args.branch
    if args.filter is None:
        rc = subprocess.check_call("./scripts/branch-compare.sh '%s' '%d'" % (branch, repeatnum), shell=True)
    else:
        rc = subprocess.check_call("./scripts/branch-compare.sh '%s' '%s' '%d'" % (branch, args.filter, repeatnum), shell=True)
