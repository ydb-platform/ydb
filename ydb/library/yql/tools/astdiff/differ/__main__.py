import argparse
import difflib

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("oldAstPath")
    parser.add_argument("newAstPath")
    
    args = parser.parse_args()
    with open(args.oldAstPath, 'r') as old_ast, open(args.newAstPath, 'r') as new_ast:
        old_lines = old_ast.readlines()
        new_lines = new_ast.readlines()
        diff = difflib.unified_diff(old_lines, new_lines, args.oldAstPath, args.newAstPath)
        print(''.join(diff))

if __name__ == "__main__":
    main()