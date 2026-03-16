from urllib.request import urlopen
import json
import toml

BASE_URL = "https://api.github.com/repos/python/cpython/pulls?per_page=1000&state=all"

def main():
    all_issues = []
    for page in range(1, 11):
        with urlopen(f"{BASE_URL}&page={page}") as response:
            issues = json.loads(response.read())
            if not issues:
                break
            all_issues.extend(issues)
            print(f"Page: {page} Total Issues: {len(all_issues)}")
    with open("issues.toml", "w") as f:
        f.write(toml.dumps({"data": all_issues}))

if __name__ == "__main__":
    main()
