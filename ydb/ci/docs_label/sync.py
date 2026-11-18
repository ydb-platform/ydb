#!/usr/bin/env python3
"""Reconcile documentation labels without downloading or executing PR code."""
import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


REPOSITORY = 'ydb-platform/ydb'
LABEL = 'documentation'
PREFIX = 'ydb/docs/'
FILE_LIMIT = 3000
PR_FIELDS = '''number state headRefOid baseRefOid
  labels(first:100) { nodes { name } pageInfo { hasNextPage } }'''


class GitHub:
    def __init__(self, token):
        if not token:
            raise ValueError('GitHub token is required')
        self.token = token
        self.calls = 0

    def request(self, method, path, payload=None, missing_ok=False):
        # Never send credentials to a URL supplied by a PR or a pagination link.
        if not path.startswith('/') or path.startswith('//'):
            raise ValueError('Expected a GitHub API path')
        headers = {'Authorization': 'Bearer ' + self.token,
                   'Accept': 'application/vnd.github+json',
                   'X-GitHub-Api-Version': '2022-11-28',
                   'User-Agent': 'ydb-docs-label-reactor'}
        data = None if payload is None else json.dumps(payload).encode()
        for attempt in range(3):
            self.calls += 1
            req = Request('https://api.github.com' + path, data=data,
                          headers=headers, method=method)
            try:
                with urlopen(req, timeout=45) as response:
                    content = response.read()
                    return json.loads(content) if content else None
            except HTTPError as error:
                if error.code == 404 and missing_ok:
                    return None
                if error.code >= 500 and attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                # Do not log response bodies, request headers, or token values.
                raise RuntimeError(f'GitHub {method} {path}: HTTP {error.code}') from None
            except (URLError, TimeoutError):
                if attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                raise RuntimeError(f'GitHub {method} {path}: connection failed') from None

    def graphql(self, query, variables=None):
        result = self.request('POST', '/graphql', {'query': query, 'variables': variables or {}})
        if result.get('errors'):
            raise RuntimeError('GitHub GraphQL returned errors; refusing partial results')
        return result['data']

    def open_prs(self):
        cursor = None
        scanned = 0
        query = '''query($cursor:String) { repository(owner:"ydb-platform",name:"ydb") {
          pullRequests(states:OPEN,first:100,after:$cursor,
            orderBy:{field:CREATED_AT,direction:ASC}) {
            nodes { ''' + PR_FIELDS + ''' }
            pageInfo { hasNextPage endCursor }
          } } }'''
        while True:
            page = self.graphql(query, {'cursor': cursor})['repository']['pullRequests']
            scanned += len(page['nodes'])
            print(f'Read metadata for {scanned} open PRs', file=sys.stderr)
            yield from page['nodes']
            if not page['pageInfo']['hasNextPage']:
                return
            next_cursor = page['pageInfo']['endCursor']
            if not next_cursor or next_cursor == cursor:
                raise RuntimeError('PR pagination did not advance')
            cursor = next_cursor

    def fresh_pr(self, number):
        query = '''query($number:Int!) { repository(owner:"ydb-platform",name:"ydb") {
          pullRequest(number:$number) { ''' + PR_FIELDS + ''' } } }'''
        return self.graphql(query, {'number': number})['repository']['pullRequest']

    def file_pages(self, pending):
        fields = []
        for index, item in enumerate(pending):
            count = min(100, FILE_LIMIT - item['seen'])
            fields.append(f'''p{index}: pullRequest(number:{int(item['pr']['number'])}) {{
              state headRefOid baseRefOid files(first:{count},after:{json.dumps(item['cursor'])}) {{
                nodes {{ path }} pageInfo {{ hasNextPage endCursor }}
              }} }}''')
        query = 'query { repository(owner:"ydb-platform",name:"ydb") { ' + ' '.join(fields) + ' } }'
        result = self.graphql(query)['repository']
        return [result[f'p{i}'] for i in range(len(pending))]

    def has_label(self, pr):
        if any(label['name'] == LABEL for label in pr['labels']['nodes']):
            return True
        if not pr['labels']['pageInfo']['hasNextPage']:
            return False
        page = 1
        while True:
            labels = self.request('GET', f'/repos/{REPOSITORY}/issues/{pr["number"]}/labels?per_page=100&page={page}')
            if any(label['name'] == LABEL for label in labels):
                return True
            if len(labels) < 100:
                return False
            page += 1

    def set_label(self, number, present):
        path = f'/repos/{REPOSITORY}/issues/{number}/labels'
        if present:
            self.request('POST', path, {'labels': [LABEL]})
        else:
            self.request('DELETE', path + '/' + LABEL, missing_ok=True)


def same_revision(a, b):
    return b is not None and b['state'] == 'OPEN' and all(
        a[key] == b[key] for key in ('headRefOid', 'baseRefOid'))


def classify(prs, github):
    """Return (PR, desired label or None, reason); None preserves current state."""
    items = [{'pr': pr, 'seen': 0, 'cursor': None, 'desired': None, 'reason': 'pending'} for pr in prs]

    def consume(item, page):
        nodes = page['nodes']
        item['seen'] += len(nodes)
        if any(node['path'].startswith(PREFIX) for node in nodes):
            item.update(desired=True, reason='docs')
        elif item['seen'] >= FILE_LIMIT:
            item['reason'] = 'file-list-limit'
        elif not page['pageInfo']['hasNextPage']:
            item.update(desired=False, reason='no-docs')
        else:
            cursor = page['pageInfo']['endCursor']
            if not cursor or cursor == item['cursor']:
                raise RuntimeError('File pagination did not advance')
            item['cursor'] = cursor

    for item in items:
        if item['pr']['state'] != 'OPEN':
            item['reason'] = 'closed'
        elif 'files' in item['pr']:
            consume(item, item['pr']['files'])
    while True:
        pending = [item for item in items if item['reason'] == 'pending']
        if not pending:
            return [(item['pr'], item['desired'], item['reason']) for item in items]
        print(f'Reading file pages for {len(pending)} PRs', file=sys.stderr)
        # Eight independent read requests at a time, each bounded to ten PRs.
        batches = [pending[start:start + 10] for start in range(0, len(pending), 10)]
        with ThreadPoolExecutor(max_workers=8) as pool:
            for batch, results in zip(batches, pool.map(github.file_pages, batches)):
                for item, result in zip(batch, results):
                    if not same_revision(item['pr'], result):
                        item['reason'] = 'changed-during-scan'
                    else:
                        consume(item, result['files'])


def reconcile(github, apply=False):
    report = {'mode': 'apply' if apply else 'dry-run', 'scanned': 0,
              'unchanged': 0, 'changes': [], 'skipped': []}
    # Finish the read-only scan before any mutation. API failures cannot turn a
    # partial file list into a decision to remove the label.
    decisions = classify(list(github.open_prs()), github)
    for pr, desired, reason in decisions:
        report['scanned'] += 1
        number = pr['number']
        if desired is None:
            report['skipped'].append({'pr': number, 'reason': reason})
            continue
        if github.has_label(pr) == desired:
            report['unchanged'] += 1
            continue
        fresh = github.fresh_pr(number)
        if not same_revision(pr, fresh):
            report['skipped'].append({'pr': number, 'reason': 'changed-before-write'})
            continue
        # Refresh labels immediately before mutation, not from an event payload.
        if github.has_label(fresh) == desired:
            report['unchanged'] += 1
            continue
        if apply:
            github.set_label(number, desired)
        report['changes'].append({'pr': number, 'action': 'add' if desired else 'remove'})
    report['api_calls'] = github.calls
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='Write labels; default is read-only')
    args = parser.parse_args()
    github = GitHub(os.environ.get('GH_TOKEN'))
    print(json.dumps(reconcile(github, apply=args.apply), indent=2))


if __name__ == '__main__':
    main()
