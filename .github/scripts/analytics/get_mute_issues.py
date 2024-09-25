import os
import re
import requests

ORG_NAME = 'ydb-platform'
PROJECT_ID = '45'
query_template = """
{
  organization(login: "%s") {
    projectV2(number: %s) {
      id
      title
      items(first: 100, after: %s) {
        nodes {
          content {
            ... on Issue {
              id
              title
              url
              state
              body
              createdAt
            }
          }
          fieldValues(first: 20) {
            nodes {
              ... on ProjectV2ItemFieldSingleSelectValue {
                field {
                  ... on ProjectV2SingleSelectField {
                    name
                  }
                }
                name
                id
                updatedAt
              }
              ... on ProjectV2ItemFieldLabelValue {
                labels(first: 20) {
                  nodes {
                    id
                    name
                  }
                }
              }
              ... on ProjectV2ItemFieldTextValue {
                text
                id
                updatedAt
                creator {
                  url
                }
              }
              ... on ProjectV2ItemFieldMilestoneValue {
                milestone {
                  id
                }
              }
              ... on ProjectV2ItemFieldRepositoryValue {
                repository {
                  id
                  url
                }
              }
            }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
  }
}
"""


def run_query(query, headers):
    request = requests.post('https://api.github.com/graphql', json={'query': query}, headers=headers)
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception(f"Query failed to run by returning code of {request.status_code}. {query}")


def fetch_all_issues(org_name, project_id):
    issues = []
    has_next_page = True
    end_cursor = "null"

    while has_next_page:
        query = query_template % (org_name, project_id, end_cursor)
        GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
        headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
        result = run_query(query, headers)

        if result:
            project_items = result['data']['organization']['projectV2']['items']
            issues.extend(project_items['nodes'])

            page_info = project_items['pageInfo']
            has_next_page = page_info['hasNextPage']
            end_cursor = f"\"{page_info['endCursor']}\"" if page_info['endCursor'] else "null"
        else:
            has_next_page = False

    return issues


def parse_body(body):
    tests = []
    branches = []
    prepared_body = ''
    start_mute_list = "<!--mute_list_start-->"
    end_mute_list = "<!--mute_list_end-->"
    start_branch_list = "<!--branch_list_start-->"
    end_branch_list = "<!--branch_list_end-->"

    # tests
    if all(x in body for x in [start_mute_list, end_mute_list]):
        idx1 = body.find(start_mute_list)
        idx2 = body.find(end_mute_list)
        lines = body[idx1 + len(start_mute_list) + 1 : idx2].split('\n')
    else:
        if body.startswith('Mute:'):
            prepared_body = body.split('Mute:', 1)[1].strip()
        elif body.startswith('Mute'):
            prepared_body = body.split('Mute', 1)[1].strip()
        elif body.startswith('ydb'):
            prepared_body = body
        lines = prepared_body.split('**Add line to')[0].split('\n')
    tests = [line.strip() for line in lines if line.strip().startswith('ydb/')]

    # branch
    if all(x in body for x in [start_branch_list, end_branch_list]):
        idx1 = body.find(start_branch_list)
        idx2 = body.find(end_branch_list)
        branches = body[idx1 + len(start_branch_list) + 1 : idx2].split('\n')
    else:
        branches = ['main']

    return tests, branches


def get_issues_and_tests_from_project(ORG_NAME, PROJECT_ID):
    issues = fetch_all_issues(ORG_NAME, PROJECT_ID)
    issues_prepared = {}
    for issue in issues:
        content = issue['content']
        if content:
            body = content['body']

            # for debug
            if content['id'] == 'I_kwDOGzZjoM6V3BoE':
                print(1)
            #

            tests, branches = parse_body(body)

            field_values = issue.get('fieldValues', {}).get('nodes', [])
            for field_value in field_values:
                field_name = field_value.get('field', {}).get('name', '').lower()

                if field_name == "status" and 'name' in field_value:
                    status = field_value.get('name', 'N/A')
                    status_updated = field_value.get('updatedAt', '1970-01-0901T00:00:01Z')
                elif field_name == "owner" and 'name' in field_value:
                    owner = field_value.get('name', 'N/A')

            print(f"Issue ID: {content['id']}")
            print(f"Title: {content['title']}")
            print(f"URL: {content['url']}")
            print(f"State: {content['state']}")
            print(f"CreatedAt: {content['createdAt']}")
            print(f"Status: {status}")
            print(f"Status updated: {status_updated}")
            print(f"Owner: {owner}")
            print("Tests:")

            issues_prepared[content['id']] = {}
            issues_prepared[content['id']]['title'] = content['title']
            issues_prepared[content['id']]['url'] = content['url']
            issues_prepared[content['id']]['state'] = content['state']
            issues_prepared[content['id']]['createdAt'] = content['createdAt']
            issues_prepared[content['id']]['status_updated'] = status_updated
            issues_prepared[content['id']]['status'] = status
            issues_prepared[content['id']]['owner'] = owner
            issues_prepared[content['id']]['tests'] = []
            issues_prepared[content['id']]['branches'] = branches

            for test in tests:
                issues_prepared[content['id']]['tests'].append(test)
                print(f"- {test}")
            print('\n')

    return issues_prepared


def get_muted_tests():
    issues = get_issues_and_tests_from_project(ORG_NAME, PROJECT_ID)
    muted_tests = {}
    for issue in issues:
        if issues[issue]["status"] == "Muted":
            for test in issues[issue]['tests']:
                if test not in muted_tests:
                    muted_tests[test] = []
                    muted_tests[test].append(
                        {
                            'url': issues[issue]['url'],
                            'createdAt': issues[issue]['createdAt'],
                            'status_updated': issues[issue]['status_updated'],
                        }
                    )

    return muted_tests


def main():
    if "GITHUB_TOKEN" not in os.environ:
        print("Error: Env variable GITHUB_TOKEN is missing, skipping")
        return 1
    get_muted_tests()


if __name__ == "__main__":
    main()