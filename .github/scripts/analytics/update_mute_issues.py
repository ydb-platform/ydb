import os
import re
import requests
from github import Github
from urllib.parse import quote, urlencode


ORG_NAME = 'ydb-platform'
REPO_NAME = f'{ORG_NAME}/ydb'
PROJECT_ID = '45'
TEST_HISTORY_DASHBOARD = "https://datalens.yandex/4un3zdm0zcnyr"
CURRENT_TEST_HISTORY_DASHBOARD = "https://datalens.yandex/34xnbsom67hcq?"

project_issues_query = """
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
        query = project_issues_query % (org_name, project_id, end_cursor)
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

def generate_github_issue_title_and_body( test_names, owner, summary_history):
    base_url = "https://github.com/ydb-platform/ydb/issues/new"

    # Кодируем заголовок
    title = f'Mute {len(test_names)} tests'
    title_encoded = quote(title)

    # Преобразование списка тестов в строку и кодирование
    test_list = "\n".join(test_names)
    test_list_encoded = quote(test_list, safe='')
    
    # Создаем ссылку на историю тестов, кодируя параметры
    
    test_run_history_params = "&".join(
        urlencode({"full_name": f"__in_{test}"})
        for test in test_names
    )
    test_run_history_link = f"{CURRENT_TEST_HISTORY_DASHBOARD}{test_run_history_params}"

    # Тело сообщения и кодирование
    body_template = (
        f"Mute:<!--mute_list_start-->\n"
        f"{test_list}\n"
        f"<!--mute_list_end-->\n\n"
        f"**Add line to [muted_ya.txt](https://github.com/ydb-platform/ydb/blob/main/.github/config/muted_ya.txt):**\n"
        "```\n"
        f"{test_list}\n"
        "```\n\n"
        f"Owner: {owner}\n\n"
        "**Read more in [mute_rules.md](https://github.com/ydb-platform/ydb/blob/main/.github/config/mute_rules.md)**\n\n"
        f"**Summary history:** in window {summary_history}\n"
        "\n\n"
        f"**Test run history:** [link]({test_run_history_link})\n\n"
        f"More info in [dashboard]({TEST_HISTORY_DASHBOARD})"
    )
    
    body_encoded = quote(body_template, safe='')
    return title_encoded, body_encoded,
  
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
    all_issues_with_contet = {}
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

            all_issues_with_contet[content['id']] = {}
            all_issues_with_contet[content['id']]['title'] = content['title']
            all_issues_with_contet[content['id']]['url'] = content['url']
            all_issues_with_contet[content['id']]['state'] = content['state']
            all_issues_with_contet[content['id']]['createdAt'] = content['createdAt']
            all_issues_with_contet[content['id']]['status_updated'] = status_updated
            all_issues_with_contet[content['id']]['status'] = status
            all_issues_with_contet[content['id']]['owner'] = owner
            all_issues_with_contet[content['id']]['tests'] = []
            all_issues_with_contet[content['id']]['branches'] = branches

            for test in tests:
                all_issues_with_contet[content['id']]['tests'].append(test)
                print(f"- {test}")
            print('\n')

    return all_issues_with_contet


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
                            'status': issues[issue]['status'],
                            'state': issues[issue]['state'],
                            'branches': issues[issue]['branches'],
                        }
                    )

    return muted_tests

def create_github_issues(tests, github_token, REPO_NAME):
    """Создает issue в GitHub репозитории для каждого теста в словаре.

    Args:
        tests (dict): Словарь тестов, где ключ - название теста, значение - body issue.
        github_token (str): Токен доступа к GitHub.
        REPO_NAME (str): Имя репозитория.

    Returns:
        None
    """

    g = Github(github_token)
    repo = g.get_repo(REPO_NAME)

    for test_name, test_body in tests.items():
        issue = repo.create_issue(
            title=f"Тест: {test_name}",
            body=test_body
        )
        print(f"Создан issue для теста {test_name}: {issue.html_url}")


def update_issue_state(muted_tests, github_token, new_state="closed"):
    """Обновляет состояние issue для muted тестов.

    Args:
        muted_tests (dict): Словарь замученных тестов.
        github_token (str): Токен доступа к GitHub.
        new_state (str): Новое состояние issue (по умолчанию "closed").

    Returns:
        None
    """
    g = Github(github_token)
    repo = g.get_repo(REPO_NAME)

    for test_name, issues in muted_tests.items():
        for issue_data in issues:
            issue_url = issue_data['url']
            issue = repo.get_issue(int(issue_url.split('/')[-1]))
            issue.edit(state=new_state)
            print(f"Обновлено состояние issue {issue_url} на {new_state}")

def main():
    if "GITHUB_TOKEN" not in os.environ:
        print("Error: Env variable GITHUB_TOKEN is missing, skipping")
        return 1
    #muted_tests = get_muted_tests()
    github_token = os.environ["GITHUB_TOKEN"]
    create_github_issues(tests, github_token, REPO_NAME)
    #update_issue_state(muted_tests, github_token, "closed")

if __name__ == "__main__":
    main()