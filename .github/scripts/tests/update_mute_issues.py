import os
import re
import requests
from github import Github #pip3 install PyGithub
from urllib.parse import quote, urlencode


ORG_NAME = 'ydb-platform'
REPO_NAME = 'ydb'
PROJECT_ID = '45'
TEST_HISTORY_DASHBOARD = "https://datalens.yandex/4un3zdm0zcnyr"
CURRENT_TEST_HISTORY_DASHBOARD = "https://datalens.yandex/34xnbsom67hcq?"

# Github api (personal access token (classic)) token shoud have permitions to
# repo
# - repo:status
# - repo_deployment
# - public_repo
# admin:org
# project

def run_query(query, variables=None):
    GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
    HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}", "Content-Type": "application/json"}
    request = requests.post(
        'https://api.github.com/graphql', json={'query': query, 'variables': variables}, headers=HEADERS
    )
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception(f"Query failed to run by returning code of {request.status_code}. {query}")


def get_repository(org_name=ORG_NAME, repo_name=REPO_NAME):
    query = """
    {
      organization(login: "%s") {
        repository(name: "%s") {
          id
        }
      }
    }
    """ % (
        org_name,
        repo_name,
    )
    result = run_query(query)
    return result['data']['organization']['repository']


def get_project_v2_fields(org_name=ORG_NAME, project_id=PROJECT_ID):
    query_template = """
   {
      organization(login: "%s") {
        projectV2(number: %s) {
          id
          fields(first: 100) {
            nodes {
              ... on ProjectV2Field {
                id
                name
              }
              ... on ProjectV2SingleSelectField {
                id
                name
                options {
                  id
                  name
                }
              
              }
              
            }
          }
        }
      }
    }
    """
    query = query_template % (org_name, project_id)

    result = run_query(query)
    return (
        result['data']['organization']['projectV2']['id'],
        result['data']['organization']['projectV2']['fields']['nodes'],
    )


def create_and_add_issue_to_project(title, body, project_id=PROJECT_ID, org_name=ORG_NAME, state=None, owner=None):
    """Добавляет issue в проект.

    Args:
        title (str): Название issue.
        body (str): Содержимое issue.
        project_id (int): ID проекта.
        org_name (str): Имя организации.

    Returns:
        None
    """

    result = None
    # Получаем ID полей "State" и "Owner"
    inner_project_id, project_fields = get_project_v2_fields(org_name, project_id)
    state_field_id = None
    owner_field_id = None
    for field in project_fields:
        if field.get('name'):
            if field['name'].lower() == "status":
                state_field_id = field['id']
                state_option_id = None
                if state:
                    for option in field['options']:
                        if option['name'].lower() == state.lower():
                            state_option_id = option['id']
                            break
            if field['name'].lower() == "owner":
                owner_field_id = field['id']
                owner_option_id = None
                if owner:
                    for option in field['options']:
                        if option['name'].lower() == owner.lower():
                            owner_option_id = option['id']
                            break

    if not state_field_id or not owner_field_id:
        raise Exception(f"Не найдены поля 'State' или 'Owner' в проекте {project_id}")
    # get repo
    repo = get_repository()
    # create issue
    query = """
    mutation ($repositoryId: ID!, $title: String!, $body: String!) {
      createIssue(input: {repositoryId: $repositoryId, title: $title, body: $body}) {
        issue {
          id,
          url
        }
      }
    }
    """
    variables = {"repositoryId": repo['id'], "title": title, "body": body}
    issue = run_query(query, variables)
    if not issue.get('errors'):
        print(f"Issue {title} created ")
    else:
        print(f"Error: Issue {title} not created ")
        return result
    issue_id = issue['data']['createIssue']['issue']['id']
    issue_url = issue['data']['createIssue']['issue']['url']

    query_add_to_project = """
     mutation ($projectId: ID!, $issueId: ID!) {
    addProjectV2ItemById(input: {projectId: $projectId, contentId: $issueId}) {
      item {
          id
        }
      }
    }
    """
    variables = {
        "projectId": inner_project_id,
        "issueId": issue_id,
    }
    result_add_to_project = run_query(query_add_to_project, variables)
    item_id = result_add_to_project['data']['addProjectV2ItemById']['item']['id']
    if not result_add_to_project.get('errors'):
        print(f"Issue {issue_url} added to project.")
    else:
        print(f"Error: Issue {title}: {issue_url} not added to project.")
        return result

    for field_name, filed_value, field_id, value_id in [
        ['state', state, state_field_id, state_option_id],
        ['owner', owner, owner_field_id, owner_option_id],
    ]:
        query_modify_fields = """
      mutation ($projectId: ID!, $itemId: ID!, $FieldId: ID!, $OptionId: String) {
        updateProjectV2ItemFieldValue(input: {
          projectId: $projectId,
          itemId: $itemId,
          fieldId: $FieldId,
          value: {
            singleSelectOptionId: $OptionId
          }
        }) {
          projectV2Item {
            id
          }
        }

      }
      """
        variables = {
            "projectId": inner_project_id,
            "itemId": item_id,
            "FieldId": field_id,
            "OptionId": value_id,
        }
        result_modify_field = run_query(query_modify_fields, variables)
        if not result_modify_field.get('errors'):
            print(f"Issue {title}: {issue_url} modified :{field_name} = {filed_value}")
        else:
            print(f"Error: Issue {title}: {issue_url}  not modified")
            return result
    result = {'issue_url': issue_url, 'owner': owner, 'title': title}
    return result


def fetch_all_issues(org_name=ORG_NAME, project_id=PROJECT_ID):
    issues = []
    has_next_page = True
    end_cursor = "null"

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
    while has_next_page:
        query = project_issues_query % (org_name, project_id, end_cursor)

        result = run_query(query)

        if result:
            project_items = result['data']['organization']['projectV2']['items']
            issues.extend(project_items['nodes'])

            page_info = project_items['pageInfo']
            has_next_page = page_info['hasNextPage']
            end_cursor = f"\"{page_info['endCursor']}\"" if page_info['endCursor'] else "null"
        else:
            has_next_page = False

    return issues


def generate_github_issue_title_and_body(test_data):
    owner = test_data[0]['owner']
    branch = test_data[0]['branch']
    test_full_names = [f"{d['full_name']}" for d in test_data]
    test_mute_strings = [f"{d['mute_string']}" for d in test_data]
    summary = [
        f"{d['test_name']}: {d['state']} last {d['days_in_state']} days, at {d['date_window']}: success_rate {d['success_rate']}%, {d['summary']}"
        for d in test_data
    ]

    # Title
    if len(test_full_names) > 1:
        title = f'Mute {test_data[0]["suite_folder"]} {len(test_full_names)} tests'
    else:
        title = f'Mute {test_data[0]["full_name"]}'

    # Преобразование списка тестов в строку и кодирование
    test_string = "\n".join(test_full_names)

    test_mute_strings_string = "\n".join(test_mute_strings)

    summary_string = "\n".join(summary)

    # Создаем ссылку на историю тестов, кодируя параметры

    test_run_history_params = "&".join(
        urlencode({"full_name": f"__in_{test}"})
        for test in test_full_names
    )
    test_run_history_link = f"{CURRENT_TEST_HISTORY_DASHBOARD}{test_run_history_params}"

    # owner
    # Тело сообщения и кодирование
    body_template = (
        f"Mute:<!--mute_list_start-->\n"
        f"{test_string}\n"
        f"<!--mute_list_end-->\n\n"
        f"Branch:<!--branch_list_start-->\n"
        f"{branch}\n"
        f"<!--branch_list_end-->\n\n"
        f"**Add line to [muted_ya.txt](https://github.com/ydb-platform/ydb/blob/main/.github/config/muted_ya.txt):**\n"
        "```\n"
        f"{test_mute_strings_string}\n"
        "```\n\n"
        f"Owner: {owner}\n\n"
        "**Read more in [mute_rules.md](https://github.com/ydb-platform/ydb/blob/main/.github/config/mute_rules.md)**\n\n"
        f"**Summary history:** \n {summary_string}\n"
        "\n\n"
        f"**Test run history:** [link]({test_run_history_link})\n\n"
        f"More info in [dashboard]({TEST_HISTORY_DASHBOARD})"
    )

    return (
        title,
        body_template,
    )


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


def get_muted_tests_from_issues():
    issues = get_issues_and_tests_from_project(ORG_NAME, PROJECT_ID)
    muted_tests = {}
    for issue in issues:
        if issues[issue]["status"] == "Muted" and issues[issue]["state"] != 'CLOSED':
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


def main():

    if "GITHUB_TOKEN" not in os.environ:
        print("Error: Env variable GITHUB_TOKEN is missing, skipping")
        return 1
    else:
        github_token = os.environ["GITHUB_TOKEN"]
    # muted_tests = get_muted_tests_from_issues()

    # create_github_issues(tests)


# create_and_add_issue_to_project('test issue','test_issue_body', state = 'Muted', owner = 'fq')
# print(1)
# update_issue_state(muted_tests, github_token, "closed")

if __name__ == "__main__":
    main()