import os
import requests
from github import Github #pip3 install PyGithub
from urllib.parse import quote_plus


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

GITHUB_MAX_BODY_LENGTH = 65000  # Setting slightly below 65536 to be safe

def truncate_issue_body(body):
    """Truncates issue body if it exceeds GitHub's maximum length.
    
    Args:
        body (str): The original issue body
        
    Returns:
        str: Truncated body if necessary, with a note about truncation
    """
    if len(body) <= GITHUB_MAX_BODY_LENGTH:
        return body
        
    truncation_message = "\n\n... [Content truncated due to length limitations] ..."
    available_length = GITHUB_MAX_BODY_LENGTH - len(truncation_message)
    
    # Find the last newline before the cutoff to avoid cutting in the middle of a line
    last_newline = body.rfind('\n', 0, available_length)
    if last_newline == -1:
        last_newline = available_length
        
    truncated_body = body[:last_newline] + truncation_message
    return truncated_body

def handle_github_errors(response):
    if 'errors' in response:
        for error in response['errors']:
            if error['type'] == 'INSUFFICIENT_SCOPES':
                print("Error: Insufficient Scopes")
                print("Message:", error['message'])
                raise Exception("Insufficient scopes. Please update your token's scopes.")
            # Handle other types of errors if necessary
            else:
                print("Unknown error type:", error.get('type', 'No type'))
                print("Message:", error.get('message', 'No message available'))
                raise Exception("GraphQL Error: " + error.get('message', 'Unknown error'))

def run_query(query, variables=None):
    GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
    HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}", "Content-Type": "application/json"}
    request = requests.post(
        'https://api.github.com/graphql', json={'query': query, 'variables': variables}, headers=HEADERS
    )
    if request.status_code == 200:
        handle_github_errors(request.json())
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
    # Truncate body if necessary
    body = truncate_issue_body(body)
    
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
        title = f'Mute {test_data[0]["suite_folder"]} {len(test_full_names)} tests in {branch}'
    else:
        title = f'Mute {test_data[0]["full_name"]} in {branch}'

    # Преобразование списка тестов в строку и кодирование
    test_string = "\n".join(test_full_names)

    test_mute_strings_string = "\n".join(test_mute_strings)

    summary_string = "\n".join(summary)

    # Создаем ссылку на историю тестов, кодируя параметры

    test_name_params = "&".join(
        f"full_name={quote_plus(f'__in_{test}')}"
        for test in test_full_names
    )
    branch_param = f"&branch={branch}"
    test_run_history_link = f"{CURRENT_TEST_HISTORY_DASHBOARD}{test_name_params}{branch_param}"

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
            print(f"Branch: {(',').join(branches) if branches else 'main'}")
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
        if issues[issue]["state"] != 'CLOSED':
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
                            'id': issue,
                        }
                    )

    return muted_tests


def close_issue(issue_id):
    """Closes GitHub issue using GraphQL API.
    
    Args:
        issue_id (str): GitHub issue node ID
    """
    query = """
    mutation ($issueId: ID!) {
      closeIssue(input: {issueId: $issueId}) {
        issue {
          id
          url
        }
      }
    }
    """
    variables = {"issueId": issue_id}
    result = run_query(query, variables)
    if not result.get('errors'):
        print(f"Issue {issue_id} closed")
    else:
        print(f"Error: Issue {issue_id} not closed")

def add_issue_comment(issue_id, comment):
    """Adds a comment to GitHub issue using GraphQL API.
    
    Args:
        issue_id (str): GitHub issue node ID
        comment (str): Comment text
    """
    query = """
    mutation ($issueId: ID!, $body: String!) {
      addComment(input: {subjectId: $issueId, body: $body}) {
        commentEdge {
          node {
            id
          }
        }
      }
    }
    """
    variables = {"issueId": issue_id, "body": comment}
    result = run_query(query, variables)
    if not result.get('errors'):
        print(f"Added comment to issue {issue_id}")
    else:
        print(f"Error: Failed to add comment to issue {issue_id}")

def update_issue_status(issue_id, status_field_id, status_option_id, issue_url):
    """Updates the status of an issue in the project.
    
    Args:
        issue_id (str): The ID of the issue
        status_field_id (str): The ID of the status field
        status_option_id (str): The ID of the status option to set
    """
    # Get project's global ID first
    query = """
    {
      organization(login: "%s") {
        projectV2(number: %s) {
          id
        }
      }
    }
    """ % (ORG_NAME, PROJECT_ID)
    
    result = run_query(query)
    if not result.get('data'):
        print("Error: Failed to fetch project ID")
        return
        
    project_global_id = result['data']['organization']['projectV2']['id']
    
    # Now update the status
    query = """
    mutation ($projectId: ID!, $itemId: ID!, $fieldId: ID!, $optionId: String) {
      updateProjectV2ItemFieldValue(input: {
        projectId: $projectId,
        itemId: $itemId,
        fieldId: $fieldId,
        value: {
          singleSelectOptionId: $optionId
        }
      }) {
        projectV2Item {
          id
        }
      }
    }
    """
    variables = {
        "projectId": project_global_id,
        "itemId": issue_id,
        "fieldId": status_field_id,
        "optionId": status_option_id
    }
    result = run_query(query, variables)
    if not result.get('errors'):
        print(f"Updated status for issue {issue_url}")
    else:
        print(f"Error: Failed to update status for issue {issue_url}")

def update_all_closed_issues_status(status_field_id, unmuted_option_id):
    """Updates status to Unmuted for all closed issues in the project.
    
    Args:
        status_field_id (str): The ID of the status field
        unmuted_option_id (str): The ID of the Unmuted status option
    """
    has_next_page = True
    end_cursor = "null"
    
    while has_next_page:
        query = """
        {
          organization(login: "%s") {
            projectV2(number: %s) {
              items(first: 100, after: %s) {
                nodes {
                  id
                  content {
                    ... on Issue {
                      id
                      state
                      url
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
        """ % (ORG_NAME, PROJECT_ID, end_cursor)
        
        result = run_query(query)
        if not result.get('data'):
            print("Error: Failed to fetch project items")
            return
            
        items = result['data']['organization']['projectV2']['items']['nodes']
        for item in items:
            if item['content'] and item['content']['state'] == 'CLOSED':
                # Check if status is not already Unmuted
                current_status = None
                for field_value in item['fieldValues']['nodes']:
                    if (field_value.get('field', {}).get('name', '').lower() == 'status' and 
                        'name' in field_value):
                        current_status = field_value.get('name')
                        break
                
                if current_status != 'Unmuted':
                    update_issue_status(item['id'], status_field_id, unmuted_option_id, item['content']['url'])
        
        # Update pagination info
        page_info = result['data']['organization']['projectV2']['items']['pageInfo']
        has_next_page = page_info['hasNextPage']
        end_cursor = f"\"{page_info['endCursor']}\"" if page_info['endCursor'] else "null"

def get_issue_comments(issue_id):
    """Gets all comments for an issue.
    
    Args:
        issue_id (str): The ID of the issue
        
    Returns:
        list: List of comment bodies
    """
    query = """
    {
      node(id: "%s") {
        ... on Issue {
          comments(first: 100) {
            nodes {
              body
            }
          }
        }
      }
    }
    """ % issue_id
    
    result = run_query(query)
    if not result.get('data', {}).get('node', {}).get('comments', {}).get('nodes'):
        return []
        
    return [comment['body'] for comment in result['data']['node']['comments']['nodes']]

def has_unmute_comment(comments, unmuted_tests):
    """Checks if there's already a comment about unmuting these tests.
    
    Args:
        comments (list): List of comment bodies
        unmuted_tests (list): List of unmuted test names
        
    Returns:
        bool: True if a comment about unmuting these tests exists
    """
    test_set = set(unmuted_tests)
    for comment in comments:
        if "tests have been unmuted" in comment:
            # Extract test names from the comment
            comment_tests = set()
            for line in comment.split('\n'):
                if line.startswith('- Test '):
                    test_name = line[7:]  # Remove '- Test ' prefix
                    comment_tests.add(test_name.replace(' unmuted', ''))
            # If all current unmuted tests are in the comment, we don't need a new one
            if test_set.issubset(comment_tests):
                return True
    return False

def close_unmuted_issues(muted_tests_set, do_not_close_issues=False):
    """Closes issues where all tests are no longer muted.
    
    Args:
        muted_tests_set (set): Set of currently muted test names
        do_not_close_issues (bool): If True, issues will NOT be closed. If False, issues will be closed.
        
    Returns:
        tuple: (closed_issues, partially_unmuted_issues) - lists of dictionaries containing information about closed and partially unmuted issues
    """
    issues = get_muted_tests_from_issues()
    closed_issues = []
    partially_unmuted_issues = []
    
    # Get status field ID and Unmuted option ID
    _, project_fields = get_project_v2_fields(ORG_NAME, PROJECT_ID)
    status_field_id = None
    unmuted_option_id = None
    for field in project_fields:
        if field.get('name') and field['name'].lower() == "status":
            status_field_id = field['id']
            for option in field['options']:
                if option['name'].lower() == "unmuted":
                    unmuted_option_id = option['id']
                    break
            break
    
    if not status_field_id or not unmuted_option_id:
        print("Warning: Could not find status field or Unmuted option")
        return closed_issues, partially_unmuted_issues
    
    # First, group tests by issue ID
    tests_by_issue = {}
    for test_name, issue_data_list in issues.items():
        for issue_data in issue_data_list:
            issue_id = issue_data['id']
            if issue_id not in tests_by_issue:
                tests_by_issue[issue_id] = {
                    'tests': set(),
                    'url': issue_data['url'],
                    'state': issue_data['state'],
                    'status': issue_data['status']
                }
            tests_by_issue[issue_id]['tests'].add(test_name)
    
    # Then check each issue
    for issue_id, issue_info in tests_by_issue.items():
        if issue_info['state'] != 'CLOSED':
            unmuted_tests = [test for test in issue_info['tests'] if test not in muted_tests_set]
            if unmuted_tests:
                # Get existing comments
                existing_comments = get_issue_comments(issue_id)
                
                # If all tests are unmuted, close the issue
                if len(unmuted_tests) == len(issue_info['tests']):
                    if not has_unmute_comment(existing_comments, unmuted_tests):
                        comment = "All tests have been unmuted:\n" + "\n".join(f"- Test {test}" for test in sorted(unmuted_tests))
                        add_issue_comment(issue_id, comment)
                    if not do_not_close_issues:
                        close_issue(issue_id)
                    closed_issues.append({
                        'url': issue_info['url'],
                        'tests': sorted(list(issue_info['tests']))
                    })
                    print(f"{'Would close' if do_not_close_issues else 'Closed'} issue as all its tests are no longer muted: {issue_info['url']}")
                    print(f"Unmuted tests: {', '.join(sorted(unmuted_tests))}")
                # If some tests are unmuted but not all, just add a comment if needed
                else:
                    if not has_unmute_comment(existing_comments, unmuted_tests):
                        comment = "Some tests have been unmuted:\n" + "\n".join(f"- Test {test}" for test in sorted(unmuted_tests))
                        add_issue_comment(issue_id, comment)
                        print(f"Added comment about unmuted tests to issue: {issue_info['url']}")
                    print(f"Unmuted tests: {', '.join(sorted(unmuted_tests))}")
                    
                    # Add to partially unmuted issues list
                    still_muted_tests = [test for test in issue_info['tests'] if test in muted_tests_set]
                    partially_unmuted_issues.append({
                        'url': issue_info['url'],
                        'unmuted_tests': sorted(unmuted_tests),
                        'still_muted_tests': sorted(still_muted_tests)
                    })
    
    # Update status for all closed issues
    print("Updating status for all closed issues...")
    update_all_closed_issues_status(status_field_id, unmuted_option_id)
    
    return closed_issues, partially_unmuted_issues


def main():

    if "GITHUB_TOKEN" not in os.environ:
        print("Error: Env variable GITHUB_TOKEN is missing, skipping")
        return 1
    else:
        github_token = os.environ["GITHUB_TOKEN"]

if __name__ == "__main__":
    main()