"""GitHub GraphQL for manual fast-unmute (timeline, labels, org project, reopen)."""

import logging

from mute.update_mute_issues import (
    ORG_NAME,
    PROJECT_ID,
    REPO_NAME,
    get_project_v2_fields,
    run_query,
)

_LABEL_ID_CACHE = {}

PROJECT_STATUS_ON_FAST_UNMUTE_REOPEN = 'Observation'
PROJECT_STATUS_ON_FAST_UNMUTE_FAIL = 'Muted'
PROJECT_STATUS_ON_FAST_UNMUTE_SUCCESS = 'Unmuted'


def fetch_issue_closers(issue_numbers):
    """Return {issue_number: {'login': str, 'type': 'User'|'Bot'|''}}.

    We need this because the exported `issues` table does not carry "closed by",
    so for the short list of candidates we query GitHub directly.
    """
    result = {}
    numbers = sorted({int(n) for n in (issue_numbers or []) if n is not None})
    if not numbers:
        return result
    chunk_size = 50
    for i in range(0, len(numbers), chunk_size):
        chunk = numbers[i : i + chunk_size]
        subqueries = []
        for number in chunk:
            subqueries.append(
                f"""
                n{number}: issue(number: {number}) {{
                    timelineItems(last: 1, itemTypes: [CLOSED_EVENT]) {{
                        nodes {{
                            ... on ClosedEvent {{
                                actor {{ __typename login }}
                            }}
                        }}
                    }}
                }}
                """
            )
        query = f"""
        query {{
            repository(owner: "{ORG_NAME}", name: "{REPO_NAME}") {{
                {' '.join(subqueries)}
            }}
        }}
        """
        response = run_query(query)
        repo_data = (response.get('data') or {}).get('repository') or {}
        for number in chunk:
            node = repo_data.get(f'n{number}')
            login = ''
            actor_type = ''
            if node:
                events = (node.get('timelineItems') or {}).get('nodes') or []
                event = events[0] if events else {}
                actor = event.get('actor') or {}
                login = actor.get('login') or ''
                actor_type = actor.get('__typename') or ''
            result[number] = {'login': login, 'type': actor_type}
    return result


def reopen_issue(issue_id):
    """Reopen a closed issue. No-op if already open."""
    state_query = """
    query ($issueId: ID!) {
      node(id: $issueId) {
        ... on Issue { state }
      }
    }
    """
    state_result = run_query(state_query, {'issueId': issue_id})
    state = ((state_result.get('data') or {}).get('node') or {}).get('state')
    if state != 'CLOSED':
        return
    mutation = """
    mutation ($issueId: ID!) {
      reopenIssue(input: {issueId: $issueId}) { issue { id } }
    }
    """
    run_query(mutation, {'issueId': issue_id})


def _issue_project_board_item_id(issue_node_id, project_number):
    """Return Project v2 **item** id for ``issue_node_id`` on board ``project_number``, or ``None``."""
    query = """
    query ($issueId: ID!) {
      node(id: $issueId) {
        ... on Issue {
          projectItems(first: 40) {
            nodes {
              id
              project { number }
            }
          }
        }
      }
    }
    """
    try:
        result = run_query(query, {'issueId': issue_node_id})
    except Exception as exc:
        logging.warning('manual_unmute: projectItems query failed: %s', exc)
        return None
    node = (result.get('data') or {}).get('node') or {}
    want = int(project_number)
    for it in ((node.get('projectItems') or {}).get('nodes')) or []:
        num = (it.get('project') or {}).get('number')
        if num is not None and int(num) == want:
            return it.get('id')
    return None


def _add_issue_to_org_project(project_global_id, issue_node_id):
    """Add issue to org project; return new project **item** id."""
    mutation = """
    mutation ($projectId: ID!, $contentId: ID!) {
      addProjectV2ItemById(input: {projectId: $projectId, contentId: $contentId}) {
        item { id }
      }
    }
    """
    try:
        result = run_query(
            mutation, {'projectId': project_global_id, 'contentId': issue_node_id}
        )
    except Exception as exc:
        logging.warning('manual_unmute: addProjectV2ItemById failed: %s', exc)
        return None
    item = (((result.get('data') or {}).get('addProjectV2ItemById') or {}).get('item') or {})
    return item.get('id')


def set_manual_unmute_project_board_status(issue_node_id, status_label):
    """Set org project ``Status`` (single select) by option name (case-insensitive).

    Issues not yet on the board are added to the project (same behaviour as mute tooling).
    Requires token scope that can read/update org projects.
    """
    label = (status_label or '').strip()
    if not label:
        return
    try:
        project_global_id, project_fields = get_project_v2_fields(ORG_NAME, PROJECT_ID)
    except Exception as exc:
        logging.warning('manual_unmute: could not load project %s fields: %s', PROJECT_ID, exc)
        return
    status_field_id = None
    option_id = None
    want = label.lower()
    for field in project_fields:
        if (field.get('name') or '').lower() != 'status':
            continue
        status_field_id = field.get('id')
        for opt in field.get('options') or []:
            if (opt.get('name') or '').lower() == want:
                option_id = opt.get('id')
                break
        break
    if not status_field_id or not option_id:
        logging.warning(
            'manual_unmute: project %s: Status field or %r option not found; skip board update',
            PROJECT_ID,
            label,
        )
        return

    item_id = _issue_project_board_item_id(issue_node_id, PROJECT_ID)
    if not item_id:
        item_id = _add_issue_to_org_project(project_global_id, issue_node_id)
    if not item_id:
        logging.warning(
            'manual_unmute: could not resolve or create project item for issue (project %s)',
            PROJECT_ID,
        )
        return

    mutation = """
    mutation ($projectId: ID!, $itemId: ID!, $fieldId: ID!, $optionId: String) {
      updateProjectV2ItemFieldValue(input: {
        projectId: $projectId,
        itemId: $itemId,
        fieldId: $fieldId,
        value: { singleSelectOptionId: $optionId }
      }) {
        projectV2Item { id }
      }
    }
    """
    try:
        run_query(
            mutation,
            {
                'projectId': project_global_id,
                'itemId': item_id,
                'fieldId': status_field_id,
                'optionId': option_id,
            },
        )
        logging.info(
            'manual_unmute: set project %s Status to %r',
            PROJECT_ID,
            label,
        )
    except Exception as exc:
        logging.warning('manual_unmute: updateProjectV2ItemFieldValue failed: %s', exc)


def _get_label_id(label_name):
    """Resolve the pre-created label node id (cached). Returns None if missing."""
    if label_name in _LABEL_ID_CACHE:
        return _LABEL_ID_CACHE[label_name]

    query = """
    query ($owner: String!, $name: String!, $labelName: String!) {
      repository(owner: $owner, name: $name) {
        label(name: $labelName) { id }
      }
    }
    """
    result = run_query(
        query,
        {'owner': ORG_NAME, 'name': REPO_NAME, 'labelName': label_name},
    )
    label = (((result.get('data') or {}).get('repository') or {}).get('label') or {})
    label_id = label.get('id')
    if not label_id:
        logging.warning(
            "Label %r not found in %s/%s — create it manually in the repository labels page",
            label_name,
            ORG_NAME,
            REPO_NAME,
        )
        return None
    _LABEL_ID_CACHE[label_name] = label_id
    return label_id


def add_label_to_issue(issue_id, label_name):
    """Attach a label to issue. Idempotent — GitHub ignores duplicates."""
    label_id = _get_label_id(label_name)
    if not label_id:
        return
    mutation = """
    mutation ($labelableId: ID!, $labelIds: [ID!]!) {
      addLabelsToLabelable(input: {labelableId: $labelableId, labelIds: $labelIds}) {
        labelable { __typename }
      }
    }
    """
    try:
        run_query(mutation, {'labelableId': issue_id, 'labelIds': [label_id]})
    except Exception as exc:
        logging.warning(
            'Failed to add label %r to issue %s: %s', label_name, issue_id, exc
        )


def remove_label_from_issue(issue_id, label_name):
    """Detach a label from issue. No-op if the label is not present."""
    label_id = _get_label_id(label_name)
    if not label_id:
        return
    mutation = """
    mutation ($labelableId: ID!, $labelIds: [ID!]!) {
      removeLabelsFromLabelable(input: {labelableId: $labelableId, labelIds: $labelIds}) {
        labelable { __typename }
      }
    }
    """
    try:
        run_query(mutation, {'labelableId': issue_id, 'labelIds': [label_id]})
    except Exception as exc:
        logging.warning(
            'Failed to remove label %r from issue %s: %s', label_name, issue_id, exc
        )


def fetch_issue_label_names(issue_numbers):
    """Return ``{issue_number: {label_name, ...}}`` from GitHub GraphQL."""
    result = {}
    numbers = sorted({int(n) for n in (issue_numbers or []) if n is not None})
    if not numbers:
        return result
    chunk_size = 50
    for i in range(0, len(numbers), chunk_size):
        chunk = numbers[i : i + chunk_size]
        subqueries = [
            f"n{n}: issue(number: {n}) {{ labels(first: 40) {{ nodes {{ name }} }} }}" for n in chunk
        ]
        query = f"""
        query {{
            repository(owner: "{ORG_NAME}", name: "{REPO_NAME}") {{
                {' '.join(subqueries)}
            }}
        }}
        """
        response = run_query(query)
        repo_data = (response.get('data') or {}).get('repository') or {}
        for number in chunk:
            node = repo_data.get(f'n{number}') or {}
            labels = (node.get('labels') or {}).get('nodes') or []
            result[number] = {
                str(label.get('name') or '').strip()
                for label in labels
                if (label or {}).get('name')
            }
    return result


def fetch_issue_numbers_in_manual_unmute_project(issue_numbers):
    """Return issue numbers that currently have a card in configured manual-unmute project."""
    result = set()
    numbers = sorted({int(n) for n in (issue_numbers or []) if n is not None})
    if not numbers:
        return result
    target_project_number = int(PROJECT_ID)
    chunk_size = 50
    for i in range(0, len(numbers), chunk_size):
        chunk = numbers[i : i + chunk_size]
        subqueries = [
            f"n{n}: issue(number: {n}) {{ projectItems(first: 40) {{ nodes {{ project {{ number }} }} }} }}"
            for n in chunk
        ]
        query = f"""
        query {{
            repository(owner: "{ORG_NAME}", name: "{REPO_NAME}") {{
                {' '.join(subqueries)}
            }}
        }}
        """
        response = run_query(query)
        repo_data = (response.get('data') or {}).get('repository') or {}
        for number in chunk:
            node = repo_data.get(f'n{number}') or {}
            items = (node.get('projectItems') or {}).get('nodes') or []
            if any(
                int((item.get('project') or {}).get('number') or -1)
                == target_project_number
                for item in items
            ):
                result.add(number)
    return result


def fetch_issue_states(issue_numbers):
    """Return ``{issue_number: {'id', 'state', 'state_reason'}}}`` from GitHub GraphQL.

    ``state_reason`` mirrors GitHub's ``stateReason`` (e.g. ``COMPLETED`` when closed).
    Issues missing from the response are omitted.
    """
    result = {}
    numbers = sorted({int(n) for n in (issue_numbers or []) if n is not None})
    if not numbers:
        return result
    chunk_size = 50
    for i in range(0, len(numbers), chunk_size):
        chunk = numbers[i : i + chunk_size]
        subqueries = [
            f"n{n}: issue(number: {n}) {{ id state stateReason }}" for n in chunk
        ]
        query = f"""
        query {{
            repository(owner: "{ORG_NAME}", name: "{REPO_NAME}") {{
                {' '.join(subqueries)}
            }}
        }}
        """
        response = run_query(query)
        repo_data = (response.get('data') or {}).get('repository') or {}
        for number in chunk:
            node = repo_data.get(f'n{number}')
            if not node or not node.get('id'):
                continue
            result[number] = {
                'id': node['id'],
                'state': str(node.get('state') or ''),
                'state_reason': str(node.get('stateReason') or ''),
            }
    return result


