"""User-visible GitHub comments for the manual fast-unmute flow."""


def format_bullet_list(tests):
    return '\n'.join(f"- `{name}`" for name in sorted(set(tests)))


COMMENT_ENTER = """🚀 **Fast-unmute started**

{closer_mention_line}
**Status** → **Watching**

These tests will be monitored in CI for {window_days} days. If they pass cleanly at least {min_runs} times — they'll be unmuted:

{tests_bullet_list}

If everything goes green before the deadline — issue closes, status **Unmuted**.
If any test stays red — issue reopens, status **Muted**.

> ✋ No action needed — the bot will handle everything. Please don't edit `muted_ya.txt` manually.

🔗 [Workflow run]({workflow_run_url})
"""


COMMENT_SUCCESS = """✅ **Fast-unmute succeeded**

All tests on this issue went green in CI before the deadline.

**Status** → **Unmuted**. Label `manual-fast-unmute` removed.

🔗 [Workflow run]({workflow_run_url})
"""


COMMENT_PROGRESS = """📊 **Fast-unmute progress**

These tests are already unmuted in CI (removed from tracking):

{unmuted_bullets}

Other tests on this issue are still being monitored.

🔗 [Workflow run]({workflow_run_url})
"""


COMMENT_ABANDON_NOT_COMPLETED = """🛑 **Fast-unmute stopped**

Tracking was cancelled. **Status** → **Muted**.

🔗 [Workflow run]({workflow_run_url})
"""


COMMENT_TTL_INCOMPLETE = """❌ **Fast-unmute: deadline passed**

Not all tests went green within {ttl_days} days. Issue reopened, **Status** → **Muted**.

**Green** (already unmuted or will be shortly):
{graduated_bullets}

**Still red** (these caused the reopen):
{stuck_bullets}

**Other tests cleared in this run:**
{cleared_other_bullets}

🔗 [Workflow run]({workflow_run_url})
"""