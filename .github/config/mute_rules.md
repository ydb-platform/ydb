## How to Mute a Test

### Through a PR Report

- {% cut "Through the [Test history](https://datalens.yandex/4un3zdm0zcnyr?tab=A4) dashboard" %}
  
  - Enter the test name or path in the `full_name contain` field, click **Apply** - the search is done by the occurrence.  ![image.png](/kikimr/ydb-qa/mute-autotests/.files/image-5.png =800x)

  - Click the `Mute` link, which will create a draft issue in GitHub.

  {% endcut %}


* Add the issue to the [Mute and Un-mute](https://github.com/orgs/ydb-platform/projects/45) project.
* Set the `status` to {red}(`Mute`).
* Set the `owner` field to the team name (see the issue for the owner's name). ![image.png](/kikimr/ydb-qa/mute-autotests/.files/image-3.png =750x)
* Open [muted_ya.txt](https://github.com/ydb-platform/ydb/blob/main/.github/config/muted_ya.txt) in a new tab and edit it.
* Copy the line under `Add line to muted_ya.txt` (for example, like in the screenshot, `ydb/core/kqp/ut/query KqpStats.SysViewClientLost`) and add it to [muted_ya.txt](https://github.com/ydb-platform/ydb/blob/main/.github/config/muted_ya.txt).
* Edit the branch for merging, for example, replace `{username}-patch-1` with `{username}/mute`.
* Create a PR - copy the PR name from the issue name.
* Copy the issue description to the PR, keep the line `Not for changelog (changelog entry is not required)`.
* Merge.
* You are awesome!

## Flaky Tests

### Who and When Monitors Flaky Tests

The CI on-call engineer checks flaky tests once a day (on working days). 

- Open the [Flaky](https://datalens.yandex/4un3zdm0zcnyr) dashboard.
- Perform the sections **[Mute Flaky Test](https://wiki.yandex-team.ru/kikimr/ydb-qa/mute-autotests/#myutim-flapayushie-testy)** and **[Test Flaps More - Need to Unmute](https://wiki.yandex-team.ru/kikimr/ydb-qa/mute-autotests/#test-bolshe-ne-flapaet-nado-razmyutit)**.

### Mute Flaky Tests

Open the [Flaky](https://datalens.yandex/4un3zdm0zcnyr) dashboard.

- Select today's date.
- Look at the tests in the Mute candidate table.

![image.png](/kikimr/ydb-qa/mute-autotests/.files/image-1.png =800x)

- Select today's date in the `date_window`.
- Select `days_ago_window = 5` (how many days back from the selected day to calculate statistics). Currently, there are calculations for 1 day and 5 days ago.
  * If you want to understand how long ago and how often the test started failing, you can click the `history` link in the table (loading may take time) or select `days_ago_window = 1`.
- For `days_ago_window = 5`, set the values to filter out isolated failures and low run counts:
  * `fail_count >= 3`
  * `run_count >= 10`
- Click the `Mute` link, which will create a draft issue in GitHub.
  * Add the issue to the [Mute and Un-mute](https://github.com/orgs/ydb-platform/projects/45) project.
  * Set the `status` to {red}(`Mute`).
  * Set the `owner` field to the team name (see the issue for the owner's name). ![image.png](/kikimr/ydb-qa/mute-autotests/.files/image-3.png =700x)
  * Copy the line under `Add line to muted_ya.txt` (for example, like in the screenshot, `ydb/core/kqp/ut/query KqpStats.SysViewClientLost`) and add it to [muted_ya.txt](https://github.com/ydb-platform/ydb/blob/main/.github/config/muted_ya.txt).
  * Create a PR, merge.
  * You are awesome!

### Test Flaps More - Need to Unmute

- Open the [Flaky](https://datalens.yandex/4un3zdm0zcnyr) dashboard.
- Look at the tests in the UNMute candidate table.

![image.png](/kikimr/ydb-qa/mute-autotests/.files/image.png =800x)

- If the `summary:` column shows `mute <= 3` and `success rate >= 98%` - **it's time to enable the test**.
- Open [mute_ya.txt](https://github.com/ydb-platform/ydb/blob/main/.github/config/muted_ya.txt).
- Delete the paths from the file that correspond to our tests.
- Create a PR and merge into main.

{% toc %} 
