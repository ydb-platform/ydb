## How to Mute a test <a id="how-to-mute"></a>

- Through a PR Report
  - Open report in PR ![screen](https://storage.yandexcloud.net/ydb-public-images/report_mute.png)
  - In context menu of test select `Crete mute issue`

 - Through the [Test history](https://datalens.yandex/4un3zdm0zcnyr?tab=A4) dashboard
  
    - Enter the test name or path in the `full_name contain` field, click **Apply** - the search is done by the occurrence.  ![image.png](https://storage.yandexcloud.net/ydb-public-images/mute_candidate.png)

   - Click the `Mute` link, which will create a draft issue in GitHub.


* Add the issue to the [Mute and Un-mute](https://github.com/orgs/ydb-platform/projects/45/views/6?visibleFields=%5B%22Title%22%2C%22Assignees%22%2C%22Status%22%2C126637100%5D) project.
* Set the `status` to `Mute`
* Set the `owner` field to the team name (see the issue for the owner's name). ![image.png](https://storage.yandexcloud.net/ydb-public-images/create_issue.png)
* Open [muted_ya.txt](https://github.com/ydb-platform/ydb/blob/main/.github/config/muted_ya.txt) in a new tab and edit it.
* Copy the line under `Add line to muted_ya.txt` (for example, like in the screenshot, `ydb/core/kqp/ut/query KqpStats.SysViewClientLost`) and add it to [muted_ya.txt](https://github.com/ydb-platform/ydb/blob/main/.github/config/muted_ya.txt).
* Edit the branch for merging, for example, replace `{username}-patch-1` with `mute/{username}`.
* Create a PR - copy the PR name from the issue name.
* Copy the issue description to the PR, keep the line `Not for changelog (changelog entry is not required)`.
* Take "OK" from member of test owner team in PR
* Merge.
* Link Issue and Pr (field "Development" in issue and PR)
* Inform test owner team about new mutes - dm or in public chat (with mention of maintainer of team)
* You are awesome!

## How to UnMute a test <a id="how-to-unmute"></a>
--IN PROGRESS--
* Open [muted_ya.txt](https://github.com/ydb-platform/ydb/blob/main/.github/config/muted_ya.txt)
* Press "Edit file" and delete line of test
* Commit changes (Edit the branch for merging, for example, replace `{username}-patch-1` with `mute/{username}`)
* Edit PR name like "UnMute {testname}"
* Take "OK" from member of test owner team in PR
* Merge
* If test have an issue in [Mute and Un-mute](https://github.com/orgs/ydb-platform/projects/45/views/6?visibleFields=%5B%22Title%22%2C%22Assignees%22%2C%22Status%22%2C126637100%5D) in status `Muted` - Move it to `Unmuted`
* Link Issue and Pr (field "Development" in issue and PR)
* Move issue to status `Unmuted`
* You are awesome!

## How to manage muted tests by team <a id="how-to-manage"></a>
--IN PROGRESS--
### Explore your tests stability
 >If you want to get more info about stability of your test visit [dashboard](https://datalens.yandex/4un3zdm0zcnyr?tab=wED) (fill field `owner`=`{your_team_name}`)
![image.png](https://storage.yandexcloud.net/ydb-public-images/test_analitycs_1.png)
![image.png](https://storage.yandexcloud.net/ydb-public-images/test_analitycs_2.png)
### Find your muted tests
 >Not all muted tests have issue in github project about this , we working on it
* Open project [Mute and Un-mute](https://github.com/orgs/ydb-platform/projects/45/views/6?visibleFields=%5B%22Title%22%2C%22Assignees%22%2C%22Status%22%2C126637100%5D)
* click in label with name of your team, example [link to qp](https://github.com/orgs/ydb-platform/projects/45/views/6?filterQuery=owner%3Aqp) muted tests (cgi `?filterQuery=owner%3Aqp`)
* Open `Mute {testname}` issue
* Perform [How to unmute](#how-to-unmute)

## Flaky Tests

### Who and When Monitors Flaky Tests

The CI duty engineer (in progress) checks flaky tests once a day (only working days). 

- Open the [Flaky](https://datalens.yandex/4un3zdm0zcnyr) dashboard.
- Perform the sections **[Mute Flaky Test](#mute-flaky)** and **[Test Flaps More - Need to Unmute](#unmute-flaky)** once a day or ondemand

### Mute Flaky Tests <a id="mute-flaky"></a>

Open the [Flaky](https://datalens.yandex/4un3zdm0zcnyr) dashboard.

- Select today's date.
- Look at the tests in the Mute candidate table.

![image.png](https://storage.yandexcloud.net/ydb-public-images/mute_candidate.png)

- Select today's date in the `date_window`.
- Select `days_ago_window = 5` (how many days back from the selected day to calculate statistics). Currently, there are calculations for 1 day and 5 days ago.
  * If you want to understand how long ago and how often the test started failing, you can click the `history` link in the table (loading may take time) or select `days_ago_window = 1`.
- For `days_ago_window = 5`, set the values to filter out isolated failures and low run counts:
  * `fail_count >= 3`
  * `run_count >= 10`
- Click the `Mute` link, which will create a draft issue in GitHub.
- Perform steps from [How to mute](#how-to-mute)
- You are awesome!

### Test is no longer flaky - Time to Unmute <a id="unmute-flaky"></a>

- Open the [Flaky](https://datalens.yandex/4un3zdm0zcnyr) dashboard.
- Look at the tests in the UNMute candidate table.

![image.png](https://storage.yandexcloud.net/ydb-public-images/unmute.png)

- If the `summary:` column shows `mute <= 3` and `success rate >= 98%` - **it's time to enable the test**.
- Perform steps from [How to Unmute](#how-to-unmute)
- You are awesome!
