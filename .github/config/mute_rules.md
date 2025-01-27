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

### Unmute stable and flaky tests automaticaly


**setup**
1) ```pip install PyGithub```
2) request git token
```
# Github api (personal access token (classic)) token shoud have permitions to
# repo
# - repo:status
# - repo_deployment
# - public_repo
# admin:org
# project
```
3) save it to env `export GITHUB_TOKEN=<token>
4) save to env `export CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=<iam_cloud_file> 

**How to use**

0) *update your branch* - you shoud have last version of muted_ya localy
1) Run instance https://github.com/ydb-platform/ydb/actions/workflows/collect_analytics.yml
2) wait till end of step `Collect all test monitor (how long tests in state)` (about 7 min)
3) run `create_new_muted_ya.py update_muted_ya` - it creates bunch of files in `%repo_path%/mute_update/`
     
| File Name                              | Description                                                                                     |
|----------------------------------------|-------------------------------------------------------------------------------------------------|
| deleted.txt                            | Tests what look like deleted (no runs 28 days in a row)                                         |
| deleted_debug.txt                      | With detailed info                                                                              |
| flaky.txt                              | Tests which are flaky today AND total runs > 3 AND fail_count > 2                               |
| flaky_debug.txt                        | With detailed info                                                                              |
| muted_stable.txt                       | Muted tests which are stable for the last 14 days                                               |
| muted_stable_debug.txt                 | With detailed info                                                                              |
| new_muted_ya.txt                       | Muted_ya.txt version with excluded **muted_stable** and **deleted** tests                       |
| new_muted_ya_debug.txt                 | With detailed info                                                                              |
| new_muted_ya_with_flaky.txt            | Muted_ya.txt version with excluded **muted_stable** and **deleted** tests and included **flaky**|
| new_muted_ya_with_flaky_debug.txt      | With detailed info                                                                              |
|muted_ya_sorted.txt| original muted_ya with resolved wildcards for real tests (not chunks)|
|muted_ya_sorted_debug.txt| With detailed info|


**1. Unmute Stable**
1) replace content of [muted_ya](https://github.com/ydb-platform/ydb/blob/main/.github/config/muted_ya.txt) with content of **new_muted_ya.txt** 
2) create new PR and paste in PR Description 
- `<Unmuted tests : stable 9 and deleted 0>`  from concole output
-  content from **muted_stable_debug** and **deleted_debug**
3) Merge
 example https://github.com/ydb-platform/ydb/pull/11099

**2. Mute Flaky** (AFTER UNMUTE STABLE ONLY)
1) replace content of [muted_ya](https://github.com/ydb-platform/ydb/blob/main/.github/config/muted_ya.txt) with content of **new_muted_ya_with_flaky.txt** 
2) create new PR 
2) run `create_new_muted_ya.py create_issues` - it creates issue for each flaky test in **flaky.txt** 
3) copy from console output text like ' Created issue ...' and paste in PR
4) merge
 example https://github.com/ydb-platform/ydb/pull/11101