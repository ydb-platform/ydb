```mermaid
flowchart TB
W[Workflow Start] --> CheckExistingPR{Is there<br/>an unmerged PR<br/>with 'rightlib'<br/>label?}

CheckExistingPR --> |No| RightlibCommitCheck{New commits<br>in rightlib branch?}
    RightlibCommitCheck --> |No| Finish
    RightlibCommitCheck --> |Yes| CreatePR[Create a new PR]
        CreatePR --> AddRightlibLabel[Add 'rightlib'<br/> label]
        AddRightlibLabel --> Finish[Finish workflow]

CheckExistingPR --> |Yes| CheckPrFailedLabel{Check PR has<br/>failed label}
CheckPrFailedLabel --> |Yes| Finish
CheckPrFailedLabel --> |No| CheckPRChecks{Check PR<br/>checks}
CheckPRChecks --> |Failed| FailedComment[Failed comment]
FailedComment --> AddPrFailedLabel

CheckPRChecks --> |Pending| Finish

CheckPRChecks --> |Success| MergePR[Merge PR branch]
MergePR --> IsMergeSuccess{Is merge<br/>success?}
IsMergeSuccess --> |Yes| Push
Push --> IsPushSuccess{Is Push<br/>success}
IsPushSuccess --> |Yes| AutoPRClose[PR closes<br/>automatically]
AutoPRClose -->  SuccessComment[Success comment] --> Finish

IsMergeSuccess --> |No| FailedMergeComment[Failed comment]
FailedMergeComment --> AddPrFailedLabel[Add PR failed label]
IsPushSuccess --> |No| FailedPushComment[Failed comment]
FailedPushComment  --> AddPrFailedLabel
AddPrFailedLabel --> Finish
```
