select * from solomon_prod.yq
with (
  program = @@{execpool=User,activity=YQ_STORAGE_PROXY,sensor=ActorsAliveByActivity}@@,
  labels = "label1, label2, activity",
  from = "2023-12-08T14:40:39Z",
  to = "2023-12-08T14:45:39Z",
  `downsampling.disabled` = "false",
  `downsampling.grid_interval` = "15",
  `downsampling.grid_aggregation` = "AVG",
  `downsampling.gap_filling` = "PREVIOUS"
) as s
limit 10;