import pyarrow.parquet as pq
import pyarrow as pa

# 读取原始 Parquet 文件
table = pq.read_table('hits.parquet')

# 计算分割点
num_rows = table.num_rows
middle_index = num_rows // 2

# 分割数据
first_half = table.slice(0, middle_index)
second_half = table.slice(middle_index, num_rows - middle_index)

# 保存为新的 Parquet 文件
pq.write_table(first_half, 'hits_part1.parquet')
pq.write_table(second_half, 'hits_part2.parquet')

