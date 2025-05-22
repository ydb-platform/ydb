select percentile(x,0.99),percentile(x,1.0)
from (values (double("nan")),(1.1),(0.5)) as a(x)
