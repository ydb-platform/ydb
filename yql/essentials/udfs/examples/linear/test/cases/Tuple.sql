$a, $b = Linear::Exchange(Linear::Producer(1),2);
select Linear::UnsafeConsumer($a),$b;
