pragma yt.UseQLFilter;
pragma yt.UseSkiff='false'; -- temporary disable skiff https://st.yandex-team.ru/YT-14644

select a, c, d, e
from plato.Input
where
    a > 5
    and
    c > 5
    and
    d > 5
    and
    e > 5;