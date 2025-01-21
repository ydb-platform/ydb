pragma yt.UseQLFilter;
pragma yt.UseSkiff='false'; -- temporary disable skiff https://st.yandex-team.ru/YT-14644

select a, c, d, e
from plato.Input
where
    5 < a
    and
    5 < c
    and
    5 < d
    and
    5 < e;