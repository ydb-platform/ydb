pragma yt.UseQLFilter;
pragma yt.UseSkiff='false'; -- temporary disable skiff https://st.yandex-team.ru/YT-14644

select b
from plato.Input
where
    a > 5;