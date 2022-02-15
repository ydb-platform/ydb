# Включение/выключение Scrubbing

## Включение/Отключение

**WILL BE SOON**

## Настройки Scrubbing

Настройки Scrub позволяют регулировать интервал времени, который проходит от начала предыдущего цикла скраббинга диска до начала следующего, а также максимальное число дисков, которые могут скрабиться одновременно. Значение по умолчанию — 1 месяц.
`$ kikimr admin bs config invoke --proto 'Command { UpdateSettings { ScrubPeriodicitySeconds: 86400 MaxScrubbedDisksAtOnce: 1 } }'`

**WILL BE SOON**
