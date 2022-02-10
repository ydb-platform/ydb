library
===
`library/` is a directory with reusable libraries. Common ideas (with [key words](https://tools.ietf.org/html/rfc2119)):
 
1. Libraries are categorized by the languages in which they will be used.
    Bindings **MUST** be put in `<language>` directory.
 
2. Grouping by any other criteria **SHOULD** be defined by the language committee in `<language>` directory.
 
3. The library **SHOULD** be in use in at least two projects.
    
    If you are not sure if you should put some library in `library/`, please contact `<language>` committee or arcadia-wg@yandex-team.ru.
 
4. The library **SHOULD** be portable.

    Please contact `<language>` committee if you cannot provide usage on all platforms: `linux`, `darwin`, `windows`.
 
5. The library **MUST** depend only on a limited list of external components
    (currently it is `util/`, `contrib/`, `vendor/`, `library/`).

6. Any code in Arcadia (except `contrib/`, `vendor/` and `util/`) **MAY** depend on the `library/`.

7. The library **MUST** be accompanied by `README.md` file and a brief description of the project.

8. The library **MUST** be accompanied by unit-tests.

9. CPU- or/and RAM-bound algorithms **SHOULD** provide benchmarks.

10. There **MUST** be no trade secrets of Yandex in `library/`: anything that can cause harm on publishing as OpenSource. For example:
    * spam filter rules;
    * coefficients for ML;
    * etc.

11. All OSS (OpenSource Software) ready code **MUST** be accompanied by macro [LICENCE](https://docs.yandex-team.ru/ya-make/manual/common/macros#licence(license...)) in `ya.make`.

12. All language specific aspects are defined by `<language>` committee: see `library/<language>/README.md`.

13. The library **MUST** satisfy `<language>` style-guide.

14. The existing library **SHOULD** be improved instead of creating a new one - if it is possible.

    Please do not create yet another library for the same thing: just improve existing one.

Contacts
===
If you have any language-specific questions, please contact `<language>` [committee](https://wiki.yandex-team.ru/devrules/#profilnyekomitety).

If you have any other question about `library/`, please contact arcadia-wg@yandex-team.ru.
