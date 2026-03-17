import yaml
from typing import Set

from fingerprints.types.common import TYPES_PATH, TypesList


def check_types_file() -> None:
    with open(TYPES_PATH, "r") as fh:
        data: TypesList = yaml.safe_load(fh)

    mains: Set[str] = set()
    forms: Set[str] = set()
    for type in data["types"]:
        main = type["main"].lower()
        type["main"] = type["main"]
        if main in mains:
            print("DUPLICATE MAIN", repr(main))
        elif main in forms:
            print("MAIN IS FORM ELSEHWERE", main)
        mains.add(main)
        forms.add(main)
        cur_forms: Set[str] = set()
        for form in type["forms"]:
            form = form.lower()
            if form == main:
                continue
            cur_forms.add(form)
            if form in forms:
                print("DUPLICATE FORM", form)
            forms.add(form)

        type["forms"] = sorted(cur_forms)

        # print(type)

    data["types"] = sorted(data["types"], key=lambda t: t["main"])
    with open(TYPES_PATH, "wb") as fh:
        fh.write(
            yaml.dump(
                data,
                allow_unicode=True,
                encoding="utf-8",
                sort_keys=False,
            )
        )


if __name__ == "__main__":
    check_types_file()
