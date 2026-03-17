from dateparser_scripts.write_complete_data import write_complete_data


def test_dateparser_data_integrity():
    files = write_complete_data(in_memory=True)

    for filename in files:
        with open(filename, 'rb') as f:
            assert (
                f.read().strip() == files[filename].strip()
            ), "The content of the file \"{}\" doesn't match the content" \
               " of the generated file.".format(filename)
