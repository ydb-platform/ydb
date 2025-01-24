#!/bin/sh

find . -type f -exec sed --regexp-extended --in-place 's/(U?INT[0-9])/SASL_\1/g' '{}' ';'
