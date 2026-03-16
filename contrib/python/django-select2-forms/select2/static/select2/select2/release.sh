#!/bin/bash

function abspath {
    if [[ -d "$1" ]]
    then
        pushd "$1" >/dev/null
        pwd
        popd >/dev/null
    elif [[ -e $1 ]]
    then
        pushd $(dirname $1) >/dev/null
        echo $(pwd)/$(basename $1)
        popd >/dev/null
    else
        echo $1 does not exist! >&2
        return 127
    fi
}

CURR_REL_DIR=$(dirname $0)
CURR_DIR=$(abspath "${CURR_REL_DIR}")

set -e

echo -n "Enter the version for this release: "

read ver

if [ ! $ver ]; then
	echo "Invalid version."
	exit
fi


name="select2"
js="$CURR_DIR/../js/$name.js"
mini="$CURR_DIR/../js/$name.min.js"
css="$CURR_DIR/../js/$name.css"
timestamp=$(date)
tokens="s/@@ver@@/$ver/g;s/\@@timestamp@@/$timestamp/g"

cp "$CURR_DIR/$name.js" $js
perl -pi -e 's/(?<=\()jQuery/(typeof DjangoSelect2 == '"'"'object'"'"' && DjangoSelect2.jQuery) ? DjangoSelect2.jQuery : jQuery/g;' $js

echo "Tokenizing..."

find . -name "$js" | xargs -I{} sed -e "$tokens" -i "" {}
find . -name "$css" | xargs -I{} sed -e "$tokens" -i "" {}
sed -e "s/latest/$ver/g" -i "" component.json

echo "Minifying..."

echo "/*" > "$mini"
cat LICENSE | sed "$tokens" >> "$mini"
echo "*/" >> "$mini"

curl -s \
	-d compilation_level=SIMPLE_OPTIMIZATIONS \
	-d output_format=text \
	-d output_info=compiled_code \
	--data-urlencode "js_code@$js" \
	http://closure-compiler.appspot.com/compile \
	>> "$mini"

# perl -pi -e 's/jQuery/(typeof grp == 'object' && DjangoSelect2.jQuery) ? DjangoSelect2.jQuery : jQuery/g;' $mini

cp $CURR_DIR/*.png $CURR_DIR/../css
cp $CURR_DIR/*.gif $CURR_DIR/../css