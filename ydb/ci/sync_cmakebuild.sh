set -e
statusfile="ydb/ci/cmakegen.txt"
prevsha=$(cat ${statusfile})
currsha=$(git rev-parse main)
if [ "${prevsha}" == "${currsha}" ];then
	echo "No changes on the main branch, exiting"
	exit 0
fi
git merge main --no-edit
./generate_cmake -k
echo ${currsha} > ${statusfile}
git add .
git commit -m "Generate cmake for ${currsha}"

