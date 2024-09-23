download() {
	table=$1
	parts=$2
	maxdownloads=10
	for i in `seq $parts`
	do
		part=part-`printf "%03d" $i`.parquet
		uri=$base/$table/$part
		echo $uri
		mkdir -p $table
		wget -q $uri -O $table/$part &
		j=`jobs -r | wc -l`
		if ((j >= maxdownloads))
		then
			wait -n
		fi
	done
	wait
}

